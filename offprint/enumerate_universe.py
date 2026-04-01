from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from .adapters import pick_adapter_for
from .adapters.scholastica_base import ScholasticaBaseAdapter
from .adapters.wordpress_academic_base import WordPressAcademicBaseAdapter
from .orchestrator import read_sitemaps_csv, read_sitemaps_dir
from .polite_requests import PoliteRequestsSession


def dedupe_seeds_by_domain(seeds: List[str]) -> List[str]:
    seen = set()
    deduped: List[str] = []
    for seed in seeds:
        domain = urlparse(seed).netloc or seed
        if domain not in seen:
            seen.add(domain)
            deduped.append(seed)
    return deduped


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def count_wp_posts(
    base_url: str, session: PoliteRequestsSession
) -> Tuple[Optional[int], Optional[str]]:
    api_url = base_url.rstrip("/") + "/wp-json/wp/v2/posts?per_page=1"
    try:
        resp = session.get(api_url, timeout=10, headers=DEFAULT_HEADERS)
        if resp is None or resp.status_code >= 400:
            return None, f"wp-json returned {resp.status_code if resp else 'no response'}"
        total = resp.headers.get("X-WP-Total")
        if total:
            return int(total), None
        data = resp.json()
        if isinstance(data, list) and data:
            return len(data), None
        return None, "wp-json returned empty payload"
    except Exception as exc:  # pragma: no cover - best-effort helper
        return None, str(exc)


def rough_count_from_archive(
    seed: str,
    session: PoliteRequestsSession,
    max_pages: int = 5,
) -> Tuple[Optional[int], Optional[str]]:
    """Best-effort HTML count when wp-json is unavailable."""
    try:
        to_visit = [seed]
        visited = set()
        total = 0

        def extract_links(soup: BeautifulSoup) -> List[str]:
            links: List[str] = []
            for sel in ["a.next", "a.nextpostslink", "a.page-numbers.next", "a[rel='next']"]:
                for a in soup.select(sel):
                    href = a.get("href")
                    if href:
                        links.append(href)
            return links

        while to_visit and len(visited) < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            resp = session.get(url, timeout=10, headers=DEFAULT_HEADERS)
            if resp is None or resp.status_code >= 400:
                return (
                    total if total else None
                ), f"archive returned {resp.status_code if resp else 'no response'}"

            soup = BeautifulSoup(resp.content, "lxml")
            articles = soup.select("article, .post, .entry-title a, h2.entry-title a, h2 a")
            if articles:
                total += len(articles)
            for link in extract_links(soup):
                if link not in visited and len(visited) + len(to_visit) < max_pages:
                    to_visit.append(link)

        if total:
            return total, None
        return None, "no obvious articles on page"
    except Exception as exc:  # pragma: no cover - best-effort helper
        return None, str(exc)


def count_from_sitemap_index(
    base_url: str, session: PoliteRequestsSession
) -> Tuple[Optional[int], Optional[str]]:
    """Count URLs via Yoast sitemap index if available."""
    try:
        index_url = base_url.rstrip("/") + "/sitemap.xml"
        resp = session.get(index_url, timeout=10, headers=DEFAULT_HEADERS)
        if resp is None or resp.status_code >= 400:
            return None, f"sitemap index returned {resp.status_code if resp else 'no response'}"

        soup = BeautifulSoup(resp.content, "xml")
        sitemap_urls = [loc.get_text() for loc in soup.find_all("loc")]
        if not sitemap_urls:
            return None, "no sitemaps found"

        total = 0
        for sm_url in sitemap_urls:
            sm_resp = session.get(sm_url, timeout=10, headers=DEFAULT_HEADERS)
            if sm_resp is None or sm_resp.status_code >= 400:
                continue
            sm_soup = BeautifulSoup(sm_resp.content, "xml")
            total += len(sm_soup.find_all("url"))

        return (total if total else None), None if total else "sitemaps empty"
    except Exception as exc:  # pragma: no cover - best-effort helper
        return None, str(exc)


def enumerate_scholastica(seed: str, adapter: ScholasticaBaseAdapter) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "seed": seed,
        "domain": urlparse(seed).netloc or seed,
        "platform": "Scholastica",
    }
    journal = adapter._extract_journal_data(seed)
    if not journal:
        result["error"] = "could not read window.JOURNAL"
        return result

    ids = journal.get("published_article_ids") or []
    issues = journal.get("published_issue_ids") or []
    adapter.journal_slug = adapter.journal_slug or journal.get("slug")
    base_url = adapter._get_base_url()
    article_urls = [f"{base_url}/articles/{aid}" for aid in ids]

    result.update(
        {
            "count": len(ids),
            "article_ids": ids,
            "issue_count": len(issues),
            "article_urls": article_urls,
        }
    )
    return result


def enumerate_wordpress(seed: str, adapter: WordPressAcademicBaseAdapter) -> Dict[str, Any]:
    parsed = urlparse(seed)
    base_url = adapter.base_url or f"{parsed.scheme}://{parsed.netloc}"
    count, error = count_wp_posts(base_url, adapter.session)  # type: ignore[arg-type]
    if count is None:
        # Try a quick HTML archive count if the API is unavailable
        count, error = rough_count_from_archive(seed, adapter.session)  # type: ignore[arg-type]
    if count is None:
        # Try Yoast-style sitemaps as a last resort
        count, error = count_from_sitemap_index(base_url, adapter.session)  # type: ignore[arg-type]
    result: Dict[str, Any] = {
        "seed": seed,
        "domain": parsed.netloc or seed,
        "platform": "WordPressAcademic",
        "count": count,
    }
    if error:
        result["error"] = error
    return result


def enumerate_seed(seed: str, min_delay: float, max_delay: float) -> Dict[str, Any]:
    session = PoliteRequestsSession(min_delay=min_delay, max_delay=max_delay)
    adapter = pick_adapter_for(seed, session=session, allow_generic=True)
    if isinstance(adapter, ScholasticaBaseAdapter):
        return enumerate_scholastica(seed, adapter)
    if isinstance(adapter, WordPressAcademicBaseAdapter):
        return enumerate_wordpress(seed, adapter)
    return {
        "seed": seed,
        "domain": urlparse(seed).netloc or seed,
        "platform": adapter.__class__.__name__,
        "count": None,
        "error": "no fast path; run full orchestrator for this site",
    }


def load_seeds(sitemaps_dir: Optional[str], sitemaps_csv: Optional[str]) -> List[str]:
    seeds: List[str] = []
    if sitemaps_dir:
        seeds.extend(read_sitemaps_dir(sitemaps_dir))
    if sitemaps_csv:
        seeds.extend(read_sitemaps_csv(sitemaps_csv))
    if not seeds:
        raise ValueError("Provide --sitemaps-dir or --sitemaps")
    return dedupe_seeds_by_domain(seeds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fast enumeration of sites to estimate PDF counts")
    parser.add_argument("--sitemaps-dir", help="Directory with JSON sitemaps")
    parser.add_argument("--sitemaps", help="CSV with embedded sitemaps")
    parser.add_argument("--max-workers", type=int, default=8, help="Concurrency level")
    parser.add_argument("--output", default="enum_universe.jsonl", help="Output JSONL")
    parser.add_argument("--min-delay", type=float, default=1.0, help="Minimum per-request delay")
    parser.add_argument("--max-delay", type=float, default=3.0, help="Maximum per-request delay")
    args = parser.parse_args()

    seeds = load_seeds(args.sitemaps_dir, args.sitemaps)
    print(f"Enumerating {len(seeds)} seeds with up to {args.max_workers} workers...")

    results: List[Dict[str, Any]] = []
    total_known = 0

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_map = {
            executor.submit(enumerate_seed, seed, args.min_delay, args.max_delay): seed
            for seed in seeds
        }
        for fut in as_completed(future_map):
            res = fut.result()
            results.append(res)
            domain = res.get("domain", res.get("seed"))
            count = res.get("count")
            platform = res.get("platform")
            error = res.get("error")
            if count is not None:
                total_known += count
            status = f"{domain} [{platform}] -> {count if count is not None else 'unknown'}"
            if error:
                status += f" ({error})"
            print(status)

    with open(args.output, "w", encoding="utf-8") as f:
        for res in results:
            f.write(json.dumps(res) + "\n")

    print(f"Estimated PDFs/articles (where known): {total_known}")
    print(f"Wrote details to {args.output}")


if __name__ == "__main__":
    main()
