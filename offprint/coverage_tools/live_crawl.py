from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup

from ..adapters.generic import DEFAULT_HEADERS
from ..polite_requests import PoliteRequestsSession


def _normalize_url(url: str) -> str:
    normalized, _frag = urldefrag(url.strip())
    return normalized


def _is_pdf_url(url: str) -> bool:
    lowered = url.lower()
    return ".pdf" in lowered and not lowered.endswith(".pdfx")  # be permissive


def _same_site(url: str, allowed_netlocs: Set[str]) -> bool:
    try:
        netloc = urlparse(url).netloc.lower()
    except Exception:
        return False
    return netloc in allowed_netlocs


def _extract_links(html: str, base_url: str) -> Tuple[Set[str], Set[str]]:
    soup = BeautifulSoup(html, "lxml")
    pdf_urls: Set[str] = set()
    other_urls: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        abs_url = _normalize_url(abs_url)
        if _is_pdf_url(abs_url):
            pdf_urls.add(abs_url)
        else:
            other_urls.add(abs_url)
    return pdf_urls, other_urls


def _pick_sample(items: Sequence[str], k: int) -> List[str]:
    if k <= 0:
        return []
    if len(items) <= k:
        return list(items)
    # Deterministic spread: first, quarter, middle, three-quarter, last...
    idxs = [0, len(items) // 4, len(items) // 2, (3 * len(items)) // 4, len(items) - 1]
    out: List[str] = []
    for i in idxs:
        if len(out) >= k:
            break
        u = items[i]
        if u not in out:
            out.append(u)
    # If still short, fill from the front.
    for u in items:
        if len(out) >= k:
            break
        if u not in out:
            out.append(u)
    return out[:k]


def _guess_archive_candidates(seed_url: str, html: str) -> List[str]:
    """Return candidate archive-ish URLs discovered on the seed page."""
    soup = BeautifulSoup(html, "lxml")
    candidates: List[str] = []
    for a in soup.select("a[href]"):
        text = (a.get_text(" ", strip=True) or "").lower()
        href = a.get("href") or ""
        lowered = href.lower()
        if any(
            k in text for k in ["archive", "issues", "past issues", "all issues", "back issues"]
        ):
            candidates.append(urljoin(seed_url, href))
            continue
        if any(
            k in lowered
            for k in ["/issue/archive", "issue/archive", "issue/view", "issues", "archive"]
        ):
            candidates.append(urljoin(seed_url, href))
    # De-dupe, keep order
    seen: Set[str] = set()
    out: List[str] = []
    for c in candidates:
        c = _normalize_url(c)
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _looks_js_required(html: str) -> bool:
    # Lightweight heuristic: minimal anchors + lots of scripts / app mounts.
    soup = BeautifulSoup(html, "lxml")
    a_count = len(soup.select("a[href]"))
    script_count = len(soup.select("script"))
    text = soup.get_text(" ", strip=True).lower()
    if "enable javascript" in text or "please enable javascript" in text:
        return True
    if a_count == 0 and script_count >= 10:
        return True
    if soup.select_one("#root") and script_count >= 5 and a_count <= 2:
        return True
    return False


@dataclass
class CrawlSiteResult:
    seed_url: str
    archive_url: str
    pages_visited: List[str] = field(default_factory=list)
    pdf_urls: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    js_required: bool = False


def crawl_site(
    seed_url: str,
    *,
    timeout_s: int = 30,
    max_issues: int = 5,
    max_articles_per_issue: int = 2,
    session: Optional[requests.Session] = None,
    playwright_fallback: bool = True,
) -> CrawlSiteResult:
    sess = session or PoliteRequestsSession(min_delay=1.0, max_delay=3.0)
    seed_url = _normalize_url(seed_url)
    allowed = {urlparse(seed_url).netloc.lower()}

    pw_sess = None

    def fetch_requests(url: str) -> Optional[str]:
        try:
            resp = sess.get(url, timeout=timeout_s, headers=DEFAULT_HEADERS)
            if resp is None or resp.status_code >= 400:
                return None
            return resp.text
        except Exception:
            return None

    def fetch(url: str) -> Tuple[Optional[str], bool]:
        """Fetch a URL with requests-first; optionally fall back to Playwright for JS-heavy pages."""
        nonlocal pw_sess
        html = fetch_requests(url)
        if html and not _looks_js_required(html):
            return html, False
        if not playwright_fallback:
            return html, False
        try:
            if pw_sess is None:
                from ..playwright_session import PlaywrightSession

                pw_sess = PlaywrightSession(min_delay=1.0, max_delay=2.0, headless=False)
            pw_resp = pw_sess.get(url, headers=DEFAULT_HEADERS, timeout=timeout_s)
            if pw_resp is None or pw_resp.status_code >= 400:
                return html, False
            return pw_resp.text, True
        except Exception:
            return html, False

    try:
        js_required = False
        seed_html, used_pw = fetch(seed_url)
        js_required = js_required or used_pw
        if not seed_html:
            return CrawlSiteResult(
                seed_url=seed_url,
                archive_url=seed_url,
                pages_visited=[seed_url],
                errors=[f"failed to fetch seed: {seed_url}"],
            )

        archive_url = seed_url
        archive_candidates = _guess_archive_candidates(seed_url, seed_html)
        if archive_candidates:
            archive_url = archive_candidates[0]

        if archive_url == seed_url:
            archive_html = seed_html
        else:
            archive_html, used_pw = fetch(archive_url)
            js_required = js_required or used_pw

        if not archive_html:
            return CrawlSiteResult(
                seed_url=seed_url,
                archive_url=archive_url,
                pages_visited=[seed_url, archive_url],
                errors=[f"failed to fetch archive: {archive_url}"],
            )

        js_required = js_required or _looks_js_required(archive_html)

        # Extract issue-like links from archive page, keep internal links.
        archive_pdfs, archive_links = _extract_links(archive_html, archive_url)
        issue_links = sorted(
            [
                u
                for u in archive_links
                if _same_site(u, allowed)
                and any(k in u.lower() for k in ["issue", "volume", "vol", "archives", "archive"])
            ]
        )
        sampled_issue_links = _pick_sample(issue_links, max_issues)

        visited: List[str] = [seed_url]
        if archive_url != seed_url:
            visited.append(archive_url)
        pdfs: Set[str] = set(archive_pdfs)

        for issue_url in sampled_issue_links:
            issue_url = _normalize_url(issue_url)
            if issue_url in visited:
                continue
            issue_html, used_pw = fetch(issue_url)
            js_required = js_required or used_pw
            visited.append(issue_url)
            if not issue_html:
                continue
            issue_pdfs, issue_links2 = _extract_links(issue_html, issue_url)
            pdfs |= issue_pdfs

            # Sample a couple of likely article pages to find PDFs hidden behind an article view.
            article_candidates = [
                u
                for u in sorted(issue_links2)
                if _same_site(u, allowed)
                and not _is_pdf_url(u)
                and any(
                    k in u.lower()
                    for k in ["article", "view", "doi", "abstract", "pdf", "download"]
                )
            ]
            for article_url in _pick_sample(article_candidates, max_articles_per_issue):
                if article_url in visited:
                    continue
                article_html, used_pw = fetch(article_url)
                js_required = js_required or used_pw
                visited.append(article_url)
                if not article_html:
                    continue
                article_pdfs, _ = _extract_links(article_html, article_url)
                pdfs |= article_pdfs
                time.sleep(0.1)

        return CrawlSiteResult(
            seed_url=seed_url,
            archive_url=_normalize_url(archive_url),
            pages_visited=visited,
            pdf_urls=sorted(pdfs),
            errors=[],
            js_required=js_required,
        )
    finally:
        if pw_sess is not None and hasattr(pw_sess, "close"):
            try:
                pw_sess.close()
            except Exception:
                pass


def run_live_crawl(
    seeds: Iterable[str],
    *,
    timeout_s: int = 30,
    max_issues: int = 5,
    max_articles_per_issue: int = 2,
    playwright_fallback: bool = True,
) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    with PoliteRequestsSession(min_delay=1.0, max_delay=3.0) as sess:
        for seed_url in seeds:
            seed_url = seed_url.strip()
            if not seed_url or seed_url.startswith("#"):
                continue
            if not seed_url.startswith("http://") and not seed_url.startswith("https://"):
                seed_url = f"https://{seed_url}"
            domain = urlparse(seed_url).netloc.lower()
            res = crawl_site(
                seed_url,
                timeout_s=timeout_s,
                max_issues=max_issues,
                max_articles_per_issue=max_articles_per_issue,
                session=sess,
                playwright_fallback=playwright_fallback,
            )
            out[domain] = asdict(res)
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sites": out,
    }


def write_live_diff_json(payload: Dict[str, Dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
