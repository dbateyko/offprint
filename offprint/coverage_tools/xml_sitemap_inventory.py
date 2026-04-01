from __future__ import annotations

import gzip
import json
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse, urldefrag

import requests
from bs4 import BeautifulSoup

from ..adapters.generic import DEFAULT_HEADERS
from ..polite_requests import PoliteRequestsSession
from .live_crawl import _extract_links, _pick_sample


def _normalize_url(url: str) -> str:
    normalized, _frag = urldefrag(url.strip())
    return normalized


def _is_same_site(url: str, allowed_netlocs: Set[str]) -> bool:
    try:
        netloc = urlparse(url).netloc.lower()
    except Exception:
        return False
    return netloc in allowed_netlocs


def _discover_sitemap_urls(
    origin: str, session: requests.Session, timeout_s: int
) -> Tuple[List[str], List[str]]:
    """Return (sitemap_urls, errors)."""
    errors: List[str] = []
    origin = origin.rstrip("/")
    robots_url = origin + "/robots.txt"

    sitemap_urls: List[str] = []
    try:
        resp = session.get(robots_url, timeout=timeout_s, headers=DEFAULT_HEADERS)
        if resp is not None and resp.status_code < 400:
            for line in (resp.text or "").splitlines():
                if line.lower().startswith("sitemap:"):
                    u = line.split(":", 1)[1].strip()
                    if u:
                        sitemap_urls.append(_normalize_url(u))
    except Exception as e:
        errors.append(f"robots: {type(e).__name__}: {e}")

    if sitemap_urls:
        # De-dupe in order
        seen: Set[str] = set()
        out: List[str] = []
        for u in sitemap_urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out, errors

    # Fallback to common endpoints.
    for path in ("/sitemap.xml", "/sitemap_index.xml", "/wp-sitemap.xml"):
        u = origin + path
        try:
            resp = session.get(
                u, timeout=timeout_s, headers={"Accept": "application/xml", **DEFAULT_HEADERS}
            )
            if resp is None or resp.status_code >= 400 or not resp.content:
                continue
            text = resp.text or ""
            if "<urlset" in text or "<sitemapindex" in text:
                sitemap_urls.append(_normalize_url(u))
        except Exception:
            continue

    if not sitemap_urls:
        errors.append("no sitemap URLs discovered (robots.txt and common endpoints)")

    return sitemap_urls, errors


def _fetch_sitemap_bytes(
    url: str, session: requests.Session, timeout_s: int
) -> Tuple[Optional[bytes], Optional[str]]:
    try:
        resp = session.get(
            url, timeout=timeout_s, headers={"Accept": "application/xml", **DEFAULT_HEADERS}
        )
        if resp is None or resp.status_code >= 400 or not resp.content:
            return None, f"http {resp.status_code if resp is not None else 'no response'}"
        content = resp.content
        if url.lower().endswith(".gz"):
            content = gzip.decompress(content)
        return content, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _parse_sitemap(content: bytes) -> Tuple[List[str], List[str]]:
    """Return (child_sitemaps, page_urls)."""
    soup = BeautifulSoup(content, "xml")
    if soup.find("sitemapindex"):
        locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
        return [u for u in locs if u], []
    if soup.find("urlset"):
        locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
        return [], [u for u in locs if u]
    return [], []


_PAGE_HINTS = (
    "/issue/",
    "/article/",
    "/vol",
    "/volume",
    "/archives",
    "/archive",
    "issue/view",
    "article/view",
)


def _filter_page_urls(urls: Sequence[str], allowed_netlocs: Set[str]) -> List[str]:
    out: List[str] = []
    for u in urls:
        u = _normalize_url(u)
        if not u or u.lower().endswith(".pdf"):
            continue
        if not _is_same_site(u, allowed_netlocs):
            continue
        low = u.lower()
        if any(h in low for h in _PAGE_HINTS):
            out.append(u)
    return sorted(set(out))


@dataclass
class SitemapInventoryResult:
    seed_url: str
    sitemap_urls: List[str] = field(default_factory=list)
    page_urls_sampled: List[str] = field(default_factory=list)
    pdf_urls: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


def inventory_site_from_sitemaps(
    seed_url: str,
    *,
    session: requests.Session,
    timeout_s: int = 30,
    max_pages: int = 50,
    max_sitemaps: int = 200,
) -> SitemapInventoryResult:
    seed_url = seed_url.strip()
    if not seed_url.startswith("http://") and not seed_url.startswith("https://"):
        seed_url = "https://" + seed_url
    seed_url = _normalize_url(seed_url)

    parsed = urlparse(seed_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    allowed = {parsed.netloc.lower()}

    sitemap_urls, errors = _discover_sitemap_urls(origin, session, timeout_s)

    visited_sitemaps: Set[str] = set()
    queue: List[str] = list(sitemap_urls)
    page_urls: List[str] = []

    while queue and len(visited_sitemaps) < max_sitemaps:
        sm = queue.pop(0)
        if sm in visited_sitemaps:
            continue
        visited_sitemaps.add(sm)

        content, err = _fetch_sitemap_bytes(sm, session, timeout_s)
        if err:
            errors.append(f"sitemap {sm}: {err}")
            continue
        if not content:
            continue

        child_sitemaps, urls = _parse_sitemap(content)
        for child in child_sitemaps:
            child = _normalize_url(child)
            if child and child not in visited_sitemaps:
                queue.append(child)
        page_urls.extend(urls)

    page_candidates = _filter_page_urls(page_urls, allowed)
    sampled_pages = _pick_sample(page_candidates, max_pages)

    pdfs: Set[str] = set()
    visited: List[str] = []
    for page in sampled_pages:
        visited.append(page)
        try:
            resp = session.get(page, timeout=timeout_s, headers=DEFAULT_HEADERS)
            if resp is None or resp.status_code >= 400 or not resp.text:
                continue
            page_pdfs, _ = _extract_links(resp.text, page)
            pdfs |= page_pdfs
        except Exception:
            continue

    return SitemapInventoryResult(
        seed_url=seed_url,
        sitemap_urls=sorted(set(sitemap_urls)),
        page_urls_sampled=visited,
        pdf_urls=sorted(pdfs),
        errors=errors,
    )


def run_sitemap_inventory(
    seeds: Iterable[str],
    *,
    timeout_s: int = 30,
    max_pages: int = 50,
    max_sitemaps: int = 200,
) -> Dict[str, Dict]:
    sites: Dict[str, Dict] = {}
    with PoliteRequestsSession(min_delay=1.0, max_delay=3.0) as session:
        for seed in seeds:
            seed = seed.strip()
            if not seed or seed.startswith("#"):
                continue
            if not seed.startswith("http://") and not seed.startswith("https://"):
                seed = "https://" + seed
            domain = urlparse(seed).netloc.lower()
            res = inventory_site_from_sitemaps(
                seed,
                session=session,
                timeout_s=timeout_s,
                max_pages=max_pages,
                max_sitemaps=max_sitemaps,
            )
            sites[domain] = asdict(res)

    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sites": sites,
    }


def write_sitemap_inventory_json(payload: Dict[str, Dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
