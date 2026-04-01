from __future__ import annotations

import json
import os
import re
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


class QuartexAdapter(Adapter):
    """Adapter for AM Quartex front ends (e.g., repository.law.upenn.edu).

    Approach:
    - Seed should be a journal home or issues listing under the host
      (e.g., /university-of-pennsylvania-law-review or /.../uplr).
    - Discover article/detail links scoped to the journal slug.
    - Follow detail pages to extract PDF links and basic metadata.
    """

    def __init__(self, session=None, force_playwright: bool = False):
        super().__init__(session=session)
        self._seen_pages: set[str] = set()
        self._seen_pdfs: set[str] = set()
        self.force_playwright = force_playwright

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20)
            if resp is None or resp.status_code >= 400:
                return None
            return BeautifulSoup(resp.text, "lxml")
        except Exception:
            return None

    def _journal_slug(self, seed_url: str) -> str:
        parsed = urlparse(seed_url)
        parts = [p for p in (parsed.path or "").split("/") if p]
        # pick the first path segment as slug
        return parts[0].lower() if parts else ""

    def _in_scope(self, url: str, seed_url: str) -> bool:
        parsed_seed = urlparse(seed_url)
        parsed = urlparse(url)
        if (parsed.netloc or "").lower() != (parsed_seed.netloc or "").lower():
            return False

        path_lower = (parsed.path or "").lower()
        # Quartex detail links are frequently global (/Documents/Detail/...) and do
        # not include the journal slug from the seed path.
        if "/documents/detail/" in path_lower:
            return True

        slug = self._journal_slug(seed_url)
        if not slug:
            return True

        query_lower = (parsed.query or "").lower()

        if path_lower.startswith(f"/{slug}"):
            return True
        if slug in path_lower:
            return True
        if slug in query_lower:
            return True
        return False

    def _extract_detail_links(self, soup: BeautifulSoup, base_url: str, seed_url: str) -> List[str]:
        links: Set[str] = set()
        slug = self._journal_slug(seed_url)

        # Cards and thumbnails sometimes put the href on nested elements; capture both
        link_candidates = list(soup.select("a[href]"))
        link_candidates.extend(soup.select(".card__content[href], .card__media[href], section.featured-thumbnail [href]"))

        for a in link_candidates:
            href = a.get("href") or ""
            if not href or href.startswith("#"):
                continue
            absolute = urljoin(base_url, href)

            parsed = urlparse(absolute)
            if parsed.scheme not in {"http", "https"}:
                continue
            if not self._in_scope(absolute, seed_url):
                continue

            lower_abs = absolute.lower()
            text_lower = a.get_text(" ", strip=True).lower()
            classes = " ".join(a.get("class", []))

            # Detail pages
            if "/documents/detail/" in lower_abs:
                links.add(absolute)
                continue

            # Issues / article cards / view all issues
            if "view all issues" in text_lower or "all issues" in text_lower:
                links.add(absolute)
                continue
            if "issue" in text_lower or "volume" in text_lower:
                links.add(absolute)
                continue
            if slug and slug in lower_abs:
                links.add(absolute)
                continue
            if "card" in classes or "thumbnail" in classes:
                links.add(absolute)
                continue

        return list(links)

    def _extract_metadata(self, soup: BeautifulSoup, page_url: str) -> Dict:
        metadata: Dict[str, object] = {"source_url": page_url, "url": page_url, "dc_source": "quartex"}

        # Structured meta tags first
        meta_title = soup.find("meta", attrs={"name": "citation_title"})
        if meta_title and (meta_title.get("content") or "").strip():
            metadata["title"] = (meta_title.get("content") or "").strip()

        meta_authors = soup.find_all("meta", attrs={"name": "citation_author"})
        authors: List[str] = [ma.get("content", "").strip() for ma in meta_authors if ma.get("content")]

        pub_date = soup.find("meta", attrs={"name": "citation_publication_date"})
        if pub_date and (pub_date.get("content") or "").strip():
            metadata["date"] = (pub_date.get("content") or "").strip()

        if not metadata.get("title"):
            title_elem = soup.find(["h1", "h2"], string=True)
            if title_elem:
                metadata["title"] = title_elem.get_text(strip=True)

        # Authors: fall back to visible blocks
        author_blocks = soup.select("[class*=author], .authors, .author")
        for blk in author_blocks:
            text = blk.get_text(" ", strip=True)
            if text:
                authors.extend([t.strip() for t in re.split(r";|,|\band\b", text) if t.strip()])
        if authors:
            metadata["authors"] = authors

        # Date/year
        year_match = re.search(r"\b(19|20)\d{2}\b", soup.get_text(" ", strip=True))
        if year_match:
            metadata["year"] = year_match.group(0)

        return metadata

    def _extract_pdf_links(self, soup: BeautifulSoup, page_url: str) -> List[str]:
        pdfs: Set[str] = set()
        page_html = str(soup)

        # Quartex detail pages often expose only a JS-driven Download flow.
        # Extract a stable API-backed full-PDF URL when tokens are present.
        api_pdf = self._extract_quartex_full_pdf_url(page_html)
        if api_pdf:
            pdfs.add(api_pdf)

        # Meta tags
        for meta in soup.find_all("meta", attrs={"name": "citation_pdf_url"}):
            href = meta.get("content") or ""
            if href:
                pdfs.add(urljoin(page_url, href))

        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if not href:
                continue
            lower = href.lower()
            if href.startswith("#"):
                continue
            if "pdf" in lower:
                pdfs.add(urljoin(page_url, href))
                continue
            if "download" in lower and "pdf" in a.get_text(" ", strip=True).lower():
                pdfs.add(urljoin(page_url, href))
                continue
            if a.get("data-file-format", "").lower() == "pdf":
                pdfs.add(urljoin(page_url, href))

        return list(pdfs)

    def _extract_quartex_download_context(self, page_html: str) -> Optional[tuple[str, str, str]]:
        website_match = re.search(r'window\["WebsiteKey"\]\s*=\s*"([^"]+)"', page_html)
        if not website_match:
            website_match = re.search(r'websiteKey\s*:\s*"([^"]+)"', page_html)
        id_match = re.search(r"parentInfo\s*=\s*\{\s*id:\s*(\d+)", page_html, flags=re.IGNORECASE)
        token_match = re.search(r'downloadToken\s*:\s*"([^"]+)"', page_html)

        if not website_match or not id_match or not token_match:
            return None

        website_key = website_match.group(1).strip()
        asset_id = id_match.group(1).strip()
        download_token = token_match.group(1).strip()
        if not website_key or not asset_id or not download_token:
            return None
        return website_key, asset_id, download_token

    def _extract_quartex_full_pdf_url(self, page_html: str) -> Optional[str]:
        context = self._extract_quartex_download_context(page_html)
        if not context:
            return None
        website_key, asset_id, download_token = context

        api_url = (
            f"https://frontend-api.quartexcollections.com/v1/{website_key}/download/"
            f"{asset_id}/pdf/init?create=true"
        )
        headers = dict(DEFAULT_HEADERS)
        headers["Accept"] = "application/json, text/plain, */*"
        headers["Authorization"] = f"Bearer {download_token}"

        try:
            resp = self.session.get(api_url, headers=headers, timeout=25)
            if resp is None or getattr(resp, "status_code", 500) >= 400:
                return None
            try:
                payload = resp.json()
            except Exception:
                payload = json.loads(getattr(resp, "text", "") or "{}")
        except Exception:
            return None

        download_url = str(payload.get("downloadUrl") or "").strip()
        encoded_token = str(payload.get("encodedImageToken") or "").strip()
        if not download_url or not encoded_token:
            return None
        sep = "&" if "?" in download_url else "?"
        return f"{download_url}{sep}jwt={encoded_token}"

    def _playwright_download_pdf(self, detail_url: str) -> List[str]:
        """Attempt to trigger Quartex download via Playwright click flow."""

        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            return []

        pdf_urls: List[str] = []
        headed_available = bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=not headed_available)
                context = browser.new_context(
                    user_agent=DEFAULT_HEADERS.get("User-Agent", ""),
                    viewport={"width": 1440, "height": 900},
                )
                page = context.new_page()

                def handle_response(resp):
                    try:
                        ctype = (resp.headers.get("content-type") or "").lower()
                    except Exception:
                        ctype = ""
                    url = resp.url
                    if "pdf" in ctype or url.lower().endswith(".pdf"):
                        pdf_urls.append(url)

                page.on("response", handle_response)

                page.goto(detail_url, wait_until="networkidle", timeout=30000)

                # Select the full PDF option if present
                try:
                    if page.query_selector("#dwld-opt-doc-fullpdf"):
                        page.check("#dwld-opt-doc-fullpdf", timeout=5000)
                except Exception:
                    pass

                # Try to trigger the download via visible Download buttons/links
                trigger_selectors = [
                    'button:has-text("Download")',
                    'a:has-text("Download")',
                    'text=Download',
                ]

                for selector in trigger_selectors:
                    try:
                        with page.expect_download(timeout=12000) as dl_info:
                            page.click(selector, timeout=5000)
                        download = dl_info.value
                        if download and download.url:
                            pdf_urls.append(download.url)
                            break
                    except Exception:
                        continue

                context.close()
                browser.close()
        except Exception:
            pass

        return list(dict.fromkeys(pdf_urls))

    # --- Playwright-assisted discovery ---
    def _discover_with_playwright(self, seed_url: str, max_depth: int) -> Iterable[DiscoveryResult]:
        """Fallback path using PlaywrightSession to render JS-heavy pages.

        Quartex download links are often attached to modal buttons that populate
        via client-side bindings. We render pages and re-use the same parsing
        logic to extract PDF anchors/meta tags.
        """

        try:
            from ..playwright_session import PlaywrightSession
        except Exception:
            return []

        results: List[DiscoveryResult] = []
        headed_available = bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))
        with PlaywrightSession(
            headless=not headed_available, min_delay=1.0, max_delay=2.5
        ) as pw:
            # Replace the adapter session with Playwright for this traversal
            original_session = self.session
            self.session = pw
            try:
                for result in self._discover_pdfs_requests(seed_url, max_depth=max_depth):
                    results.append(result)
            finally:
                # Restore original session to avoid side effects
                self.session = original_session

        return results

    def _discover_pdfs_requests(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        """Internal requests-based discovery (no Playwright fallback)."""
        queue: List[tuple[str, int]] = [(seed_url, 0)]
        host = urlparse(seed_url).netloc

        while queue:
            url, depth = queue.pop(0)
            if url in self._seen_pages:
                continue
            self._seen_pages.add(url)

            soup = self._get(url)
            if not soup:
                continue

            # If this page has PDF links, yield them with metadata
            pdf_links = self._extract_pdf_links(soup, url)

            # If no direct links on a detail page, try the Playwright click flow
            if not pdf_links and "/documents/detail/" in url.lower():
                pdf_links = self._playwright_download_pdf(url)

            if pdf_links:
                metadata = self._extract_metadata(soup, url)
                for pdf_url in pdf_links:
                    normalized_pdf = pdf_url
                    if normalized_pdf in self._seen_pdfs:
                        continue
                    self._seen_pdfs.add(normalized_pdf)
                    yield DiscoveryResult(
                        page_url=url,
                        pdf_url=normalized_pdf,
                        metadata=dict(metadata),
                        source_adapter="quartex",
                        extraction_path="quartex_page",
                    )

            # Enqueue detail/issue links within scope
            if max_depth == 0 or depth < max_depth:
                for link in self._extract_detail_links(soup, url, seed_url):
                    parsed = urlparse(link)
                    if parsed.netloc != host:
                        continue
                    if link not in self._seen_pages:
                        queue.append((link, depth + 1))

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        """Try requests-based discovery first; fall back to Playwright if no results.

        Quartex pages are often JavaScript-heavy, so the initial requests-based pass
        may return zero links. In that case, we automatically retry with Playwright
        to render the page and discover links.
        """
        if not self.force_playwright:
            # First pass: requests-based discovery
            results = list(self._discover_pdfs_requests(seed_url, max_depth=max_depth))
            if results:
                for r in results:
                    yield r
                return

        # Fallback: Playwright-rendered traversal (resets seen sets to allow re-crawl)
        self._seen_pages.clear()
        self._seen_pdfs.clear()
        for r in self._discover_with_playwright(seed_url, max_depth):
            yield r

    def discover_pdfs_with_playwright(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        """Explicit Playwright entry (for callers who want to force Playwright)."""
        return self._discover_with_playwright(seed_url, max_depth)

    def download_pdf(self, pdf_url: str, out_dir: str) -> Optional[str]:
        # Reuse generic download; set last_download_meta accordingly
        return super().download_pdf(pdf_url, out_dir)
