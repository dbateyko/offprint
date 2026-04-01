from __future__ import annotations

import os
import re
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


class DrupalAdapter(Adapter):
    """Adapter for Drupal-based sites, specifically UC Davis SiteFarm.

    Approach:
    - Uses Playwright fallback for discovery to bypass WAF/403 on archives.
    - Discovers article links from /archives pages.
    - Extracts PDF links from article detail pages.
    """

    def __init__(self, session=None):
        super().__init__(session=session)
        self._seen_pages: Set[str] = set()
        self._seen_pdfs: Set[str] = set()
        value = str(os.getenv("LRS_ADAPTER_PLAYWRIGHT_FALLBACK", "1")).strip().lower()
        self.enable_playwright_fallback = value not in {"0", "false", "no", "off"}

    @staticmethod
    def _looks_like_pdf_href(href: str, anchor_text: str = "") -> bool:
        lowered_href = href.lower()
        lowered_text = anchor_text.lower()
        return (
            lowered_href.endswith(".pdf")
            or ".pdf?" in lowered_href
            or "/sites/default/files/" in lowered_href
            or (("download" in lowered_text) and ("/files/" in lowered_href))
        )

    @staticmethod
    def _is_likely_journal_pdf_url(pdf_url: str) -> bool:
        lowered = (pdf_url or "").lower()
        # Drop obvious site-policy/admin PDFs that frequently appear in footers.
        blocked = (
            "non-discrimination",
            "nondiscrimination",
            "policy",
            "privacy",
            "accessibility",
            "notice",
            "handbook",
            "student-code",
            "terms-of-use",
        )
        return not any(token in lowered for token in blocked)

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch page with requests first, then optional Playwright fallback."""
        response_status = 0
        # Prefer requests first to keep high-concurrency backlog runs fast.
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20)
            if resp is not None:
                response_status = int(resp.status_code or 0)
                if response_status < 400:
                    return BeautifulSoup(resp.text, "lxml")
        except Exception:
            response_status = 0

        if not self.enable_playwright_fallback:
            return None

        # Use Playwright only for blocked/errored responses.
        try:
            if response_status not in {0, 401, 403, 429, 500, 502, 503, 504}:
                return None
            from ..playwright_session import PlaywrightSession
            with PlaywrightSession(headless=True) as pw:
                resp = pw.get(url, timeout=30)
                if resp and resp.status_code < 400:
                    return BeautifulSoup(resp.text, "lxml")
        except Exception:
            pass
        return None

    def discover_pdfs(self, seed_url: str, max_depth: int = 1) -> Iterable[DiscoveryResult]:
        """Discover PDFs from Drupal site traversal."""
        # Ensure each discovery run starts fresh, even if adapter instance is reused.
        self._seen_pages = set()
        self._seen_pdfs = set()

        effective_max_depth = max_depth
        lowered_seed = (seed_url or "").lower()
        # Archive seeds often require two hops:
        # archive -> volume/issue -> article/news with PDF button.
        if max_depth <= 0 and any(
            token in lowered_seed for token in ("/print-edition-archive", "/archive", "/issues")
        ):
            effective_max_depth = 2
        archive_mode = any(
            token in lowered_seed for token in ("/print-edition-archive", "/archive", "/issues")
        )

        queue: List[tuple[str, int]] = [(seed_url, 0)]
        parsed_seed = urlparse(seed_url)
        host = parsed_seed.netloc
        root_url = f"{parsed_seed.scheme or 'https'}://{host}/" if host else seed_url
        tried_root_fallback = False

        yielded_any = False
        while queue:
            url, depth = queue.pop(0)
            if url in self._seen_pages:
                continue
            self._seen_pages.add(url)

            soup = self._get(url)
            if not soup:
                if depth == 0 and not tried_root_fallback and root_url not in self._seen_pages:
                    tried_root_fallback = True
                    queue.append((root_url, 0))
                continue

            # 1. Look for direct PDF links on this page
            # In archive mode, prefer article/news detail pages to avoid issue-level
            # statements/mastheads being selected before article PDFs.
            page_lower = url.lower()
            is_article_context = any(
                token in page_lower for token in ("/news/", "/article/", "/articles/")
            )
            if (not archive_mode) or is_article_context:
                for a in soup.select("a[href]"):
                    href = (a.get("href") or "").strip()
                    if not href:
                        continue
                    if not self._looks_like_pdf_href(href, a.get_text(" ", strip=True)):
                        continue
                    pdf_url = urljoin(url, href)
                    if not self._is_likely_journal_pdf_url(pdf_url):
                        continue
                    if pdf_url not in self._seen_pdfs:
                        self._seen_pdfs.add(pdf_url)
                        metadata = self._extract_metadata(soup, url, a)
                        yielded_any = True
                        yield DiscoveryResult(
                            page_url=url,
                            pdf_url=pdf_url,
                            metadata=metadata,
                            source_adapter="drupal",
                            extraction_path="direct_link",
                        )

            # 2. Look for article/issue links to follow
            if depth < effective_max_depth:
                for a in soup.select("a[href]"):
                    href = a.get("href") or ""
                    if not href or href.startswith("#") or "javascript:" in href.lower():
                        continue
                    
                    absolute = urljoin(url, href)
                    if urlparse(absolute).netloc != host:
                        continue
                    
                    # Heuristics for article or issue pages in Drupal/SiteFarm
                    lowered = absolute.lower()
                    text = a.get_text(" ", strip=True).lower()
                    
                    is_article = (
                        "/article/" in lowered
                        or "/articles/" in lowered
                        or "/news/" in lowered
                        or "/print-archive/" in lowered
                        or "/print/" in lowered
                    )
                    is_archive = (
                        "/archives" in lowered
                        or "/archive" in lowered
                        or "/print-edition-archive" in lowered
                        or "/print/" in lowered
                        or "/volume-" in lowered
                        or "-issue-" in lowered
                        or "volume" in text
                        or "issue" in text
                    )
                    
                    if is_article or is_archive:
                        if absolute not in self._seen_pages:
                            queue.append((absolute, depth + 1))

        if not yielded_any:
            # Fallback to GenericAdapter if standard Drupal traversal found nothing.
            from .generic import GenericAdapter
            generic = GenericAdapter(session=self.session)
            # Use a slightly deeper crawl for the generic fallback
            for result in generic.discover_pdfs(seed_url, max_depth=max(1, effective_max_depth)):
                if result.pdf_url not in self._seen_pdfs:
                    self._seen_pdfs.add(result.pdf_url)
                    yield result

    def _extract_metadata(self, soup: BeautifulSoup, page_url: str, anchor: BeautifulSoup) -> Dict:
        """Extract metadata from page context."""
        metadata = {
            "source_url": page_url,
            "url": page_url,
            "platform": "Drupal/SiteFarm",
        }

        # Try to find title from page h1 or anchor text
        title = soup.select_one("h1.page-title, h1.headline, main h1, article h1") or soup.find("h1")
        if title:
            metadata["title"] = title.get_text(strip=True)
        else:
            metadata["title"] = anchor.get_text(strip=True)

        # Look for authors in typical Drupal locations
        author_elem = soup.select_one(
            ".field--name-field-authors, .author, [itemprop='author'], .article-feed-author"
        )
        if author_elem:
            authors_text = author_elem.get_text(strip=True)
            # Split common author separators
            metadata["authors"] = [a.strip() for a in re.split(r",|&|and", authors_text) if a.strip()]

        # Look for date
        date_elem = soup.select_one(
            ".field--name-post-date, .date, [itemprop='datePublished'], .article-created-date"
        )
        if date_elem:
            raw_date = date_elem.get_text(" ", strip=True)
            metadata["date"] = re.sub(r"^[Pp]ublished:\s*", "", raw_date).strip()

        # Volume/Issue from URL or page text
        vol_match = re.search(r"volume[- ]?(\d+)", page_url + soup.get_text(), re.I)
        if vol_match:
            metadata["volume"] = vol_match.group(1)
            
        iss_match = re.search(r"issue[- ]?(\d+)", page_url + soup.get_text(), re.I)
        if iss_match:
            metadata["issue"] = iss_match.group(1)

        return metadata
