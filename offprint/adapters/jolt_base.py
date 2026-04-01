"""Shared base class for JOLT (Journal of Law & Technology) adapters.

Five JOLT adapters share ~70% identical logic (page fetching, PDF URL
detection, citation generation, metadata extraction, BFS discovery).  This
base extracts the common skeleton; site-specific adapters override selector
properties and hook methods.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


class JOLTBaseAdapter(Adapter):
    """Common base for Journal of Law & Technology adapters.

    Subclasses must set:
      - ``journal_short_cite``  (e.g. ``"Harv. J.L. & Tech."``)

    Subclasses may override:
      - ``volume_link_selector`` — CSS for volume links on index page
      - ``article_link_selector`` — CSS for article links on volume/issue pages
      - ``pdf_link_selector`` — CSS for PDF links on article pages
      - ``_extra_pdf_link_patterns`` — additional PDF link selectors
      - ``_discover_from_seed`` — full override of discovery entry-point
    """

    # --- Configuration (override in subclasses) ---

    journal_short_cite: str = "J.L. & Tech."

    volume_link_selector: str = "a[href*='/volume']"
    article_link_selector: str = "li a, p a"
    pdf_link_selector: str = "a[href*='.pdf'], a[href*='/pdf/'], .file a, .download a"

    # Additional PDF URL substrings beyond the standard set
    extra_pdf_url_markers: List[str] = []

    # UA profiles to rotate through when fetching pages (empty = single default)
    ua_profiles: List[str] = []

    # --- Shared helpers ---

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        profiles = self.ua_profiles or [DEFAULT_HEADERS.get("User-Agent", "")]
        for ua in profiles:
            headers = dict(DEFAULT_HEADERS)
            headers["User-Agent"] = ua
            try:
                resp = self.session.get(url, headers=headers, timeout=20)
            except Exception:
                continue
            if resp.status_code >= 400:
                continue
            return BeautifulSoup(resp.text, "lxml")
        return None

    def _is_likely_pdf_url(self, url: str) -> bool:
        url_lower = url.lower()
        if (
            url_lower.endswith(".pdf")
            or "/pdf" in url_lower
            or "download" in url_lower
            or ".pdf?" in url_lower
        ):
            return True
        return any(marker in url_lower for marker in self.extra_pdf_url_markers)

    def _check_pdf_content_type(self, url: str) -> bool:
        profiles = self.ua_profiles or [DEFAULT_HEADERS.get("User-Agent", "")]
        for ua in profiles:
            headers = dict(DEFAULT_HEADERS)
            headers["User-Agent"] = ua
            try:
                resp = self.session.head(url, headers=headers, timeout=10, allow_redirects=True)
            except Exception:
                continue
            if resp.status_code >= 400:
                continue
            if "application/pdf" in resp.headers.get("Content-Type", "").lower():
                return True
        return False

    def _generate_citation(self, metadata: dict) -> None:
        if not metadata.get("title"):
            return
        parts: List[str] = []
        authors = metadata.get("authors")
        if authors:
            if isinstance(authors, list):
                parts.append(", ".join(authors))
            else:
                parts.append(str(authors))
        parts.append(f'"{metadata["title"]}"')
        vol = metadata.get("volume")
        if vol:
            cite = f"{vol} {self.journal_short_cite}"
            issue = metadata.get("issue")
            if issue:
                cite += f", Issue {issue}"
            parts.append(cite)
        else:
            parts.append(self.journal_short_cite)
        year = metadata.get("date") or metadata.get("year")
        if year:
            parts.append(f"({year})")
        metadata["citation"] = " ".join(parts)

    def _extract_volume_number(self, text: str) -> str:
        m = re.search(r"volume\s+(\d+)", text, re.IGNORECASE)
        return m.group(1) if m else ""

    def _extract_issue_number(self, text: str) -> str:
        m = re.search(r"issue\s+(\d+)", text, re.IGNORECASE)
        return m.group(1) if m else ""

    def _extract_year(self, text: str) -> str:
        m = re.search(r"\b((?:19|20)\d{2})\b", text)
        return m.group(1) if m else ""

    def _extract_authors_from_text(self, text: str) -> List[str]:
        text = re.sub(r"^By\s+", "", text, flags=re.IGNORECASE).strip()
        if not text:
            return []
        if " and " in text or "," in text:
            return [a.strip() for a in re.split(r",\s*|\s+and\s+", text) if a.strip()]
        return [text]

    def _extract_metadata_from_article_page(
        self, soup: BeautifulSoup, article_url: str, base_metadata: dict
    ) -> dict:
        metadata = base_metadata.copy()

        # Title
        title_el = soup.select_one("h1, .article-title")
        if title_el:
            title = title_el.get_text(strip=True)
            title = re.sub(r"\s*[|–—]\s*.*$", "", title)
            if title and len(title) > len(metadata.get("title", "")):
                metadata["title"] = title

        # Authors
        if not metadata.get("authors"):
            author_els = soup.select(".author, .byline, [class*='author']")
            if author_els:
                authors = [e.get_text(strip=True) for e in author_els if e.get_text(strip=True)]
                if authors:
                    metadata["authors"] = authors

        # Date
        if not metadata.get("date"):
            for el in soup.select("[class*='date'], time, .published, .post-date"):
                yr = self._extract_year(el.get_text(strip=True))
                if yr:
                    metadata["date"] = yr
                    break

        if not metadata.get("date"):
            yr = self._extract_year(article_url)
            if yr:
                metadata["date"] = yr

        self._generate_citation(metadata)
        metadata["url"] = article_url
        return metadata

    def _find_pdf_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        seen: Set[str] = set()
        results: List[str] = []
        for link in soup.select(self.pdf_link_selector):
            href = link.get("href")
            if href and self._is_likely_pdf_url(href):
                pdf_url = urljoin(base_url, href)
                if pdf_url not in seen:
                    seen.add(pdf_url)
                    results.append(pdf_url)
        return results

    def _process_article_page(
        self, article_url: str, base_metadata: dict
    ) -> Iterable[DiscoveryResult]:
        soup = self._get_page(article_url)
        if not soup:
            return
        metadata = self._extract_metadata_from_article_page(soup, article_url, base_metadata)
        for pdf_url in self._find_pdf_links(soup, article_url):
            yield DiscoveryResult(page_url=article_url, pdf_url=pdf_url, metadata=metadata)

    # --- Discovery (subclasses may override) ---

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        soup = self._get_page(seed_url)
        if not soup:
            return
        yield from self._discover_from_seed(soup, seed_url, max_depth)

    def _discover_from_seed(
        self, soup: BeautifulSoup, seed_url: str, max_depth: int
    ) -> Iterable[DiscoveryResult]:
        volume_links = {
            urljoin(seed_url, a.get("href", ""))
            for a in soup.select(self.volume_link_selector)
            if a.get("href")
        }
        if volume_links:
            for vol_url in sorted(volume_links):
                yield from self._process_volume_page(vol_url)
        else:
            yield from self._process_volume_page(seed_url, soup)

    def _process_volume_page(
        self, volume_url: str, soup: Optional[BeautifulSoup] = None
    ) -> Iterable[DiscoveryResult]:
        if soup is None:
            soup = self._get_page(volume_url)
        if not soup:
            return

        vol_text = ""
        vol_el = soup.select_one("h1, h2, title")
        if vol_el:
            vol_text = vol_el.get_text(strip=True)

        volume = self._extract_volume_number(vol_text)
        issue = self._extract_issue_number(vol_text)
        year = self._extract_year(vol_text)

        for link in soup.select(self.article_link_selector):
            href = link.get("href")
            if not href:
                continue
            article_url = urljoin(volume_url, href)
            metadata = {"source_url": volume_url}
            if volume:
                metadata["volume"] = volume
            if issue:
                metadata["issue"] = issue
            if year:
                metadata["date"] = year

            link_text = link.get_text(strip=True)
            if link_text:
                metadata["title"] = link_text

            if self._is_likely_pdf_url(article_url):
                self._generate_citation(metadata)
                metadata["url"] = article_url
                yield DiscoveryResult(page_url=volume_url, pdf_url=article_url, metadata=metadata)
            else:
                yield from self._process_article_page(article_url, metadata)
