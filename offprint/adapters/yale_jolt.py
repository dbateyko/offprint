from __future__ import annotations

import re
from typing import Iterable, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .jolt_base import JOLTBaseAdapter


class YaleJOLTAdapter(JOLTBaseAdapter):
    """Adapter for Yale Journal of Law & Technology (yjolt.org).

    Drupal-based platform with volume pages and Drupal views structure.
    """

    journal_short_cite = "Yale J.L. & Tech."
    pdf_link_selector = ".file a, a[href*='.pdf'], a[href*='/pdf/']"

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        soup = self._get_page(seed_url)
        if not soup:
            return

        target_pages: list[str] = []
        if re.search(r"/volume/\d+", seed_url):
            target_pages.append(seed_url)
        else:
            target_pages.extend(self._iter_volume_pages(soup, seed_url))

        if not target_pages:
            target_pages.append(seed_url)

        seen_article_urls: Set[str] = set()
        for page_url in target_pages:
            page_soup = soup if page_url == seed_url else self._get_page(page_url)
            if not page_soup:
                continue

            volume_match = re.search(r"/volume/(\d+)", page_url)
            volume_number = volume_match.group(1) if volume_match else ""

            rows = page_soup.select(".view-current-issue div.views-row")
            if not rows:
                rows = page_soup.select("div.views-row")

            for row in rows:
                for result in self._process_article_row(row, page_url, volume_number):
                    if result.page_url in seen_article_urls:
                        continue
                    seen_article_urls.add(result.page_url)
                    yield result

    def _iter_volume_pages(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        out: list[str] = []
        seen: Set[str] = set()
        for link in soup.select("a[href]"):
            href = link.get("href", "")
            absolute = urljoin(page_url, href)
            parsed = urlparse(absolute)
            if parsed.scheme not in {"http", "https"}:
                continue
            if re.search(r"/volume/\d+(?:$|[/?#])", parsed.path):
                normalized = absolute.rstrip("/")
                if normalized not in seen:
                    seen.add(normalized)
                    out.append(normalized)
        return out

    def _process_article_row(
        self, row: BeautifulSoup, page_url: str, volume_number: str
    ) -> Iterable[DiscoveryResult]:
        try:
            metadata = self._extract_row_metadata(row, page_url, volume_number)
            for link in row.select("a"):
                article_url = urljoin(page_url, link.get("href", ""))
                if article_url:
                    yield from self._process_article_page(article_url, metadata)
        except Exception:
            return

    def _extract_row_metadata(self, row: BeautifulSoup, page_url: str, volume_number: str) -> dict:
        metadata: dict = {}
        if volume_number:
            metadata["volume"] = volume_number

        strong = row.select_one("strong")
        if strong:
            metadata["authors"] = self._extract_authors_from_text(strong.get_text(strip=True))

        em = row.select_one("em")
        if em:
            cite_text = em.get_text(strip=True)
            metadata["citation"] = cite_text
            yr = self._extract_year(cite_text)
            if yr:
                metadata["date"] = yr

        metadata["source_url"] = page_url
        return metadata

    def _extract_metadata_from_article_page(
        self, soup: BeautifulSoup, article_url: str, base_metadata: dict
    ) -> dict:
        metadata = base_metadata.copy()

        for el in soup.select("h1, .article-title, title"):
            title = el.get_text(strip=True)
            title = re.sub(r"\s*\|\s*Yale.*$", "", title, flags=re.IGNORECASE)
            if title and len(title) > 10:
                metadata["title"] = title
                break

        if not metadata.get("authors"):
            author_els = soup.select(".author, .byline, [class*='author']")
            if author_els:
                authors = [e.get_text(strip=True) for e in author_els if e.get_text(strip=True)]
                if authors:
                    metadata["authors"] = authors

        if metadata.get("title") and metadata.get("volume"):
            self._generate_citation(metadata)

        metadata["url"] = article_url
        return metadata
