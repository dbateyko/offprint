from __future__ import annotations

from typing import Iterable, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .jolt_base import JOLTBaseAdapter


class HarvardJOLTAdapter(JOLTBaseAdapter):
    """Adapter for Harvard Journal of Law & Technology (jolt.law.harvard.edu)."""

    journal_short_cite = "Harv. J.L. & Tech."
    volume_link_selector = "a[href*='/volumes/volume-']"
    article_link_selector = "li a, p a"
    pdf_link_selector = "a[href*='.pdf'], a[href*='/pdf/'], .file a, .download a"

    def _process_volume_page(
        self, volume_url: str, soup: Optional[BeautifulSoup] = None
    ) -> Iterable[DiscoveryResult]:
        if soup is None:
            soup = self._get_page(volume_url)
        if not soup:
            return

        for section in soup.select(".volume--fall section:nth-of-type(1), section.volume, article"):
            vol_el = section.select_one("h2")
            vol_text = vol_el.get_text(strip=True) if vol_el else ""

            for link in section.select(self.article_link_selector):
                href = link.get("href")
                if not href:
                    continue
                article_url = urljoin(volume_url, href)

                metadata = self._metadata_from_link(link, vol_text, volume_url)

                if article_url.lower().endswith(".pdf"):
                    yield DiscoveryResult(
                        page_url=volume_url, pdf_url=article_url, metadata=metadata
                    )
                else:
                    yield from self._process_article_page(article_url, metadata)

    def _metadata_from_link(self, link_el: BeautifulSoup, vol_text: str, page_url: str) -> dict:
        metadata: dict = {}
        metadata["title"] = link_el.get_text(strip=True)

        volume = self._extract_volume_number(vol_text)
        if volume:
            metadata["volume"] = volume

        if "fall" in vol_text.lower():
            metadata["issue"] = "Fall"
        elif "spring" in vol_text.lower():
            metadata["issue"] = "Spring"

        year = self._extract_year(vol_text)
        if year:
            metadata["date"] = year

        metadata["url"] = urljoin(page_url, link_el.get("href", ""))
        metadata["source_url"] = page_url
        return metadata

    def _extract_metadata_from_article_page(
        self, soup: BeautifulSoup, article_url: str, base_metadata: dict
    ) -> dict:
        metadata = super()._extract_metadata_from_article_page(soup, article_url, base_metadata)
        # Harvard-specific citation format
        if metadata.get("title") and metadata.get("volume"):
            self._generate_citation(metadata)
        metadata["url"] = article_url
        return metadata
