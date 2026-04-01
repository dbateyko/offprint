from __future__ import annotations

from typing import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .generic import DISCOVERY_UA_PROFILES
from .jolt_base import JOLTBaseAdapter


class NorthCarolinaJOLTAdapter(JOLTBaseAdapter):
    """Adapter for North Carolina Journal of Law & Technology (ncjolt.org)."""

    journal_short_cite = "N.C. J.L. & Tech."
    extra_pdf_url_markers = ["wp-content/uploads/"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ua_profiles = list(DISCOVERY_UA_PROFILES)

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        soup = self._get_page(seed_url)
        if not soup:
            return

        articles = soup.select("article")
        if not articles:
            # Legacy NCJOLT archives expose direct PDF links on static pages.
            seen = set()
            for link in soup.select("a[href]"):
                href = link.get("href") or ""
                if not self._is_likely_pdf_url(href):
                    continue
                pdf_url = urljoin(seed_url, href)
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                yield DiscoveryResult(
                    page_url=seed_url,
                    pdf_url=pdf_url,
                    metadata={"source_url": seed_url, "url": seed_url},
                )
            return

        for article_el in articles:
            title_link = article_el.select_one(".title a")
            if not title_link:
                continue
            article_url = urljoin(seed_url, title_link.get("href", ""))
            if article_url:
                yield from self._process_article_page(article_url, {"source_url": seed_url})

    def _extract_metadata_from_article_page(
        self, soup: BeautifulSoup, article_url: str, base_metadata: dict
    ) -> dict:
        metadata = base_metadata.copy()

        # Title
        h1 = soup.select_one("h1")
        if h1:
            metadata["title"] = h1.get_text(strip=True)
        elif soup.title:
            import re

            title = soup.title.get_text(strip=True)
            title = re.sub(r"\s*–\s*NC\s*JOLT.*$", "", title, flags=re.IGNORECASE)
            metadata["title"] = title

        # Authors from specific paragraph
        author_el = soup.select_one("p:nth-of-type(3) strong")
        if author_el:
            metadata["authors"] = self._extract_authors_from_text(author_el.get_text(strip=True))

        # Volume/issue from specific paragraph
        vol_el = soup.select_one("p:nth-of-type(5) strong")
        if vol_el:
            vol_text = vol_el.get_text(strip=True)
            v = self._extract_volume_number(vol_text)
            if v:
                metadata["volume"] = v
            i = self._extract_issue_number(vol_text)
            if i:
                metadata["issue"] = i

        # Date
        if not metadata.get("date"):
            for el in soup.select(".date, .post-date, time, .published"):
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

    def _find_pdf_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        pdf_links = []
        for paragraph in soup.select("p"):
            for link in paragraph.select("a[href]"):
                href = link.get("href")
                if href and self._is_likely_pdf_url(href):
                    pdf_links.append(urljoin(base_url, href))

            import re

            text = paragraph.get_text()
            if re.search(r"(download|pdf|available\s+here)", text, re.IGNORECASE):
                for link in paragraph.select("a[href]"):
                    href = link.get("href")
                    if href:
                        pdf_url = urljoin(base_url, href)
                        if self._check_pdf_content_type(pdf_url):
                            pdf_links.append(pdf_url)
        return list(set(pdf_links))
