from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .jolt_base import JOLTBaseAdapter
from .wordpress_academic_base import WordPressAcademicBaseAdapter


class RichmondJOLTAdapter(JOLTBaseAdapter):
    """Adapter for Richmond Journal of Law & Technology (jolt.richmond.edu).

    WordPress-based with past-issues page linking to individual issues.
    Falls back to WordPressAcademicBaseAdapter when structure is not detected.
    """

    journal_short_cite = "Rich. J.L. & Tech."

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        soup = self._get_page(seed_url)
        if not soup:
            wp = WordPressAcademicBaseAdapter(session=self.session)
            yield from wp.discover_pdfs(seed_url, max_depth=max(2, max_depth))
            return

        issue_links = soup.select(
            "li > strong a, .post-content li > a, span strong a, li:nth-of-type(n+2) span > a"
        )

        yielded = False
        for link in issue_links:
            issue_url = urljoin(seed_url, link.get("href", ""))
            if not issue_url:
                continue
            yielded = True
            yield from self._process_issue_page(issue_url)

        if not yielded:
            wp = WordPressAcademicBaseAdapter(session=self.session)
            yield from wp.discover_pdfs(seed_url, max_depth=max(2, max_depth))

    def _process_issue_page(self, issue_url: str) -> Iterable[DiscoveryResult]:
        soup = self._get_page(issue_url)
        if not soup:
            return

        issue_meta = self._extract_issue_metadata(soup, issue_url)

        # Structure 1: entry-content paragraphs
        for p in soup.select(".entry-content p:nth-of-type(n+2)"):
            yield from self._process_paragraph(p, issue_url, issue_meta)

        # Structure 2: table cells
        for cell in soup.select("p tr:nth-of-type(1) td[valign], tr:nth-of-type(n+2) [valign] p"):
            yield from self._process_cell(cell, issue_url, issue_meta)

    def _extract_issue_metadata(self, soup: BeautifulSoup, issue_url: str) -> dict:
        metadata: dict = {}
        title_text = ""
        if soup.title:
            title_text = soup.title.get_text(strip=True)
        for header in soup.select("h1, h2, h3"):
            ht = header.get_text(strip=True)
            if len(ht) > len(title_text):
                title_text = ht

        v = self._extract_volume_number(title_text)
        if v:
            metadata["volume"] = v
        i = self._extract_issue_number(title_text)
        if i:
            metadata["issue"] = i
        yr = self._extract_year(title_text)
        if yr:
            metadata["year"] = yr
        metadata["source_url"] = issue_url
        return metadata

    def _process_paragraph(
        self, paragraph: BeautifulSoup, issue_url: str, issue_meta: dict
    ) -> Iterable[DiscoveryResult]:
        try:
            for link in paragraph.select("a"):
                href = link.get("href")
                if href and self._is_likely_pdf_url(href):
                    pdf_url = urljoin(issue_url, href)
                    metadata = self._metadata_from_text(
                        paragraph.get_text(strip=True), issue_url, issue_meta
                    )
                    yield DiscoveryResult(page_url=issue_url, pdf_url=pdf_url, metadata=metadata)
        except Exception:
            return

    def _process_cell(
        self, cell: BeautifulSoup, issue_url: str, issue_meta: dict
    ) -> Iterable[DiscoveryResult]:
        try:
            for link in cell.select("font a, a"):
                href = link.get("href")
                if href and self._is_likely_pdf_url(href):
                    pdf_url = urljoin(issue_url, href)
                    metadata = self._metadata_from_text(
                        cell.get_text(strip=True), issue_url, issue_meta
                    )
                    yield DiscoveryResult(page_url=issue_url, pdf_url=pdf_url, metadata=metadata)
        except Exception:
            return

    def _metadata_from_text(self, text: str, page_url: str, issue_meta: dict) -> dict:
        metadata = issue_meta.copy()
        if " by " in text:
            parts = text.split(" by ", 1)
            metadata["title"] = parts[0].strip()
            author = re.sub(r"\s*\(PDF\)\s*$", "", parts[1].strip(), flags=re.IGNORECASE)
            metadata["authors"] = [author]
        else:
            metadata["title"] = re.sub(r"\s*\(PDF\)\s*$", "", text, flags=re.IGNORECASE)
        self._generate_citation(metadata)
        metadata["url"] = page_url
        return metadata
