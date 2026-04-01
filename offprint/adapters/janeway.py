from __future__ import annotations

import re
from typing import Dict, Iterable, List, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


class JanewayAdapter(Adapter):
    """Adapter for Janeway-based sites, e.g., journals.library.wustl.edu.

    Janeway is an open-source scholarly publishing platform.
    It typically has /issue/archive/ for the listing and
    citation_* meta tags on article pages.
    """

    def __init__(self, session=None):
        super().__init__(session=session)
        self._seen_pages: Set[str] = set()
        self._seen_pdfs: Set[str] = set()

    def discover_pdfs(self, seed_url: str, max_depth: int = 1) -> Iterable[DiscoveryResult]:
        """Discover PDFs from Janeway site traversal."""
        # Janeway archive is usually at /issue/archive/
        archive_url = seed_url.rstrip("/")
        if not archive_url.endswith("/issue/archive"):
            archive_url = urljoin(seed_url, "issue/archive/")

        queue: List[tuple[str, int]] = [(archive_url, 0)]
        host = urlparse(seed_url).netloc

        while queue:
            url, depth = queue.pop(0)
            if url in self._seen_pages:
                continue
            self._seen_pages.add(url)

            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20)
            if not resp or resp.status_code >= 400:
                continue
            soup = BeautifulSoup(resp.text, "lxml")

            # 1. Extract PDFs from article pages
            if "/article/" in url.lower():
                for result in self._extract_from_article_page(soup, url):
                    yield result
                continue

            # 2. Look for article links or issue links
            for a in soup.select("a[href]"):
                href = a["href"]
                absolute = urljoin(url, href)
                if urlparse(absolute).netloc != host:
                    continue
                
                lowered = absolute.lower()
                # Follow article detail pages or more archive pages
                if "/article/" in lowered or "/issue/" in lowered:
                    if absolute not in self._seen_pages and depth < max_depth + 1:
                        queue.append((absolute, depth + 1))

    def _extract_from_article_page(self, soup: BeautifulSoup, page_url: str) -> Iterable[DiscoveryResult]:
        """Extract PDF and metadata from a Janeway article page."""
        # Janeway uses HighWire Press / Dublin Core tags
        metadata = self._extract_metadata(soup, page_url)
        
        # Look for PDF links
        # 1. citation_pdf_url
        pdf_tag = soup.find("meta", attrs={"name": "citation_pdf_url"})
        if pdf_tag and pdf_tag.get("content"):
            pdf_url = urljoin(page_url, pdf_tag["content"])
            if pdf_url not in self._seen_pdfs:
                self._seen_pdfs.add(pdf_url)
                yield DiscoveryResult(
                    page_url=page_url,
                    pdf_url=pdf_url,
                    metadata=metadata,
                    source_adapter="janeway",
                    extraction_path="janeway_article",
                )
                return

        # 2. Look for "Download PDF" or similar
        for a in soup.select("a[href]"):
            text = a.get_text(" ", strip=True).lower()
            href = a["href"]
            if "pdf" in text and ("download" in text or "view" in text):
                pdf_url = urljoin(page_url, href)
                if pdf_url not in self._seen_pdfs:
                    self._seen_pdfs.add(pdf_url)
                    yield DiscoveryResult(
                        page_url=page_url,
                        pdf_url=pdf_url,
                        metadata=metadata,
                        source_adapter="janeway",
                        extraction_path="janeway_link",
                    )
                    return

    def _extract_metadata(self, soup: BeautifulSoup, page_url: str) -> Dict:
        metadata = {"source_url": page_url, "url": page_url, "platform": "Janeway"}
        
        # Standard citation tags
        tags = {
            "citation_title": "title",
            "citation_author": "authors",
            "citation_publication_date": "date",
            "citation_volume": "volume",
            "citation_issue": "issue",
            "citation_firstpage": "first_page",
            "citation_lastpage": "last_page",
            "citation_doi": "doi",
        }
        
        for name, key in tags.items():
            found = soup.find_all("meta", attrs={"name": name})
            if not found:
                continue
            values = [f.get("content", "").strip() for f in found if f.get("content")]
            if not values:
                continue
            if key == "authors":
                metadata[key] = values
            else:
                metadata[key] = values[0]
                if key == "date":
                    match = re.search(r"\b(19|20)\d{2}\b", values[0])
                    if match:
                        metadata["year"] = match.group(0)

        if not metadata.get("title"):
            h1 = soup.find("h1")
            if h1:
                metadata["title"] = h1.get_text(strip=True)

        return metadata
