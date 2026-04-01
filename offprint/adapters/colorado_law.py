from __future__ import annotations

import re
from typing import Iterable, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS
from .wordpress_academic_base import WordPressAcademicBaseAdapter


class ColoradoJTHTLAdapter(Adapter):
    """Adapter for Colorado Journal of Technology, Health & Technology Law.

    Handles the custom platform at www.jthtl.org
    URL pattern: http://www.jthtl.org/articles.php?volume=[1-12]

    Structure:
    - Volume pages with full issue content
    - Articles organized by issue sections
    - Direct PDF links within article blocks
    """

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a page."""
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20)
            if resp.status_code >= 400:
                return None
            return BeautifulSoup(resp.text, "lxml")
        except Exception:
            return None

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        """Discover PDFs from Colorado JTHTL volume pages."""
        soup = self._get_page(seed_url)
        if not soup:
            return

        # Extract volume number from URL
        volume_match = re.search(r"volume=(\d+)", seed_url)
        volume_number = volume_match.group(1) if volume_match else ""

        # Process full issue blocks
        for issue_block in soup.select("#main div:nth-of-type(n+2), div.issueFull"):
            yield from self._process_issue_block(issue_block, seed_url, volume_number)

    def _process_issue_block(
        self, block: BeautifulSoup, page_url: str, volume_number: str
    ) -> Iterable[DiscoveryResult]:
        """Process individual issue block containing articles."""
        try:
            # Extract issue metadata
            issue_metadata = self._extract_issue_metadata_from_block(block, volume_number)

            # Get all elements within the block
            titles = block.select("h6")
            authors = block.select("span.author")
            pdf_links = block.select("a:nth-of-type(n+2)")

            # Match titles, authors, and PDF links
            for i in range(min(len(titles), len(pdf_links))):
                try:
                    metadata = issue_metadata.copy()

                    # Title
                    if i < len(titles):
                        metadata["title"] = titles[i].get_text(strip=True)

                    # Authors
                    if i < len(authors):
                        author_text = authors[i].get_text(strip=True)
                        if author_text:
                            metadata["authors"] = [author_text]

                    # PDF link
                    if i < len(pdf_links):
                        href = pdf_links[i].get("href")
                        if href and self._is_likely_pdf_url(href):
                            pdf_url = urljoin(page_url, href)

                            # Generate citation
                            self._generate_citation(metadata)
                            metadata["url"] = page_url

                            yield DiscoveryResult(
                                page_url=page_url, pdf_url=pdf_url, metadata=metadata
                            )

                except Exception:
                    continue

        except Exception:
            return

    def _extract_issue_metadata_from_block(self, block: BeautifulSoup, volume_number: str) -> dict:
        """Extract issue-level metadata from issue block."""
        metadata = {}

        if volume_number:
            metadata["volume"] = volume_number

        # Look for issue header
        issue_header = block.select_one("h2")
        if issue_header:
            issue_text = issue_header.get_text(strip=True)
            metadata["issue_info"] = issue_text

            # Extract issue number
            issue_match = re.search(r"issue\s+(\d+)", issue_text, re.IGNORECASE)
            if issue_match:
                metadata["issue"] = issue_match.group(1)

            # Extract year
            year_match = re.search(r"\b(20\d{2})\b", issue_text)
            if year_match:
                metadata["date"] = year_match.group(1)

        return metadata

    def _generate_citation(self, metadata: dict) -> None:
        """Generate citation for the article."""
        if metadata.get("title"):
            citation_parts = []

            if metadata.get("authors"):
                if isinstance(metadata["authors"], list):
                    citation_parts.append(", ".join(metadata["authors"]))
                else:
                    citation_parts.append(str(metadata["authors"]))

            citation_parts.append(f'"{metadata["title"]}"')

            if metadata.get("volume"):
                vol_text = f"{metadata['volume']} Colo. J. Tech. & Health L."
                if metadata.get("issue"):
                    vol_text += f", Issue {metadata['issue']}"
                citation_parts.append(vol_text)

            if metadata.get("date"):
                citation_parts.append(f"({metadata['date']})")

            metadata["citation"] = " ".join(citation_parts)

    def _is_likely_pdf_url(self, url: str) -> bool:
        """Check if URL is likely a PDF."""
        url_lower = url.lower()
        return (
            url_lower.endswith(".pdf")
            or "/pdf" in url_lower
            or "download" in url_lower
            or ".pdf?" in url_lower
        )


class ColoradoCTLJAdapter(Adapter):
    """Adapter for Colorado Technology Law Journal.

    Handles the WordPress-based platform at ctlj.colorado.edu
    URL pattern: https://ctlj.colorado.edu/?paged=[1-6]&cat=9

    Structure:
    - Archive pages with article listings
    - Schema.org markup for articles
    - Individual article pages with PDF downloads
    """

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a page."""
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20)
            if resp.status_code >= 400:
                return None
            return BeautifulSoup(resp.text, "lxml")
        except Exception:
            return None

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        """Discover PDFs from Colorado CTLJ archive pages."""
        candidate_urls = self._candidate_archive_urls(seed_url)
        seen_pdfs: set[str] = set()
        yielded = False

        for candidate_url in candidate_urls:
            soup = self._get_page(candidate_url)
            if not soup:
                continue

            for article_element in soup.select("article"):
                for result in self._process_article_element(article_element, candidate_url):
                    if result.pdf_url in seen_pdfs:
                        continue
                    seen_pdfs.add(result.pdf_url)
                    yielded = True
                    yield result

        if yielded:
            return

        # Safety fallback for layout drift: reuse generic WordPress discovery.
        wp = WordPressAcademicBaseAdapter.from_url(seed_url, session=self.session)
        for result in wp.discover_pdfs(seed_url, max_depth=max(2, max_depth)):
            if result.pdf_url in seen_pdfs:
                continue
            seen_pdfs.add(result.pdf_url)
            yield result

    def _candidate_archive_urls(self, seed_url: str) -> list[str]:
        normalized = seed_url.strip()
        candidates = [normalized]
        if "cat=" not in normalized:
            candidates.extend(
                [
                    "https://ctlj.colorado.edu/?cat=49",  # Volume archive
                    "https://ctlj.colorado.edu/?cat=50",  # Issue archive
                    "https://ctlj.colorado.edu/?cat=9",  # Printed
                ]
            )

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        return deduped

    def _process_article_element(
        self, article_element: BeautifulSoup, page_url: str
    ) -> Iterable[DiscoveryResult]:
        """Process individual article element."""
        try:
            # Extract metadata from article element
            metadata = self._extract_metadata_from_article(article_element, page_url)

            # Get article page link
            article_link = article_element.select_one("[itemprop='headline'] a")
            if not article_link:
                return

            article_url = urljoin(page_url, article_link.get("href", ""))
            if not article_url:
                return

            # Visit article page to find PDF
            yield from self._process_article_page(article_url, metadata)

        except Exception:
            return

    def _extract_metadata_from_article(self, article_element: BeautifulSoup, page_url: str) -> dict:
        """Extract metadata from article element."""
        metadata = {}

        # Title
        title_element = article_element.select_one("[itemprop='headline'] a")
        if title_element:
            metadata["title"] = title_element.get_text(strip=True)

        # Issue
        issue_element = article_element.select_one(".cat-links a:nth-of-type(1)")
        if issue_element:
            metadata["issue_info"] = issue_element.get_text(strip=True)

        # Volume
        volume_element = article_element.select_one("a:nth-of-type(3)")
        if volume_element:
            volume_text = volume_element.get_text(strip=True)
            vol_match = re.search(r"(\d+)", volume_text)
            if vol_match:
                metadata["volume"] = vol_match.group(1)

        # Authors
        author_element = article_element.select_one("span[itemprop='name']")
        if author_element:
            author_text = author_element.get_text(strip=True)
            if author_text:
                metadata["authors"] = [author_text]

        metadata["source_url"] = page_url
        return metadata

    def _process_article_page(
        self, article_url: str, base_metadata: dict
    ) -> Iterable[DiscoveryResult]:
        """Process individual article page to find PDF."""
        soup = self._get_page(article_url)
        if not soup:
            return

        # Look for PDF links in article content
        for link in soup.select("p a"):
            href = link.get("href")
            if href and self._is_likely_pdf_url(href):
                pdf_url = urljoin(article_url, href)

                # Enhanced metadata
                enhanced_metadata = base_metadata.copy()
                self._enhance_metadata_from_page(soup, enhanced_metadata)
                enhanced_metadata["url"] = article_url

                yield DiscoveryResult(
                    page_url=article_url, pdf_url=pdf_url, metadata=enhanced_metadata
                )

    def _enhance_metadata_from_page(self, soup: BeautifulSoup, metadata: dict) -> None:
        """Enhance metadata from article page."""
        # Extract year from page content
        for elem in soup.select(".date, time, .published"):
            date_text = elem.get_text(strip=True)
            year_match = re.search(r"\b(20\d{2})\b", date_text)
            if year_match and not metadata.get("date"):
                metadata["date"] = year_match.group(1)
                break

        # Generate citation
        if metadata.get("title"):
            citation_parts = []

            if metadata.get("authors"):
                if isinstance(metadata["authors"], list):
                    citation_parts.append(", ".join(metadata["authors"]))
                else:
                    citation_parts.append(str(metadata["authors"]))

            citation_parts.append(f'"{metadata["title"]}"')

            if metadata.get("volume"):
                citation_parts.append(f"{metadata['volume']} Colo. Tech. L.J.")

            if metadata.get("date"):
                citation_parts.append(f"({metadata['date']})")

            metadata["citation"] = " ".join(citation_parts)

    def _is_likely_pdf_url(self, url: str) -> bool:
        """Check if URL is likely a PDF."""
        url_lower = url.lower()
        return (
            url_lower.endswith(".pdf")
            or "/pdf" in url_lower
            or "download" in url_lower
            or ".pdf?" in url_lower
        )
