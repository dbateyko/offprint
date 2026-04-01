from __future__ import annotations

import re
from typing import Iterable, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .digital_commons_issue_article_hop import DigitalCommonsIssueArticleHopAdapter
from .generic import DEFAULT_HEADERS


class DigitalCommonsAdapter(DigitalCommonsIssueArticleHopAdapter):
    """Adapter for DigitalCommons/bepress repositories.

    Handles law reviews hosted on platforms like:
    - digitalcommons.law.umaryland.edu (Maryland JBTL)
    - digitalcommons.law.uw.edu (Washington WJLTA)
    - scholarship.law.duke.edu (Duke LTR)
    - scholarlycommons.law.northwestern.edu (Northwestern JTIP)
    - digitalcommons.law.scu.edu (Santa Clara HTLJ)
    - repository.uclawsf.edu (UC Hastings STLJ)
    - scholarship.law.ufl.edu (Florida JTLP)
    - scholarship.law.vanderbilt.edu (Vanderbilt JETLAW)
    - scholarship.law.edu (Catholic University JLT)

    Common structure:
    - All issues page: /all_issues.html
    - Issue pages with articles: div.doc elements
    - PDF links: .pdf a elements
    - Metadata in structured format
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
        yield from super().discover_pdfs(seed_url, max_depth=max_depth)

    def _process_issue_page(self, issue_url: str) -> Iterable[DiscoveryResult]:
        """Process an individual issue page to extract articles and PDFs."""
        soup = self._get_page(issue_url)
        if not soup:
            return

        # Extract issue metadata
        issue_title = ""
        issue_element = soup.select_one("#alpha h1")
        if issue_element:
            issue_title = issue_element.get_text(strip=True)

        # Process each article
        for article_element in soup.select("div.doc"):
            try:
                metadata = self._extract_metadata_from_article(
                    article_element, issue_url, issue_title
                )

                # Find PDF link
                pdf_link = article_element.select_one(".pdf a")
                if pdf_link:
                    pdf_url = urljoin(issue_url, pdf_link.get("href", ""))
                    if pdf_url:
                        if "viewcontent.cgi" in pdf_url and "type=pdf" not in pdf_url.lower():
                            separator = "&" if "?" in pdf_url else "?"
                            pdf_url = f"{pdf_url}{separator}type=pdf"
                        yield DiscoveryResult(
                            page_url=issue_url, pdf_url=pdf_url, metadata=metadata
                        )
            except Exception:
                # Skip malformed articles but continue processing
                continue

    def _extract_metadata_from_article(
        self, article_soup: BeautifulSoup, issue_url: str, issue_title: str
    ) -> dict:
        """Extract comprehensive metadata from article element."""
        metadata = {}

        # Title from article link
        title_element = article_soup.select_one("p:nth-of-type(2) a, a.doctitle")
        if title_element:
            metadata["title"] = title_element.get_text(strip=True)

        # Authors
        author_element = article_soup.select_one("span")
        if author_element:
            author_text = author_element.get_text(strip=True)
            # Split multiple authors if separated by " and "
            if " and " in author_text:
                authors = re.split(r"\s+and\s+", author_text)
                metadata["authors"] = [author.strip() for author in authors if author.strip()]
            else:
                metadata["authors"] = [author_text]

        # Date from various possible locations
        date_element = article_soup.select_one("p.index_date, .date")
        if date_element:
            metadata["date"] = date_element.get_text(strip=True)

        # Parse volume and issue from issue title
        if issue_title:
            # Extract volume/issue numbers from titles like "Volume 10, Issue 2 (2023)"
            volume_match = re.search(r"vol(?:ume)?\.?\s+(\d+)", issue_title, re.IGNORECASE)
            if volume_match:
                metadata["volume"] = volume_match.group(1)

            issue_match = re.search(r"(?:issue|no)\.?\s+(\d+)", issue_title, re.IGNORECASE)
            if issue_match:
                metadata["issue"] = issue_match.group(1)

            # Extract year
            year_match = re.search(r"\((\d{4})\)", issue_title)
            if year_match and not metadata.get("date"):
                metadata["date"] = year_match.group(1)

        # URL to article page
        article_link = article_soup.select_one("p:nth-of-type(2) a, a.doctitle")
        if article_link:
            metadata["url"] = urljoin(issue_url, article_link.get("href", ""))

        # Generate citation if we have enough info
        if metadata.get("title") and metadata.get("authors"):
            citation_parts = []
            if metadata.get("authors"):
                if isinstance(metadata["authors"], list):
                    citation_parts.append(", ".join(metadata["authors"]))
                else:
                    citation_parts.append(str(metadata["authors"]))

            citation_parts.append(f'"{metadata["title"]}"')

            if metadata.get("volume"):
                vol_text = f"Vol. {metadata['volume']}"
                if metadata.get("issue"):
                    vol_text += f", No. {metadata['issue']}"
                citation_parts.append(vol_text)

            if metadata.get("date"):
                citation_parts.append(f"({metadata['date']})")

            metadata["citation"] = ", ".join(citation_parts)

        metadata["source_url"] = issue_url
        return metadata

    def download_pdf(self, pdf_url: str, out_dir: str, **kwargs) -> Optional[str]:
        return super().download_pdf(pdf_url, out_dir, **kwargs)
