from __future__ import annotations

import re
from typing import Iterable, Optional, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS
from .wordpress_academic_base import WordPressAcademicBaseAdapter


class GeorgetownGLTRAdapter(Adapter):
    """Georgetown Law Technology Review adapter.

    Handles WordPress-based journal with two content types:
    1. Main articles organized by volume/issue
    2. Technology explainers with separate pagination

    URLs:
    - Issues: https://georgetownlawtechreview.org/issues/
    - Explainers: https://georgetownlawtechreview.org/technology-explainers/page/[1-4]
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
        """Discover PDFs from Georgetown GLTR pages."""
        yielded = False

        def emit(stream: Iterable[DiscoveryResult]) -> Iterable[DiscoveryResult]:
            nonlocal yielded
            for row in stream:
                yielded = True
                yield row

        if "technology-explainers" in seed_url:
            yield from emit(self._discover_explainers(seed_url))
        elif "issues" in seed_url:
            yield from emit(self._discover_main_articles(seed_url))
        else:
            # Try generic discovery as fallback
            yield from emit(self._discover_generic(seed_url))

        # Fallback to WordPress base logic if custom patterns missed current theme structure.
        if not yielded:
            wp = WordPressAcademicBaseAdapter(session=self.session)
            yield from wp.discover_pdfs(seed_url, max_depth=max(2, max_depth))

    def _discover_main_articles(self, seed_url: str) -> Iterable[DiscoveryResult]:
        """Discover PDFs from main issues archive."""
        soup = self._get_page(seed_url)
        if not soup:
            return

        # Strategy 1: Look for volume/issue structure
        # Georgetown uses direct linking rather than CSS classes from sitemap
        volume_links = soup.find_all("a", href=re.compile(r"/issues/volume-\d+"))

        seen_urls: Set[str] = set()

        for volume_link in volume_links:
            volume_url = urljoin(seed_url, volume_link["href"])
            if volume_url in seen_urls:
                continue
            seen_urls.add(volume_url)

            if self._looks_like_issue_url(volume_url):
                yield from self._process_issue_page(volume_url)
            else:
                yield from self._process_volume_page(volume_url)

    def _looks_like_issue_url(self, url: str) -> bool:
        path = (urlparse(url).path or "").lower()
        if "/issues/volume-" not in path:
            return False
        return any(token in path for token in ["/issue-", "/special-issue-", "/voll-"])

    def _process_volume_page(self, volume_url: str) -> Iterable[DiscoveryResult]:
        """Process individual volume page to find issue pages and articles."""
        soup = self._get_page(volume_url)
        if not soup:
            return

        # Look for issue links within the volume
        issue_links = soup.find_all("a", href=re.compile(r"/issues/volume-\d+/issue-\d+"))

        for issue_link in issue_links:
            issue_url = urljoin(volume_url, issue_link["href"])
            yield from self._process_issue_page(issue_url)

    def _process_issue_page(self, issue_url: str) -> Iterable[DiscoveryResult]:
        """Process individual issue page to find articles and PDFs."""
        soup = self._get_page(issue_url)
        if not soup:
            return

        issue_metadata = self._extract_issue_metadata(soup, issue_url)

        # Many GLTR issues publish a full-issue PDF directly on the issue page.
        for direct_pdf in soup.find_all("a", href=re.compile(r"\.pdf($|[?#])", re.I)):
            pdf_url = urljoin(issue_url, direct_pdf.get("href", ""))
            if pdf_url:
                metadata = dict(issue_metadata)
                yield DiscoveryResult(page_url=issue_url, pdf_url=pdf_url, metadata=metadata)

        # Look for article title links that lead to individual article pages
        article_links = soup.find_all(
            "a", href=re.compile(r"^(?!.*/(issues|technology-explainers)/).*$")
        )

        for article_link in article_links:
            # Skip navigation links, only follow article links
            href = article_link.get("href", "")
            if not href or any(
                skip in href for skip in ["/issues/", "/page/", "#", "mailto:", "tel:"]
            ):
                continue

            article_url = urljoin(issue_url, href)
            yield from self._extract_pdf_from_article(
                article_url, issue_url, issue_metadata=issue_metadata
            )

    def _discover_explainers(self, seed_url: str) -> Iterable[DiscoveryResult]:
        """Discover PDFs from technology explainers section."""
        soup = self._get_page(seed_url)
        if not soup:
            return

        # Look for article links in WordPress block templates or post listings
        selectors = [
            ".wp-block-post-template a",
            ".post-teaser--title a",
            ".entry-title a",
            "article a",
        ]

        article_links = []
        for selector in selectors:
            links = soup.select(selector)
            if links:
                article_links = links
                break

        for article_link in article_links:
            href = article_link.get("href", "")
            if not href or any(skip in href for skip in ["/page/", "#", "mailto:", "tel:"]):
                continue

            article_url = urljoin(seed_url, href)
            yield from self._extract_pdf_from_article(article_url, seed_url)

    def _extract_pdf_from_article(
        self,
        article_url: str,
        source_page: str,
        issue_metadata: Optional[dict] = None,
    ) -> Iterable[DiscoveryResult]:
        """Extract PDF from individual article page."""
        soup = self._get_page(article_url)
        if not soup:
            return

        # Extract metadata according to sitemap specification
        metadata = self._extract_metadata_from_article(soup, article_url)
        if issue_metadata:
            for key, value in issue_metadata.items():
                metadata.setdefault(key, value)

        # Strategy 1: Look for direct PDF links in wp-content/uploads
        pdf_links = soup.find_all("a", href=re.compile(r"wp-content/uploads/.*\.pdf", re.I))

        for pdf_link in pdf_links:
            pdf_url = urljoin(article_url, pdf_link["href"])
            yield DiscoveryResult(page_url=article_url, pdf_url=pdf_url, metadata=metadata)
            return  # One PDF per article typically

        # Strategy 2: Look for any PDF links
        all_pdf_links = soup.find_all("a", href=re.compile(r"\.pdf$", re.I))

        for pdf_link in all_pdf_links:
            pdf_url = urljoin(article_url, pdf_link["href"])
            if self._is_valid_pdf_url(pdf_url):
                yield DiscoveryResult(page_url=article_url, pdf_url=pdf_url, metadata=metadata)
                return

        # Strategy 3: Look for download links (might not have .pdf in URL)
        download_selectors = [".download-link", ".pdf-download", ".btn-download", "a[download]"]

        for selector in download_selectors:
            download_links = soup.select(selector)
            for link in download_links:
                href = link.get("href", "")
                if href:
                    pdf_url = urljoin(article_url, href)
                    if self._check_pdf_content_type(pdf_url):
                        yield DiscoveryResult(
                            page_url=article_url, pdf_url=pdf_url, metadata=metadata
                        )
                        return

    def _discover_generic(self, seed_url: str) -> Iterable[DiscoveryResult]:
        """Fallback generic PDF discovery."""
        soup = self._get_page(seed_url)
        if not soup:
            return

        # Find all PDF links on the page
        pdf_links = soup.find_all("a", href=re.compile(r"\.pdf$", re.I))

        for pdf_link in pdf_links:
            pdf_url = urljoin(seed_url, pdf_link["href"])
            if self._is_valid_pdf_url(pdf_url):
                yield DiscoveryResult(page_url=seed_url, pdf_url=pdf_url)

    def _is_valid_pdf_url(self, url: str) -> bool:
        """Validate if URL is likely a valid PDF."""
        if not url:
            return False

        # Basic URL validation
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False

        url_lower = url.lower()

        # Check for PDF extension or Georgetown patterns
        if (
            url_lower.endswith(".pdf")
            or "wp-content/uploads/" in url_lower
            or "georgetownlawtechreview.org" in url_lower
        ):
            return True

        return False

    def _check_pdf_content_type(self, url: str) -> bool:
        """Make HEAD request to check if URL serves PDF content."""
        try:
            resp = self.session.head(url, headers=DEFAULT_HEADERS, timeout=10)
            content_type = resp.headers.get("Content-Type", "").lower()
            return "application/pdf" in content_type
        except Exception:
            return False

    def download_pdf(self, pdf_url: str, out_dir: str) -> Optional[str]:
        """Download PDF using the generic adapter's download logic."""
        return self._download_with_generic(pdf_url, out_dir)

    def _extract_metadata_from_article(self, article_soup: BeautifulSoup, article_url: str) -> dict:
        """Extract article metadata following sitemap specification."""
        metadata = {"source_url": article_url, "url": article_url}

        try:
            # Title: selector "h2.post--title" (from sitemap)
            title_elem = article_soup.select_one("h2.post--title")
            if not title_elem:
                # Fallback selectors
                title_elem = article_soup.select_one("h1") or article_soup.select_one(
                    ".entry-title"
                )
            if title_elem:
                metadata["title"] = title_elem.get_text(strip=True)

            # Author: selector "div.post--author" (from sitemap)
            author_elem = article_soup.select_one("div.post--author")
            if author_elem:
                metadata["author"] = author_elem.get_text(strip=True)

            # Date: selector "div.post--date" (from sitemap)
            date_elem = article_soup.select_one("div.post--date")
            if date_elem:
                metadata["date"] = date_elem.get_text(strip=True)

            # Citation: selector "div.post--citation" (from sitemap)
            citation_elem = article_soup.select_one("div.post--citation")
            if citation_elem:
                citation_text = citation_elem.get_text(strip=True)
                metadata["citation"] = citation_text

                # Try to extract volume/issue from citation for additional context
                citation_match = re.search(
                    r"(\d+)\s+Geo\.\s+L\.\s+Tech\.\s+Rev\.\s+(\d+)", citation_text
                )
                if citation_match:
                    metadata["volume"] = citation_match.group(1)
                    metadata["page_start"] = citation_match.group(2)

            # PDF link would be handled in calling method

        except Exception:
            # Return partial metadata if extraction fails
            pass

        return metadata

    def _extract_issue_metadata(self, issue_soup: BeautifulSoup, issue_url: str) -> dict:
        metadata = {"source_url": issue_url, "url": issue_url}

        title_elem = issue_soup.select_one("h1") or issue_soup.select_one("title")
        issue_title = title_elem.get_text(" ", strip=True) if title_elem else ""
        if issue_title:
            metadata["title"] = issue_title
            year_in_title = re.search(r"\b(19|20)\d{2}\b", issue_title)
            if year_in_title:
                metadata["year"] = year_in_title.group(0)

        path = urlparse(issue_url).path
        m = re.search(r"/issues/volume-(\d+)/issue-(\d+)-((?:19|20)\d{2})", path)
        if m:
            metadata["volume"] = m.group(1)
            metadata["issue"] = m.group(2)
            metadata.setdefault("year", m.group(3))

        return metadata

    def extract_metadata(self, article_soup: BeautifulSoup, article_url: str) -> dict:
        """Public wrapper to expose metadata extraction for tests and callers."""
        return self._extract_metadata_from_article(article_soup, article_url)
