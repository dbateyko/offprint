from __future__ import annotations

from typing import Iterable, Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS

# Path tokens that identify STLR-specific archive pages
_STLR_PATH_TOKENS = ("stanford-technology-law-review", "stlr-archive", "stlr")

# Safety bound on the FacetWP ?_paged= walk when the seed does not pin a page.
_MAX_FACETWP_PAGES = 200


class StanfordSTLRAdapter(Adapter):
    """Stanford Technology Law Review adapter.

    Handles WordPress-based archive with FacetWP pagination and Schema.org markup.
    URL pattern: https://law.stanford.edu/stanford-technology-law-review-stlr/stlr-archive/?_paged=[1-13]

    For non-STLR seeds on law.stanford.edu (e.g. Law & Policy Review, CRCL), falls
    back to WordPressAcademicBaseAdapter so those journals are still discovered.
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

    def _is_stlr_seed(self, seed_url: str) -> bool:
        """Return True if *seed_url* points to the STLR-specific archive."""
        lower = seed_url.lower()
        return any(token in lower for token in _STLR_PATH_TOKENS)

    @staticmethod
    def _same_origin(seed_url: str, candidate_url: str) -> bool:
        """Origin gate: a discovered file must live on the seed's own host.

        Without this a link sweep can attribute a third-party PDF (e.g. one cited
        in a reference list) to the journal. Compared case-insensitively with the
        leading ``www.`` ignored so split galley hosts on the same site still work.
        """

        def _host(url: str) -> str:
            netloc = urlparse(url).netloc.lower()
            if "@" in netloc:
                netloc = netloc.rsplit("@", 1)[1]
            if netloc.startswith("www."):
                netloc = netloc[4:]
            return netloc

        return bool(_host(candidate_url)) and _host(candidate_url) == _host(seed_url)

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        """Discover PDFs from a law.stanford.edu journal archive page.

        Every journal archive on this host (STLR, SJCRCL, SJLBF, SELJ, SLPR, ...)
        renders the same Schema.org ``ScholarlyArticle`` rows, so that path is used
        whenever it is available and the generic WordPress crawler is only a
        fallback for pages without it. Discovery is publication-scoped because the
        archive page belongs to one journal; the owning journal is echoed back on
        every result as ``metadata['publisher']`` so it can be audited downstream.
        """
        soup = self._get_page(seed_url)
        articles = (
            soup.find_all("article", {"itemtype": "https://schema.org/ScholarlyArticle"})
            if soup
            else []
        )

        # The law.stanford.edu WordPress theme renders every journal's FacetWP
        # archive (STLR, SJCRCL, SJLBF, SELJ, SLPR, ...) with the same Schema.org
        # markup. Use that path whenever it is present; only fall back to the
        # generic WordPress crawler when the page has no ScholarlyArticle nodes.
        if not articles:
            if self._is_stlr_seed(seed_url):
                return
            from .wordpress_academic_base import WordPressAcademicBaseAdapter

            wp = WordPressAcademicBaseAdapter(session=self.session)
            yield from wp.discover_pdfs(seed_url, max_depth=max_depth)
            return

        yield from self._yield_articles(seed_url, articles)

        # FacetWP pagination. When the seed already pins ?_paged=N (the
        # range-expanded seed form, e.g. "?_paged=[1-14]"), the orchestrator owns
        # paging and this adapter must stay on the single requested page.
        # Otherwise walk ?_paged=2,3,... until a page yields no articles.
        if "_paged" in parse_qs(urlparse(seed_url).query):
            return

        for page in range(2, _MAX_FACETWP_PAGES + 1):
            page_url = self._with_paged(seed_url, page)
            page_soup = self._get_page(page_url)
            if not page_soup:
                return
            page_articles = page_soup.find_all(
                "article", {"itemtype": "https://schema.org/ScholarlyArticle"}
            )
            if not page_articles:
                return
            yield from self._yield_articles(page_url, page_articles)

    @staticmethod
    def _with_paged(url: str, page: int) -> str:
        parts = urlparse(url)
        query = parse_qs(parts.query, keep_blank_values=True)
        query["_paged"] = [str(page)]
        return urlunparse(parts._replace(query=urlencode(query, doseq=True)))

    def _yield_articles(self, page_url: str, articles) -> Iterable[DiscoveryResult]:
        for article in articles:
            try:
                # Extract PDF URL using Schema.org workExample property
                pdf_link = article.find("a", {"itemprop": "url workExample"})
                if not pdf_link or not pdf_link.get("href"):
                    continue

                pdf_url = urljoin(page_url, pdf_link["href"])

                # Origin gate: never attribute an off-host file to this journal.
                if not self._same_origin(page_url, pdf_url):
                    continue

                # Validate it's actually a PDF URL
                if not self._is_likely_pdf_url(pdf_url):
                    continue

                # Extract metadata according to sitemap specification
                metadata = self._extract_metadata_from_article(article)

                yield DiscoveryResult(page_url=page_url, pdf_url=pdf_url, metadata=metadata)

            except Exception:
                # Skip malformed articles but continue processing
                continue

    def _is_likely_pdf_url(self, url: str) -> bool:
        """Check if URL is likely a PDF based on patterns."""
        url_lower = url.lower()

        # Direct PDF file extension
        if url_lower.endswith(".pdf"):
            return True

        # Stanford's wp-content/uploads pattern for PDFs
        if "wp-content/uploads/" in url_lower and (
            "pdf" in url_lower or self._check_pdf_content_type(url)
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

    def extract_metadata(self, article_soup: BeautifulSoup) -> dict:
        """Extract metadata from a Stanford STLR article element.

        Args:
            article_soup: BeautifulSoup object containing the article element

        Returns:
            Dictionary containing extracted metadata
        """
        return self._extract_metadata_from_article(article_soup)

    def _extract_metadata_from_article(self, article_soup: BeautifulSoup) -> dict:
        """Extract article metadata following sitemap specification."""
        metadata = {}

        try:
            # Title: selector "a[itemprop='url sameAs']"
            title_link = article_soup.find("a", {"itemprop": "url sameAs"})
            if title_link:
                metadata["title"] = title_link.get_text(strip=True)

            # Authors: selector ".li-left-wrap ul.li-meta:nth-of-type(1)" (from sitemap)
            # But based on analysis, use the improved Schema.org selector
            authors = []
            author_elements = article_soup.find_all(attrs={"itemprop": "author"})
            for author_elem in author_elements:
                name_elem = author_elem.find(attrs={"itemprop": "name"})
                if name_elem:
                    authors.append(name_elem.get_text(strip=True))

            # Fallback to sitemap selector if Schema.org fails
            if not authors:
                author_container = article_soup.select_one(
                    ".li-left-wrap ul.li-meta:nth-of-type(1)"
                )
                if author_container:
                    author_text = author_container.get_text(strip=True)
                    if author_text:
                        authors = [author_text]

            metadata["authors"] = authors

            # Volume: selector "span[itemprop='volumeNumber']"
            volume_elem = article_soup.find("span", {"itemprop": "volumeNumber"})
            if volume_elem:
                metadata["volume"] = volume_elem.get_text(strip=True)

            # Issue: selector "span[itemprop='issueNumber']"
            issue_elem = article_soup.find("span", {"itemprop": "issueNumber"})
            if issue_elem:
                metadata["issue"] = issue_elem.get_text(strip=True)

            # Date: selector "time" (from sitemap) - prefer datePublished
            date_elem = article_soup.find(
                "time", {"itemprop": "datePublished"}
            ) or article_soup.find("time")
            if date_elem:
                metadata["date"] = date_elem.get_text(strip=True)

            # Page: selector ".ptp-meta li:nth-of-type(3) > span" (fallback to itemprop pagination)
            page_elem = article_soup.select_one(".ptp-meta li:nth-of-type(3) > span")
            if not page_elem:
                # Fallback to Schema.org pagination for tests
                page_elem = article_soup.find(attrs={"itemprop": "pagination"})
            if page_elem:
                metadata["pages"] = page_elem.get_text(
                    strip=True
                )  # Changed from "page" to "pages" to match test

            # Publisher / periodical name: the archive theme stamps the owning
            # journal on every row, so downstream can verify publication scope.
            publisher_elem = article_soup.select_one(
                "li.ptp-publisher span[itemprop='name']"
            ) or article_soup.select_one(
                "[itemtype='https://schema.org/Periodical'] [itemprop='name']"
            )
            if publisher_elem:
                metadata["publisher"] = publisher_elem.get_text(strip=True)
                metadata.setdefault("journal", metadata["publisher"])

            # URL (article page link): selector "a[itemprop='url sameAs']"
            if title_link and title_link.get("href"):
                metadata["url"] = title_link.get("href")

        except Exception:
            # Return partial metadata if extraction fails
            pass

        return metadata
