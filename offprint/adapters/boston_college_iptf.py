from __future__ import annotations

import re
from typing import Iterable, Optional
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase
from .utils import DEFAULT_HEADERS, request_verify_for_url


class BostonCollegeIPTFAdapter(SiteArchiveAdapterBase):
    """Publication-scoped adapter for Boston College's IPTF journal."""

    PUBLICATION_PREFIX = "/iptf/"
    LIRA_WORK_PREFIXES = ("/works/publication-article/", "/work/sc/")
    MAX_ARCHIVE_PAGES = 30
    NON_ARTICLE_RE = re.compile(r"\b(?:blog post|masthead|journal staff)\b", re.IGNORECASE)
    CHALLENGE_RE = re.compile(
        r"(?:cf-chl-|cloudflare ray id|attention required|verify you are human)",
        re.IGNORECASE,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_discovery = False

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        parsed_seed = urlparse(seed_url)
        if parsed_seed.netloc.lower() != "sites.bc.edu" or not parsed_seed.path.startswith(
            self.PUBLICATION_PREFIX
        ):
            return

        archive_url = f"{parsed_seed.scheme}://{parsed_seed.netloc}{self.PUBLICATION_PREFIX}"
        first_page = self._fetch_html(archive_url)
        if not first_page:
            return

        max_pages = min(self._archive_page_count(first_page), self.MAX_ARCHIVE_PAGES)
        seen_articles: set[str] = set()
        seen_pdfs: set[str] = set()

        for page_number in range(1, max_pages + 1):
            if self._stop_discovery:
                return
            listing_url = archive_url if page_number == 1 else f"{archive_url}?paged={page_number}"
            listing = first_page if page_number == 1 else self._fetch_html(listing_url)
            if not listing:
                return

            cards = listing.select("article.post")
            if not cards:
                return

            for card in cards:
                article = self._article_link(card, archive_url)
                if not article:
                    continue
                listing_title, article_url = article
                if article_url in seen_articles or self._is_non_article(listing_title, card):
                    continue
                seen_articles.add(article_url)

                result = self._discover_article(article_url, listing_title)
                if result is None:
                    if self._stop_discovery:
                        return
                    continue

                if result.pdf_url in seen_pdfs:
                    continue
                seen_pdfs.add(result.pdf_url)
                yield result

    def _discover_article(
        self, article_url: str, listing_title: str
    ) -> Optional[DiscoveryResult]:
        article = self._fetch_html(article_url)
        if not article or self._is_non_article(listing_title, article):
            return None

        work_url = self._lira_work_url(article, article_url)
        if not work_url:
            return None
        work = self._fetch_html(work_url)
        if not work or "Intellectual Property and Technology Forum" not in work.get_text(
            " ", strip=True
        ):
            return None

        download = work.select_one('a[href*="/downloads/"][href*=".pdf"]')
        if not download:
            return None
        pdf_url = urljoin(work_url, download.get("href", ""))

        title = self._work_title(work) or listing_title
        authors = self._work_authors(work)
        published = work.select_one("time[datetime]")
        date = (published.get("datetime") or "").strip() if published else ""

        metadata: dict[str, object] = {
            "title": title,
            "source_url": article_url,
            "url": article_url,
            "repository_url": work_url,
            "document_type": "article",
        }
        if authors:
            metadata["authors"] = authors
        if date:
            metadata["date"] = date
            metadata["year"] = date[:4]

        return DiscoveryResult(
            page_url=article_url,
            pdf_url=pdf_url,
            metadata=metadata,
            source_adapter=type(self).__name__,
            extraction_path="iptf_post_to_lira_work",
        )

    def _fetch_html(self, url: str) -> Optional[BeautifulSoup]:
        if self._stop_discovery:
            return None
        try:
            response = self.session.get(
                url,
                headers=DEFAULT_HEADERS,
                timeout=20,
                verify=request_verify_for_url(url),
            )
        except Exception:
            return None

        if response.status_code in {403, 429}:
            self._stop_discovery = True
            return None
        if response.status_code >= 400:
            return None
        text = response.text or ""
        if not text.strip() or self.CHALLENGE_RE.search(text):
            self._stop_discovery = True
            return None
        return BeautifulSoup(text, "lxml")

    @classmethod
    def _archive_page_count(cls, soup: BeautifulSoup) -> int:
        nav = soup.select_one("nav[data-pagination-max-pages]")
        if nav:
            try:
                return max(1, int(nav.get("data-pagination-max-pages", "1")))
            except ValueError:
                pass
        pages = [1]
        for anchor in soup.select('a[href*="paged="]'):
            try:
                pages.append(int(parse_qs(urlparse(anchor.get("href", "")).query)["paged"][0]))
            except (KeyError, TypeError, ValueError):
                continue
        return max(pages)

    @classmethod
    def _article_link(cls, card: Tag, base_url: str) -> Optional[tuple[str, str]]:
        anchor = card.select_one("h2.entry-title a[href]")
        if not anchor:
            return None
        title = " ".join(anchor.get_text(" ", strip=True).split())
        url = urljoin(base_url, anchor.get("href", ""))
        parsed = urlparse(url)
        if parsed.netloc.lower() != "sites.bc.edu" or not parsed.path.startswith(
            cls.PUBLICATION_PREFIX
        ):
            return None
        return title, url

    @classmethod
    def _is_non_article(cls, title: str, element: Tag) -> bool:
        if cls.NON_ARTICLE_RE.search(title or ""):
            return True
        classes = {str(value).lower() for value in element.get("class", [])}
        if "category-blog-post" in classes or "tag-blog-post" in classes:
            return True
        body = element.select_one(".yuki-article-content, .entry-content")
        body_text = body.get_text(" ", strip=True)[:600] if body else ""
        return bool(re.search(r"\bnot (?:a|an) (?:published )?IPTF Journal article\b", body_text, re.I))

    @classmethod
    def _lira_work_url(cls, soup: BeautifulSoup, base_url: str) -> str:
        for anchor in soup.select('a[href*="lira.bc.edu/"]'):
            url = urljoin(base_url, anchor.get("href", ""))
            parsed = urlparse(url)
            if parsed.netloc.lower() == "lira.bc.edu" and any(
                parsed.path.startswith(prefix) for prefix in cls.LIRA_WORK_PREFIXES
            ):
                return url
        return ""

    @staticmethod
    def _work_title(soup: BeautifulSoup) -> str:
        heading = soup.select_one("main h1, h1")
        return " ".join(heading.get_text(" ", strip=True).split()) if heading else ""

    @staticmethod
    def _work_authors(soup: BeautifulSoup) -> list[str]:
        heading = soup.select_one("#creators-list")
        if not heading or not heading.parent:
            return []
        authors: list[str] = []
        for item in heading.parent.select("li button"):
            author = " ".join(item.get_text(" ", strip=True).split())
            if author and author not in authors:
                authors.append(author)
        return authors
