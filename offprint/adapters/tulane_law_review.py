"""Adapter for Tulane Law Review Online (Squarespace).

Scope note -- read before extending this
----------------------------------------
The *print* Tulane Law Review has NO open-access PDFs.  ``tulanelawreview.org``
carries ~2,300 ``/pub/volumeNN/issueM/<slug>`` pages spanning volumes 52-100
(1977-2026), but each one is an abstract stub whose only full-text link points
at Westlaw.  A 30-page random sample across that whole volume range found zero
article files.  Only the *online* companion (``/tlr-online/``) publishes PDFs,
and it hosts them same-origin as Squarespace ``/s/<name>.pdf`` assets.

This adapter therefore covers Tulane Law Review Online only.

Origin gate
-----------
``GenericAdapter`` would also find these files, but its ``looks_like_candidate``
test accepts *any* host's ``.pdf`` href (only its crawl frontier is
origin-scoped).  On footnote-heavy law-review pages that is the 2026-08-24 OJS
scope-leak shape.  This adapter accepts a PDF only when it is same-origin with
the seed, so a cited third-party PDF can never be recorded as a Tulane article.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase

JOURNAL_NAME = "Tulane Law Review Online"

#: Listing paths that are navigation, not articles.
NON_ARTICLE_SEGMENTS = ("/tag/", "/category/", "/author/")

#: "01-95OEnglehartfinal.pdf" / "92onlineKaufman7.pdf" -> volume 95 / 92.
VOLUME_IN_FILENAME_RE = re.compile(r"(?:^|[^0-9])(?P<volume>\d{2,3})\s*O", re.IGNORECASE)


class TulaneLawReviewOnlineAdapter(SiteArchiveAdapterBase):
    """Squarespace adapter restricted to same-origin article PDFs."""

    @staticmethod
    def _origin(url: str) -> str:
        return (urlparse(url or "").netloc or "").lower()

    @classmethod
    def _accept_pdf_url(cls, url: str, seed_url: str) -> bool:
        """Same-origin gate: a cited third-party PDF is not our article."""
        if cls._origin(url) != cls._origin(seed_url):
            return False
        return urlparse(url).path.lower().endswith(".pdf")

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        index_soup = self._get(seed_url)
        if not index_soup:
            return
        seen: set = set()
        for post_url, listing_title in self._post_links(index_soup, seed_url):
            post_soup = self._get(post_url)
            if not post_soup:
                continue
            title = self._page_title(post_soup) or listing_title
            for pdf_url in self._same_origin_pdfs(post_soup, post_url, seed_url):
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                metadata = {
                    "title": title,
                    "journal": JOURNAL_NAME,
                    "source_url": post_url,
                    "url": post_url,
                }
                volume = self._volume_from_filename(pdf_url)
                if volume:
                    metadata["volume"] = volume
                yield DiscoveryResult(
                    page_url=post_url,
                    pdf_url=pdf_url,
                    metadata=metadata,
                    source_adapter=type(self).__name__,
                    extraction_path="squarespace_online_post",
                )

    def _post_links(self, soup: BeautifulSoup, seed_url: str) -> List[Tuple[str, str]]:
        seed_path = urlparse(seed_url).path.rstrip("/") or "/tlr-online"
        out: List[Tuple[str, str]] = []
        seen: set = set()
        for anchor in soup.select("a[href]"):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            absolute = urljoin(seed_url, href).split("#", 1)[0].split("?", 1)[0]
            if self._origin(absolute) != self._origin(seed_url):
                continue
            path = urlparse(absolute).path.rstrip("/")
            if not path.startswith(seed_path + "/"):
                continue
            if any(segment in path + "/" for segment in NON_ARTICLE_SEGMENTS):
                continue
            if absolute in seen:
                continue
            seen.add(absolute)
            out.append((absolute, " ".join(anchor.get_text(" ", strip=True).split())))
        return out

    def _same_origin_pdfs(
        self, soup: BeautifulSoup, page_url: str, seed_url: str
    ) -> List[str]:
        out: List[str] = []
        for selector, attribute in (
            ("a[href]", "href"),
            ("iframe[src]", "src"),
            ("embed[src]", "src"),
            ("object[data]", "data"),
        ):
            for element in soup.select(selector):
                raw = (element.get(attribute) or "").strip()
                if not raw:
                    continue
                absolute = urljoin(page_url, raw)
                if self._accept_pdf_url(absolute, seed_url) and absolute not in out:
                    out.append(absolute)
        return out

    #: Tried in order.  A bare ``h1`` is last because Squarespace blog posts
    #: render a date heading before the article title, and BeautifulSoup's
    #: comma-selector returns document order rather than selector priority.
    TITLE_SELECTORS = (
        "h1.entry-title",
        ".entry-title",
        "article h1",
        "main h1",
        "h1",
    )

    @classmethod
    def _page_title(cls, soup: BeautifulSoup) -> str:
        for selector in cls.TITLE_SELECTORS:
            for element in soup.select(selector):
                text = " ".join(element.get_text(" ", strip=True).split())
                if text and not cls._looks_like_date(text):
                    return text
        meta = soup.select_one('meta[property="og:title"]')
        if meta:
            return " ".join((meta.get("content") or "").split())
        return ""

    @staticmethod
    def _looks_like_date(text: str) -> bool:
        return bool(re.match(r"^[A-Z][a-z]+ \d{1,2}, \d{4}$", text.strip()))

    @staticmethod
    def _volume_from_filename(pdf_url: str) -> str:
        stem = urlparse(pdf_url).path.rsplit("/", 1)[-1]
        match = VOLUME_IN_FILENAME_RE.search(stem)
        return match.group("volume") if match else ""
