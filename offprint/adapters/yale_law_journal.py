from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import urlparse

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase


class YaleLawJournalAdapter(SiteArchiveAdapterBase):
    """Yale Law Journal issue adapter for article pages with /pdf/ links."""

    _ARTICLE_PATH_RE = re.compile(
        r"/(article|note|comment|essay|feature|review|tribute)/", re.IGNORECASE
    )

    @staticmethod
    def _canonical_host(host: str) -> str:
        lowered = (host or "").lower()
        return lowered[4:] if lowered.startswith("www.") else lowered

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        del max_depth
        seed_soup = self._get(seed_url)
        if not seed_soup:
            return

        seed_host = (urlparse(seed_url).netloc or "").lower()
        canonical_seed_host = self._canonical_host(seed_host)
        seen_pdf: set[str] = set()

        # Some forum archive pages directly include in-domain PDF links.
        for text, url in self._iter_links(seed_soup, seed_url):
            parsed = urlparse(url)
            link_host = self._canonical_host(parsed.netloc or "")
            lowered = url.lower()
            if link_host != canonical_seed_host:
                continue
            if not (
                lowered.endswith(".pdf")
                or ".pdf?" in lowered
                or "/pdf/" in lowered
                or "/images/pdfs/" in lowered
            ):
                continue
            if url in seen_pdf:
                continue
            seen_pdf.add(url)
            yield DiscoveryResult(
                page_url=seed_url,
                pdf_url=url,
                metadata={"source_url": seed_url, "title": text or "YLJ forum/pdf"},
            )

        article_urls: list[tuple[str, str]] = []
        for text, url in self._iter_links(seed_soup, seed_url):
            parsed = urlparse(url)
            if self._canonical_host(parsed.netloc or "") != canonical_seed_host:
                continue
            if not self._ARTICLE_PATH_RE.search(parsed.path or ""):
                continue
            article_urls.append((text, url))

        seen_article: set[str] = set()
        for article_title, article_url in article_urls[:120]:
            if article_url in seen_article:
                continue
            seen_article.add(article_url)

            article_soup = self._get(article_url)
            for text, url in self._iter_links(article_soup, article_url):
                lowered = url.lower()
                if not ("/pdf/" in lowered or self._is_pdf_candidate(url, text)):
                    continue
                if url in seen_pdf:
                    continue
                seen_pdf.add(url)
                yield DiscoveryResult(
                    page_url=article_url,
                    pdf_url=url,
                    metadata={
                        "source_url": article_url,
                        "title": article_title or text or "YLJ article",
                    },
                )
