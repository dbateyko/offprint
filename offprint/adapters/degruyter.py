from __future__ import annotations

from typing import Iterable

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase


class DeGruyterAdapter(SiteArchiveAdapterBase):
    """De Gruyter (degruyter.com) adapter."""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0, browser_session=None) -> Iterable[DiscoveryResult]:
        soup = self._get(seed_url)
        if not soup:
            return

        # Journal home page: list of issues
        if "/journal/key/" in seed_url and seed_url.endswith("/html"):
            for text, url in self._iter_links(soup, seed_url):
                if "/journal/key/" in url and url.endswith("/html") and url != seed_url:
                    # Issue URL looks like /journal/key/TIL/26/2/html
                    yield from self.discover_pdfs(url, max_depth=max_depth-1)
            return

        # Issue page: list of articles with PDF links
        for text, url in self._iter_links(soup, seed_url):
            if "/document/doi/" in url and url.endswith("/pdf"):
                yield DiscoveryResult(
                    page_url=seed_url,
                    pdf_url=url,
                    metadata={"source_url": seed_url, "title": text or "De Gruyter article"},
                )
