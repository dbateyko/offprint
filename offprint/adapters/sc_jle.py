from __future__ import annotations

from typing import Iterable
from urllib.parse import urlparse

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase


class SCJLEAdapter(SiteArchiveAdapterBase):
    """Journal of Law & Education adapter (sc.edu)."""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        del max_depth
        soup = self._get(seed_url)
        if not soup:
            return

        host = (urlparse(seed_url).netloc or "").lower()
        seen: set[str] = set()
        for text, url in self._iter_links(soup, seed_url):
            if not self._is_pdf_candidate(url, text):
                continue
            if (urlparse(url).netloc or "").lower() != host:
                continue
            if url in seen:
                continue
            seen.add(url)
            yield DiscoveryResult(
                page_url=seed_url,
                pdf_url=url,
                metadata={"source_url": seed_url, "title": text or "JLE article"},
            )
