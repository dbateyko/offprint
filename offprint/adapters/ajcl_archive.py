from __future__ import annotations

from typing import Iterable

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase


class AJCLArchiveAdapter(SiteArchiveAdapterBase):
    """American Journal of Criminal Law archive adapter (ajcl.org)."""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        del max_depth
        soup = self._get(seed_url)
        if not soup:
            return

        seen: set[str] = set()
        for text, url in self._iter_links(soup, seed_url):
            if not self._is_pdf_candidate(url, text):
                continue
            if url in seen:
                continue
            seen.add(url)
            yield DiscoveryResult(
                page_url=seed_url,
                pdf_url=url,
                metadata={"source_url": seed_url, "title": text or "AJCL article"},
            )
