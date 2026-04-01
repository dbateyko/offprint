from __future__ import annotations

from typing import Iterable

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase
from .utils import absolutize


class UHHJILAdapter(SiteArchiveAdapterBase):
    """Houston Journal of International Law adapter (law.uh.edu/hjil)."""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        del max_depth
        seed_soup = self._get(seed_url)
        if not seed_soup:
            return

        archive_url = ""
        for text, url in self._iter_links(seed_soup, seed_url):
            if "archives.asp" in url.lower() or text.lower() == "archives":
                archive_url = url
                break
        if not archive_url:
            archive_url = absolutize(seed_url, "archives.asp")

        archive_soup = self._get(archive_url)
        if not archive_soup:
            return

        seen: set[str] = set()
        for text, url in self._iter_links(archive_soup, archive_url):
            if not self._is_pdf_candidate(url, text):
                continue
            if url in seen:
                continue
            seen.add(url)
            yield DiscoveryResult(
                page_url=archive_url,
                pdf_url=url,
                metadata={"source_url": archive_url, "title": text or "HJIL article"},
            )
