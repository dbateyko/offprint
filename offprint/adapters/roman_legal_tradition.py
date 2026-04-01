from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import urlparse

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase


class RomanLegalTraditionAdapter(SiteArchiveAdapterBase):
    """Roman Legal Tradition archive adapter."""

    @staticmethod
    def _derive_volume_year(pdf_url: str) -> tuple[str, str]:
        path = (urlparse(pdf_url).path or "").lower()
        year_match = re.search(r"/contents/(19|20)\d{2}/", path)
        year = year_match.group(0).strip("/").split("/")[-1] if year_match else ""
        volume_match = re.search(r"/rlt(\d+)-", path)
        volume = volume_match.group(1) if volume_match else ""
        return volume, year

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
            volume, year = self._derive_volume_year(url)
            yield DiscoveryResult(
                page_url=seed_url,
                pdf_url=url,
                metadata={
                    "source_url": seed_url,
                    "title": text or "Roman Legal Tradition article",
                    "volume": volume,
                    "year": year,
                },
            )
