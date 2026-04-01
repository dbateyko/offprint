from __future__ import annotations

from typing import Iterable

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase


class UNDLawReviewAdapter(SiteArchiveAdapterBase):
    """North Dakota Law Review adapter (law.und.edu)."""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        del max_depth
        seed_soup = self._get(seed_url)
        if not seed_soup:
            return

        issue_pages: list[str] = []
        for _text, url in self._iter_links(seed_soup, seed_url):
            lowered = url.lower()
            if "/law-review/issues/" in lowered or lowered.endswith("/law-review/archive.html"):
                issue_pages.append(url)

        if not issue_pages:
            issue_pages.append(seed_url)

        seen: set[str] = set()
        for page_url in dict.fromkeys(issue_pages):
            page_soup = self._get(page_url)
            for text, url in self._iter_links(page_soup, page_url):
                if not self._is_pdf_candidate(url, text):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                yield DiscoveryResult(
                    page_url=page_url,
                    pdf_url=url,
                    metadata={"source_url": page_url, "title": text or "ND Law Review article"},
                )
