from __future__ import annotations

from typing import Iterable

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase


class AIPLAQuarterlyJournalAdapter(SiteArchiveAdapterBase):
    """AIPLA Quarterly Journal issue/detail adapter."""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        del max_depth
        seed_soup = self._get(seed_url)
        if not seed_soup:
            return

        issue_urls: list[str] = []
        for _text, url in self._iter_links(seed_soup, seed_url):
            if "/detail/journal-issue/" in url.lower():
                issue_urls.append(url)
        if not issue_urls:
            issue_urls.append(seed_url)

        seen: set[str] = set()
        for issue_url in dict.fromkeys(issue_urls):
            issue_soup = self._get(issue_url)
            for text, url in self._iter_links(issue_soup, issue_url):
                lowered = url.lower()
                if not (
                    ("/docs/default-source/" in lowered and ".pdf" in lowered)
                    or self._is_pdf_candidate(url, text)
                ):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                yield DiscoveryResult(
                    page_url=issue_url,
                    pdf_url=url,
                    metadata={"source_url": issue_url, "title": text or "AIPLA article"},
                )
