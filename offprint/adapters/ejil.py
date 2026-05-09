from __future__ import annotations

from typing import Iterable

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase


class EJILAdapter(SiteArchiveAdapterBase):
    """European Journal of International Law (ejil.org) adapter."""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0, browser_session=None) -> Iterable[DiscoveryResult]:
        soup = self._get(seed_url)
        if not soup:
            return

        # Handle index page (archives.php)
        if "archives.php" in seed_url or seed_url.endswith("/"):
            for text, url in self._iter_links(soup, seed_url):
                if "archive.php?issue=" in url:
                    # Recursive call; decrement max_depth if we want to honor it strictly
                    # but here we just need one level of depth for the issue.
                    yield from self.discover_pdfs(url, max_depth=max_depth-1)
            return

        # Handle issue page (archive.php?issue=N)
        for text, url in self._iter_links(soup, seed_url):
            if self._is_pdf_candidate(url, text):
                yield DiscoveryResult(
                    page_url=seed_url,
                    pdf_url=url,
                    metadata={"source_url": seed_url, "title": text or "EJIL article"},
                )
            elif "article.php?article=" in url:
                # Direct links to article pages
                # We could crawl them, but issue pages usually have direct PDF links
                # for free content. If we wanted to be exhaustive for all metadata
                # we would crawl, but for smoke test/discovery this is enough.
                pass
