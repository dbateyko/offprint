from __future__ import annotations

from typing import Iterable, List, Set
from urllib.parse import urlparse

from .base import DiscoveryResult
from .drupal import DrupalAdapter
from .site_archive_base import SiteArchiveAdapterBase


class RutgersLawJournalsAdapter(SiteArchiveAdapterBase):
    """Hub adapter for Rutgers Law's journals index page.

    The seed is a directory of journal sites, not a journal archive itself.
    This adapter discovers outbound journal links and delegates to the mapped
    adapter for each linked journal host.
    """

    HUB_HINTS = ("journal", "law review", "review")
    BLOCKED_HOST_TOKENS = ("rutgers.edu", "linkedin.com", "instagram.com", "facebook.com", "x.com")

    def _journal_targets(self, seed_url: str) -> List[str]:
        soup = self._get(seed_url)
        targets: List[str] = []
        seen: Set[str] = set()
        for text, url in self._iter_links(soup, seed_url):
            parsed = urlparse(url)
            host = (parsed.netloc or "").lower()
            if not host:
                continue
            if any(token in host for token in self.BLOCKED_HOST_TOKENS):
                continue
            lowered_text = (text or "").lower()
            if not any(hint in lowered_text or hint in host for hint in self.HUB_HINTS):
                continue
            if url in seen:
                continue
            seen.add(url)
            targets.append(url)
        return targets

    def discover_pdfs(self, seed_url: str, max_depth: int = 1) -> Iterable[DiscoveryResult]:
        from .registry import pick_adapter_for

        yielded = False
        for target_url in self._journal_targets(seed_url):
            target_host = (urlparse(target_url).netloc or "").lower()
            if target_host == "law.rutgers.edu":
                continue
            try:
                adapter = pick_adapter_for(target_url, session=self.session, allow_generic=False)
            except Exception:
                continue

            for result in adapter.discover_pdfs(target_url, max_depth=max_depth):
                yielded = True
                result.metadata.setdefault("source_hub_url", seed_url)
                result.metadata.setdefault("extraction_method", "rutgers_law_journals_hub")
                yield result

        if yielded:
            return

        # Fallback preserves old behavior if journal link extraction fails.
        yield from DrupalAdapter(session=self.session).discover_pdfs(seed_url, max_depth=max_depth)
