from __future__ import annotations

from collections.abc import Iterable

from .base import DiscoveryResult
from .generic import GenericAdapter


class DSpaceAdapter(GenericAdapter):
    """Adapter for DSpace 7 repositories."""

    def discover_pdfs(
        self, seed_url: str, max_depth: int = 0
    ) -> Iterable[DiscoveryResult]:
        seen_pdf_urls: set[str] = set()
        scope_id = self._extract_dspace_scope_id(seed_url)

        for result in self._iter_dspace_pdf_candidates(seed_url, scope_id=scope_id or None):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result

        if seen_pdf_urls:
            return

        if "/handle/" in (seed_url or "").lower():
            for result in self._iter_dspace_pdf_candidates(seed_url, scope_id=""):
                if result.pdf_url in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(result.pdf_url)
                yield result

        if seen_pdf_urls:
            return

        for result in super().discover_pdfs(seed_url, max_depth=max_depth):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result

