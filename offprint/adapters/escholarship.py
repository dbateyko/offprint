from __future__ import annotations

from collections.abc import Iterable

from .base import DiscoveryResult
from .generic import GenericAdapter


class EScholarshipAdapter(GenericAdapter):
    """Adapter for UC eScholarship (escholarship.org)."""

    def discover_pdfs(
        self, seed_url: str, max_depth: int = 0
    ) -> Iterable[DiscoveryResult]:
        seen_pdf_urls: set[str] = set()

        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=max(1, max_depth),
            targeted_only=False,
            max_pages=260,
        ):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result

        if seen_pdf_urls:
            return

        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=max(3, max_depth),
            targeted_only=True,
            max_pages=220,
        ):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result

