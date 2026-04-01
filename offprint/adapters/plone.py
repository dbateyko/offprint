from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urlparse

from .base import DiscoveryResult
from .generic import GenericAdapter


class PloneAdapter(GenericAdapter):
    """Adapter for Plone-backed journal pages."""

    def discover_pdfs(
        self, seed_url: str, max_depth: int = 0
    ) -> Iterable[DiscoveryResult]:
        seed_host = (urlparse(seed_url).netloc or "").lower()
        seen: set[str] = set()

        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=max(2, max_depth),
            targeted_only=True,
            max_pages=220,
        ):
            pdf = result.pdf_url
            parsed = urlparse(pdf)
            path = (parsed.path or "").lower()
            if (parsed.netloc or "").lower() != seed_host:
                continue
            if ".pdf" not in path and "/@@download/file" not in path and "/@@display-file/file" not in path:
                continue
            if pdf in seen:
                continue
            seen.add(pdf)
            yield result

