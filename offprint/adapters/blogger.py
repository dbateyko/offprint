from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urlparse

from .base import DiscoveryResult
from .generic import GenericAdapter


class BloggerAdapter(GenericAdapter):
    """Adapter for Blogger-hosted archives."""

    def discover_pdfs(
        self, seed_url: str, max_depth: int = 0
    ) -> Iterable[DiscoveryResult]:
        seed_host = (urlparse(seed_url).netloc or "").lower()
        seen: set[str] = set()

        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=max(2, max_depth),
            targeted_only=True,
            max_pages=200,
        ):
            pdf = result.pdf_url
            parsed = urlparse(pdf)
            if (parsed.netloc or "").lower() != seed_host:
                continue
            if ".pdf" not in (parsed.path or "").lower():
                continue
            if pdf in seen:
                continue
            seen.add(pdf)
            yield result

