from __future__ import annotations

from typing import Iterable
from urllib.parse import urlparse

from .base import DiscoveryResult
from .ojs import OJSAdapter


class UMassDOJSAdapter(OJSAdapter):
    """Host-specific OJS adapter for UMass Dartmouth Law Review."""

    @staticmethod
    def _normalized_seed(seed_url: str) -> str:
        parsed = urlparse(seed_url)
        path = (parsed.path or "").rstrip("/")
        if path.endswith("/issue/archive"):
            return seed_url
        if path.endswith("/umlr"):
            return seed_url.rstrip("/") + "/issue/archive"
        return seed_url

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        normalized = self._normalized_seed(seed_url)
        if normalized != seed_url:
            yielded = False
            for result in super().discover_pdfs(normalized, max_depth=max_depth):
                yielded = True
                yield result
            if yielded:
                return
        yield from super().discover_pdfs(seed_url, max_depth=max_depth)
