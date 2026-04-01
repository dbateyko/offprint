from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urlparse

from .base import DiscoveryResult
from .generic import GenericAdapter


class SquarespaceAdapter(GenericAdapter):
    """Generic-like adapter tuned for Squarespace journal archives."""

    SQUARESPACE_PROBE_SUFFIXES = (
        "/archive",
        "/archives",
        "/issue",
        "/issues",
        "/issues-archive",
        "/print",
        "/printarchive",
        "/content",
        "/volumes",
        "/publications",
    )

    def _collect_archive_probe_urls(self, seed_url: str) -> list[str]:
        probes = list(super()._collect_archive_probe_urls(seed_url))
        parsed = urlparse(seed_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        seed_path = parsed.path.rstrip("/")

        for suffix in self.SQUARESPACE_PROBE_SUFFIXES:
            probes.append(f"{base}{suffix}")
            if seed_path:
                probes.append(f"{base}{seed_path}{suffix}")

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in probes:
            normalized = candidate.rstrip("/")
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
            if len(deduped) >= 60:
                break
        return deduped

    def discover_pdfs(
        self, seed_url: str, max_depth: int = 0
    ) -> Iterable[DiscoveryResult]:
        seen_pdf_urls: set[str] = set()
        baseline_depth = max(1, max_depth)

        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=baseline_depth,
            targeted_only=False,
            max_pages=240,
        ):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result

        if seen_pdf_urls:
            return

        fallback_depth = max(3, max_depth)
        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=fallback_depth,
            targeted_only=True,
            max_pages=220,
        ):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result
