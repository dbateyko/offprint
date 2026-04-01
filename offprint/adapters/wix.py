from __future__ import annotations

import re
from collections.abc import Iterable
from urllib.parse import urlparse

from .base import DiscoveryResult
from .generic import GenericAdapter


class WixAdapter(GenericAdapter):
    """Generic-like adapter tuned for Wix-hosted law-journal archives."""

    WIX_PROBE_SUFFIXES = (
        "/archive",
        "/archives",
        "/issue",
        "/issues",
        "/publications",
        "/symposia",
        "/print",
        "/online",
        "/content",
    )

    def _collect_archive_probe_urls(self, seed_url: str) -> list[str]:
        probes = list(super()._collect_archive_probe_urls(seed_url))
        parsed = urlparse(seed_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        seed_path = parsed.path.rstrip("/")

        for suffix in self.WIX_PROBE_SUFFIXES:
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

        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=max(1, max_depth),
            targeted_only=True,
            max_pages=220,
        ):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            self._enrich_metadata(result)
            yield result

        if seen_pdf_urls:
            return

        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=max(2, max_depth),
            targeted_only=False,
            max_pages=220,
        ):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            self._enrich_metadata(result)
            yield result

    @staticmethod
    def _infer_volume_year(text: str) -> tuple[str, str]:
        text = text or ""
        volume = ""
        year = ""
        vol_match = re.search(r"\bvol(?:ume)?[.\s_-]*([0-9]{1,3})\b", text, re.I)
        if vol_match:
            volume = vol_match.group(1)
        year_match = re.search(r"(19\d{2}|20\d{2})", text)
        if year_match:
            year = year_match.group(1)
        return volume, year

    def _enrich_metadata(self, result: DiscoveryResult) -> None:
        metadata = result.metadata or {}
        if metadata.get("volume") and metadata.get("date"):
            return
        text = " ".join(
            part for part in (result.pdf_url or "", metadata.get("title") or "", result.page_url or "") if part
        )
        volume, year = self._infer_volume_year(text)
        if volume and not metadata.get("volume"):
            metadata["volume"] = volume
        if year and not metadata.get("date"):
            metadata["date"] = year
        result.metadata = metadata
