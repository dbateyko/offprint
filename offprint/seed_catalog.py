from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from glob import glob
from typing import Any, Iterable
from urllib.parse import urlparse

RANGE_RE = re.compile(r"\[(\d+)-(\d+)\]")

SEED_STATUS_ACTIVE = "active"
SEED_STATUS_TODO_ADAPTER = "todo_adapter"
SEED_STATUS_PAUSED_404 = "paused_404"
SEED_STATUS_PAUSED_WAF = "paused_waf"
SEED_STATUS_PAUSED_LOGIN = "paused_login"
SEED_STATUS_PAUSED_PAYWALL = "paused_paywall"
SEED_STATUS_PAUSED_OTHER = "paused_other"

SEED_STATUSES = {
    SEED_STATUS_ACTIVE,
    SEED_STATUS_TODO_ADAPTER,
    SEED_STATUS_PAUSED_404,
    SEED_STATUS_PAUSED_WAF,
    SEED_STATUS_PAUSED_LOGIN,
    SEED_STATUS_PAUSED_PAYWALL,
    SEED_STATUS_PAUSED_OTHER,
}


@dataclass(frozen=True)
class SeedEntry:
    seed_url: str
    sitemap_id: str
    sitemap_file: str
    source_path: str
    journal_name: str
    metadata: dict[str, Any]
    adapter_config: dict[str, Any]
    status: str
    status_reason: str
    status_updated_at: str
    status_evidence_ref: str
    status_inferred: bool
    has_selectors: bool


def expand_ranges(url: str) -> list[str]:
    match = RANGE_RE.search(url)
    if not match:
        return [url]
    start, end = int(match.group(1)), int(match.group(2))
    expanded: list[str] = []
    for n in range(start, end + 1):
        expanded.extend(expand_ranges(RANGE_RE.sub(str(n), url, count=1)))
    return expanded


def _is_http_seed(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(_normalize_text(v) for v in value if _normalize_text(v))
    return str(value).strip()


def _extract_journal_name(metadata: dict[str, Any]) -> str:
    for key in ("journal", "journal_name", "publication_title", "name"):
        text = _normalize_text(metadata.get(key))
        if text:
            return re.sub(r"\s+", " ", text)
    return ""


def _normalize_start_urls(raw: dict[str, Any]) -> list[str]:
    start_urls = raw.get("start_urls") or raw.get("startUrl") or raw.get("url") or []
    if not start_urls:
        return []

    if isinstance(start_urls, str):
        start_urls = [start_urls]
    if not isinstance(start_urls, list):
        return []
    return [u for u in start_urls if isinstance(u, str) and u.strip()]


def load_seed_entries(sitemaps_dir: str) -> list[SeedEntry]:
    entries: list[SeedEntry] = []
    for path in sorted(glob(os.path.join(sitemaps_dir, "*.json"))):
        try:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            continue

        metadata = payload.get("metadata")
        metadata = dict(metadata) if isinstance(metadata, dict) else {}
        adapter_config = payload.get("adapter_config")
        adapter_config = dict(adapter_config) if isinstance(adapter_config, dict) else {}
        selectors = payload.get("selectors")
        if not isinstance(selectors, list):
            raw_sitemap = payload.get("raw_sitemap")
            selectors = raw_sitemap.get("selectors") if isinstance(raw_sitemap, dict) else []
        has_selectors = bool(isinstance(selectors, list) and selectors)
        status_raw = _normalize_text(metadata.get("status")).lower()
        status_inferred = not bool(status_raw)
        status = status_raw or SEED_STATUS_ACTIVE

        sitemap_file = os.path.basename(path)
        sitemap_id = str(
            payload.get("id") or payload.get("_id") or os.path.splitext(sitemap_file)[0]
        ).strip()
        journal_name = _extract_journal_name(metadata)

        for seed in _normalize_start_urls(payload):
            for expanded in expand_ranges(seed):
                if not _is_http_seed(expanded):
                    continue
                entries.append(
                    SeedEntry(
                        seed_url=expanded,
                        sitemap_id=sitemap_id,
                        sitemap_file=sitemap_file,
                        source_path=path,
                        journal_name=journal_name,
                        metadata=dict(metadata),
                        adapter_config=dict(adapter_config),
                        status=status,
                        status_reason=_normalize_text(metadata.get("status_reason")),
                        status_updated_at=_normalize_text(metadata.get("status_updated_at")),
                        status_evidence_ref=_normalize_text(
                            metadata.get("status_evidence_ref")
                        ),
                        status_inferred=status_inferred,
                        has_selectors=has_selectors,
                    )
                )
    return entries


def filter_active(entries: Iterable[SeedEntry]) -> list[SeedEntry]:
    # Status markers are informational only; all sitemap entries are runnable.
    return list(entries)


def seed_context_by_url(entries: Iterable[SeedEntry]) -> dict[str, dict[str, Any]]:
    context: dict[str, dict[str, Any]] = {}
    for entry in entries:
        context.setdefault(
            entry.seed_url,
            {
                "seed_url": entry.seed_url,
                "journal_name": entry.journal_name,
                "sitemap_id": entry.sitemap_id,
                "sitemap_file": entry.sitemap_file,
                "adapter_config": dict(entry.adapter_config),
                "status": entry.status,
                "status_reason": entry.status_reason,
                "status_updated_at": entry.status_updated_at,
                "status_evidence_ref": entry.status_evidence_ref,
                "has_selectors": entry.has_selectors,
            },
        )
    return context
