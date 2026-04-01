from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Set
from urllib.parse import urldefrag, urlparse


def normalize_url(url: str) -> str:
    # Strip fragments so the same PDF URL compares equal across crawlers.
    normalized, _frag = urldefrag(url.strip())
    return normalized


@dataclass(frozen=True)
class ManifestRecord:
    page_url: Optional[str]
    pdf_url: Optional[str]
    ok: bool
    metadata: Dict[str, Any]
    domain: Optional[str] = None


def iter_manifest_records(path: str) -> Iterator[ManifestRecord]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            yield ManifestRecord(
                page_url=obj.get("page_url"),
                pdf_url=obj.get("pdf_url"),
                ok=bool(obj.get("ok", True)),
                metadata=(obj.get("metadata") or {}),
                domain=obj.get("domain"),
            )


def load_manifest_pdf_urls(path: str, *, only_ok: bool = True) -> Set[str]:
    urls: Set[str] = set()
    for rec in iter_manifest_records(path):
        if only_ok and not rec.ok:
            continue
        if rec.pdf_url:
            urls.add(normalize_url(rec.pdf_url))
    return urls


def union_manifest_pdf_urls(paths: Iterable[str], *, only_ok: bool = True) -> Set[str]:
    all_urls: Set[str] = set()
    for p in paths:
        all_urls |= load_manifest_pdf_urls(p, only_ok=only_ok)
    return all_urls


def is_run_dir(path: str) -> bool:
    return os.path.isdir(path) and os.path.exists(os.path.join(path, "records.jsonl"))


def load_run_pdf_urls_by_domain(run_dir: str, *, only_ok: bool = True) -> Dict[str, Set[str]]:
    records_path = os.path.join(run_dir, "records.jsonl")
    by_domain: Dict[str, Set[str]] = {}
    if not os.path.exists(records_path):
        return by_domain

    for rec in iter_manifest_records(records_path):
        if only_ok and not rec.ok:
            continue
        if not rec.pdf_url:
            continue

        domain = (rec.domain or "").strip().lower()
        if not domain:
            domain = (urlparse(rec.pdf_url).netloc or "").lower()
        if not domain:
            continue

        by_domain.setdefault(domain, set()).add(normalize_url(rec.pdf_url))

    return by_domain


def load_run_structural_metrics_by_domain(run_dir: str) -> Dict[str, Dict[str, Any]]:
    stats_path = os.path.join(run_dir, "stats.json")
    if not os.path.exists(stats_path):
        return {}

    try:
        with open(stats_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return {}

    by_domain: Dict[str, Dict[str, Any]] = {}
    domains_payload = payload.get("domains")
    if isinstance(domains_payload, Mapping):
        for domain, domain_data in domains_payload.items():
            domain_key = str(domain or "").lower()
            if not domain_key:
                continue
            if isinstance(domain_data, Mapping):
                completeness = domain_data.get("completeness")
                if isinstance(completeness, Mapping):
                    by_domain[domain_key] = dict(completeness)

    seeds = payload.get("seeds")
    if isinstance(seeds, Mapping):
        for seed_payload in seeds.values():
            if not isinstance(seed_payload, Mapping):
                continue
            domain = str(seed_payload.get("domain") or "").lower()
            if not domain:
                continue
            completeness = seed_payload.get("completeness")
            if isinstance(completeness, Mapping):
                by_domain[domain] = dict(completeness)
    return by_domain
