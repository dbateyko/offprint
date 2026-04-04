import re
from typing import Any, Dict, List, Optional

from .normalization import (
    _article_key,
    _extract_journal_name,
    _normalize_journal_name,
    _parse_partial_date,
)

def _resolve_journal_name(
    *,
    seed: str,
    domain: str,
    records: List[Dict[str, Any]],
    seed_context: Optional[Dict[str, Any]],
) -> str:
    context_name = _normalize_journal_name((seed_context or {}).get("journal_name"))
    if context_name:
        return context_name

    for record in records:
        metadata = record.get("metadata")
        if not isinstance(metadata, dict):
            continue
        name = _extract_journal_name(metadata)
        if name:
            return name

    return _normalize_journal_name(domain or seed)

def _compute_seed_journal_summary(
    *,
    seed: str,
    domain: str,
    records: List[Dict[str, Any]],
    ok_total: int,
    seed_context: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    article_keys: set[str] = set()
    article_keys_with_date: set[str] = set()
    parsed_dates: List[Dict[str, Any]] = []

    for record in records:
        article_key = _article_key(record)
        article_keys.add(article_key)

        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        metadata = metadata or {}
        date_values: List[Any] = []
        for key in (
            "date",
            "year",
            "publication_date",
            "citation_publication_date",
            "citation_date",
        ):
            value = metadata.get(key)
            if isinstance(value, list):
                date_values.extend(value)
            elif value is not None:
                date_values.append(value)

        best_for_record: Optional[Dict[str, Any]] = None
        for candidate in date_values:
            parsed = _parse_partial_date(candidate)
            if not parsed:
                continue
            parsed_dates.append(parsed)
            if not best_for_record or parsed["start_key"] < best_for_record["start_key"]:
                best_for_record = parsed
        if best_for_record:
            article_keys_with_date.add(article_key)

    earliest = min(parsed_dates, key=lambda d: d["start_key"]) if parsed_dates else None
    latest = max(parsed_dates, key=lambda d: d["end_key"]) if parsed_dates else None
    ok_records = sum(1 for r in records if bool(r.get("ok")))

    if not records:
        ok_records = ok_total

    return {
        "journal_name": _resolve_journal_name(
            seed=seed,
            domain=domain,
            records=records,
            seed_context=seed_context,
        ),
        "seed_url": seed,
        "domain": domain,
        "pdf_records_total": len(records),
        "pdfs_downloaded_total": ok_records,
        "pdfs_failed_total": max(len(records) - ok_records, 0),
        "articles_total": len(article_keys),
        "articles_with_date_total": len(article_keys_with_date),
        "earliest_date": earliest["normalized"] if earliest else None,
        "latest_date": latest["normalized"] if latest else None,
        "earliest_year": earliest["year"] if earliest else None,
        "latest_year": latest["year"] if latest else None,
    }

def _journal_key(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", (name or "").strip().lower()).strip("_")
    return cleaned or "unknown"

def _merge_journal_stats(
    existing: Optional[Dict[str, Any]], incoming: Dict[str, Any]
) -> Dict[str, Any]:
    merged = dict(existing or {})
    merged.setdefault("journal_name", incoming.get("journal_name") or "")
    if incoming.get("journal_name"):
        merged["journal_name"] = incoming["journal_name"]

    merged["seeds"] = int(merged.get("seeds") or 0) + 1

    domain_list = set(merged.get("domains") or [])
    if incoming.get("domain"):
        domain_list.add(str(incoming["domain"]))
    merged["domains"] = sorted(domain_list)

    for key in (
        "pdf_records_total",
        "pdfs_downloaded_total",
        "pdfs_failed_total",
        "articles_total",
        "articles_with_date_total",
    ):
        merged[key] = int(merged.get(key) or 0) + int(incoming.get(key) or 0)

    incoming_earliest = _parse_partial_date(incoming.get("earliest_date"))
    existing_earliest = _parse_partial_date(merged.get("earliest_date"))
    if incoming_earliest and (
        not existing_earliest or incoming_earliest["start_key"] < existing_earliest["start_key"]
    ):
        merged["earliest_date"] = incoming_earliest["normalized"]

    incoming_latest = _parse_partial_date(incoming.get("latest_date"))
    existing_latest = _parse_partial_date(merged.get("latest_date"))
    if incoming_latest and (
        not existing_latest or incoming_latest["end_key"] > existing_latest["end_key"]
    ):
        merged["latest_date"] = incoming_latest["normalized"]

    years = [
        y for y in [merged.get("earliest_year"), incoming.get("earliest_year")] if y is not None
    ]
    merged["earliest_year"] = min(int(y) for y in years) if years else None
    years = [y for y in [merged.get("latest_year"), incoming.get("latest_year")] if y is not None]
    merged["latest_year"] = max(int(y) for y in years) if years else None

    return merged
