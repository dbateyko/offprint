import os
from typing import Any, Dict

from .io import _load_jsonl
from ..adapters.registry import pick_adapter_for

def _is_dc_record(record: Dict[str, Any]) -> bool:
    source_adapter = str(record.get("source_adapter") or "").lower()
    extraction_path = str(record.get("extraction_path") or "").lower()
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    dc_source = str((metadata or {}).get("dc_source") or "").lower()
    pdf_url = str(record.get("pdf_url") or "").lower()
    page_url = str(record.get("page_url") or "").lower()
    return (
        "digitalcommons" in source_adapter
        or "digital_commons" in source_adapter
        or "dc_" in extraction_path
        or dc_source in {"oai", "siteindex", "all_issues", "hybrid"}
        or "viewcontent.cgi" in pdf_url
        or "digitalcommons" in pdf_url
        or "digitalcommons" in page_url
    )

def _is_dc_adapter(adapter: Any) -> bool:
    if adapter is None:
        return False
    if callable(getattr(adapter, "configure_dc", None)):
        return True
    adapter_name = adapter.__class__.__name__.lower()
    return "digitalcommons" in adapter_name or "digital_commons" in adapter_name

def _count_ok_downloads_by_seed(records_path: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    if not os.path.exists(records_path):
        return counts
    for row in _load_jsonl(records_path):
        seed_url = str(row.get("seed_url") or "").strip()
        if not seed_url:
            continue
        if bool(row.get("ok")) and row.get("pdf_url"):
            counts[seed_url] = int(counts.get(seed_url, 0)) + 1
    return counts

def _seed_routes_to_dc(seed_url: str) -> bool:
    try:
        adapter = pick_adapter_for(seed_url, allow_generic=True)
    except Exception:
        return False
    return _is_dc_adapter(adapter)
