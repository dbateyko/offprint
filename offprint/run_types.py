from __future__ import annotations

from typing import Any, Dict, Optional, TypedDict


class RunPaths(TypedDict):
    run_dir: str
    manifest_path: str
    records_path: str
    errors_path: str
    stats_path: str


class LegacyManifestRecord(TypedDict):
    page_url: str
    pdf_url: str
    local_path: Optional[str]
    ok: bool
    metadata: Dict[str, Any]


class GoldenRunPointer(TypedDict):
    run_id: str
    promoted_at: str  # ISO-8601
    notes: str
    coverage_gate_status: str  # "PASS" | "FAIL" | "SKIPPED"
    summary: Dict[str, Any]  # manifest summary snapshot


class DomainBaseline(TypedDict):
    best_pdf_count: int
    best_run_id: str
    confidence: str  # "HIGH" | "MEDIUM" | "LOW"
    last_updated: str  # ISO-8601
