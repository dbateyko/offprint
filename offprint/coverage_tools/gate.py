from __future__ import annotations

import glob
import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from .manifest import (
    is_run_dir,
    load_manifest_pdf_urls,
    load_run_pdf_urls_by_domain,
    load_run_structural_metrics_by_domain,
    normalize_url,
)


@dataclass
class SiteCoverage:
    domain: str
    seed_url: Optional[str]
    live_pdf_total: int
    sitemap_pdf_total: int
    probe_pdf_total: int
    manifest_pdf_total: int
    matched: int
    missing: List[str] = field(default_factory=list)
    status: str = "UNKNOWN"  # PASS | FAIL | JS_REQUIRED | LOW_CONFIDENCE | UNKNOWN
    notes: str = ""
    next_action: str = ""
    pdf_ratio: Optional[float] = None
    volume_gaps: List[str] = field(default_factory=list)
    issue_outliers: List[str] = field(default_factory=list)
    confidence: Optional[str] = None
    dc_oai_discovered: Optional[int] = None
    dc_sitemap_discovered: Optional[int] = None
    dc_union_unique: Optional[int] = None
    dc_pdf_blocked_count: Optional[int] = None
    dc_download_success_rate: Optional[float] = None


def _status_from_missing(missing: List[str], js_required: bool, probe_total: int) -> str:
    if js_required:
        return "JS_REQUIRED"
    if probe_total == 0:
        return "UNKNOWN"
    return "FAIL" if missing else "PASS"


def load_live_diff(path: str) -> Dict[str, Dict]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _manifest_paths_for_targets(manifest_dir: str, domains: Set[str]) -> Dict[str, str]:
    files = glob.glob(os.path.join(manifest_dir, "*.jsonl"))
    by_domain: Dict[str, str] = {}
    for fp in files:
        name = os.path.basename(fp)
        if not name.endswith(".jsonl"):
            continue
        domain = name[: -len(".jsonl")].replace("_", ":").lower()
        # Most manifests are netloc filenames; keep exact match only.
        if domain in domains:
            by_domain[domain] = fp
    return by_domain


def _resolve_manifest_sources(
    manifest_dir: str,
    domains: Set[str],
) -> Tuple[Dict[str, str], Dict[str, Set[str]], Dict[str, Dict[str, Any]]]:
    if is_run_dir(manifest_dir):
        run_urls = load_run_pdf_urls_by_domain(manifest_dir, only_ok=True)
        run_metrics = load_run_structural_metrics_by_domain(manifest_dir)
        # Keep only requested domains.
        run_urls = {k: v for k, v in run_urls.items() if k in domains}
        run_metrics = {k: v for k, v in run_metrics.items() if k in domains}
        return {}, run_urls, run_metrics

    manifest_index = _manifest_paths_for_targets(manifest_dir, domains)
    return manifest_index, {}, {}


def coverage_report(
    *,
    manifest_dir: str,
    live_diff_path: str,
    sitemap_diff_path: Optional[str] = None,
    targets: Optional[Set[str]] = None,
    max_missing_urls: int = 20,
) -> Dict:
    live = load_live_diff(live_diff_path)
    sites = live.get("sites") or {}

    sitemap: Dict[str, Dict] = {}
    if sitemap_diff_path:
        try:
            with open(sitemap_diff_path, "r", encoding="utf-8") as f:
                sitemap = json.load(f)
        except Exception:
            sitemap = {}
    sitemap_sites = sitemap.get("sites") or {}

    domains = set((targets or set(sites.keys())))
    # Keep only domains present in the live probe.
    domains &= set(sites.keys())

    manifest_index, run_manifest_urls, run_structural = _resolve_manifest_sources(
        manifest_dir, domains
    )

    report_sites: List[SiteCoverage] = []
    gate_fail = False

    for domain in sorted(domains):
        site = sites.get(domain) or {}
        seed_url = site.get("seed_url")
        live_js_required = bool(site.get("js_required", False))

        live_pdfs = {
            normalize_url(u) for u in (site.get("pdf_urls") or []) if isinstance(u, str) and u
        }
        live_total = len(live_pdfs)

        sm_site = sitemap_sites.get(domain) or {}
        sitemap_pdfs = {
            normalize_url(u) for u in (sm_site.get("pdf_urls") or []) if isinstance(u, str) and u
        }
        sitemap_total = len(sitemap_pdfs)

        probe_pdfs = live_pdfs | sitemap_pdfs
        probe_total = len(probe_pdfs)

        manifest_path = manifest_index.get(domain)
        manifest_pdfs: Set[str] = set()
        if domain in run_manifest_urls:
            manifest_pdfs = run_manifest_urls[domain]
        elif manifest_path and os.path.exists(manifest_path):
            manifest_pdfs = load_manifest_pdf_urls(manifest_path, only_ok=True)

        matched = len(probe_pdfs & manifest_pdfs) if probe_total else 0
        missing = sorted(probe_pdfs - manifest_pdfs)[:max_missing_urls]
        status = _status_from_missing(missing, live_js_required, probe_total)
        notes = ""
        if domain not in run_manifest_urls and not manifest_path:
            notes = "manifest missing"
        elif probe_total == 0 and not live_js_required:
            notes = "no probe PDFs detected (sample may be too small)"

        structural = run_structural.get(domain, {})
        pdf_ratio = structural.get("pdf_ratio")
        volume_gaps = list(structural.get("volume_gaps") or [])
        issue_outliers = list(structural.get("issue_outliers") or [])
        confidence = structural.get("confidence")
        dc_oai_discovered = structural.get("dc_oai_discovered")
        dc_sitemap_discovered = structural.get("dc_sitemap_discovered")
        dc_union_unique = structural.get("dc_union_unique")
        dc_pdf_blocked_count = structural.get("dc_pdf_blocked_count")
        dc_download_success_rate = structural.get("dc_download_success_rate")

        if status == "PASS" and str(confidence or "").upper() == "LOW":
            status = "LOW_CONFIDENCE"

        next_action = ""
        if status == "JS_REQUIRED":
            next_action = "use playwright probe / increase JS handling"
        elif status == "FAIL":
            if len(manifest_pdfs) == 0 and probe_total > 0:
                next_action = "seed likely wrong or adapter returned zero"
            else:
                next_action = "investigate missing PDF URLs"
        elif status == "LOW_CONFIDENCE":
            next_action = "investigate structural completeness warnings"

        if status in {"FAIL", "JS_REQUIRED"}:
            gate_fail = True

        report_sites.append(
            SiteCoverage(
                domain=domain,
                seed_url=seed_url,
                live_pdf_total=live_total,
                sitemap_pdf_total=sitemap_total,
                probe_pdf_total=probe_total,
                manifest_pdf_total=len(manifest_pdfs),
                matched=matched,
                missing=missing,
                status=status,
                notes=notes,
                next_action=next_action,
                pdf_ratio=pdf_ratio if isinstance(pdf_ratio, (int, float)) else None,
                volume_gaps=volume_gaps,
                issue_outliers=issue_outliers,
                confidence=str(confidence) if confidence else None,
                dc_oai_discovered=int(dc_oai_discovered)
                if isinstance(dc_oai_discovered, (int, float))
                else None,
                dc_sitemap_discovered=int(dc_sitemap_discovered)
                if isinstance(dc_sitemap_discovered, (int, float))
                else None,
                dc_union_unique=int(dc_union_unique)
                if isinstance(dc_union_unique, (int, float))
                else None,
                dc_pdf_blocked_count=int(dc_pdf_blocked_count)
                if isinstance(dc_pdf_blocked_count, (int, float))
                else None,
                dc_download_success_rate=float(dc_download_success_rate)
                if isinstance(dc_download_success_rate, (int, float))
                else None,
            )
        )

    return {
        "manifest_dir": manifest_dir,
        "live_diff": live_diff_path,
        "sitemap_diff": sitemap_diff_path,
        "targets": sorted(domains),
        "sites": [asdict(s) for s in report_sites],
        "gate_status": "FAIL" if gate_fail else "PASS",
    }
