from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import queue
import random
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional
from urllib.parse import urlparse

from .adapters import UnmappedAdapterError, pick_adapter_for
from .adapters.utils import compute_pdf_sha256_and_size, validate_pdf_magic_bytes
from .coverage_tools.sequence_validator import (
    compute_pdf_ratio,
    detect_issue_count_outliers,
    detect_issue_gaps,
    detect_volume_gaps,
    journal_confidence,
)
from .http_cache import HttpSnapshotCache
from .legacy_manifest import should_write_legacy_manifests
from .path_policy import warn_legacy_paths
from .polite_requests import PoliteRequestsSession
from .run_types import LegacyManifestRecord, RunPaths
from .seed_catalog import (
    SEED_STATUS_ACTIVE,
    filter_active as filter_active_seed_entries,
    load_seed_entries as load_seed_catalog_entries,
    seed_context_by_url as seed_catalog_context_by_url,
)

from .pipeline.io import (
    ensure_dir,
    _count_existing_pdfs_by_domain,
    _env_flag,
    _utc_now_iso,
    _default_run_id,
    _git_commit,
    _read_json,
    _write_json,
    _append_jsonl,
    _write_jsonl_atomic,
    _load_jsonl,
)

from .pipeline.normalization import (
    _normalize_metadata,
    _normalize_text,
    _normalize_journal_name,
    _extract_journal_name,
    _normalize_adapter_config,
    _coerce_bool,
    _coerce_int,
    _seed_dc_overrides,
    _article_key,
    _parse_partial_date,
)

from .pipeline.stats import (
    _resolve_journal_name,
    _compute_seed_journal_summary,
    _journal_key,
    _merge_journal_stats,
)

from .pipeline.network import (
    _download_with_retries,
    _response_meta,
)

from .pipeline.waf_utils import (
    _count_waf_stats,
    _count_curl_cffi_downloads,
    _collect_waf_blocked_urls,
    _process_waf_browser_fallback,
)

from .pipeline.sitemaps import (
    _is_http_seed,
    read_sitemaps_csv,
    read_sitemaps_dir,
    _seed_checkpoint_path,
    _load_completed_issue_urls,
)

from .pipeline.dc_utils import (
    _is_dc_record,
    _is_dc_adapter,
    _count_ok_downloads_by_seed,
    _seed_routes_to_dc,
)
from .pipeline.browser_utils import _download_single_with_browser

LEGACY_DEFAULT_PATHS = {
    "out_dir": "pdfs",
    "manifest_dir": "runs",
    "cache_dir": "cache/http",
}


@dataclass
class SeedProcessResult:
    seed: str
    domain: str
    records_total: int
    ok_total: int
    errors_total: int
    status: str
    completeness: Dict[str, Any]
    journal_summary: Dict[str, Any]


def process_seed(
    *,
    seed: str,
    out_dir: str,
    legacy_manifest_dir: str,
    run_records_path: str,
    run_errors_path: str,
    links_only: bool,
    discovery_only: bool,
    use_playwright: bool,
    playwright_headed: bool,
    max_depth: int,
    min_delay: float,
    max_delay: float,
    snapshot_cache: Optional[HttpSnapshotCache],
    dc_enum_mode: str,
    dc_use_siteindex: bool,
    dc_ua_fallback_profiles: List[str],
    dc_robots_enforce: bool,
    dc_max_oai_records: int,
    dc_max_sitemap_urls: int,
    dc_download_timeout: int,
    dc_min_domain_delay_ms: int,
    dc_max_domain_delay_ms: int,
    dc_waf_fail_threshold: int,
    dc_waf_cooldown_seconds: int,
    dc_disable_unscoped_oai_no_slug: bool,
    dc_session_rotate_threshold: int,
    dc_use_curl_cffi: bool,
    dc_round_robin_downloads: bool,
    write_legacy_manifests: bool,
    seed_context: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    issue_checkpoint_dir: Optional[str] = None,
    shared_session: Optional[Any] = None,
) -> SeedProcessResult:
    domain = urlparse(seed).netloc or "unknown"
    print(f"▶️  Starting {domain}...", flush=True)
    verbose_pdf_log = _env_flag("LRS_VERBOSE_PDF_LOG", default=False)
    seed_started_at = time.time()
    heartbeat_interval_s = 30.0
    heartbeat_raw = str(os.getenv("LRS_SEED_HEARTBEAT_SECONDS", "")).strip()
    if heartbeat_raw:
        try:
            heartbeat_interval_s = max(float(heartbeat_raw), 5.0)
        except ValueError:
            heartbeat_interval_s = 30.0

    progress_lock = threading.Lock()
    progress_state: Dict[str, Any] = {
        "stage": "initializing",
        "discovered": 0,
        "downloaded": 0,
        "failed_downloads": 0,
    }
    heartbeat_stop = threading.Event()
    adapter_name = "unknown"

    def _emit_progress(event: str, **payload: Any) -> None:
        if not callable(progress_callback):
            return
        evt = {
            "event": event,
            "seed": seed,
            "domain": domain,
            "timestamp": time.time(),
        }
        evt.update(payload)
        try:
            progress_callback(evt)
        except Exception:
            pass

    def _set_progress(**updates: Any) -> None:
        with progress_lock:
            progress_state.update(updates)
            snapshot = dict(progress_state)
        _emit_progress("seed_progress", **snapshot)

    def _inc_progress(key: str, delta: int = 1) -> None:
        with progress_lock:
            progress_state[key] = int(progress_state.get(key) or 0) + int(delta)

    def _heartbeat() -> None:
        while not heartbeat_stop.wait(timeout=heartbeat_interval_s):
            elapsed = max(time.time() - seed_started_at, 0.0)
            with progress_lock:
                stage = str(progress_state.get("stage") or "running")
                discovered = int(progress_state.get("discovered") or 0)
                downloaded = int(progress_state.get("downloaded") or 0)
                failed_downloads = int(progress_state.get("failed_downloads") or 0)
            print(
                f"⏱️  Seed heartbeat {domain}: elapsed={elapsed:.0f}s stage={stage} "
                f"discovered={discovered} downloaded={downloaded} failed={failed_downloads} "
                f"adapter={adapter_name}",
                flush=True,
            )
            _emit_progress(
                "seed_heartbeat",
                stage=stage,
                discovered=discovered,
                downloaded=downloaded,
                failed_downloads=failed_downloads,
                adapter=adapter_name,
                elapsed_seconds=elapsed,
            )

    _emit_progress("seed_start", stage="initializing")
    heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True)
    heartbeat_thread.start()

    _owns_session = shared_session is None
    if shared_session is not None:
        session = shared_session
    elif use_playwright:
        from .playwright_session import PlaywrightSession

        session = PlaywrightSession(
            min_delay=min_delay,
            max_delay=max_delay,
            headless=not playwright_headed,
        )
    else:
        session = PoliteRequestsSession(
            min_delay=min_delay,
            max_delay=max_delay,
            snapshot_cache=snapshot_cache,
        )

    discovered_records: List[Dict[str, Any]] = []
    ok_total = 0
    errors_total = 0
    dc_oai_discovered = 0
    dc_sitemap_discovered = 0
    dc_all_issues_discovered = 0
    dc_pdf_blocked_count = 0
    dc_download_deferred_count = 0

    issue_completed_count = 0
    issue_skipped_on_resume = 0
    checkpoint_path = ""

    try:
        _set_progress(stage="selecting_adapter")
        allow_generic_fallback = bool((seed_context or {}).get("has_selectors"))
        adapter = pick_adapter_for(seed, session=session, allow_generic=allow_generic_fallback)
        adapter_name = adapter.__class__.__name__
        is_dc_seed = _is_dc_adapter(adapter)
        defer_dc_downloads = bool(
            dc_round_robin_downloads and is_dc_seed and not links_only and not discovery_only
        )
        _set_progress(stage="configuring_adapter")
        configure_dc = getattr(adapter, "configure_dc", None)
        if callable(configure_dc):
            dc_config: Dict[str, Any] = {
                "enum_mode": dc_enum_mode,
                "use_siteindex": dc_use_siteindex,
                "ua_profiles": dc_ua_fallback_profiles,
                "robots_enforce": dc_robots_enforce,
                "max_oai_records": dc_max_oai_records,
                "max_sitemap_urls": dc_max_sitemap_urls,
                "download_timeout": dc_download_timeout,
                "min_domain_delay_ms": dc_min_domain_delay_ms,
                "max_domain_delay_ms": dc_max_domain_delay_ms,
                "waf_fail_threshold": dc_waf_fail_threshold,
                "waf_cooldown_seconds": dc_waf_cooldown_seconds,
                "disable_unscoped_oai_no_slug": dc_disable_unscoped_oai_no_slug,
                "allow_generic_fallback": allow_generic_fallback,
                "session_rotate_threshold": dc_session_rotate_threshold,
                "use_curl_cffi": dc_use_curl_cffi,
            }
            seed_overrides = _seed_dc_overrides(seed_context)
            if seed_overrides:
                dc_config.update(seed_overrides)
            configure_dc(**dc_config)

        if issue_checkpoint_dir:
            ensure_dir(issue_checkpoint_dir)
            checkpoint_path = _seed_checkpoint_path(issue_checkpoint_dir, seed)
            completed_issues = _load_completed_issue_urls(checkpoint_path)
            issue_skipped_on_resume = len(completed_issues)
            configure_issue_checkpoint = getattr(adapter, "configure_issue_checkpoint", None)
            if callable(configure_issue_checkpoint):

                def _on_issue_complete(issue_url: str, emitted_count: int) -> None:
                    nonlocal issue_completed_count
                    issue_completed_count += 1
                    _append_jsonl(
                        checkpoint_path,
                        {
                            "seed_url": seed,
                            "domain": domain,
                            "issue_url": issue_url,
                            "status": "completed",
                            "emitted_count": int(emitted_count or 0),
                            "completed_at": _utc_now_iso(),
                        },
                    )

                configure_issue_checkpoint(
                    skip_issue_urls=completed_issues,
                    on_issue_complete=_on_issue_complete,
                )

        _set_progress(stage="discovering")
        legacy_manifest_path = os.path.join(
            legacy_manifest_dir, f"{domain.replace(':', '_')}.jsonl"
        )
        first_result_elapsed: Optional[float] = None
        stop_seed_on_subscription = False

        for result in adapter.discover_pdfs(seed, max_depth=max_depth):
            _set_progress(stage="processing_result")
            _inc_progress("discovered", 1)
            if first_result_elapsed is None:
                first_result_elapsed = max(time.time() - seed_started_at, 0.0)
                print(
                    f"📥 First PDF candidate for {domain} after {first_result_elapsed:.1f}s",
                    flush=True,
                )
                _emit_progress(
                    "first_pdf_candidate",
                    elapsed_seconds=first_result_elapsed,
                    adapter=adapter_name,
                )
            metadata = _normalize_metadata(getattr(result, "metadata", {}) or {})
            page_meta = _response_meta(session, result.page_url)

            source_adapter = result.source_adapter or adapter_name
            extraction_path = result.extraction_path or "unknown"
            retrieved_at = result.retrieved_at or str(
                page_meta.get("retrieved_at") or _utc_now_iso()
            )
            http_status = int(result.http_status or page_meta.get("status") or 0)
            content_type = result.content_type or str(page_meta.get("content_type") or "")
            dc_source = str(metadata.get("dc_source") or "").strip().lower()
            if dc_source not in {"oai", "siteindex", "all_issues", "hybrid"}:
                path = (extraction_path or "").lower()
                if "oai" in path:
                    dc_source = "oai"
                elif "siteindex" in path or "sitemap" in path:
                    dc_source = "siteindex"
                else:
                    dc_source = "all_issues"
                metadata["dc_source"] = dc_source
            metadata.setdefault("dc_set_spec", "")

            if dc_source in {"oai", "hybrid"}:
                dc_oai_discovered += 1
            if dc_source in {"siteindex", "hybrid"}:
                dc_sitemap_discovered += 1
            if dc_source == "all_issues":
                dc_all_issues_discovered += 1

            local_path: Optional[str] = None
            pdf_sha256 = result.pdf_sha256
            pdf_size_bytes = result.pdf_size_bytes
            ok = True
            error_type = ""
            error_message = ""
            retries = 0
            ua_profile_used = ""
            robots_allowed = None
            download_status_class = "ok"
            blocked_reason = ""

            if not (links_only or discovery_only or defer_dc_downloads):
                _set_progress(stage="downloading")
                error_context = {
                    "seed_url": seed,
                    "domain": domain,
                    "page_url": result.page_url,
                    "pdf_url": result.pdf_url,
                    "source_adapter": source_adapter,
                    "extraction_path": extraction_path,
                    "metadata": metadata,
                    "dc_source": dc_source,
                }
                download_outcome = _download_with_retries(
                    adapter=adapter,
                    pdf_url=result.pdf_url,
                    out_dir=os.path.join(out_dir, domain),
                    errors_path=run_errors_path,
                    error_context=error_context,
                    max_attempts=3,
                )
                local_path = download_outcome.get("local_path")
                ok = bool(download_outcome.get("ok", False))
                pdf_sha256 = download_outcome.get("pdf_sha256")
                pdf_size_bytes = download_outcome.get("pdf_size_bytes")
                retries = int(download_outcome.get("retries") or 0)
                ua_profile_used = str(download_outcome.get("ua_profile_used") or "")
                robots_allowed = download_outcome.get("robots_allowed")
                download_status_class = str(
                    download_outcome.get("download_status_class") or "network"
                )
                blocked_reason = str(download_outcome.get("blocked_reason") or "")
                if download_outcome.get("content_type"):
                    content_type = str(download_outcome["content_type"])
                if int(download_outcome.get("http_status") or 0):
                    http_status = int(download_outcome["http_status"])
                if not ok:
                    errors_total += 1
                    error_type = str(download_outcome.get("error_type") or "download_failed")
                    error_message = str(
                        download_outcome.get("error_message") or "PDF download failed"
                    )
                    if error_type == "subscription_blocked":
                        stop_seed_on_subscription = True
                    _emit_progress(
                        "pdf_failed",
                        pdf_url=result.pdf_url,
                        error_type=error_type,
                        error_message=error_message,
                        download_status_class=download_status_class,
                    )
                    if download_status_class in {"blocked_waf", "blocked_robots"}:
                        dc_pdf_blocked_count += 1
            elif defer_dc_downloads:
                dc_download_deferred_count += 1

            if local_path:
                relative_path = os.path.relpath(local_path, out_dir)
                metadata["pdf_relative_path"] = relative_path
                metadata["pdf_filename"] = os.path.basename(local_path)

            is_pending = bool(discovery_only or defer_dc_downloads)

            record = {
                "seed_url": seed,
                "domain": domain,
                "page_url": result.page_url,
                "pdf_url": result.pdf_url,
                "local_path": local_path,
                "ok": ok if not (links_only or is_pending) else (True if links_only else False),
                "metadata": metadata,
                "source_adapter": source_adapter,
                "extraction_path": extraction_path,
                "retrieved_at": retrieved_at,
                "http_status": http_status,
                "content_type": content_type,
                "pdf_sha256": pdf_sha256,
                "pdf_size_bytes": pdf_size_bytes,
                "retries": retries,
                "error_type": error_type,
                "error_message": error_message,
                "dc_source": dc_source,
                "dc_set_spec": str(metadata.get("dc_set_spec") or ""),
                "ua_profile_used": ua_profile_used,
                "robots_allowed": robots_allowed,
                "download_status_class": download_status_class,
                "blocked_reason": blocked_reason,
                "is_dc_seed": is_dc_seed,
                "download_state": (
                    "downloaded"
                    if local_path
                    else (
                        "pending"
                        if is_pending
                        else ("skipped_links_only" if links_only else "failed")
                    )
                ),
                "download_attempts": 0 if (links_only or is_pending) else int(retries or 0),
                "download_transport": (
                    ""
                    if (links_only or is_pending)
                    else str(
                        getattr(adapter, "last_download_meta", {}).get("download_method")
                        or "requests"
                    )
                ),
                "download_updated_at": _utc_now_iso(),
            }
            _append_jsonl(run_records_path, record)

            legacy_record: LegacyManifestRecord = {
                "page_url": result.page_url,
                "pdf_url": result.pdf_url,
                "local_path": local_path,
                "ok": True if links_only else (False if discovery_only else bool(local_path)),
                "metadata": metadata,
            }
            if write_legacy_manifests:
                _append_jsonl(legacy_manifest_path, legacy_record)

            discovered_records.append(record)
            if links_only or local_path:
                ok_total += 1
                if local_path:
                    _emit_progress(
                        "pdf_saved",
                        pdf_url=result.pdf_url,
                        local_path=local_path,
                    )
                if verbose_pdf_log:
                    path_display = local_path or "(links-only)"
                    print(
                        f"📄 PDF {domain} -> {path_display} ({result.pdf_url})",
                        flush=True,
                    )
            _set_progress(
                stage="discovering",
                downloaded=ok_total,
                failed_downloads=errors_total,
            )
            if stop_seed_on_subscription:
                print(
                    f"⛔ [dc] Stopping seed early due to subscription/login wall for {seed}",
                    flush=True,
                )
                break

        volume_gaps = detect_volume_gaps(discovered_records)
        issue_gaps = detect_issue_gaps(discovered_records)
        issue_outliers = detect_issue_count_outliers(discovered_records, threshold=0.5)
        pdf_ratio = compute_pdf_ratio(discovered_records)
        completeness = {
            "volume_gaps": volume_gaps,
            "issue_gaps": issue_gaps,
            "issue_outliers": issue_outliers,
            "pdf_ratio": round(pdf_ratio, 3),
            "dc_oai_discovered": dc_oai_discovered,
            "dc_sitemap_discovered": dc_sitemap_discovered,
            "dc_all_issues_discovered": dc_all_issues_discovered,
            "dc_union_unique": len(discovered_records),
            "dc_pdf_blocked_count": dc_pdf_blocked_count,
            "dc_download_deferred_count": dc_download_deferred_count,
            "dc_issues_completed": issue_completed_count,
            "dc_issues_skipped_on_resume": issue_skipped_on_resume,
            "dc_download_success_rate": (
                round((ok_total / len(discovered_records)), 3) if discovered_records else 0.0
            ),
        }
        completeness["confidence"] = journal_confidence(completeness)

        for warning in volume_gaps + issue_gaps + issue_outliers:
            _append_jsonl(
                run_errors_path,
                {
                    "seed_url": seed,
                    "domain": domain,
                    "page_url": seed,
                    "pdf_url": None,
                    "source_adapter": adapter_name,
                    "extraction_path": "sequence_validator",
                    "metadata": {},
                    "error_type": "completeness_warning",
                    "message": warning,
                    "http_status": 0,
                    "attempt": 0,
                    "retries": 0,
                    "retrieved_at": _utc_now_iso(),
                },
            )

        return SeedProcessResult(
            seed=seed,
            domain=domain,
            records_total=len(discovered_records),
            ok_total=ok_total,
            errors_total=errors_total,
            status="completed",
            completeness=completeness,
            journal_summary=_compute_seed_journal_summary(
                seed=seed,
                domain=domain,
                records=discovered_records,
                ok_total=ok_total,
                seed_context=seed_context,
            ),
        )
    except UnmappedAdapterError as exc:
        _set_progress(stage="blocked_todo_adapter")
        _append_jsonl(
            run_errors_path,
            {
                "seed_url": seed,
                "domain": domain,
                "page_url": seed,
                "pdf_url": None,
                "source_adapter": "orchestrator",
                "extraction_path": "seed",
                "metadata": {},
                "error_type": "todo_adapter_blocked",
                "message": str(exc),
                "http_status": 0,
                "attempt": 1,
                "retries": 0,
                "blocked_host": exc.host,
                "retrieved_at": _utc_now_iso(),
            },
        )
        return SeedProcessResult(
            seed=seed,
            domain=domain,
            records_total=len(discovered_records),
            ok_total=ok_total,
            errors_total=errors_total + 1,
            status="todo_adapter_blocked",
            completeness={
                "volume_gaps": [],
                "issue_gaps": [],
                "issue_outliers": [],
                "pdf_ratio": 0.0,
                "confidence": "LOW",
            },
            journal_summary=_compute_seed_journal_summary(
                seed=seed,
                domain=domain,
                records=discovered_records,
                ok_total=ok_total,
                seed_context=seed_context,
            ),
        )

    except Exception as exc:
        _set_progress(stage="failed")
        _append_jsonl(
            run_errors_path,
            {
                "seed_url": seed,
                "domain": domain,
                "page_url": seed,
                "pdf_url": None,
                "source_adapter": "orchestrator",
                "extraction_path": "seed",
                "metadata": {},
                "error_type": "seed_failure",
                "message": str(exc),
                "http_status": 0,
                "attempt": 1,
                "retries": 0,
                "retrieved_at": _utc_now_iso(),
            },
        )
        return SeedProcessResult(
            seed=seed,
            domain=domain,
            records_total=len(discovered_records),
            ok_total=ok_total,
            errors_total=errors_total + 1,
            status="failed",
            completeness={
                "volume_gaps": [],
                "issue_gaps": [],
                "issue_outliers": [],
                "pdf_ratio": 0.0,
                "confidence": "LOW",
            },
            journal_summary=_compute_seed_journal_summary(
                seed=seed,
                domain=domain,
                records=discovered_records,
                ok_total=ok_total,
                seed_context=seed_context,
            ),
        )
    finally:
        final_stage = "completed"
        with progress_lock:
            if str(progress_state.get("stage") or "") == "failed":
                final_stage = "failed"
            discovered_final = int(progress_state.get("discovered") or 0)
            downloaded_final = int(progress_state.get("downloaded") or 0)
            failed_final = int(progress_state.get("failed_downloads") or 0)
        _emit_progress(
            "seed_done",
            stage=final_stage,
            discovered=discovered_final,
            downloaded=downloaded_final,
            failed_downloads=failed_final,
            adapter=adapter_name,
        )
        heartbeat_stop.set()
        if heartbeat_thread.is_alive():
            heartbeat_thread.join(timeout=0.5)
        if _owns_session and hasattr(session, "close"):
            try:
                session.close()
            except Exception:
                pass


def _resolve_runtime_capabilities(
    *,
    dc_use_curl_cffi: bool,
    dc_waf_browser_fallback: bool,
    dc_browser_backend: str,
) -> Dict[str, Any]:
    from .digital_commons_download import CURL_CFFI_AVAILABLE
    from .mcp_browser_download import CAMOUFOX_AVAILABLE, PLAYWRIGHT_AVAILABLE

    npx_available = bool(shutil.which("npx"))

    browser_backend_requested = str(dc_browser_backend or "auto")
    browser_backend_selected = browser_backend_requested
    if browser_backend_selected == "auto":
        browser_backend_selected = "camoufox" if CAMOUFOX_AVAILABLE else "playwright"

    browser_backend_available = False
    browser_backend_reason = ""
    if browser_backend_selected == "camoufox":
        browser_backend_available = bool(CAMOUFOX_AVAILABLE)
        if not browser_backend_available:
            browser_backend_reason = (
                "Camoufox backend requested but package is not installed "
                "(install with: pip install 'camoufox[geoip]')."
            )
    elif browser_backend_selected == "playwright":
        browser_backend_available = bool(PLAYWRIGHT_AVAILABLE)
        if not browser_backend_available:
            browser_backend_reason = (
                "Playwright backend requested but Playwright is not installed "
                "(install with: pip install playwright and playwright install chromium)."
            )
    elif browser_backend_selected == "chrome_mcp":
        browser_backend_available = npx_available
        if not browser_backend_available:
            browser_backend_reason = (
                "Chrome MCP backend requires `npx` on PATH."
            )
    else:
        browser_backend_reason = f"Unsupported browser backend: {browser_backend_selected}"

    if dc_waf_browser_fallback and not browser_backend_available:
        detail = browser_backend_reason or "No available browser backend."
        raise ValueError(f"Cannot enable --dc-waf-browser-fallback: {detail}")

    return {
        "curl_cffi_available": bool(CURL_CFFI_AVAILABLE),
        "curl_cffi_enabled": bool(dc_use_curl_cffi and CURL_CFFI_AVAILABLE),
        "camoufox_available": bool(CAMOUFOX_AVAILABLE),
        "playwright_available": bool(PLAYWRIGHT_AVAILABLE),
        "npx_available": bool(npx_available),
        "browser_backend_requested": browser_backend_requested,
        "browser_backend_selected": browser_backend_selected,
        "browser_backend_available": bool(browser_backend_available),
        "browser_fallback_enabled": bool(dc_waf_browser_fallback and browser_backend_available),
    }


def _build_operator_intervention_config(
    *,
    operator_mode: bool,
    operator_intervention_scope: str,
    operator_wait_mode: str,
    operator_manual_retries: int,
    run_id: str,
    run_dir: str,
) -> Dict[str, Any]:
    scope = str(operator_intervention_scope or "off").strip().lower()
    if scope not in {"off", "browser_fallback_only"}:
        scope = "off"
    wait_mode = str(operator_wait_mode or "off").strip().lower()
    if wait_mode not in {"prompt_enter", "off"}:
        wait_mode = "off"
    retries = max(int(operator_manual_retries or 0), 0)
    enabled = bool(
        operator_mode
        and scope == "browser_fallback_only"
        and wait_mode == "prompt_enter"
        and retries > 0
    )
    return {
        "enabled": enabled,
        "scope": scope,
        "wait_mode": wait_mode,
        "manual_retries": retries,
        "run_id": run_id,
        "events_path": os.path.join(run_dir, "operator_events.jsonl"),
    }


def _initialize_run(
    *,
    manifest_dir: str,
    out_dir: str,
    run_id: str,
    links_only: bool,
    discovery_only: bool,
    max_depth: int,
    min_delay: float,
    max_delay: float,
    max_workers: int,
    use_playwright: bool,
    playwright_headed: bool,
    is_resume: bool,
    dc_enum_mode: str,
    dc_use_siteindex: bool,
    dc_ua_fallback_profiles: str,
    dc_robots_enforce: bool,
    dc_max_oai_records: int,
    dc_max_sitemap_urls: int,
    dc_download_timeout: int,
    dc_min_domain_delay_ms: int,
    dc_max_domain_delay_ms: int,
    dc_waf_fail_threshold: int,
    dc_waf_cooldown_seconds: int,
    dc_disable_unscoped_oai_no_slug: bool,
    dc_session_rotate_threshold: int,
    dc_use_curl_cffi: bool,
    dc_waf_browser_fallback: bool,
    dc_browser_backend: str,
    dc_browser_staging_dir: str,
    dc_browser_user_data_dir: str,
    dc_browser_timeout: int,
    dc_browser_headless: bool,
    skip_well_covered_seeds: bool,
    skip_dc_sites: bool,
    well_covered_pdf_threshold: int,
    max_consecutive_seed_failures_per_domain: int,
    dc_round_robin_downloads: bool,
    dc_round_robin_strict_first_pass: bool,
    dc_round_robin_revisit_interval_seconds: int,
    operator_mode: bool,
    operator_intervention_scope: str,
    operator_wait_mode: str,
    operator_manual_retries: int,
    write_legacy_manifests: bool,
    runtime_capabilities: Optional[Dict[str, Any]] = None,
) -> RunPaths:
    run_dir = os.path.join(manifest_dir, run_id)
    ensure_dir(run_dir)

    manifest_path = os.path.join(run_dir, "manifest.json")
    records_path = os.path.join(run_dir, "records.jsonl")
    errors_path = os.path.join(run_dir, "errors.jsonl")
    stats_path = os.path.join(run_dir, "stats.json")

    if is_resume:
        manifest = _read_json(manifest_path, {})
        if not manifest:
            raise ValueError(f"Cannot resume missing run: {run_id}")
        if manifest.get("status") == "completed":
            raise ValueError(f"Run {run_id} is already completed")
        manifest["status"] = "running"
        manifest["resumed_at"] = _utc_now_iso()
        manifest["runtime_capabilities"] = dict(runtime_capabilities or {})
        config = manifest.setdefault("config", {})
        if isinstance(config, dict):
            config["write_legacy_manifests"] = bool(write_legacy_manifests)
        _write_json(manifest_path, manifest)
        if not os.path.exists(records_path):
            open(records_path, "a", encoding="utf-8").close()
        if not os.path.exists(errors_path):
            open(errors_path, "a", encoding="utf-8").close()
        if not os.path.exists(stats_path):
            _write_json(
                stats_path,
                {
                    "run_id": run_id,
                    "seeds": {},
                    "domains": {},
                    "journals": {},
                    "updated_at": _utc_now_iso(),
                },
            )
    else:
        if os.path.exists(manifest_path):
            raise ValueError(
                f"Run directory already exists for run_id={run_id}. Use --resume {run_id} to continue."
            )
        manifest = {
            "run_id": run_id,
            "started_at": _utc_now_iso(),
            "finished_at": None,
            "status": "running",
            "argv": sys.argv,
            "python_version": sys.version,
            "git_commit": _git_commit(),
            "config": {
                "links_only": links_only,
                "discovery_only": discovery_only,
                "max_depth": max_depth,
                "min_delay": min_delay,
                "max_delay": max_delay,
                "max_workers": max_workers,
                "use_playwright": use_playwright,
                "playwright_headed": playwright_headed,
                "out_dir": out_dir,
                "manifest_dir": manifest_dir,
                "dc_enum_mode": dc_enum_mode,
                "dc_use_siteindex": dc_use_siteindex,
                "dc_ua_fallback_profiles": dc_ua_fallback_profiles,
                "dc_robots_enforce": dc_robots_enforce,
                "dc_max_oai_records": dc_max_oai_records,
                "dc_max_sitemap_urls": dc_max_sitemap_urls,
                "dc_download_timeout": dc_download_timeout,
                "dc_min_domain_delay_ms": dc_min_domain_delay_ms,
                "dc_max_domain_delay_ms": dc_max_domain_delay_ms,
                "dc_waf_fail_threshold": dc_waf_fail_threshold,
                "dc_waf_cooldown_seconds": dc_waf_cooldown_seconds,
                "dc_disable_unscoped_oai_no_slug": dc_disable_unscoped_oai_no_slug,
                "dc_session_rotate_threshold": dc_session_rotate_threshold,
                "dc_use_curl_cffi": dc_use_curl_cffi,
                "dc_waf_browser_fallback": dc_waf_browser_fallback,
                "dc_browser_backend": dc_browser_backend,
                "dc_browser_staging_dir": dc_browser_staging_dir,
                "dc_browser_user_data_dir": dc_browser_user_data_dir,
                "dc_browser_timeout": dc_browser_timeout,
                "dc_browser_headless": dc_browser_headless,
                "skip_well_covered_seeds": skip_well_covered_seeds,
                "skip_dc_sites": skip_dc_sites,
                "well_covered_pdf_threshold": well_covered_pdf_threshold,
                "max_consecutive_seed_failures_per_domain": max_consecutive_seed_failures_per_domain,
                "dc_round_robin_downloads": dc_round_robin_downloads,
                "dc_round_robin_strict_first_pass": dc_round_robin_strict_first_pass,
                "dc_round_robin_revisit_interval_seconds": dc_round_robin_revisit_interval_seconds,
                "operator_mode": operator_mode,
                "operator_intervention_scope": operator_intervention_scope,
                "operator_wait_mode": operator_wait_mode,
                "operator_manual_retries": operator_manual_retries,
                "write_legacy_manifests": bool(write_legacy_manifests),
                "runtime_capabilities": dict(runtime_capabilities or {}),
            },
            "runtime_capabilities": dict(runtime_capabilities or {}),
        }
        _write_json(manifest_path, manifest)
        open(records_path, "a", encoding="utf-8").close()
        open(errors_path, "a", encoding="utf-8").close()
        _write_json(
            stats_path,
            {
                "run_id": run_id,
                "seeds": {},
                "domains": {},
                "journals": {},
                "updated_at": _utc_now_iso(),
            },
        )

    return RunPaths(
        run_dir=run_dir,
        manifest_path=manifest_path,
        records_path=records_path,
        errors_path=errors_path,
        stats_path=stats_path,
    )



def _update_manifest_record_with_outcome(
    *,
    record: Dict[str, Any],
    outcome: Dict[str, Any],
    transport: str,
    out_dir: str,
) -> Dict[str, Any]:
    updated = dict(record)
    local_path = outcome.get("local_path")
    ok = bool(outcome.get("ok"))
    if local_path:
        metadata = updated.get("metadata") if isinstance(updated.get("metadata"), dict) else {}
        metadata = dict(metadata or {})
        metadata["pdf_relative_path"] = os.path.relpath(str(local_path), out_dir)
        metadata["pdf_filename"] = os.path.basename(str(local_path))
        updated["metadata"] = metadata
    updated["local_path"] = local_path
    updated["ok"] = ok
    updated["pdf_sha256"] = outcome.get("pdf_sha256")
    updated["pdf_size_bytes"] = outcome.get("pdf_size_bytes")
    updated["http_status"] = int(outcome.get("http_status") or 0)
    updated["content_type"] = str(outcome.get("content_type") or "")
    updated["error_type"] = "" if ok else str(outcome.get("error_type") or "download_failed")
    updated["error_message"] = (
        "" if ok else str(outcome.get("error_message") or "PDF download failed")
    )
    updated["download_status_class"] = str(
        outcome.get("download_status_class") or ("ok" if ok else "network")
    )
    updated["blocked_reason"] = str(outcome.get("blocked_reason") or "")
    updated["ua_profile_used"] = str(outcome.get("ua_profile_used") or "")
    updated["download_transport"] = transport
    updated["download_attempts"] = int(updated.get("download_attempts") or 0) + int(
        outcome.get("retries") or 1
    )
    updated["download_state"] = "downloaded" if ok else "failed"
    updated["download_updated_at"] = _utc_now_iso()
    return updated


def _run_download_from_manifest(
    *,
    run_id: str,
    manifest_dir: str,
    out_dir: str,
    max_workers: int,
    max_downloads_per_domain: int,
    min_delay: float,
    max_delay: float,
    dc_waf_browser_fallback: bool,
    browser_backend: str,
    browser_backend_available: bool,
    browser_staging_dir: str,
    browser_user_data_dir: str,
    browser_timeout: int,
    browser_headless: bool,
    target_scope: Literal["all", "dc_only", "non_dc_only"] = "all",
    strict_first_pass: bool = False,
    domain_revisit_interval_seconds: int = 0,
    operator_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    run_dir = os.path.join(manifest_dir, run_id)
    manifest_path = os.path.join(run_dir, "manifest.json")
    records_path = os.path.join(run_dir, "records.jsonl")
    errors_path = os.path.join(run_dir, "errors.jsonl")
    if not os.path.exists(manifest_path):
        raise ValueError(f"Missing run manifest for download phase: {manifest_path}")
    if not os.path.exists(records_path):
        raise ValueError(f"Missing run records for download phase: {records_path}")

    target_scope = str(target_scope or "all").strip().lower()  # type: ignore[assignment]
    if target_scope not in {"all", "dc_only", "non_dc_only"}:
        raise ValueError(
            f"Unsupported target_scope={target_scope!r}; expected 'all', 'dc_only', or 'non_dc_only'"
        )
    domain_revisit_interval_seconds = max(int(domain_revisit_interval_seconds or 0), 0)

    rows = _load_jsonl(records_path)
    if not rows:
        return {
            "run_id": run_id,
            "sites": 0,
            "pdfs": 0,
            "download_phase_attempted": 0,
            "download_phase_succeeded": 0,
            "download_phase_failed": 0,
            "todo_adapter_blocked_seeds": 0,
            "todo_adapter_hosts": 0,
            "operator_interventions_prompted": 0,
            "operator_interventions_retried": 0,
            "operator_interventions_recovered": 0,
            "operator_interventions_unresolved": 0,
        }

    seen_keys: set[str] = set()
    merged_rows: List[Dict[str, Any]] = []
    for row in rows:
        key = str(row.get("pdf_url") or row.get("page_url") or "").strip()
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        merged_rows.append(dict(row))

    pending_indices: List[int] = []
    for idx, row in enumerate(merged_rows):
        state = str(row.get("download_state") or "").strip().lower()
        local_path = str(row.get("local_path") or "").strip()
        if local_path and os.path.exists(local_path):
            merged_rows[idx]["download_state"] = "downloaded"
            merged_rows[idx]["ok"] = True
            continue
        if state == "downloaded":
            merged_rows[idx]["download_state"] = "failed"
        if not row.get("pdf_url"):
            continue
        is_dc_row = _is_dc_record(row)
        if target_scope == "dc_only" and not is_dc_row:
            continue
        if target_scope == "non_dc_only" and is_dc_row:
            continue
        pending_indices.append(idx)

    prior_waf_domains = _domains_with_waf_errors(errors_path)
    force_browser_transport = bool((operator_config or {}).get("enabled"))
    domain_browser_mode: Dict[str, bool] = {}
    domain_queues: Dict[str, deque[int]] = defaultdict(deque)
    for idx in pending_indices:
        row = merged_rows[idx]
        domain = str(
            row.get("domain") or urlparse(str(row.get("pdf_url") or "")).netloc or "unknown"
        )
        row["domain"] = domain
        domain_queues[domain].append(idx)
        # In operator mode, force browser transport for all pending records.
        # Otherwise, prefer browser for DC records and known WAF-prone domains.
        domain_browser_mode.setdefault(
            domain,
            bool(
                force_browser_transport
                or _is_dc_record(row)
                or (domain in prior_waf_domains and dc_waf_browser_fallback)
            ),
        )

    domains = deque(sorted(domain_queues.keys()))
    initial_domains = set(domain_queues.keys())
    effective_revisit_interval_seconds = (
        domain_revisit_interval_seconds if len(initial_domains) > 1 else 0
    )
    first_pass_pending = set(initial_domains) if strict_first_pass else set()
    first_pass_completed_at: str = ""
    domain_next_eligible_at: Dict[str, float] = {domain: 0.0 for domain in initial_domains}
    attempted = 0
    succeeded = 0
    failed = 0
    operator_prompted = 0
    operator_retried = 0
    operator_recovered = 0
    operator_unresolved = 0

    def _run_one(idx: int, transport: str) -> tuple[int, Dict[str, Any], bool, int, int, int, int]:
        row = dict(merged_rows[idx])
        domain = str(row.get("domain") or "unknown")
        if transport == "browser" and browser_backend_available:
            outcome = _download_single_with_browser(
                record=row,
                out_dir=out_dir,
                backend=browser_backend,
                staging_dir=browser_staging_dir,
                user_data_dir=os.path.join(browser_user_data_dir, domain.replace(":", "_")),
                timeout=browser_timeout,
                headless=browser_headless,
                operator_config=operator_config,
            )
        else:
            seed_url = str(row.get("seed_url") or row.get("page_url") or row.get("pdf_url"))
            with PoliteRequestsSession(min_delay=min_delay, max_delay=max_delay) as session:
                try:
                    adapter = pick_adapter_for(seed_url, session=session, allow_generic=False)
                except UnmappedAdapterError as exc:
                    _append_jsonl(
                        errors_path,
                        {
                            "seed_url": row.get("seed_url"),
                            "domain": row.get("domain"),
                            "page_url": row.get("page_url"),
                            "pdf_url": row.get("pdf_url"),
                            "source_adapter": "download_from_manifest",
                            "extraction_path": str(row.get("extraction_path") or ""),
                            "metadata": row.get("metadata") or {},
                            "error_type": "todo_adapter_blocked",
                            "message": str(exc),
                            "http_status": 0,
                            "attempt": 1,
                            "retries": int(row.get("download_attempts") or 0),
                            "blocked_host": exc.host,
                            "retrieved_at": _utc_now_iso(),
                        },
                    )
                    outcome = {
                        "ok": False,
                        "local_path": None,
                        "pdf_sha256": None,
                        "pdf_size_bytes": None,
                        "content_type": "",
                        "http_status": 0,
                        "error_type": "todo_adapter_blocked",
                        "error_message": str(exc),
                        "ua_profile_used": "",
                        "robots_allowed": None,
                        "download_status_class": "network",
                        "blocked_reason": "no_registered_adapter",
                        "retry_after_hint": None,
                        "retries": 0,
                    }
                    updated = _update_manifest_record_with_outcome(
                        record=row,
                        outcome=outcome,
                        transport=transport,
                        out_dir=out_dir,
                    )
                    return idx, updated, False, 0, 0, 0, 0
                error_context = {
                    "seed_url": row.get("seed_url"),
                    "domain": row.get("domain"),
                    "page_url": row.get("page_url"),
                    "pdf_url": row.get("pdf_url"),
                    "source_adapter": row.get("source_adapter"),
                    "extraction_path": row.get("extraction_path"),
                    "metadata": row.get("metadata") or {},
                    "dc_source": str((row.get("metadata") or {}).get("dc_source") or ""),
                }
                outcome = _download_with_retries(
                    adapter=adapter,
                    pdf_url=str(row.get("pdf_url") or ""),
                    out_dir=os.path.join(out_dir, domain),
                    errors_path=errors_path,
                    error_context=error_context,
                    max_attempts=3,
                )
                if not outcome.get("ok"):
                    outcome["error_type"] = str(outcome.get("error_type") or "download_failed")
                    outcome["error_message"] = str(
                        outcome.get("error_message") or "PDF download failed"
                    )
        updated = _update_manifest_record_with_outcome(
            record=row,
            outcome=outcome,
            transport=transport,
            out_dir=out_dir,
        )
        op_prompted = int(outcome.get("operator_intervention_prompted") or 0)
        op_retried = int(outcome.get("operator_intervention_retried") or 0)
        op_recovered = 1 if bool(outcome.get("operator_intervention_recovered")) else 0
        op_unresolved = 1 if bool(outcome.get("operator_intervention_unresolved")) else 0
        was_waf = (
            not bool(updated.get("ok")) and "waf" in str(updated.get("error_type") or "").lower()
        )
        return idx, updated, was_waf, op_prompted, op_retried, op_recovered, op_unresolved

    with ThreadPoolExecutor(max_workers=max(max_workers, 1)) as executor:
        inflight: Dict[Any, tuple[str, int, str]] = {}

        def _dispatch() -> float:
            nonlocal first_pass_completed_at
            if not domains:
                return 0.0
            now_ts = time.time()
            min_wait_s: Optional[float] = None
            for _ in range(len(domains)):
                if len(inflight) >= max(max_workers, 1):
                    break
                domain = domains[0]
                domains.rotate(-1)
                queue_for_domain = domain_queues.get(domain)
                if not queue_for_domain:
                    continue
                inflight_count = sum(1 for _f, (d, _idx, _t) in inflight.items() if d == domain)
                if inflight_count >= max(max_downloads_per_domain, 1):
                    continue
                if strict_first_pass and first_pass_pending and domain not in first_pass_pending:
                    continue
                next_eligible_at = float(domain_next_eligible_at.get(domain) or 0.0)
                if next_eligible_at > now_ts:
                    wait_s = max(next_eligible_at - now_ts, 0.0)
                    min_wait_s = wait_s if min_wait_s is None else min(min_wait_s, wait_s)
                    continue
                idx = queue_for_domain.popleft()
                if not queue_for_domain:
                    domain_queues.pop(domain, None)
                    try:
                        domains.remove(domain)
                    except ValueError:
                        pass
                if effective_revisit_interval_seconds > 0:
                    domain_next_eligible_at[domain] = now_ts + float(
                        effective_revisit_interval_seconds
                    )
                if strict_first_pass and domain in first_pass_pending:
                    first_pass_pending.discard(domain)
                    if not first_pass_pending and not first_pass_completed_at:
                        first_pass_completed_at = _utc_now_iso()
                transport = (
                    "browser"
                    if (domain_browser_mode.get(domain) and browser_backend_available)
                    else "http"
                )
                future = executor.submit(_run_one, idx, transport)
                inflight[future] = (domain, idx, transport)
            return float(min_wait_s or 0.0)

        while inflight or domains:
            min_wait_s = _dispatch()
            if not inflight:
                if not domains:
                    break
                sleep_s = min_wait_s if min_wait_s > 0 else 0.05
                time.sleep(min(sleep_s, 1.0))
                continue
            wait_timeout = min_wait_s if min_wait_s > 0 else 0.5
            done, _ = wait(
                tuple(inflight.keys()),
                timeout=max(wait_timeout, 0.05),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                continue
            for future in done:
                domain, idx, transport = inflight.pop(future)
                attempted += 1
                (
                    task_idx,
                    updated_row,
                    was_waf,
                    op_prompted,
                    op_retried,
                    op_recovered,
                    op_unresolved,
                ) = future.result()
                operator_prompted += max(op_prompted, 0)
                operator_retried += max(op_retried, 0)
                operator_recovered += max(op_recovered, 0)
                operator_unresolved += max(op_unresolved, 0)
                if (
                    was_waf
                    and dc_waf_browser_fallback
                    and transport != "browser"
                    and browser_backend_available
                ):
                    domain_browser_mode[domain] = True
                    domain_queues.setdefault(domain, deque()).appendleft(task_idx)
                    if domain not in domains:
                        domains.append(domain)
                else:
                    merged_rows[task_idx] = updated_row
                    if bool(updated_row.get("ok")):
                        succeeded += 1
                    else:
                        failed += 1
                    _write_jsonl_atomic(records_path, merged_rows)

    manifest_payload = _read_json(manifest_path, {})
    first_pass_completed = bool(strict_first_pass and not first_pass_pending)
    manifest_payload["download_phase"] = {
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
        "target_scope": target_scope,
        "strict_first_pass": bool(strict_first_pass),
        "domain_revisit_interval_seconds": int(effective_revisit_interval_seconds),
        "domains": len(initial_domains),
        "first_pass_completed": first_pass_completed,
        "first_pass_completed_at": first_pass_completed_at or None,
        "operator_interventions_prompted": operator_prompted,
        "operator_interventions_retried": operator_retried,
        "operator_interventions_recovered": operator_recovered,
        "operator_interventions_unresolved": operator_unresolved,
        "completed_at": _utc_now_iso(),
    }
    _write_json(manifest_path, manifest_payload)

    blocked_seed_urls = {
        str(row.get("seed_url") or "")
        for row in merged_rows
        if str(row.get("error_type") or "") == "todo_adapter_blocked"
    }
    blocked_hosts = {
        str(row.get("domain") or "").lower()
        for row in merged_rows
        if str(row.get("error_type") or "") == "todo_adapter_blocked"
    }
    blocked_seed_urls.discard("")
    blocked_hosts.discard("")

    return {
        "run_id": run_id,
        "sites": 0,
        "pdfs": succeeded,
        "download_phase_attempted": attempted,
        "download_phase_succeeded": succeeded,
        "download_phase_failed": failed,
        "download_phase_target_scope": target_scope,
        "download_phase_domains": len(initial_domains),
        "download_phase_strict_first_pass": bool(strict_first_pass),
        "download_phase_domain_revisit_interval_seconds": int(effective_revisit_interval_seconds),
        "download_phase_first_pass_completed": first_pass_completed,
        "download_phase_first_pass_completed_at": first_pass_completed_at,
        "todo_adapter_blocked_seeds": len(blocked_seed_urls),
        "todo_adapter_hosts": len(blocked_hosts),
        "operator_interventions_prompted": operator_prompted,
        "operator_interventions_retried": operator_retried,
        "operator_interventions_recovered": operator_recovered,
        "operator_interventions_unresolved": operator_unresolved,
    }


def _seed_counts_as_failure(result: SeedProcessResult, links_only: bool) -> bool:
    """Classify whether a seed should increment domain failure streak."""
    if result.status != "completed":
        return True
    if links_only:
        return result.records_total == 0
    return result.ok_total == 0

def run_orchestrator(
    sitemaps_csv: Optional[str] = None,
    sitemaps_dir: Optional[str] = None,
    out_dir: str = "artifacts/pdfs",
    manifest_dir: str = "artifacts/runs",
    max_depth: int = 0,
    seeds_override: Optional[List[str]] = None,
    links_only: bool = False,
    discovery_only: bool = False,
    download_from_manifest: Optional[str] = None,
    use_playwright: bool = False,
    playwright_headed: bool = False,
    min_delay: float = 2.0,
    max_delay: float = 5.0,
    max_workers: int = 1,
    max_seeds_per_domain: int = 3,
    run_id: Optional[str] = None,
    resume: Optional[str] = None,
    cache_dir: str = "artifacts/cache/http",
    cache_ttl_hours: int = 24,
    cache_max_bytes: int = 2_147_483_648,
    dc_enum_mode: str = "oai_sitemap_union",
    dc_use_siteindex: bool = True,
    dc_ua_fallback_profiles: str = "browser,transparent,python_requests,wget,curl",
    dc_robots_enforce: bool = True,
    dc_max_oai_records: int = 0,
    dc_max_sitemap_urls: int = 0,
    dc_download_timeout: int = 30,
    dc_min_domain_delay_ms: int = 1000,
    dc_max_domain_delay_ms: int = 2000,
    dc_waf_fail_threshold: int = 3,
    dc_waf_cooldown_seconds: int = 900,
    dc_disable_unscoped_oai_no_slug: bool = True,
    dc_session_rotate_threshold: int = 300,
    dc_use_curl_cffi: bool = True,
    dc_waf_browser_fallback: bool = False,
    dc_browser_backend: str = "auto",
    dc_browser_staging_dir: str = "artifacts/browser_staging",
    dc_browser_user_data_dir: str = "artifacts/browser_profile",
    dc_browser_timeout: int = 60,
    dc_browser_headless: bool = False,
    max_downloads_per_domain: int = 1,
    skip_well_covered_seeds: bool = True,
    skip_dc_sites: bool = False,
    well_covered_pdf_threshold: int = 250,
    max_consecutive_seed_failures_per_domain: int = 3,
    dc_round_robin_downloads: bool = True,
    dc_round_robin_strict_first_pass: bool = True,
    dc_round_robin_revisit_interval_seconds: int = 90,
    no_pdf_progress_timeout_seconds: int = 60,
    stalled_seed_timeout_seconds: int = 60,
    retry_stalled_seeds: bool = True,
    stalled_retry_max_depth: int = 1,
    operator_mode: bool = False,
    operator_intervention_scope: Literal["off", "browser_fallback_only"] = "off",
    operator_wait_mode: Literal["prompt_enter", "off"] = "off",
    operator_manual_retries: int = 1,
    write_legacy_manifests: Optional[bool] = None,
) -> Dict[str, Any]:
    warn_legacy_paths(
        tool_name="orchestrator",
        values_by_arg={k: str(locals().get(k) or "") for k in LEGACY_DEFAULT_PATHS},
        legacy_by_arg={k: {v} for k, v in LEGACY_DEFAULT_PATHS.items()},
    )
    write_legacy_manifests_enabled = should_write_legacy_manifests(write_legacy_manifests)

    ensure_dir(out_dir)
    ensure_dir(manifest_dir)

    scope = str(operator_intervention_scope or "off").strip().lower()
    if scope not in {"off", "browser_fallback_only"}:
        scope = "off"
    operator_intervention_scope = scope  # type: ignore[assignment]
    wait_mode = str(operator_wait_mode or "off").strip().lower()
    if wait_mode not in {"prompt_enter", "off"}:
        wait_mode = "off"
    operator_wait_mode = wait_mode  # type: ignore[assignment]
    operator_manual_retries = max(int(operator_manual_retries or 0), 0)

    if operator_mode and operator_intervention_scope == "off":
        operator_intervention_scope = "browser_fallback_only"
    if operator_mode and operator_wait_mode == "off":
        operator_wait_mode = "prompt_enter"
    if operator_mode:
        if max_workers != 1 or max_seeds_per_domain != 1 or max_downloads_per_domain != 1:
            print(
                "ℹ️  Operator mode enabled: forcing serial scheduling "
                "(max_workers=1, max_seeds_per_domain=1, max_downloads_per_domain=1).",
                flush=True,
            )
        max_workers = 1
        max_seeds_per_domain = 1
        max_downloads_per_domain = 1
        if not dc_waf_browser_fallback:
            print("ℹ️  Operator mode enabled: forcing --dc-waf-browser-fallback.", flush=True)
        dc_waf_browser_fallback = True
        if dc_browser_headless:
            print("ℹ️  Operator mode enabled: forcing headed DC browser fallback.", flush=True)
        dc_browser_headless = False

    runtime_capabilities = _resolve_runtime_capabilities(
        dc_use_curl_cffi=dc_use_curl_cffi,
        dc_waf_browser_fallback=dc_waf_browser_fallback,
        dc_browser_backend=dc_browser_backend,
    )

    if download_from_manifest:
        operator_config = _build_operator_intervention_config(
            operator_mode=operator_mode,
            operator_intervention_scope=operator_intervention_scope,
            operator_wait_mode=operator_wait_mode,
            operator_manual_retries=operator_manual_retries,
            run_id=download_from_manifest,
            run_dir=os.path.join(manifest_dir, download_from_manifest),
        )
        return _run_download_from_manifest(
            run_id=download_from_manifest,
            manifest_dir=manifest_dir,
            out_dir=out_dir,
            max_workers=max_workers,
            max_downloads_per_domain=max_downloads_per_domain,
            min_delay=min_delay,
            max_delay=max_delay,
            dc_waf_browser_fallback=dc_waf_browser_fallback,
            browser_backend=str(
                runtime_capabilities.get("browser_backend_selected") or dc_browser_backend
            ),
            browser_backend_available=bool(runtime_capabilities.get("browser_backend_available")),
            browser_staging_dir=dc_browser_staging_dir,
            browser_user_data_dir=dc_browser_user_data_dir,
            browser_timeout=dc_browser_timeout,
            browser_headless=dc_browser_headless,
            target_scope="non_dc_only" if skip_dc_sites else "all",
            strict_first_pass=False,
            domain_revisit_interval_seconds=0,
            operator_config=operator_config,
        )

    seed_context_map: Dict[str, Dict[str, Any]] = {}
    inactive_seed_entries_by_status: Dict[str, int] = {}
    inactive_seed_entries_total = 0
    if seeds_override:
        seeds = seeds_override
    else:
        seeds = []
        if sitemaps_dir:
            all_seed_entries = load_seed_catalog_entries(sitemaps_dir)
            active_seed_entries = filter_active_seed_entries(all_seed_entries)
            seed_context_map.update(seed_catalog_context_by_url(active_seed_entries))
            seeds.extend(entry.seed_url for entry in active_seed_entries)
            inactive_seed_entries_total = max(len(all_seed_entries) - len(active_seed_entries), 0)
            for entry in all_seed_entries:
                if entry.status == SEED_STATUS_ACTIVE:
                    continue
                inactive_seed_entries_by_status[entry.status] = (
                    inactive_seed_entries_by_status.get(entry.status, 0) + 1
                )
        if sitemaps_csv:
            seeds.extend(read_sitemaps_csv(sitemaps_csv))
        if not seeds:
            raise ValueError("Provide seeds via seeds_override or --sitemaps or --sitemaps-dir")

    actual_run_id = resume or run_id or _default_run_id()
    run_paths = _initialize_run(
        manifest_dir=manifest_dir,
        out_dir=out_dir,
        run_id=actual_run_id,
        links_only=links_only,
        discovery_only=discovery_only,
        max_depth=max_depth,
        min_delay=min_delay,
        max_delay=max_delay,
        max_workers=max_workers,
        use_playwright=use_playwright,
        playwright_headed=playwright_headed,
        is_resume=bool(resume),
        dc_enum_mode=dc_enum_mode,
        dc_use_siteindex=dc_use_siteindex,
        dc_ua_fallback_profiles=dc_ua_fallback_profiles,
        dc_robots_enforce=dc_robots_enforce,
        dc_max_oai_records=dc_max_oai_records,
        dc_max_sitemap_urls=dc_max_sitemap_urls,
        dc_download_timeout=dc_download_timeout,
        dc_min_domain_delay_ms=dc_min_domain_delay_ms,
        dc_max_domain_delay_ms=dc_max_domain_delay_ms,
        dc_waf_fail_threshold=dc_waf_fail_threshold,
        dc_waf_cooldown_seconds=dc_waf_cooldown_seconds,
        dc_disable_unscoped_oai_no_slug=dc_disable_unscoped_oai_no_slug,
        dc_session_rotate_threshold=dc_session_rotate_threshold,
        dc_use_curl_cffi=dc_use_curl_cffi,
        dc_waf_browser_fallback=dc_waf_browser_fallback,
        dc_browser_backend=dc_browser_backend,
        dc_browser_staging_dir=dc_browser_staging_dir,
        dc_browser_user_data_dir=dc_browser_user_data_dir,
        dc_browser_timeout=dc_browser_timeout,
        dc_browser_headless=dc_browser_headless,
        skip_well_covered_seeds=skip_well_covered_seeds,
        skip_dc_sites=skip_dc_sites,
        well_covered_pdf_threshold=well_covered_pdf_threshold,
        max_consecutive_seed_failures_per_domain=max_consecutive_seed_failures_per_domain,
        dc_round_robin_downloads=dc_round_robin_downloads,
        dc_round_robin_strict_first_pass=dc_round_robin_strict_first_pass,
        dc_round_robin_revisit_interval_seconds=dc_round_robin_revisit_interval_seconds,
        operator_mode=operator_mode,
        operator_intervention_scope=str(operator_intervention_scope),
        operator_wait_mode=str(operator_wait_mode),
        operator_manual_retries=operator_manual_retries,
        write_legacy_manifests=write_legacy_manifests_enabled,
        runtime_capabilities=runtime_capabilities,
    )
    operator_config = _build_operator_intervention_config(
        operator_mode=operator_mode,
        operator_intervention_scope=str(operator_intervention_scope),
        operator_wait_mode=str(operator_wait_mode),
        operator_manual_retries=operator_manual_retries,
        run_id=actual_run_id,
        run_dir=run_paths["run_dir"],
    )

    stats_payload = _read_json(
        run_paths["stats_path"],
        {"run_id": actual_run_id, "seeds": {}, "domains": {}, "journals": {}},
    )
    stats_payload.setdefault("seeds", {})
    stats_payload.setdefault("domains", {})
    stats_payload.setdefault("journals", {})
    completed_seed_urls = {
        seed_url
        for seed_url, seed_stats in (stats_payload.get("seeds") or {}).items()
        if isinstance(seed_stats, dict) and seed_stats.get("status") == "completed"
    }
    seeds_to_run = [seed for seed in seeds if seed not in completed_seed_urls]
    well_covered_skipped_seed_urls: List[str] = []
    well_covered_domain_counts: Dict[str, int] = {}
    effective_well_covered_threshold = max(int(well_covered_pdf_threshold or 0), 1)
    should_filter_well_covered = bool(
        skip_well_covered_seeds and not links_only and not discovery_only
    )
    if should_filter_well_covered and seeds_to_run:
        existing_pdf_counts = _count_existing_pdfs_by_domain(out_dir)
        filtered_seeds: List[str] = []
        for seed in seeds_to_run:
            domain = urlparse(seed).netloc or "unknown"
            count = int(existing_pdf_counts.get(domain) or 0)
            if count >= effective_well_covered_threshold:
                well_covered_skipped_seed_urls.append(seed)
                well_covered_domain_counts[domain] = count
                continue
            filtered_seeds.append(seed)
        seeds_to_run = filtered_seeds
    dc_skipped_seed_urls: List[str] = []
    dc_skipped_domains: set[str] = set()
    if skip_dc_sites and seeds_to_run:
        retained_seeds: List[str] = []
        for seed in seeds_to_run:
            if _seed_routes_to_dc(seed):
                dc_skipped_seed_urls.append(seed)
                dc_skipped_domains.add(urlparse(seed).netloc or "unknown")
                continue
            retained_seeds.append(seed)
        seeds_to_run = retained_seeds

    snapshot_cache = HttpSnapshotCache(
        cache_dir=cache_dir,
        ttl_seconds=max(cache_ttl_hours, 0) * 3600,
        max_bytes=cache_max_bytes,
        scope=actual_run_id,
    )
    parsed_dc_profiles = [
        p.strip() for p in str(dc_ua_fallback_profiles or "").split(",") if p.strip()
    ] or ["browser", "transparent", "python_requests", "wget", "curl"]

    summary: Dict[str, Any] = {
        "run_id": actual_run_id,
        "sites": 0,
        "pdfs": 0,
        "skipped": len(seeds) - len(seeds_to_run),
        "skipped_due_consecutive_failures": 0,
        "waf_blocked": 0,
        "curl_cffi_downloads": 0,
        "curl_cffi_enabled": bool(runtime_capabilities.get("curl_cffi_enabled")),
        "browser_fallback_backend": str(runtime_capabilities.get("browser_backend_selected") or ""),
        "browser_fallback_enabled": bool(runtime_capabilities.get("browser_fallback_enabled")),
        "operator_mode": bool(operator_mode),
        "operator_intervention_scope": str(operator_intervention_scope),
        "operator_wait_mode": str(operator_wait_mode),
        "operator_manual_retries": int(operator_manual_retries),
        "no_pdf_progress_timeout_seconds": max(int(no_pdf_progress_timeout_seconds or 0), 1),
        "stalled_seed_timeout_seconds": max(int(stalled_seed_timeout_seconds or 0), 1),
        "retry_stalled_seeds": bool(retry_stalled_seeds),
        "phase2_retry_attempted_seeds": 0,
        "phase2_retry_recovered_seeds": 0,
        "phase2_retry_total_added_pdfs": 0,
        "stalled_seeds_observed": 0,
        "no_pdf_progress_warnings": 0,
        "browser_fallback_attempted": 0,
        "browser_fallback_succeeded": 0,
        "operator_interventions_prompted": 0,
        "operator_interventions_retried": 0,
        "operator_interventions_recovered": 0,
        "operator_interventions_unresolved": 0,
        "inactive_seed_entries": inactive_seed_entries_total,
        "inactive_seed_entries_by_status": dict(sorted(inactive_seed_entries_by_status.items())),
        "todo_adapter_blocked_seeds": 0,
        "todo_adapter_hosts": 0,
        "skip_well_covered_seeds": should_filter_well_covered,
        "skip_dc_sites": bool(skip_dc_sites),
        "well_covered_pdf_threshold": effective_well_covered_threshold,
        "skipped_well_covered_seeds": len(well_covered_skipped_seed_urls),
        "well_covered_domains": len(well_covered_domain_counts),
        "skipped_dc_seeds": len(dc_skipped_seed_urls),
        "skipped_dc_domains": len(dc_skipped_domains),
        "dc_round_robin_enabled": bool(
            dc_round_robin_downloads and not links_only and not discovery_only and not skip_dc_sites
        ),
        "dc_round_robin_strict_first_pass": bool(dc_round_robin_strict_first_pass),
        "dc_round_robin_revisit_interval_seconds": max(
            int(dc_round_robin_revisit_interval_seconds or 0), 0
        ),
        "dc_round_robin_eligible_records": 0,
        "dc_round_robin_attempted": 0,
        "dc_round_robin_succeeded": 0,
        "dc_round_robin_failed": 0,
        "dc_round_robin_domains": 0,
        "dc_round_robin_first_pass_completed": False,
        "dc_round_robin_first_pass_completed_at": "",
    }

    print(
        f"🚀 Starting scraper with {len(seeds_to_run)} seeds (workers={max_workers}, run_id={actual_run_id})"
    )
    if inactive_seed_entries_total > 0:
        rendered = ", ".join(
            f"{status}={count}" for status, count in sorted(inactive_seed_entries_by_status.items())
        )
        print(
            f"ℹ️  Excluded non-active sitemap seeds: {inactive_seed_entries_total} ({rendered})",
            flush=True,
        )
    if well_covered_skipped_seed_urls:
        rendered = ", ".join(
            f"{domain}={count}" for domain, count in sorted(well_covered_domain_counts.items())
        )
        print(
            "ℹ️  Excluded well-covered seeds: "
            f"{len(well_covered_skipped_seed_urls)} seeds across "
            f"{len(well_covered_domain_counts)} domains "
            f"(threshold={effective_well_covered_threshold} PDFs; {rendered})",
            flush=True,
        )
    if dc_skipped_seed_urls:
        rendered = ", ".join(sorted(dc_skipped_domains))
        print(
            "ℹ️  Excluded DC seeds: "
            f"{len(dc_skipped_seed_urls)} seeds across {len(dc_skipped_domains)} domains "
            f"({rendered})",
            flush=True,
        )

    try:
        pending_seeds = deque(seeds_to_run)
        domain_failure_streak: Dict[str, int] = {}
        circuit_breaker_domains: set[str] = set()
        processed_waf_urls: set[str] = set()
        todo_adapter_blocked_seed_set: set[str] = set()
        todo_adapter_blocked_host_set: set[str] = set()
        progress_events: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        seed_runtime: Dict[str, Dict[str, Any]] = {}
        seed_results: Dict[str, SeedProcessResult] = {}
        seed_ok_totals: Dict[str, int] = {}
        stalled_seed_candidates: set[str] = set()
        no_pdf_timeout_s = float(max(int(no_pdf_progress_timeout_seconds or 0), 1))
        stalled_seed_timeout_s = float(max(int(stalled_seed_timeout_seconds or 0), 1))
        # Poll at least every 2 seconds so short thresholds are observable.
        wait_poll_timeout_s = max(2.0, min(30.0, no_pdf_timeout_s / 2.0))
        last_successful_pdf_at = time.time()
        live_pdf_success_total = 0
        last_no_pdf_warning_at = 0.0

        def _progress_callback(event: Dict[str, Any]) -> None:
            progress_events.put(event)

        def _normalize_seed_runtime(seed: str, domain: str, started_at: float) -> Dict[str, Any]:
            state = seed_runtime.setdefault(
                seed,
                {
                    "seed": seed,
                    "domain": domain,
                    "stage": "queued",
                    "started_at": started_at,
                    "last_change_at": started_at,
                    "discovered": 0,
                    "downloaded": 0,
                    "failed_downloads": 0,
                    "done": False,
                },
            )
            state["domain"] = domain
            return state

        def _drain_progress_events() -> None:
            nonlocal last_successful_pdf_at, live_pdf_success_total
            while True:
                try:
                    event = progress_events.get_nowait()
                except queue.Empty:
                    break

                seed = str(event.get("seed") or "").strip()
                if not seed:
                    continue
                domain = str(event.get("domain") or (urlparse(seed).netloc or "unknown"))
                event_ts = float(event.get("timestamp") or time.time())
                event_type = str(event.get("event") or "")

                state = _normalize_seed_runtime(seed, domain, event_ts)
                state["last_change_at"] = max(float(state.get("last_change_at") or 0.0), event_ts)
                if "stage" in event:
                    state["stage"] = str(event.get("stage") or state.get("stage") or "running")
                if "discovered" in event:
                    state["discovered"] = max(int(event.get("discovered") or 0), 0)
                if "downloaded" in event:
                    state["downloaded"] = max(int(event.get("downloaded") or 0), 0)
                if "failed_downloads" in event:
                    state["failed_downloads"] = max(int(event.get("failed_downloads") or 0), 0)
                if event_type == "first_pdf_candidate":
                    first_candidate_at = float(state.get("first_candidate_at") or 0.0)
                    if first_candidate_at <= 0.0:
                        state["first_candidate_at"] = event_ts
                if event_type == "pdf_saved":
                    live_pdf_success_total += 1
                    last_successful_pdf_at = max(last_successful_pdf_at, event_ts)
                if event_type == "seed_done":
                    state["done"] = True

        def _apply_result_to_summary(result: SeedProcessResult) -> None:
            previous_ok = seed_ok_totals.get(result.seed)
            if previous_ok is None:
                summary["sites"] += 1
                summary["pdfs"] += int(result.ok_total)
            else:
                summary["pdfs"] += int(result.ok_total) - int(previous_ok)
            seed_ok_totals[result.seed] = int(result.ok_total)
            summary["dc_round_robin_eligible_records"] = int(
                summary.get("dc_round_robin_eligible_records") or 0
            ) + int((result.completeness or {}).get("dc_download_deferred_count") or 0)

        def _emit_no_pdf_gap_warning(
            *,
            now_ts: float,
            inflight: Dict[Any, tuple[str, str, float]],
        ) -> None:
            nonlocal last_no_pdf_warning_at
            if links_only or discovery_only or not inflight:
                return

            gap = max(now_ts - last_successful_pdf_at, 0.0)
            if gap < no_pdf_timeout_s:
                return
            if (now_ts - last_no_pdf_warning_at) < max(no_pdf_timeout_s / 2.0, 10.0):
                return

            stalled_details: List[tuple[float, str, str, int, int, str]] = []
            for _future, (seed, domain, started_at) in inflight.items():
                state = _normalize_seed_runtime(seed, domain, started_at)
                last_change = float(state.get("last_change_at") or started_at)
                stalled_for = max(now_ts - last_change, 0.0)
                if stalled_for >= stalled_seed_timeout_s:
                    stalled_seed_candidates.add(seed)
                stalled_details.append(
                    (
                        stalled_for,
                        seed,
                        str(state.get("stage") or "unknown"),
                        int(state.get("discovered") or 0),
                        int(state.get("downloaded") or 0),
                        str(state.get("domain") or domain),
                    )
                )

            stalled_details.sort(key=lambda item: item[0], reverse=True)
            top = stalled_details[:3]
            top_rendered = ", ".join(
                f"{seed} stage={stage} stall={stall_for:.0f}s d={discovered}/ok={downloaded}"
                for stall_for, seed, stage, discovered, downloaded, _domain in top
            )
            print(
                "⚠️  No successful PDF download for "
                f"{gap:.0f}s (threshold={no_pdf_timeout_s:.0f}s). "
                f"Top stalled seeds: {top_rendered or 'n/a'}",
                flush=True,
            )
            summary["no_pdf_progress_warnings"] = (
                int(summary.get("no_pdf_progress_warnings") or 0) + 1
            )
            last_no_pdf_warning_at = now_ts

        def _record_stats_for_result(result: SeedProcessResult) -> None:
            previous_seed_stats = stats_payload.setdefault("seeds", {}).get(result.seed)
            previous_domain = (
                str(previous_seed_stats.get("domain") or "")
                if isinstance(previous_seed_stats, dict)
                else ""
            )
            if previous_domain:
                previous_domain_stats = stats_payload.setdefault("domains", {}).setdefault(
                    previous_domain,
                    {
                        "domain": previous_domain,
                        "seeds": 0,
                        "skipped_seeds": 0,
                        "records_total": 0,
                        "ok_total": 0,
                        "errors_total": 0,
                        "completeness": {},
                    },
                )
                previous_domain_stats["records_total"] = max(
                    0,
                    int(previous_domain_stats.get("records_total") or 0)
                    - int(previous_seed_stats.get("records_total") or 0),
                )
                previous_domain_stats["ok_total"] = max(
                    0,
                    int(previous_domain_stats.get("ok_total") or 0)
                    - int(previous_seed_stats.get("ok_total") or 0),
                )
                previous_domain_stats["errors_total"] = max(
                    0,
                    int(previous_domain_stats.get("errors_total") or 0)
                    - int(previous_seed_stats.get("errors_total") or 0),
                )
                if previous_seed_stats.get("status") == "skipped_site_circuit_breaker":
                    previous_domain_stats["skipped_seeds"] = max(
                        0,
                        int(previous_domain_stats.get("skipped_seeds") or 0) - 1,
                    )

            seed_stats = {
                "seed_url": result.seed,
                "domain": result.domain,
                "status": result.status,
                "records_total": result.records_total,
                "ok_total": result.ok_total,
                "errors_total": result.errors_total,
                "completeness": result.completeness,
                "journal_summary": dict(result.journal_summary or {}),
                "updated_at": _utc_now_iso(),
            }
            runtime_state = seed_runtime.get(result.seed)
            if isinstance(runtime_state, dict):
                started_at = float(runtime_state.get("started_at") or 0.0)
                last_change_at = float(runtime_state.get("last_change_at") or 0.0)
                first_candidate_at = float(runtime_state.get("first_candidate_at") or 0.0)
                runtime_payload = {
                    "stage": str(runtime_state.get("stage") or ""),
                    "discovered": int(runtime_state.get("discovered") or 0),
                    "downloaded": int(runtime_state.get("downloaded") or 0),
                    "failed_downloads": int(runtime_state.get("failed_downloads") or 0),
                    "started_at_epoch_s": started_at if started_at > 0.0 else None,
                    "last_change_at_epoch_s": last_change_at if last_change_at > 0.0 else None,
                    "elapsed_seconds": (
                        round(max(last_change_at - started_at, 0.0), 3)
                        if started_at > 0.0 and last_change_at > 0.0
                        else None
                    ),
                    "first_pdf_candidate_seconds": (
                        round(max(first_candidate_at - started_at, 0.0), 3)
                        if started_at > 0.0 and first_candidate_at > 0.0
                        else None
                    ),
                }
                seed_stats["runtime"] = runtime_payload
            stats_payload.setdefault("seeds", {})[result.seed] = seed_stats

            domain_stats = stats_payload.setdefault("domains", {}).setdefault(
                result.domain,
                {
                    "domain": result.domain,
                    "seeds": 0,
                    "skipped_seeds": 0,
                    "records_total": 0,
                    "ok_total": 0,
                    "errors_total": 0,
                    "completeness": {},
                },
            )
            if not previous_seed_stats:
                domain_stats["seeds"] += 1
            elif previous_domain and previous_domain != result.domain:
                previous_domain_stats = stats_payload.setdefault("domains", {}).setdefault(
                    previous_domain,
                    {
                        "domain": previous_domain,
                        "seeds": 0,
                        "skipped_seeds": 0,
                        "records_total": 0,
                        "ok_total": 0,
                        "errors_total": 0,
                        "completeness": {},
                    },
                )
                previous_domain_stats["seeds"] = max(
                    0,
                    int(previous_domain_stats.get("seeds") or 0) - 1,
                )
                domain_stats["seeds"] += 1
            domain_stats["records_total"] += result.records_total
            domain_stats["ok_total"] += result.ok_total
            domain_stats["errors_total"] += result.errors_total
            domain_stats["completeness"] = dict(result.completeness or {})

            seed_journal_summary = dict(result.journal_summary or {})
            journal_name = str(seed_journal_summary.get("journal_name") or result.domain)
            journal_stats = stats_payload.setdefault("journals", {}).get(_journal_key(journal_name))
            stats_payload.setdefault("journals", {})[_journal_key(journal_name)] = (
                _merge_journal_stats(
                    journal_stats,
                    seed_journal_summary,
                )
            )

            stats_payload["updated_at"] = _utc_now_iso()
            _write_json(run_paths["stats_path"], stats_payload)

        def _track_todo_adapter_block(result: SeedProcessResult) -> None:
            if result.status != "todo_adapter_blocked":
                return
            todo_adapter_blocked_seed_set.add(result.seed)
            host = str(result.domain or urlparse(result.seed).netloc or "").strip().lower()
            if host:
                todo_adapter_blocked_host_set.add(host)

        def _mark_seed_skipped(seed: str, domain: str, reason: str) -> None:
            summary["skipped"] += 1
            summary["skipped_due_consecutive_failures"] += 1
            stats_payload.setdefault("seeds", {})[seed] = {
                "seed_url": seed,
                "domain": domain,
                "status": "skipped_site_circuit_breaker",
                "records_total": 0,
                "ok_total": 0,
                "errors_total": 0,
                "completeness": {},
                "journal_summary": {},
                "skip_reason": reason,
                "updated_at": _utc_now_iso(),
            }
            domain_stats = stats_payload.setdefault("domains", {}).setdefault(
                domain,
                {
                    "domain": domain,
                    "seeds": 0,
                    "skipped_seeds": 0,
                    "records_total": 0,
                    "ok_total": 0,
                    "errors_total": 0,
                    "completeness": {},
                },
            )
            domain_stats["skipped_seeds"] += 1
            _append_jsonl(
                run_paths["errors_path"],
                {
                    "seed_url": seed,
                    "domain": domain,
                    "page_url": seed,
                    "pdf_url": None,
                    "source_adapter": "orchestrator",
                    "extraction_path": "seed_scheduler",
                    "metadata": {},
                    "error_type": "seed_skipped_site_circuit_breaker",
                    "message": reason,
                    "http_status": 0,
                    "attempt": 0,
                    "retries": 0,
                    "retrieved_at": _utc_now_iso(),
                },
            )
            stats_payload["updated_at"] = _utc_now_iso()
            _write_json(run_paths["stats_path"], stats_payload)

        # Create a shared session so all workers reuse browsers from a capped
        # pool instead of each spawning its own Chromium process.
        shared_session = None
        if use_playwright:
            from .playwright_session import PlaywrightSession

            max_browsers = int(os.getenv("LRS_MAX_BROWSERS", "4") or "4")
            PlaywrightSession.set_max_browsers(max_browsers)
            shared_session = PlaywrightSession(
                min_delay=min_delay,
                max_delay=max_delay,
                headless=not playwright_headed,
            )
            print(
                f"🌐 Playwright browser pool: max {max_browsers} concurrent browsers",
                flush=True,
            )
        else:
            shared_session = PoliteRequestsSession(
                min_delay=min_delay,
                max_delay=max_delay,
                snapshot_cache=snapshot_cache,
            )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            inflight: Dict[Any, tuple[str, str, float]] = {}
            inflight_domain_counts: Dict[str, int] = {}

            def _dispatch_available() -> None:
                while len(inflight) < max_workers and pending_seeds:
                    dispatched = False
                    for _ in range(len(pending_seeds)):
                        seed = pending_seeds.popleft()
                        domain = urlparse(seed).netloc or "unknown"
                        if domain in circuit_breaker_domains:
                            _mark_seed_skipped(
                                seed,
                                domain,
                                f"Skipped after {max_consecutive_seed_failures_per_domain} consecutive failed seeds for domain",
                            )
                            dispatched = True
                            continue
                        current_count = inflight_domain_counts.get(domain, 0)
                        if current_count >= max_seeds_per_domain:
                            pending_seeds.append(seed)
                            continue
                        future = executor.submit(
                            process_seed,
                            seed=seed,
                            out_dir=out_dir,
                            legacy_manifest_dir=manifest_dir,
                            run_records_path=run_paths["records_path"],
                            run_errors_path=run_paths["errors_path"],
                            links_only=links_only,
                            discovery_only=discovery_only,
                            use_playwright=use_playwright,
                            playwright_headed=playwright_headed,
                            max_depth=max_depth,
                            min_delay=min_delay,
                            max_delay=max_delay,
                            snapshot_cache=snapshot_cache,
                            dc_enum_mode=dc_enum_mode,
                            dc_use_siteindex=dc_use_siteindex,
                            dc_ua_fallback_profiles=parsed_dc_profiles,
                            dc_robots_enforce=dc_robots_enforce,
                            dc_max_oai_records=dc_max_oai_records,
                            dc_max_sitemap_urls=dc_max_sitemap_urls,
                            dc_download_timeout=dc_download_timeout,
                            dc_min_domain_delay_ms=dc_min_domain_delay_ms,
                            dc_max_domain_delay_ms=dc_max_domain_delay_ms,
                            dc_waf_fail_threshold=dc_waf_fail_threshold,
                            dc_waf_cooldown_seconds=dc_waf_cooldown_seconds,
                            dc_disable_unscoped_oai_no_slug=dc_disable_unscoped_oai_no_slug,
                            dc_session_rotate_threshold=dc_session_rotate_threshold,
                            dc_use_curl_cffi=dc_use_curl_cffi,
                            dc_round_robin_downloads=dc_round_robin_downloads,
                            write_legacy_manifests=write_legacy_manifests_enabled,
                            seed_context=seed_context_map.get(seed),
                            progress_callback=_progress_callback,
                            issue_checkpoint_dir=os.path.join(
                                run_paths["run_dir"], "checkpoints", "issues"
                            ),
                            shared_session=shared_session,
                        )
                        dispatched_at = time.time()
                        _normalize_seed_runtime(seed, domain, dispatched_at)
                        inflight[future] = (seed, domain, dispatched_at)
                        inflight_domain_counts[domain] = inflight_domain_counts.get(domain, 0) + 1
                        dispatched = True
                        break
                    if not dispatched:
                        break

            _dispatch_available()
            while inflight:
                _drain_progress_events()
                done, _ = wait(
                    tuple(inflight.keys()),
                    timeout=wait_poll_timeout_s,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    now = time.time()
                    _drain_progress_events()
                    _emit_no_pdf_gap_warning(now_ts=now, inflight=inflight)
                    oldest_entries = sorted(
                        (
                            (
                                max(now - started_at, 0.0),
                                seed,
                                _domain,
                            )
                            for _future, (seed, _domain, started_at) in inflight.items()
                        ),
                        key=lambda item: item[0],
                        reverse=True,
                    )[:3]
                    oldest_rendered = ", ".join(
                        f"{seed} ({elapsed:.0f}s)" for elapsed, seed, _domain in oldest_entries
                    )
                    print(
                        "⏳ Waiting on "
                        f"{len(inflight)} in-flight seeds; completed={summary['sites']}/{len(seeds_to_run)} "
                        f"pdfs={summary['pdfs']} (live={live_pdf_success_total}); "
                        f"oldest: {oldest_rendered or 'n/a'}",
                        flush=True,
                    )
                    continue
                for future in done:
                    seed, domain, _started_at = inflight.pop(future)
                    inflight_domain_counts[domain] = max(
                        0, inflight_domain_counts.get(domain, 1) - 1
                    )
                    state = _normalize_seed_runtime(seed, domain, _started_at)
                    state["done"] = True
                    state["last_change_at"] = max(
                        float(state.get("last_change_at") or 0.0), time.time()
                    )

                    try:
                        result = future.result()
                    except Exception as exc:
                        _append_jsonl(
                            run_paths["errors_path"],
                            {
                                "seed_url": seed,
                                "domain": domain,
                                "page_url": seed,
                                "pdf_url": None,
                                "source_adapter": "orchestrator",
                                "extraction_path": "seed",
                                "metadata": {},
                                "error_type": "seed_failure",
                                "message": str(exc),
                                "http_status": 0,
                                "attempt": 1,
                                "retries": 0,
                                "retrieved_at": _utc_now_iso(),
                            },
                        )
                        result = SeedProcessResult(
                            seed=seed,
                            domain=domain,
                            records_total=0,
                            ok_total=0,
                            errors_total=1,
                            status="failed",
                            completeness={
                                "volume_gaps": [],
                                "issue_gaps": [],
                                "issue_outliers": [],
                                "pdf_ratio": 0.0,
                                "confidence": "LOW",
                            },
                            journal_summary=_compute_seed_journal_summary(
                                seed=seed,
                                domain=domain,
                                records=[],
                                ok_total=0,
                                seed_context=seed_context_map.get(seed),
                            ),
                        )

                    # If a seed is WAF-blocked with no successful downloads, defer immediate
                    # browser fallback before failure accounting/circuit-breakers.
                    if (
                        dc_waf_browser_fallback
                        and not links_only
                        and not discovery_only
                        and int(getattr(result, "ok_total", 0) or 0) == 0
                        and int((result.completeness or {}).get("dc_pdf_blocked_count") or 0) > 0
                    ):
                        seed_waf_urls = _collect_waf_blocked_urls(
                            run_paths["errors_path"],
                            seed_url=result.seed,
                            domain=result.domain,
                            exclude_urls=processed_waf_urls,
                        )
                        if seed_waf_urls:
                            browser_results = _process_waf_browser_fallback(
                                waf_blocked_urls=seed_waf_urls,
                                out_dir=out_dir,
                                run_dir=run_paths["run_dir"],
                                records_path=run_paths["records_path"],
                                errors_path=run_paths["errors_path"],
                                backend=str(
                                    runtime_capabilities.get("browser_backend_selected") or "auto"
                                ),
                                staging_dir=dc_browser_staging_dir,
                                user_data_dir=dc_browser_user_data_dir,
                                timeout=dc_browser_timeout,
                                headless=dc_browser_headless,
                                operator_config=operator_config,
                            )
                            recovered = int(browser_results.get("succeeded") or 0)
                            summary["browser_fallback_attempted"] = int(
                                summary.get("browser_fallback_attempted") or 0
                            ) + len(seed_waf_urls)
                            summary["browser_fallback_succeeded"] = (
                                int(summary.get("browser_fallback_succeeded") or 0) + recovered
                            )
                            summary["operator_interventions_prompted"] = int(
                                summary.get("operator_interventions_prompted") or 0
                            ) + int(browser_results.get("operator_prompted") or 0)
                            summary["operator_interventions_retried"] = int(
                                summary.get("operator_interventions_retried") or 0
                            ) + int(browser_results.get("operator_retried") or 0)
                            summary["operator_interventions_recovered"] = int(
                                summary.get("operator_interventions_recovered") or 0
                            ) + int(browser_results.get("operator_recovered") or 0)
                            summary["operator_interventions_unresolved"] = int(
                                summary.get("operator_interventions_unresolved") or 0
                            ) + int(browser_results.get("operator_unresolved") or 0)
                            for item in seed_waf_urls:
                                url = str(item.get("url") or "")
                                if url:
                                    processed_waf_urls.add(url)
                            if recovered > 0:
                                result.ok_total = int(result.ok_total or 0) + recovered
                                result.completeness = dict(result.completeness or {})
                                result.completeness["browser_fallback_recovered"] = recovered

                    _apply_result_to_summary(result)
                    _track_todo_adapter_block(result)
                    seed_results[result.seed] = result
                    blocked_count = int(
                        (result.completeness or {}).get("dc_pdf_blocked_count") or 0
                    )
                    if blocked_count > 0:
                        print(
                            f"✅ Finished {seed} (Found {result.ok_total} PDFs, blocked {blocked_count})"
                        )
                    else:
                        print(f"✅ Finished {seed} (Found {result.ok_total} PDFs)")
                    _record_stats_for_result(result)

                    domain_key = result.domain or domain
                    if _seed_counts_as_failure(
                        result,
                        links_only=(links_only or discovery_only),
                    ):
                        domain_failure_streak[domain_key] = (
                            domain_failure_streak.get(domain_key, 0) + 1
                        )
                    else:
                        domain_failure_streak[domain_key] = 0

                    streak = domain_failure_streak.get(domain_key, 0)
                    if (
                        max_consecutive_seed_failures_per_domain > 0
                        and streak >= max_consecutive_seed_failures_per_domain
                        and domain_key not in circuit_breaker_domains
                    ):
                        circuit_breaker_domains.add(domain_key)
                        skipped_now = 0
                        retained = deque()
                        reason = f"Skipped after {streak} consecutive failed seeds for domain"
                        while pending_seeds:
                            pending_seed = pending_seeds.popleft()
                            pending_domain = urlparse(pending_seed).netloc or "unknown"
                            if pending_domain == domain_key:
                                _mark_seed_skipped(pending_seed, pending_domain, reason)
                                skipped_now += 1
                            else:
                                retained.append(pending_seed)
                        pending_seeds.extend(retained)
                        if skipped_now:
                            print(
                                f"⚠️  Circuit breaker active for {domain_key}: "
                                f"skipped {skipped_now} remaining seeds after {streak} consecutive failures."
                            )

                _dispatch_available()

        _drain_progress_events()
        summary["stalled_seeds_observed"] = len(stalled_seed_candidates)

        phase2_retry_candidates: List[str] = []
        if (
            retry_stalled_seeds
            and stalled_seed_candidates
            and not links_only
            and not discovery_only
        ):
            for stalled_seed in sorted(stalled_seed_candidates):
                latest = seed_results.get(stalled_seed)
                if not latest:
                    continue
                if int(latest.ok_total or 0) > 0:
                    continue
                if int((latest.completeness or {}).get("dc_download_deferred_count") or 0) > 0:
                    continue
                phase2_retry_candidates.append(stalled_seed)

        if phase2_retry_candidates:
            summary["phase2_retry_attempted_seeds"] = len(phase2_retry_candidates)
            retry_depth = max(max_depth, max(int(stalled_retry_max_depth or 0), 0))
            retry_min_delay = max(min_delay, 1.5)
            retry_max_delay = max(max_delay, 4.0)
            print(
                "\n🔁 Phase 2 retry pass: "
                f"{len(phase2_retry_candidates)} stalled no-PDF seeds "
                f"(playwright=on, depth={retry_depth}, delay={retry_min_delay}-{retry_max_delay}s)",
                flush=True,
            )

            for retry_seed in phase2_retry_candidates:
                retry_domain = urlparse(retry_seed).netloc or "unknown"
                print(f"🔁 Retrying stalled seed {retry_seed}", flush=True)
                previous_ok = int(seed_ok_totals.get(retry_seed) or 0)
                retry_result = process_seed(
                    seed=retry_seed,
                    out_dir=out_dir,
                    legacy_manifest_dir=manifest_dir,
                    run_records_path=run_paths["records_path"],
                    run_errors_path=run_paths["errors_path"],
                    links_only=links_only,
                    discovery_only=discovery_only,
                    use_playwright=True,
                    playwright_headed=playwright_headed,
                    max_depth=retry_depth,
                    min_delay=retry_min_delay,
                    max_delay=retry_max_delay,
                    snapshot_cache=snapshot_cache,
                    dc_enum_mode=dc_enum_mode,
                    dc_use_siteindex=dc_use_siteindex,
                    dc_ua_fallback_profiles=parsed_dc_profiles,
                    dc_robots_enforce=dc_robots_enforce,
                    dc_max_oai_records=dc_max_oai_records,
                    dc_max_sitemap_urls=dc_max_sitemap_urls,
                    dc_download_timeout=dc_download_timeout,
                    dc_min_domain_delay_ms=dc_min_domain_delay_ms,
                    dc_max_domain_delay_ms=dc_max_domain_delay_ms,
                    dc_waf_fail_threshold=dc_waf_fail_threshold,
                    dc_waf_cooldown_seconds=dc_waf_cooldown_seconds,
                    dc_disable_unscoped_oai_no_slug=dc_disable_unscoped_oai_no_slug,
                    dc_session_rotate_threshold=dc_session_rotate_threshold,
                    dc_use_curl_cffi=dc_use_curl_cffi,
                    dc_round_robin_downloads=False,
                    write_legacy_manifests=write_legacy_manifests_enabled,
                    seed_context=seed_context_map.get(retry_seed),
                    progress_callback=_progress_callback,
                    issue_checkpoint_dir=os.path.join(
                        run_paths["run_dir"], "checkpoints", "issues"
                    ),
                    shared_session=shared_session,
                )
                _apply_result_to_summary(retry_result)
                _track_todo_adapter_block(retry_result)
                seed_results[retry_seed] = retry_result
                _record_stats_for_result(retry_result)
                _drain_progress_events()

                added = max(int(retry_result.ok_total or 0) - previous_ok, 0)
                if added > 0:
                    summary["phase2_retry_total_added_pdfs"] = (
                        int(summary.get("phase2_retry_total_added_pdfs") or 0) + added
                    )
                if previous_ok == 0 and int(retry_result.ok_total or 0) > 0:
                    summary["phase2_retry_recovered_seeds"] = (
                        int(summary.get("phase2_retry_recovered_seeds") or 0) + 1
                    )
                print(
                    f"✅ Phase 2 finished {retry_domain} (Found {retry_result.ok_total} PDFs)",
                    flush=True,
                )

        summary["dc_round_robin_enabled"] = bool(
            summary.get("dc_round_robin_enabled")
            and int(summary.get("dc_round_robin_eligible_records") or 0) > 0
        )

        if bool(summary.get("dc_round_robin_enabled")):
            print(
                "\n🔁 DC round-robin download phase: "
                f"strict_first_pass={bool(dc_round_robin_strict_first_pass)} "
                f"revisit_interval={max(int(dc_round_robin_revisit_interval_seconds or 0), 0)}s",
                flush=True,
            )
            dc_download_summary = _run_download_from_manifest(
                run_id=actual_run_id,
                manifest_dir=manifest_dir,
                out_dir=out_dir,
                max_workers=max_workers,
                max_downloads_per_domain=max_downloads_per_domain,
                min_delay=min_delay,
                max_delay=max_delay,
                dc_waf_browser_fallback=dc_waf_browser_fallback,
                browser_backend=str(runtime_capabilities.get("browser_backend_selected") or "auto"),
                browser_backend_available=bool(
                    runtime_capabilities.get("browser_backend_available")
                ),
                browser_staging_dir=dc_browser_staging_dir,
                browser_user_data_dir=dc_browser_user_data_dir,
                browser_timeout=dc_browser_timeout,
                browser_headless=dc_browser_headless,
                target_scope="dc_only",
                strict_first_pass=bool(dc_round_robin_strict_first_pass),
                domain_revisit_interval_seconds=max(
                    int(dc_round_robin_revisit_interval_seconds or 0), 0
                ),
                operator_config=operator_config,
            )
            summary["dc_round_robin_attempted"] = int(
                dc_download_summary.get("download_phase_attempted") or 0
            )
            summary["dc_round_robin_succeeded"] = int(
                dc_download_summary.get("download_phase_succeeded") or 0
            )
            summary["dc_round_robin_failed"] = int(
                dc_download_summary.get("download_phase_failed") or 0
            )
            summary["dc_round_robin_domains"] = int(
                dc_download_summary.get("download_phase_domains") or 0
            )
            summary["dc_round_robin_first_pass_completed"] = bool(
                dc_download_summary.get("download_phase_first_pass_completed")
            )
            summary["dc_round_robin_first_pass_completed_at"] = str(
                dc_download_summary.get("download_phase_first_pass_completed_at") or ""
            )
            summary["operator_interventions_prompted"] = int(
                summary.get("operator_interventions_prompted") or 0
            ) + int(dc_download_summary.get("operator_interventions_prompted") or 0)
            summary["operator_interventions_retried"] = int(
                summary.get("operator_interventions_retried") or 0
            ) + int(dc_download_summary.get("operator_interventions_retried") or 0)
            summary["operator_interventions_recovered"] = int(
                summary.get("operator_interventions_recovered") or 0
            ) + int(dc_download_summary.get("operator_interventions_recovered") or 0)
            summary["operator_interventions_unresolved"] = int(
                summary.get("operator_interventions_unresolved") or 0
            ) + int(dc_download_summary.get("operator_interventions_unresolved") or 0)
            refreshed_ok_totals = _count_ok_downloads_by_seed(run_paths["records_path"])
            for seed in seeds_to_run:
                seed_ok_totals[seed] = int(refreshed_ok_totals.get(seed, 0))
            summary["pdfs"] = int(sum(seed_ok_totals.get(seed, 0) for seed in seeds_to_run))

        seeds_with_pdfs = sum(1 for seed in seeds_to_run if int(seed_ok_totals.get(seed) or 0) > 0)
        summary["seeds_with_pdfs"] = seeds_with_pdfs
        summary["seeds_without_pdfs"] = max(len(seeds_to_run) - seeds_with_pdfs, 0)

        no_pdf_reasons: Dict[str, int] = {}
        for seed in seeds_to_run:
            if int(seed_ok_totals.get(seed) or 0) > 0:
                continue
            seed_stats = stats_payload.get("seeds", {}).get(seed)
            if not isinstance(seed_stats, dict):
                reason = "unknown"
            else:
                status = str(seed_stats.get("status") or "unknown")
                records_total = int(seed_stats.get("records_total") or 0)
                if status == "completed" and records_total == 0:
                    reason = "completed_no_candidates"
                elif status == "completed":
                    reason = "completed_download_failures"
                else:
                    reason = status or "unknown"
            no_pdf_reasons[reason] = no_pdf_reasons.get(reason, 0) + 1
        summary["no_pdf_seeds_by_reason"] = no_pdf_reasons
        summary["todo_adapter_blocked_seeds"] = len(todo_adapter_blocked_seed_set)
        summary["todo_adapter_hosts"] = len(todo_adapter_blocked_host_set)
        summary["todo_adapter_host_list"] = sorted(todo_adapter_blocked_host_set)

        if summary["seeds_without_pdfs"] > 0:
            reason_rendered = ", ".join(
                f"{k}={v}" for k, v in sorted(no_pdf_reasons.items(), key=lambda item: item[0])
            )
            print(
                f"📊 Completeness summary: seeds_with_pdfs={summary['seeds_with_pdfs']} "
                f"seeds_without_pdfs={summary['seeds_without_pdfs']} "
                f"reasons=[{reason_rendered}]",
                flush=True,
            )

        # Calculate WAF statistics before browser fallback
        waf_stats = _count_waf_stats(run_paths["errors_path"])
        summary["waf_blocked"] = waf_stats["waf_blocked"]
        summary["waf_domains_affected"] = len(waf_stats["domains_affected"])

        if waf_stats["waf_blocked"] > 0:
            print(
                f"\n📊 WAF Statistics: {waf_stats['waf_blocked']} blocked URLs across "
                f"{len(waf_stats['domains_affected'])} domains",
                flush=True,
            )

        # Browser fallback for WAF-blocked PDFs
        if dc_waf_browser_fallback and not links_only and not discovery_only:
            waf_blocked = _collect_waf_blocked_urls(
                run_paths["errors_path"],
                exclude_urls=processed_waf_urls,
            )
            if waf_blocked:
                browser_results = _process_waf_browser_fallback(
                    waf_blocked_urls=waf_blocked,
                    out_dir=out_dir,
                    run_dir=run_paths["run_dir"],
                    records_path=run_paths["records_path"],
                    errors_path=run_paths["errors_path"],
                    backend=dc_browser_backend,
                    staging_dir=dc_browser_staging_dir,
                    user_data_dir=dc_browser_user_data_dir,
                    timeout=dc_browser_timeout,
                    headless=dc_browser_headless,
                    operator_config=operator_config,
                )
                summary["browser_fallback_attempted"] = int(
                    summary.get("browser_fallback_attempted") or 0
                ) + len(waf_blocked)
                summary["browser_fallback_succeeded"] = int(
                    summary.get("browser_fallback_succeeded") or 0
                ) + int(browser_results.get("succeeded") or 0)
                summary["operator_interventions_prompted"] = int(
                    summary.get("operator_interventions_prompted") or 0
                ) + int(browser_results.get("operator_prompted") or 0)
                summary["operator_interventions_retried"] = int(
                    summary.get("operator_interventions_retried") or 0
                ) + int(browser_results.get("operator_retried") or 0)
                summary["operator_interventions_recovered"] = int(
                    summary.get("operator_interventions_recovered") or 0
                ) + int(browser_results.get("operator_recovered") or 0)
                summary["operator_interventions_unresolved"] = int(
                    summary.get("operator_interventions_unresolved") or 0
                ) + int(browser_results.get("operator_unresolved") or 0)
                summary["pdfs"] += browser_results.get("succeeded", 0)

        # Count curl_cffi downloads
        summary["curl_cffi_downloads"] = _count_curl_cffi_downloads(run_paths["records_path"])
        if summary["curl_cffi_downloads"] > 0:
            print(
                f"📊 curl_cffi downloads: {summary['curl_cffi_downloads']} successful",
                flush=True,
            )

        stats_payload["journals"] = _rebuild_journal_summaries_from_records(
            run_paths["records_path"]
        )
        stats_payload["updated_at"] = _utc_now_iso()
        _write_json(run_paths["stats_path"], stats_payload)

        manifest_payload = _read_json(run_paths["manifest_path"], {})
        manifest_payload["status"] = "completed"
        manifest_payload["finished_at"] = _utc_now_iso()
        manifest_payload["summary"] = summary

        # Informational coverage summary — not blocking.
        try:
            journal_data = stats_payload.get("journals") or {}
            domains_with_gaps = 0
            confidence_dist: Dict[str, int] = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
            total_domains = len(journal_data)
            total_pdfs = summary.get("pdfs", 0)
            for _jname, jstats in journal_data.items():
                ok_total = jstats.get("ok_total", 0)
                if ok_total >= 50:
                    confidence_dist["HIGH"] += 1
                elif ok_total >= 10:
                    confidence_dist["MEDIUM"] += 1
                else:
                    confidence_dist["LOW"] += 1
                if jstats.get("has_volume_gaps"):
                    domains_with_gaps += 1
            manifest_payload["coverage_summary"] = {
                "total_domains": total_domains,
                "total_pdfs": total_pdfs,
                "domains_with_volume_gaps": domains_with_gaps,
                "confidence_distribution": confidence_dist,
            }
        except Exception:
            pass  # Coverage summary is best-effort

        _write_json(run_paths["manifest_path"], manifest_payload)

    except KeyboardInterrupt:
        stats_payload["journals"] = _rebuild_journal_summaries_from_records(
            run_paths["records_path"]
        )
        stats_payload["updated_at"] = _utc_now_iso()
        _write_json(run_paths["stats_path"], stats_payload)

        manifest_payload = _read_json(run_paths["manifest_path"], {})
        manifest_payload["status"] = "interrupted"
        manifest_payload["finished_at"] = _utc_now_iso()
        manifest_payload["summary"] = summary
        _write_json(run_paths["manifest_path"], manifest_payload)
        raise

    finally:
        # Close the shared session (and release any remaining browser pool slots).
        if shared_session is not None and hasattr(shared_session, "close"):
            try:
                shared_session.close()
            except Exception:
                pass

    return summary


def _rebuild_journal_summaries_from_records(records_path: str) -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(records_path):
        return {}

    journals: Dict[str, Dict[str, Any]] = {}
    with open(records_path, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            domain = str(record.get("domain") or "")
            metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            metadata = metadata or {}
            journal_name = _normalize_journal_name(
                _extract_journal_name(metadata) or domain or record.get("seed_url") or ""
            )
            journal_key = _journal_key(
                journal_name or domain or record.get("seed_url") or "unknown"
            )
            group = journals.setdefault(
                journal_key,
                {
                    "records": [],
                    "domains": set(),
                    "seeds": set(),
                    "journal_name": journal_name or domain or "",
                },
            )
            group["records"].append(record)
            if domain:
                group["domains"].add(domain)
            seed_url = record.get("seed_url")
            if seed_url:
                group["seeds"].add(seed_url)

    aggregated: Dict[str, Dict[str, Any]] = {}
    for key, group in journals.items():
        records = group["records"]
        ok_total = sum(1 for rec in records if bool(rec.get("ok")))
        domain = next(iter(group["domains"]), records[0].get("domain") if records else "")
        seed_url = next(iter(group["seeds"]), records[0].get("seed_url") if records else "")
        summary = _compute_seed_journal_summary(
            seed=seed_url,
            domain=domain,
            records=records,
            ok_total=ok_total,
            seed_context=None,
        )
        aggregated[key] = {
            "journal_name": summary.get("journal_name"),
            "domains": sorted(group["domains"]),
            "articles_total": summary["articles_total"],
            "articles_with_date_total": summary["articles_with_date_total"],
            "earliest_date": summary.get("earliest_date"),
            "earliest_year": summary.get("earliest_year"),
            "latest_date": summary.get("latest_date"),
            "latest_year": summary.get("latest_year"),
            "pdf_records_total": summary["pdf_records_total"],
            "pdfs_downloaded_total": summary["pdfs_downloaded_total"],
            "pdfs_failed_total": summary["pdfs_failed_total"],
            "seeds": len(group["seeds"]),
        }

    return aggregated


def main() -> None:
    parser = argparse.ArgumentParser(description="Law review PDF orchestrator")
    parser.add_argument("--sitemaps", required=False, help="Path to law_review_sitemaps.csv")
    parser.add_argument("--sitemaps-dir", required=False, help="Directory with JSON sitemap files")
    parser.add_argument(
        "--seed",
        action="append",
        help="Seed URL to scrape (can be given multiple times; overrides --sitemaps)",
    )
    parser.add_argument("--out", default="artifacts/pdfs", help="Output directory for PDFs")
    parser.add_argument(
        "--manifest", default="artifacts/runs", help="Directory for JSONL manifests"
    )
    parser.add_argument("--max-depth", type=int, default=0, help="Link-follow depth for discovery")
    parser.add_argument(
        "--links-only",
        action="store_true",
        help="Do not download PDFs; only record discovered PDF links to manifests",
    )
    parser.add_argument(
        "--discovery-only",
        action="store_true",
        help=(
            "Discover and write records with pending download_state, but do not download PDFs. "
            "Use with --download-from-manifest for two-phase runs."
        ),
    )
    parser.add_argument(
        "--download-from-manifest",
        help=(
            "Run download phase against an existing run_id under --manifest "
            "(reads and updates records.jsonl in place)."
        ),
    )
    parser.add_argument(
        "--use-playwright",
        dest="use_playwright",
        action="store_true",
        default=True,
        help="Use Playwright browser instead of requests for main scraping (default: enabled)",
    )
    parser.add_argument(
        "--no-use-playwright",
        dest="use_playwright",
        action="store_false",
        help="Disable Playwright for main scraping and use requests session",
    )
    parser.add_argument(
        "--playwright-headed",
        dest="playwright_headed",
        action="store_true",
        default=True,
        help="Run main --use-playwright browser in headed mode (default: enabled)",
    )
    parser.add_argument(
        "--playwright-headless",
        dest="playwright_headed",
        action="store_false",
        help="Run main --use-playwright browser in headless mode",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=2.0,
        help="Minimum delay between requests in seconds (default: 2.0)",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=5.0,
        help="Maximum delay between requests in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Number of parallel worker threads (default: 4)",
    )
    parser.add_argument(
        "--max-consecutive-seed-failures-per-domain",
        type=int,
        default=3,
        help=(
            "Stop scheduling additional seeds for a domain after this many consecutive "
            "failed seeds (default: 3, set 0 to disable)"
        ),
    )
    parser.add_argument(
        "--max-seeds-per-domain",
        type=int,
        default=3,
        help=(
            "Maximum concurrent seeds to run per domain (default: 3). "
            "Higher values speed up multi-seed domains but increase load."
        ),
    )
    parser.add_argument(
        "--max-downloads-per-domain",
        type=int,
        default=1,
        help=(
            "Maximum concurrent in-flight downloads per domain in --download-from-manifest mode "
            "(default: 1)"
        ),
    )
    parser.add_argument(
        "--dc-round-robin-downloads",
        dest="dc_round_robin_downloads",
        action="store_true",
        default=True,
        help=(
            "Defer Digital Commons PDF downloads and run a DC-only round-robin download phase "
            "(default: enabled)"
        ),
    )
    parser.add_argument(
        "--no-dc-round-robin-downloads",
        dest="dc_round_robin_downloads",
        action="store_false",
        help="Disable DC-only round-robin and download Digital Commons PDFs inline",
    )
    parser.add_argument(
        "--dc-round-robin-strict-first-pass",
        dest="dc_round_robin_strict_first_pass",
        action="store_true",
        default=True,
        help=(
            "During DC round-robin phase, require one dispatch per DC domain before a second "
            "dispatch is allowed (default: enabled)"
        ),
    )
    parser.add_argument(
        "--no-dc-round-robin-strict-first-pass",
        dest="dc_round_robin_strict_first_pass",
        action="store_false",
        help="Disable strict first-pass behavior in DC round-robin mode",
    )
    parser.add_argument(
        "--dc-round-robin-revisit-interval-seconds",
        type=int,
        default=90,
        help=(
            "Minimum seconds between dispatches for the same DC domain in round-robin phase "
            "(default: 90)"
        ),
    )
    parser.add_argument(
        "--skip-well-covered-seeds",
        dest="skip_well_covered_seeds",
        action="store_true",
        default=True,
        help=(
            "Skip active seeds for domains that already have at least "
            "--well-covered-pdf-threshold PDFs in --out (default: enabled)"
        ),
    )
    parser.add_argument(
        "--no-skip-well-covered-seeds",
        dest="skip_well_covered_seeds",
        action="store_false",
        help="Do not skip domains with existing substantial PDF coverage",
    )
    parser.add_argument(
        "--skip-dc-sites",
        dest="skip_dc_sites",
        action="store_true",
        default=False,
        help="Skip Digital Commons seeds entirely for this run",
    )
    parser.add_argument(
        "--no-skip-dc-sites",
        dest="skip_dc_sites",
        action="store_false",
        help="Include Digital Commons seeds (default)",
    )
    parser.add_argument(
        "--operator-mode",
        dest="operator_mode",
        action="store_true",
        default=False,
        help=(
            "Enable operator-monitored mode for headed runs "
            "(forces serial scheduling and browser fallback)."
        ),
    )
    parser.add_argument(
        "--operator-intervention-scope",
        choices=["off", "browser_fallback_only"],
        default="off",
        help=(
            "Scope for manual operator intervention prompts "
            "(default: off; operator mode coerces to browser_fallback_only)."
        ),
    )
    parser.add_argument(
        "--operator-wait-mode",
        choices=["prompt_enter", "off"],
        default="off",
        help="How to wait for manual captcha/auth intervention (default: off).",
    )
    parser.add_argument(
        "--operator-manual-retries",
        type=int,
        default=1,
        help="Manual retry attempts per browser fallback URL after operator prompt (default: 1).",
    )
    parser.add_argument(
        "--well-covered-pdf-threshold",
        type=int,
        default=250,
        help=("PDF count threshold used by --skip-well-covered-seeds " "(default: 250)"),
    )
    parser.add_argument(
        "--no-pdf-progress-timeout-seconds",
        type=int,
        default=60,
        help=(
            "Emit watchdog warning when no successful PDF download has occurred for this many "
            "seconds while seeds are still in-flight (default: 60)"
        ),
    )
    parser.add_argument(
        "--stalled-seed-timeout-seconds",
        type=int,
        default=60,
        help=(
            "Mark in-flight seeds as stalled when their progress counters have not changed for "
            "this many seconds (default: 60)"
        ),
    )
    parser.add_argument(
        "--retry-stalled-seeds",
        dest="retry_stalled_seeds",
        action="store_true",
        default=True,
        help=(
            "After phase 1, retry stalled no-PDF seeds with heavier settings "
            "(Playwright + deeper crawl)"
        ),
    )
    parser.add_argument(
        "--no-retry-stalled-seeds",
        dest="retry_stalled_seeds",
        action="store_false",
        help="Disable phase 2 stalled-seed retries",
    )
    parser.add_argument(
        "--stalled-retry-max-depth",
        type=int,
        default=1,
        help="Max depth for phase 2 stalled-seed retries (default: 1)",
    )
    parser.add_argument(
        "--write-legacy-manifests",
        dest="write_legacy_manifests",
        action="store_true",
        default=None,
        help=(
            "Also write legacy per-domain manifests (<manifest>/<domain>.jsonl). "
            "Default: disabled."
        ),
    )
    parser.add_argument(
        "--no-write-legacy-manifests",
        dest="write_legacy_manifests",
        action="store_false",
        help="Disable legacy per-domain manifest writing (default).",
    )
    parser.add_argument(
        "--run-id", help="Optional explicit run id for artifact directory <manifest>/<run_id>"
    )
    parser.add_argument("--resume", help="Resume an incomplete run id")
    parser.add_argument(
        "--cache-dir",
        default="artifacts/cache/http",
        help="Directory for raw HTTP snapshot cache (default: artifacts/cache/http)",
    )
    parser.add_argument(
        "--cache-ttl-hours",
        type=int,
        default=24,
        help="HTTP cache TTL in hours (default: 24)",
    )
    parser.add_argument(
        "--cache-max-bytes",
        type=int,
        default=2_147_483_648,
        help="HTTP cache max total bytes before eviction (default: 2147483648)",
    )
    parser.add_argument(
        "--dc-enum-mode",
        choices=["oai_sitemap_union", "oai_only", "sitemap_only"],
        default="oai_sitemap_union",
        help="Digital Commons enumeration mode (default: oai_sitemap_union)",
    )
    parser.add_argument(
        "--dc-use-siteindex",
        dest="dc_use_siteindex",
        action="store_true",
        default=True,
        help="Use /siteindex.xml as first Digital Commons sitemap root",
    )
    parser.add_argument(
        "--no-dc-use-siteindex",
        dest="dc_use_siteindex",
        action="store_false",
        help="Disable /siteindex.xml probing for Digital Commons sitemap discovery",
    )
    parser.add_argument(
        "--dc-ua-fallback-profiles",
        default="browser,transparent,python_requests,wget,curl",
        help="Comma-separated UA profiles for Digital Commons PDF fallback order",
    )
    parser.add_argument(
        "--dc-robots-enforce",
        dest="dc_robots_enforce",
        action="store_true",
        default=True,
        help="Enforce robots.txt before Digital Commons PDF downloads",
    )
    parser.add_argument(
        "--no-dc-robots-enforce",
        dest="dc_robots_enforce",
        action="store_false",
        help="Disable robots.txt enforcement for Digital Commons downloads",
    )
    parser.add_argument(
        "--dc-max-oai-records",
        type=int,
        default=0,
        help="Digital Commons OAI max records per seed (0 = unbounded)",
    )
    parser.add_argument(
        "--dc-max-sitemap-urls",
        type=int,
        default=0,
        help="Digital Commons sitemap max article URLs per seed (0 = unbounded)",
    )
    parser.add_argument(
        "--dc-download-timeout",
        type=int,
        default=30,
        help="Digital Commons download timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--dc-min-domain-delay-ms",
        type=int,
        default=1000,
        help="Digital Commons min delay per request in ms (default: 1000)",
    )
    parser.add_argument(
        "--dc-max-domain-delay-ms",
        type=int,
        default=2000,
        help="Digital Commons max delay per request in ms (default: 2000)",
    )
    parser.add_argument(
        "--dc-waf-fail-threshold",
        type=int,
        default=3,
        help="Open per-host Digital Commons WAF circuit after N WAF blocks (default: 3)",
    )
    parser.add_argument(
        "--dc-waf-cooldown-seconds",
        type=int,
        default=900,
        help="Cooldown before retrying a host with open Digital Commons WAF circuit (default: 900)",
    )
    parser.add_argument(
        "--dc-disable-unscoped-oai-no-slug",
        dest="dc_disable_unscoped_oai_no_slug",
        action="store_true",
        default=True,
        help="Disable unscoped OAI on root Digital Commons seeds without a journal slug",
    )
    parser.add_argument(
        "--no-dc-disable-unscoped-oai-no-slug",
        dest="dc_disable_unscoped_oai_no_slug",
        action="store_false",
        help="Allow unscoped OAI fallback for root Digital Commons seeds without a journal slug",
    )
    parser.add_argument(
        "--dc-session-rotate-threshold",
        type=int,
        default=300,
        help=(
            "Rotate session cookies after N requests per host to prevent WAF session "
            "invalidation (default: 300, set 0 to disable)"
        ),
    )
    parser.add_argument(
        "--dc-use-curl-cffi",
        dest="dc_use_curl_cffi",
        action="store_true",
        default=True,
        help="Use curl_cffi for TLS fingerprint impersonation (Chrome 124) when available",
    )
    parser.add_argument(
        "--no-dc-use-curl-cffi",
        dest="dc_use_curl_cffi",
        action="store_false",
        help="Disable curl_cffi TLS fingerprint impersonation (use standard requests)",
    )
    parser.add_argument(
        "--dc-waf-browser-fallback",
        dest="dc_waf_browser_fallback",
        action="store_true",
        default=False,
        help=(
            "Enable browser-based fallback for WAF-blocked PDFs. "
            "WAF-blocked URLs are collected and batch-processed at end of run."
        ),
    )
    parser.add_argument(
        "--dc-browser-backend",
        choices=["auto", "camoufox", "playwright", "chrome_mcp"],
        default="auto",
        help=(
            "Browser backend for WAF fallback: "
            "'camoufox' (recommended, anti-detection Firefox), "
            "'playwright' (local Chromium browser), "
            "'chrome_mcp' (Chrome DevTools MCP), or "
            "'auto' (use camoufox if available, otherwise playwright) (default: auto)"
        ),
    )
    parser.add_argument(
        "--dc-browser-staging-dir",
        default="artifacts/browser_staging",
        help="Staging directory for browser downloads (default: artifacts/browser_staging)",
    )
    parser.add_argument(
        "--dc-browser-user-data-dir",
        default="artifacts/browser_profile",
        help="Browser profile directory for session persistence (default: artifacts/browser_profile)",
    )
    parser.add_argument(
        "--dc-browser-timeout",
        type=int,
        default=60,
        help="Browser download timeout in seconds (default: 60)",
    )
    parser.add_argument(
        "--dc-browser-headless",
        dest="dc_browser_headless",
        action="store_true",
        default=False,
        help="Run browser in headless mode (default: visible for debugging)",
    )

    args = parser.parse_args()

    summary = run_orchestrator(
        sitemaps_csv=args.sitemaps,
        sitemaps_dir=args.sitemaps_dir,
        out_dir=args.out,
        manifest_dir=args.manifest,
        max_depth=args.max_depth,
        seeds_override=args.seed,
        links_only=args.links_only,
        discovery_only=args.discovery_only,
        download_from_manifest=args.download_from_manifest,
        use_playwright=args.use_playwright,
        playwright_headed=args.playwright_headed,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_workers=args.max_workers,
        max_seeds_per_domain=args.max_seeds_per_domain,
        max_downloads_per_domain=args.max_downloads_per_domain,
        skip_dc_sites=args.skip_dc_sites,
        dc_round_robin_downloads=args.dc_round_robin_downloads,
        dc_round_robin_strict_first_pass=args.dc_round_robin_strict_first_pass,
        dc_round_robin_revisit_interval_seconds=args.dc_round_robin_revisit_interval_seconds,
        skip_well_covered_seeds=args.skip_well_covered_seeds,
        well_covered_pdf_threshold=args.well_covered_pdf_threshold,
        run_id=args.run_id,
        resume=args.resume,
        cache_dir=args.cache_dir,
        cache_ttl_hours=args.cache_ttl_hours,
        cache_max_bytes=args.cache_max_bytes,
        dc_enum_mode=args.dc_enum_mode,
        dc_use_siteindex=args.dc_use_siteindex,
        dc_ua_fallback_profiles=args.dc_ua_fallback_profiles,
        dc_robots_enforce=args.dc_robots_enforce,
        dc_max_oai_records=args.dc_max_oai_records,
        dc_max_sitemap_urls=args.dc_max_sitemap_urls,
        dc_download_timeout=args.dc_download_timeout,
        dc_min_domain_delay_ms=args.dc_min_domain_delay_ms,
        dc_max_domain_delay_ms=args.dc_max_domain_delay_ms,
        dc_waf_fail_threshold=args.dc_waf_fail_threshold,
        dc_waf_cooldown_seconds=args.dc_waf_cooldown_seconds,
        dc_disable_unscoped_oai_no_slug=args.dc_disable_unscoped_oai_no_slug,
        dc_session_rotate_threshold=args.dc_session_rotate_threshold,
        dc_use_curl_cffi=args.dc_use_curl_cffi,
        dc_waf_browser_fallback=args.dc_waf_browser_fallback,
        dc_browser_backend=args.dc_browser_backend,
        dc_browser_staging_dir=args.dc_browser_staging_dir,
        dc_browser_user_data_dir=args.dc_browser_user_data_dir,
        dc_browser_timeout=args.dc_browser_timeout,
        dc_browser_headless=args.dc_browser_headless,
        max_consecutive_seed_failures_per_domain=args.max_consecutive_seed_failures_per_domain,
        no_pdf_progress_timeout_seconds=args.no_pdf_progress_timeout_seconds,
        stalled_seed_timeout_seconds=args.stalled_seed_timeout_seconds,
        retry_stalled_seeds=args.retry_stalled_seeds,
        stalled_retry_max_depth=args.stalled_retry_max_depth,
        operator_mode=args.operator_mode,
        operator_intervention_scope=args.operator_intervention_scope,
        operator_wait_mode=args.operator_wait_mode,
        operator_manual_retries=args.operator_manual_retries,
        write_legacy_manifests=args.write_legacy_manifests,
    )
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
