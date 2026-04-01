#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Allow running as `python scripts/run_pipeline.py` from anywhere.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import (
    DEFAULT_EXPORT_DIR,
    DEFAULT_PDF_ROOT,
    DEFAULT_RUNS_DIR,
    DEFAULT_SITEMAPS_DIR,
)
from offprint.orchestrator import run_orchestrator
from offprint.retry_queue import retry_failed


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Canonical production pipeline runner (full, delta, retry)"
    )
    parser.add_argument("--mode", choices=["full", "delta", "retry"], default="full")
    parser.add_argument("--sitemaps-dir", default=DEFAULT_SITEMAPS_DIR)
    parser.add_argument("--out-dir", default=DEFAULT_PDF_ROOT)
    parser.add_argument("--manifest-dir", default=DEFAULT_RUNS_DIR)
    parser.add_argument("--export-dir", default=DEFAULT_EXPORT_DIR)
    parser.add_argument("--max-depth", type=int, default=0)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument(
        "--max-seeds-per-domain",
        type=int,
        default=3,
        help=(
            "Maximum concurrent seeds for the same domain "
            "(default: 3; set 1 for strict per-domain serial execution)"
        ),
    )
    parser.add_argument(
        "--max-consecutive-seed-failures-per-domain",
        type=int,
        default=3,
        help=(
            "Stop scheduling additional seeds for a domain after this many consecutive "
            "failed seeds (set 0 to disable)"
        ),
    )
    parser.add_argument(
        "--no-pdf-progress-timeout-seconds",
        type=int,
        default=60,
        help=(
            "Emit watchdog warning when no successful PDF has been downloaded for this many "
            "seconds while work is still active (default: 60)"
        ),
    )
    parser.add_argument(
        "--stalled-seed-timeout-seconds",
        type=int,
        default=60,
        help=(
            "Mark a seed as stalled when its progress counters have not changed for this many "
            "seconds (default: 60)"
        ),
    )
    parser.add_argument(
        "--retry-stalled-seeds",
        dest="retry_stalled_seeds",
        action="store_true",
        default=True,
        help=(
            "Run phase 2 targeted retries for stalled no-PDF seeds with heavier settings "
            "(default: enabled)"
        ),
    )
    parser.add_argument(
        "--no-retry-stalled-seeds",
        dest="retry_stalled_seeds",
        action="store_false",
        help="Disable phase 2 stalled no-PDF retries",
    )
    parser.add_argument(
        "--stalled-retry-max-depth",
        type=int,
        default=1,
        help="Max depth for phase 2 stalled-seed retries (default: 1)",
    )
    parser.add_argument("--min-delay", type=float, default=1.5)
    parser.add_argument("--max-delay", type=float, default=4.0)
    parser.add_argument("--cache-dir", default="artifacts/cache/http")
    parser.add_argument("--cache-ttl-hours", type=int, default=24)
    parser.add_argument("--cache-max-bytes", type=int, default=2_147_483_648)
    parser.add_argument("--run-id", help="Optional explicit run ID")
    parser.add_argument("--resume", help="Resume an existing run ID")
    parser.add_argument("--base-run-id", help="Base run ID for delta comparison")
    parser.add_argument("--links-only", action="store_true")
    parser.add_argument("--discovery-only", action="store_true")
    parser.add_argument("--download-from-manifest")
    parser.add_argument(
        "--use-playwright",
        dest="use_playwright",
        action="store_true",
        default=True,
        help="Use Playwright browser for primary scraping (default: enabled)",
    )
    parser.add_argument(
        "--no-use-playwright",
        dest="use_playwright",
        action="store_false",
        help="Disable Playwright and use requests session for primary scraping",
    )
    parser.add_argument(
        "--playwright-headed",
        dest="playwright_headed",
        action="store_true",
        default=True,
        help="Run Playwright in headed mode for primary scraping (default: enabled)",
    )
    parser.add_argument(
        "--playwright-headless",
        dest="playwright_headed",
        action="store_false",
        help="Run Playwright in headless mode for primary scraping",
    )
    parser.add_argument("--skip-retry-pass", action="store_true")
    parser.add_argument("--retry-max-retries", type=int, default=3)
    parser.add_argument("--retry-include-waf", action="store_true")
    parser.add_argument(
        "--dc-ua-fallback-profiles",
        default="browser,transparent,python_requests,wget,curl",
    )
    parser.add_argument(
        "--dc-enum-mode",
        choices=["oai_sitemap_union", "oai_only", "sitemap_only"],
        default="oai_sitemap_union",
    )
    parser.add_argument(
        "--dc-use-siteindex",
        dest="dc_use_siteindex",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-dc-use-siteindex",
        dest="dc_use_siteindex",
        action="store_false",
    )
    parser.add_argument(
        "--dc-robots-enforce",
        dest="dc_robots_enforce",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-dc-robots-enforce",
        dest="dc_robots_enforce",
        action="store_false",
    )
    parser.add_argument("--dc-max-oai-records", type=int, default=0)
    parser.add_argument("--dc-max-sitemap-urls", type=int, default=0)
    parser.add_argument("--dc-download-timeout", type=int, default=30)
    parser.add_argument("--dc-min-domain-delay-ms", type=int, default=1000)
    parser.add_argument("--dc-max-domain-delay-ms", type=int, default=2000)
    parser.add_argument("--dc-waf-fail-threshold", type=int, default=3)
    parser.add_argument("--dc-waf-cooldown-seconds", type=int, default=900)
    parser.add_argument("--dc-session-rotate-threshold", type=int, default=300)
    parser.add_argument(
        "--dc-use-curl-cffi",
        dest="dc_use_curl_cffi",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-dc-use-curl-cffi",
        dest="dc_use_curl_cffi",
        action="store_false",
    )
    parser.add_argument(
        "--dc-waf-browser-fallback",
        dest="dc_waf_browser_fallback",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--dc-browser-backend",
        choices=["auto", "camoufox", "playwright", "chrome_mcp"],
        default="auto",
    )
    parser.add_argument("--dc-browser-staging-dir", default="artifacts/browser_staging")
    parser.add_argument("--dc-browser-user-data-dir", default="artifacts/browser_profile")
    parser.add_argument("--dc-browser-timeout", type=int, default=60)
    parser.add_argument(
        "--dc-browser-headless",
        dest="dc_browser_headless",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--dc-disable-unscoped-oai-no-slug",
        dest="dc_disable_unscoped_oai_no_slug",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-dc-disable-unscoped-oai-no-slug",
        dest="dc_disable_unscoped_oai_no_slug",
        action="store_false",
    )
    parser.add_argument(
        "--max-downloads-per-domain",
        type=int,
        default=1,
        help="Max concurrent downloads per domain in --download-from-manifest mode",
    )
    parser.add_argument(
        "--dc-round-robin-downloads",
        dest="dc_round_robin_downloads",
        action="store_true",
        default=True,
        help="Enable DC-only round-robin download phase (default: enabled)",
    )
    parser.add_argument(
        "--no-dc-round-robin-downloads",
        dest="dc_round_robin_downloads",
        action="store_false",
        help="Disable DC-only round-robin download phase",
    )
    parser.add_argument(
        "--dc-round-robin-strict-first-pass",
        dest="dc_round_robin_strict_first_pass",
        action="store_true",
        default=True,
        help=(
            "Require one dispatch per DC domain before a second dispatch in round-robin mode "
            "(default: enabled)"
        ),
    )
    parser.add_argument(
        "--no-dc-round-robin-strict-first-pass",
        dest="dc_round_robin_strict_first_pass",
        action="store_false",
        help="Disable strict first-pass dispatch behavior for DC round-robin",
    )
    parser.add_argument(
        "--dc-round-robin-revisit-interval-seconds",
        type=int,
        default=90,
        help="Minimum seconds between dispatches for the same DC domain (default: 90)",
    )
    parser.add_argument(
        "--skip-well-covered-seeds",
        dest="skip_well_covered_seeds",
        action="store_true",
        default=True,
        help=(
            "Skip active seeds for domains that already have at least "
            "--well-covered-pdf-threshold PDFs in --out-dir (default: enabled)"
        ),
    )
    parser.add_argument(
        "--no-skip-well-covered-seeds",
        dest="skip_well_covered_seeds",
        action="store_false",
        help="Do not skip domains with existing substantial PDF coverage",
    )
    parser.add_argument(
        "--well-covered-pdf-threshold",
        type=int,
        default=250,
        help="PDF count threshold used by --skip-well-covered-seeds (default: 250)",
    )
    parser.add_argument(
        "--skip-dc-sites",
        dest="skip_dc_sites",
        action="store_true",
        default=False,
        help="Skip Digital Commons seeds entirely in production runs",
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
            "Enable operator-monitored mode "
            "(serial scheduling + headed browser fallback + manual prompts)."
        ),
    )
    parser.add_argument(
        "--operator-intervention-scope",
        choices=["off", "browser_fallback_only"],
        default="off",
        help=(
            "Scope for manual intervention prompts "
            "(default: off; coerced to browser_fallback_only in operator mode)."
        ),
    )
    parser.add_argument(
        "--operator-wait-mode",
        choices=["prompt_enter", "off"],
        default="off",
        help="Manual intervention wait behavior (default: off).",
    )
    parser.add_argument(
        "--operator-manual-retries",
        type=int,
        default=1,
        help="Manual retry attempts per browser fallback URL (default: 1).",
    )
    parser.add_argument(
        "--write-retry-targets",
        dest="write_retry_targets",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-write-retry-targets", dest="write_retry_targets", action="store_false"
    )
    parser.add_argument(
        "--write-legacy-manifests",
        dest="write_legacy_manifests",
        action="store_true",
        default=None,
        help=(
            "Also write legacy per-domain manifests (<manifest-dir>/<domain>.jsonl). "
            "Default: disabled."
        ),
    )
    parser.add_argument(
        "--no-write-legacy-manifests",
        dest="write_legacy_manifests",
        action="store_false",
        help="Disable legacy per-domain manifest writing (default).",
    )
    return parser.parse_args(argv)


def _coerce_operator_profile(args: argparse.Namespace) -> None:
    args.operator_manual_retries = max(int(args.operator_manual_retries or 0), 0)
    if args.operator_mode and args.operator_intervention_scope == "off":
        args.operator_intervention_scope = "browser_fallback_only"
    if args.operator_mode and args.operator_wait_mode == "off":
        args.operator_wait_mode = "prompt_enter"
    if not args.operator_mode:
        return

    if (
        args.max_workers != 1
        or args.max_seeds_per_domain != 1
        or args.max_downloads_per_domain != 1
    ):
        print(
            "[pipeline] Operator mode: forcing serial scheduling "
            "(max_workers=1, max_seeds_per_domain=1, max_downloads_per_domain=1)"
        )
    args.max_workers = 1
    args.max_seeds_per_domain = 1
    args.max_downloads_per_domain = 1
    if not args.dc_waf_browser_fallback:
        print("[pipeline] Operator mode: enabling --dc-waf-browser-fallback")
    args.dc_waf_browser_fallback = True
    if args.dc_browser_headless:
        print("[pipeline] Operator mode: forcing headed DC browser fallback")
    args.dc_browser_headless = False


def _ensure_run_id_for_interrupt_resume(args: argparse.Namespace) -> str:
    if args.resume:
        return str(args.resume)
    if args.run_id:
        return str(args.run_id)
    if args.download_from_manifest:
        return str(args.download_from_manifest)
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    args.run_id = run_id
    return run_id


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def _parse_date(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _journal_name(record: Dict[str, Any]) -> str:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    for key in ("journal", "journal_name", "publication_title", "name"):
        value = str((metadata or {}).get(key) or "").strip()
        if value:
            return value
    return str(record.get("domain") or "unknown").strip() or "unknown"


def _article_identity(record: Dict[str, Any]) -> str:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    metadata = metadata or {}
    for key in ("url", "article_url", "doi"):
        value = str(metadata.get(key) or "").strip().lower()
        if value:
            return f"{key}:{value}"
    title = str(metadata.get("title") or "").strip().lower()
    volume = str(metadata.get("volume") or "").strip().lower()
    issue = str(metadata.get("issue") or "").strip().lower()
    year = str(metadata.get("year") or metadata.get("date") or "").strip().lower()
    if title:
        return f"title:{title}|v:{volume}|i:{issue}|y:{year}"
    return str(record.get("page_url") or record.get("pdf_url") or "").strip().lower()


def _build_exports(run_dir: Path, export_root: Path) -> Dict[str, str]:
    records_path = run_dir / "records.jsonl"
    if not records_path.exists():
        return {}

    journal_rows: Dict[Tuple[str, str], Dict[str, Any]] = {}
    issue_rows: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    seen_article_by_journal: Dict[Tuple[str, str], set[str]] = {}
    seen_article_by_issue: Dict[Tuple[str, str, str, str], set[str]] = {}

    for record in _iter_jsonl(records_path):
        domain = str(record.get("domain") or "unknown").strip().lower() or "unknown"
        journal = _journal_name(record)
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        metadata = metadata or {}
        article_id = _article_identity(record)
        volume = str(metadata.get("volume") or "unknown").strip() or "unknown"
        issue = str(metadata.get("issue") or "unknown").strip() or "unknown"
        date_raw = metadata.get("date") or metadata.get("year")
        parsed_date = _parse_date(date_raw)
        date_str = parsed_date.strftime("%Y-%m-%d") if parsed_date else ""
        journal_key = (domain, journal)
        issue_key = (domain, journal, volume, issue)

        j = journal_rows.setdefault(
            journal_key,
            {
                "domain": domain,
                "journal_name": journal,
                "article_total": 0,
                "pdf_total": 0,
                "earliest_date": "",
                "latest_date": "",
            },
        )
        i = issue_rows.setdefault(
            issue_key,
            {
                "domain": domain,
                "journal_name": journal,
                "volume": volume,
                "issue": issue,
                "article_total": 0,
                "pdf_total": 0,
                "earliest_date": "",
                "latest_date": "",
            },
        )
        seen_article_by_journal.setdefault(journal_key, set())
        seen_article_by_issue.setdefault(issue_key, set())

        if article_id and article_id not in seen_article_by_journal[journal_key]:
            seen_article_by_journal[journal_key].add(article_id)
            j["article_total"] += 1
        if article_id and article_id not in seen_article_by_issue[issue_key]:
            seen_article_by_issue[issue_key].add(article_id)
            i["article_total"] += 1

        if bool(record.get("ok")) and record.get("pdf_url"):
            j["pdf_total"] += 1
            i["pdf_total"] += 1

        if date_str:
            if not j["earliest_date"] or date_str < j["earliest_date"]:
                j["earliest_date"] = date_str
            if not j["latest_date"] or date_str > j["latest_date"]:
                j["latest_date"] = date_str
            if not i["earliest_date"] or date_str < i["earliest_date"]:
                i["earliest_date"] = date_str
            if not i["latest_date"] or date_str > i["latest_date"]:
                i["latest_date"] = date_str

    for row in journal_rows.values():
        total = int(row["article_total"])
        row["pdf_coverage_ratio"] = round((row["pdf_total"] / total), 4) if total else 0.0
    for row in issue_rows.values():
        total = int(row["article_total"])
        row["pdf_coverage_ratio"] = round((row["pdf_total"] / total), 4) if total else 0.0

    export_dir = export_root / run_dir.name
    export_dir.mkdir(parents=True, exist_ok=True)

    journal_json = export_dir / "journal_summary.json"
    issue_json = export_dir / "issue_summary.json"
    journal_csv = export_dir / "journal_summary.csv"
    issue_csv = export_dir / "issue_summary.csv"

    journal_payload = sorted(journal_rows.values(), key=lambda r: (r["domain"], r["journal_name"]))
    issue_payload = sorted(
        issue_rows.values(), key=lambda r: (r["domain"], r["journal_name"], r["volume"], r["issue"])
    )

    journal_json.write_text(json.dumps(journal_payload, indent=2, sort_keys=True), encoding="utf-8")
    issue_json.write_text(json.dumps(issue_payload, indent=2, sort_keys=True), encoding="utf-8")

    if journal_payload:
        with journal_csv.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(journal_payload[0].keys()))
            writer.writeheader()
            writer.writerows(journal_payload)
    else:
        journal_csv.write_text(
            "domain,journal_name,article_total,pdf_total,pdf_coverage_ratio,earliest_date,latest_date\n",
            encoding="utf-8",
        )

    if issue_payload:
        with issue_csv.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(issue_payload[0].keys()))
            writer.writeheader()
            writer.writerows(issue_payload)
    else:
        issue_csv.write_text(
            "domain,journal_name,volume,issue,article_total,pdf_total,pdf_coverage_ratio,earliest_date,latest_date\n",
            encoding="utf-8",
        )

    return {
        "export_dir": str(export_dir),
        "journal_json": str(journal_json),
        "journal_csv": str(journal_csv),
        "issue_json": str(issue_json),
        "issue_csv": str(issue_csv),
    }


def _write_retry_targets(run_dir: Path) -> Dict[str, Any]:
    errors_path = run_dir / "errors.jsonl"
    if not errors_path.exists():
        return {"all": 0, "waf": 0, "network": 0, "invalid_pdf": 0}

    all_urls: set[str] = set()
    waf_urls: set[str] = set()
    network_urls: set[str] = set()
    invalid_pdf_urls: set[str] = set()

    for entry in _iter_jsonl(errors_path):
        pdf_url = str(entry.get("pdf_url") or "").strip()
        if not pdf_url:
            continue
        error_type = str(entry.get("error_type") or "").strip().lower()
        all_urls.add(pdf_url)
        if "waf" in error_type or error_type in {"403", "blocked_waf"}:
            waf_urls.add(pdf_url)
        if error_type in {"timeout", "network", "5xx", "http_error"}:
            network_urls.add(pdf_url)
        if error_type == "invalid_pdf":
            invalid_pdf_urls.add(pdf_url)

    targets = {
        "retry_targets_all.txt": sorted(all_urls),
        "retry_targets_waf.txt": sorted(waf_urls),
        "retry_targets_network.txt": sorted(network_urls),
        "retry_targets_invalid_pdf.txt": sorted(invalid_pdf_urls),
    }
    for filename, urls in targets.items():
        (run_dir / filename).write_text("\n".join(urls) + ("\n" if urls else ""), encoding="utf-8")

    return {
        "all": len(all_urls),
        "waf": len(waf_urls),
        "network": len(network_urls),
        "invalid_pdf": len(invalid_pdf_urls),
    }


def _load_pdf_index(run_dir: Path) -> Dict[str, str]:
    records_path = run_dir / "records.jsonl"
    index: Dict[str, str] = {}
    for record in _iter_jsonl(records_path):
        if not bool(record.get("ok")):
            continue
        pdf_url = str(record.get("pdf_url") or "").strip()
        if not pdf_url:
            continue
        index[pdf_url] = str(record.get("pdf_sha256") or "")
    return index


def _write_delta_summary(
    base_run_dir: Path, new_run_dir: Path, export_root: Path
) -> Dict[str, Any]:
    base_idx = _load_pdf_index(base_run_dir)
    new_idx = _load_pdf_index(new_run_dir)

    added = sorted(set(new_idx) - set(base_idx))
    removed = sorted(set(base_idx) - set(new_idx))
    changed_hash = sorted(
        url
        for url in set(base_idx).intersection(new_idx)
        if base_idx.get(url) and new_idx.get(url) and base_idx[url] != new_idx[url]
    )
    unchanged = len(set(base_idx).intersection(new_idx)) - len(changed_hash)

    payload = {
        "base_run_id": base_run_dir.name,
        "new_run_id": new_run_dir.name,
        "added_count": len(added),
        "removed_count": len(removed),
        "changed_hash_count": len(changed_hash),
        "unchanged_count": max(unchanged, 0),
        "added_urls_sample": added[:100],
        "removed_urls_sample": removed[:100],
        "changed_hash_urls_sample": changed_hash[:100],
    }

    export_dir = export_root / new_run_dir.name
    export_dir.mkdir(parents=True, exist_ok=True)
    out_path = export_dir / "delta_summary.json"
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    payload["path"] = str(out_path)
    return payload


def _latest_completed_run_id(manifest_dir: Path) -> Optional[str]:
    # Prefer the golden run pointer if it exists — this is the authoritative
    # production baseline set by ``scripts/promote_run.py``.
    golden_path = manifest_dir / "golden_run.json"
    if golden_path.exists():
        try:
            golden = json.loads(golden_path.read_text(encoding="utf-8"))
            golden_id = golden.get("run_id")
            if golden_id and (manifest_dir / golden_id).is_dir():
                return str(golden_id)
        except Exception:
            pass  # Fall through to timestamp scan

    candidates: List[Tuple[float, str]] = []
    for child in manifest_dir.iterdir():
        if not child.is_dir():
            continue
        manifest_path = child / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("status") != "completed":
            continue
        finished_at = str(payload.get("finished_at") or "")
        ts = 0.0
        if finished_at:
            try:
                ts = datetime.strptime(finished_at, "%Y-%m-%dT%H:%M:%SZ").timestamp()
            except ValueError:
                ts = manifest_path.stat().st_mtime
        else:
            ts = manifest_path.stat().st_mtime
        candidates.append((ts, child.name))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _run_full_or_delta(args: argparse.Namespace) -> Dict[str, Any]:
    _coerce_operator_profile(args)
    print(f"[pipeline] Starting mode={args.mode}")
    _ensure_run_id_for_interrupt_resume(args)
    summary = run_orchestrator(
        sitemaps_dir=args.sitemaps_dir,
        out_dir=args.out_dir,
        manifest_dir=args.manifest_dir,
        max_depth=args.max_depth,
        links_only=args.links_only,
        discovery_only=args.discovery_only,
        download_from_manifest=args.download_from_manifest,
        use_playwright=args.use_playwright,
        playwright_headed=args.playwright_headed,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_workers=args.max_workers,
        max_seeds_per_domain=args.max_seeds_per_domain,
        max_consecutive_seed_failures_per_domain=args.max_consecutive_seed_failures_per_domain,
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
        dc_session_rotate_threshold=args.dc_session_rotate_threshold,
        dc_use_curl_cffi=args.dc_use_curl_cffi,
        dc_waf_browser_fallback=args.dc_waf_browser_fallback,
        dc_browser_backend=args.dc_browser_backend,
        dc_browser_staging_dir=args.dc_browser_staging_dir,
        dc_browser_user_data_dir=args.dc_browser_user_data_dir,
        dc_browser_timeout=args.dc_browser_timeout,
        dc_browser_headless=args.dc_browser_headless,
        max_downloads_per_domain=args.max_downloads_per_domain,
        dc_round_robin_downloads=args.dc_round_robin_downloads,
        dc_round_robin_strict_first_pass=args.dc_round_robin_strict_first_pass,
        dc_round_robin_revisit_interval_seconds=args.dc_round_robin_revisit_interval_seconds,
        skip_dc_sites=args.skip_dc_sites,
        skip_well_covered_seeds=args.skip_well_covered_seeds,
        well_covered_pdf_threshold=args.well_covered_pdf_threshold,
        dc_disable_unscoped_oai_no_slug=args.dc_disable_unscoped_oai_no_slug,
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

    run_id = str(summary.get("run_id") or "")
    if not run_id:
        raise RuntimeError("Pipeline run finished without run_id")
    run_dir = Path(args.manifest_dir) / run_id
    print(f"[pipeline] Run complete run_id={run_id} path={run_dir}")

    retry_summary: Optional[Dict[str, Any]] = None
    if (
        not args.links_only
        and not args.discovery_only
        and not args.download_from_manifest
        and not args.skip_retry_pass
    ):
        print(f"[pipeline] Running retry pass max_retries={args.retry_max_retries}")
        retry_summary = retry_failed(
            str(run_dir),
            max_retries=args.retry_max_retries,
            include_waf=args.retry_include_waf,
        )
        print(
            "[pipeline] Retry summary "
            f"attempted={retry_summary.get('attempted')} recovered={retry_summary.get('recovered')} failed={retry_summary.get('failed')}"
        )

    export_paths = _build_exports(run_dir, Path(args.export_dir))
    print(f"[pipeline] Exports written under {export_paths.get('export_dir', args.export_dir)}")

    retry_target_counts = None
    if args.write_retry_targets:
        retry_target_counts = _write_retry_targets(run_dir)
        print(f"[pipeline] Retry target files written in {run_dir}")

    delta_summary = None
    if args.mode == "delta":
        base_run_id = args.base_run_id
        if not base_run_id:
            base_run_id = _latest_completed_run_id(Path(args.manifest_dir))
            if base_run_id == run_id:
                base_run_id = None
        if base_run_id:
            base_run_dir = Path(args.manifest_dir) / base_run_id
            if base_run_dir.exists():
                delta_summary = _write_delta_summary(base_run_dir, run_dir, Path(args.export_dir))
                print(
                    f"[pipeline] Delta summary written: {delta_summary.get('path')} "
                    f"(added={delta_summary.get('added_count')} changed={delta_summary.get('changed_hash_count')})"
                )
            else:
                print(f"[pipeline] Delta base run not found: {base_run_dir}")
        else:
            print("[pipeline] Delta base run not provided and no prior completed run found")

    return {
        "mode": args.mode,
        "summary": summary,
        "retry_summary": retry_summary,
        "run_dir": str(run_dir),
        "export_paths": export_paths,
        "retry_targets": retry_target_counts,
        "delta_summary": delta_summary,
    }


def _run_retry_mode(args: argparse.Namespace) -> Dict[str, Any]:
    if not args.run_id:
        raise ValueError("--run-id is required for --mode retry")
    run_dir = Path(args.manifest_dir) / args.run_id
    if not run_dir.exists():
        raise ValueError(f"Run directory does not exist: {run_dir}")

    print(f"[pipeline] Running retry mode for run_id={args.run_id}")
    retry_summary = retry_failed(
        str(run_dir),
        max_retries=args.retry_max_retries,
        include_waf=args.retry_include_waf,
    )
    export_paths = _build_exports(run_dir, Path(args.export_dir))
    retry_target_counts = _write_retry_targets(run_dir) if args.write_retry_targets else None
    return {
        "mode": args.mode,
        "run_dir": str(run_dir),
        "retry_summary": retry_summary,
        "export_paths": export_paths,
        "retry_targets": retry_target_counts,
    }


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        if args.mode == "retry":
            payload = _run_retry_mode(args)
        else:
            payload = _run_full_or_delta(args)
        print("[pipeline] Final summary:")
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    except KeyboardInterrupt:
        run_id = _ensure_run_id_for_interrupt_resume(args)
        if args.operator_mode:
            resume_cmd = f"make production-monitored-resume RUN_ID={run_id}"
        else:
            resume_cmd = f"make production-resume RUN_ID={run_id}"
        print("\n[pipeline] Interrupted by operator.")
        print(f"[pipeline] Resume command: {resume_cmd}")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
