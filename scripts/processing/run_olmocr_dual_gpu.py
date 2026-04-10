#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_PDF_ROOT, DEFAULT_RUNS_DIR  # noqa: E402


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _check_endpoint(url: str, timeout_s: float = 2.0) -> tuple[bool, str]:
    model_url = url.rstrip("/") + "/models"
    try:
        resp = requests.get(model_url, timeout=timeout_s)
        if resp.status_code == 200:
            return True, ""
        return False, f"{model_url} returned HTTP {resp.status_code}"
    except Exception as exc:  # pragma: no cover
        return False, f"{model_url} failed: {exc}"


def _build_extract_cmd(
    *,
    python_bin: Path,
    shard_index: int,
    shard_count: int,
    args: argparse.Namespace,
    report_path: Path,
) -> list[str]:
    cmd = [
        str(python_bin),
        str((ROOT / "scripts" / "processing" / "extract_footnotes.py").resolve()),
        "--pdf-root",
        args.pdf_root,
        "--features",
        args.features,
        "--workers",
        str(args.workers),
        "--classifier-workers",
        str(args.classifier_workers),
        "--ocr-workers",
        str(args.ocr_workers),
        "--ocr-backend",
        "olmocr",
        "--ocr-mode",
        getattr(args, "ocr_mode", "always"),
        "--text-parser-mode",
        args.text_parser_mode,
        "--report-out",
        str(report_path),
        "--respect-qc-exclusions",
        "true" if args.respect_qc_exclusions else "false",
        "--overwrite",
        "true" if args.overwrite else "false",
        "--shard-count",
        str(shard_count),
        "--shard-index",
        str(shard_index),
    ]
    if args.qc_exclusion_manifest:
        cmd.extend(["--qc-exclusion-manifest", args.qc_exclusion_manifest])
    if args.doc_policy:
        cmd.extend(["--doc-policy", args.doc_policy])
    if args.limit and args.limit > 0:
        cmd.extend(["--limit", str(args.limit)])
    if getattr(args, "skip_classification", False):
        cmd.append("--skip-classification")
    if getattr(args, "shuffle", False):
        cmd.append("--shuffle")
    shuffle_seed = getattr(args, "shuffle_seed", None)
    if shuffle_seed is not None:
        cmd.extend(["--shuffle-seed", str(shuffle_seed)])
    return cmd


def _read_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "missing_report", "report_path": str(path)}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover
        return {"status": "invalid_report", "report_path": str(path), "error": str(exc)}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run OlmOCR footnote extraction in parallel on 2 shards / 2 endpoints."
    )
    parser.add_argument("--pdf-root", default=DEFAULT_PDF_ROOT)
    parser.add_argument("--features", choices=["core", "legal", "all"], default="legal")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--classifier-workers", type=int, default=6)
    parser.add_argument("--ocr-workers", type=int, default=2)
    parser.add_argument(
        "--text-parser-mode",
        choices=[
            "balanced",
            "pdfplumber_only",
            "pypdf_only",
            "docling_only",
            "opendataloader_only",
            "footnote_optimized",
        ],
        default="footnote_optimized",
    )
    parser.add_argument("--doc-policy", choices=["article_only", "include_issue_compilations", "all"], default="article_only")
    parser.add_argument("--qc-exclusion-manifest", default="")
    parser.add_argument("--respect-qc-exclusions", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--overwrite", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port-a", type=int, default=8080)
    parser.add_argument("--port-b", type=int, default=8081)
    parser.add_argument("--model", default="allenai/olmOCR-2-7B-1025-FP8")
    parser.add_argument("--python-bin", default=str((ROOT / ".venv" / "bin" / "python").resolve()))
    parser.add_argument("--report-dir", default=DEFAULT_RUNS_DIR)
    parser.add_argument("--log-dir", default="artifacts/logs")
    parser.add_argument("--olmocr-timeout-seconds", type=int, default=900)
    parser.add_argument(
        "--ocr-mode",
        choices=["off", "fallback", "always"],
        default="always",
        help="OCR strategy: always (default for dual-GPU), fallback, or off.",
    )
    parser.add_argument("--no-endpoint-check", action="store_true")
    parser.add_argument(
        "--skip-classification",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Skip document classification and extract all PDFs directly.",
    )
    parser.add_argument(
        "--shuffle",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Randomize PDF processing order for broader domain coverage per run.",
    )
    parser.add_argument(
        "--shuffle-seed",
        type=int,
        default=None,
        help="Seed for shuffle RNG (default: random).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    stamp = _utc_stamp()
    python_bin = Path(args.python_bin).resolve()
    if not python_bin.exists():
        raise FileNotFoundError(f"python binary not found: {python_bin}")

    report_dir = (ROOT / args.report_dir).resolve()
    log_dir = (ROOT / args.log_dir).resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    endpoint_a = f"http://{args.host}:{args.port_a}/v1"
    endpoint_b = f"http://{args.host}:{args.port_b}/v1"
    if not args.no_endpoint_check:
        ok_a, err_a = _check_endpoint(endpoint_a)
        ok_b, err_b = _check_endpoint(endpoint_b)
        if not ok_a or not ok_b:
            errors = [msg for msg in [err_a, err_b] if msg]
            raise RuntimeError("Endpoint check failed: " + " | ".join(errors))

    shard_specs = [
        {"idx": 0, "endpoint": endpoint_a, "name": "a"},
        {"idx": 1, "endpoint": endpoint_b, "name": "b"},
    ]

    procs: list[tuple[str, subprocess.Popen[str], Path, Any]] = []
    for spec in shard_specs:
        report_path = report_dir / f"footnote_extract_olmocr_dual_{stamp}_shard{spec['idx']}.json"
        log_path = log_dir / f"footnote_extract_olmocr_dual_{stamp}_shard{spec['idx']}.log"
        cmd = _build_extract_cmd(
            python_bin=python_bin,
            shard_index=spec["idx"],
            shard_count=2,
            args=args,
            report_path=report_path,
        )
        env = os.environ.copy()
        env["OLMOCR_SERVER_URL"] = str(spec["endpoint"])
        env["OLMOCR_MODEL"] = args.model
        env["OLMOCR_TIMEOUT_SECONDS"] = str(max(60, int(args.olmocr_timeout_seconds)))
        log_handle = log_path.open("w", encoding="utf-8")
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        procs.append((f"shard{spec['idx']}", proc, report_path, log_handle))

    rc_by_shard: dict[str, int] = {}
    for shard_name, proc, _report_path, log_handle in procs:
        rc_by_shard[shard_name] = int(proc.wait())
        log_handle.close()

    shard_reports: dict[str, dict[str, Any]] = {}
    merged = {
        "started_at": stamp,
        "mode": "olmocr_dual_gpu",
        "ocr_mode": "always",
        "pdf_root": args.pdf_root,
        "endpoints": {"shard0": endpoint_a, "shard1": endpoint_b},
        "rc_by_shard": rc_by_shard,
        "totals": {
            "total_pdfs": 0,
            "eligible_pdfs": 0,
            "processed": 0,
            "ok": 0,
            "failed": 0,
            "skipped_existing": 0,
            "ocr_used": 0,
            "notes_extracted": 0,
        },
        "shards": {},
    }
    for shard_name, _proc, report_path, _log_handle in procs:
        rep = _read_summary(report_path)
        shard_reports[shard_name] = rep
        merged["shards"][shard_name] = {
            "report_path": str(report_path),
            "status": rep.get("status"),
            "run_elapsed_seconds": rep.get("run_elapsed_seconds"),
            "ok": rep.get("ok", 0),
            "failed": rep.get("failed", 0),
            "skipped_existing": rep.get("skipped_existing", 0),
        }
        for key in [
            "total_pdfs",
            "eligible_pdfs",
            "processed",
            "ok",
            "failed",
            "skipped_existing",
            "ocr_used",
            "notes_extracted",
        ]:
            merged["totals"][key] += int(rep.get(key) or 0)

    merged_path = report_dir / f"footnote_extract_olmocr_dual_{stamp}_merged.json"
    merged_path.write_text(json.dumps(merged, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"merged_report": str(merged_path), "rc_by_shard": rc_by_shard}, indent=2))

    if any(code != 0 for code in rc_by_shard.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
