#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.adapters.utils import compute_pdf_sha256_and_size  # noqa: E402
from offprint.cli import DEFAULT_PDF_ROOT, DEFAULT_RUNS_DIR, parse_bool  # noqa: E402
from offprint.pdf_footnotes.doc_policy import (  # noqa: E402
    DocDecision,
    classify_pdf,
    collect_signals,
    default_rules_path,
    infer_domain,
    infer_platform_family,
    load_rules,
    read_first_page_overview,
)
from offprint.pdf_footnotes.qc_filter import (
    latest_qc_manifest,
    load_excluded_paths,
)  # noqa: E402
from offprint.pdf_footnotes.text_extract import PARSER_MODES, extract_document_text  # noqa: E402


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _bool_to_status(value: bool) -> str:
    return "ok" if value else "failed"


def _default_report_path() -> str:
    os.makedirs(DEFAULT_RUNS_DIR, exist_ok=True)
    return os.path.join(DEFAULT_RUNS_DIR, f"text_extract_{_utc_stamp()}.json")


def _default_output_jsonl_path() -> str:
    os.makedirs(DEFAULT_RUNS_DIR, exist_ok=True)
    return os.path.join(DEFAULT_RUNS_DIR, f"text_extract_{_utc_stamp()}.jsonl")


def _default_doctype_manifest_path() -> str:
    os.makedirs(DEFAULT_RUNS_DIR, exist_ok=True)
    return os.path.join(DEFAULT_RUNS_DIR, f"text_doc_type_exclusions_{_utc_stamp()}.jsonl")


def _default_ocr_review_manifest_path() -> str:
    os.makedirs(DEFAULT_RUNS_DIR, exist_ok=True)
    return os.path.join(DEFAULT_RUNS_DIR, f"text_ocr_review_queue_{_utc_stamp()}.jsonl")


def _default_qc_manifest_path() -> str | None:
    manifest = latest_qc_manifest(DEFAULT_RUNS_DIR)
    if manifest:
        return manifest
    return latest_qc_manifest("runs")


def _path_in_shard(pdf_path: str, shard_count: int, shard_index: int) -> bool:
    if shard_count <= 1:
        return True
    digest = hashlib.sha1(os.path.abspath(pdf_path).encode("utf-8")).hexdigest()
    bucket = int(digest[:16], 16) % shard_count
    return bucket == shard_index


def _discover_pdfs(
    pdf_root: str,
    limit: int = 0,
    *,
    shard_count: int = 1,
    shard_index: int = 0,
) -> list[str]:
    discovered: list[str] = []
    for root, _dirs, files in os.walk(pdf_root):
        for filename in sorted(files):
            if not filename.lower().endswith(".pdf"):
                continue
            pdf_path = os.path.join(root, filename)
            if _path_in_shard(pdf_path, shard_count=shard_count, shard_index=shard_index):
                discovered.append(pdf_path)
    discovered.sort()
    if limit and limit > 0:
        return discovered[:limit]
    return discovered


def _write_json_atomic(path: str, payload: dict[str, Any]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _write_jsonl_atomic(path: str, rows: list[dict[str, Any]]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    os.replace(tmp, path)


def _classify_pdf_path(
    pdf_path: str,
    *,
    pdf_root: str,
    doc_policy: str,
    rules: dict[str, Any],
) -> tuple[str, DocDecision]:
    domain = infer_domain(pdf_path, pdf_root=pdf_root)
    platform_family = infer_platform_family(domain=domain)
    page_count, first_page_text = read_first_page_overview(pdf_path)
    signals = collect_signals(first_page_text, page_count, metadata=None)
    decision = classify_pdf(
        pdf_path=pdf_path,
        domain=domain,
        platform_family=platform_family,
        signals=signals,
        doc_policy=doc_policy,
        rules=rules,
    )
    return pdf_path, decision


def _sidecar_path(pdf_path: str) -> str:
    return f"{pdf_path}.text.json"


def _document_text(document: Any) -> str:
    pages = getattr(document, "pages", []) or []
    chunks: list[str] = []
    for page in pages:
        raw = str(getattr(page, "raw_text", "") or "").strip()
        if raw:
            chunks.append(raw)
    return "\n\n".join(chunks).strip()


def _derive_ocr_review_reasons(document: Any) -> list[str]:
    reasons: list[str] = []
    page_count = int(getattr(document, "page_count", 0) or 0)
    total_chars = int(getattr(document, "total_text_chars", 0) or 0)
    warnings = {str(w).strip() for w in (getattr(document, "warnings", []) or []) if str(w).strip()}
    if page_count == 0:
        reasons.append("native_text_extraction_empty")
    if total_chars < 600:
        reasons.append("low_text_volume")
    if "reversed_word_order_suspected" in warnings:
        reasons.append("reversed_word_order_suspected")
    if "low_font_variance_detected" in warnings:
        reasons.append("low_font_variance_detected")
    return reasons


@dataclass
class BatchConfig:
    pdf_root: str
    workers: int = 6
    classifier_workers: int = 6
    text_parser_mode: str = "footnote_optimized"
    include_pdf_sha256: bool = False
    overwrite: bool = False
    limit: int = 0
    report_detail: str = "summary"
    report_out: str | None = None
    output_jsonl: str | None = None
    max_text_chars: int = 0
    qc_exclusion_manifest: str | None = None
    respect_qc_exclusions: bool = True
    doc_policy: str = "article_only"
    doc_rules_path: str | None = None
    emit_doctype_manifest: bool = True
    doctype_manifest_out: str | None = None
    emit_ocr_review_manifest: bool = True
    ocr_review_manifest_out: str | None = None
    shard_count: int = 1
    shard_index: int = 0


def _extract_for_pdf(
    pdf_path: str,
    config: BatchConfig,
    decision: DocDecision | None,
) -> dict[str, Any]:
    sidecar_path = _sidecar_path(pdf_path)
    if os.path.exists(sidecar_path) and not config.overwrite:
        payload = {}
        try:
            payload = json.loads(Path(sidecar_path).read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        return {
            "pdf_path": pdf_path,
            "sidecar_path": sidecar_path,
            "status": "skipped_existing",
            "doc_type": decision.doc_type if decision else "",
            "platform_family": decision.platform_family if decision else "",
            "domain": decision.domain if decision else "",
            "warnings": list(payload.get("warnings") or []),
            "needs_ocr_review": bool(payload.get("needs_ocr_review")),
            "ocr_review_reasons": list(payload.get("ocr_review_reasons") or []),
            "row": payload if isinstance(payload, dict) else {},
        }

    parser_mode = (config.text_parser_mode or "footnote_optimized").strip().lower()
    if parser_mode not in PARSER_MODES:
        parser_mode = "footnote_optimized"
    document = extract_document_text(pdf_path, parser_mode=parser_mode)
    text = _document_text(document)
    if config.max_text_chars > 0 and len(text) > config.max_text_chars:
        text = text[: config.max_text_chars]
        warnings = list(getattr(document, "warnings", []) or [])
        warnings.append("text_truncated")
    else:
        warnings = list(getattr(document, "warnings", []) or [])

    pdf_sha256: str | None = None
    if config.include_pdf_sha256:
        pdf_sha256, _size = compute_pdf_sha256_and_size(pdf_path)

    needs_ocr_reasons = _derive_ocr_review_reasons(document)
    needs_ocr_review = bool(needs_ocr_reasons)
    if needs_ocr_review and "needs_ocr_review" not in warnings:
        warnings.append("needs_ocr_review")

    row = {
        "source_pdf_path": pdf_path,
        "pdf_sha256": pdf_sha256,
        "created_at": _utc_now_iso(),
        "parser_used": str(getattr(document, "parser", "") or ""),
        "text_parser_mode": parser_mode,
        "doc_type": decision.doc_type if decision else "",
        "platform_family": decision.platform_family if decision else "",
        "domain": decision.domain if decision else "",
        "ocr_candidate": bool(decision.ocr_candidate) if decision else False,
        "page_count": int(getattr(document, "page_count", 0) or 0),
        "char_count": len(text),
        "text": text,
        "warnings": sorted(set(str(w) for w in warnings if str(w).strip())),
        "needs_ocr_review": needs_ocr_review,
        "ocr_review_reasons": needs_ocr_reasons,
    }
    _write_json_atomic(sidecar_path, row)

    return {
        "pdf_path": pdf_path,
        "sidecar_path": sidecar_path,
        "status": "ok",
        "doc_type": decision.doc_type if decision else "",
        "platform_family": decision.platform_family if decision else "",
        "domain": decision.domain if decision else "",
        "parser_used": row["parser_used"],
        "char_count": row["char_count"],
        "page_count": row["page_count"],
        "warnings": row["warnings"],
        "needs_ocr_review": row["needs_ocr_review"],
        "ocr_review_reasons": row["ocr_review_reasons"],
        "row": row,
    }


def run_batch(config: BatchConfig) -> dict[str, Any]:
    run_started_monotonic = time.monotonic()
    pdf_root = os.path.abspath(config.pdf_root)
    if int(config.shard_count) <= 0:
        raise ValueError("shard_count must be >= 1")
    if int(config.shard_index) < 0 or int(config.shard_index) >= int(config.shard_count):
        raise ValueError("shard_index must be in [0, shard_count)")

    discovered = _discover_pdfs(
        pdf_root,
        limit=0,
        shard_count=int(config.shard_count),
        shard_index=int(config.shard_index),
    )
    if config.limit and config.limit > 0:
        discovered = discovered[: config.limit]

    rules_path = config.doc_rules_path or None
    rules = load_rules(rules_path)
    report_detail = (config.report_detail or "summary").strip().lower()
    if report_detail not in {"summary", "full"}:
        report_detail = "summary"

    qc_manifest_path: str | None = None
    excluded_by_qc_paths: set[str] = set()
    if config.respect_qc_exclusions:
        qc_manifest_path = config.qc_exclusion_manifest or _default_qc_manifest_path()
        if config.qc_exclusion_manifest and not os.path.exists(config.qc_exclusion_manifest):
            raise ValueError(f"QC exclusion manifest not found: {config.qc_exclusion_manifest}")
        if qc_manifest_path and os.path.exists(qc_manifest_path):
            excluded_by_qc_paths = load_excluded_paths(qc_manifest_path)
    candidates = [path for path in discovered if os.path.abspath(path) not in excluded_by_qc_paths]

    summary: dict[str, Any] = {
        "started_at": _utc_now_iso(),
        "pdf_root": pdf_root,
        "text_parser_mode": config.text_parser_mode,
        "doc_policy": config.doc_policy,
        "doc_rules_path": rules_path or default_rules_path(),
        "workers": config.workers,
        "classifier_workers": config.classifier_workers,
        "shard_count": int(config.shard_count),
        "shard_index": int(config.shard_index),
        "include_pdf_sha256": bool(config.include_pdf_sha256),
        "report_detail": report_detail,
        "output_jsonl": config.output_jsonl or "",
        "total_pdfs": len(discovered),
        "excluded_by_qc": len(discovered) - len(candidates),
        "qc_manifest_path": qc_manifest_path,
        "classify_candidates": len(candidates),
        "classify_processed": 0,
        "eligible_pdfs": 0,
        "excluded_by_doc_policy": 0,
        "processed": 0,
        "ok": 0,
        "failed": 0,
        "skipped_existing": 0,
        "text_chars_extracted": 0,
        "needs_ocr_review": 0,
        "parser_used_counts": {},
        "doc_type_manifest_path": "",
        "ocr_review_manifest_path": "",
        "results": [],
    }
    results_for_jsonl: list[dict[str, Any]] = []
    excluded_rows: list[dict[str, Any]] = []
    ocr_review_rows: list[dict[str, Any]] = []

    def _consume_result(result: dict[str, Any]) -> None:
        summary["processed"] += 1
        status = result.get("status", "failed")
        if status == "ok":
            summary["ok"] += 1
            summary["text_chars_extracted"] += int(result.get("char_count") or 0)
            parser_used = str(result.get("parser_used") or "unknown").strip() or "unknown"
            parser_counts = summary["parser_used_counts"]
            parser_counts[parser_used] = int(parser_counts.get(parser_used) or 0) + 1
        elif status == "skipped_existing":
            summary["skipped_existing"] += 1
        else:
            summary["failed"] += 1

        if bool(result.get("needs_ocr_review")):
            summary["needs_ocr_review"] += 1
            ocr_review_rows.append(
                {
                    "created_at": _utc_now_iso(),
                    "source_pdf_path": str(result.get("pdf_path") or ""),
                    "sidecar_path": str(result.get("sidecar_path") or ""),
                    "parser_used": str(result.get("parser_used") or ""),
                    "char_count": int(result.get("char_count") or 0),
                    "ocr_review_reasons": list(result.get("ocr_review_reasons") or []),
                    "warnings": list(result.get("warnings") or []),
                }
            )
        row = result.get("row")
        if isinstance(row, dict) and row:
            results_for_jsonl.append(row)
        if report_detail == "full":
            summary["results"].append(result)

    with ThreadPoolExecutor(
        max_workers=max(1, int(config.classifier_workers))
    ) as classify_executor:
        with ThreadPoolExecutor(max_workers=max(1, int(config.workers))) as extract_executor:
            classify_futures: dict[Future[tuple[str, DocDecision]], str] = {
                classify_executor.submit(
                    _classify_pdf_path,
                    pdf_path,
                    pdf_root=pdf_root,
                    doc_policy=config.doc_policy,
                    rules=rules,
                ): pdf_path
                for pdf_path in candidates
            }
            pending_extract: set[Future[dict[str, Any]]] = set()
            max_inflight_extract = max(8, max(1, int(config.workers)) * 2)

            for classify_future in as_completed(classify_futures):
                pdf_path, decision = classify_future.result()
                summary["classify_processed"] += 1
                if decision.include:
                    summary["eligible_pdfs"] += 1
                    pending_extract.add(
                        extract_executor.submit(
                            _extract_for_pdf,
                            pdf_path,
                            config,
                            decision,
                        )
                    )
                else:
                    summary["excluded_by_doc_policy"] += 1
                    excluded_rows.append(
                        {
                            "created_at": _utc_now_iso(),
                            "source_pdf_path": pdf_path,
                            "domain": decision.domain,
                            "platform_family": decision.platform_family,
                            "doc_type": decision.doc_type,
                            "decision": "exclude",
                            "reason_codes": list(decision.reason_codes),
                            "rule_confidence": decision.confidence,
                            "ocr_candidate": decision.ocr_candidate,
                            "doc_policy": config.doc_policy,
                            "doc_rules_path": rules_path or "",
                        }
                    )

                while len(pending_extract) >= max_inflight_extract:
                    done, pending_extract = wait(pending_extract, return_when=FIRST_COMPLETED)
                    for future in done:
                        _consume_result(future.result())

            for future in as_completed(pending_extract):
                _consume_result(future.result())

    output_jsonl = config.output_jsonl or _default_output_jsonl_path()
    _write_jsonl_atomic(output_jsonl, results_for_jsonl)
    summary["output_jsonl"] = output_jsonl

    if config.emit_doctype_manifest and excluded_rows:
        manifest_path = config.doctype_manifest_out or _default_doctype_manifest_path()
        _write_jsonl_atomic(manifest_path, excluded_rows)
        summary["doc_type_manifest_path"] = manifest_path

    if config.emit_ocr_review_manifest and ocr_review_rows:
        manifest_path = config.ocr_review_manifest_out or _default_ocr_review_manifest_path()
        _write_jsonl_atomic(manifest_path, ocr_review_rows)
        summary["ocr_review_manifest_path"] = manifest_path

    summary["run_elapsed_seconds"] = round(time.monotonic() - run_started_monotonic, 3)
    summary["status"] = _bool_to_status(summary["failed"] == 0)
    summary["finished_at"] = _utc_now_iso()
    if report_detail != "full":
        summary["results_omitted"] = True
        summary["results"] = []

    report_out = config.report_out or _default_report_path()
    _write_json_atomic(report_out, summary)
    summary["report_path"] = report_out
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract article text from PDFs into JSONL (+ per-PDF sidecars)."
    )
    parser.add_argument("--pdf-root", default=DEFAULT_PDF_ROOT, help="Root directory containing PDF files")
    parser.add_argument("--workers", type=int, default=6, help="Text extraction worker count")
    parser.add_argument(
        "--classifier-workers",
        type=int,
        default=6,
        help="Document classification worker count",
    )
    parser.add_argument(
        "--text-parser-mode",
        choices=sorted(PARSER_MODES),
        default="footnote_optimized",
        help="Text parser strategy",
    )
    parser.add_argument(
        "--include-pdf-sha256",
        type=parse_bool,
        default=False,
        help="Compute and store PDF SHA-256 in output rows (true/false)",
    )
    parser.add_argument(
        "--max-text-chars",
        type=int,
        default=0,
        help="Optional max output characters per document (0 = no cap)",
    )
    parser.add_argument("--overwrite", type=parse_bool, default=False, help="Overwrite text sidecars")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of PDFs processed")
    parser.add_argument(
        "--report-detail",
        choices=["summary", "full"],
        default="summary",
        help="Report verbosity level",
    )
    parser.add_argument("--report-out", default="", help="Optional output path for report JSON")
    parser.add_argument("--output-jsonl", default="", help="Output JSONL path for extracted rows")
    parser.add_argument(
        "--qc-exclusion-manifest",
        default="",
        help="Optional explicit QC exclusion manifest JSONL path",
    )
    parser.add_argument(
        "--respect-qc-exclusions",
        type=parse_bool,
        default=True,
        help="Skip files listed in QC exclusion manifest (true/false)",
    )
    parser.add_argument(
        "--doc-policy",
        choices=["article_only", "include_issue_compilations", "all"],
        default="article_only",
        help="Document inclusion policy",
    )
    parser.add_argument(
        "--doc-rules-path",
        default="",
        help="Optional JSON rules path for doc-type classification overrides",
    )
    parser.add_argument(
        "--emit-doctype-manifest",
        type=parse_bool,
        default=True,
        help="Write JSONL manifest of PDFs excluded by doc-policy (true/false)",
    )
    parser.add_argument(
        "--doctype-manifest-out",
        default="",
        help="Optional explicit output path for doc-type exclusion manifest JSONL",
    )
    parser.add_argument(
        "--emit-ocr-review-manifest",
        type=parse_bool,
        default=True,
        help="Write JSONL manifest for PDFs flagged as needing OCR review (true/false)",
    )
    parser.add_argument(
        "--ocr-review-manifest-out",
        default="",
        help="Optional explicit output path for OCR-review queue JSONL",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="Deterministic shard count for parallel runs",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help="Deterministic shard index in [0, shard-count)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = BatchConfig(
        pdf_root=args.pdf_root,
        workers=args.workers,
        classifier_workers=args.classifier_workers,
        text_parser_mode=args.text_parser_mode,
        include_pdf_sha256=bool(args.include_pdf_sha256),
        overwrite=bool(args.overwrite),
        limit=int(args.limit),
        report_detail=args.report_detail,
        report_out=(args.report_out or None),
        output_jsonl=(args.output_jsonl or None),
        max_text_chars=max(0, int(args.max_text_chars)),
        qc_exclusion_manifest=(args.qc_exclusion_manifest or None),
        respect_qc_exclusions=bool(args.respect_qc_exclusions),
        doc_policy=args.doc_policy,
        doc_rules_path=(args.doc_rules_path or None),
        emit_doctype_manifest=bool(args.emit_doctype_manifest),
        doctype_manifest_out=(args.doctype_manifest_out or None),
        emit_ocr_review_manifest=bool(args.emit_ocr_review_manifest),
        ocr_review_manifest_out=(args.ocr_review_manifest_out or None),
        shard_count=int(args.shard_count),
        shard_index=int(args.shard_index),
    )
    summary = run_batch(config)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
