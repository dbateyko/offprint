#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_PDF_ROOT, parse_bool  # noqa: E402
from offprint.pdf_footnotes.pipeline import BatchConfig, run_batch  # noqa: E402
from offprint.path_policy import warn_legacy_paths  # noqa: E402

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract footnotes/endnotes from downloaded PDFs")
    parser.add_argument(
        "--pdf-root", default=DEFAULT_PDF_ROOT, help="Root directory containing PDF files"
    )
    parser.add_argument(
        "--features",
        choices=["core", "legal", "all"],
        default="legal",
        help="Feature preset (core, legal, all)",
    )
    parser.add_argument("--workers", type=int, default=6, help="Main parser worker count")
    parser.add_argument(
        "--classifier-workers",
        type=int,
        default=6,
        help="Document classification worker count",
    )
    parser.add_argument("--ocr-workers", type=int, default=2, help="OCR worker count")
    parser.add_argument(
        "--ocr-backend",
        choices=["olmocr", "glmocr"],
        default="glmocr",
        help="OCR backend (olmocr or glmocr)",
    )
    parser.add_argument(
        "--ocr-mode",
        choices=["off", "fallback", "always"],
        default="fallback",
        help="OCR strategy",
    )
    parser.add_argument(
        "--text-parser-mode",
        choices=[
            "balanced",
            "pdfplumber_only",
            "pypdf_only",
            "docling_only",
            "opendataloader_only",
            "liteparse_only",
            "footnote_optimized",
        ],
        default="footnote_optimized",
        help="Text parser strategy (footnote_optimized uses liteparse first, then pdfplumber fallback for spatial layout)",
    )
    parser.add_argument(
        "--include-pdf-sha256",
        type=parse_bool,
        default=False,
        help="Compute and store PDF SHA-256 in each sidecar (true/false)",
    )
    parser.add_argument(
        "--report-detail",
        choices=["summary", "full"],
        default="summary",
        help="Report verbosity level",
    )
    parser.add_argument(
        "--heartbeat-every",
        type=int,
        default=500,
        help="Emit progress heartbeat every N docs",
    )
    parser.add_argument(
        "--overwrite",
        type=parse_bool,
        default=False,
        help="Overwrite existing sidecars (true/false)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Limit number of PDFs processed")
    parser.add_argument(
        "--report-out",
        default="",
        help="Optional output path for extraction report JSON",
    )
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
        help="Document inclusion policy before extraction",
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
        "--text-cache",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Enable/disable text extraction caching (default: on)",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="Deterministic shard count for parallel extraction runs (default: 1).",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help="Deterministic shard index in [0, shard-count) (default: 0).",
    )
    parser.add_argument(
        "--ordinality-patch",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Enable/disable page-local native patch pass for ordinality-invalid docs (default: on).",
    )
    parser.add_argument(
        "--ordinality-patch-max-pages",
        type=int,
        default=20,
        help="Max number of candidate pages to include in ordinality patch pass.",
    )
    parser.add_argument(
        "--ordinality-patch-expand",
        type=int,
        default=1,
        help="Page expansion radius around inferred gap boundaries for ordinality patch.",
    )
    parser.add_argument(
        "--ordinality-patch-ocr-escalation-passes",
        type=int,
        default=1,
        help="Force OCR evaluation after unresolved native patch pass when > 0.",
    )
    parser.add_argument(
        "--skip-classification",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Skip document classification and extract all PDFs directly (default: off).",
    )
    parser.add_argument(
        "--shuffle",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Randomize PDF processing order for broader domain coverage per run (default: off).",
    )
    parser.add_argument(
        "--shuffle-seed",
        type=int,
        default=None,
        help="Seed for shuffle RNG (default: random). Use a fixed seed for reproducible ordering.",
    )
    return parser.parse_args()


def _warn_legacy_paths(args: argparse.Namespace) -> None:
    warn_legacy_paths(
        tool_name="footnotes",
        values_by_arg={"pdf_root": str(getattr(args, "pdf_root", ""))},
        legacy_by_arg={"pdf_root": {"pdfs"}},
    )


def main() -> None:
    args = _parse_args()
    _warn_legacy_paths(args)
    config = BatchConfig(
        pdf_root=args.pdf_root,
        features=args.features,
        workers=args.workers,
        classifier_workers=args.classifier_workers,
        ocr_workers=args.ocr_workers,
        ocr_mode=args.ocr_mode,
        ocr_backend=args.ocr_backend,
        text_parser_mode=args.text_parser_mode,
        include_pdf_sha256=bool(args.include_pdf_sha256),
        report_detail=args.report_detail,
        heartbeat_every=max(1, int(args.heartbeat_every)),
        overwrite=bool(args.overwrite),
        limit=args.limit,
        report_out=(args.report_out or None),
        qc_exclusion_manifest=(args.qc_exclusion_manifest or None),
        respect_qc_exclusions=bool(args.respect_qc_exclusions),
        doc_policy=args.doc_policy,
        doc_rules_path=(args.doc_rules_path or None),
        emit_doctype_manifest=bool(args.emit_doctype_manifest),
        doctype_manifest_out=(args.doctype_manifest_out or None),
        emit_ocr_review_manifest=bool(args.emit_ocr_review_manifest),
        ocr_review_manifest_out=(args.ocr_review_manifest_out or None),
        text_cache_enabled=bool(args.text_cache),
        shard_count=int(args.shard_count),
        shard_index=int(args.shard_index),
        ordinality_patch=bool(args.ordinality_patch),
        ordinality_patch_max_pages=max(1, int(args.ordinality_patch_max_pages)),
        ordinality_patch_expand=max(0, int(args.ordinality_patch_expand)),
        ordinality_patch_ocr_escalation_passes=max(
            0, int(args.ordinality_patch_ocr_escalation_passes)
        ),
        shuffle=bool(args.shuffle),
        shuffle_seed=args.shuffle_seed,
        skip_classification=bool(args.skip_classification),
    )
    summary = run_batch(config)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
