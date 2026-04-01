#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_PDF_ROOT, DEFAULT_QUARANTINE_ROOT, parse_bool  # noqa: E402
from offprint.pdf_footnotes.qc_filter import QCConfig, run_qc  # noqa: E402
from offprint.path_policy import warn_legacy_paths  # noqa: E402

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="High-precision QC quarantine for stray PDFs")
    parser.add_argument(
        "--pdf-root", default=DEFAULT_PDF_ROOT, help="Root directory containing PDF files"
    )
    parser.add_argument(
        "--quarantine-root",
        default=DEFAULT_QUARANTINE_ROOT,
        help="Destination root for copied excluded PDFs",
    )
    parser.add_argument(
        "--manifest-out",
        default="",
        help="Optional explicit path for exclusion manifest JSONL",
    )
    parser.add_argument(
        "--report-out",
        default="",
        help="Optional explicit path for QC summary report JSON",
    )
    parser.add_argument(
        "--dry-run",
        type=parse_bool,
        default=False,
        help="Evaluate and write artifacts without copying files (true/false)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional limit of PDFs to scan")
    return parser.parse_args()


def _warn_legacy_paths(args: argparse.Namespace) -> None:
    warn_legacy_paths(
        tool_name="qc",
        values_by_arg={
            "pdf_root": str(getattr(args, "pdf_root", "")),
            "quarantine_root": str(getattr(args, "quarantine_root", "")),
        },
        legacy_by_arg={"pdf_root": {"pdfs"}, "quarantine_root": {"pdfs_quarantine"}},
    )


def main() -> None:
    args = _parse_args()
    _warn_legacy_paths(args)
    config = QCConfig(
        pdf_root=args.pdf_root,
        quarantine_root=args.quarantine_root,
        manifest_out=(args.manifest_out or None),
        report_out=(args.report_out or None),
        dry_run=bool(args.dry_run),
        limit=args.limit,
    )
    result = run_qc(config)
    print(json.dumps(result.__dict__, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
