#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_PDF_ROOT, DEFAULT_RUNS_DIR  # noqa: E402
from offprint.pdf_footnotes.issue_splitter import SplitConfig, run_issue_split  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split issue-compilation PDFs into child article PDFs using conservative TOC heuristics."
    )
    parser.add_argument(
        "--pdf-root",
        default=DEFAULT_PDF_ROOT,
        help="Root directory containing PDFs, usually artifacts/pdfs or a domain directory.",
    )
    parser.add_argument(
        "--domain-filter",
        default="",
        help="Optional substring filter applied to candidate PDF paths, e.g. www.abdn.ac.uk.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional limit of unique PDFs to split.")
    parser.add_argument(
        "--output-root",
        default="artifacts/pdfs_split",
        help="Destination root for child PDFs.",
    )
    parser.add_argument(
        "--runs-dir",
        default=DEFAULT_RUNS_DIR,
        help="Directory for issue_split_manifest_<STAMP>.jsonl.",
    )
    parser.add_argument(
        "--candidate-file",
        default="",
        help=(
            "Optional TSV with columns including pdf_path, domain, sha256. "
            "When provided, candidates are read from this file instead of scanning --pdf-root."
        ),
    )
    parser.add_argument(
        "--candidate-issue-only",
        action="store_true",
        help=(
            "When using --candidate-file, keep only issue-like candidates based on filename/heuristics/pages."
        ),
    )
    parser.add_argument(
        "--candidate-min-priority",
        type=float,
        default=0.0,
        help="When using --candidate-file, skip rows with priority lower than this value.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = run_issue_split(
        SplitConfig(
            pdf_root=args.pdf_root,
            output_root=args.output_root,
            runs_dir=args.runs_dir,
            domain_filter=args.domain_filter,
            limit=args.limit,
            candidate_file=args.candidate_file,
            candidate_issue_only=args.candidate_issue_only,
            candidate_min_priority=args.candidate_min_priority,
        )
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
