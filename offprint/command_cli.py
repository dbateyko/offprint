from __future__ import annotations

import argparse
import runpy
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


@dataclass(frozen=True)
class CommandSpec:
    script_rel: str
    summary: str


COMMANDS: dict[str, dict[str, CommandSpec]] = {
    "pipeline": {
        "run": CommandSpec("scripts/pipeline/run_pipeline.py", "Run full/delta/retry crawl pipeline"),
        "smoke": CommandSpec(
            "scripts/pipeline/smoke_one_pdf_per_site.py",
            "Smoke-test one PDF per site",
        ),
        "promote": CommandSpec(
            "scripts/pipeline/promote_run.py",
            "Promote run to golden baseline",
        ),
    },
    "onboarding": {
        "fingerprint": CommandSpec(
            "scripts/onboarding/fingerprint_site.py",
            "Fingerprint site platform and adapter fit",
        ),
        "auto-onboard": CommandSpec(
            "scripts/onboarding/auto_onboard_site.py",
            "Generate sitemap + registry onboarding changes",
        ),
    },
    "processing": {
        "qc": CommandSpec(
            "scripts/processing/qc_quarantine_pdfs.py",
            "Quarantine non-article PDFs",
        ),
        "extract-footnotes": CommandSpec(
            "scripts/processing/extract_footnotes.py",
            "Extract footnotes/endnotes from PDFs",
        ),
        "extract-text": CommandSpec(
            "scripts/processing/extract_text_jsonl.py",
            "Extract article text JSONL + sidecars",
        ),
        "extract-metadata": CommandSpec(
            "scripts/processing/extract_pdf_metadata.py",
            "Extract title/author/date/citation metadata",
        ),
        "build-hf": CommandSpec(
            "scripts/processing/build_hf_dataset.py",
            "Build Hugging Face parquet datasets",
        ),
        "extract-footnotes-docling": CommandSpec(
            "scripts/processing/extract_footnotes_from_docling.py",
            "Extract footnotes from PDFs using docling",
        ),
        "run-olmocr": CommandSpec(
            "scripts/processing/run_olmocr_dual_gpu.py",
            "Run olmOCR extraction (dual GPU)",
        ),
    },
    "quality": {
        "check-layout": CommandSpec(
            "scripts/quality/check_repo_layout.py",
            "Enforce canonical repository layout",
        ),
        "check-adapters": CommandSpec(
            "scripts/quality/check_no_generic_active_seeds.py",
            "Fail if active seeds resolve to generic/unmapped adapters",
        ),
        "evaluate-footnotes": CommandSpec(
            "scripts/quality/evaluate_footnotes.py",
            "Evaluate extraction against gold labels",
        ),
    },
    "reporting": {
        "site-status": CommandSpec(
            "scripts/reporting/site_status_report.py",
            "Generate site status summary/report",
        ),
        "metadata-quality": CommandSpec(
            "scripts/reporting/metadata_quality_report.py",
            "Report metadata coverage by domain",
        ),
    },
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _format_catalog() -> str:
    lines: list[str] = []
    for group in sorted(COMMANDS):
        lines.append(f"{group}:")
        for name in sorted(COMMANDS[group]):
            lines.append(f"  {name:<18} {COMMANDS[group][name].summary}")
    return "\n".join(lines)


def _print_group_help(group: str) -> None:
    print(f"Subcommands for '{group}':")
    for name in sorted(COMMANDS[group]):
        spec = COMMANDS[group][name]
        print(f"  {name:<18} {spec.summary}")
    print()
    print(f"Run one command help with: offprint-cli {group} <subcommand> --help")


def _dispatch(spec: CommandSpec, argv: list[str]) -> int:
    script = (_repo_root() / spec.script_rel).resolve()
    if not script.exists():
        print(f"[error] script not found: {script}", file=sys.stderr)
        return 2

    old_argv = sys.argv
    sys.argv = [str(script), *argv]
    try:
        runpy.run_path(str(script), run_name="__main__")
        return 0
    except SystemExit as exc:
        if isinstance(exc.code, int):
            return exc.code
        return 1
    finally:
        sys.argv = old_argv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="offprint-cli",
        description="Unified Offprint command router. Use GROUP + SUBCOMMAND, then pass script flags.",
        epilog=(
            "Available subcommands:\n\n"
            f"{_format_catalog()}\n\n"
            "Examples:\n"
            "  offprint-cli pipeline run --help\n"
            "  offprint-cli pipeline smoke --target-file /tmp/targets.txt --max-workers 1\n"
            "  offprint-cli processing qc --pdf-root artifacts/pdfs --dry-run true\n\n"
            "Archived/specialized commands remain callable via direct script paths:\n"
            "  scripts/processing/extract_footnotes_from_docling.py\n"
            "  scripts/processing/run_olmocr_dual_gpu.py\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="store_true", dest="help_requested")
    parser.add_argument("group", nargs="?", help="Command group (pipeline, onboarding, processing, quality, reporting)")
    parser.add_argument("subcommand", nargs="?", help="Subcommand within the group")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args, rest = parser.parse_known_args(list(argv) if argv is not None else None)

    if not args.group:
        parser.print_help()
        return 0
    if args.group not in COMMANDS:
        print(f"[error] unknown group: {args.group}", file=sys.stderr)
        print("Use --help to list valid groups/subcommands.", file=sys.stderr)
        return 2
    if not args.subcommand:
        _print_group_help(args.group)
        return 0
    if args.subcommand not in COMMANDS[args.group]:
        print(f"[error] unknown subcommand for group '{args.group}': {args.subcommand}", file=sys.stderr)
        _print_group_help(args.group)
        return 2

    if args.help_requested:
        return _dispatch(COMMANDS[args.group][args.subcommand], ["--help", *rest])

    if rest and rest[0] == "--":
        rest = rest[1:]
    return _dispatch(COMMANDS[args.group][args.subcommand], list(rest))


if __name__ == "__main__":
    raise SystemExit(main())
