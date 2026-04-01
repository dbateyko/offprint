from __future__ import annotations

import argparse

DEFAULT_SITEMAPS_DIR = "offprint/sitemaps"
DEFAULT_PDF_ROOT = "artifacts/pdfs"
DEFAULT_RUNS_DIR = "artifacts/runs"
DEFAULT_QUARANTINE_ROOT = "artifacts/quarantine"
DEFAULT_EXPORT_DIR = "artifacts/exports"


def parse_bool(raw: str) -> bool:
    value = (raw or "").strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {raw}")
