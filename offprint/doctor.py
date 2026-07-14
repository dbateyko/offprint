from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence


@dataclass(frozen=True)
class Check:
    name: str
    level: str
    detail: str


def _module_check(module: str, *, required: bool) -> Check:
    available = importlib.util.find_spec(module) is not None
    if available:
        return Check(module, "pass", "import is available")
    level = "fail" if required else "warn"
    kind = "required" if required else "optional"
    return Check(module, level, f"{kind} dependency is not installed")


def run_checks(repo_root: Path) -> list[Check]:
    root = repo_root.resolve()
    checks: list[Check] = []

    python_ok = sys.version_info >= (3, 8)
    checks.append(
        Check(
            "python",
            "pass" if python_ok else "fail",
            f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        )
    )

    required_paths = (
        "pyproject.toml",
        "data/registry/lawjournals.csv",
        "offprint/sitemaps",
        "scripts/pipeline/run_pipeline.py",
        "scripts/processing/extract_footnotes.py",
    )
    for relative in required_paths:
        exists = (root / relative).exists()
        checks.append(
            Check(relative, "pass" if exists else "fail", "found" if exists else "missing")
        )

    registry = root / "data/registry/lawjournals.csv"
    if registry.exists():
        try:
            with registry.open(newline="", encoding="utf-8-sig") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)
            required_fields = {
                "journal_name",
                "host",
                "platform",
                "status",
                "sitemap_file",
                "source",
            }
            missing = sorted(required_fields - set(reader.fieldnames or []))
            checks.append(
                Check(
                    "gazetteer schema",
                    "fail" if missing else "pass",
                    f"missing columns: {', '.join(missing)}" if missing else f"{len(rows):,} rows",
                )
            )
        except (OSError, csv.Error) as exc:
            checks.append(Check("gazetteer schema", "fail", str(exc)))

    sitemaps = root / "offprint/sitemaps"
    if sitemaps.is_dir():
        paths = sorted(sitemaps.glob("*.json"))
        invalid: list[str] = []
        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    invalid.append(path.name)
            except (OSError, json.JSONDecodeError):
                invalid.append(path.name)
        detail = f"{len(paths):,} files"
        if invalid:
            detail += f"; invalid: {', '.join(invalid[:5])}"
        checks.append(Check("sitemap JSON", "fail" if invalid else "pass", detail))

    for module in ("requests", "bs4", "lxml"):
        checks.append(_module_check(module, required=True))
    for module in ("fitz", "liteparse", "playwright"):
        checks.append(_module_check(module, required=False))

    artifacts = root / "artifacts"
    writable_target = artifacts if artifacts.exists() else artifacts.parent
    checks.append(
        Check(
            "artifact output",
            "pass" if os.access(writable_target, os.W_OK) else "fail",
            f"writable parent: {writable_target}",
        )
    )
    return checks


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Check whether this checkout can run Offprint")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    args = parser.parse_args(argv)

    checks = run_checks(args.repo_root)
    for check in checks:
        marker = {"pass": "PASS", "warn": "WARN", "fail": "FAIL"}[check.level]
        print(f"[{marker}] {check.name}: {check.detail}")

    failures = sum(check.level == "fail" for check in checks)
    warnings = sum(check.level == "warn" for check in checks)
    print(
        f"\n{len(checks) - failures - warnings} passed, {warnings} optional warnings, {failures} failed"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
