#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
from pathlib import Path

ROOT_DISALLOWED_GLOBS = ("*.csv", "*.jsonl", "*.log")
ROOT_DISALLOWED_PREFIXES = ("hs_err_pid", "replay_pid")
ROOT_DISALLOWED_EXACT = {
    "promoted_urls.txt",
    "roadmap.md",
}


def _is_disallowed_root_file(path: Path) -> bool:
    name = path.name
    if name in ROOT_DISALLOWED_EXACT:
        return True
    if any(name.startswith(prefix) for prefix in ROOT_DISALLOWED_PREFIXES):
        return True
    return any(fnmatch.fnmatch(name, pattern) for pattern in ROOT_DISALLOWED_GLOBS)


def check_layout(repo_root: Path) -> list[str]:
    failures: list[str] = []

    for child in sorted(repo_root.iterdir(), key=lambda p: p.name):
        if child.is_file() and _is_disallowed_root_file(child):
            failures.append(f"root clutter file not allowed: {child.relative_to(repo_root)}")

    data_registry = repo_root / "data" / "registry"
    if not data_registry.exists():
        failures.append("missing required directory: data/registry")
        return failures

    sitemaps = repo_root / "offprint" / "sitemaps"
    if not sitemaps.exists():
        failures.append("missing required directory: offprint/sitemaps")

    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fail when repository layout drifts from canonical root/data/docs boundaries."
    )
    parser.add_argument("--repo-root", default=".", help="Path to repository root (default: .)")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    failures = check_layout(repo_root)
    if failures:
        print("Repository layout check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Repository layout check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
