#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, Optional, Sequence

CORE_DOCUMENTS = (
    "README.md",
    "CONTRIBUTING.md",
    "docs/README.md",
    "docs/ARCHITECTURE.md",
    "docs/ADAPTER_DEVELOPMENT.md",
    "docs/CONTRIBUTOR_START_HERE.md",
    "docs/DATA_AND_RELEASE_POLICY.md",
    "docs/DEVELOPER_WORKFLOW.md",
    "docs/GAZETTEER.md",
    "docs/OPERATIONS.md",
    "docs/OPERATOR_PLAYBOOK.md",
    "docs/PROJECT_OVERVIEW.md",
    "docs/REPO_LAYOUT.md",
    "docs/generated/GAZETTEER_SNAPSHOT.md",
    "scripts/README.md",
    "data/registry/README.md",
)

LINK_RE = re.compile(r"!?\[[^]]*\]\(([^)]+)\)")


def local_links(text: str) -> Iterable[str]:
    for match in LINK_RE.finditer(text):
        target = match.group(1).strip()
        if target.startswith("<") and ">" in target:
            target = target[1 : target.index(">")]
        else:
            target = target.split(" ", 1)[0]
        target = target.split("#", 1)[0]
        if not target or target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        yield target


def find_broken_links(repo_root: Path, documents: Sequence[str]) -> list[str]:
    failures: list[str] = []
    for relative in documents:
        document = repo_root / relative
        if not document.exists():
            failures.append(f"missing maintained document: {relative}")
            continue
        for target in local_links(document.read_text(encoding="utf-8")):
            if not (document.parent / target).resolve().exists():
                failures.append(f"{relative}: missing link target {target}")
    return failures


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Check local links in maintained Markdown docs")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    args = parser.parse_args(argv)

    failures = find_broken_links(args.repo_root.resolve(), CORE_DOCUMENTS)
    if failures:
        print("Markdown link check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print(f"Markdown link check passed ({len(CORE_DOCUMENTS)} maintained documents).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
