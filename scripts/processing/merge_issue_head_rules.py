#!/usr/bin/env python3
"""Merge per-journal head-rule batches into issue_head_rules.json.

Rules are authored in batches, one file per pass, then merged here so the
splitter reads a single file. Merging is deliberately a separate, reviewable
step: a head rule is a regex that decides where articles get cut, and an
unverified one silently mis-splits every issue of its journal.

`pattern_off_by_one` entries are NOT enabled by merging. That kind records a
pattern whose keys are correct but whose boundaries land at the wrong offset,
and the right `back_off` has to be measured against real issues per domain --
one of the three domains carrying that marker turned out to put its boundaries
on running-head pages at every offset. Enable them by hand, after checking.

Usage:
    python3 scripts/processing/merge_issue_head_rules.py --dry-run
    python3 scripts/processing/merge_issue_head_rules.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

RULES_DIR = Path(__file__).resolve().parents[2] / "offprint/pdf_footnotes"
TARGET = RULES_DIR / "issue_head_rules.json"

ACTIVE_KINDS = {"pattern", "single_article_domain", "no_split"}


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def merge(target: dict, batches: list[tuple[str, dict]]) -> tuple[dict, list[str]]:
    domains = dict(target.get("domains") or {})
    notes: list[str] = []
    for name, batch in batches:
        for domain, rule in (batch.get("domains") or {}).items():
            kind = str(rule.get("kind") or "")
            if kind not in ACTIVE_KINDS:
                notes.append(f"{name}: {domain} skipped (kind={kind!r}, needs review)")
                continue
            if domain in domains:
                notes.append(f"{name}: {domain} already present, keeping existing")
                continue
            merged = dict(rule)
            # Batches have used both spellings for the same field.
            if "boundary_backoff_pages" in merged and "back_off" not in merged:
                merged["back_off"] = merged.pop("boundary_backoff_pages")
            merged["source_batch"] = name
            domains[domain] = merged
            notes.append(f"{name}: {domain} added ({kind})")
    return {**target, "domains": domains}, notes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    batch_paths = sorted(RULES_DIR.glob("issue_head_rules_batch*.json"))
    if not batch_paths:
        raise SystemExit("no batch files to merge")
    target = load(TARGET)
    merged, notes = merge(target, [(p.stem, load(p)) for p in batch_paths])

    for note in notes:
        print(note)
    print(
        f"\ndomains: {len(target.get('domains') or {})} -> {len(merged['domains'])}"
        f"  (from {len(batch_paths)} batch file(s))"
    )
    if args.dry_run:
        print("\ndry run - nothing written")
        return
    TARGET.write_text(json.dumps(merged, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {TARGET}")


if __name__ == "__main__":
    main()
