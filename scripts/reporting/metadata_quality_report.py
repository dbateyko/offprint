#!/usr/bin/env python3
"""
metadata_quality_report.py — Per-domain metadata coverage report for a pipeline run.

Usage:
    python scripts/metadata_quality_report.py                    # golden run
    python scripts/metadata_quality_report.py --run-id 20260304T225711Z
    python scripts/metadata_quality_report.py --run-id 20260304T225711Z --min-records 10
    python scripts/metadata_quality_report.py --run-id 20260304T225711Z --platform wordpress
    python scripts/metadata_quality_report.py --run-id 20260304T225711Z --warn-only
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

from offprint.cli import DEFAULT_RUNS_DIR

REGISTRY_CSV = Path("data/registry/lawjournals.csv")
RUNS_DIR = Path(DEFAULT_RUNS_DIR)
GOLDEN_POINTER = RUNS_DIR / "golden_run.json"

DC_PLATFORMS = {"digitalcommons", "digital_commons", "digital commons", "bepress_digital_commons"}


def load_registry():
    if not REGISTRY_CSV.exists():
        return {}
    host_map = {}
    with open(REGISTRY_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            host = row.get("host", "").strip()
            if host:
                host_map[host] = {
                    "journal_name": row.get("journal_name", ""),
                    "platform": row.get("platform", "").strip().lower(),
                }
    return host_map


def resolve_run_id(run_id: str | None) -> str:
    if run_id:
        return run_id
    if GOLDEN_POINTER.exists():
        return json.loads(GOLDEN_POINTER.read_text()).get("run_id", "")
    # Fall back to most recent run
    runs = sorted([p.name for p in RUNS_DIR.iterdir() if p.is_dir() and p.name != "golden_run.json"])
    return runs[-1] if runs else ""


def main():
    parser = argparse.ArgumentParser(description="Metadata coverage report for a pipeline run")
    parser.add_argument("--run-id", default=None, help="Run ID (default: golden run)")
    parser.add_argument("--min-records", type=int, default=20, help="Minimum records to include domain (default: 20)")
    parser.add_argument(
        "--platform",
        default=None,
        help="Filter to a platform (e.g. wordpress, digitalcommons, ojs). Default: all non-DC",
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Only show domains with author<50%% or volume<50%%",
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    run_id = resolve_run_id(args.run_id)
    if not run_id:
        print("ERROR: no run_id found and no golden run pointer", file=sys.stderr)
        sys.exit(1)

    records_path = RUNS_DIR / run_id / "records.jsonl"
    if not records_path.exists():
        print(f"ERROR: records.jsonl not found at {records_path}", file=sys.stderr)
        sys.exit(1)

    registry = load_registry()

    # Tally per domain
    domain_stats: dict[str, dict] = defaultdict(
        lambda: {"total": 0, "title": 0, "author": 0, "volume": 0, "issue": 0, "date": 0, "doi": 0}
    )

    print(f"Reading {records_path} ...", file=sys.stderr)
    with open(records_path) as f:
        for line in f:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            domain = r.get("domain", "")
            meta = r.get("metadata", {})
            s = domain_stats[domain]
            s["total"] += 1
            if meta.get("title"):
                s["title"] += 1
            if meta.get("authors"):
                s["author"] += 1
            if meta.get("volume"):
                s["volume"] += 1
            if meta.get("issue"):
                s["issue"] += 1
            if meta.get("date") or meta.get("year"):
                s["date"] += 1
            if meta.get("doi"):
                s["doi"] += 1

    # Filter and sort
    rows = []
    for domain, s in domain_stats.items():
        t = s["total"]
        if t < args.min_records:
            continue

        reg = registry.get(domain, {})
        platform = reg.get("platform", "")

        # Platform filter
        if args.platform:
            pf = args.platform.lower()
            if pf not in platform:
                continue
        else:
            # Default: exclude DC (they're always well-covered by OAI-PMH)
            if platform in DC_PLATFORMS:
                continue

        author_pct = s["author"] / t
        vol_pct = s["volume"] / t
        warn = author_pct < 0.5 or vol_pct < 0.5

        if args.warn_only and not warn:
            continue

        rows.append(
            {
                "domain": domain,
                "journal_name": reg.get("journal_name", ""),
                "platform": platform,
                "total": t,
                "title_pct": round(s["title"] / t * 100),
                "author_pct": round(author_pct * 100),
                "volume_pct": round(vol_pct * 100),
                "issue_pct": round(s["issue"] / t * 100),
                "date_pct": round(s["date"] / t * 100),
                "doi_pct": round(s["doi"] / t * 100),
                "warn": warn,
            }
        )

    rows.sort(key=lambda r: -r["total"])

    if args.json:
        print(json.dumps(rows, indent=2))
        return

    # Human-readable table
    print(f"\nMETADATA QUALITY REPORT — run {run_id}")
    print(f"min_records={args.min_records}  platform={args.platform or 'non-DC'}  warn_only={args.warn_only}")
    print(f"{'Domain':45s} {'N':>6}  {'title':>6} {'author':>7} {'vol':>5} {'iss':>5} {'date':>5} {'doi':>5}")
    print("-" * 90)

    warn_count = 0
    for r in rows:
        flag = "⚠" if r["warn"] else "✓"
        if r["warn"]:
            warn_count += 1
        print(
            f"{flag} {r['domain']:43s} {r['total']:6d}  "
            f"{r['title_pct']:5d}%  {r['author_pct']:6d}%  "
            f"{r['volume_pct']:4d}%  {r['issue_pct']:4d}%  "
            f"{r['date_pct']:4d}%  {r['doi_pct']:4d}%"
        )

    print(f"\nTotal domains shown: {len(rows)}")
    print(f"Domains with author<50%% or volume<50%%: {warn_count}")

    if warn_count > 0:
        sys.exit(1)  # Non-zero exit so CI can gate on this


if __name__ == "__main__":
    main()
