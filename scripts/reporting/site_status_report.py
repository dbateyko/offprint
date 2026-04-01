#!/usr/bin/env python3
"""Site status dashboard — cross-references sitemaps, adapter registry,
PDF artifacts, and run stats into a single CSV report."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_PDF_ROOT, DEFAULT_RUNS_DIR, DEFAULT_SITEMAPS_DIR
from offprint.seed_catalog import load_seed_entries

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _domain_from_url(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _count_pdfs_on_disk(pdf_root: Path) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    if not pdf_root.is_dir():
        return counts
    for domain_dir in sorted(pdf_root.iterdir()):
        if domain_dir.is_dir():
            domain = domain_dir.name.lower()
            if domain.startswith("www."):
                domain = domain[4:]
            n = sum(1 for f in domain_dir.iterdir() if f.suffix.lower() == ".pdf")
            if n:
                counts[domain] += n
    return dict(counts)


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _adapter_class_for(domain: str) -> str:
    """Look up adapter class name from registry without instantiating."""
    try:
        from offprint.adapters.registry import ADAPTERS  # type: ignore[attr-defined]

        cls = ADAPTERS.get(domain) or ADAPTERS.get(f"www.{domain}")
        if cls:
            return cls.__name__
    except Exception:
        pass
    return ""


def _latest_run_stats(runs_dir: Path) -> Optional[Dict[str, Any]]:
    """Load stats.json from the most recent run directory."""
    if not runs_dir.is_dir():
        return None
    run_dirs = sorted(
        [d for d in runs_dir.iterdir() if d.is_dir() and (d / "stats.json").exists()],
        key=lambda d: d.name,
        reverse=True,
    )
    if not run_dirs:
        return None
    return _load_json(run_dirs[0] / "stats.json")


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------


def build_report(
    sitemaps_dir: Path,
    pdf_root: Path,
    runs_dir: Path,
) -> List[Dict[str, Any]]:
    # 1. Seed entries (grouped by domain)
    seeds = load_seed_entries(str(sitemaps_dir))
    domain_seeds: Dict[str, list] = defaultdict(list)
    for s in seeds:
        domain_seeds[_domain_from_url(s.seed_url)].append(s)

    # 2. PDFs on disk
    pdfs_on_disk = _count_pdfs_on_disk(pdf_root)

    # 3. Domain baselines
    baselines = _load_json(runs_dir / "domain_baselines.json") or {}

    # 4. Golden run
    _load_json(runs_dir / "golden_run.json")  # validate existence

    # 5. Latest run stats
    latest_stats = _latest_run_stats(runs_dir)
    latest_domains = (latest_stats or {}).get("domains", {})

    # 6. All known domains (union)
    all_domains = sorted(
        set(domain_seeds.keys())
        | set(pdfs_on_disk.keys())
        | set(baselines.keys())
        | set(latest_domains.keys())
    )

    rows: List[Dict[str, Any]] = []
    for domain in all_domains:
        entries = domain_seeds.get(domain, [])
        # Pick representative seed for metadata
        rep = entries[0] if entries else None

        baseline = baselines.get(domain, {})
        latest = latest_domains.get(domain, {})

        status = rep.status if rep else ""
        platform = (rep.metadata.get("platform", "") if rep else "").lower()
        adapter = _adapter_class_for(domain)
        disk_pdfs = pdfs_on_disk.get(domain, 0)
        best_pdfs = baseline.get("best_pdf_count", 0)
        best_run = baseline.get("best_run_id", "")
        latest_pdfs = latest.get("ok_total", 0)

        needs_run = bool(adapter and status == "active" and disk_pdfs == 0)
        needs_adapter = status == "todo_adapter"

        rows.append(
            {
                "domain": domain,
                "journal_name": rep.journal_name if rep else "",
                "sitemap_status": status,
                "adapter_class": adapter,
                "platform_guess": platform,
                "seeds": len(entries),
                "pdfs_on_disk": disk_pdfs,
                "best_run_pdfs": best_pdfs,
                "best_run_id": best_run,
                "latest_run_pdfs": latest_pdfs,
                "needs_run": needs_run,
                "needs_adapter": needs_adapter,
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

COLUMNS = [
    "domain",
    "journal_name",
    "sitemap_status",
    "adapter_class",
    "platform_guess",
    "seeds",
    "pdfs_on_disk",
    "best_run_pdfs",
    "best_run_id",
    "latest_run_pdfs",
    "needs_run",
    "needs_adapter",
]


def write_csv(rows: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows → {out_path}")


def print_summary(rows: List[Dict[str, Any]]) -> None:
    total = len(rows)
    total_pdfs = sum(r["pdfs_on_disk"] for r in rows)

    status_counts = Counter(r["sitemap_status"] for r in rows)
    with_adapter = sum(1 for r in rows if r["adapter_class"])
    with_pdfs = sum(1 for r in rows if r["pdfs_on_disk"] > 0)

    needs_run = [r for r in rows if r["needs_run"]]
    needs_adapter = [r for r in rows if r["needs_adapter"]]
    orphans = [r for r in rows if r["pdfs_on_disk"] > 0 and not r["sitemap_status"]]

    print("\n" + "=" * 60)
    print("SITE STATUS SUMMARY")
    print("=" * 60)
    print(f"  Total domains:          {total}")
    print(f"  Total PDFs on disk:     {total_pdfs:,}")
    print(f"  Domains with adapter:   {with_adapter}")
    print(f"  Domains with PDFs:      {with_pdfs}")
    print()

    print("By sitemap status:")
    for status in sorted(status_counts):
        label = status or "(no sitemap)"
        print(f"  {label:25s} {status_counts[status]:>5}")
    print()

    if needs_run:
        print(f"ACTIVE sites with adapter but 0 PDFs on disk ({len(needs_run)}):")
        for r in sorted(needs_run, key=lambda x: x["domain"])[:20]:
            print(f"  {r['domain']:45s} adapter={r['adapter_class']}")
        if len(needs_run) > 20:
            print(f"  ... and {len(needs_run) - 20} more")
        print()

    if orphans:
        print(f"Orphan domains (PDFs on disk, no sitemap) ({len(orphans)}):")
        for r in sorted(orphans, key=lambda x: -x["pdfs_on_disk"])[:10]:
            print(f"  {r['domain']:45s} pdfs={r['pdfs_on_disk']}")
        if len(orphans) > 10:
            print(f"  ... and {len(orphans) - 10} more")
        print()

    if needs_adapter:
        platform_groups = Counter(r["platform_guess"] or "(unknown)" for r in needs_adapter)
        print(f"todo_adapter sites by platform ({len(needs_adapter)} total):")
        for plat, cnt in platform_groups.most_common():
            print(f"  {plat:25s} {cnt:>5}")
        print()

    paused = [r for r in rows if r["sitemap_status"].startswith("paused_")]
    if paused:
        reason_counts = Counter(r["sitemap_status"] for r in paused)
        print(f"Paused sites ({len(paused)} total):")
        for reason, cnt in reason_counts.most_common():
            print(f"  {reason:25s} {cnt:>5}")
        print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Generate site status report")
    parser.add_argument("--sitemaps-dir", default=DEFAULT_SITEMAPS_DIR, help="Directory containing sitemap JSONs")
    parser.add_argument("--pdf-root", default=DEFAULT_PDF_ROOT, help="Root directory for downloaded PDFs")
    parser.add_argument("--runs-dir", default=DEFAULT_RUNS_DIR, help="Directory containing run artifacts")
    parser.add_argument(
        "--out",
        default="artifacts/reports/site_status.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print terminal summary only (skip CSV)",
    )
    args = parser.parse_args(argv)

    rows = build_report(
        sitemaps_dir=Path(args.sitemaps_dir),
        pdf_root=Path(args.pdf_root),
        runs_dir=Path(args.runs_dir),
    )

    if not args.summary:
        write_csv(rows, Path(args.out))

    print_summary(rows)


if __name__ == "__main__":
    main()
