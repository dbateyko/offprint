#!/usr/bin/env python3
"""Which journals are genuinely uncollected, judged by journal rather than host.

The registry's status column and corpus/scraped's host layout both mislead: by
those signals Fordham Law Review, the Journal of Criminal Law and Criminology and
Columbia Business Law Review all read as uncovered, while the corpus held 3,246,
2,463 and 1,858 of their PDFs respectively. Each was crawled again before anyone
noticed.

This joins the registry to the attribution index by normalized journal name and
reports what is actually missing, using no network at all.

Usage:
  python scripts/quality/coverage_by_journal.py [--min-rank N] [--json out.json]
"""
from __future__ import annotations

import argparse, collections, csv, json, os, re, sys
from typing import Dict, List, Optional

INDEX = "artifacts/attribution_index.json"
REGISTRY = "/mnt/shared_storage/law-review-corpus/offprint/data/registry/lawjournals.csv"
_STOP = re.compile(r"\b(the|of|and|for|a|in|on|at)\b")


def norm(name: str) -> str:
    return re.sub(r"[^a-z]", "", _STOP.sub("", (name or "").lower()))


def staging_holdings(staging_root: str, seeds_dir: str) -> Dict[str, int]:
    """PDFs sitting in staging, by journal.

    Staging is where a run lands before promotion, and unpromoted work is
    invisible to the corpus. Georgetown Law Journal reads as uncollected while
    304 of its PDFs sit in a staging run from the day before; without this the
    report would send someone to crawl it again.
    """
    import glob
    by_url: Dict[str, str] = {}
    for path in glob.glob(os.path.join(seeds_dir, "*.json")):
        try:
            seed = json.loads(open(path, encoding="utf-8").read())
        except (OSError, ValueError):
            continue
        name = (seed.get("metadata") or {}).get("journal_name")
        if name:
            for url in seed.get("start_urls") or []:
                by_url[str(url).strip()] = name
    counts: Dict[str, int] = collections.Counter()
    for records in glob.glob(os.path.join(staging_root, "**", "records.jsonl"), recursive=True):
        for line in open(records, encoding="utf-8", errors="ignore"):
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if not (rec.get("local_path") or "").lower().endswith(".pdf"):
                continue
            meta = rec.get("metadata") or {}
            journal = (meta.get("journal") or meta.get("journal_name")
                       or by_url.get(str(rec.get("seed_url") or "").strip(), ""))
            if journal:
                counts[norm(journal)] += 1
    return counts


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--index", default=INDEX)
    ap.add_argument("--registry", default=REGISTRY)
    ap.add_argument("--status", default="active", help="registry status to consider ('' for all)")
    ap.add_argument("--show", type=int, default=25)
    ap.add_argument("--staging-root", default="/mnt/shared_storage/law-review-corpus/staging")
    ap.add_argument("--seeds-dir", default="offprint/sitemaps")
    ap.add_argument("--json", default="")
    args = ap.parse_args(argv)

    index = json.loads(open(args.index, encoding="utf-8").read())
    held: Dict[str, int] = collections.Counter()
    for entry in index.values():
        j = entry.get("journal")
        if j:
            held[norm(j)] += 1

    staged = staging_holdings(args.staging_root, args.seeds_dir) if args.staging_root else {}
    if staged:
        print(f"staging holds PDFs for {len(staged)} journals (counted as collected)")

    seen, rows = set(), []
    for row in csv.DictReader(open(args.registry, encoding="utf-8")):
        name = (row.get("journal_name") or "").strip()
        if not name or (args.status and row.get("status") != args.status):
            continue
        key = norm(name)
        if not key or key in seen:
            continue
        seen.add(key)
        try:
            rank = int(row.get("wlu_rank") or 0)
        except ValueError:
            rank = 0
        rows.append({"journal": name, "held": held.get(key, 0),
                     "staged": staged.get(key, 0), "rank": rank,
                     "host": (row.get("host") or "").lower()})

    missing = [r for r in rows if r["held"] == 0 and r["staged"] == 0]
    thin = [r for r in rows if 0 < r["held"] + r["staged"] < 50]
    staged_only = [r for r in rows if r["held"] == 0 and r["staged"] > 0]
    print(f"registry journals considered (status={args.status or 'any'}): {len(rows)}")
    print(f"  already collected (>=50 pdfs) : {sum(1 for r in rows if r['held'] >= 50)}")
    print(f"  thinly collected  (1-49)      : {len(thin)}")
    print(f"  in staging, not yet promoted  : {len(staged_only)}")
    print(f"  genuinely uncollected (0)     : {len(missing)}")

    ranked = sorted([r for r in missing if r["rank"]], key=lambda r: r["rank"])
    print(f"\ngenuinely uncollected, best-ranked first (of {len(ranked)} with a W&L rank):")
    for r in ranked[:args.show]:
        print(f"  {r['rank']:>5}  {r['journal'][:52]:53s} {r['host'][:30]}")
    if args.json:
        json.dump({"missing": missing, "thin": thin}, open(args.json, "w"), indent=1)
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
