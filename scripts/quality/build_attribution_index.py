#!/usr/bin/env python3
"""Map every collected PDF back to the journal it came from.

Coverage questions kept getting the wrong answer because nothing in the corpus
says which journal a file belongs to. corpus/scraped is laid out by HOST, and
one host serves many journals: scholarlycommons.law.northwestern.edu holds 6,779
PDFs of which only 491 carry a dc-jclc- prefix and 5,816 are named
viewcontent.cgi-NNNN.pdf with no journal in the name at all. So "is this journal
collected?" could not be answered from the filesystem, and host presence was
being used as a proxy - which said Fordham Law Review was uncovered when 3,241 of
its PDFs were already held, and would equally have hidden a real gap.

The attribution does exist, in the run manifests: every records.jsonl row pairs a
local_path with the journal metadata of the seed that fetched it. This walks the
run history and builds filename -> {journal, host, run} so coverage can be judged
per journal instead of per host.

Usage:
  python scripts/quality/build_attribution_index.py [--runs-dir artifacts/runs] --out index.json
  python scripts/quality/build_attribution_index.py --query "Fordham Law Review"
"""
from __future__ import annotations

import argparse, collections, glob, json, os, sys
from typing import Dict, List, Optional

DEFAULT_OUT = "artifacts/attribution_index.json"


def seed_journal_by_url(seeds_dir: str) -> Dict[str, str]:
    """start_url -> journal name, so a record's seed_url can name its journal.

    metadata.journal is populated by only a handful of adapters (8 journals across
    the whole run history), but every record carries seed_url, and the seed that
    owns that URL knows the journal. This recovers attribution for the rest.
    """
    out: Dict[str, str] = {}
    for path in glob.glob(os.path.join(seeds_dir, "*.json")):
        try:
            seed = json.loads(open(path, encoding="utf-8").read())
        except (OSError, ValueError):
            continue
        name = (seed.get("metadata") or {}).get("journal_name")
        if not name:
            continue
        for url in seed.get("start_urls") or []:
            out[str(url).strip()] = name
    return out


def build(runs_dir: str, seeds_dir: str = "offprint/sitemaps") -> Dict[str, dict]:
    by_seed = seed_journal_by_url(seeds_dir)
    print(f"seed start_urls mapped to a journal: {len(by_seed)}", file=sys.stderr)
    index: Dict[str, dict] = {}
    runs = 0
    for path in glob.iglob(os.path.join(runs_dir, "*", "records.jsonl")):
        runs += 1
        run_id = os.path.basename(os.path.dirname(path))
        try:
            fh = open(path, encoding="utf-8")
        except OSError:
            continue
        with fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                local = rec.get("local_path") or ""
                if not local:
                    continue
                name = os.path.basename(local).lower()
                if not name.endswith(".pdf"):
                    continue
                meta = rec.get("metadata") or {}
                journal = (meta.get("journal") or meta.get("journal_name")
                           or meta.get("publisher") or "")
                if not journal:
                    journal = by_seed.get(str(rec.get("seed_url") or "").strip(), "")
                prev = index.get(name)
                # keep the entry that actually names a journal
                if prev and prev.get("journal") and not journal:
                    continue
                index[name] = {"journal": journal, "host": rec.get("domain") or "",
                               "run": run_id}
    print(f"scanned {runs} runs -> {len(index)} attributed files", file=sys.stderr)
    return index


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--runs-dir", default="artifacts/runs")
    ap.add_argument("--seeds-dir", default="offprint/sitemaps")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--query", default="", help="report the file count for a journal name")
    ap.add_argument("--top", type=int, default=0, help="show the N best-covered journals")
    args = ap.parse_args(argv)

    if os.path.exists(args.out) and not args.query and not args.top:
        print(f"{args.out} exists; rebuilding")
    index = build(args.runs_dir, args.seeds_dir)

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        json.dump(index, open(args.out, "w"))
        print(f"wrote {args.out}")

    counts = collections.Counter(v["journal"] for v in index.values() if v["journal"])
    unattributed = sum(1 for v in index.values() if not v["journal"])
    print(f"journals named: {len(counts)}   files with no journal in the manifest: {unattributed}")
    if args.query:
        q = args.query.lower()
        hits = [(j, n) for j, n in counts.items() if q in j.lower()]
        print(f"\nmatches for {args.query!r}:")
        for j, n in sorted(hits, key=lambda x: -x[1]):
            print(f"  {n:>6}  {j}")
        if not hits:
            print("  none - not collected, or collected without journal metadata")
    if args.top:
        print(f"\ntop {args.top} journals by attributed files:")
        for j, n in counts.most_common(args.top):
            print(f"  {n:>6}  {j[:66]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
