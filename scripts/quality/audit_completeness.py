#!/usr/bin/env python3
"""Audit historical runs for silently truncated collections.

Every undercollection bug found on 2026-08-29 exited 0 with a cheerful summary:
a DSpace pagination loop that stopped on the first transient failure, a
Squarespace adapter that never followed its pager, and a per-domain circuit
breaker that treated PDF-less back-volumes as failures. The evidence was always
present -- sequence_validator volume gaps in errors.jsonl, per-seed counts in
stats.json -- but nothing ever compared it to what the journal should hold.

This walks the run history and reports, per domain, the best result ever
achieved against the count the journal's own seed declares, so truncation that
was previously invisible becomes a list you can act on.

Usage:
  python scripts/quality/audit_completeness.py [--runs-dir artifacts/runs]
      [--seeds-dir offprint/sitemaps] [--min-ratio 0.75] [--out report.csv]
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

EXPECTED_KEYS = ("expected_pdfs", "articles_observed", "pdfs_found")


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower().removeprefix("www.")
    except ValueError:
        return ""


def load_expected(seeds_dir: str) -> Dict[str, Tuple[int, str]]:
    """domain -> (expected articles, journal name), from the seeds themselves."""
    out: Dict[str, Tuple[int, str]] = {}
    for path in sorted(glob.glob(os.path.join(seeds_dir, "*.json"))):
        try:
            payload = json.loads(open(path, encoding="utf-8").read())
        except (OSError, ValueError):
            continue
        meta = payload.get("metadata") or {}
        nav = meta.get("navigation") or {}
        expected: Optional[int] = None
        for key in EXPECTED_KEYS:
            value = nav.get(key)
            if isinstance(value, int) and value > 0:
                expected = value
                break
        if not expected:
            continue
        name = str(meta.get("journal_name") or os.path.basename(path))
        for url in payload.get("start_urls") or []:
            dom = _domain(str(url).replace("[", "1").replace("]", ""))
            if not dom:
                continue
            # keep the largest declared count seen for a domain
            if dom not in out or expected > out[dom][0]:
                out[dom] = (expected, name)
    return out


def scan_runs(runs_dir: str) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, str]]:
    """Walk run history -> best downloads per domain, volume-gap counts, last run id."""
    best: Dict[str, int] = defaultdict(int)
    gaps: Dict[str, int] = defaultdict(int)
    last: Dict[str, str] = {}
    for stats_path in glob.iglob(os.path.join(runs_dir, "*", "stats.json")):
        try:
            stats = json.loads(open(stats_path, encoding="utf-8").read())
        except (OSError, ValueError):
            continue
        run_id = os.path.basename(os.path.dirname(stats_path))
        for seed_url, info in (stats.get("seeds") or {}).items():
            if not isinstance(info, dict):
                continue
            dom = str(info.get("domain") or _domain(seed_url)).lower().removeprefix("www.")
            if not dom:
                continue
            runtime = info.get("runtime") or {}
            got = int(info.get("ok_total") or runtime.get("downloaded") or 0)
            if got > best[dom]:
                best[dom] = got
            comp = info.get("completeness") or {}
            gaps[dom] += len(comp.get("volume_gaps") or [])
            if run_id > last.get(dom, ""):
                last[dom] = run_id
    return best, gaps, last


def gaps_by_journal(runs_dir: str, seeds_dir: str) -> Dict[str, Dict[str, Any]]:
    """Attribute sequence_validator warnings to journals, not hosts.

    The warnings are the one truncation signal recorded for the whole corpus, but
    they were only ever reported per host - and a host serves many journals, so
    "digitalcommons.law.byu.edu has 1,923 volume gaps" names no journal anyone
    can act on. Every warning row carries seed_url, and the seed knows its
    journal.
    """
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
    out: Dict[str, Dict[str, Any]] = {}
    for errs in glob.iglob(os.path.join(runs_dir, "*", "errors.jsonl")):
        try:
            fh = open(errs, encoding="utf-8", errors="ignore")
        except OSError:
            continue
        with fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                if rec.get("error_type") != "completeness_warning":
                    continue
                journal = by_url.get(str(rec.get("seed_url") or "").strip())
                if not journal:
                    continue
                slot = out.setdefault(journal, {"gaps": set(), "domain": rec.get("domain") or ""})
                slot["gaps"].add(str(rec.get("message") or ""))
    return out


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--runs-dir", default="artifacts/runs")
    ap.add_argument("--seeds-dir", default="offprint/sitemaps")
    ap.add_argument("--min-ratio", type=float, default=0.75)
    ap.add_argument("--out", default="")
    ap.add_argument("--index", default="artifacts/attribution_index.json")
    args = ap.parse_args(argv)

    expected = load_expected(args.seeds_dir)
    best, gaps, last = scan_runs(args.runs_dir)
    print(f"seeds declaring an expected count: {len(expected)}")
    print(f"domains seen in run history:       {len(best)}")

    rows: List[Dict[str, Any]] = []
    for dom, (exp, name) in sorted(expected.items()):
        got = best.get(dom, 0)
        ratio = (got / exp) if exp else 0.0
        rows.append({
            "domain": dom, "journal": name, "collected": got, "expected": exp,
            "ratio": round(ratio, 3), "volume_gaps": gaps.get(dom, 0),
            "last_run": last.get(dom, ""),
            "verdict": "NEVER_RUN" if got == 0 else
                       ("SHORT" if ratio < args.min_ratio else "OK"),
        })

    short = [r for r in rows if r["verdict"] == "SHORT"]
    never = [r for r in rows if r["verdict"] == "NEVER_RUN"]
    ok = [r for r in rows if r["verdict"] == "OK"]
    print(f"\nverifiable domains: {len(rows)}   OK: {len(ok)}   SHORT: {len(short)}   never run: {len(never)}")

    if short:
        print(f"\n=== TRUNCATED: collected < {args.min_ratio:.0%} of declared ===")
        print(f"{'ratio':>6} {'got':>7} {'exp':>7} {'gaps':>5}  domain / journal")
        for r in sorted(short, key=lambda r: r["ratio"]):
            print(f"{r['ratio']:>6.0%} {r['collected']:>7} {r['expected']:>7} "
                  f"{r['volume_gaps']:>5}  {r['domain'][:30]:31s} {r['journal'][:38]}")
    # gaps recorded even where the ratio looks fine
    gappy = [r for r in ok if r["volume_gaps"]]
    if gappy:
        print(f"\n=== volume gaps despite an acceptable count ({len(gappy)}) ===")
        for r in sorted(gappy, key=lambda r: -r["volume_gaps"])[:15]:
            print(f"  {r['volume_gaps']:>4} gaps  {r['domain'][:32]:33s} {r['journal'][:40]}")

    # Volume gaps are recorded by sequence_validator on every run, with or without a
    # declared expected count, so they are the one truncation signal available for the
    # whole corpus today. Surface them regardless of verifiability.
    gap_rows = [(dom, n, best.get(dom, 0), last.get(dom, "")) for dom, n in gaps.items() if n]
    if gap_rows:
        print(f"\n=== domains with recorded volume gaps ({len(gap_rows)} of {len(best)}) ===")
        print(f"{'gaps':>5} {'collected':>10}  domain")
        for dom, n, got, run in sorted(gap_rows, key=lambda r: -r[1])[:25]:
            mark = "" if dom in expected else "   (no expected count -- unverifiable)"
            print(f"{n:>5} {got:>10}  {dom[:44]:45s}{mark}")
        print(f"\ntotal recorded volume-gap warnings: {sum(n for _, n, _, _ in gap_rows)}")

    # journal-level view: which collected journals show missing volumes
    jgaps = gaps_by_journal(args.runs_dir, args.seeds_dir)
    if jgaps:
        try:
            index = json.loads(open(args.index, encoding="utf-8").read())
            held: Dict[str, int] = {}
            for entry in index.values():
                j = entry.get("journal")
                if j:
                    held[j] = held.get(j, 0) + 1
        except (OSError, ValueError):
            held = {}
        print(f"\n=== journals with recorded volume gaps ({len(jgaps)}) ===")
        print(f"{'gaps':>5} {'held':>7}  journal")
        for journal, slot in sorted(jgaps.items(), key=lambda kv: -len(kv[1]["gaps"]))[:20]:
            print(f"{len(slot['gaps']):>5} {held.get(journal, 0):>7}  {journal[:56]}")

    if args.out:
        with open(args.out, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()) if rows else
                               ["domain","journal","collected","expected","ratio",
                                "volume_gaps","last_run","verdict"])
            w.writeheader(); w.writerows(rows)
        print(f"\nwrote {args.out} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
