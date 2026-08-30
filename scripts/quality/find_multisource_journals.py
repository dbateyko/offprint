#!/usr/bin/env python3
"""Flag journals whose seeds point at more than one source host.

A filename-level coverage check catches re-crawling the SAME source. It cannot
catch the more expensive mistake: crawling a second source for a journal we
already hold. Fordham Law Review is collected as 3,241 dc-flr-*.pdf files from
its Digital Commons mirror, while a separate seed targets the journal's own
WordPress site with 2,366 articles. The two sources name files completely
differently, so nothing filename-based would ever notice the overlap.

This compares seeds by normalized journal name and reports where the same
journal has seeds on several hosts, marking which of those hosts the corpus
already holds.

A flag is not automatically a skip: the two sources often split by era - a
bepress backfile covering early volumes and a WordPress site carrying recent
ones - so the uncollected source may still hold genuinely new years. Check the
volume ranges before deciding.

Usage:
  python scripts/quality/find_multisource_journals.py [--only-risky] [--json out.json]
"""
from __future__ import annotations

import argparse, collections, glob, json, os, re, sys
from typing import Dict, List, Optional
from urllib.parse import urlparse

CORPUS = "/mnt/shared_storage/law-review-corpus/corpus/scraped"
_STOP = re.compile(r"\b(the|of|and|for|journal|review|law|a|on)\b")


def norm_name(name: str) -> str:
    return re.sub(r"[^a-z]", "", _STOP.sub("", (name or "").lower()))


def host_of(url: str) -> str:
    return urlparse(str(url).replace("[", "1").replace("]", "")).netloc.lower()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seeds-dir", default="offprint/sitemaps")
    ap.add_argument("--only-risky", action="store_true",
                    help="only journals where some sources are collected and others are not")
    ap.add_argument("--json", default="")
    args = ap.parse_args(argv)

    held = set(os.listdir(CORPUS)) if os.path.isdir(CORPUS) else set()

    def collected(host: str) -> bool:
        bare = host.removeprefix("www.")
        return host in held or bare in held or f"www.{bare}" in held

    def pdf_count(host: str) -> int:
        for cand in (host, host.removeprefix("www."), "www." + host.removeprefix("www.")):
            p = os.path.join(CORPUS, cand)
            if os.path.isdir(p):
                return sum(1 for f in os.listdir(p) if f.lower().endswith(".pdf"))
        return 0

    groups: Dict[str, List[dict]] = collections.defaultdict(list)
    for path in sorted(glob.glob(os.path.join(args.seeds_dir, "*.json"))):
        try:
            seed = json.loads(open(path, encoding="utf-8").read())
        except (OSError, ValueError):
            continue
        meta = seed.get("metadata") or {}
        name = meta.get("journal_name")
        urls = seed.get("start_urls") or []
        if not name or not urls:
            continue
        host = host_of(urls[0])
        if not host:
            continue
        groups[norm_name(name)].append(
            {"seed": os.path.basename(path), "host": host, "name": name,
             "collected": collected(host), "pdfs": pdf_count(host)})

    multi = {k: v for k, v in groups.items() if len({e["host"] for e in v}) > 1}
    risky = {k: v for k, v in multi.items()
             if any(e["collected"] for e in v) and any(not e["collected"] for e in v)}
    print(f"journals with seeds on >1 host : {len(multi)}")
    print(f"  of those, part-collected     : {len(risky)}  <- crawling the uncollected "
          f"source may duplicate the journal")
    chosen = risky if args.only_risky else multi
    for _k, entries in sorted(chosen.items(), key=lambda kv: kv[1][0]["name"]):
        print(f"\n  {entries[0]['name'][:60]}")
        for e in sorted(entries, key=lambda e: (not e["collected"], e["host"])):
            mark = f"COLLECTED ({e['pdfs']} pdfs)" if e["collected"] else "not collected"
            print(f"     {mark:26s} {e['host'][:34]:35s} {e['seed'][:42]}")
    if args.json:
        json.dump({k: v for k, v in chosen.items()}, open(args.json, "w"), indent=1)
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
