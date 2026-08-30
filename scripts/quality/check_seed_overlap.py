#!/usr/bin/env python3
"""Report how much of a journal we already hold before crawling it.

Written after a Harvard CR-CL run re-downloaded 185 PDFs the corpus already
had. The mistake was checking coverage at HOST level: journals.law.harvard.edu
showed as "covered", so a per-journal gap looked new when most of it was not --
while journals.library.columbia.edu holds 2,012 PDFs of which none are CBLR, so
host presence alone would also wrongly skip a real gap. Coverage has to be
judged per journal, by filename, against corpus/scraped/<host>/.

Give it PDF URLs (a mapped list, or a seed whose start_urls are PDFs) and it
reports what is already held, what is genuinely new, and whether crawling is
worth the requests.

Usage:
  python scripts/quality/check_seed_overlap.py --urls-json mapped.json --host journals.law.harvard.edu
  python scripts/quality/check_seed_overlap.py --seed offprint/sitemaps/x.json
"""
from __future__ import annotations

import argparse, json, os, sys
from typing import Iterable, List, Optional, Set
from urllib.parse import urlparse

CORPUS = "/mnt/shared_storage/law-review-corpus/corpus/scraped"
INDEX = "artifacts/attribution_index.json"


def journal_holdings(journal: str, index_path: str = INDEX) -> int:
    """How many PDFs the corpus already holds for this journal, by name.

    Filename comparison only catches re-crawling the SAME source. It said "0
    duplicates" for Columbia Business Law Review while 1,121 of its PDFs sat on
    disk under citation-style names, and "worth crawling" for Fordham Law Review
    with 3,241 already held. The attribution index answers by journal instead,
    which is the question onboarding actually asks.
    """
    if not journal:
        return 0
    try:
        index = json.loads(open(index_path, encoding="utf-8").read())
    except (OSError, ValueError):
        return 0
    want = journal.strip().lower()
    return sum(1 for v in index.values()
               if (v.get("journal") or "").strip().lower() == want)


def corpus_pdfs(host: str) -> Set[str]:
    """Filenames already held for a host, trying the www and bare forms."""
    for cand in (host, host.removeprefix("www."), "www." + host.removeprefix("www.")):
        path = os.path.join(CORPUS, cand)
        if os.path.isdir(path):
            return {f.lower() for f in os.listdir(path) if f.lower().endswith(".pdf")}
    return set()


def staged_pdfs(staging_root: str) -> Set[str]:
    out: Set[str] = set()
    if not staging_root or not os.path.isdir(staging_root):
        return out
    for root, _dirs, files in os.walk(staging_root):
        out |= {f.lower() for f in files if f.lower().endswith(".pdf")}
    return out


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--urls-json", help="JSON list of PDF URLs, or an object with a 'pdfs' key")
    ap.add_argument("--seed", help="seed file whose start_urls are PDF URLs")
    ap.add_argument("--host", help="override the host to compare against")
    ap.add_argument("--staging", default="", help="also treat this staging dir as already held")
    ap.add_argument("--journal", default="", help="journal name to look up in the attribution index")
    ap.add_argument("--index", default=INDEX)
    ap.add_argument("--write-missing", default="", help="write the not-yet-held URLs here")
    args = ap.parse_args(argv)

    journal = ""
    if args.seed:
        try:
            journal = ((json.loads(open(args.seed, encoding="utf-8").read()).get("metadata")
                        or {}).get("journal_name") or "")
        except (OSError, ValueError):
            journal = ""
    journal = args.journal or journal
    if journal:
        held_by_name = journal_holdings(journal, args.index)
        print(f"attribution index  : {held_by_name} pdfs already held for {journal!r}")
        if held_by_name:
            print("  NOTE: this journal is already represented in the corpus. A filename")
            print("  comparison cannot see holdings that came from a different source.")

    urls: List[str] = []
    if args.urls_json:
        blob = json.loads(open(args.urls_json, encoding="utf-8").read())
        urls = blob if isinstance(blob, list) else list(blob.get("pdfs") or [])
    elif args.seed:
        urls = list(json.loads(open(args.seed, encoding="utf-8").read()).get("start_urls") or [])
    if not urls:
        print("no URLs to check"); return 2
    pdf_urls = [u for u in urls if u.lower().split("?")[0].endswith(".pdf")]
    if not pdf_urls:
        host = args.host or urlparse(urls[0]).netloc
        held = corpus_pdfs(host)
        print(f"start_urls are landing pages, not PDFs - filename comparison is not possible.")
        print(f"corpus already holds {len(held)} PDFs for {host}.")
        print("Map the journal's PDF URLs first, or inspect a sample before crawling.")
        return 0

    host = args.host or urlparse(pdf_urls[0]).netloc
    held = corpus_pdfs(host) | staged_pdfs(args.staging)
    names = {u: u.rsplit("/", 1)[-1].lower() for u in pdf_urls}
    have = [u for u, n in names.items() if n in held]
    missing = [u for u, n in names.items() if n not in held]
    total = len(pdf_urls)
    print(f"host              : {host}")
    print(f"corpus holds      : {len(held)} pdfs for this host")
    print(f"journal lists     : {total} pdfs")
    print(f"  already held    : {len(have)} ({len(have)/total:.0%})")
    print(f"  genuinely new   : {len(missing)} ({len(missing)/total:.0%})")
    if len(missing) == 0:
        print("\nVERDICT: nothing to fetch - do not crawl.")
    elif len(have) / total > 0.5:
        print(f"\nVERDICT: mostly duplicate. Fetch the {len(missing)} missing files directly "
              "rather than re-crawling the archive.")
    else:
        print("\nVERDICT: worth crawling.")
    if args.write_missing and missing:
        json.dump(sorted(missing), open(args.write_missing, "w"))
        print(f"wrote {len(missing)} missing URLs -> {args.write_missing}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
