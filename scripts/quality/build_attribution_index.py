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

import argparse, collections, glob, json, os, re, sys
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


DC_HOST_HINTS = ("digitalcommons", "scholarlycommons", "scholarship", "ir.", "repository",
                 "scholarworks", "commons", "ideaexchange", "brooklynworks", "lawnet",
                 "opencommons", "scholarcommons", "digitalrepository", "epublications")


def dc_series_map(seeds_dir: str, registry: str = "") -> Dict[str, str]:
    """(host, series) -> journal, read off bepress URLs like /flr/sitemap.xml.

    Reads the registry as well as the seeds, because many seeds carry no
    journal_name at all: both Fordham Law Review DC seeds are nameless, so the
    journal stayed unattributed with 3,241 dc-flr files sitting on disk. The
    registry row for that host does name it.
    """
    out: Dict[str, str] = {}
    if registry:
        import csv
        try:
            for row in csv.DictReader(open(registry, encoding="utf-8")):
                name = row.get("journal_name")
                for field in ("fixed_domain_url", "url"):
                    m = re.match(r"https?://([^/]+)/([A-Za-z0-9_\-]+)(?:/|$)",
                                 str(row.get(field) or "").strip())
                    if name and m and m.group(2).lower() not in {"index.php", "cgi", "do"}:
                        out.setdefault((m.group(1).lower(), m.group(2).lower()), name)
        except OSError:
            pass
    for path in glob.glob(os.path.join(seeds_dir, "*.json")):
        try:
            seed = json.loads(open(path, encoding="utf-8").read())
        except (OSError, ValueError):
            continue
        name = (seed.get("metadata") or {}).get("journal_name")
        if not name:
            continue
        for url in seed.get("start_urls") or []:
            m = re.match(r"https?://([^/]+)/([A-Za-z0-9_\-]+)(?:/|$)", str(url).strip())
            if not m:
                continue
            host, series = m.group(1).lower(), m.group(2).lower()
            if series in {"index.php", "cgi", "do", "wp-content"}:
                continue
            out.setdefault((host, series), name)
    return {f"{h}|{s}": n for (h, s), n in out.items()}


def backfill_from_filenames(index: Dict[str, dict], corpus: str, seeds_dir: str,
                            registry: str = "") -> int:
    """Attribute pre-manifest files using the dc-<series>- naming convention.

    Most of the corpus predates the run manifests, so the seed_url join cannot
    reach it. Digital Commons files were saved as dc-<series>-<id>.pdf, and the
    series segment is exactly what bepress seed URLs carry, so the two can be
    joined without any network access.
    """
    series = dc_series_map(seeds_dir, registry)
    added = 0
    if not os.path.isdir(corpus):
        return 0
    for host in sorted(os.listdir(corpus)):
        hdir = os.path.join(corpus, host)
        if not os.path.isdir(hdir):
            continue
        hl = host.lower()
        for fname in os.listdir(hdir):
            low = fname.lower()
            # Presence is not attribution: a record can be indexed from a manifest
            # with an empty journal, and skipping on presence alone left those
            # unattributed forever. Fordham Law Review was invisible this way,
            # with 3,241 dc-flr files on disk and none of them named.
            if not low.endswith(".pdf") or index.get(low, {}).get("journal"):
                continue
            m = re.match(r"dc-([a-z0-9]+)-", low)
            if not m:
                continue
            journal = series.get(f"{hl}|{m.group(1)}") or series.get(
                f"{hl.removeprefix('www.')}|{m.group(1)}")
            if not journal:
                continue
            index[low] = {"journal": journal, "host": host, "run": "filename-backfill"}
            added += 1
    return added


_ABBR_STOP = {"the", "of", "and", "for", "a", "in", "on", "at"}
_MULTI_TLD = ("ac.uk", "co.uk", "edu.au", "org.uk")


def _institution(host: str) -> str:
    """Registrable domain, so sibling hosts of one school group together."""
    parts = host.lower().removeprefix("www.").split(".")
    if len(parts) < 3:
        return ".".join(parts)
    tail = ".".join(parts[-2:])
    return ".".join(parts[-3:]) if tail in _MULTI_TLD else tail


_BLUEBOOK = {
    "law": "l", "review": "rev", "journal": "j", "business": "bus",
    "technology": "tech", "international": "int", "university": "u",
    "quarterly": "q", "policy": "pol", "science": "sci", "environmental": "envtl",
    "constitutional": "const", "criminal": "crim", "commercial": "com",
    "comparative": "comp", "corporate": "corp", "entertainment": "ent",
    "intellectual": "intell", "property": "prop", "college": "coll",
    "national": "natl", "american": "am", "public": "pub", "legal": "legal",
    "society": "socy", "rights": "rts", "liberties": "lib", "affairs": "aff",
    "forum": "f", "annual": "ann", "bulletin": "bull", "digest": "dig",
}


def _abbrevs(name: str) -> set:
    """Citation-style tokens a filename might carry for this journal.

    Files are saved with the Bluebook abbreviation, punctuation stripped:
    Columbia Business Law Review as ...columbuslrev... (colum + bus + l + rev),
    Berkeley Technology Law Journal as ...berkeley-tech-l-j... Each word takes
    its own standard abbreviation rather than a uniform prefix, so build the
    leading word at several lengths and abbreviate the rest by table.

    catalog/abbrev_map.csv would be the natural lookup but is corrupted - its
    abbreviation column holds paragraphs of article text.
    """
    words = [w for w in re.findall(r"[A-Za-z]+", name.lower()) if w not in _ABBR_STOP]
    if not words:
        return set()
    head, rest = words[0], words[1:]
    tail_std = "".join(_BLUEBOOK.get(w, w[:4]) for w in rest)
    tail_full = "".join(rest)
    out = set()
    for n in (3, 4, 5, 6, len(head)):
        out.add(head[:n] + tail_std)
        out.add(head[:n] + tail_full)
    out.add("".join(words))
    out.add("".join(_BLUEBOOK.get(w, w[:4]) for w in words))
    return {o for o in out if len(o) >= 8}


def backfill_from_abbreviations(index: Dict[str, dict], corpus: str, registry: str) -> int:
    """Attribute files named with the journal's citation abbreviation.

    Only journals the registry lists for that host are considered, so a token can
    only ever resolve to a journal actually published there.
    """
    import csv
    by_host: Dict[str, set] = collections.defaultdict(set)
    try:
        for row in csv.DictReader(open(registry, encoding="utf-8")):
            host = (row.get("host") or "").lower().removeprefix("www.")
            if host and row.get("journal_name"):
                # Index under the institution, not the exact host. A journal is
                # routinely registered at one hostname and served from another:
                # Columbia Business Law Review is registered at cblr.columbia.edu
                # while its PDFs live under journals.library.columbia.edu. Keying
                # on the exact host misses every such case - which is how a
                # duplicate CBLR crawl got launched.
                by_host[host].add(row["journal_name"])
                by_host[_institution(host)].add(row["journal_name"])
    except OSError:
        return 0
    added = 0
    for host in sorted(os.listdir(corpus)):
        hdir = os.path.join(corpus, host)
        if not os.path.isdir(hdir):
            continue
        cands: Dict[str, str] = {}
        hl = host.lower().removeprefix("www.")
        for jname in set(by_host.get(hl, ())) | set(by_host.get(_institution(hl), ())):
            for token in _abbrevs(jname):
                cands.setdefault(token, jname)
        if not cands:
            continue
        for fname in os.listdir(hdir):
            low = fname.lower()
            if not low.endswith(".pdf") or (low in index and index[low].get("journal")):
                continue
            flat = re.sub(r"[^a-z]", "", low)
            for token, jname in cands.items():
                if token in flat:
                    index[low] = {"journal": jname, "host": host, "run": "abbrev-backfill"}
                    added += 1
                    break
    return added


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--runs-dir", default="artifacts/runs")
    ap.add_argument("--seeds-dir", default="offprint/sitemaps")
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--query", default="", help="report the file count for a journal name")
    ap.add_argument("--top", type=int, default=0, help="show the N best-covered journals")
    ap.add_argument("--backfill", action="store_true",
                    help="also attribute pre-manifest files via the dc-<series>- convention")
    ap.add_argument("--corpus", default="/mnt/shared_storage/law-review-corpus/corpus/scraped")
    ap.add_argument("--registry",
                    default="/mnt/shared_storage/law-review-corpus/offprint/data/registry/lawjournals.csv")
    args = ap.parse_args(argv)

    if os.path.exists(args.out) and not args.query and not args.top:
        print(f"{args.out} exists; rebuilding")
    index = build(args.runs_dir, args.seeds_dir)
    if args.backfill:
        n = backfill_from_filenames(index, args.corpus, args.seeds_dir, args.registry)
        print(f"dc-series backfill attributed {n} further files", file=sys.stderr)
        n2 = backfill_from_abbreviations(index, args.corpus, args.registry)
        print(f"abbreviation backfill attributed {n2} further files", file=sys.stderr)

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
