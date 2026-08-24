#!/usr/bin/env python3
"""Promote staged PDFs from artifacts/scraped_v2/ into corpus/scraped/.

The drainer/launcher writes to ``offprint/artifacts/scraped_v2/<host>/`` as a
staging bucket. This script SHA-256-dedups them against the canonical
``corpus/scraped/<host>/`` and hardlinks net-new files into place. Hardlinks
keep both paths pointing at the same inode — atomic, free disk space, and
reversible (the scraped_v2 copy is retained as a rollback safety net).

Each promotion appends a row to ``corpus/scraped/PROMOTION_LOG.csv``:
    host, n_promoted, n_skipped_dup, n_corpus_before, n_corpus_after, ts

Usage:
    promote_pdfs.py --all
    promote_pdfs.py --host brooklynworks.brooklaw.edu
    promote_pdfs.py --host example.edu --allowlist eligible_paths.txt
    promote_pdfs.py --all --dry-run
"""

from __future__ import annotations
import argparse
import csv
import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(os.environ.get("OFFPRINT_ROOT", "/mnt/shared_storage/law-review-corpus"))
SCRAPED_V2 = ROOT / "staging" / "scrape_inbox"
CORPUS = ROOT / "corpus" / "scraped"
LOG = CORPUS / "PROMOTION_LOG.csv"


def sha256(p: Path, buf_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        while chunk := fh.read(buf_size):
            h.update(chunk)
    return h.hexdigest()


def index_host(host_dir: Path) -> dict[str, Path]:
    """Map sha256 -> first-seen path. Skips files matching '.partial'."""
    out: dict[str, Path] = {}
    if not host_dir.exists():
        return out
    for p in host_dir.rglob("*.pdf"):
        if p.suffix == ".partial" or ".partial" in p.name:
            continue
        try:
            digest = sha256(p)
        except OSError as e:
            print(f"  warn: cannot read {p}: {e}", file=sys.stderr)
            continue
        out.setdefault(digest, p)
    return out


def index_paths(paths: list[Path]) -> dict[str, Path]:
    """Map SHA-256 to the first selected PDF path."""
    out: dict[str, Path] = {}
    for p in paths:
        try:
            digest = sha256(p)
        except OSError as e:
            print(f"  warn: cannot read {p}: {e}", file=sys.stderr)
            continue
        out.setdefault(digest, p)
    return out


def safe_dest(corpus_host: Path, src: Path) -> Path:
    """Compute a non-clobbering destination filename in corpus_host."""
    dest = corpus_host / src.name
    if not dest.exists():
        return dest
    # If existing has identical content (different sha but same name? rare),
    # disambiguate with a short hash suffix.
    stem, suffix = dest.stem, dest.suffix
    n = 1
    while True:
        cand = corpus_host / f"{stem}__v{n}{suffix}"
        if not cand.exists():
            return cand
        n += 1


def promote_host(
    host: str, *, dry_run: bool = False, selected_paths: list[Path] | None = None
) -> dict:
    v2_dir = SCRAPED_V2 / host
    corpus_host = CORPUS / host
    if not v2_dir.exists():
        return {"host": host, "skipped": "no v2 dir"}

    n_corpus_before = sum(1 for _ in corpus_host.rglob("*.pdf")) if corpus_host.exists() else 0
    print(f"\n=== {host} ===")
    staged_count = (
        len(selected_paths)
        if selected_paths is not None
        else sum(1 for _ in v2_dir.rglob("*.pdf"))
    )
    print(f"  scraped_v2: {staged_count} PDFs")
    print(f"  corpus before: {n_corpus_before} PDFs")

    print("  hashing corpus...", end=" ", flush=True)
    corpus_idx = index_host(corpus_host)
    print(f"{len(corpus_idx)} unique sha")

    print("  hashing scraped_v2...", end=" ", flush=True)
    v2_idx = index_paths(selected_paths) if selected_paths is not None else index_host(v2_dir)
    print(f"{len(v2_idx)} unique sha")

    new_shas = sorted(set(v2_idx) - set(corpus_idx))
    dup_shas = set(v2_idx) & set(corpus_idx)
    print(f"  net-new: {len(new_shas)}  dup: {len(dup_shas)}")

    if dry_run:
        return {
            "host": host,
            "n_promoted": 0,
            "n_skipped_dup": len(dup_shas),
            "n_corpus_before": n_corpus_before,
            "n_corpus_after": n_corpus_before,
            "dry_run": True,
        }

    if not new_shas:
        return {
            "host": host,
            "n_promoted": 0,
            "n_skipped_dup": len(dup_shas),
            "n_corpus_before": n_corpus_before,
            "n_corpus_after": n_corpus_before,
        }

    corpus_host.mkdir(parents=True, exist_ok=True)
    n_promoted = 0
    n_skipped_link_collision = 0
    for sha in new_shas:
        src = v2_idx[sha]
        dest = safe_dest(corpus_host, src)
        try:
            os.link(src, dest)
            n_promoted += 1
        except OSError as e:
            n_skipped_link_collision += 1
            print(f"  warn: link {src} -> {dest} failed: {e}", file=sys.stderr)

    n_corpus_after = sum(1 for _ in corpus_host.rglob("*.pdf"))
    print(f"  promoted {n_promoted} hardlinks. corpus now {n_corpus_after}")

    return {
        "host": host,
        "n_promoted": n_promoted,
        "n_skipped_dup": len(dup_shas),
        "n_corpus_before": n_corpus_before,
        "n_corpus_after": n_corpus_after,
        "n_link_collision": n_skipped_link_collision,
    }


def append_log(row: dict) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    write_header = not LOG.exists()
    fields = [
        "ts",
        "host",
        "n_promoted",
        "n_skipped_dup",
        "n_corpus_before",
        "n_corpus_after",
        "n_link_collision",
    ]
    out = {k: "" for k in fields}
    out.update(row)
    out["ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with LOG.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerow(out)


def main() -> None:
    global ROOT, SCRAPED_V2, CORPUS, LOG
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=ROOT)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--host", help="Promote only this host directory")
    g.add_argument("--all", action="store_true", help="Promote every host in scraped_v2/")
    ap.add_argument(
        "--allowlist",
        type=Path,
        help="With --host, promote only listed PDFs (absolute or relative to that host directory)",
    )
    ap.add_argument(
        "--source-root",
        type=Path,
        help="Optional staging root for allowlisted derived PDFs (must remain under ROOT/staging)",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    ROOT = args.root.expanduser().resolve()
    SCRAPED_V2 = ROOT / "staging" / "scrape_inbox"
    CORPUS = ROOT / "corpus" / "scraped"
    LOG = CORPUS / "PROMOTION_LOG.csv"

    if not SCRAPED_V2.exists():
        print(f"no scraped_v2 dir at {SCRAPED_V2}")
        return

    if (args.allowlist or args.source_root) and not args.host:
        ap.error("--allowlist/--source-root require --host")
    if args.source_root and not args.allowlist:
        ap.error("--source-root requires --allowlist")

    selected_paths: list[Path] | None = None
    if args.allowlist:
        host_root = (
            args.source_root.expanduser().resolve()
            if args.source_root
            else (SCRAPED_V2 / args.host).resolve()
        )
        staging_root = (ROOT / "staging").resolve()
        try:
            host_root.relative_to(staging_root)
        except ValueError:
            ap.error(f"allowlist source root must remain under {staging_root}: {host_root}")
        if not host_root.is_dir():
            ap.error(f"allowlist source root does not exist: {host_root}")
        selected_paths = []
        for raw in args.allowlist.read_text(encoding="utf-8").splitlines():
            value = raw.strip()
            if not value or value.startswith("#"):
                continue
            candidate = Path(value)
            if not candidate.is_absolute():
                candidate = host_root / candidate
            candidate = candidate.resolve()
            try:
                candidate.relative_to(host_root)
            except ValueError:
                ap.error(f"allowlisted path escapes host staging directory: {candidate}")
            if candidate.suffix.lower() != ".pdf" or not candidate.is_file():
                ap.error(f"allowlisted PDF does not exist: {candidate}")
            selected_paths.append(candidate)

    if args.host:
        hosts = [args.host]
    else:
        hosts = sorted(d.name for d in SCRAPED_V2.iterdir() if d.is_dir())

    totals = {"n_promoted": 0, "n_skipped_dup": 0, "hosts": 0}
    for host in hosts:
        result = promote_host(
            host,
            dry_run=args.dry_run,
            selected_paths=selected_paths if host == args.host else None,
        )
        if result.get("n_promoted") is not None:
            totals["n_promoted"] += result["n_promoted"]
            totals["n_skipped_dup"] += result["n_skipped_dup"]
            totals["hosts"] += 1
            if not args.dry_run and result["n_promoted"]:
                append_log(result)

    print("\n--- SUMMARY ---")
    print(f"hosts processed: {totals['hosts']}")
    print(f"PDFs promoted:   {totals['n_promoted']}")
    print(f"dups skipped:    {totals['n_skipped_dup']}")
    if args.dry_run:
        print("(dry run — nothing was hardlinked)")
    elif totals["n_promoted"]:
        print(f"log: {LOG}")


if __name__ == "__main__":
    main()
