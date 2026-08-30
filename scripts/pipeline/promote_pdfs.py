#!/usr/bin/env python3
"""Promote staged PDFs from artifacts/scraped_v2/ into corpus/scraped/.

The drainer/launcher writes to ``offprint/artifacts/scraped_v2/<host>/`` as a
staging bucket. This script SHA-256-dedups them against the canonical
``corpus/scraped/<host>/`` and MOVES net-new files into place. Staged files whose
bytes are already in the corpus are moved to ``archive/staging_retired/<ts>/`` so
they can be reviewed and then deleted.

Promotion used to hardlink and leave the staged copy in place as a "rollback
safety net". That convention is what grew the 160 GB of staging duplicates
removed on 2026-08-20, and this volume is NTFS via ntfs-3g (FUSE), where
hardlinks are not reliable in the first place. Staging is ephemeral: after a
promotion its files should be gone, not duplicated. Rollback now means moving a
file back out of ``archive/staging_retired/``, which is explicit and auditable.

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
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(os.environ.get("OFFPRINT_ROOT", "/mnt/shared_storage/law-review-corpus"))
SCRAPED_V2 = ROOT / "staging" / "scrape_inbox"
CORPUS = ROOT / "corpus" / "scraped"
LOG = CORPUS / "PROMOTION_LOG.csv"
RETIRED = ROOT / "archive" / "staging_retired"


def sha256(p: Path, buf_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        while chunk := fh.read(buf_size):
            h.update(chunk)
    return h.hexdigest()


def index_host(host_dir: Path, *, sizes: set[int] | None = None) -> dict[str, Path]:
    """Map SHA-256 to first path, optionally hashing only relevant byte sizes."""
    out: dict[str, Path] = {}
    if not host_dir.exists():
        return out
    for p in host_dir.rglob("*.pdf"):
        if p.suffix == ".partial" or ".partial" in p.name:
            continue
        try:
            if sizes is not None and p.stat().st_size not in sizes:
                continue
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


def retire_dest(src: Path, run_id: str) -> Path:
    """Where a redundant staged file goes so it can be deleted later.

    The staging path is preserved under the run directory, so what was retired
    and where it came from stays legible when someone decides to delete it.
    """
    try:
        relative = src.relative_to(ROOT / "staging")
    except ValueError:
        relative = Path(src.name)
    dest = RETIRED / run_id / relative
    stem, suffix, n = dest.stem, dest.suffix, 1
    while dest.exists():
        dest = dest.parent / f"{stem}__v{n}{suffix}"
        n += 1
    return dest


def promote_host(
    host: str, *, dry_run: bool = False, selected_paths: list[Path] | None = None,
    retire_duplicates: bool = True, retire_run: str = "",
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

    print("  hashing scraped_v2...", end=" ", flush=True)
    v2_idx = index_paths(selected_paths) if selected_paths is not None else index_host(v2_dir)
    print(f"{len(v2_idx)} unique sha")

    print("  hashing corpus...", end=" ", flush=True)
    selected_sizes = (
        {path.stat().st_size for path in selected_paths}
        if selected_paths is not None
        else None
    )
    corpus_idx = index_host(corpus_host, sizes=selected_sizes)
    print(f"{len(corpus_idx)} relevant unique sha")

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
        # `safe_dest` already guarantees a path that does not exist, which is what
        # keeps `shutil.move` -- which would otherwise overwrite silently -- from
        # clobbering a corpus manifestation.
        dest = safe_dest(corpus_host, src)
        try:
            shutil.move(str(src), str(dest))
            n_promoted += 1
        except OSError as e:
            n_skipped_link_collision += 1
            print(f"  warn: move {src} -> {dest} failed: {e}", file=sys.stderr)

    n_retired = 0
    if retire_duplicates:
        for sha in sorted(dup_shas):
            src = v2_idx[sha]
            try:
                dest = retire_dest(src, retire_run)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dest))
                n_retired += 1
            except OSError as e:
                print(f"  warn: retire {src} failed: {e}", file=sys.stderr)

    n_corpus_after = sum(1 for _ in corpus_host.rglob("*.pdf"))
    print(f"  promoted {n_promoted} moved. corpus now {n_corpus_after}"
          + (f"; retired {n_retired} staged duplicate(s)" if n_retired else ""))

    return {
        "host": host,
        "n_promoted": n_promoted,
        "n_skipped_dup": len(dup_shas),
        "n_retired_dup": n_retired,
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
        "n_retired_dup",
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
    global ROOT, SCRAPED_V2, CORPUS, LOG, RETIRED
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=ROOT)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--host", help="Promote only this host directory")
    g.add_argument("--all", action="store_true", help="Promote every host in scraped_v2/")
    ap.add_argument(
        "--keep-duplicates",
        action="store_true",
        help=(
            "Leave staged files whose bytes are already in the corpus where they "
            "are. Default is to MOVE them to archive/staging_retired/<ts>/ so the "
            "staging tree empties out and the retired copies can be deleted later."
        ),
    )
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
    RETIRED = ROOT / "archive" / "staging_retired"

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

    retire_run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    totals = {"n_promoted": 0, "n_skipped_dup": 0, "n_retired_dup": 0, "hosts": 0}
    for host in hosts:
        result = promote_host(
            host,
            dry_run=args.dry_run,
            selected_paths=selected_paths if host == args.host else None,
            retire_duplicates=not args.keep_duplicates,
            retire_run=retire_run,
        )
        if result.get("n_promoted") is not None:
            totals["n_promoted"] += result["n_promoted"]
            totals["n_skipped_dup"] += result["n_skipped_dup"]
            totals["n_retired_dup"] += result.get("n_retired_dup", 0)
            totals["hosts"] += 1
            if not args.dry_run and (result["n_promoted"] or result.get("n_retired_dup")):
                append_log(result)

    print("\n--- SUMMARY ---")
    print(f"hosts processed: {totals['hosts']}")
    print(f"PDFs promoted:   {totals['n_promoted']}")
    print(f"dups skipped:    {totals['n_skipped_dup']}")
    print(f"dups retired:    {totals['n_retired_dup']}")
    if totals["n_retired_dup"]:
        print(f"retired to:      {RETIRED / retire_run}")
    if args.dry_run:
        print("(dry run — nothing was moved)")
    elif totals["n_promoted"]:
        print(f"log: {LOG}")


if __name__ == "__main__":
    main()
