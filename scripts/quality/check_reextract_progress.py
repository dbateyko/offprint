#!/usr/bin/env python3
"""Progress + health check for the P0-1 boundary-fix corpus re-extract.

Reports, for a re-extract run started at a given epoch time:
  * sidecars rewritten since the run start (mtime > run_start)
  * % complete vs the eligible sidecar universe (total minus OCR-protected)
  * empty / zero-note sidecar count among rewritten files (ALARM if >0.5%)
  * cross-page vs same-page note median text length on a rolling random
    sample of freshly-rewritten sidecars (confirms the bleed fix is landing)

CPU-only, read-only. Safe to run alongside the live re-extract and the GPU
labeling run.

Example:
    python scripts/quality/check_reextract_progress.py \
        --pdf-root /mnt/shared_storage/law-review-corpus/corpus/scraped \
        --run-start-file artifacts/runs/reextract_boundaryfix.start \
        --protected-manifest artifacts/runs/ocr_protection_exclusions_20260710.jsonl \
        --sample 400
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import random
import statistics
import time


def _load_protected(manifest_path: str) -> set[str]:
    protected: set[str] = set()
    if not manifest_path or not os.path.exists(manifest_path):
        return protected
    with open(manifest_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            src = str(row.get("source_pdf_path") or "").strip()
            if src:
                # sidecar sits next to the pdf
                protected.add(os.path.abspath(src) + ".footnotes.json")
    return protected


def _note_lengths(sidecar_path: str) -> tuple[list[int], list[int], int]:
    """Return (same_page_lengths, cross_page_lengths, note_count)."""
    try:
        with open(sidecar_path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except Exception:
        return [], [], -1
    notes = doc.get("notes") or {}
    same: list[int] = []
    cross: list[int] = []
    for n in notes.values():
        text = n.get("text") or ""
        ps, pe = n.get("page_start"), n.get("page_end")
        if ps is not None and pe is not None and pe > ps:
            cross.append(len(text))
        else:
            same.append(len(text))
    return same, cross, len(notes)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--pdf-root",
        default="/mnt/shared_storage/law-review-corpus/corpus/scraped",
        help="Root under which *.pdf.footnotes.json sidecars live.",
    )
    ap.add_argument(
        "--run-start",
        type=float,
        default=None,
        help="Run-start epoch seconds. A sidecar counts as rewritten if mtime >= this.",
    )
    ap.add_argument(
        "--run-start-file",
        default="",
        help="File whose mtime (or single float line) marks the run start. "
        "Used when --run-start is not given.",
    )
    ap.add_argument(
        "--protected-manifest",
        default="",
        help="JSONL of OCR-protected PDFs (source_pdf_path per line) to exclude from the eligible denominator.",
    )
    ap.add_argument("--sample", type=int, default=400, help="Rolling sample size for length stats.")
    ap.add_argument(
        "--empty-alarm-pct",
        type=float,
        default=0.5,
        help="Alarm threshold (percent) for empty/zero-note rewritten sidecars.",
    )
    args = ap.parse_args()

    run_start = args.run_start
    if run_start is None and args.run_start_file:
        if os.path.exists(args.run_start_file):
            try:
                with open(args.run_start_file) as fh:
                    run_start = float(fh.read().strip())
            except (ValueError, OSError):
                run_start = os.path.getmtime(args.run_start_file)
    if run_start is None:
        raise SystemExit("Provide --run-start EPOCH or --run-start-file PATH")

    root = os.path.abspath(args.pdf_root)
    all_sidecars = glob.glob(os.path.join(root, "*", "*.pdf.footnotes.json"))
    total = len(all_sidecars)

    protected = _load_protected(args.protected_manifest)
    eligible = [s for s in all_sidecars if os.path.abspath(s) not in protected]
    n_eligible = len(eligible)
    n_protected_present = total - n_eligible

    # rewritten = eligible sidecar with mtime at/after run start
    rewritten = []
    for s in eligible:
        try:
            if os.path.getmtime(s) >= run_start:
                rewritten.append(s)
        except OSError:
            continue
    n_rewritten = len(rewritten)

    # empty-sidecar check + length stats on a sample of rewritten files
    sample = rewritten
    if len(sample) > args.sample:
        random.seed(0)
        sample = random.sample(rewritten, args.sample)

    empty = 0
    same_all: list[int] = []
    cross_all: list[int] = []
    read_ok = 0
    for s in sample:
        same, cross, nc = _note_lengths(s)
        if nc < 0:
            continue
        read_ok += 1
        if nc == 0:
            empty += 1
        same_all.extend(same)
        cross_all.extend(cross)

    empty_pct = (100.0 * empty / read_ok) if read_ok else 0.0
    pct_complete = (100.0 * n_rewritten / n_eligible) if n_eligible else 0.0

    def _med(xs: list[int]) -> str:
        return f"{statistics.median(xs):.0f}" if xs else "n/a"

    print("=== re-extract progress check ===")
    print(f"run_start           : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(run_start))}")
    print(f"pdf_root            : {root}")
    print(f"total sidecars      : {total}")
    print(f"OCR-protected (skip): {n_protected_present}")
    print(f"eligible            : {n_eligible}")
    print(f"rewritten (mtime>=) : {n_rewritten}")
    print(f"pct complete        : {pct_complete:.2f}%")
    print(f"sample read         : {read_ok} sidecars")
    print(
        f"empty/zero-note     : {empty} ({empty_pct:.2f}%)  "
        f"{'*** ALARM ***' if empty_pct > args.empty_alarm_pct else 'ok'}"
    )
    print(f"same-page notes      n={len(same_all):>6d}  median_len={_med(same_all)}")
    print(f"cross-page notes     n={len(cross_all):>6d}  median_len={_med(cross_all)}")


if __name__ == "__main__":
    main()
