#!/usr/bin/env python3
"""Run the full liteparse+solver selector on a manifest of PDFs and report
strict-valid / vwg / invalid / empty distribution. No pre-computed baselines
required — operates directly on a sample manifest.

Usage:
    bench_holdout_1k.py --manifest artifacts/samples/sample_1k_holdout.txt --workers 4
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

os.chdir("/mnt/shared_storage/law-review-corpus/offprint")
sys.path.insert(0, "/mnt/shared_storage/law-review-corpus/offprint")

from offprint.pdf_footnotes.text_extract import extract_liteparse_candidate_documents
from offprint.pdf_footnotes.pipeline import (
    _select_liteparse_candidate_document,
    FootnoteProfile,
)
from offprint.pdf_footnotes.note_segment import segment_document_notes_extended


def run_one(pdf: str) -> dict:
    try:
        cands = extract_liteparse_candidate_documents(pdf)
        if not cands:
            return {"pdf": pdf, "error": "no_candidates"}
        sel = _select_liteparse_candidate_document(
            cands, profile_for=lambda d: FootnoteProfile()
        )
        if sel is None:
            return {"pdf": pdf, "error": "no_selection"}
        meta = sel.metadata or {}
        name = meta.get("liteparse_selected_candidate", "unknown")
        pc = meta.get("sequence_solver_precomputed")
        if pc:
            rep = pc.get("ordinality")
            notes = list(pc.get("notes") or [])
        else:
            notes, _, rep, _ = segment_document_notes_extended(sel, gap_tolerance=0)
        return {
            "pdf": pdf,
            "selected_candidate": name,
            "status": rep.status if rep else "empty",
            "notes": len(notes),
            "expected": rep.expected_range if rep else None,
            "gap_count": len(rep.gaps) if rep else 0,
        }
    except Exception as e:
        return {"pdf": pdf, "error": str(e)[:200]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="Path to manifest (one abs PDF path per line)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--out", default="artifacts/runs/holdout_1k_result.json")
    args = ap.parse_args()

    pdfs = [
        line.strip()
        for line in Path(args.manifest).read_text().splitlines()
        if line.strip()
    ]
    print(f"running {len(pdfs)} PDFs through full selector with {args.workers} workers")

    t0 = time.time()
    results = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(run_one, pdf): pdf for pdf in pdfs}
        done = 0
        for f in as_completed(futs):
            results.append(f.result())
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(pdfs)} ({time.time()-t0:.0f}s)")

    print(f"total elapsed: {time.time()-t0:.0f}s")

    cand_ct = Counter()
    status_ct = Counter()
    for r in results:
        cand_ct[r.get("selected_candidate") or "error"] += 1
        status_ct[r.get("status") or "error"] += 1

    print("\n=== Selected candidate breakdown ===")
    for c, ct in cand_ct.most_common():
        print(f"  {c:<50}: {ct}")

    print("\n=== Status breakdown ===")
    for s, ct in status_ct.most_common():
        print(f"  {s:<20}: {ct}")

    total = len(results)
    valid = status_ct.get("valid", 0)
    vog = status_ct.get("valid_with_gaps", 0)
    inv = status_ct.get("invalid", 0)
    emp = status_ct.get("empty", 0)
    err = sum(1 for r in results if r.get("error"))

    print(f"\nTotal: {total} (errors: {err})")
    print(f"strict-valid:    {valid}/{total} = {100*valid/max(total,1):.1f}%")
    print(f">=valid_with_gaps: {valid+vog}/{total} = {100*(valid+vog)/max(total,1):.1f}%")
    honest_denom = total - emp - err
    if honest_denom:
        print(f"strict (honest, excluding empty/error): {valid}/{honest_denom} = {100*valid/honest_denom:.1f}%")
        print(f">=vwg (honest): {valid+vog}/{honest_denom} = {100*(valid+vog)/honest_denom:.1f}%")

    Path(args.out).write_text(
        json.dumps(
            {"rows": results, "cand_ct": dict(cand_ct), "status_ct": dict(status_ct)},
            default=str,
            indent=2,
        )
    )
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
