#!/usr/bin/env python3
"""Run the real candidate selector (including sequence_solver) on the 1K manifest
and report which candidate wins per doc + end-to-end status."""
from __future__ import annotations
import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import sys
import os

os.chdir('/mnt/shared_storage/law-review-corpus/offprint')
sys.path.insert(0, '/mnt/shared_storage/law-review-corpus/offprint')

from offprint.pdf_footnotes.text_extract import extract_liteparse_candidate_documents
from offprint.pdf_footnotes.pipeline import _select_liteparse_candidate_document, FootnoteProfile
from offprint.pdf_footnotes.note_segment import segment_document_notes_extended


def run_one(pdf: str) -> dict:
    try:
        cands = extract_liteparse_candidate_documents(pdf)
        if not cands:
            return {"pdf": pdf, "error": "no_candidates"}
        sel = _select_liteparse_candidate_document(cands, profile_for=lambda d: FootnoteProfile())
        if sel is None:
            return {"pdf": pdf, "error": "no_selection"}
        meta = sel.metadata or {}
        name = meta.get("liteparse_selected_candidate", "unknown")
        # Prefer precomputed solver output when the selected candidate carried one.
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
    baseline = {r["pdf"]: r for r in json.load(open("artifacts/runs/liteparse_only_1k.json"))["rows"]}
    solver_baseline = {r["pdf"]: r for r in json.load(open("artifacts/runs/solver_1k.json"))["rows"]}
    included = [r["pdf"] for r in baseline.values() if r.get("doc_policy", {}).get("include")]
    print(f"running {len(included)} PDFs through full selector")

    t0 = time.time()
    results = []
    with ProcessPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(run_one, pdf): pdf for pdf in included}
        done = 0
        for f in as_completed(futs):
            r = f.result()
            results.append(r)
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(included)} ({time.time()-t0:.0f}s)")

    print(f"total elapsed: {time.time()-t0:.0f}s")

    from collections import Counter
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

    total = sum(1 for r in results if r.get("status") and r.get("status") != "error")
    valid = status_ct.get("valid", 0)
    vog = status_ct.get("valid_with_gaps", 0)
    print(f"\nFull-selector strict-valid: {valid}/{total} = {100*valid/max(total,1):.1f}%")
    print(f"Full-selector ≥valid_with_gaps: {valid+vog}/{total} = {100*(valid+vog)/max(total,1):.1f}%")

    # Baseline (ensemble without solver) for comparison
    base_valid = sum(1 for p in included if baseline[p].get("selected_status") == "valid")
    solver_only_valid = sum(1 for p in included if solver_baseline.get(p, {}).get("status") == "valid")
    print(f"\nFor comparison:")
    print(f"  Baseline ensemble (no solver):    {base_valid}/{len(included)} = {100*base_valid/len(included):.1f}% strict valid")
    print(f"  Solver standalone:                 {solver_only_valid}/{len(included)} = {100*solver_only_valid/len(included):.1f}% strict valid")

    Path("artifacts/runs/solver_integration_1k.json").write_text(
        json.dumps({"rows": results, "cand_ct": dict(cand_ct), "status_ct": dict(status_ct)},
                   default=str, indent=2)
    )


if __name__ == "__main__":
    main()
