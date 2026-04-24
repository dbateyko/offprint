#!/usr/bin/env python3
"""End-to-end measurement on the 1K sample with current doc_policy + solver +
precomputed-aware selector. Produces the honest article_validity_rate."""
from __future__ import annotations
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

os.chdir('/mnt/shared_storage/law-review-corpus/offprint')
sys.path.insert(0, '/mnt/shared_storage/law-review-corpus/offprint')

from offprint.pdf_footnotes.doc_policy import (
    classify_pdf, collect_signals, read_first_page_overview,
    infer_domain, infer_platform_family, load_rules,
)
from offprint.pdf_footnotes.text_extract import extract_liteparse_candidate_documents
from offprint.pdf_footnotes.pipeline import _select_liteparse_candidate_document, FootnoteProfile
from offprint.pdf_footnotes.note_segment import segment_document_notes_extended


def run_one(pdf: str) -> dict:
    try:
        rules = load_rules()
        domain = infer_domain(pdf, pdf_root='/mnt/shared_storage/law-review-corpus/corpus/scraped')
        platform = infer_platform_family(domain=domain)
        page_count, first_text = read_first_page_overview(pdf)
        signals = collect_signals(first_text, page_count, metadata=None)
        decision = classify_pdf(
            pdf_path=pdf, domain=domain, platform_family=platform,
            signals=signals, doc_policy='article_only', rules=rules,
        )
        if not decision.include:
            return {"pdf": pdf, "status": "excluded", "doc_type": decision.doc_type}
        # Extract + select
        cands = extract_liteparse_candidate_documents(pdf)
        if not cands:
            return {"pdf": pdf, "status": "empty", "doc_type": decision.doc_type, "note": "no_candidates"}
        sel = _select_liteparse_candidate_document(cands, profile_for=lambda d: FootnoteProfile())
        if sel is None:
            return {"pdf": pdf, "status": "empty", "doc_type": decision.doc_type, "note": "no_selection"}
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
            "status": rep.status if rep else "empty",
            "doc_type": decision.doc_type,
            "selected_candidate": name,
            "notes": len(notes),
            "expected": rep.expected_range if rep else None,
            "gap_count": len(rep.gaps) if rep else 0,
            "gaps": list(rep.gaps) if rep else [],
            "pages": decision and (decision.to_dict().get("page_count") if hasattr(decision, "to_dict") else None),
        }
    except Exception as e:
        return {"pdf": pdf, "error": str(e)[:200]}


def main():
    manifest = Path("artifacts/samples/sample_1k.txt")
    pdfs = [ln.strip() for ln in manifest.read_text().splitlines() if ln.strip()]
    print(f"running end-to-end on {len(pdfs)} PDFs")

    t0 = time.time()
    results = []
    with ProcessPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(run_one, p) for p in pdfs]
        done = 0
        for f in as_completed(futs):
            r = f.result()
            results.append(r)
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(pdfs)} ({time.time()-t0:.0f}s)")

    from collections import Counter
    status_ct = Counter(r.get("status", "error") for r in results)
    doc_type_ct = Counter(r.get("doc_type", "—") for r in results)
    print(f"\ntotal elapsed: {time.time()-t0:.0f}s\n")
    print("=== Status counts ===")
    for s, ct in status_ct.most_common():
        print(f"  {s:<20}: {ct}")
    print("\n=== Doc_type counts ===")
    for d, ct in doc_type_ct.most_common():
        print(f"  {d:<20}: {ct}")

    included = [r for r in results if r.get("status") != "excluded" and r.get("status") != "error"]
    valid = sum(1 for r in included if r.get("status") == "valid")
    vog = sum(1 for r in included if r.get("status") == "valid_with_gaps")
    empty_nonfm = sum(1 for r in included if r.get("status") == "empty")
    invalid = sum(1 for r in included if r.get("status") == "invalid")

    print(f"\n=== Article validity on {len(included)} included docs ===")
    print(f"  strict valid:      {valid}/{len(included)} = {100*valid/max(len(included),1):.1f}%")
    print(f"  ≥valid_with_gaps:  {valid+vog}/{len(included)} = {100*(valid+vog)/max(len(included),1):.1f}%")
    print(f"  invalid:           {invalid}")
    print(f"  empty:             {empty_nonfm}")

    Path("artifacts/runs/end_to_end_1k.json").write_text(
        json.dumps({"rows": results, "status": dict(status_ct), "doc_type": dict(doc_type_ct)},
                   default=str, indent=2)
    )
    print(f"\nWrote artifacts/runs/end_to_end_1k.json")


if __name__ == "__main__":
    main()
