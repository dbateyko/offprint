#!/usr/bin/env python3
"""Triage the solver's remaining failures to separate fixable from needs-OCR."""
from __future__ import annotations
import json
import re
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from solver_poc import load_lit_json, collect_candidates  # noqa

NONARTICLE_RE = re.compile(
    r'full[-_]?issue|full[-_]?volume|masthead|front[-_]?matter|back[-_]?matter|'
    r'table[-_]?of[-_]?contents|toc[-_]|foreword|preface|coversheet|masthead|'
    r'announcement|staff[-_]?(?:member|app)|memorial|tribute|acknowledg|brochure|bulletin|'
    r'symposium[-_](?:program|agenda)|program|call[-_]for[-_]papers|'
    r'blueprint|bibliography|testimony|weekly[-_]?report|'
    r'application|roundtable|summary|chart|non[-_]?discrimination|'
    r'one[-_]pager|picture|image|vol[-_]?\d+[_-]?(?:iss|no)|'
    r'compressed|ebriefing|viewpiece|viewbook|eprs-stu|rules[-_]of[-_]procedure',
    re.IGNORECASE,
)


def triage_one(row: dict) -> dict:
    pdf = row['pdf']
    name = pdf.split('/')[-1].lower()
    result = dict(row)
    # Is non-article by filename or length
    if NONARTICLE_RE.search(name):
        result['triage'] = 'non_article_filename'
        return result
    if (row.get('pages') or 0) > 80:
        result['triage'] = 'long_doc_likely_compilation'
        return result
    # Check text layer for each gap: do the missing labels exist as raw textItems?
    try:
        pages = load_lit_json(pdf)
    except Exception as e:
        result['triage'] = 'extraction_error'
        result['err'] = str(e)[:100]
        return result
    gaps = row.get('gaps') or []
    missing_in_text: list[int] = []
    have_in_text: list[int] = []
    for gap in gaps[:10]:
        found = False
        for p in pages:
            for it in p.items:
                t = (it.get('text') or '').strip()
                if t in (str(gap), f'{gap}.', f'{gap})'):
                    found = True
                    break
            if found:
                break
        (have_in_text if found else missing_in_text).append(gap)
    result['gaps_in_text_layer'] = have_in_text
    result['gaps_not_in_text_layer'] = missing_in_text
    result['page_count_solver'] = len(pages)
    # Decision:
    if missing_in_text and not have_in_text:
        result['triage'] = 'needs_ocr_all_gaps_missing'
    elif missing_in_text and have_in_text:
        result['triage'] = 'partial_ocr_partial_fixable'
    else:
        result['triage'] = 'solver_fixable'
    return result


def main():
    # Triage against the end-to-end benchmark (which applies doc_policy + solver
    # + precomputed selector) so we see failures on real articles only.
    d = json.load(open('artifacts/runs/end_to_end_1k.json'))
    failures = [r for r in d['rows'] if r.get('status') in ('invalid', 'valid_with_gaps')]
    print(f"Triaging {len(failures)} solver failures...")

    with ProcessPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(triage_one, r): r for r in failures}
        triaged = []
        for f in as_completed(futs):
            triaged.append(f.result())

    from collections import Counter
    buckets = Counter(r.get('triage') for r in triaged)
    print("\n=== Triage buckets ===")
    for k, v in buckets.most_common():
        print(f"  {k:>32}: {v}")

    # Dump solver_fixable for the next iteration
    fixable = [r for r in triaged if r.get('triage') == 'solver_fixable']
    partial = [r for r in triaged if r.get('triage') == 'partial_ocr_partial_fixable']
    print(f"\n=== Solver-fixable (all gaps have textItems): {len(fixable)} docs ===")
    for r in fixable[:20]:
        gaps = r.get('gaps') or []
        print(f"  pages={r.get('pages') or '?':>3} exp={r.get('expected')} gaps={list(gaps)[:4]} {r['pdf'].split('/')[-2]}/{r['pdf'].split('/')[-1][:55]}")

    print(f"\n=== Partial (some gaps fixable, some need OCR): {len(partial)} docs ===")
    for r in partial[:10]:
        print(f"  pages={r.get('pages'):>3} in_text={r.get('gaps_in_text_layer')} missing={r.get('gaps_not_in_text_layer')} {r['pdf'].split('/')[-1][:70]}")

    Path('/tmp/solver_triage.json').write_text(json.dumps(triaged, indent=2, default=str))
    print("\nDetails written to /tmp/solver_triage.json")


if __name__ == "__main__":
    main()
