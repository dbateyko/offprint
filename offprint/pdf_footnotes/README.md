# Footnote Extraction

Text and footnote extraction pipeline for law review PDFs. Ingests LiteParse textItems, produces sidecar JSON with per-note records, ordinality validation, and QC flags.

## Architecture today

Pipeline (`pipeline.py`):

1. LiteParse extracts raw textItems per page (`text_extract._extract_with_liteparse`).
2. Line clustering groups textItems by y-proximity (`_cluster_words_to_lines`).
3. Classifier splits each page's lines into body vs. notes (`_classify_liteparse_note_candidates`) using rails for dashed-glyph separator, bimodal font split, and position+citation scoring.
4. Five candidate interpretations are generated with different classifier settings (`default`, `marker_density`, `low_variance_density`, `bottom_60`, `bottom_72`), each optionally augmented with `body_marker_promotion`.
5. Each candidate is segmented into notes (`note_segment.segment_document_notes_extended`) and validated for ordinality.
6. A scorer picks the best candidate based on ordinality status, gap count, unique labels, duplicate labels, and zero-note penalty.

Performance on a 1K stratified sample of the corpus (`artifacts/samples/sample_1k.txt`):

- Strict-valid on real articles: **81.6 %** (493/604 after solver + tight doc_policy; see details below).
- ≥ valid_with_gaps: **87.4 %**.
- Wall time: ~72 s on 8 workers end-to-end.

Historical context: the pre-solver heuristic ensemble was at 33.2 % strict-valid on the same 1K, rising to 47.4 % after segmenter/trimmer/bimodal fixes in early April. The sequence-solver PoC then lifted the standalone classification layer to 69.4 %; full integration (solver preference + precomputed NoteRecords bypassing segmenter re-validation) lifted it further to 73.1 %; and doc_policy tightening (removing 140 mis-classified non-articles from the denominator) brought the article-validity rate to 81.6 %.

## Sequence-solver proof-of-concept (April 2026)

Built as a prototype in `scripts/research/solver_poc.py`. Replaces steps 3–6 with a global constraint-solver approach:

1. **Collect label candidates** — any textItem whose text looks like a numeric label (digit +/- punctuation, small font, near left margin, not a recurring header/footer text).
2. **Solve for the best sequence** — DP over candidates for the longest monotonically-increasing sequence with small deltas and forward-progressing spatial positions, scored by per-candidate confidence.
3. **Gap-fill pass** — for each missing label in the selected sequence, admit any candidate at that digit value between the neighbors.
4. **Trim tail outliers** — drop stray high-valued labels (citation years, volume numbers) using median-delta-based thresholds.
5. **Guard against degenerate validity** — require ≥3 labels starting at 1 before declaring a sequence valid.

### Results on the same 1K

| Metric | Heuristic ensemble (original) | Standalone solver | **End-to-end (solver + tight doc_policy)** |
|---|---:|---:|---:|
| Strict valid | 247 / 744 (33.2 %) | 516 / 744 (69.4 %) | **493 / 604 (81.6 %)** |
| ≥ valid_with_gaps | 433 (58.2 %) | 568 (76.3 %) | **528 (87.4 %)** |
| Invalid | 185 | ~40 | 35 |
| Empty | 126 | ~140 | 41 |
| Excluded (non-article) | 256 | 256 | **396** (+140 correctly re-classified) |
| Wall time | ~244 s | ~76 s | ~72 s (8 workers) |
| LOC (solver / classifier only) | ~1500 | ~280 | ~300 |

**Denominator math:** the end-to-end run reclassifies 140 docs that the original `doc_policy` accepted as articles but are actually issue compilations (`page_count > 200`), reports (`bulletin`, `blueprint`, `viewbook`, `ebriefing`, `eprs-stu`, `icct-report`), forms (`staff-application`, `one-pager`, `land-acknowledgment`), or case briefs. Honest article-validity rate on the remaining 604 real articles: **81.6 % strict-valid**, **87.4 % ≥ valid_with_gaps**.

**+36.2 pp strict-valid** from one-fifth the code. Most wins came in three iterations:

1. Initial PoC: 65.9 % strict-valid. A single bug fix made most of the jump — the repeating-header detector was blocking entire y-bands rather than specific `(y_band, text)` pairs.
2. Candidate-scoring refinements: added `cluster_peers` (dense footnote blocks) and `substantive_text` (English prose after label) as positive signals. Penalized isolated pure-digit candidates (body superscripts).
3. Gap-fill relaxation: +3.1 pp. For each missing label in the selected sequence, search all candidates with that digit value within ±2 pages of the bracket and accept the highest-scoring one above threshold 1.0. Closed the "good candidate exists but DP path missed it" cases.

### Key transitions (baseline → solver)

- `invalid → valid`: ~115  (rescued from the 185-doc invalid bucket)
- `valid_with_gaps → valid`: ~165  (closed the near-miss bucket)
- `valid → valid`: ~215  (preserved — 87% of baseline-valid)
- `valid → invalid`: 5  (all non-articles that baseline called trivially valid; solver correctly rejects)
- `valid → empty`: ~17  (degenerate sequences the baseline's scorer accepted)
- `empty → valid`: ~12  (real short articles baseline missed)

Net real regressions: 2–5 docs. Net real wins: ~290 docs.

### Remaining residuals (of 744 included docs)

Triage of the ~225 non-strict-valid docs:
- **25 solver-fixable** (all missing labels present in text layer — tractable with more scoring features or per-journal templates)
- **20 partial** (some missing labels need OCR, some fixable)
- **15 need OCR** (all gap labels missing from liteparse text layer — corrupt glyph encoding)
- **14 long docs / likely issue-compilations** (doc_policy should exclude; out of solver scope)
- **7 non-articles by filename pattern** (coversheets, reports, bulletins)
- **~140 empty** — mostly genuine frontmatter/non-articles the solver correctly rejects (min 3 labels starting at 1)

Honest ceiling for liteparse-only on this corpus is ~82–85 % strict-valid on real articles once doc_policy cleanup and 25 remaining solver-fixable cases are closed. Higher requires OCR.

### Why this works

Footnote labels form a **near-monotonic 1..N sequence** — a global constraint the heuristic stack only checks at validation time. The solver makes ordinality a design guarantee: the output is valid-by-construction because the optimizer selects only those candidates that fit the sequence.

The bugs that motivated the rebuild (Charleston's rejected pincite, Seattle U's stray-label inflation, UBC's corrupt-glyph label) all become automatic in the solver: candidates that don't fit the sequence are rejected in context, not by local regex.

### What does NOT become easier

- Docs with **no usable text layer** (scanned-image PDFs, corrupted glyph encodings). These need OCR regardless of architecture. Honest ceiling for liteparse-only on this corpus: ~85–92 %.
- Docs with **non-standard footnote conventions** (endnote-only articles, docs with labels that restart per-section). These need per-journal rules.

### Status

- PoC validated on the full 1K.
- **Integrated** as `sequence_solver` candidate in `offprint/pdf_footnotes/sequence_solver.py` + `text_extract.py::extract_liteparse_candidate_documents`.
- Selector preference (`_liteparse_candidate_selection_key`): solver wins whenever it produces a valid or valid_with_gaps result; heuristic ensemble is the fallback.
- Solver output is precomputed (`build_note_records`) and carried on `ExtractedDocument.metadata["sequence_solver_precomputed"]`. The selector hydrates `NoteRecord`s + `OrdinalityReport` directly from the precomputed payload, bypassing `segment_document_notes_extended`'s label-rejection rules (which disagreed with the solver ~30 % of the time on the 1K).
- `doc_policy` tightened: expanded `_NON_ARTICLE_FILENAME_RE`, added long-doc rules (>200 pp → issue_compilation; 120+ pp without notes or metadata → issue_compilation).
- Full test suite: 36 tests passing, including 3 new `tests/test_sequence_solver.py` tests covering the precomputed path.

### Run the full-corpus audit

See **[`docs/full_corpus_audit_roadmap.md`](../../docs/full_corpus_audit_roadmap.md)** for the canonical plan, pre-flight checklist, and definition of "done." Short version:

```bash
# Inside a screen session:
screen -S corpus_audit
./scripts/processing/run_corpus_audit.sh
# On resume after a crash:
OVERWRITE=false ./scripts/processing/run_corpus_audit.sh
```

Produces three manifests in `artifacts/runs/corpus_audit_<version>/`:
- `all_pdfs_manifest.jsonl` — doc_policy classification for every PDF
- `liteparse_results.jsonl` — extraction outcomes for real articles
- `ocr_backlog.jsonl` — articles flagged for OCR review

### Run the benchmarks

```bash
# 40-doc smoke test (solver standalone, no full pipeline)
python scripts/research/solver_poc.py

# Full 1K — solver standalone (liteparse-only, no doc_policy)
python scripts/research/solver_1k.py
# → artifacts/runs/solver_1k.json

# Full 1K — integrated selector with solver preference
python scripts/research/solver_integration_1k.py
# → artifacts/runs/solver_integration_1k.json

# Full 1K — end-to-end (doc_policy + integrated selector) — canonical benchmark
python scripts/research/end_to_end_1k.py
# → artifacts/runs/end_to_end_1k.json
```

Triage remaining failures:

```bash
python scripts/research/solver_triage.py
# Bucket failures into {solver_fixable, partial_ocr, needs_ocr, long_doc, non_article}
```

## Files

- `pipeline.py` — orchestration, candidate scoring, sidecar assembly, precomputed-aware selector
- `text_extract.py` — LiteParse wrapper, line clustering, classifier rails, `sequence_solver` candidate branch
- `sequence_solver.py` — global constraint-solver for label selection + `build_note_records`
- `note_segment.py` — label parsing, validation, ordinality (used as fallback when solver abstains)
- `doc_policy.py` — article / frontmatter / issue_compilation classification (tightened 2026-04)
- `qc_filter.py` — post-extraction QC
- `ocr_worker.py` — OlmOCR fallback (not used in liteparse-only mode)
- `schema.py` — sidecar record types
- `../../scripts/research/solver_poc.py` — sequence-solver standalone PoC
- `../../scripts/research/solver_1k.py` — standalone solver benchmark
- `../../scripts/research/solver_integration_1k.py` — integrated selector benchmark
- `../../scripts/research/solver_triage.py` — failure-class triage
- `../../scripts/research/end_to_end_1k.py` — canonical end-to-end benchmark
