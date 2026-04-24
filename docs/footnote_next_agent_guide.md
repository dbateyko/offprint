# Handoff: next steps for the footnote extractor

State as of this handoff (see `offprint/pdf_footnotes/README.md` for full detail):

- **Strict-valid on real articles: 81.6 %** (493 / 604 on the 1K benchmark).
- Canonical benchmark: `python scripts/research/end_to_end_1k.py` → `artifacts/runs/end_to_end_1k.json`.
- Solver at `offprint/pdf_footnotes/sequence_solver.py`, integrated as candidate `sequence_solver`.
- 36 tests pass (`pytest tests/ -k "sequence_solver or note_ or liteparse or text_extract or doc_policy or issue_splitter or cosmetic"`).

## How to get past 81.6 %: five parallel workstreams

The remaining work splits cleanly along files that don't touch each other. A coordinator agent should launch subagents per workstream, give each the benchmark numbers + the file paths below, and require each to re-run `end_to_end_1k.py` and report the delta.

### Workstream A — Dataset schema + exporter (`scripts/export_footnote_dataset.py`, new file)

**Why first:** without a schema, there's no definition of "done." Once you have an exporter, every other workstream's output is measurable against corpus-level dataset quality, not just per-doc validity.

**Task for this subagent:**
1. Define a JSON Lines record shape for the dataset. Candidate fields:
   ```
   {
     "source_pdf_sha256": "...",
     "source_pdf_path": "...",
     "journal_domain": "...",
     "doc_policy": {"doc_type", "platform_family", "include"},
     "article": {"title", "authors", "volume", "issue", "year", "doi"},  // from scraping + first-page extraction
     "ordinality": {"status", "expected_range", "gaps", "solver_selected_labels"},
     "notes": [
       {"ordinal", "label", "text", "page_start", "page_end", "confidence", "features"}
     ]
   }
   ```
2. Walk all `.footnotes.json` sidecars under `artifacts/pdfs/` (or `artifacts/samples/sample_1k_pdfs/` for the 1K benchmark).
3. Emit `artifacts/datasets/footnotes_v1.jsonl.gz`. Add `--include-statuses valid,valid_with_gaps`, `--min-notes N`, `--journal-domain` filters.
4. Write a sibling `artifacts/datasets/footnotes_v1_manifest.json` with counts, statuses, journals covered.
5. Unit test against 3 fixture sidecars that mock the schema.

**Do not touch:** `offprint/pdf_footnotes/**` — this is pure consumption of existing output.

**Deliverable:** one exporter script + one manifest + one jsonl.gz. Report: how many docs made it into the dataset, how many were dropped and why.

### Workstream B — Deduplication (`scripts/research/dedupe_articles.py`, new file)

Many journals ship `vermeule-a.pdf`, `vermeule-a-3.pdf`, `vermeule-a-4.pdf` — same article, different version suffixes. Also many `viewcontent.cgi-xxxx.pdf` collisions.

**Task for this subagent:**
1. Scan `corpus/scraped/` for sidecar-matched PDFs.
2. Group by: (a) `pdf_sha256` exact match, (b) normalized title + first-500-char-of-text hash, (c) `(journal_domain, filename_stem_without_version_suffix)` where version suffix = `-\d+$` before `.pdf`.
3. For each group, pick canonical = newest `mtime`, largest `page_count` ties broken by newest.
4. Emit `artifacts/datasets/canonical_pdfs.csv` with columns `canonical_pdf, duplicate_pdfs, reason, group_size`.
5. Integrate into the exporter (Workstream A) via a `--dedupe-manifest` flag.

**Do not touch:** the extraction pipeline — dedup operates on post-extraction outputs only.

**Deliverable:** dedup CSV + integration into exporter. Report: how many duplicate PDFs found, how many unique canonical articles remain.

### Workstream C — Text fidelity + column-aware clustering (`offprint/pdf_footnotes/text_extract.py`)

Today the solver can produce an ordinally-valid output whose note text reads like `"COMMONWEALTH VIRGINIA, BOARD EDUCATION, STANDARDS LEARNING OF OF SOF OF FO"` — two-column layouts are mixed across columns in `_cluster_words_to_lines`.

**Task for this subagent:**
1. Add per-page column detection in `_cluster_words_to_lines` (`offprint/pdf_footnotes/text_extract.py:410`). Heuristic: histogram word x-centers; look for a bimodal split with a gap near `page_width/2`.
2. When two columns detected: cluster each column's words separately, then concatenate (left column top-to-bottom, then right column). Single-column pages: unchanged behavior.
3. Add a `text_fidelity_score` to `ExtractedDocument.metadata`: fraction of notes whose first 10 words have monotonically-increasing `x` within a single y-band (or adjacent bands).
4. Propagate `text_fidelity_score` into the sidecar via `pipeline.py::_build_sidecar`.
5. Re-run `end_to_end_1k.py` and report: ordinal-validity delta (should be ≥0), mean `text_fidelity_score` before/after.

**Guard rails:** write fixture tests for (a) single-column page (must be unchanged), (b) two-column page (must cluster correctly), (c) two-column with footnote spanning both columns (known edge case).

**Do not touch:** `sequence_solver.py` — it consumes the clustered lines downstream.

**Deliverable:** updated `_cluster_words_to_lines` + `text_fidelity_score` in sidecars + tests. Report: before/after fidelity on the 1K.

### Workstream D — Eyecite pass over extracted notes (`offprint/pdf_footnotes/citation_enrichment.py`, new file)

Each `NoteRecord.text` is a blob. Running `eyecite.get_citations(text)` turns it into structured citation records, which is what downstream research tooling actually wants.

**Task for this subagent:**
1. New module `offprint/pdf_footnotes/citation_enrichment.py` with `enrich_note(note: NoteRecord) -> NoteRecord` that:
   - Runs `eyecite.get_citations(note.text)`.
   - Attaches `note.features["citations"] = [{"kind": "FullCaseCitation"/"IdCitation"/..., "text": "...", "span": [start,end], "volume": ..., "reporter": ..., "page": ...}]`.
2. Integration hook in `pipeline.py::_extract_for_pdf` (after NoteRecord construction, before sidecar write).
3. Opt-in via `BatchConfig.enrich_citations: bool = True`.
4. Performance: eyecite is ~5 ms per note; 50 k notes per 1K sample ≈ 250 s single-process. Add batching so this runs in a pool if `workers > 1`.
5. Unit tests with 5 canonical citation strings: `"See Smith v. Jones, 123 U.S. 456 (2020)"`, `"Id. at 460"`, short-form, statute citation, pincite-only.

**Do not touch:** solver / classifier / doc_policy. This is post-processing.

**Deliverable:** enrichment module + integration + tests. Report: citation coverage rate (% of notes with ≥1 eyecite hit) on the 1K.

### Workstream E — Individual hard-doc investigation (fan-out: one agent per doc)

35 docs remain `invalid` after the full pipeline. The triage bucket (`scripts/research/solver_triage.py`) classifies them into `solver_fixable`, `partial_ocr`, `needs_ocr`. The ~20 `solver_fixable` + `partial_ocr_partial_fixable` cases are each a specific diagnosis exercise.

**Task for the coordinator:**
1. Run `python scripts/research/solver_triage.py` → `/tmp/solver_triage.json`.
2. Filter to `triage == "solver_fixable"`: 20-ish docs.
3. Launch one subagent per doc with this prompt template:

> Diagnose the specific failure in `{pdf_path}`. Load it via `offprint.pdf_footnotes.text_extract.extract_liteparse_candidate_documents`, find the `sequence_solver` candidate, compare the solver's `selected_labels` against the expected sequence (ordinality report has `expected_range` and `gaps`). For each gap: find the raw textItems matching that digit on the liteparse output, determine why the solver didn't select them (score too low, spatial violation, filtered by recurring-header detector, etc.). Report in ≤200 words: root cause + a proposed code fix, with file path + line number. Do NOT modify code. The solver is at `offprint/pdf_footnotes/sequence_solver.py`.

4. Aggregate root causes across the 20 reports. Cluster into 3–5 common patterns. Ship fixes one per PR, each with an `end_to_end_1k.py` delta.

**Do not parallelize code changes** — have one agent apply each fix and measure delta before the next fix is attempted, to avoid stacking conflicting scoring tweaks.

**Deliverable:** 20 root-cause reports, clustered into patterns, 3–5 shipped fixes.

## Suggested coordinator dispatch

Fan out Workstreams A, B, C, D in parallel (no file conflicts). Workstream E fans out one agent per doc, then serializes its fixes.

```
[coordinator agent]
├── A: exporter (blocks on nothing, enables B's integration)
├── B: dedup (light dependency on A for integration hook)
├── C: column clustering + fidelity score (fully isolated)
├── D: eyecite enrichment (fully isolated)
└── E: 20-doc diagnosis fan-out (fully isolated; serialize fixes)
```

Each subagent should:
1. Read `offprint/pdf_footnotes/README.md` for architecture context.
2. Read this handoff for scope.
3. Check `artifacts/runs/end_to_end_1k.json` for current state per-doc.
4. Produce a working change and verify with:
   ```
   pytest tests/ -k "sequence_solver or note_ or liteparse or text_extract or doc_policy or issue_splitter or cosmetic"
   python scripts/research/end_to_end_1k.py
   ```
5. Report: files changed, tests added, benchmark delta (strict-valid rate before/after).

## Scale-out question

Before any new workstream starts, someone should run the current pipeline on the **full 122 k-PDF corpus** — the 1K benchmark may not represent the long tail. Estimated runtime: ~20 minutes on 8 workers end-to-end. That produces a realistic failure-class distribution at scale and tells you which workstream delivers the most absolute value.

```
python scripts/extract_footnotes.py --pdf-root corpus/scraped --workers 8
# Then triage with diagnose_footnote_corpus.py --by-doc-policy against the new sidecars.
```

If the 122 k results diverge substantially from the 1K (e.g., scale reveals a failure class we haven't seen), revisit priorities before dispatching Workstreams A–E.

## What NOT to do

- Do **not** tune the existing heuristic candidates (`default`, `marker_density`, etc.). They are fallbacks and should stay as-is; the solver is the primary. Tuning them wastes effort and the solver preference deprioritizes them anyway.
- Do **not** add more candidate rails. Oracle analysis in earlier work showed the current candidate set already covers what a better candidate could add; gains now come from the solver's scoring features and downstream processing.
- Do **not** re-open the segmenter's label-rejection regexes (`_is_likely_false_positive`). The solver is authoritative for solver-selected labels; the segmenter is the fallback only when the solver abstains.
- Do **not** integrate OCR without an explicit scope change from the user. The 15–20 docs with corrupt text layers are a known irreducible floor for liteparse-only.
