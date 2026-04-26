# Footnote Full-Corpus Audit Run

This document defines how we run the full PDF corpus so the results are useful for both dataset accounting and future OCR routing.

The goal is not just to extract footnotes. The goal is to produce three separated ledgers:

1. Every PDF we saw.
2. Every PDF excluded from the law-review article denominator.
3. Every included article that liteparse could not extract to strict ordinal validity and should be revisited by solver work or OCR.

Keeping those ledgers separate prevents three common mistakes:

- Counting mastheads, tables of contents, programs, coversheets, and issue frontmatter as extraction failures.
- Counting image-only PDFs as liteparse regressions.
- Sending already strict-valid native-text PDFs through expensive OCR.

## Definitions

### Corpus

The corpus is every `*.pdf` under the selected `--pdf-root`, normally `corpus/scraped` or the canonical artifact PDF root used for production.

### Article Denominator

The extraction denominator is:

```text
doc_policy.include == true
AND doc_policy.doc_type == article, unless the run explicitly includes issue compilations
```

Excluded PDFs are not extraction failures. They are denominator-cleanup candidates and should be categorized separately.

### Strict Valid

A document is strict-valid when the numbered footnote sequence has no gaps under the current ordinality validator:

```text
ordinality.status == "valid"
AND ordinality.gaps == []
```

### OCR Backlog

The OCR backlog is every included article where liteparse-first extraction does not produce strict-valid output, plus included articles with native text-layer failure signals.

Examples:

- `empty` with `has_text_layer=false`: OCR likely required.
- `invalid` with `has_text_layer=true`: inspect liteparse/solver first, then OCR if labels or note text are absent from textItems.
- `valid_with_gaps`: usually a solver or segmentation target before full-document OCR.
- `valid` with bad text-fidelity flags: not an ordinality failure, but may need targeted layout work or selective OCR.

## Required Artifacts

A full run must produce these artifacts under a timestamped run directory:

```text
artifacts/runs/footnote_full_corpus_<YYYYMMDD_HHMMSS>/
  run_config.json
  extraction_report.json
  all_pdfs_manifest.jsonl
  doc_policy_exclusions.jsonl
  liteparse_results.jsonl
  ocr_backlog.jsonl
  summary.md
```

### `run_config.json`

Records the exact command and environment assumptions.

Required fields:

```json
{
  "run_id": "footnote_full_corpus_YYYYMMDD_HHMMSS",
  "pdf_root": "corpus/scraped",
  "workers": 8,
  "classifier_workers": 8,
  "ocr_mode": "off",
  "text_parser_mode": "footnote_optimized",
  "doc_policy": "article_only",
  "git_commit": "...",
  "started_at_utc": "..."
}
```

### `all_pdfs_manifest.jsonl`

One row per PDF, regardless of inclusion or extraction outcome.

Required fields:

```json
{
  "pdf": "corpus/scraped/example.edu/article.pdf",
  "sha256": "...",
  "domain": "example.edu",
  "page_count": 42,
  "has_text_layer": true,
  "doc_policy": {
    "include": true,
    "doc_type": "article",
    "platform_family": "...",
    "reason": "..."
  },
  "sidecar_path": "corpus/scraped/example.edu/article.pdf.footnotes.json",
  "extraction_attempted": true
}
```

Use this file for denominator accounting, coverage checks, and rerun planning.

### `doc_policy_exclusions.jsonl`

One row per PDF excluded from the article denominator.

Required fields:

```json
{
  "pdf": "...",
  "sha256": "...",
  "domain": "...",
  "page_count": 2,
  "doc_type": "frontmatter",
  "include": false,
  "reason": "masthead|toc|cover|program|staff|bibliography|issue_compilation|other",
  "confidence": 0.0,
  "review_bucket": "denominator_cleanup"
}
```

This is the queue for improving and auditing document classification. It is not the OCR queue.

### `liteparse_results.jsonl`

One row per included article where extraction was attempted.

Required fields:

```json
{
  "pdf": "...",
  "sha256": "...",
  "domain": "...",
  "page_count": 42,
  "has_text_layer": true,
  "status": "valid|valid_with_gaps|invalid|empty",
  "note_count": 210,
  "ordinality": {
    "status": "valid",
    "expected_range": [1, 210],
    "gaps": []
  },
  "selected_candidate": "sequence_solver",
  "solver": {
    "selected_labels": [1, 2, 3],
    "candidate_count": 512
  },
  "warnings": [],
  "text_fidelity_score": 0.98,
  "sidecar_path": "..."
}
```

This is the canonical ledger for native extraction quality.

### `ocr_backlog.jsonl`

One row per included article that should be revisited after liteparse-first extraction.

Required fields:

```json
{
  "pdf": "...",
  "sha256": "...",
  "domain": "...",
  "status": "invalid",
  "note_count": 188,
  "expected_range": [1, 210],
  "missing_labels": [14, 15, 88],
  "has_text_layer": true,
  "failure_reasons": ["ordinality_invalid", "missing_labels"],
  "recommended_next_step": "solver_triage|targeted_page_ocr|full_ocr|doc_policy_review",
  "priority": "high|medium|low",
  "sidecar_path": "..."
}
```

This file is the future OCR work queue. OCR should be reserved for rows where native text is absent, corrupted, or where solver triage shows the required glyphs do not exist in liteparse textItems.

## Recommended Run Command

Use OCR off for the audit pass. The point is to measure liteparse's ceiling before falling back.

```bash
RUN_ID="footnote_full_corpus_$(date -u +%Y%m%d_%H%M%S)"
RUN_DIR="artifacts/runs/${RUN_ID}"
mkdir -p "$RUN_DIR"

python scripts/processing/extract_footnotes.py \
  --pdf-root corpus/scraped \
  --workers 8 \
  --classifier-workers 8 \
  --ocr-mode off \
  --text-parser-mode footnote_optimized \
  --doc-policy article_only \
  --include-pdf-sha256 true \
  --report-detail full \
  --heartbeat-every 500 \
  --overwrite false \
  --emit-doctype-manifest true \
  --doctype-manifest-out "$RUN_DIR/doc_policy_exclusions.jsonl" \
  --emit-ocr-review-manifest true \
  --ocr-review-manifest-out "$RUN_DIR/ocr_backlog.raw.jsonl" \
  --report-out "$RUN_DIR/extraction_report.json"
```

Then normalize the raw run outputs into the required ledgers:

```bash
python scripts/quality/build_footnote_audit_ledgers.py \
  --pdf-root corpus/scraped \
  --extraction-report "$RUN_DIR/extraction_report.json" \
  --doc-policy-exclusions "$RUN_DIR/doc_policy_exclusions.jsonl" \
  --ocr-review-raw "$RUN_DIR/ocr_backlog.raw.jsonl" \
  --out-dir "$RUN_DIR"
```

If `build_footnote_audit_ledgers.py` does not exist yet, create it before the first production run. Do not rely only on the extraction report, because the extraction report is a job summary, not a durable per-PDF audit ledger.

## Sharded Runs

For very large runs, use deterministic shards and merge ledgers afterward.

```bash
python scripts/processing/extract_footnotes.py \
  --pdf-root corpus/scraped \
  --workers 8 \
  --classifier-workers 8 \
  --ocr-mode off \
  --text-parser-mode footnote_optimized \
  --doc-policy article_only \
  --shard-count 8 \
  --shard-index 0 \
  --include-pdf-sha256 true \
  --report-detail full \
  --report-out "$RUN_DIR/shard_0.report.json" \
  --doctype-manifest-out "$RUN_DIR/shard_0.doc_policy_exclusions.jsonl" \
  --ocr-review-manifest-out "$RUN_DIR/shard_0.ocr_backlog.raw.jsonl"
```

Run shard indexes `0..7`, then merge by `sha256` and `pdf`.

## Post-Run Summary

Every full run must produce `summary.md` with these counts:

```text
Total PDFs scanned: N
Included articles: N
Excluded by doc_policy: N
  frontmatter: N
  issue_compilation: N
  other: N
  unknown/manual_review: N

Liteparse included-article outcomes:
  strict_valid: N / included_articles_with_text_layer (%)
  valid_with_gaps: N
  invalid: N
  empty: N

OCR backlog:
  full_ocr: N
  targeted_page_ocr: N
  solver_triage: N
  doc_policy_review: N

Text layer:
  has_text_layer: N
  no_text_layer_or_too_sparse: N
```

The headline quality metric is:

```text
strict_valid_rate = strict_valid / included_articles_with_text_layer
```

Also report the operational metric:

```text
ocr_backlog_rate = ocr_backlog / included_articles
```

## Triage Rules

Use these rules when building `ocr_backlog.jsonl`.

| Condition | Recommended next step | Reason |
| --- | --- | --- |
| `doc_policy.include=false` | `doc_policy_review` only if exclusion seems suspect | Not an extraction failure. |
| `has_text_layer=false` and article included | `full_ocr` | Liteparse cannot extract absent glyphs. |
| `status=empty` and `has_text_layer=true` | `solver_triage` first | Could be doc_policy leak, endnotes, or parser failure. |
| `status=invalid` with many gaps | `solver_triage` or `targeted_page_ocr` | Check whether missing labels exist in textItems. |
| `status=valid_with_gaps` | `solver_triage` | Often fixable without OCR. |
| `status=valid` but low `text_fidelity_score` | layout/fidelity work | Ordinality passes but text may be scrambled. |

## What We Learn From This Run

### Question 1: Which PDFs are not law-review articles?

Use `doc_policy_exclusions.jsonl` plus suspicious `empty` rows from included articles. This creates the denominator-cleanup queue.

Expected categories:

- masthead
- table of contents
- cover or coversheet
- staff page
- symposium program
- bibliography
- picture/image-only non-article
- roundtable or issue compilation
- non-law-review content
- corrupted or inaccessible PDF

### Question 2: Which articles need OCR later?

Use `ocr_backlog.jsonl`, not `doc_policy_exclusions.jsonl`.

OCR candidates should have one of these reasons:

- `no_text_layer`
- `sparse_text_layer`
- `missing_label_glyphs`
- `corrupt_text_encoding`
- `native_extraction_empty`
- `targeted_gap_pages`

Do not OCR strict-valid documents by default.

## Non-Goals

- Do not enable OCR during this audit run. That hides liteparse failures and makes the denominator harder to reason about.
- Do not tune solver thresholds from the full run until the ledgers exist. First classify failures, then patch common causes.
- Do not collapse doc-policy exclusions and extraction failures into one failure count.
- Do not report `valid / total_pdfs` as the quality metric. The denominator must be included articles with a usable native text layer.

## Immediate Implementation Gap

The extraction script already supports:

- `--doctype-manifest-out`
- `--ocr-review-manifest-out`
- `--report-out`
- `--include-pdf-sha256`
- `--shard-count` / `--shard-index`

The missing piece is a small ledger builder:

```text
scripts/quality/build_footnote_audit_ledgers.py
```

That script should read sidecars, raw manifests, and the extraction report, then emit the four normalized files specified above. Build this before the first 122k production audit so every subsequent run is comparable.

## Hill-Climb Status (2026-04-26)

### Holdout-1K headline (`artifacts/samples/sample_1k_holdout.txt`)

| Metric | Pre-2026-04-26 | After today's fixes | Δ |
|---|---|---|---|
| Articles (post-doc_policy) | 605 | 593 | −12 |
| Article strict-valid | 91.7% | **93.4%** | **+1.7 pp** |
| Article ≥vwg | 95.5% | 96.6% | +1.1 pp |
| Article honest strict (excl. empty) | 94.4% | 95.4% | +1.0 pp |
| Article honest ≥vwg | 98.3% | 98.6% | +0.3 pp |

Reproduce: `.venv/bin/python scripts/research/bench_holdout_1k.py --manifest artifacts/samples/sample_1k_holdout.txt --workers 4 --out artifacts/runs/holdout_1k_after_doc_policy_only.json`

### Landed (in commit order)

1. **`29ee499`** — `_cache_compatible_with_mode` now requires `parser=liteparse` for `balanced`/`footnote_optimized`. Older runs that fell back to pypdf populated text caches with `parser=pypdf`; subsequent footnote_optimized runs reused those caches and silently skipped liteparse. The first 122k corpus pass on 2026-04-26 was reading 27,427 stale pypdf caches (92% of all caches), producing zero notes with no warnings on PDFs liteparse handles cleanly.
2. **`257ea93`** — `extract_document_text(footnote_optimized)` no longer falls back to pdfplumber/pypdf. PDFs liteparse cannot read return `pages=[]` with warning `liteparse_returned_none`, signalling downstream OCR routing rather than producing low-quality fallback output. Other parser modes (`pdfplumber_only`, `pypdf_only`, etc.) still call their respective parsers — pruning queued for a follow-up.
3. **`88ae539`** — `_NON_ARTICLE_FILENAME_RE` in `doc_policy.py` extended with patterns for moot court issues, financial statements, treaty instruments, books-received lists, legal-help pages, ABA-509 disclosures, agency reports, open letters, and court briefs. The 1K holdout previously misclassified ~12 such docs as articles. **This is the source of the +1.7 pp headline lift.**

### Reverted (today, after holdout regression)

1. **`238fb87`** reverts the liteparse zero-line + cmap-failure detector. Threshold (>5% non-ASCII glyphs at small font size) caught Spanish-language journals (`derecho.uprrp.edu`, `tabla-de-citaci-n-*.pdf`) and journals with diacritics, regressing 14 article-valid → article-empty.
2. **`7f55217`** reverts the body-text digit penalty in `sequence_solver._candidate_score`. The −3.0 penalty for body-size, non-margin, non-punctuated digit candidates dropped legitimate footnote labels in some short-document layouts, regressing 7 article-valid → article-vwg/invalid.

Both fixes targeted real pathologies (huberfeld 0-line, MJLR cmap mojibake, BC body-digit mis-routing) but were too aggressive at the chosen thresholds. Proposals at `/tmp/huberfeld_proposal.md`, `/tmp/longgap_proposal.md` retain the diagnoses for a more careful retry.

### Operational state

- Corpus run: PID in screen `corpus_extract`, launched 2026-04-26 13:40, manifest size 74,764 PDFs, log `/tmp/full_corpus_run_20260426_1340.log`, report destination `artifacts/runs/full_corpus_20260426_1340.json`.
- Pre-existing kept sidecars: 41,026 with `status=valid` (verified by sampling — 27/30 identical to fresh extraction, 3/30 recover slightly more notes; no regressions). These are skipped via `overwrite=False`.
- Previous 122k run (2026-04-26 00:42) scrapped: cache-poisoned by stale parser=pypdf caches; 92% of caches were stale.

### Deferred levers (not blocking current run)

| Lever | Status | File / proposal | Est. lift |
|---|---|---|---|
| Long-gap solver scoring (BC body-digit) | retry needed with narrower trigger | `/tmp/longgap_proposal.md` | ~0.5–0.8 pp |
| Liteparse 0-line / cmap detector → OCR | retry needed with stricter thresholds (Latin-1 supplement only, exclude Spanish/diacritic-heavy domains) | `/tmp/huberfeld_proposal.md`, `/tmp/longgap_proposal.md` | ~0.3–0.5 pp |
| vwg solver collector relaxation | needs instrumented experiment first | `/tmp/vwg_proposal.md` | ~0.5–1.0 pp |
| OCR routing for `liteparse_returned_none` | infra is ready (`--ocr-mode fallback --ocr-backend olmocr`); just a flag change at next run | `/tmp/ocr_routing_proposal.md` | ~0.5–0.6 pp on Drake-style scans |
| Phase 2 cleanup: remove pdfplumber/pypdf/docling parser code paths and tests | deferred until corpus run completes | `text_extract.py`, `pipeline.py` | code clarity only |

### Pre-flight before next corpus run

If the in-flight run finishes clean, the headline number to publish is the article-scoped strict-valid % from `artifacts/runs/full_corpus_20260426_1340.json` aggregated by `doc_type=='article'`. Compare against the 93.4% holdout baseline. A material gap (>3 pp) likely indicates either (a) a journal-specific layout that holdout under-samples, or (b) an OCR-only bucket (Drake-class) larger than expected. If the gap is OCR-driven, re-run with `--ocr-mode fallback --ocr-backend olmocr --ocr-workers 4` after the vLLM screen is confirmed up.
