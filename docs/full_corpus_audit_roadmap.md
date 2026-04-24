# Full-Corpus Audit Run Roadmap

Status: planned, not yet executed.

This document specifies the **liteparse-first full-corpus extraction pass** over all 122k+ PDFs in `corpus/scraped/`. It produces three manifests that let us separate denominator cleanup (what shouldn't count as a law review article) from extraction-failure triage (what needs OCR later).

## Goals

1. **Classify every PDF.** Every PDF gets a `doc_policy` record (`article`, `frontmatter`, `issue_compilation`, `other`), whether or not we try to extract footnotes from it. This is raw material for future denominator cleanup — deciding which PDFs belong in the law-review-article dataset.

2. **Extract footnotes from every real article.** For `include=True` docs, run the liteparse pipeline with the `sequence_solver` candidate + preferred ensemble selector. No OCR — we're measuring liteparse's ceiling.

3. **Produce a triage-able OCR backlog.** Every article that doesn't reach strict-valid gets flagged with enough detail to decide whether it needs OCR, a solver bug fix, or a doc_policy tweak.

These are **three separate artifacts**. Keep the categories clean: a masthead should not count as an extraction failure; an image-only article should not count as a solver regression.

## The three output manifests

### A. `all_pdfs_manifest.jsonl` — denominator-cleanup raw material

One row per PDF in the corpus. Every doc, regardless of type.

```json
{
  "pdf": "corpus/scraped/<domain>/<file>.pdf",
  "pdf_sha256": "...",
  "page_count": 3,
  "has_text_layer": true,
  "doc_policy": {
    "doc_type": "frontmatter",
    "include": false,
    "reason_codes": ["short_doc_without_footnotes"],
    "platform_family": "digital_commons",
    "domain": "digitalcommons.law.umn.edu",
    "confidence": 0.9
  },
  "title_guess": "first ~100 chars of page 1 text or null",
  "extractor_version": "0.3.0"
}
```

**Purpose.** Later we'll sample `include=false` docs by `reason_code` to verify doc_policy isn't over-excluding real articles. We'll sample `include=true + page_count≤3` docs to catch under-excluding.

### B. `liteparse_results.jsonl` — extraction outcomes for real articles

One row per PDF where `doc_policy.include=True`. Subset of A's included bucket.

```json
{
  "pdf": "...",
  "pdf_sha256": "...",
  "status": "valid|valid_with_gaps|invalid|empty",
  "selected_candidate": "sequence_solver|default|bottom_72+body_marker_promotion|...",
  "notes_found": 188,
  "expected_range": [1, 210],
  "gap_count": 22,
  "gaps": [14, 15, 88, 102, ...],
  "solver_selected_labels_count": 188,
  "text_fidelity_score": 0.92,
  "warnings": ["low_font_variance_detected", "liteparse_candidate_selected=sequence_solver"],
  "elapsed_sec": 0.74,
  "extractor_version": "0.3.0"
}
```

**Purpose.** Measure `article_validity_rate = valid / include_count` at corpus scale. Identify the journals/platforms where liteparse is failing most. Decide whether the 1K benchmark's 82.9 % holds up on 122K.

### C. `ocr_backlog.jsonl` — candidates for OCR fallback

One row per PDF where B's `status != "valid"`. Subset of B.

```json
{
  "pdf": "...",
  "pdf_sha256": "...",
  "status": "invalid",
  "notes_found": 188,
  "expected_max": 210,
  "missing_labels": [14, 15, 88, 102, ...],
  "has_text_layer": true,
  "selected_candidate": "sequence_solver",
  "failure_reasons": ["missing_labels", "low_text_layer_signal", "solver_gap"],
  "ocr_recommendation": "full|pages:[p12,p13,p41]|do_not_ocr",
  "extractor_version": "0.3.0"
}
```

**Priority tiers** (set by the aggregator, not the extractor):
- `empty + has_text_layer=false` → OCR almost certainly needed.
- `invalid + has_text_layer=true` → inspect liteparse failure first; OCR if text layer corrupt or labels missing.
- `valid_with_gaps` → likely fixable by solver improvements OR targeted OCR of a few pages.
- `strict_valid` → no OCR unless text-fidelity-score is low (handled in a separate pass).

## The run

### Pre-flight

1. **Dry-run**: `python scripts/research/end_to_end_1k.py` — confirm current state is 82.9 % strict-valid / 88.4 % ≥vog. Any regression means don't kick off.
2. **Disk check**: `df -h /mnt/shared_storage` — need ~5 GB free (sidecars + logs + manifests).
3. **Memory check**: `free -h` — need 16 GB headroom (8 workers × liteparse `node` subprocess).
4. **Bump version**: `EXTRACTOR_VERSION` in `offprint/pdf_footnotes/pipeline.py` to `"0.3.0"`.
5. **Confirm OCR is OFF**: `--ocr-mode off`. Do NOT start vLLM servers.
6. **Screen session**: `screen -S corpus_audit` so it survives terminal disconnect.

### Command shape

```bash
# Inside screen, inside offprint/
cd /mnt/shared_storage/law-review-corpus/offprint
.venv/bin/python scripts/processing/extract_footnotes.py \
  --pdf-root /mnt/shared_storage/law-review-corpus/corpus/scraped \
  --workers 8 \
  --classifier-workers 4 \
  --doc-policy all \
  --text-parser-mode footnote_optimized \
  --ocr-mode off \
  --overwrite true \
  --heartbeat-every 100 \
  --report-out artifacts/runs/corpus_audit_0.3.0/report.json \
  2>&1 | tee artifacts/logs/corpus_audit_$(date +%Y%m%dT%H%M%SZ).log
```

Key flags:
- `--doc-policy all` — classify every PDF, emit sidecars for all of them.
- `--ocr-mode off` — liteparse-only pass.
- `--overwrite true` — ignore any pre-existing sidecars (assumed stale from prior experiments).
- `--workers 8` — matches the 1K benchmark throughput.

Expected wall time: **~12–16 h** at 8 workers.

### Resume (if the run crashes)

Same command but with `--overwrite false`. The pipeline skips PDFs that already have sidecars of the current `extractor_version`. Don't change `EXTRACTOR_VERSION` mid-run.

### Post-run aggregation

After extraction completes, run the aggregator (to be written):

```bash
.venv/bin/python scripts/research/aggregate_corpus_audit.py \
  --pdf-root /mnt/shared_storage/law-review-corpus/corpus/scraped \
  --out-dir artifacts/runs/corpus_audit_0.3.0/ \
  --extractor-version 0.3.0
```

This walks every `.footnotes.json` sidecar, joins it with the PDF's doc_policy classification, and writes `all_pdfs_manifest.jsonl`, `liteparse_results.jsonl`, `ocr_backlog.jsonl` into the output dir.

Aggregation is safe to re-run; it doesn't re-extract, just re-reads sidecars.

## After the run: how we review

### Denominator cleanup (Workstream 1)

1. Sample 50 `include=false` docs stratified by `reason_code`. Human review: are these actually non-articles?
2. Sample 50 `include=true + page_count < 5` docs. Are short articles really articles, or missed frontmatter?
3. Sample 30 `doc_type=issue_compilation` docs. Are these really compilations? Any single articles mis-classified?
4. Update `doc_policy.py` regexes; rerun aggregator (no re-extraction needed) to see the denominator shift.

### Solver improvement (Workstream 2)

1. From `liteparse_results.jsonl`, filter `status in {invalid, valid_with_gaps}`.
2. Join with `ocr_backlog.jsonl` failure_reasons.
3. For `failure_reason = solver_gap` (no missing text-layer glyphs), triage into root-cause patterns using `solver_triage.py`.
4. Ship solver fixes that target specific pattern buckets. Each fix: re-run just those PDFs, measure delta.

### OCR backlog (Workstream 3)

1. From `ocr_backlog.jsonl`, filter `ocr_recommendation=full` and sort by `missing_label_count desc`.
2. Start vLLM servers (see `scripts/processing/run_overnight_footnotes.sh`).
3. Run `extract_footnotes.py` with `--ocr-mode fallback` over just the backlog PDFs (use `--pdf-list`).
4. Compare post-OCR status vs pre-OCR; add resolved PDFs to the dataset.

## Explicit do-nots

- **Do not run OCR in this pass.** It's a separate, GPU-gated step with different SLAs. Mixing them makes the results unattributable.
- **Do not overwrite with a mid-run version bump.** If you discover a bug, finish the run, fix the bug, bump to `0.3.1`, rerun.
- **Do not aggregate into the three manifests during extraction.** Post-hoc aggregation lets us iterate on the manifest schema without re-extracting.
- **Do not route `doc_policy.include=false` docs to OCR.** They're not articles.
- **Do not cross-contaminate categories.** A `valid` article with low text-fidelity is NOT an OCR candidate under the current policy — that's a separate "fidelity improvement" workstream.

## Definition of "done" for this run

The run is complete when:
1. Every PDF in `corpus/scraped/` has either a sidecar or a logged extraction error.
2. The three manifests exist in `artifacts/runs/corpus_audit_0.3.0/`.
3. `article_validity_rate` on the full corpus is reported and within ±5 pp of the 1K benchmark (82.9 %). Larger divergence means the 1K wasn't representative; investigate before moving to Workstream 2/3.
4. Stratified review queues (Workstreams 1–3 above) are published.
