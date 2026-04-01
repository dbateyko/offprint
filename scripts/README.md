# Scripts

Operational and maintenance scripts for production scraping workflows.

## Directory Layout

- `scripts/pipeline/`: crawl orchestration, smoke validation, run promotion.
- `scripts/onboarding/`: fingerprinting and new-site onboarding helpers.
- `scripts/processing/`: QC quarantine, metadata/text/footnote extraction, HF dataset build.
- `scripts/quality/`: repo and adapter policy checks plus extraction evaluation.
- `scripts/reporting/`: run and metadata coverage reporting.

Backward-compatible wrappers remain at `scripts/<name>.py` so existing commands keep working.
Shared CLI defaults/boolean parsing are centralized in `offprint/cli.py`.

## Workflow Index

| Workflow | Use When | Primary Script(s) | Example |
|---|---|---|---|
| Onboarding smoke (forced signal) | You need one-site onboarding smoke even when sitemap/CSV status is `todo_adapter` | `onboard_smoke_target.py` | `python scripts/onboard_smoke_target.py https://example.edu/journal/issues/ --out-dir /tmp/onboard_pdfs_example --report-dir /tmp/onboard_report_example` |
| Repository layout enforcement | You want to prevent root clutter and docs/data drift | `quality/check_repo_layout.py` | `python scripts/quality/check_repo_layout.py --repo-root .` |
| Production run | You want a canonical full/delta/retry run with resumable artifacts | `pipeline/run_pipeline.py` | `python scripts/pipeline/run_pipeline.py --mode full --sitemaps-dir offprint/sitemaps --out-dir artifacts/pdfs --manifest-dir artifacts/runs --export-dir artifacts/exports` |
| Quick validation | You need one PDF per site to validate routing/discovery quickly | `pipeline/smoke_one_pdf_per_site.py` | `python scripts/pipeline/smoke_one_pdf_per_site.py --sitemaps-dir offprint/sitemaps --out-dir artifacts/smoke/pdfs --report-dir artifacts/runs` |
| High-yield smoke batching | You want short-run, KPI-driven triage lanes from recent smoke history | `build_high_yield_lanes.py`, `run_high_yield_cycle.py` | `python scripts/run_high_yield_cycle.py --help` |
| Preflight checks | You want readiness checks before production | `run_preflight.py` | `python scripts/run_preflight.py --sitemaps-dir sitemaps` |
| Resume/Promote/Status | You need baseline management and run health visibility | `pipeline/promote_run.py`, `reporting/site_status_report.py`, `export_run_health.py` | `python scripts/reporting/site_status_report.py --summary` |
| New-site onboarding | You need platform fingerprint + seed/registry bootstrap | `onboarding/fingerprint_site.py`, `onboarding/auto_onboard_site.py` | `python scripts/onboarding/auto_onboard_site.py https://example.edu/law-review/ --dry-run --smoke-test` |
| Adapter autoresearch loop | You want autonomous keep/discard cycles for non-DC adapter backlog targets | `build_adapter_autoresearch_queue.py`, `run_adapter_autoresearch.py` | `python scripts/build_adapter_autoresearch_queue.py && python scripts/run_adapter_autoresearch.py --headless --max-cycles 10` |
| Bulk seed promotion | You want to promote `todo_adapter` seeds to active with auto-registration | `promote_todo_adapter_seeds.py` | `python scripts/promote_todo_adapter_seeds.py --platform wordpress,drupal,ojs,scholastica,unknown --dry-run` |
| Backlog triage | You need evidence-driven classification of `todo_adapter` and failed smoke hosts | `probe_generic_todo_backlog.py`, `probe_issue_archive_lanes.py`, `classify_failed_smoke_links.py`, `bulk_failed_smoke_triage.py` | `python scripts/classify_failed_smoke_links.py --smoke-root artifacts --max-depth 1 --max-workers 12` |
| Seed maintenance | You need sitemap migration, URL audit, or canonical import | `migrate_sitemaps_format.py`, `audit_seed_urls.py`, `import_registry_canonical_to_todo_sitemaps.py` | `python scripts/migrate_sitemaps_format.py --apply` |
| Metadata and post-processing | You need sidecars, metadata enrichment, HF dataset exports, or DOCX matching | `generate_metadata_sidecars.py`, `processing/extract_pdf_metadata.py`, `processing/build_hf_dataset.py`, `match_docx_candidates_to_pdfs.py` | `python scripts/processing/extract_pdf_metadata.py --help` |
| Footnote/QC pipeline | You need article-quality filtering and citation/footnote extraction | `processing/qc_quarantine_pdfs.py`, `processing/extract_footnotes.py`, `processing/run_olmocr_dual_gpu.py`, `quality/evaluate_footnotes.py`, `footnote_sample_audit.py` | `make extract-footnotes-olmocr-dual` |
| Manual test helpers | You want quick operator smoke checks outside CI | `docs/testing/` | `python docs/testing/footnote_extraction_10pdfs_smoke.py` |
| Recovery (non-downloaded PDFs) | You need replay/recovery for failed URLs | `wayback_resolve_non_downloaded.py`, `wayback_retry_non_downloaded.py`, `unified_retry_pipeline.py` | `python scripts/wayback_retry_non_downloaded.py --help` |

## High-Impact Scripts
- `run_pipeline.py`: canonical production runner (`full`, `delta`, `retry`) with resume and export outputs.
- `smoke_one_pdf_per_site.py`: one-PDF-per-site confidence check (browser/WAF fallback lane currently disabled).
- `build_high_yield_lanes.py`: generate fast/adapter/drop/waf-excluded target lanes from historical outcomes.
- `run_high_yield_cycle.py`: execute one short fast-lane cycle and emit continue/stop recommendation.
- `site_status_report.py`: sitemap + adapter + local corpus + run stats rollup.
- `fingerprint_site.py`: platform detection for single URL or sitemap batch.
- `auto_onboard_site.py`: generate sitemap seed and append registry registration.

For a concise operator entrypoint to QC + footnote + text extraction commands, see the root [README](../README.md#document-processing).

## PDF QC Quarantine (`qc_quarantine_pdfs.py`)

Scans `artifacts/pdfs/` for non-article PDFs (mastheads, TOCs, frontmatter, cover pages, submission guidelines) and copies them to `artifacts/quarantine/` with an exclusion manifest.

```bash
# Dry-run: evaluate rules without copying files
python scripts/qc_quarantine_pdfs.py --dry-run true

# Full run
python scripts/qc_quarantine_pdfs.py --pdf-root artifacts/pdfs --quarantine-root artifacts/quarantine

# Limit to first 500 PDFs for spot-checking
python scripts/qc_quarantine_pdfs.py --dry-run true --limit 500
```

The exclusion manifest (`artifacts/runs/pdf_qc_exclusions_<STAMP>.jsonl`) records each excluded PDF's path, SHA-256, reason codes, and signal payload. Downstream tools (`build_hf_dataset.py`, `extract_footnotes.py`) can load this manifest to skip quarantined files.

### QC Ruleset (v2)

Classification lives in `offprint/pdf_footnotes/qc_filter.py`. Each rule fires independently; any rule match with `confidence ≥ 0.98` and no active guardrail triggers exclusion.

#### Exclusion rules

| Rule | Signal | Notes |
|------|--------|-------|
| `masthead_no_cites` | `masthead` in filename tokens **and** eyecite finds 0 legal citations | Preferred path — no page-count limit needed |
| `masthead_short` | `masthead` in filename tokens **and** page count ≤ 3 | Fallback when eyecite is unavailable |
| `toc_no_cites` | `toc` or `table`+`contents` in filename tokens **and** eyecite finds 0 citations **and** ≤ 15 pages | Relaxed from the old ≤ 3 page limit |
| `toc_short_name` | `toc` or `table`+`contents` in filename tokens **and** page count ≤ 3 | Fallback when eyecite is unavailable |
| `frontmatter_in_filename` | `frontmatter` in filename tokens **and** eyecite finds 0 citations | Catches files explicitly named `*frontmatter*` |
| `cover_no_cites` | `cover` in filename tokens **and** eyecite finds 0 citations **and** ≤ 3 pages | Inside covers, title-page covers |
| `editorial_short` | `editorialboard` or `editorial`+`board` in filename tokens **and** ≤ 6 pages | Editorial board listings |
| `multi_marker_frontmatter` | ≥ 2 of `title-page`, `masthead`, `toc`, `editorialboard`, `frontmatter` in filename tokens | Compound frontmatter labels |
| `frontmatter_layout_onepage` | Exactly 1 page **and** text contains `volume`, `number`, `articles`/`notes` in a list layout | Single-page TOC sheets with no filename signal |
| `manual_guideline_strict` | `manual`/`guidelines`/`style guide` in **both** filename tokens **and** first-page text | Submission guidelines, author manuals |

#### Guardrails (prevent exclusion)

| Guardrail | Signal |
|-----------|--------|
| `article_like_longform` | ≥ 8 pages **and** eyecite finds ≥ 1 citation (falls back to regex count ≥ 2 if eyecite is unavailable) |
| `article_like_footnotes` | ≥ 3 footnote-marker lines on the first page |
| `metadata_article_present` | Scraped metadata includes a non-empty title **and** at least one of: author, citation, year |
| `toc_phrase_only` | "table of contents" appears in text but not in the filename and no other rules fired — ambiguous, keep |

#### Citation detection (eyecite)

Rules that require "0 citations" use [eyecite](https://github.com/freelawproject/eyecite) (v2.7+), which understands the full U.S. legal reporter corpus (`F.3d`, `U.S.`, `A.2d`, state reporters, `U.S.C.`, `C.F.R.`, etc.). This is significantly more precise than the legacy regex-based `CITATION_RE` pattern, which only matched a narrow subset. When eyecite is unavailable, rules fall back to page-count heuristics.

**Why keep forewords?** Law review forewords vary widely: some are 1-page ceremonial introductions; others are full scholarly pieces with citations. Since eyecite-confirmed citations are the article-like guardrail, a substantive foreword will not be excluded, while a purely administrative one that eventually gets scraped will pass through as well. We deliberately do not apply a blanket foreword exclusion rule.

#### Platform/domain overrides

Additional per-platform and per-domain rules live in `offprint/pdf_footnotes/doc_type_rules.json`. These are applied by `doc_policy.py` (used by the HF dataset builder and footnote pipeline) rather than the QC quarantine script. They cover known patterns like DC repository wrapper pages and site-specific non-article filenames.

## Footnote Pipeline Onboarding (Recommended: OlmOCR)

### Quickstart: Single vLLM Endpoint (Fastest Path)

If you only have one GPU endpoint, start one OpenAI-compatible vLLM server and run
`extract_footnotes.py` directly with `OLMOCR_SERVER_URL`.

1) Start vLLM server:

```bash
source .venv/bin/activate
vllm serve allenai/olmOCR-2-7B-1025-FP8 \
  --host 127.0.0.1 --port 8080 \
  --max-model-len 16384
```

2) Verify endpoint:

```bash
curl http://127.0.0.1:8080/v1/models
```

3) Run footnote extraction against that endpoint:

```bash
source .venv/bin/activate
export OLMOCR_SERVER_URL=http://127.0.0.1:8080/v1
export OLMOCR_MODEL=allenai/olmOCR-2-7B-1025-FP8
python scripts/extract_footnotes.py \
  --pdf-root artifacts/pdfs \
  --ocr-mode fallback
```

Notes:
- If `OLMOCR_SERVER_URL` is unset, `olmocr.pipeline` runs local inference and may
  incur heavy startup overhead.
- Page-scoped OCR now batches requested pages in one call; tune chunk size with
  `OLMOCR_PAGE_BATCH_SIZE` (default: `4`).
- If you see slow OCR batches, raise `OLMOCR_TIMEOUT_SECONDS` (default: `900`) or
  reduce `OLMOCR_PAGE_BATCH_SIZE` to `1` or `2`.

### 1) Start dual vLLM servers (one per 3090)

```bash
CUDA_VISIBLE_DEVICES=0 .venv/bin/vllm serve allenai/olmOCR-2-7B-1025-FP8 \
  --host 127.0.0.1 --port 8080 \
  --tensor-parallel-size 1 \
  --gpu-memory-utilization 0.85 \
  --max-model-len 16384
```

```bash
CUDA_VISIBLE_DEVICES=1 .venv/bin/vllm serve allenai/olmOCR-2-7B-1025-FP8 \
  --host 127.0.0.1 --port 8081 \
  --tensor-parallel-size 1 \
  --gpu-memory-utilization 0.85 \
  --max-model-len 16384
```

### 2) Run dual-shard extraction

```bash
make extract-footnotes-olmocr-dual \
  PDF_ROOT=artifacts/pdfs \
  FEATURES=legal \
  WORKERS=6 \
  OCR_WORKERS=2
```

This runs `ocr_mode=always` across two deterministic shards (`shard0`, `shard1`) against `:8080` and `:8081`.

### 3) Fallback semantics (safety path)

- In `ocr_mode=always`, extraction now tries OlmOCR first.
- If OlmOCR fails/timeouts for a PDF, native extraction runs as fallback for that PDF only.
- Warnings captured in sidecars/report include:
  - `olmocr_primary_failed`
  - `native_fallback_after_ocr_failure`

### 4) Native ordinality patch pass (new default)

`extract_footnotes.py` now runs a page-local native repair pass before OCR fallback when ordinality is invalid:

- Detect gap labels from ordinality validation.
- Build a temporary mini-PDF from inferred gap boundary pages.
- Re-run native extraction on that mini-PDF with a slightly lower cutoff.
- Merge only missing numeric labels, then re-validate ordinality.
- If still unresolved, optionally force OCR escalation for that document.

Controls:

```bash
python scripts/extract_footnotes.py \
  --pdf-root artifacts/pdfs \
  --ordinality-patch \
  --ordinality-patch-max-pages 20 \
  --ordinality-patch-expand 1 \
  --ordinality-patch-ocr-escalation-passes 1
```

Summary/report counters now include:
- `ordinality_patch_attempted_docs`
- `ordinality_patch_resolved_docs`
- `ordinality_patch_notes_added`

### 4b) OCR review queue for scanned/unreliable PDFs

The extractor now flags PDFs that likely need OCR/VLM follow-up instead of trusting native/pdftotext output. Typical triggers include:

- native extraction produced no spatial/body/note text
- pdftotext candidate was selected (`selected_pdftotext_output`)
- resulting note stream is ordinality-invalid under pdftotext fallback

Per-PDF result fields:
- `needs_ocr_review` (bool)
- `ocr_review_reasons` (list[str])

Sidecars for flagged docs include:
- warning: `needs_ocr_review`
- `document_metadata.needs_ocr_review = true`
- `document_metadata.ocr_review_reasons = [...]`

Run summary now includes:
- `needs_ocr_review`
- `ocr_review_manifest_path`

By default, a JSONL queue is emitted when flagged docs exist:
- `artifacts/runs/pdf_ocr_review_queue_<STAMP>.jsonl`

Controls:

```bash
python scripts/extract_footnotes.py \
  --emit-ocr-review-manifest true \
  --ocr-review-manifest-out artifacts/runs/my_ocr_review_queue.jsonl
```

### 5) Benchmark evidence (current repo snapshot)

From apples-to-apples overlap benchmark (`13` docs):

- GLM/OlmOCR sidecars: `notes_total=1459`, `ordinality_invalid_doc_rate=0.0000`
- ODL baseline: `notes_total=1503`, `ordinality_invalid_doc_rate=0.6923`

Reports:
- `artifacts/runs/footnote_benchmark_glm_apples_covered13_mar23a.json`
- `artifacts/runs/footnote_benchmark_odl_apples_covered13_mar23a.json`

Interpretation:
- OlmOCR path has much cleaner ordinality/label hygiene in this sample.
- ODL currently shows better context/citation field coverage; keep this in mind for downstream consumers.

### 6) Pixel-separator ablation v2 (hybrid CV + text + stabilization)

Repro command:

```bash
python scripts/benchmark_pixel_split_ablation.py \
  --ablation-name pixel_split_ablation_v2 \
  --line-weight 0.62 \
  --text-weight 0.38 \
  --max-jump-ratio 0.07 \
  --smooth-window 1
```

Medium-budget sweep template (4 configs):

```bash
for cfg in \
  "0.62 0.38 0.70 0.07 1" \
  "0.70 0.30 0.72 0.06 1" \
  "0.55 0.45 0.68 0.08 1" \
  "0.62 0.38 0.70 0.07 2"; do
  read -r LW TW MBS MJR SW <<<"$cfg"
  python scripts/benchmark_pixel_split_ablation.py \
    --ablation-name pixel_split_ablation_v2 \
    --line-weight "$LW" \
    --text-weight "$TW" \
    --min-blend-score "$MBS" \
    --max-jump-ratio "$MJR" \
    --smooth-window "$SW"
done
```

Most recent v1 baseline snapshot (`20260323T045618Z`, 13-doc subset):

- `notes_total=916` (vs OlmOCR baseline `1459`)
- `ordinality_invalid_doc_rate=0.6923` (vs OlmOCR baseline `0.0000`)
- Fallback cutoff used on `12/13` docs (`92.31%`)
- Separator detection rate by page: `0.5041`

Artifacts:
- `artifacts/runs/pixel_split_ablation_v2_summary_<STAMP>.json`
- `artifacts/runs/footnote_benchmark_pixel_split_ablation_v2_<STAMP>.json`

Key v2 diagnostics:
- `pages_with_text_anchor`, `text_anchor_rate`
- `pages_hybrid`, `hybrid_page_rate`
- `stabilized_adjustments`

Acceptance gate for keeping pixel-split as fallback candidate:
- `ordinality_invalid_doc_rate` improves by at least `0.10` vs v1 baseline.
- `frontmatter_leak_doc_rate` does not regress.
- `fallback_doc_rate` trends downward on the same sample.

Recommendation:
- Keep this approach as an optional targeted repair experiment only (e.g., niche scanned layouts).
- Do not replace the primary OlmOCR lane with it.

## Metadata Enrichment Onboarding

`extract_pdf_metadata.py` now supports two engines and shard-safe updates:

- `native_first_page`: fast PyMuPDF first-page heuristics.
- `olmocr_first2`: OlmOCR on temporary 2-page PDFs (pages 1-2) for stronger title/author/date/citation recovery.

Key behaviors:
- Reads `records.jsonl` in `artifacts/runs/<RUN_ID>/`.
- Applies metadata patches with lock-protected writes.
- Mirrors extracted fields into PDF sidecars under `document_metadata`.
- Supports deterministic sharding (`--shard-count`, `--shard-index`) for parallel workers.

Example (single process):

```bash
python scripts/extract_pdf_metadata.py \
  --run-id <RUN_ID> \
  --engine olmocr_first2 \
  --workers 8 \
  --overwrite-policy fill_gaps_only
```

Example (two shards in parallel):

```bash
python scripts/extract_pdf_metadata.py --run-id <RUN_ID> --engine olmocr_first2 --shard-count 2 --shard-index 0 --workers 6
python scripts/extract_pdf_metadata.py --run-id <RUN_ID> --engine olmocr_first2 --shard-count 2 --shard-index 1 --workers 6
```

Useful flags:
- `--overwrite-policy {fill_gaps_only,higher_confidence,always}`
- `--lock-path <path>` for explicit cross-process lock file
- `--report-out <path>` for run summary JSON
- `--dry-run` for no-write validation

## HuggingFace Dataset Build

`build_hf_dataset.py` extracts full text from downloaded law review PDFs and writes HuggingFace-compatible parquet shards. It produces two dataset configs:

- **`hf/text/fulltext/part-*.parquet`** — one row per article with full text, metadata, citations, and section headers
- **`hf/footnotes/footnotes/part-*.parquet`** — one row per footnote, linked to parent document via `doc_id`

### Fulltext columns

| Column | Type | Description |
|---|---|---|
| `doc_id` | string | Stable 24-char hash from `pdf_sha256` or path |
| `run_id` | string | Pipeline run that produced the source record |
| `pdf_sha256` | string | SHA-256 of the PDF file |
| `pdf_relative_path` | string | Path relative to `artifacts/pdfs/` |
| `domain` | string | Source domain (e.g. `harvardlawreview.org`) |
| `doc_type` | string | `article`, `issue_compilation`, `frontmatter`, or `other` |
| `doc_type_reason_codes` | list[str] | Why the doc_type was assigned |
| `doc_type_confidence` | float | Classification confidence (0–1) |
| `platform_family` | string | `digital_commons`, `wordpress`, `ojs`, `scholastica`, `custom_unknown` |
| `seed_url` | string | Original seed that led to this PDF |
| `page_url` | string | HTML page where the PDF was linked |
| `pdf_url` | string | Direct URL to the PDF |
| `title` | string | Article title (from adapter metadata) |
| `authors` | list[str] | Author names |
| `journal` | string | Journal name |
| `volume` | string | Volume number |
| `issue` | string | Issue number |
| `year` | string | Publication year |
| `text` | string | Full extracted text (capped at `--max-text-chars`, default 300k) |
| `char_count` | int | Length of `text` |
| `page_count` | int | Number of PDF pages |
| `ocr_used` | bool | Whether OCR was used for this document |
| `ocr_backend` | string | OCR backend used (`olmocr` or null) |
| `extraction_method` | string | `native` or `olmocr` |
| `warnings` | list[str] | Extraction warnings (e.g. `text_truncated`) |
| `citations` | list[str] | Extracted legal citations (case cites, U.S.C., C.F.R.) |
| `section_headers` | list[str] | Detected section headings |
| `built_at` | string | ISO timestamp of row creation |

### Document-type filtering

Before text extraction, each PDF is classified as `article`, `issue_compilation`, `frontmatter`, or `other` using first-page heuristics and configurable rules (`offprint/pdf_footnotes/doc_type_rules.json`). The `--doc-policy` flag controls which types are included:

- `article_only` (default) — only individual articles
- `include_issue_compilations` — articles + full-issue PDFs
- `all` — everything

Excluded documents are logged to `hf/metadata/hf_doc_type_exclusions_*.jsonl`.

### Error handling

- Per-PDF extraction errors are caught and counted (not fatal to the build).
- Errors are logged to `hf/metadata/hf_extraction_errors.jsonl` with `pdf_path`, `domain`, and `error` fields.
- The final summary (`hf/metadata/hf_dataset_build_summary.json`) reports total errors, rows written, shards, and elapsed time.
- With `--verbose` / `-v`, individual PDF errors are logged to stderr at DEBUG level.
- The tqdm progress bar shows real-time extraction progress (works in screen/tmux).

### Incremental builds (adding new PDFs)

To avoid re-extracting all PDFs when new ones are scraped, use `--incremental`:

```bash
python scripts/build_hf_dataset.py --run-id <new_run_id> --ocr-mode off --incremental
```

This reads `hf/metadata/done_shas.txt` (a newline-delimited list of `pdf_sha256` values already processed) and skips those records. After writing, newly processed SHAs are appended to the sidecar. New parquet shards are appended alongside existing ones.

To start fresh, delete `hf/metadata/done_shas.txt` and the `hf/text/` directory.

### Running in screen/tmux

The script logs to stderr and prints a tqdm progress bar, so it works well detached:

```bash
# In screen or tmux
screen -S hf-build
python scripts/build_hf_dataset.py --run-id 20260312T065010Z --ocr-mode off --workers 12 -v 2>&1 | tee hf_build.log
# Ctrl-A D to detach

# Reattach later
screen -r hf-build
```

### Parallelism

Text extraction is parallelized via `ThreadPoolExecutor` (default 6 workers, configurable with `--workers`). The workload is CPU-bound (PDF parsing), so workers should roughly match available cores. For OCR-enabled builds, the OCR pool has its own worker count.

### Quick start examples

```bash
# Basic build from latest run, no OCR
python scripts/build_hf_dataset.py --ocr-mode off

# Build from golden run with 12 workers
python scripts/build_hf_dataset.py --run-id 20260304T225711Z --ocr-mode off --workers 12

# Include issue compilations when no article-level PDFs exist
python scripts/build_hf_dataset.py --ocr-mode off --include-issue-compilations-when-no-articles

# Limit to first 100 records for testing
python scripts/build_hf_dataset.py --ocr-mode off --limit 100

# Incremental: process only new PDFs
python scripts/build_hf_dataset.py --run-id <new_run> --ocr-mode off --incremental

# Load the result
python -c "from datasets import load_dataset; ds = load_dataset('parquet', data_files='hf/text/fulltext/part-*.parquet'); print(ds)"
```

## Notes
- Canonical paths are `python scripts/<stage>/<name>.py --help`; compatibility wrappers also support `python scripts/<name>.py --help`.
- Runtime outputs default to `artifacts/`.
- For policy and run semantics, use `docs/OPERATIONS.md` as source of truth.
