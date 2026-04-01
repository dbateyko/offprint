# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project uses Python primarily. Key domains: law review journal scraping (Playwright browser pool), PDF-to-text pipelines with footnote extraction (liteparse), HTML provisions explorer pages, and ML experiment workflows (clustering, BM25, reranking). Journal onboarding involves registering adapters, building sitemaps, and smoke testing.

Production-grade PDF scraper toolkit for U.S. law review journals. Discovers and downloads law review PDFs from 160+ journal sites across platforms (Digital Commons/BePress, OJS, Scholastica, WordPress, custom), extracts metadata (title, authors, volume, issue, date, DOI, abstract), and produces immutable, auditable run artifacts.

## General Instructions

When implementing a user's plan or spec, follow it step-by-step exactly as described. Do not skip diagnostic/measurement steps or reorder phases. If the plan says 'measure first, then implement,' do not jump to implementation.

## PDF Processing

When working with this codebase, always use `liteparse` for PDF parsing/footnote extraction unless explicitly told otherwise. Do not substitute other parsers like opendataloader.

## Development Workflow

After making code changes, always run the existing test suite before committing. If tests fail due to new kwargs or changed signatures, fix the tests before proceeding.

## Git Conventions

For git merge conflicts, prefer `git rebase` over merge commit strategies. When merging branches, attempt fast-forward first, then rebase if needed.

## Performance Guidelines

When working with large datasets (millions of rows), use streaming/chunked loading. Never load full corpus into memory for reranking or similarity operations. Use memory-mapped files or batch processing.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .[dev] responses playwright && playwright install chromium
```

## Common Commands

```bash
# Full quality gate (lint + format + adapter policy + tests — mirrors CI)
make quality-check

# Lint & format individually
ruff check offprint tests scripts
black --check offprint tests scripts

# Run all tests
pytest -q

# Run a single test file
pytest tests/test_digitalcommons_adapter.py -v

# Focused regression suite (adapter/seed/orchestrator critical path)
make critical-path-tests

# Production pipeline (full run)
make production MAX_WORKERS=120 MIN_DELAY=1.5 MAX_DELAY=4.0

# Resume interrupted run
make production-resume RUN_ID=<run_id>

# Delta mode (only newer articles vs base run)
make production-delta BASE_RUN_ID=<run_id>

# Retry failures from existing run
make production-retry RUN_ID=<run_id>

# Operator-monitored mode (serial, headed browser, manual captcha intervention)
make production-monitored

# Smoke test (one PDF per site)
python scripts/smoke_one_pdf_per_site.py --sitemaps-dir sitemaps --max-workers 8

# Unattended overnight run (8h, conservative settings)
make production-overnight

# Resume overnight
make production-overnight-resume RUN_ID=<run_id>

# DC-only production run (new Digital Commons sitemaps, polite settings)
make production-dc

# Metadata quality report (title/author/vol/date coverage per domain)
make metadata-quality-report                          # golden run, non-DC domains
make metadata-quality-report METADATA_QUALITY_RUN_ID=<run_id> METADATA_QUALITY_PLATFORM=wordpress

# Post-processing: PDF metadata enrichment
python scripts/extract_pdf_metadata.py --run-id <run_id> --max-workers 24

# Post-processing: footnote extraction
make extract-footnotes

# Build fast/adapter/drop lanes from smoke history
python scripts/build_high_yield_lanes.py

# Run one short fast-lane smoke cycle
python scripts/run_high_yield_cycle.py
```

### Operational Commands

```bash
# CSV + terminal summary of sitemap/adapter/PDF coverage
make site-status

# Promote a completed run as the golden baseline
make promote-run RUN_ID=<run_id>

# Detect platform for a URL
python scripts/fingerprint_site.py --url <url>

# Create seed + register adapter for a new site
python scripts/auto_onboard_site.py --url <url>
```

Useful Make overrides: `SKIP_RETRY_PASS=1`, `LINKS_ONLY=1` (discovery only, no downloads), `MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN=0` disables the circuit breaker.

**Playwright runtime**: headed browser is the default transport. Opt-out flags: `--playwright-headless`, `--dc-browser-headless`, `--no-use-playwright`.

## Architecture

### Adapter Pattern

Every journal platform has an **Adapter** subclass (in `offprint/adapters/`) implementing two methods:
- `discover_pdfs(seed_url, max_depth)` → yields `DiscoveryResult` (page_url, pdf_url, metadata, provenance)
- `download_pdf(pdf_url, out_dir)` → returns local path; populates `self.last_download_meta`

Adapter hierarchy:
- `Adapter` (base in `base.py`) — interface + shared `_download_with_generic` fallback
- `GenericAdapter` — BFS link crawler with UA rotation; fallback for unknown sites
- `DigitalCommonsBaseAdapter` — OAI-PMH + sitemap enumeration for BePress sites
- `OJSAdapter` — Open Journal Systems
- `ScholasticaBaseAdapter`, `WordPressAcademicBaseAdapter`, `QuartexAdapter`
- Site-specific adapters (e.g., `BerkeleyBTLJAdapter`, `HarvardJOLTAdapter`)

**Adapter routing** (`registry.py:pick_adapter_for(url)`): domain → exact match → parent-domain match → platform heuristic → GenericAdapter fallback.

**Two-phase registration**: `registry.py` registers base mappings, then `__init__.py` imports and overrides some entries (e.g., `law.stanford.edu` → `StanfordSTLRAdapter`, `journals.library.columbia.edu` → `ColumbiaSTLRAdapter`). Always check both files when debugging adapter routing.

**Platform heuristics** (in order): URL contains `all_issues.html` → Digital Commons; domain substring matches (`digitalcommons.`, `scholarship.`, `scholarlycommons.`, `scholarworks.`, etc.) → Digital Commons; OJS URL patterns (`/index.php/`, `/issue/archive`, `/article/view`) → OJS; otherwise → GenericAdapter.

**WordPress scoping**: `WordPressAcademicBaseAdapter` extracts the first path segment from the seed URL to scope discovery to a single journal on multi-tenant WordPress sites (e.g., seed `/american-criminal-law-review/in-print/` scopes to `/american-criminal-law-review/`).

### Orchestrator Pipeline

Entry point: `scripts/run_pipeline.py --mode {full|delta|retry}`

Flow: load seeds from `sitemaps/*.json` → create timestamped run_id → parallel worker threads per seed → adapter selection → discovery → download with retry (max 3, exponential backoff) → PDF validation (magic bytes + SHA-256) → write JSONL records/errors/stats to `artifacts/runs/<run_id>/`.

Circuit breaker: 3 consecutive failures on a domain → skip remaining seeds for that domain. The workload is I/O-bound (network + disk), so `MAX_WORKERS` can safely exceed core count (e.g., 120 workers on a 28-core machine). Resume mode skips seeds already marked `completed` in `stats.json`.

### Polite Requests

`PoliteRequestsSession` (`polite_requests.py`): random delay between requests, thread-safe rate limiting, connection pooling, built-in retry with Retry-After respect, UA rotation.

`HttpSnapshotCache` (`http_cache.py`): TTL-based (24h default) gzip-compressed cache for HTML/XML/JSON responses (not PDFs).

### Error Taxonomy

Errors classified in orchestrator as: `http_error`, `timeout`, `network`, `filesystem`, `invalid_pdf`, `blocked_robots`, `waf_challenge`, `precheck_failed`, `unknown`. Retryable: timeout, network, 5xx, invalid_pdf, unknown. Not retryable: 403, filesystem, blocked_robots, waf_challenge.

### Run Artifacts

Each run produces in `artifacts/runs/<run_id>/`:
- `manifest.json` — config snapshot, timing, summary
- `records.jsonl` — discovered/downloaded records with full provenance
- `errors.jsonl` — failures with error taxonomy
- `stats.json` — per-seed/per-domain/per-journal metrics

**Golden baseline**: `artifacts/runs/golden_run.json` points to the current promoted run. Delta mode uses it automatically when `BASE_RUN_ID` is omitted. Promote a new baseline with `make promote-run RUN_ID=<run_id>`.

### Coverage Validation

`offprint/coverage_tools/`: volume/issue gap detection (`sequence_validator.py`), CI/CD gate (`gate.py`), live crawl validation (`live_crawl.py`).

### PDF Footnote Extraction (Optional)

`offprint/pdf_footnotes/`: OCR + footnote parsing pipeline, legal citation classification, QC filtering. Requires `[pdf_footnotes]` optional deps.

**Docling-primary extraction**: The pipeline uses docling as the sole text source for footnote extraction. Docling produces clean text without the small-caps mangling issues seen in pdfplumber/pdftotext (e.g., "U. PA. L. REV." rendered correctly instead of garbled). Key details:

1. `_normalize_docling_text()` cleans docling's systematic spacing artifacts at extraction time: spaces before punctuation, spaced reporter citations (`A. 2 d` → `A.2d`), star-page spacing, etc.
2. `_split_merged_note_lines()` handles docling's one known weakness — occasionally merging consecutive short notes into a single text block (e.g., "25 Id. 26 See..."). The splitter detects sentence-ending punctuation followed by a new numeric label and splits them.
3. Fallback: if docling is unavailable, the pipeline falls back to pdfplumber/pypdf for the main pipeline or pdftotext for the standalone script (`scripts/extract_footnotes_from_docling.py`).

### Metadata Extraction Stages

1. **Discovery-time** (adapter-specific): title, authors, volume, issue, year, abstract, DOI from HTML. Coverage varies — OJS/Scholastica/DC have high coverage; WordPress/Generic are partial.
2. **PDF first-page text** (post-download): `extract_pdf_metadata.py` parses the first page; law review articles follow title-in-caps + author-with-footnote-marker patterns. Fills gaps uniformly across adapters.
3. **PDF document properties** (`/Title`, `/Author`): generally unreliable for law reviews and not used.

## Adding a New Adapter

1. Create adapter in `offprint/adapters/<platform>.py` subclassing `Adapter`
2. Register domain mapping in `offprint/adapters/registry.py`
3. Add seed JSON in `sitemaps/<domain-slug>.json`
4. Add tests with HTML fixtures in `tests/fixtures/`
5. Validate with smoke test targeting the new site

### Seed JSON Format

```json
{
  "id": "<domain-slug>",
  "start_urls": ["<url>"],
  "source": "manual",
  "metadata": {
    "journal_name": "<name>",
    "platform": "<wordpress|digitalcommons|ojs|drupal|scholastica|quartex|janeway>",
    "url": "<canonical_url>",
    "created_date": "<YYYY-MM-DD>",
    "status": "<active|todo_adapter|paused_404|paused_waf|paused_login|paused_paywall|paused_other>",
    "status_reason": "<human-readable explanation>",
    "status_updated_at": "<ISO UTC timestamp>",
    "status_evidence_ref": "<link to issue, smoke log, or artifact>"
  }
}
```

Legacy format uses `"url"` key instead of `"start_urls"` (still supported).

## Code Style

- Python 3.8+, type hints, Black (100-char line length), Ruff (E/F rules, E501 and E402 ignored)
- Conventional Commits: `feat:`, `fix:`, `refactor:`, etc.
- Tests use `pytest` with `responses` library for HTTP mocking; fixtures in `tests/fixtures/`
- Config in `pyproject.toml` (pytest addopts: `-v --tb=short`)
- **Shared-adapter rule**: changes to base adapters (`WordPressAcademicBaseAdapter`, `DigitalCommonsBaseAdapter`, `OJSAdapter`, `DrupalAdapter`) affect many journals. Prefer per-site tuning in sitemap metadata or host-specific adapters first. If a shared adapter must change, keep logic domain-guarded and add regression tests covering both the target host and existing family behavior.

## Key Constraints

- Polite crawling: respect robots.txt, honor 429/5xx backoff, use random delays between requests
- Immutable runs: artifacts in `artifacts/runs/<run_id>/` are never overwritten
- No runtime artifacts in git: `pdfs/`, `runs/`, `cache/http/` are gitignored
- Core deps are minimal: `requests`, `beautifulsoup4`, `lxml` only
- Legacy per-domain manifests (`<domain>.jsonl`) are disabled by default; opt in with `--write-legacy-manifests` when needed by downstream consumers

## Journal Registry

`data/registry/lawjournals.csv` is the master journal list (1,400+ entries), merged from three sources: sitemaps on disk (canonical), Washington & Lee law journal rankings, and Current Index to Legal Periodicals (CILP). Key columns: `url` (canonical homepage), `fixed_domain_url` (host-level fallback URL when uniquely mappable from `data/registry/upstream/fixed_domains.txt`), `confirmed_working` (PDF count from production runs, set by `stage_lawjournals.py`), `status`. Upstream source snapshots live in `data/registry/upstream/` (`LawJournals.csv`, `wlu_all_journals.csv`, `cilp_journals.csv`). Build/rebuild with `python data/registry/build_lawjournals.py`. Legacy derived CSVs are archived in `data/registry/attic/`.

## Repo Structure (post-restructure)

- `docs/skills/` — canonical Claude Code skills (onboard-journal, probe-selectors, analyze-footnotes, etc.)
- `research/` — experiment notebooks and scripts (renamed from `experiments/`)
- `archive/` — stale temp artifacts, old probe results, duplicate skill dirs
- `docs/action_lists/` — prioritized work queues (thin-site reonboard, metadata gaps, WL top-50 coverage)
- `scripts/audit_adapter_coverage.py` — flags underperforming domains (<=N PDFs); `--apply` updates sitemap statuses, `--csv` for reports

## QC Filter Rules

`offprint/pdf_footnotes/qc_filter.py` uses a two-pass `run_qc`: first collects all signals per document, then evaluates with domain-level context. Rule families:

- **Full-volume detection**: filename patterns (`full-issue`, `fm-NNNN-NNNN`), size anomaly (>5x median AND >10MB), page anomaly (>150pp vs <40pp median). Guardrail suppression: `article_like_*` guardrails don't protect full volumes.
- **Non-law-review content**: filename-based (`annual-report`, `ipcc`, `thesis`, etc.) with eyecite confirmation; book-review/appendix allowlisted.
- **Ordinality gap recovery**: page-scoped OCR anchors recover footnote numbering gaps across page boundaries.

## Footnote Pipeline: OCR Backend

The footnote pipeline requires a running olmocr/vLLM server for OCR. Start it before running extraction:
```bash
# See docs/skills/run-vllm-qwen35-27b/SKILL.md for full setup
vllm serve <model> ...
```
