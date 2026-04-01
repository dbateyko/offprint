# Repository Layout

## Top-Level Structure

```
offprint/           Core Python package: orchestrator, adapters, PDF footnotes, coverage tools
scripts/               Operational CLIs and maintenance tools (see scripts/README.md)
tests/                 All executable tests and fixtures
offprint/sitemaps/     Seed JSON configs — one per journal, source of truth for scrape targets
data/registry/         Versioned registry CSVs and reference datasets
docs/                  Documentation, skills, and operational playbooks
autoresearch/          LLM-powered site discovery (placeholder — see autoresearch/README.md)
research/              Experimental analysis projects (e.g., facial unconstitutionality scan)
hf/                    Hugging Face dataset exports (parquet, metadata, footnotes)
ci/                    CI configuration (coverage targets)
.github/workflows/     GitHub Actions (quality checks, coverage gate)
.claude/commands/      Claude Code skill definitions (local use — canonical copies in docs/skills/)
prompts/               Agent prompt templates (used by opencode.json)
```

## Key Directories

### `offprint/` — Core Package
- `orchestrator.py` — Main CLI and PDF download orchestrator
- `adapters/` — 60+ platform adapters (Digital Commons, OJS, WordPress, Drupal, Scholastica, etc.)
- `adapters/registry.py` — Adapter routing by domain
- `adapters/selector_driven.py` — CSS selector-based generic adapter
- `pdf_footnotes/` — Footnote extraction pipeline (text extraction, segmentation, OCR, evaluation)
- `coverage_tools/` — Quality gate and coverage validation
- `seed_catalog.py` — Seed file management
- `polite_requests.py` — Rate-limited HTTP with robots.txt respect

### `scripts/` — Operational Tools
Organized by workflow — see `scripts/README.md` for the full index.
- `pipeline/` — run orchestrator, smoke crawler, baseline promotion
- `onboarding/` — site fingerprinting + seed/adapter onboarding
- `processing/` — QC quarantine and PDF/text/footnote/data extraction
- `quality/` — policy/lint checks and extraction evaluation
- `reporting/` — status and metadata coverage reporting

### `docs/skills/` — Claude Code Skills
Shareable prompt-based workflows. See `docs/skills/README.md`.

## Runtime Artifacts (gitignored)

```
artifacts/             All runtime outputs
  pdfs/                Downloaded PDFs
  smoke/               Smoke test outputs
  runs/                Production run manifests and reports
  cache/http/          HTTP provenance cache
archive/               Archived files from repo cleanups (gitignored)
```

## Policy
- Test code goes in `tests/` only.
- Runtime outputs go in `artifacts/` only.
- Do not commit runtime artifacts or large binaries.
- Seed JSON is the source of truth — one file per journal in `offprint/sitemaps/`.
- Skills live canonically in `docs/skills/`; `.claude/commands/` is for local use.
