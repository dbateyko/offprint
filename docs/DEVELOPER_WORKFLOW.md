# Developer Workflow

This page is the canonical contributor workflow for local development and CI parity.

## Environment Setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e .[dev]
```

Optional runtime tools:

```bash
pip install responses playwright
playwright install chromium
```

Optional footnote pipeline extras:

```bash
pip install -e .[pdf_footnotes]
```

## Core Quality Gates

Local checks that mirror CI:

```bash
ruff check offprint tests scripts
black --check offprint tests scripts
make adapter-policy-check
make repo-layout-check
pytest -q
```

One-command wrapper:

```bash
make quality-check
```

Focused regression slice for adapter/seed/orchestrator critical path:

```bash
make critical-path-tests
```

## Production Pipeline Commands

```bash
make production
make production-resume RUN_ID=<run_id>
make production-delta BASE_RUN_ID=<run_id>
make production-retry RUN_ID=<run_id>
```

Operator-monitored mode:

```bash
make production-monitored
make production-monitored-resume RUN_ID=<run_id>
```

## Artifact Policy

- Canonical run artifacts are written under `artifacts/runs/<run_id>/`.
- Legacy per-domain manifests (`artifacts/runs/<domain>.jsonl`) are disabled by default.
- Enable legacy manifests only when needed by downstream consumers:
  - pipeline: `--write-legacy-manifests`
  - orchestrator: `--write-legacy-manifests`
  - env fallback: `LRS_WRITE_LEGACY_MANIFESTS=true`

## Directory Conventions

- Runtime outputs: `artifacts/`
- Seeds: `sitemaps/`
- Core runtime code: `offprint/`
- Operational scripts: `scripts/`
- Versioned registry/reference data: `data/registry/`
- Tests: `tests/`
