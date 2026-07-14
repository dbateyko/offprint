# Contributing

Offprint welcomes focused improvements to journal coverage, acquisition reliability,
document parsing, quality evaluation, and public metadata.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
make doctor
```

For parser work, also install `python -m pip install -e '.[pdf_footnotes]'`. For browser
fallback work, install Playwright separately and run `playwright install chromium`.

## Before a Pull Request

```bash
make quality-check
pytest -q
```

The quality target checks the repository-facing Python tooling, repository layout, maintained
documentation links, generated gazetteer freshness, and its focused tests. Run the full test
suite separately.

## Choose the Smallest Extension Point

1. Adjust sitemap metadata or `adapter_config` for one target.
2. Add a host-specific adapter for genuinely unique behavior.
3. Change a shared platform adapter only when the behavior generalizes.

Shared adapter changes must include regression tests for the target and at least one nearby
existing-family behavior. Keep routing deterministic, failures structured, and request
behavior observable.

## Add a Journal

Follow [the journal-onboarding guide](docs/skills/onboard-journal.md). A contribution should
include:

- a sitemap JSON with identity, platform, lifecycle status, and evidence;
- adapter routing or configuration when the existing route does not fit;
- a deterministic fixture test for new logic; and
- dated smoke evidence for the live target when feasible.

Do not mark a target successful merely because its homepage responds. The smoke must exercise
article discovery and PDF validation.

## Parser Changes

Use real-layout regression fixtures or minimal synthetic documents that reproduce the failure.
Report which denominator changes: document qualification, native extraction, OCR routing,
note ordinality, or field coverage. Avoid aggregate quality claims that mix non-articles,
scans, and parser failures.

## Data and Security

- Never commit credentials, cookies, browser profiles, or tokens.
- Do not commit downloaded PDFs, corpus text, run caches, or large runtime logs.
- Document provenance for registry inputs and small evaluation fixtures.
- Respect publisher terms, `robots.txt`, access controls, and backoff signals.
- Review [data and release policy](docs/DATA_AND_RELEASE_POLICY.md) before adding datasets.

## Review Notes

Describe what changed, why, test and smoke evidence, operational impact, and any migration or
rerun required. Keep commits scoped and do not include unrelated generated artifacts.
