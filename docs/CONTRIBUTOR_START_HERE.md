# Contributor Start

This path gets a clean checkout to a small, validated contribution without private data or
production infrastructure.

## 1. Install the Base Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Install `.[pdf_footnotes]` only when working on document parsing. Browser and OCR workflows
have additional system/runtime requirements and are not needed for registry, adapter-fixture,
or documentation changes.

## 2. Check the Checkout

```bash
make doctor
make gazetteer-check
pytest -q tests/test_imports.py tests/test_gazetteer.py
```

Optional dependency warnings from `make doctor` are expected unless you are working on that
capability. Required failures should be fixed before continuing.

## 3. Learn the Relevant Path

| Change | Read first | Typical files |
|---|---|---|
| Add a journal using an existing adapter | [Journal onboarding](skills/onboard-journal.md) | `offprint/sitemaps/*.json` |
| Change scraping behavior | [Adapter development](ADAPTER_DEVELOPMENT.md) | `offprint/adapters/`, `tests/` |
| Change registry/reporting | [Gazetteer](GAZETTEER.md) | `data/registry/`, `offprint/gazetteer.py` |
| Change text or footnotes | [Architecture](ARCHITECTURE.md#document-lifecycle) | `offprint/pdf_footnotes/`, `tests/fixtures/` |
| Change run behavior | [Operations](OPERATIONS.md) | `offprint/orchestrator.py`, `scripts/pipeline/` |

## 4. Make a Narrow Change

- Prefer sitemap `adapter_config` for isolated site differences.
- Add a host-specific adapter when the behavior is genuinely unique.
- Change a shared platform adapter only with regression coverage for the target and a nearby
  existing site.
- Keep runtime outputs, downloaded PDFs, and local paths out of the commit.

## 5. Validate

Run the targeted test while developing, then the repository gates:

```bash
make quality-check
pytest -q
```

Network smoke evidence is useful for adapter changes but should not replace deterministic
fixtures. Include the command, target, date, and result in the pull request without committing
the downloaded corpus.

## 6. Open the Change

Explain the behavior changed, why the existing behavior was insufficient, what evidence
supports the new behavior, and any expected effect on request volume or corpus inclusion.
See [CONTRIBUTING.md](../CONTRIBUTING.md) for the full review contract.
