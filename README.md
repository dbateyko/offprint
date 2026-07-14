<p align="center">
  <img src="./offprint.png" width="520" alt="Offprint" />
</p>

<h1 align="center">Offprint</h1>

<p align="center">
  A reproducible toolkit for cataloging law journals, collecting article PDFs, and extracting research-ready text and footnotes.
</p>

<p align="center">
  <a href="https://github.com/dbateyko/offprint/actions/workflows/quality.yml"><img alt="quality checks" src="https://github.com/dbateyko/offprint/actions/workflows/quality.yml/badge.svg"></a>
  <a href="./LICENSE"><img alt="MIT license" src="https://img.shields.io/badge/license-MIT-1f6f5c.svg"></a>
  <a href="https://www.python.org/"><img alt="Python 3.8+" src="https://img.shields.io/badge/python-3.8%2B-3776ab.svg"></a>
</p>

Offprint joins four pieces that are usually scattered across one-off research scripts: a
versioned journal gazetteer, site-specific acquisition adapters, auditable run manifests,
and document parsers with explicit quality checks. It is designed for legal-research
collections where provenance and failure accounting matter as much as download volume.

> Offprint tracks metadata and code in Git. Downloaded PDFs and derived corpus artifacts
> stay local and are not distributed by this repository.

## Pipeline

```mermaid
flowchart LR
    G[Journal gazetteer] --> S[Sitemap seeds]
    S --> A[Platform and site adapters]
    A --> R[Run records and PDFs]
    R --> Q[Document QC]
    Q --> P[Text and footnote parsers]
    P --> E[Research exports]
```

| Stage | What Offprint provides | Canonical entry point |
|---|---|---|
| Find | Journal names, hosts, platforms, provenance, and lifecycle state | `data/registry/lawjournals.csv` |
| Configure | One JSON seed per crawl target | `offprint/sitemaps/` |
| Collect | Adapter routing, polite requests, resume/retry, immutable run records | `scripts/pipeline/run_pipeline.py` |
| Inspect | Reproducible gazetteer tables and local run/corpus reports | `scripts/reporting/` |
| Parse | PDF QC, metadata, article text, citations, and ordinal footnotes | `scripts/processing/` |
| Evaluate | Fixture tests, gold scoring, corpus diagnostics, and policy gates | `scripts/quality/` |

## Tracked Coverage

These figures are generated from the versioned registry and sitemap files. They describe
what the repository knows and can address, not what any one machine has downloaded.

| Tracked measure | Current value |
|---|---:|
| Journal registry rows | 2,600 |
| Registry rows linked to a sitemap | 1,850 |
| Sitemap files | 1,958 |
| Unique sitemap hosts | 839 |
| Invalid sitemap JSON files | 0 |

See the full [gazetteer snapshot](docs/generated/GAZETTEER_SNAPSHOT.md) for status,
platform, source, and metadata-completeness tables. Regenerate it with `make gazetteer`.

## Quickstart

The base install supports registry inspection, requests-based collection, and the unit-test
suite. It does not require a browser, GPU, or private corpus.

```bash
git clone https://github.com/dbateyko/offprint.git
cd offprint
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'

make doctor
make gazetteer-check
pytest -q tests/test_gazetteer.py tests/test_imports.py
```

Optional parsing dependencies are installed separately because OCR and layout packages are
large:

```bash
python -m pip install -e '.[pdf_footnotes]'
```

## Try One Journal

Scope exploratory runs to a copied seed and temporary outputs. This example performs
requests-based discovery for one OJS journal without downloading the entire registry:

```bash
mkdir -p /tmp/offprint-seeds
cp offprint/sitemaps/aalj-org.json /tmp/offprint-seeds/

python scripts/pipeline/run_pipeline.py \
  --mode full \
  --sitemaps-dir /tmp/offprint-seeds \
  --out-dir /tmp/offprint-pdfs \
  --manifest-dir /tmp/offprint-runs \
  --export-dir /tmp/offprint-exports \
  --max-workers 1 \
  --max-depth 1 \
  --links-only \
  --no-use-playwright \
  --skip-retry-pass
```

To parse a local PDF directory after installing `pdf_footnotes`:

```bash
python scripts/processing/extract_footnotes.py \
  --pdf-root /path/to/pdfs \
  --features legal \
  --ocr-mode off
```

Collection touches third-party sites. Review the seed, source terms, and request settings
before running it; use conservative concurrency and honor backoff signals.

## Start by Task

| I want to... | Start here |
|---|---|
| Understand the system and artifact flow | [Architecture](docs/ARCHITECTURE.md) |
| Understand journal counts and readiness | [Gazetteer and coverage](docs/GAZETTEER.md) |
| Add or repair a journal scraper | [Adapter development](docs/ADAPTER_DEVELOPMENT.md) |
| Run or recover a collection job | [Operations](docs/OPERATIONS.md) |
| Work on parsing or footnotes | [Script catalog](scripts/README.md#document-processing) |
| Make a first contribution | [Contributor start](docs/CONTRIBUTOR_START_HERE.md) |
| Understand what may be committed or released | [Data and release policy](docs/DATA_AND_RELEASE_POLICY.md) |
| See every maintained guide | [Documentation index](docs/README.md) |

## Repository Boundaries

This public repository contains the reusable package, scraper adapters, parser code,
versioned journal metadata, tests, and reporting definitions. Runtime outputs under
`artifacts/` are gitignored. A separate private data-operations workspace may combine local
Offprint outputs with donated or licensed collections and research labels; it is not required
to install, test, or contribute to Offprint.

## Contributing

Use focused changes with fixture-based tests and evidence for scraper behavior. Run
`make quality-check` before opening a pull request. See [CONTRIBUTING.md](CONTRIBUTING.md)
for setup, adapter policy, data rules, and review expectations.

## Responsible Use

Offprint is for scholarly research workflows. Respect publisher terms, access controls,
copyright, and `robots.txt`; identify your client appropriately; keep request rates polite;
and do not redistribute collected documents without verifying rights.
