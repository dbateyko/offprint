# Operations

This is the canonical runbook for acquisition and document processing. Run commands from the
repository root and keep outputs under `artifacts/` or an explicit external directory.

## Preflight

```bash
source .venv/bin/activate
make doctor
make quality-check
make site-status
```

`make doctor` distinguishes required failures from optional parser/browser warnings.
`make site-status` uses local artifacts when available; its totals are machine-specific.

Before network acquisition, review target lifecycle state, `robots.txt`, source terms, adapter
routing, concurrency, and available disk space.

## Bounded Trial

Never use the entire sitemap directory for a first test. Copy one or a few reviewed seeds:

```bash
rm -rf /tmp/offprint-seeds /tmp/offprint-trial
mkdir -p /tmp/offprint-seeds
cp offprint/sitemaps/aalj-org.json /tmp/offprint-seeds/

python scripts/pipeline/run_pipeline.py \
  --mode full \
  --sitemaps-dir /tmp/offprint-seeds \
  --out-dir /tmp/offprint-trial/pdfs \
  --manifest-dir /tmp/offprint-trial/runs \
  --export-dir /tmp/offprint-trial/exports \
  --cache-dir /tmp/offprint-trial/cache \
  --max-workers 1 \
  --max-depth 1 \
  --links-only \
  --no-use-playwright \
  --skip-retry-pass
```

Remove `--links-only` only after inspecting discovery records and confirming the target and
request settings are appropriate.

## Production Entry Points

```bash
make production-help
make production
make production-resume RUN_ID=<run_id>
make production-delta BASE_RUN_ID=<run_id>
make production-retry RUN_ID=<run_id>
```

The Makefile is the source of truth for defaults. Override paths and concurrency explicitly:

```bash
make production \
  SITEMAPS_DIR=/path/to/reviewed-seeds \
  OUT_DIR=/path/to/pdfs \
  MANIFEST_DIR=/path/to/runs \
  EXPORT_DIR=/path/to/exports \
  MAX_WORKERS=4 \
  MIN_DELAY=1.5 \
  MAX_DELAY=4.0
```

The default production target uses Playwright in headed mode for supervised operation. Use
`PIPELINE_EXTRA_ARGS` deliberately when changing browser, Digital Commons, or completeness
behavior. Do not raise concurrency to compensate for WAF or backoff responses.

## Run Records

Canonical output is `artifacts/runs/<run_id>/`:

| File | Inspect for |
|---|---|
| `manifest.json` | Exact configuration and run identity |
| `records.jsonl` | Successful discovery/download provenance |
| `errors.jsonl` | Structured failure types and retry candidates |
| `stats.json` | Per-domain and aggregate accounting |

Resume an interrupted run with the same paths and `RUN_ID`; do not create a fresh run merely
to hide partial accounting. Promote a completed, reviewed baseline with:

```bash
make promote-run RUN_ID=<run_id>
```

## Local Status Tables

```bash
python scripts/reporting/site_status_report.py --summary
python scripts/reporting/metadata_quality_report.py --warn-only
```

The first joins seeds, routes, local PDFs, baselines, and the latest run. The second measures
metadata coverage. Date and preserve commands when using either report in analysis.

For reproducible public registry counts, use `make gazetteer`; those counts intentionally
exclude local PDFs and parse outputs.

## Document Processing

Install the optional parser dependencies:

```bash
python -m pip install -e '.[pdf_footnotes]'
```

Apply article-oriented QC, then parse:

```bash
make qc-quarantine PDF_ROOT=artifacts/pdfs QUARANTINE_ROOT=artifacts/quarantine

python scripts/processing/extract_text_jsonl.py \
  --pdf-root artifacts/pdfs \
  --respect-qc-exclusions true

make extract-footnotes \
  PDF_ROOT=artifacts/pdfs \
  FEATURES=legal \
  OCR_MODE=off
```

Start with `OCR_MODE=off` to measure the native path. Route image-only and unreliable
documents through an explicit OCR queue; do not silently mix changing OCR backends into a
benchmark denominator.

GPU-backed OlmOCR commands and endpoint variables are exposed by `make help` and the
[script catalog](../scripts/README.md#document-processing). Confirm endpoint health and use a
small sample before launching shards.

## Failure Response

| Signal | Response |
|---|---|
| Repeated `403` / WAF failures | Stop adding traffic, preserve errors, wait for reputation recovery |
| Authentication or paywall | Mark/defer the target; do not attempt circumvention |
| No PDFs discovered | Inspect seed scope, routing, and live HTML before changing shared adapters |
| Duplicate downloads | Preserve records and diagnose canonical URL/hash behavior |
| Parser empty on long PDF | Check scan/text-layer status and route to OCR review |
| Invalid note ordinality | Preserve the sidecar and add a minimal regression fixture before tuning |
| Disk or process interruption | Resume the same run/shard using recorded state |

Digital Commons hosts can share IP-level reputation. Run one conservative bepress batch per
egress IP, stagger new targets, and allow cooldown after a cascade. Non-bepress concurrency
does not make bepress throttling safe.

## Closeout

1. Confirm the process exited and no required job remains running.
2. Inspect run stats and error denominators, not only successful PDF count.
3. Record the commit, command, run ID, target set, and output paths.
4. Run local status/quality reports.
5. Promote only a completed and reviewed baseline.
6. Keep PDFs, caches, browser state, and parse artifacts out of Git.

See [Operator playbook](OPERATOR_PLAYBOOK.md) for the decision checklist and
[Data and release policy](DATA_AND_RELEASE_POLICY.md) before exporting results.
