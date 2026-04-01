# Operations Guide

## Primary Commands

Production run:
```bash
make production
```

Operator-monitored headed run (manual captcha/auth handoff, serial scheduling):
```bash
make production-monitored
```

Default production scheduling now skips well-covered domains (>= `250` PDFs already present under `artifacts/pdfs/<domain>/`). Override with:
```bash
make production PIPELINE_EXTRA_ARGS="--no-skip-well-covered-seeds"
```

Digital Commons policy for baseline production runs:
- DC seeds are skipped by default (`SKIP_DC_SITES=1` / `--skip-dc-sites`).
- Rationale: current DC path has elevated WAF/challenge rates and low throughput relative to non-DC hosts.
- Priority order: complete non-DC coverage first, then run DC in isolated follow-up passes only when explicitly requested.

If you need to include DC again, override explicitly:
```bash
make production SKIP_DC_SITES=0
make production-delta SKIP_DC_SITES=0
```

Production command help:
```bash
make production-help
```

Enable legacy per-domain manifests only when required by downstream consumers:
```bash
make production PIPELINE_EXTRA_ARGS="--write-legacy-manifests"
```

Preflight:
```bash
python scripts/run_preflight.py --sitemaps-dir sitemaps
```

Adapter policy gate:
```bash
make adapter-policy-check
```

Headless smoke (one PDF per target):
```bash
python scripts/smoke_one_pdf_per_site.py \
  --sitemaps-dir sitemaps \
  --out-dir artifacts/smoke/pdfs \
  --report-dir artifacts/runs
```

## Resume and Recovery

Resume pipeline run:
```bash
make production-resume RUN_ID=<RUN_ID>
```

Resume operator-monitored run:
```bash
make production-monitored-resume RUN_ID=<RUN_ID>
```

Operator interruption behavior:
- Press `Ctrl+C` to pause a monitored run.
- The pipeline prints a deterministic resume command.
- Prefer resume over rerun to avoid reprocessing completed seeds.

Run pipeline delta mode:
```bash
make production-delta SKIP_DC_SITES=1 BASE_RUN_ID=<RUN_ID>
```

Resume interrupted delta run (recommended, non-DC priority):
```bash
python3 scripts/run_pipeline.py \
  --mode delta \
  --sitemaps-dir sitemaps \
  --out-dir artifacts/pdfs \
  --manifest-dir artifacts/runs \
  --export-dir artifacts/exports \
  --resume <RUN_ID> \
  --skip-dc-sites \
  --use-playwright \
  --playwright-headless
```

Important delta note:
- `make production-delta RUN_ID=<RUN_ID>` does not resume; it starts a new run with that ID.
- If `artifacts/runs/<RUN_ID>/` already exists, use the explicit `--resume <RUN_ID>` command above.

Replay retryable errors:
```bash
make production-retry RUN_ID=<RUN_ID>
```

Resume smoke run from report:
```bash
python scripts/smoke_one_pdf_per_site.py --resume-report artifacts/runs/smoke_one_pdf_<STAMP>.json
```

Build prioritized failure queue (fast local scan, no network):
```bash
python scripts/build_failure_queue.py \
  --smoke-globs "artifacts/runs/smoke_one_pdf_*.json,artifacts/smoke/**/smoke_one_pdf_*.json,artifacts/smoke_patch/**/smoke_one_pdf_*.json" \
  --stats-path artifacts/runs/<RUN_ID>/stats.json \
  --ok-ratio-threshold 0.9 \
  --out-jsonl artifacts/recon/failure_queue.jsonl \
  --out-tsv artifacts/recon/failure_queue_top.tsv \
  --top-n 200
```

Retry non-downloaded records with curl and emit per-domain rate-limit recommendations:
```bash
python scripts/curl_retry_non_downloaded.py \
  --run-id <RUN_ID> \
  --manifest-dir artifacts/runs \
  --out-dir artifacts/pdfs_curl_retry \
  --log-jsonl artifacts/runs/<RUN_ID>/curl_retry_results_<STAMP>.jsonl \
  --state-filter non_downloaded \
  --workers 32 \
  --max-per-domain 1 \
  --domain-min-interval-s 0.5 \
  --respect-retry-after \
  --resume-from-log
```
Outputs:
- attempt log (`--log-jsonl`) with `attempt_index_global` and `attempt_index_domain`
- per-domain rate-limit reports:
  - `artifacts/runs/<RUN_ID>/curl_retry_rate_limit_report_<STAMP>.json`
  - `artifacts/runs/<RUN_ID>/curl_retry_rate_limit_report_<STAMP>.csv`

Fingerprint unresolved backlog hosts with `curl` (save HTML/headers + suggest adapter class):
```bash
python scripts/fingerprint_backlog_hosts.py \
  --backlog-csv data/registry/adapter_backlog.csv \
  --out-csv docs/registry/input/fingerprint_report.csv \
  --out-json artifacts/site_fingerprints/fingerprint_summary.json \
  --playwright-queue artifacts/site_fingerprints/playwright_probe_queue.txt \
  --snapshot-dir artifacts/site_fingerprints \
  --max-workers 64 \
  --max-time 8 \
  --connect-timeout 4
```
Primary outputs:
- `docs/registry/input/fingerprint_report.csv` (all targets with suggested adapter + confidence)
- `artifacts/site_fingerprints/playwright_probe_queue.txt` (WAF/blocked/manual-browser queue)
- `artifacts/site_fingerprints/<host>/` (raw HTML/headers/meta snapshots per probe)
- Superseded exploratory fingerprint reports are kept in `docs/attic/2026-03-02_fingerprint_intermediate/`.

Build consolidated US journal registry slices:
```bash
python scripts/build_us_journal_registry.py \
  --rank-csv data/registry/upstream/LawJournals.csv \
  --wlu-all-csv data/registry/upstream/wlu_all_journals.csv \
  --wlu-diff-csv data/registry/wlu_us_url_diff_and_adapter.csv \
  --fingerprint-csv docs/registry/input/source_fingerprint_wlu_all_1079_urls.csv \
  --out-dir docs/registry
```
Primary outputs:
- `docs/registry/us_journals_full_1564.csv`
- `docs/registry/us_urls_canonical.csv`
- `docs/registry/us_urls_working_confirmed.csv`
- `docs/registry/us_urls_errors_exact.csv`
- `docs/registry/us_urls_need_adapter.csv`
- `docs/registry/registry_summary.json`

Import missing canonical URLs into sitemap TODO inventory (non-active by default):
```bash
python scripts/import_registry_canonical_to_todo_sitemaps.py
# then apply:
python scripts/import_registry_canonical_to_todo_sitemaps.py --apply
```
Primary outputs:
- `sitemaps/registry_*.json` (new non-active sitemap entries)
- `docs/registry/import_canonical_todo_report.csv`
- `docs/registry/import_canonical_todo_summary.json`

Capture full URL evidence corpus (HTML + network traces + next-step links):
```bash
python scripts/capture_url_html_evidence.py \
  --input-csv docs/registry/us_urls_canonical.csv \
  --url-column canonical_url \
  --out-dir artifacts/url_capture \
  --summary-csv docs/registry/input/url_capture_summary.csv \
  --needs-work-csv docs/registry/input/url_capture_needs_work.csv \
  --jsonl docs/registry/input/url_capture_summary.jsonl \
  --max-workers 120 \
  --max-time 12 \
  --connect-timeout 5
```
Primary outputs:
- `artifacts/url_capture/<host>/url_<hash>/body.html`
- `artifacts/url_capture/<host>/url_<hash>/headers.txt`
- `artifacts/url_capture/<host>/url_<hash>/trace.txt`
- `artifacts/url_capture/<host>/url_<hash>/analysis.json`
- `docs/registry/input/url_capture_summary.csv`
- `docs/registry/input/url_capture_needs_work.csv`

## Validation and Quality Gates

Tests and lint:
```bash
ruff check offprint tests scripts
black --check offprint tests scripts
pytest -q
```

Unified local quality gate:
```bash
make quality-check
```

Focused critical-path tests:
```bash
make critical-path-tests
```

Footnote pipeline with high-precision QC:
```bash
python scripts/qc_quarantine_pdfs.py --pdf-root artifacts/pdfs --quarantine-root artifacts/quarantine --dry-run false
python scripts/extract_footnotes.py --pdf-root artifacts/pdfs --features legal --respect-qc-exclusions true
```

Coverage gate:
```bash
python skills/law-review-coverage/scripts/run_full_gate.py --sitemaps-dir sitemaps --manifest artifacts/runs_links_only --out artifacts/pdfs --report artifacts/reports/coverage_report.json
```

Export TODO/paused adapter inventory:
```bash
python scripts/export_adapter_todo.py
```

Probe generic TODO backlog with Playwright evidence (headed default):
```bash
python scripts/probe_generic_todo_backlog.py --limit 50
```
Primary outputs:
- `docs/registry/generic_todo_probe_results.csv`
- `docs/registry/generic_todo_probe_summary.json`
- `artifacts/site_fingerprints/<host>/url_<hash>/...`

Auto-tag unresolved generic-routed seeds (migration helper):
```bash
python scripts/mark_todo_adapter_seeds.py --dry-run
# then apply:
python scripts/mark_todo_adapter_seeds.py --apply
```

## Troubleshooting

Common failure classes:
- `no PDF candidates discovered`: seed likely wrong or parser drift.
- `all candidate downloads failed`: candidates found, download blocked/invalid.
- `WAF_BLOCKED_HEADLESS`: request and headless fallback both blocked.
- `precheck_failed`: HEAD validation mismatch, often host-specific.
- `seed_skipped_site_circuit_breaker`: domain hit consecutive seed-failure threshold; remaining seeds were skipped.
- `todo_adapter_blocked`: active seed has no registered adapter (runtime generic fallback is disabled).

Recommended triage order:
1. Confirm seed URL points to archive/issues page.
2. Confirm sitemap `metadata.status` is correct (`active` vs `todo_adapter`/`paused_*`).
3. Run `make adapter-policy-check` before full runs.
4. Check `docs/registry/adapter_todo.csv` for backlog state and evidence references.
5. Check `attempt_trace` and `failure_details` in smoke report.
6. Check `artifacts/runs/<run_id>/errors.jsonl` and adapter `last_download_meta` fields.
7. Re-run targeted smoke with `--target-file` and smaller `--max-candidates`.

## Runtime Artifact Policy
Do not commit runtime outputs:
- `artifacts/pdfs/`, `artifacts/smoke/`
- `artifacts/quarantine/`
- `artifacts/runs/`, `artifacts/runs_links_only/`
- `artifacts/cache/http/`
- `*.footnotes.json`
