.PHONY: help production-help production production-resume production-monitored production-monitored-resume production-delta production-retry production-overnight production-overnight-resume production-dc pull pull-siu qc-quarantine extract-footnotes extract-footnotes-overnight extract-footnotes-olmocr-dual diagnose-footnotes evaluate-footnotes adapter-policy-check repo-layout-check quality-check critical-path-tests promote-run site-status metadata-quality-report

PY ?= python3
SITEMAPS_DIR ?= offprint/sitemaps
OUT_DIR ?= artifacts/pdfs
MANIFEST_DIR ?= artifacts/runs
EXPORT_DIR ?= artifacts/exports
MAX_WORKERS ?= 40
MAX_SEEDS_PER_DOMAIN ?= 3
MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN ?= 3
MIN_DELAY ?= 0.5
MAX_DELAY ?= 1.5
CACHE_DIR ?= artifacts/cache/http
CACHE_TTL_HOURS ?= 24
CACHE_MAX_BYTES ?= 2147483648
WELL_COVERED_PDF_THRESHOLD ?= 250
SKIP_DC_SITES ?= 1
DC_ROUND_ROBIN_DOWNLOADS ?= 1
DC_ROUND_ROBIN_STRICT_FIRST_PASS ?= 1
DC_ROUND_ROBIN_REVISIT_INTERVAL_SECONDS ?= 90
RETRY_MAX_RETRIES ?= 3
VERBOSE_PDF_LOG ?= 1
RUN_ID ?=
BASE_RUN_ID ?=
SKIP_RETRY_PASS ?=
LINKS_ONLY ?=
PIPELINE_EXTRA_ARGS ?=
PDF_ROOT ?= artifacts/pdfs
QUARANTINE_ROOT ?= artifacts/quarantine
FEATURES ?= legal
OCR_MODE ?= fallback
WORKERS ?= 4
OCR_WORKERS ?= 2
QC_MANIFEST ?=
FOOTNOTE_GOLD ?= artifacts/runs/footnote_gold.json
OLMOCR_HOST ?= 127.0.0.1
OLMOCR_PORT_A ?= 8080
OLMOCR_PORT_B ?= 8081
OLMOCR_MODEL ?= allenai/olmOCR-2-7B-1025-FP8
OLMOCR_TIMEOUT_SECONDS ?= 900
OVERNIGHT_DURATION ?= 8h
OVERNIGHT_MAX_WORKERS ?= 8
OVERNIGHT_MAX_SEEDS_PER_DOMAIN ?= 1
OVERNIGHT_MIN_DELAY ?= 1.5
OVERNIGHT_MAX_DELAY ?= 4.0
OVERNIGHT_MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN ?= 2
OVERNIGHT_RETRY_MAX_RETRIES ?= 1
MAX_BROWSERS ?= 4
OVERNIGHT_EXTRA_ARGS ?= --dc-robots-enforce --dc-min-domain-delay-ms 1500 --dc-max-domain-delay-ms 4000 --dc-waf-fail-threshold 2 --dc-waf-cooldown-seconds 3600

PIPELINE_COMMON_ARGS = \
	--sitemaps-dir $(SITEMAPS_DIR) \
	--out-dir $(OUT_DIR) \
	--manifest-dir $(MANIFEST_DIR) \
	--export-dir $(EXPORT_DIR) \
	--max-workers $(MAX_WORKERS) \
	--max-seeds-per-domain $(MAX_SEEDS_PER_DOMAIN) \
	--max-consecutive-seed-failures-per-domain $(MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN) \
	--min-delay $(MIN_DELAY) \
	--max-delay $(MAX_DELAY) \
	--cache-dir $(CACHE_DIR) \
	--cache-ttl-hours $(CACHE_TTL_HOURS) \
	--cache-max-bytes $(CACHE_MAX_BYTES) \
	--well-covered-pdf-threshold $(WELL_COVERED_PDF_THRESHOLD) \
	$(if $(filter 1 true TRUE yes YES,$(SKIP_DC_SITES)),--skip-dc-sites,--no-skip-dc-sites) \
	$(if $(filter 1 true TRUE yes YES,$(DC_ROUND_ROBIN_DOWNLOADS)),--dc-round-robin-downloads,--no-dc-round-robin-downloads) \
	$(if $(filter 1 true TRUE yes YES,$(DC_ROUND_ROBIN_STRICT_FIRST_PASS)),--dc-round-robin-strict-first-pass,--no-dc-round-robin-strict-first-pass) \
	--dc-round-robin-revisit-interval-seconds $(DC_ROUND_ROBIN_REVISIT_INTERVAL_SECONDS) \
	--retry-max-retries $(RETRY_MAX_RETRIES)

help:
	@echo "make production-help - canonical production command usage"
	@echo "make production      - canonical full production pipeline run"
	@echo "make production-resume RUN_ID=<run_id> - resume interrupted pipeline run"
	@echo "make production-monitored - headed operator-monitored run (serial + manual captcha/auth intervention)"
	@echo "make production-monitored-resume RUN_ID=<run_id> - resume interrupted operator-monitored run"
	@echo "make production-delta BASE_RUN_ID=<run_id> - run delta pipeline mode (uses golden run if BASE_RUN_ID omitted)"
	@echo "make promote-run RUN_ID=<run_id> - promote a completed run as the golden baseline"
	@echo "make production-retry RUN_ID=<run_id> - retry failures for an existing run"
	@echo "make production-overnight - conservative unattended 8h production run + log"
	@echo "make production-overnight-resume RUN_ID=<run_id> - conservative unattended resume + log"
	@echo "make qc-quarantine   - high-precision QC quarantine run for PDFs"
	@echo "make extract-footnotes - extract footnotes/endnotes with optional QC exclusions"
	@echo "make extract-footnotes-olmocr-dual - dual-shard, dual-endpoint OlmOCR extraction (always OCR)"
	@echo "make evaluate-footnotes - evaluate extraction against a gold set"
	@echo "make site-status     - generate site status report (CSV + terminal summary)"
	@echo "make metadata-quality-report - title/author/vol/date coverage per domain (exits 1 if gaps found)"
	@echo "make production-dc   - run only Digital Commons sitemaps (polite DC settings, new sites only)"
	@echo "make repo-layout-check - enforce canonical root/data/docs file layout"
	@echo "make quality-check   - lint + format check + adapter policy + core pytest"
	@echo "make critical-path-tests - focused adapter/seed/orchestrator shared-path tests"

production-help:
	@echo "Canonical production entrypoint:"
	@echo "  make production"
	@echo ""
	@echo "Other canonical commands:"
	@echo "  make production-resume RUN_ID=<run_id>"
	@echo "  make production-monitored"
	@echo "  make production-monitored-resume RUN_ID=<run_id>"
	@echo "  make production-delta BASE_RUN_ID=<run_id>"
	@echo "  # delta resume (explicit): python3 scripts/pipeline/run_pipeline.py --mode delta ... --resume <run_id> --skip-dc-sites"
	@echo "  make production-retry RUN_ID=<run_id>"
	@echo "  make production-overnight"
	@echo "  make production-overnight-resume RUN_ID=<run_id>"
	@echo ""
	@echo "Common override variables:"
	@echo "  SITEMAPS_DIR=$(SITEMAPS_DIR)"
	@echo "  OUT_DIR=$(OUT_DIR)"
	@echo "  MANIFEST_DIR=$(MANIFEST_DIR)"
	@echo "  EXPORT_DIR=$(EXPORT_DIR)"
	@echo "  MAX_WORKERS=$(MAX_WORKERS)"
	@echo "  MAX_SEEDS_PER_DOMAIN=$(MAX_SEEDS_PER_DOMAIN)"
	@echo "  MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN=$(MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN)"
	@echo "  MIN_DELAY=$(MIN_DELAY)"
	@echo "  MAX_DELAY=$(MAX_DELAY)"
	@echo "  RETRY_MAX_RETRIES=$(RETRY_MAX_RETRIES)"
	@echo "  WELL_COVERED_PDF_THRESHOLD=$(WELL_COVERED_PDF_THRESHOLD)"
	@echo "  SKIP_DC_SITES=$(SKIP_DC_SITES)"
	@echo "  DC_ROUND_ROBIN_DOWNLOADS=$(DC_ROUND_ROBIN_DOWNLOADS)"
	@echo "  DC_ROUND_ROBIN_STRICT_FIRST_PASS=$(DC_ROUND_ROBIN_STRICT_FIRST_PASS)"
	@echo "  DC_ROUND_ROBIN_REVISIT_INTERVAL_SECONDS=$(DC_ROUND_ROBIN_REVISIT_INTERVAL_SECONDS)"
	@echo "  VERBOSE_PDF_LOG=$(VERBOSE_PDF_LOG)"
	@echo "  PIPELINE_EXTRA_ARGS=<extra run_pipeline args>"
	@echo "  SKIP_RETRY_PASS=1"
	@echo "  LINKS_ONLY=1"
	@echo "  PIPELINE_EXTRA_ARGS=\"--no-use-playwright\"   # opt out"
	@echo "  PIPELINE_EXTRA_ARGS=\"--playwright-headless\" # opt out"
	@echo "  PIPELINE_EXTRA_ARGS=\"--no-skip-well-covered-seeds\" # re-run already-covered domains"
	@echo "  PIPELINE_EXTRA_ARGS=\"--write-legacy-manifests\" # opt in to legacy domain manifests"

production-overnight:
	@mkdir -p artifacts/logs
	@RUN_TS=$$(date -u +%Y%m%dT%H%M%SZ); \
	LOG_FILE=artifacts/logs/production_overnight_$${RUN_TS}.log; \
	echo "Starting unattended production run for $(OVERNIGHT_DURATION)"; \
	echo "Log file: $$LOG_FILE"; \
	timeout $(OVERNIGHT_DURATION) $(MAKE) production \
		MAX_WORKERS=$(OVERNIGHT_MAX_WORKERS) \
		MAX_SEEDS_PER_DOMAIN=$(OVERNIGHT_MAX_SEEDS_PER_DOMAIN) \
		MIN_DELAY=$(OVERNIGHT_MIN_DELAY) \
		MAX_DELAY=$(OVERNIGHT_MAX_DELAY) \
		MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN=$(OVERNIGHT_MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN) \
		RETRY_MAX_RETRIES=$(OVERNIGHT_RETRY_MAX_RETRIES) \
		SKIP_RETRY_PASS=1 \
		PIPELINE_EXTRA_ARGS="$(OVERNIGHT_EXTRA_ARGS) $(PIPELINE_EXTRA_ARGS)" \
		>"$$LOG_FILE" 2>&1; \
	STATUS=$$?; \
	if [ "$$STATUS" -eq 124 ]; then \
		echo "Overnight window ended after $(OVERNIGHT_DURATION). Resume with: make production-resume RUN_ID=<run_id>"; \
		echo "See log: $$LOG_FILE"; \
		exit 0; \
	fi; \
	echo "Run exited with status $$STATUS. See log: $$LOG_FILE"; \
	exit "$$STATUS"

production-overnight-resume:
	@if [ -z "$(RUN_ID)" ]; then echo "RUN_ID is required (e.g., make production-overnight-resume RUN_ID=20260224T010203Z)"; exit 2; fi
	@mkdir -p artifacts/logs
	@RUN_TS=$$(date -u +%Y%m%dT%H%M%SZ); \
	LOG_FILE=artifacts/logs/production_overnight_resume_$(RUN_ID)_$${RUN_TS}.log; \
	echo "Starting unattended resume for run_id=$(RUN_ID) for $(OVERNIGHT_DURATION)"; \
	echo "Log file: $$LOG_FILE"; \
	timeout $(OVERNIGHT_DURATION) $(MAKE) production-resume \
		RUN_ID=$(RUN_ID) \
		MAX_WORKERS=$(OVERNIGHT_MAX_WORKERS) \
		MAX_SEEDS_PER_DOMAIN=$(OVERNIGHT_MAX_SEEDS_PER_DOMAIN) \
		MIN_DELAY=$(OVERNIGHT_MIN_DELAY) \
		MAX_DELAY=$(OVERNIGHT_MAX_DELAY) \
		MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN=$(OVERNIGHT_MAX_CONSECUTIVE_SEED_FAILURES_PER_DOMAIN) \
		RETRY_MAX_RETRIES=$(OVERNIGHT_RETRY_MAX_RETRIES) \
		SKIP_RETRY_PASS=1 \
		PIPELINE_EXTRA_ARGS="$(OVERNIGHT_EXTRA_ARGS) $(PIPELINE_EXTRA_ARGS)" \
		>"$$LOG_FILE" 2>&1; \
	STATUS=$$?; \
	if [ "$$STATUS" -eq 124 ]; then \
		echo "Overnight window ended after $(OVERNIGHT_DURATION). Resume again with: make production-overnight-resume RUN_ID=$(RUN_ID)"; \
		echo "See log: $$LOG_FILE"; \
		exit 0; \
	fi; \
	echo "Resume run exited with status $$STATUS. See log: $$LOG_FILE"; \
	exit "$$STATUS"

pull:
	$(PY) -m offprint.orchestrator \
		--sitemaps-dir $(SITEMAPS_DIR) \
		--out $(OUT_DIR) \
		--manifest $(MANIFEST_DIR)

production:
	LRS_VERBOSE_PDF_LOG=$(VERBOSE_PDF_LOG) LRS_MAX_BROWSERS=$(MAX_BROWSERS) $(PY) scripts/pipeline/run_pipeline.py \
		--mode full \
		$(PIPELINE_COMMON_ARGS) \
		$(if $(RUN_ID),--run-id $(RUN_ID),) \
		$(if $(SKIP_RETRY_PASS),--skip-retry-pass,) \
		$(if $(LINKS_ONLY),--links-only,) \
		--use-playwright \
		--playwright-headed \
		$(PIPELINE_EXTRA_ARGS)

production-resume:
	@if [ -z "$(RUN_ID)" ]; then echo "RUN_ID is required (e.g., make production-resume RUN_ID=20260224T010203Z)"; exit 2; fi
	LRS_VERBOSE_PDF_LOG=$(VERBOSE_PDF_LOG) LRS_MAX_BROWSERS=$(MAX_BROWSERS) $(PY) scripts/pipeline/run_pipeline.py \
		--mode full \
		$(PIPELINE_COMMON_ARGS) \
		--resume $(RUN_ID) \
		$(if $(SKIP_RETRY_PASS),--skip-retry-pass,) \
		$(if $(LINKS_ONLY),--links-only,) \
		--use-playwright \
		--playwright-headed \
		$(PIPELINE_EXTRA_ARGS)

production-monitored:
	LRS_VERBOSE_PDF_LOG=$(VERBOSE_PDF_LOG) $(PY) scripts/pipeline/run_pipeline.py \
		--mode full \
		$(PIPELINE_COMMON_ARGS) \
		$(if $(RUN_ID),--run-id $(RUN_ID),) \
		$(if $(SKIP_RETRY_PASS),--skip-retry-pass,) \
		$(if $(LINKS_ONLY),--links-only,) \
		--operator-mode \
		--operator-intervention-scope browser_fallback_only \
		--operator-wait-mode prompt_enter \
		--operator-manual-retries 1 \
		--max-workers 1 \
		--max-seeds-per-domain 1 \
		--max-downloads-per-domain 1 \
		--dc-waf-browser-fallback \
		--use-playwright \
		--playwright-headed \
		$(PIPELINE_EXTRA_ARGS)

production-monitored-resume:
	@if [ -z "$(RUN_ID)" ]; then echo "RUN_ID is required (e.g., make production-monitored-resume RUN_ID=20260224T010203Z)"; exit 2; fi
	LRS_VERBOSE_PDF_LOG=$(VERBOSE_PDF_LOG) $(PY) scripts/pipeline/run_pipeline.py \
		--mode full \
		$(PIPELINE_COMMON_ARGS) \
		--resume $(RUN_ID) \
		$(if $(SKIP_RETRY_PASS),--skip-retry-pass,) \
		$(if $(LINKS_ONLY),--links-only,) \
		--operator-mode \
		--operator-intervention-scope browser_fallback_only \
		--operator-wait-mode prompt_enter \
		--operator-manual-retries 1 \
		--max-workers 1 \
		--max-seeds-per-domain 1 \
		--max-downloads-per-domain 1 \
		--dc-waf-browser-fallback \
		--use-playwright \
		--playwright-headed \
		$(PIPELINE_EXTRA_ARGS)

production-delta:
	LRS_VERBOSE_PDF_LOG=$(VERBOSE_PDF_LOG) LRS_MAX_BROWSERS=$(MAX_BROWSERS) $(PY) scripts/pipeline/run_pipeline.py \
		--mode delta \
		$(PIPELINE_COMMON_ARGS) \
		$(if $(BASE_RUN_ID),--base-run-id $(BASE_RUN_ID),) \
		$(if $(RUN_ID),--run-id $(RUN_ID),) \
		$(if $(SKIP_RETRY_PASS),--skip-retry-pass,) \
		$(if $(LINKS_ONLY),--links-only,) \
		--use-playwright \
		--playwright-headed \
		$(PIPELINE_EXTRA_ARGS)

production-retry:
	@if [ -z "$(RUN_ID)" ]; then echo "RUN_ID is required (e.g., make production-retry RUN_ID=20260224T010203Z)"; exit 2; fi
	$(PY) scripts/pipeline/run_pipeline.py \
		--mode retry \
		--manifest-dir $(MANIFEST_DIR) \
		--export-dir $(EXPORT_DIR) \
		--run-id $(RUN_ID) \
		--retry-max-retries $(RETRY_MAX_RETRIES)

pull-siu:
	$(PY) -c "import json; from offprint.orchestrator import run_orchestrator; seeds=json.load(open('offprint/sitemaps/siu-law-journal.json'))['start_urls']; print(run_orchestrator(sitemaps_dir=None, out_dir='$(OUT_DIR)', manifest_dir='$(MANIFEST_DIR)', max_depth=0, seeds_override=seeds))"

qc-quarantine:
	$(PY) scripts/processing/qc_quarantine_pdfs.py \
		--pdf-root $(PDF_ROOT) \
		--quarantine-root $(QUARANTINE_ROOT) \
		--dry-run false

extract-footnotes:
	$(PY) scripts/processing/extract_footnotes.py \
		--pdf-root $(PDF_ROOT) \
		--features $(FEATURES) \
		--workers $(WORKERS) \
		--ocr-workers $(OCR_WORKERS) \
		--ocr-mode $(OCR_MODE) \
		--respect-qc-exclusions true \
		$(if $(QC_MANIFEST),--qc-exclusion-manifest $(QC_MANIFEST),)

extract-footnotes-overnight:
	@bash scripts/processing/run_overnight_footnotes.sh

extract-footnotes-olmocr-dual:
	$(PY) scripts/processing/run_olmocr_dual_gpu.py \
		--pdf-root $(PDF_ROOT) \
		--features $(FEATURES) \
		--workers $(WORKERS) \
		--classifier-workers $(WORKERS) \
		--ocr-workers $(OCR_WORKERS) \
		--text-parser-mode footnote_optimized \
		--doc-policy article_only \
		--host $(OLMOCR_HOST) \
		--port-a $(OLMOCR_PORT_A) \
		--port-b $(OLMOCR_PORT_B) \
		--model $(OLMOCR_MODEL) \
		--olmocr-timeout-seconds $(OLMOCR_TIMEOUT_SECONDS) \
		--respect-qc-exclusions true \
		$(if $(QC_MANIFEST),--qc-exclusion-manifest $(QC_MANIFEST),)

diagnose-footnotes:
	$(PY) scripts/quality/diagnose_footnote_corpus.py \
		--pdf-root $(PDF_ROOT) \
		$(if $(DIAG_REPORT),--report-out $(DIAG_REPORT),)

evaluate-footnotes:
	$(PY) scripts/quality/evaluate_footnotes.py --gold $(FOOTNOTE_GOLD)

adapter-policy-check:
	$(PY) scripts/quality/check_no_generic_active_seeds.py --sitemaps-dir $(SITEMAPS_DIR)

repo-layout-check:
	$(PY) scripts/quality/check_repo_layout.py --repo-root .

quality-check:
	ruff check offprint scripts
	black --check offprint scripts
	$(MAKE) adapter-policy-check
	$(MAKE) repo-layout-check

promote-run:
	@if [ -z "$(RUN_ID)" ]; then echo "RUN_ID is required (e.g., make promote-run RUN_ID=20260224T010203Z)"; exit 2; fi
	$(PY) scripts/pipeline/promote_run.py \
		--run-id $(RUN_ID) \
		--manifest-dir $(MANIFEST_DIR)

site-status:
	$(PY) scripts/reporting/site_status_report.py \
		--sitemaps-dir $(SITEMAPS_DIR) \
		--pdf-root $(PDF_ROOT) \
		--runs-dir $(MANIFEST_DIR)

# Metadata coverage report — shows title/author/volume/date % per domain
# Exits non-zero if any domain has author<50% or volume<50% (use WARN_ONLY=1 to just print)
METADATA_QUALITY_RUN_ID ?=
METADATA_QUALITY_MIN_RECORDS ?= 20
METADATA_QUALITY_PLATFORM ?=
metadata-quality-report:
	$(PY) scripts/reporting/metadata_quality_report.py \
		$(if $(METADATA_QUALITY_RUN_ID),--run-id $(METADATA_QUALITY_RUN_ID),) \
		--min-records $(METADATA_QUALITY_MIN_RECORDS) \
		$(if $(METADATA_QUALITY_PLATFORM),--platform $(METADATA_QUALITY_PLATFORM),) \
		$(if $(filter 1 true TRUE yes YES,$(WARN_ONLY)),--warn-only,)

# DC-only production run — scrapes only Digital Commons sitemaps (polite settings)
# Builds a filtered sitemaps dir from the registry, then runs the pipeline.
DC_SITEMAPS_DIR ?= /tmp/law_review_dc_sitemaps
production-dc:
	@echo "Building DC-only sitemaps directory at $(DC_SITEMAPS_DIR) ..."
	@$(PY) -c "\
import csv, json, re, shutil; \
from pathlib import Path; \
out = Path('$(DC_SITEMAPS_DIR)'); \
[shutil.rmtree(out, ignore_errors=True), out.mkdir(parents=True, exist_ok=True)]; \
DC = {'digitalcommons','digital_commons','digital commons','bepress_digital_commons'}; \
copied = 0; \
rows = list(csv.DictReader(open('data/registry/lawjournals.csv'))); \
seen = set(); \
[None for row in rows \
 if row.get('platform','').strip().lower() in DC \
 and row.get('sitemap_file','').strip() \
 and row.get('status','').strip() == 'active' \
 and row['sitemap_file'] not in seen \
 and seen.add(row['sitemap_file']) is None \
 and Path('offprint/sitemaps/' + row['sitemap_file']).exists() \
 and shutil.copy('offprint/sitemaps/' + row['sitemap_file'], out / row['sitemap_file']) is not None]; \
print(f'Copied {len(seen)} DC sitemaps to {out}')"
	@echo "Starting DC-only pipeline run ..."
	$(MAKE) production \
		SITEMAPS_DIR=$(DC_SITEMAPS_DIR) \
		SKIP_DC_SITES=0 \
		DC_ROUND_ROBIN_DOWNLOADS=1 \
		DC_ROUND_ROBIN_STRICT_FIRST_PASS=1 \
		DC_ROUND_ROBIN_REVISIT_INTERVAL_SECONDS=90 \
		MAX_WORKERS=20 \
		MIN_DELAY=1.5 \
		MAX_DELAY=4.0 \
		WELL_COVERED_PDF_THRESHOLD=50 \
		PIPELINE_EXTRA_ARGS="--dc-robots-enforce --dc-waf-fail-threshold 2 --dc-waf-cooldown-seconds 3600 $(PIPELINE_EXTRA_ARGS)"

critical-path-tests:
	pytest -q tests/
