# Operator Playbook

Single-page reference for picking up where the last session left off. For detailed flag documentation see [OPERATIONS.md](OPERATIONS.md).

---

## 1. Where Are We?

```bash
# Current golden baseline
cat artifacts/runs/golden_run.json | python -m json.tool

# How many PDFs, domains, sitemaps?
python scripts/site_status_report.py              # full CSV + terminal summary
python scripts/site_status_report.py --summary    # terminal summary only

# What happened in the last run?
ls -lt artifacts/runs/ | head -5
cat artifacts/runs/<run_id>/manifest.json
```

---

## 2. Run Lifecycle

Runs are named `<YYYYMMDD>T<HHMMSS>Z`. Each produces four artifacts in `artifacts/runs/<run_id>/`:

| File | Purpose |
|------|---------|
| `manifest.json` | Config snapshot, timing, summary stats |
| `records.jsonl` | Discovered/downloaded records with full provenance |
| `errors.jsonl` | Failures with error taxonomy |
| `stats.json` | Per-seed/per-domain/per-journal metrics |

**Golden run pointer chain:**
- `artifacts/runs/golden_run.json` → authoritative baseline (set by `make promote-run`)
- `artifacts/runs/domain_baselines.json` → per-domain PDF high-water marks across promoted runs
- Delta mode compares against the golden run automatically

### Decision Table

| Situation | Command |
|-----------|---------|
| First run / fresh start | `make production` |
| Interrupted run | `make production-resume RUN_ID=<id>` |
| New sites added since last run | `make production-delta` |
| Retry failures from a run | `make production-retry RUN_ID=<id>` |
| Promote a successful run | `make promote-run RUN_ID=<id>` |
| Check what needs work | `python scripts/site_status_report.py` |

---

## 3. Expanding Coverage — The Onboarding Funnel

### Step by step

1. **Check status** — identify `todo_adapter` sites:
   ```bash
   python scripts/site_status_report.py --summary
   ```

2. **Fingerprint** a site to detect its platform:
   ```bash
   python scripts/fingerprint_site.py https://example.edu/law-review/
   ```

3. **Auto-onboard** (dry-run first, then with smoke test):
   ```bash
   python scripts/auto_onboard_site.py https://example.edu/law-review/ --dry-run --smoke-test
   python scripts/auto_onboard_site.py https://example.edu/law-review/ --smoke-test
   ```

4. **Batch fingerprint** todo sites:
   ```bash
   python scripts/fingerprint_site.py --batch sitemaps/ --status todo_adapter --max-workers 4
   ```

5. **Run** to download PDFs:
   ```bash
   make production          # or make production-delta
   ```

6. **Promote** the run:
   ```bash
   make promote-run RUN_ID=<id>
   ```

7. **Verify** new PDFs landed:
   ```bash
   python scripts/site_status_report.py --summary
   ```

---

## 4. Footnote Extraction

Recommended path: dual-endpoint OlmOCR (`ocr_mode=always`) with deterministic sharding.

```bash
# Start one vLLM server per GPU (separate terminals/screen panes)
CUDA_VISIBLE_DEVICES=0 .venv/bin/vllm serve allenai/olmOCR-2-7B-1025-FP8 \
  --host 127.0.0.1 --port 8080 --tensor-parallel-size 1 --gpu-memory-utilization 0.85 --max-model-len 16384
CUDA_VISIBLE_DEVICES=1 .venv/bin/vllm serve allenai/olmOCR-2-7B-1025-FP8 \
  --host 127.0.0.1 --port 8081 --tensor-parallel-size 1 --gpu-memory-utilization 0.85 --max-model-len 16384

# Run extraction (dual shard)
make extract-footnotes-olmocr-dual

# Single-process fallback path (if needed)
make extract-footnotes OCR_MODE=fallback

# Evaluate against gold set
make evaluate-footnotes

# Spot-check a sample
python scripts/footnote_sample_audit.py \
  --sidecar-root artifacts/pdfs \
  --pdf-root artifacts/pdfs \
  --sample-size 20
```

Fallback behavior in `ocr_mode=always`:
- OlmOCR is attempted first per PDF.
- If OlmOCR fails/timeouts, native parser fallback runs for that PDF and emits:
  - `olmocr_primary_failed`
  - `native_fallback_after_ocr_failure`

---

## 5. Quick-Reference Cheat Sheet

### Quality & Testing
| Command | Purpose |
|---------|---------|
| `make quality-check` | Full gate: lint + format + policy + tests |
| `pytest -q` | Run all tests |
| `make critical-path-tests` | Focused adapter/seed/orchestrator tests |

### Production Runs
| Command | Purpose |
|---------|---------|
| `make production` | Full production run |
| `make production-resume RUN_ID=<id>` | Resume interrupted run |
| `make production-delta` | Only newer articles vs golden baseline |
| `make production-retry RUN_ID=<id>` | Retry failures from existing run |
| `make production-monitored` | Serial, headed browser, manual captcha |

### Reporting & Status
| Command | Purpose |
|---------|---------|
| `python scripts/site_status_report.py` | Full CSV report + summary |
| `python scripts/site_status_report.py --summary` | Terminal summary only |
| `cat artifacts/runs/golden_run.json` | Current golden baseline |

### Coverage Expansion
| Command | Purpose |
|---------|---------|
| `python scripts/fingerprint_site.py <url>` | Detect site platform |
| `python scripts/auto_onboard_site.py <url>` | Create seed + register adapter |
| `python scripts/migrate_sitemaps_format.py --apply` | Migrate legacy sitemaps to new format |
| `make promote-run RUN_ID=<id>` | Promote run as golden baseline |

### Post-Processing
| Command | Purpose |
|---------|---------|
| `python scripts/extract_pdf_metadata.py --run-id <id>` | Enrich metadata from PDF text |
| `make extract-footnotes` | Extract footnotes from PDFs |
| `python scripts/smoke_one_pdf_per_site.py` | One PDF per site smoke test |

### Useful Overrides
| Override | Effect |
|----------|--------|
| `MAX_WORKERS=120` | Parallel download threads |
| `MIN_DELAY=1.5 MAX_DELAY=4.0` | Request delay range |
| `SKIP_RETRY_PASS=1` | Skip automatic retry pass |
| `LINKS_ONLY=1` | Discovery only, no downloads |
| `PIPELINE_EXTRA_ARGS="--no-use-playwright"` | Disable Playwright |
