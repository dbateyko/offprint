<p align="center">
  <img src="offprint.png" width="600" alt="Offprint Logo">
</p>

# Project Overview (Offprint)

Long-form project context moved from `README.md` on March 31, 2026.

An open toolkit for building a comprehensive corpus of U.S. law review articles. Crawls journal websites, downloads PDFs, and extracts structured metadata so researchers can search and analyze legal scholarship at scale.

---

## → Add a journal in one command

```
/onboard-journal https://example.edu/law-review/
```

Run this in [Claude Code](https://claude.ai/code). It explores the site, detects the platform, builds the seed config, smoke-tests it, updates the registry, and opens a PR — no Python setup needed. Or leave the URL blank and it picks the next unonboarded journal from the registry automatically.

Full skill docs: [skills/onboard-journal.md](skills/onboard-journal.md)

---

## Why this exists

Law reviews publish across a patchwork of platforms — WordPress blogs, Digital Commons repositories, Open Journal Systems instances, Drupal sites, and one-off custom setups. There's no single API or database that covers them all. This project handles that complexity with platform-aware adapters that know how to navigate each type of site, find article PDFs, and extract metadata.

**Current scale** (run `make site-status` for live numbers, `python data/registry/stage_lawjournals.py` to refresh):

| Metric | Count |
|--------|-------|
| Journals in registry | 1,420 |
| — with known URL / domain | 1,106 |
| — with active sitemap (ready to scrape) | 728 |
| — confirmed working (20+ PDFs downloaded) | 101 |
| Still need sitemaps | 398 |
| Pending DC adapter | 28 |
| Paused (WAF / login / paywall / 404) | 266 |
| Total PDFs on disk | 112,000+ |

## Top-50 Coverage (W&L)

As of **March 26, 2026**, coverage for the W&L Top 50 law reviews (ranked by `wlu_rank`) is:

- **48 / 50** with at least one landed PDF under `artifacts/`
- **48 / 48** when excluding **2 clearly Digital Commons journals**

Evidence report: `artifacts/top50_coverage_report_20260326_rerun.json`

Current Top-50 missing journals:
- Non-DC: none
- DC-excluded: Vanderbilt Law Review, Washington Law Review

## How it works

```
data/registry/lawjournals.csv    Master journal list — 1,421 journals from sitemaps + W&L + CILP
         |
         v
sitemaps/*.json                  Seed files — one per journal, pointing to the archive page
         |
         v
Adapter routing                  Picks the right scraper based on domain + platform
         |
         v
Discovery phase                  Walks archive → issues → articles, collecting PDF URLs + metadata
         |
         v
Download phase                   Fetches PDFs with retry, validates magic bytes + SHA-256
         |
         v
artifacts/runs/<id>/             Timestamped output: records.jsonl, errors.jsonl, stats.json
```

Each journal has a **seed file** in `sitemaps/` that tells the scraper where to start. The **adapter** for that site's platform handles the rest.

## Quick start

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .[dev]

# Smoke test — download one PDF per site to verify adapters work
python scripts/smoke_one_pdf_per_site.py --sitemaps-dir sitemaps --max-workers 8

# Full production scrape
make production

# Run tests
pytest -q
```

## Contributing

### Add a new journal (recommended path)

Use the `/onboard-journal` Claude Code skill — see the banner at the top of this file.

**What NOT to onboard:** Digital Commons (BePress) sites are handled separately via `DigitalCommonsIssueArticleHopAdapter` and don't need manual seeds. The skill detects and skips them automatically.

### Add a journal with Python scripts (alternative)

```bash
# 1. Detect the platform
python scripts/fingerprint_site.py --url https://example.edu/law-review/

# 2. Generate seed file and registry entry
python scripts/auto_onboard_site.py --url https://example.edu/law-review/

# 3. Smoke test
python scripts/smoke_one_pdf_per_site.py --target-file urls.txt --max-workers 1

# 4. Submit a PR with sitemaps/{slug}.json
```

### Fix a broken scraper

Sites change layouts, add WAFs, or move URLs. If one stopped working:

1. Check `artifacts/runs/<run_id>/errors.jsonl` for the failure type
2. Look at the adapter in `offprint/adapters/`
3. Fix and add a test in `tests/` with an HTML fixture

### Before submitting a PR

```bash
ruff check offprint tests scripts
black --check offprint tests scripts
make repo-layout-check
make adapter-policy-check
pytest -q
```

**Shared adapter policy**: base adapters (`WordPressAcademicBaseAdapter`, `DigitalCommonsIssueArticleHopAdapter`, `OJSAdapter`, etc.) serve many journals. Prefer a site-specific adapter or sitemap config over changing shared code. See [CONTRIBUTING.md](../CONTRIBUTING.md) for the full policy.

## Supported platforms

| Platform | Adapter | Notes |
|----------|---------|-------|
| Digital Commons (BePress) | `DigitalCommonsIssueArticleHopAdapter` | OAI-PMH + sitemaps |
| WordPress | `WordPressAcademicBaseAdapter` | REST API + archive crawl |
| Open Journal Systems (OJS) | `OJSAdapter` | Issue archive pages |
| Drupal / SiteFarm | `DrupalAdapter` | Menu/archive crawl |
| Scholastica | `ScholasticaBaseAdapter` | Issue API traversal |
| Quartex | `QuartexAdapter` | Collection API |
| Squarespace | `SquarespaceAdapter` | Page crawl |
| DSpace | `DSpaceAdapter` | Handle/bitstream URLs |
| Custom / unknown | `GenericAdapter` | BFS link crawler (fallback) |

Site-specific adapters exist for unusual layouts (Harvard JOLT, Yale Law Journal, Penn Law Review, etc.).

## Repository structure

```
offprint/        Core package — orchestrator, adapters, footnote extraction
scripts/            CLI tools — production runs, onboarding, smoke tests, footnotes
tests/              Test suite
sitemaps/           Seed JSON configs (one per journal)
data/registry/      Journal registry — lawjournals.csv, W&L source, CILP source
docs/               Documentation and skills
  skills/           Claude Code skill prompts (onboard-journal, probe-selectors, etc.)
research/           Experimental analysis projects
hf/                 Hugging Face dataset exports
```

See [REPO_LAYOUT.md](REPO_LAYOUT.md) for the full layout guide.

## External corpus imports

- 2026-03-30: Stetson Law Review volume PDFs were imported to `artifacts/pdfs/www.stetson.edu/` from https://github.com/SarthakPattnaik1/Stetson-Law-Review-AI-Assistant/tree/main.

## Post-processing

### Footnote extraction

```bash
python scripts/extract_footnotes.py --ocr-mode off --workers 8
```

Uses the `footnote_optimized` pipeline: `liteparse` primary, then `pdfplumber`, then `pypdf`, followed by ordinality repair and OCR fallback when needed. See `scripts/README.md` for the full guide.

### Sherlock Roadmap (SLURM)

Use deterministic sharding (`--shard-count`, `--shard-index`) to parallelize safely across jobs/nodes.

1. Pilot one shard for 2-4 hours to measure throughput.
2. Choose shard count `N` so each shard fits your walltime budget.
3. Launch a SLURM array (`0..N-1`) with one shard per task.
4. Re-run failed/timeout shards; sidecars make reruns resumable when `--overwrite false`.

Throughput math:

- `docs_per_hour = processed / run_elapsed_hours`
- `hours_for_100k = 100000 / docs_per_hour`
- `hours_with_N_shards = hours_for_100k / N`

Example batch script (CPU-only, liteparse-first, no OCR):

```bash
#!/bin/bash
#SBATCH -J footnote-liteparse
#SBATCH -p normal
#SBATCH --cpus-per-task=16
#SBATCH --mem=96G
#SBATCH -t 24:00:00
#SBATCH --array=0-31
#SBATCH -o logs/%x_%A_%a.out
#SBATCH -e logs/%x_%A_%a.err

set -euo pipefail
export PYTHONUNBUFFERED=1
source .venv/bin/activate

python scripts/extract_footnotes.py \
  --pdf-root artifacts/pdfs \
  --ocr-mode off \
  --text-parser-mode footnote_optimized \
  --workers 16 \
  --classifier-workers 16 \
  --shard-count 32 \
  --shard-index "${SLURM_ARRAY_TASK_ID}" \
  --respect-qc-exclusions true \
  --overwrite false \
  --report-out "artifacts/runs/footnote_extract_shard_${SLURM_ARRAY_TASK_ID}.json"
```

tqdm/progress:

- `extract_footnotes.py` already uses `tqdm` for classify/extract phases.
- Progress bars go to stderr, so they appear in SLURM logs (`-e` file).
- If needed for cleaner logs: `export TQDM_DISABLE=1`.

Environment requirements on Sherlock (typical):

```bash
module purge
module load python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .[dev] responses playwright
```

Check what partitions you can use:

```bash
sh_part
sacctmgr show assoc where user=$USER format=Account,Partition,QOS%30 -p
```

`sh_part` is the Sherlock-specific quickest view of accessible partitions and their limits.

Partition note:

- `service` is for lightweight recurring tasks and is capped (docs list: 2 jobs, 16 cores/user total).
- For large extraction runs, use `normal`/`bigmem` (or your owners partition) and spread shards across many jobs/nodes.

Yes, you can run multiple nodes with 16 CPUs each on Sherlock when your partition/allocation allows it. The `service` partition is the exception because of the per-user cap above.

For a copy-friendly, step-by-step package (SLURM scripts + merge helpers), see
[`sherlock_runpack/`](../sherlock_runpack/README.md).

### Hugging Face dataset

```bash
python scripts/build_hf_dataset.py --run-id <run_id> --ocr-mode off --workers 8
```

Produces `hf/text/fulltext/part-*.parquet` (articles) and `hf/footnotes/footnotes/part-*.parquet` (footnotes with citation mentions).

## Documentation

| Doc | What it covers |
|-----|----------------|
| [README.md](README.md) | Documentation index |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design and adapter pattern |
| [CONTRIBUTOR_START_HERE.md](CONTRIBUTOR_START_HERE.md) | Contributor onboarding (45 min path) |
| [ADAPTER_DEVELOPMENT.md](ADAPTER_DEVELOPMENT.md) | How to write a new adapter |
| [OPERATOR_PLAYBOOK.md](OPERATOR_PLAYBOOK.md) | Production operations |
| [skills/onboard-journal.md](skills/onboard-journal.md) | `/onboard-journal` skill reference |
| [skills/](skills/) | All Claude Code skills |
| [scripts/README.md](../scripts/README.md) | Script index by workflow |

## Acknowledgements

Special thanks to **Yonathan** and the **[lrscraper](https://lrscraper.battleoftheforms.com/)** project for contributing vital signals, platform detection logic, and coverage benchmarks that have helped refine and expand this toolkit.

## Compliance and purpose

This project exists for **scholarly research purposes** — building a searchable corpus of legal scholarship to support academic analysis. It is not a commercial scraping operation.

- Respect `robots.txt` and publisher terms of service
- Polite delays and `429`/`5xx` backoff — we never hammer sites
- Digital Commons sites are rate-limited and WAF-protected; we defer them in baseline runs
- Do not redistribute PDFs without verifying rights
- Keep secrets in environment variables, never in git
