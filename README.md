<p align="center">
  <img src="./offprint.png" width="600" alt="Offprint Logo" />
</p>

# Offprint V0.1

*Offprint* *(noun)*: a reprint of an article that originally appeared as part of a larger publication.

This repository catalogs law review journals and gathers downloadable PDFs for personal use.

`875 domains tracked` | `112,264 PDFs landed` | `W&L Top-50: 48/50`

`Seed -> Crawl -> PDF -> Footnotes/Text -> Dataset`

## Start In 10 Seconds

```text
/onboard-journal https://example.edu/law-review/
```

Run this in Claude Code. If you omit the URL, the skill can pick the next unonboarded journal from the registry.

Skill reference: [docs/skills/onboard-journal.md](docs/skills/onboard-journal.md)

## Project Goals

- Expand high-quality coverage across U.S. law review hosts.
- Keep scraping runs reproducible, resumable, and evidence-backed.
- Produce article-grade PDF, text, and footnote outputs for downstream research.
- Prefer conservative filtering over false-positive article inclusion.

## Coverage So Far

As of **March 31, 2026** (`python3 scripts/reporting/site_status_report.py --summary`) and Top-50 evidence as of **March 26, 2026** (`artifacts/top50_coverage_report_20260326_rerun.json`):

| Metric | Value |
|---|---:|
| Total known domains | 875 |
| Total PDFs on disk | 112,264 |
| Domains with adapter mapping | 92 |
| Domains with landed PDFs | 350 |
| Active sitemap domains | 523 |
| Paused domains (`paused_*`) | 255 |
| `todo_adapter` domains | 10 |
| W&L Top-50 landed coverage | 48 / 50 |
| W&L Top-50 non-DC missing | 0 |

## Document Processing

1. Quarantine non-article PDFs (high-precision QC):

```bash
python scripts/qc_quarantine_pdfs.py \
  --pdf-root artifacts/pdfs \
  --quarantine-root artifacts/quarantine \
  --dry-run false
```

2. Extract footnotes and article text:

```bash
python scripts/extract_footnotes.py \
  --pdf-root artifacts/pdfs \
  --features legal \
  --respect-qc-exclusions true

python scripts/extract_text_jsonl.py \
  --pdf-root artifacts/pdfs \
  --respect-qc-exclusions true
```

More detail: [docs/FOOTNOTE_QC_WORKFLOW.md](docs/FOOTNOTE_QC_WORKFLOW.md) and [scripts/README.md](scripts/README.md).

## Footnote Extraction Quality

We use a Dynamic Programming (DP) sequence solver over LiteParse layouts to extract ordinal footnote streams. 

As of **April 25, 2026**, the pipeline achieves high-fidelity extraction on standard text-based PDFs. Benchmarks on a 1k random sample (v3) show:

| Metric | LiteParse Only | Roadmap (w/ OCR) |
|---|---|---|
| **Articles Identified** | 672 / 1000 | 672 / 1000 |
| **Strict-Valid (Honest)** | **84.2%** | **~95% (Est.)** |
| **Valid with Gaps (Honest)** | **87.8%** | **~98% (Est.)** |
| **Empty (Image-only)** | 6.5% | < 1% |
| **Invalid (Garbled/OCR)** | 5.7% | < 2% |

*Honest Denominator:* Only includes documents identified as "articles" by `doc_policy` (excludes mastheads, TOCs, transcripts, and agendas).

### The OCR Frontier
The remaining ~12% of failures are primarily:
1. **Empty (6.5%):** Image-only scans (e.g. old volumes or scanned submissions) with zero text.
2. **Invalid (5.7%):** Existing OCR scans with highly garbled text that LiteParse cannot reliably parse.

**Next Step:** Route these documents through the `olmOCR/vLLM` pipeline to rescue the remaining scanned articles.

## Operational Lessons

### Don't double-stack bepress hosts from one IP

Bepress (Digital Commons) sites enforce rate limits at the **IP** level, not just per-host or per-circuit. The pipeline's per-host polite delay (`--dc-min-domain-delay-ms`) and per-circuit cooldown (`--dc-waf-fail-threshold`/`--dc-waf-cooldown-seconds`) only protect *within* a single host's reputation; they do nothing to limit aggregate request rate from your IP across many bepress hosts.

**What we observed (2026-05-02)**: 4 screens scraping 9 bepress hosts ran cleanly for 18+ hours. Adding a second 4-screen run (4 more bepress hosts) tripped 10 new WAF circuits within 5 minutes — and not just on the new run: previously-stable hosts in the original run also started 403'ing (BYU 29/585 fail, LMU 37/634 fail). The patched headless-Playwright fallback also fails because the WAF fingerprints beyond user-agent. Effective rule of thumb:

- **One ~4-worker bepress run per IP at a time**. If you need more parallelism, use distinct IPs (proxies, separate egress).
- **Stagger when adding new bepress targets** to a running cluster: let the existing run wind down to ~1-2 active hosts before bringing more bepress online.
- **Non-bepress (WordPress / OJS / Squarespace) is safe to stack** — those sites generally don't share an IP rep. We've run a third batch of 117 WP/OJS seeds concurrent with bepress without issue.
- After a WAF cascade, **wait 24h** for IP reputation to clear before resuming bepress work. A cron entry one calendar day out is the cleanest pattern (`scripts/cron/flagship_batch_resume.sh` shows the shape).

The pipeline doesn't yet expose a global "max-concurrent-bepress-hosts" knob — feature opportunity. For now, scale your scraping fleet by *batch* not by *worker count*.

## Disclaimer

This project is for scholarly research workflows. Respect publisher terms and `robots.txt`, apply polite request behavior, and do not redistribute PDFs without verifying rights.

## Full Documentation

- Long-form overview moved from README: [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md)
- Documentation index: [docs/README.md](docs/README.md)
