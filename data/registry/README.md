# data/registry

Durable reference datasets for journal discovery, onboarding, and coverage tracking.

## URL authority and freshness

**Sitemap `start_url` values are the most authoritative and up-to-date URLs we have.**
They were discovered via agent-assisted site exploration and smoke-tested to confirm live
PDF availability. W&L and CILP URLs are reference data only — both sources are often
outdated (dead domains, redirects, moved repositories) and should not be trusted over a
sitemap entry. W&L rankings similarly lag; a journal's rank in `LawJournals.csv` reflects
citation data through 2024 but the list itself is not continuously updated.

When a journal has a sitemap entry, use `start_url` as the live crawl entrypoint and
`url` as the canonical homepage. For journals with only W&L or CILP coverage
(`status = no_sitemap`), treat the URL as a starting point for exploration, not a
confirmed live link.

## Source files (do not edit by hand)

All upstream snapshots now live in `data/registry/upstream/`.

| File | Source | Notes |
|------|--------|-------|
| `upstream/wlu_all_journals.csv` | Washington & Lee Law Library | Raw W&L enumeration export. 1,867 journals with ISSN, contact info, full-text URLs, citation cost metrics. URLs often stale. |
| `upstream/LawJournals.csv` | Washington & Lee Law Library | W&L citation ranking export. ~1,565 journals ranked by combined 2020–2024 score. Rankings lag reality. |
| `upstream/cilp_journals.csv` | Current Index to Legal Periodicals (HeinOnline) | 644 journal titles. Source: https://libguides.heinonline.org/c.php?g=1045270&p=7595542 — list coverage changes; verify against live site. |
| `upstream/adapter_locks.csv` | Pipeline-generated reference snapshot | Per-adapter stability snapshot from sampled production reports (not runtime state). |
| `upstream/fixed_domains.txt` | Manual reference notes | Domains with manual URL fixes from prior cleanup cycles. |

## Derived / operational files

| File | Description |
|------|-------------|
| `lawjournals.csv` | **Master journal list.** Unique journals merged from sitemaps + W&L + CILP. Sitemaps win on URL and status. Columns: `journal_name`, `url` (canonical homepage), `host`, `platform`, `status`, `sitemap_file`, `wlu_mainid`, `wlu_rank`, `in_cilp`, `source`, `fixed_domain_url` (host-level fallback URL from `upstream/fixed_domains.txt` when uniquely mappable). |
| `build_lawjournals.py` | Script to regenerate `lawjournals.csv` from current sitemaps + source files. Run after bulk sitemap changes. |
| `stage_lawjournals.py` | Optional helper to refresh `confirmed_working` counts from run stats. |

## Regenerating lawjournals.csv

```bash
python data/registry/build_lawjournals.py
```

Run this after significant sitemap additions or when source files are refreshed from upstream.

## adapter_locks.csv semantics

`upstream/adapter_locks.csv` is a reference snapshot for adapter policy tuning and backlog triage.

- `adapter`: adapter class name.
- `status`: lock state from prior review (`locked`, `candidate`, etc.).
- `successes` / `failures`: observed counts from sampled reports.
- `success_rate`: `successes / (successes + failures)` in the snapshot window.
- `source_reports`: number of run reports included in the snapshot.
- `sample_url`: representative URL used in the review cycle.
- `notes`: free-form review notes.

It is not consumed by the crawler at runtime; treat it as analyst input for follow-up decisions.

## attic/

Archived files superseded by `lawjournals.csv`:
`adapter_backlog.csv`, `all_journals.csv` (was a corrupted CILP+sitemap mix),
`discovered_journals_batch2.csv`, `new_journals_smoke.csv`, `unconfirmed_journals.csv`,
`wlu_new_urls_triage.csv`, `wlu_us_url_diff_and_adapter.csv`, `lawreviewcommons_hosts.jsonl`,
`user_list_extended.txt`, `wlu_all_journals_errors.jsonl`.

## Policy

- Source files: never edit; re-export from upstream when refreshing.
- `lawjournals.csv`: regenerate from script, do not hand-edit.
- Narrative runbooks → `docs/`; runtime artifacts → `artifacts/`.
