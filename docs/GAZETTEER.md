# Gazetteer and Coverage

## What the Gazetteer Is

`data/registry/lawjournals.csv` is Offprint's public journal gazetteer: one merged table of
known journal titles, hosts, platform hints, external identifiers, provenance, and crawl
configuration state. It is a discovery and planning index, not a claim that every row has
been scraped.

The generated [gazetteer snapshot](generated/GAZETTEER_SNAPSHOT.md) converts this table and
the sitemap directory into readable GitHub tables. The generated
[journal catalog](generated/JOURNAL_CATALOG.md) exposes the individual rows for browsing.

```bash
make gazetteer        # rewrite the committed snapshot
make gazetteer-check  # fail if the snapshot is stale
```

## Coverage Vocabulary

| Term | Evidence required | What it does not establish |
|---|---|---|
| Known | A row exists in `lawjournals.csv` | The URL is current or crawlable |
| Configured | A sitemap JSON names at least one start URL | The site has a working adapter |
| Routed | Adapter registry resolves the target | Discovery currently returns article PDFs |
| Smoke-tested | A dated smoke report records the result | Historical or issue-level completeness |
| Landed | A validated PDF and acquisition record exist locally | That the PDF is a scholarly article |
| Article-qualified | Document policy/QC includes the PDF | Text or footnotes parsed correctly |
| Parsed | A parser sidecar records an output and method | The output passed a research quality gate |
| Released | A versioned export documents scope and rights | Permission to redistribute source PDFs |

These stages should remain separate in analysis. In particular, registry and sitemap totals
are reproducible from Git, while landed and parsed totals depend on local artifacts.

## Registry Schema

| Column | Meaning |
|---|---|
| `journal_name` | Display title selected during source merge |
| `url` | Best-known journal homepage or source URL |
| `host` | Normalized network host when available |
| `platform` | Source platform label; spelling is not fully normalized in the CSV |
| `status` | Registry lifecycle state derived from the winning source |
| `sitemap_file` | Linked JSON configuration in `offprint/sitemaps/` |
| `wlu_mainid`, `wlu_rank` | Washington & Lee identifiers/ranking snapshot fields |
| `in_cilp` | Presence in the CILP source list |
| `source` | Source that supplied the winning merged row |
| `fixed_domain_url` | Curated fallback URL when uniquely mappable |

The build script and upstream provenance are documented in
[`data/registry/README.md`](../data/registry/README.md). Do not edit the derived CSV by hand.

## Sitemap Schema

Sitemaps are JSON configuration records under `offprint/sitemaps/`. Common fields are:

| Field | Meaning |
|---|---|
| `id` | Stable configuration identifier |
| `start_urls` | One or more issue/archive entry points |
| `metadata.journal_name` | Human-readable journal identity |
| `metadata.platform` | Platform evidence used for routing and reporting |
| `metadata.status` | Lifecycle state such as `active`, `todo_adapter`, or `paused_waf` |
| `metadata.status_reason` | Concise explanation for non-active or exceptional state |
| `metadata.status_updated_at` | Date of the status evidence |
| `metadata.status_evidence_ref` | Smoke report, issue, or other evidence pointer |
| `adapter_config` | Narrow per-site tuning that avoids changing a shared adapter |

Legacy files may omit status, journal name, or platform. The loader treats a missing status
as inferred `active`; the generated snapshot reports inferred and explicit values separately.

## Current Tables

The snapshot reports:

- registry rows and unique hosts;
- registry and sitemap lifecycle counts;
- normalized platform families while preserving raw CSV values;
- winning-source provenance counts; and
- missing-field counts for both registry rows and sitemap files.

Normalization is display-only. For example, `digital_commons`, `Digital Commons`, and other
case variants appear as one platform family in the Markdown report. This avoids misleading
visual fragmentation without silently rewriting source metadata.

## Local Coverage

When runtime artifacts are present, generate a machine-specific report with:

```bash
python scripts/reporting/site_status_report.py --summary
```

That report joins sitemap targets, adapter routing, local PDF directories, baselines, and the
latest run. Its counts are operational state and should be dated when cited. They are not
committed as public package facts.

## Known Limitations

- One publication may have multiple historical hosts or sitemap records.
- A host may serve multiple journals, so host counts are not journal counts.
- External source lists have different scopes, update schedules, and title conventions.
- Platform values contain legacy aliases and unknown/custom labels.
- Lifecycle statuses are not yet complete across all legacy sitemap files.
- Registry presence is an intentionally recall-oriented universe; article qualification is a
  later, precision-oriented step.
