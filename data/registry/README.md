# Journal Registry

This directory contains the versioned inputs and build logic for Offprint's public journal
gazetteer. Start with [Gazetteer and coverage](../../docs/GAZETTEER.md) for analytical
semantics and the generated [gazetteer snapshot](../../docs/generated/GAZETTEER_SNAPSHOT.md)
for current counts.

## Files

| Path | Role | Editing policy |
|---|---|---|
| `lawjournals.csv` | Derived master journal table used for discovery and planning | Regenerate; do not hand-edit |
| `build_lawjournals.py` | Merge sitemap metadata and upstream lists | Edit with tests/review |
| `stage_lawjournals.py` | Optional helper for staging confirmed-working counts | Edit with tests/review |
| `upstream/wlu_all_journals.csv` | Washington & Lee journal enumeration snapshot | Replace from source |
| `upstream/LawJournals.csv` | Washington & Lee ranking snapshot | Replace from source |
| `upstream/cilp_journals.csv` | Current Index to Legal Periodicals title snapshot | Replace from source |
| `upstream/lawreviewcommons.html` | Law Review Commons platform snapshot | Replace from source |
| `upstream/adapter_locks.csv` | Historical adapter-stability reference | Update from documented review |
| `upstream/fixed_domains.txt` | Curated fallback-domain notes | Review manually |

## Row Provenance

The `source` column identifies the winning source after the merge:

- `sitemap`: tracked sitemap metadata, preferred because the crawl entry point has been
  curated inside Offprint;
- `wlu`: Washington & Lee enumeration/ranking data; and
- `cilp`: CILP title data.

Other upstream lists can enrich or introduce candidates during the build without remaining
the winning `source` label. The merge priority and transformations are defined by
`build_lawjournals.py`, which is more authoritative than prose when they differ.

## URL Authority

For a row linked to a sitemap, the sitemap `start_urls` are the operational crawl entry
points. Registry `url` values from external lists are discovery hints and may be stale,
redirected, or point to a journal homepage rather than an issue archive.

Never infer successful acquisition from URL or sitemap presence. Use dated smoke/run records
and local PDF validation for that claim.

## Regenerate

```bash
python data/registry/build_lawjournals.py
make gazetteer
make gazetteer-check
```

Review both the CSV diff and generated tables. Large changes in source, platform, status, or
missing-field counts should be explained in the commit.

## Status and Platform Values

Registry lifecycle values describe readiness or deferral (`active`, `todo_adapter`,
`paused_waf`, `no_sitemap`, and related states). Legacy rows may be blank. Platform strings
also retain source spelling and case.

The Markdown snapshot normalizes platform aliases for display while preserving raw CSV data.
It reports missing values explicitly rather than silently treating them as confirmed.

## Policy

- Preserve upstream snapshots and document refresh provenance.
- Keep the derived table reproducible from tracked inputs.
- Do not add downloaded PDFs, corpus text, credentials, or browser state here.
- Treat rankings and external URLs as dated reference data.
- Follow [data and release policy](../../docs/DATA_AND_RELEASE_POLICY.md).
