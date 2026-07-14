# Inventory

Offprint exposes two inventories with different evidentiary meanings.

## Journal Universe

The [searchable journal catalog](generated/JOURNAL_CATALOG.md) contains every row in
`data/registry/lawjournals.csv`. It shows:

- journal name and best-known host;
- normalized platform family;
- registry lifecycle status; and
- a direct link to the crawl configuration when one exists.

The catalog is generated from tracked inputs and can be reproduced from a clean checkout:

```bash
make gazetteer
make gazetteer-check
```

Use browser find to search the catalog. Use the CSV for filtering or joining.

## Downloaded Holdings

The [holdings snapshot](generated/HOLDINGS_BY_JOURNAL.md) aggregates the operator's local
`records.jsonl` files. The current snapshot contains:

| Measure | Value |
|---|---:|
| Deduplicated successful PDF records | 90,873 |
| Journal or inferred collection groups | 327 |
| Records with a title | 90,196 |
| Latest recorded retrieval date | 2026-06-11 |

These are successful PDF records, not automatically 90,873 scholarly articles. Some may be
frontmatter, issue compilations, administrative documents, or incorrectly titled source
material. Document policy and QC establish the article-qualified denominator later.

Generate the report for your own artifact directory:

```bash
make holdings
```

Outputs:

| Output | Contents |
|---|---|
| `artifacts/reports/HOLDINGS_BY_JOURNAL.md` | Per-journal counts, path presence, titles, years, hosts |
| `artifacts/reports/article_holdings.csv` | One row per deduplicated record with title, author, year, URL, path, and hash |

The CSV is the direct answer to “which articles do I have?” for the current machine. Runtime
records and PDFs are gitignored, so it is generated locally rather than committed.

## Stages and Claims

| Stage | Evidence | Permitted description |
|---|---|---|
| Known | Registry row | Offprint knows about this journal |
| Configured | Sitemap JSON | Offprint has a crawl entry point |
| Routed | Adapter resolution | Offprint has selected discovery logic |
| Recorded download | Successful run record and PDF hash | The pipeline downloaded this PDF |
| Present | File exists at the recorded path | The PDF remains at that local path |
| Article-qualified | Document policy/QC inclusion | The PDF is treated as an article candidate |
| Parsed | Text or footnote sidecar | A named parser produced an output |
| Quality-gated | Evaluation result | The output met a stated quality criterion |

Do not substitute one stage for another in reporting. In particular, the journal catalog does
not establish downloads, and a successful PDF download does not establish article status.

## Journal Inference

Holdings records do not always contain a clean journal field. The report therefore:

1. extracts repository context from query parameters, set specs, and URL paths;
2. matches context and host against registry journal URLs;
3. uses a journal name only when the match is unique; and
4. retains an explicit `context (host)` label when assignment remains ambiguous.

This favors visible uncertainty over silently assigning PDFs from a multi-journal repository
to the wrong publication.

See [Gazetteer and Coverage](GAZETTEER.md) for registry provenance and
[Data and Release Policy](DATA_AND_RELEASE_POLICY.md) before publishing derived inventories.
