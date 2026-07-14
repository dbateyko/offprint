# Data and Release Policy

## Data Classes

| Class | Examples | Git policy | Release posture |
|---|---|---|---|
| Source code and tests | `offprint/`, `scripts/`, `tests/` | Tracked | Public under repository license |
| Public reference metadata | registry CSVs, sitemap JSON, adapter config | Tracked with provenance | May be released with source notes |
| Small evaluation fixtures | synthetic or rights-reviewed test inputs | Tracked when necessary | Document origin and permitted use |
| Acquisition records | run manifests, URLs, hashes, status/error records | Local by default | Review for sensitive fields before release |
| Downloaded documents | article PDFs and page images | Gitignored | Do not redistribute without rights review |
| Derived document data | text, footnotes, OCR, embeddings, labels | Gitignored by default | Release only with scope, method, and rights notes |
| Secrets and browser state | credentials, cookies, profiles, tokens | Never tracked | Never release |

## Repository Boundary

The public repository must remain installable and testable without private corpus holdings.
No documented quickstart may depend on machine-specific paths, donated data, credentials, or
the private data-operations workspace.

Private downstream projects may join Offprint records with donated, licensed, or separately
acquired corpora. They should reference a version or commit of Offprint rather than copying
scraper or parser code into the private repository.

## Provenance Minimum

Any released derived table should record:

- Offprint commit or package version;
- generation command and relevant configuration;
- source registry/sitemap version;
- run or document identifiers sufficient to audit failures;
- inclusion, exclusion, and deduplication rules;
- missingness and quality denominators; and
- rights or redistribution limitations.

Counts should name their unit. Journals, hosts, sitemap files, seeds, candidate URLs, PDFs,
article-qualified documents, and parsed documents are not interchangeable.

## Pull Request Rules

- Do not commit runtime output under `artifacts/`, browser profiles, caches, or local logs.
- Do not add credentials, personal contact data collected incidentally, or access tokens.
- Keep public reference snapshots reasonably sized and document their source and refresh date.
- Use synthetic or minimal rights-reviewed fixtures for regressions whenever possible.
- Remove document text from issue reports unless it is necessary and permitted to share.

## Responsible Acquisition

Review publisher terms and `robots.txt`, identify clients appropriately, respect authentication
and paywalls, throttle requests, honor retry/backoff signals, and stop when a site indicates
that automation is unwelcome. Technical ability to retrieve a resource is not a rights
determination.

See [Gazetteer and coverage](GAZETTEER.md) for public metadata semantics and
[Operations](OPERATIONS.md) for run controls.
