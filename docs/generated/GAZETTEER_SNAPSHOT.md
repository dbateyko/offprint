# Gazetteer Snapshot

> Generated from `data/registry/lawjournals.csv` and `offprint/sitemaps/*.json`.
> Regenerate with `make gazetteer`; verify freshness with `make gazetteer-check`.

## At a Glance

| Measure | Count |
|---|---:|
| Journal registry rows | 2,600 |
| Unique registry hosts | 1,140 |
| Registry rows linked to a sitemap | 1,850 |
| Sitemap files | 1,958 |
| Sitemap start URLs | 2,353 |
| Unique sitemap hosts | 839 |
| Invalid sitemap files | 0 |

## Registry Status

| Registry Status | Rows |
|---|---:|
| active | 794 |
| no_sitemap | 763 |
| (missing) | 489 |
| paused_waf | 267 |
| paused_login | 79 |
| paused_paywall | 71 |
| todo_adapter | 47 |
| paused_404 | 40 |
| paused_other | 24 |
| active_partial_metadata | 8 |
| ready_for_promotion | 7 |
| paused_unmapped | 5 |
| paused_migrated | 2 |
| blocked | 1 |
| paused_print_only | 1 |
| paused_restricted | 1 |
| todo_dspace_v8 | 1 |

## Sitemap Lifecycle

| Sitemap Lifecycle | Rows |
|---|---:|
| active | 793 |
| active (inferred) | 587 |
| paused_waf | 269 |
| paused_login | 79 |
| paused_paywall | 71 |
| todo_adapter | 47 |
| paused_404 | 42 |
| paused_other | 31 |
| no_sitemap | 13 |
| active_partial_metadata | 8 |
| ready_for_promotion | 7 |
| paused_unmapped | 5 |
| paused_migrated | 2 |
| blocked | 1 |
| paused_print_only | 1 |
| paused_restricted | 1 |
| todo_dspace_v8 | 1 |

## Normalized Platform Family

| Normalized Platform Family | Rows |
|---|---:|
| Unspecified | 764 |
| Unknown / generic | 579 |
| Digital Commons | 528 |
| WordPress | 491 |
| Drupal | 85 |
| OJS | 53 |
| Custom / publisher | 33 |
| Squarespace | 13 |
| Wix | 12 |
| scholastica | 10 |
| DSpace | 6 |
| eScholarship | 4 |
| silverchair | 4 |
| Quartex | 3 |
| pubpub | 2 |
| livewhale | 1 |
| OpenYLS | 1 |
| esploro_via_law_school_listing | 1 |
| Westlaw/Thomson | 1 |
| Ubiquity Journal Platform | 1 |
| akademiai | 1 |
| Joomla | 1 |
| Sitefinity | 1 |
| academic_oup | 1 |
| Joomla/Gantry | 1 |
| quartex | 1 |
| Plone | 1 |
| WUSTL Journals | 1 |

## Registry Row Provenance

| Registry Row Provenance | Rows |
|---|---:|
| sitemap | 1,850 |
| wlu | 690 |
| cilp | 60 |

## Metadata Completeness

| Check | Rows / files |
|---|---:|
| Registry missing `journal_name` | 0 |
| Registry missing `host` | 60 |
| Registry missing `platform` | 764 |
| Registry missing `status` | 489 |
| Registry missing `sitemap_file` | 750 |
| Sitemaps missing `explicit_status` | 587 |
| Sitemaps missing `journal_name` | 108 |
| Sitemaps missing `platform` | 75 |
| Sitemaps missing `start_url` | 3 |

## Interpretation

- A registry row means a journal is known; it does not mean PDFs have been downloaded.
- A sitemap file means Offprint has a crawl seed. Its lifecycle status records readiness
  or the reason it is deferred.
- Missing sitemap status is interpreted by the current loader as `active`; the snapshot
  labels this legacy behavior as `active (inferred)`.
- Platform families above normalize spelling and case for readability. The CSV retains
  the original source value.
- Download and parse totals depend on local, gitignored artifacts and are intentionally
  excluded from this reproducible snapshot.

See [Gazetteer and Coverage](../GAZETTEER.md) for schema, provenance, and caveats.
