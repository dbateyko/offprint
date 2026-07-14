# Journal Reconnaissance: Wave 1

> Status: first four dossiers reviewed; remaining targets queued  
> Prioritization date: 2026-07-14  
> Holdings snapshot: successful Offprint records through 2026-06-11  
> Combined catalog snapshot: local `catalog/site_coverage/journal_view.csv` at prioritization time

This is the first implementation queue for the
[Journal Reconnaissance Roadmap](JOURNAL_RECONNAISSANCE_ROADMAP.md). It is an evidence-
collection queue, not permission to run a production crawl. Each target must receive a validated
dossier before its sitemap, adapter, registry status, or acquisition settings are changed.

Current implementation status: the first three dossiers validate. Northwestern and William & Mary
are `metadata_only` because representative Digital Commons PDF delivery returned HTTP 403; William
& Mary's HTML archive nevertheless maps volumes 1-67. U.C. Irvine is `blocked_waf`: its law-school
page delegates reading to eScholarship, whose sampled collection, item, and file routes returned
Cloudflare challenges. No production crawl or collection run has been authorized.

Penn Journal of Business Law now has a fourth validated dossier. Its authoritative Quartex archive
advertises 746 records across volumes 1-28 (1998-2026), but client-populated enumeration and final
PDF bytes remain unverified, so its verdict is `in_progress`. The dead legacy WordPress route and
blocked law-school route are no longer treated as acquisition candidates.

## Selection Rules

The queue joins three different denominators instead of sorting the holdings Markdown directly:

1. W&L rank from `data/registry/lawjournals.csv` as an importance signal.
2. Successful Offprint records and host/slug counters from the holdings snapshot and local scrape
   worklist.
3. Validated cross-source articles from the combined catalog (`scraped`, `donation`, and `AA`).

Counts from multi-journal hosts are never treated as journal coverage without a collection slug or
other journal-specific match. Name variants are resolved explicitly; for example, the combined
catalog's `UC Irvine Law Review` row corresponds to the registry's `U.C. Irvine Law Review`.

## First Assignments

These four targets exercise the main reconnaissance lanes and should be investigated first.

| Order | Journal | W&L rank | Host / collection | Combined evidence | Lane | Why now |
|---:|---|---:|---|---|---|---|
| 1 | Northwestern University Law Review | 18 | `northwesternlawreview.org`; verify DC `/nulr` separately | 373 donation, 0 scraped; 2008–2018 | WordPress plus collection-attribution audit | Best traditional-T14 historical gap; main-site seeds failed 4 and 10 times. |
| 2 | William & Mary Law Review | 36 | `scholarship.law.wm.edu/wmlr` | One scraped record is associated with an implausible domain; effective coverage is unverified | Digital Commons | Near-zero trustworthy coverage and a clean publication-scoped DC target. |
| 3 | U.C. Irvine Law Review | 40 | `law.uci.edu/lawreview`; `escholarship.org/uc/ucilr` | Alias-resolved catalog row has 246 articles, 9 scraped; 2011–2026 | Dual-route / eScholarship | Establish which route supplies the canonical archive and repair identity/scoping before more acquisition. |
| 4 | University of Pennsylvania Journal of Business Law | 61 | `pennlawbusinesslaw.com`; `law.upenn.edu`; Quartex repository | No combined-catalog journal row and zero host/slug holdings | WordPress plus Quartex | Strongest verified zero-coverage target in the first wave. |
| 5 | Yale Journal on Regulation | 29 | `yalejreg.com/print`; `yalejreg.com/bulletin` | No matched normalized holdings; current seeds return Cloudflare 403 | WordPress / access barrier | High-importance gap, but pause until an authorized browser or allowlisted route is available. |

## Historical-Gap Follow-up

| Order | Journal | W&L rank | Host / collection | Combined evidence | Lane | Reconnaissance question |
|---:|---|---:|---|---|---|---|
| 5 | Connecticut Law Review | 56 | `connecticutlawreview.law.uconn.edu` | 30 scraped; 2023–2026 | WordPress/custom | Where is the historical print-edition archive, and why do existing routes resolve to `todo_adapter_blocked`? |
| 6 | Iowa Law Review | 14 | `ilr.law.uiowa.edu` | 587 total, 67 scraped; 2008–2026 | Custom/Drupal | Can the current archive expose pre-2008 volumes, and what replaces the repeatedly blocked adapter path? |
| 7 | Emory Law Journal | 42 | `scholarlycommons.law.emory.edu/elj` | 390 total, 17 scraped; 2008–2018 | Digital Commons | Enumerate `/elj` without counting other Emory collections and confirm the historical extent. |
| 8 | Seton Hall Law Review | 91 | `scholarship.shu.edu/shlr` | 361 donation, 0 scraped; 2008–2018 | Digital Commons | Confirm publication-scoped discovery, metadata completeness, and file accessibility. |
| 9 | Tulane Law Review | 100 | `tulanelawreview.org` | 410 donation, 0 scraped; 2008–2018 | WordPress/Squarespace drift | Identify the canonical platform and post-2018 plus pre-2008 archive routes. |
| 10 | UC Davis Law Review | 23 | `lawreview.law.ucdavis.edu` | 391 donation, 0 scraped; 2008–2018 | Access barrier | Classify the current WAF and look for an allowed canonical repository or archive route; do not bypass controls. |

## Attribution and Authoritative-Site Repair Lane

These are important but are not net-new corpus emergencies.

| Journal | W&L rank | Combined evidence | Repair objective |
|---|---:|---|---|
| University of Pennsylvania Law Review | 6 | 1,769 total, 24 scraped | Map the authoritative print archive and diagnose invalid-PDF results; preserve existing cross-source coverage. |
| California Law Review | 5 | 607 total, 15 scraped | Collapse path-derived holdings labels, document the Squarespace print archive, and recover journal identity. |

## Agent Output Contract

Each target produces one dossier under `data/reference/journal_recon/`. The agent must:

- read and obey `robots.txt` before probing routes;
- inspect only a bounded sample: oldest, middle, and newest issue/layout eras plus at least three
  article pages when safely available;
- record the archive-to-file route graph, field-level provenance, pagination and stop conditions,
  file-delivery mechanics, access evidence, scope exclusions, and an adapter recommendation;
- separate discovery success, landing-page access, and PDF access;
- stop on WAF, login, paywall, CAPTCHA, or an explicit robots prohibition rather than trying to
  bypass it; and
- validate the dossier before changing executable scrape configuration.

The coordinator owns this queue and any shared registry or adapter edits. Journal agents must not
edit `data/registry/lawjournals.csv`, shared adapters, or another journal's dossier concurrently.

## Completion Gate

A row leaves this queue only when its dossier validates and has:

- three dated layout-era samples, or a documented reason fewer eras exist;
- a terminal pagination condition;
- field mappings for title, authors, year/date, volume, issue, abstract, landing URL, and file URL,
  with missing fields explicitly represented;
- a journal-scoped inclusion/exclusion rule;
- separate route statuses for discovery, landing pages, and files; and
- a concrete next action: existing adapter, per-site configuration, new adapter, metadata-only,
  stale route, or an access blocker.
