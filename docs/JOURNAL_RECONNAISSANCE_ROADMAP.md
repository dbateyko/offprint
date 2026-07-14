# Journal Reconnaissance and Backfile Acquisition Roadmap

## Purpose

Build a repeatable path from an apparent journal coverage gap to a verified, journal-scoped
collection run. The immediate objective is to understand how priority journal sites expose
volumes, issues, articles, metadata, and files before expanding the PDF holdings.

This roadmap separates four facts that are easy to conflate:

1. a journal is known to the registry;
2. a site exposes discoverable article records;
3. the scraper can route and download a valid PDF with useful metadata; and
4. the local corpus contains article-qualified, correctly attributed documents across the
   journal's publication history.

Only the fourth is coverage. A registry row, an `active` status, a host-level PDF count, or a
successful HTTP response is not a substitute for it.

This is a maintained operational document. Update the first-wave table, milestone checklist,
and baseline date whenever a reconnaissance wave is reviewed or promoted to collection.

## Baseline and Evidence Problem

The initial prioritization uses these local evidence sources:

- `docs/generated/HOLDINGS_BY_JOURNAL.md`, an operational holdings snapshot through
  2026-06-11;
- `../catalog/site_coverage/journal_view.csv`, the cross-source normalized journal view;
- `../catalog/scrape_worklist.csv`, the site-level run and failure history;
- `data/registry/lawjournals.csv`, the tracked journal registry;
- `data/reference/onboarding-selector-pagination-tracker.csv`, the current onboarding
  evidence tracker; and
- `offprint/sitemaps/*.json`, the tracked seed and selector records.

The holdings report cannot be ranked naively. It groups some records under path-derived labels
such as `uploads`, `content`, or a repository publication slug, and it warns that successful
downloads are candidates rather than article-qualified documents. Its local counts are also not
reproducible from a clean Git checkout. The cross-source catalog improves identity resolution,
but source donations may cover only a narrow year range and host-level counts can hide an empty
publication collection. Consequently, a low count is a signal for investigation, not proof of a
gap; a high count is not proof of complete or correctly attributed coverage.

Priority is therefore a scored editorial decision based on:

- importance: W&L rank/citation standing and project priorities;
- normalized article count, not the smallest raw path-label count;
- year-span gaps and source concentration;
- expected backfile size and metadata quality;
- likelihood that one platform solution will unlock several journals; and
- access cost, including WAF, login, paywall, JavaScript, and migration barriers.

Record the inputs and rationale when changing priority. Do not silently reorder the queue from a
new raw count.

## Goals

- Produce a versioned, reviewable dossier for each priority publication.
- Map the complete route from archive or repository collection to volume, issue, article, and
  final file.
- Record observed metadata sources and their reliability: title, authors, abstract, publication
  date/year, volume, issue, pages, DOI, and preferred citation.
- Distinguish direct PDFs from viewers, iframes, redirects, APIs, tokenized links, whole issues,
  front matter, mastheads, and supplements.
- Make publication boundaries explicit on multi-publication repository hosts.
- Convert approved dossiers into the smallest platform or journal adapter change that can pass a
  real one-journal smoke test.
- Expand holdings in controlled batches, validate document identity, and feed the resulting
  coverage evidence back into prioritization.

## Non-Goals

- Bulk downloading during reconnaissance.
- Treating every PDF on a journal or repository host as a scholarly article.
- Circumventing robots exclusions, authentication, paywalls, CAPTCHAs, WAFs, signed-link controls,
  or other access restrictions.
- Replacing the registry, sitemap, tracker, or run-record systems with the dossier.
- Claiming historical completeness from a newest-issue smoke test.
- Repairing journal-name deduplication across the whole corpus as a prerequisite for the first
  wave; uncertain aliases should instead be documented and queued for focused reconciliation.
- Publishing downloaded PDFs or private corpus state in Git.

## Priority Lanes

### Lane A: High-importance custom and WordPress sites

These receive one publication per reconnaissance agent because layouts and historical eras may
vary substantially:

- Northwestern University Law Review;
- University of Pennsylvania Journal of Business Law;
- Connecticut Law Review;
- Iowa Law Review; and
- Tulane Law Review.

### Lane B: Publication-scoped Digital Commons

Use a platform specialist, but maintain one dossier and one collection boundary per publication:

- William & Mary Law Review (`wmlr`);
- Emory Law Journal (`elj`); and
- Seton Hall Law Review (`shlr`).

Do not use the repository root as the unit of work. A host can contain many unrelated collections,
and a host-level PDF count is not evidence for any one journal.

### Lane C: Dual-route and attribution work

U.C. Irvine Law Review requires comparison of its law-school route and the publication-scoped
eScholarship collection. The combined catalog contains an alias-resolved `UC Irvine Law Review`
row with 246 articles (nine scraped) spanning 2011-2026, while the registry uses `U.C. Irvine Law
Review`. The initial task is to prove the collection attribution, reconcile that name variant, and
identify the authoritative metadata/file route.

Pennsylvania Law Review and California Law Review belong in a later authoritative-metadata and
attribution-repair wave. Their combined-source totals indicate substantial holdings, so they
should not displace genuine acquisition gaps merely because the authoritative-site scraper is
thin or holdings are fragmented under path labels.

### Lane D: Access barriers and migrations

UC Davis Law Review begins with a barrier dossier, not repeated scraping. Record the exact working
or blocked route, response class, date, network context, and permitted next action. A WAF finding
is an operational state, not an invitation to evade it.

## First Wave

The ordering below is the initial queue, not a permanent ranking. Counts are cross-source baseline
signals observed during the July 2026 review and must be refreshed before a collection run.

| Order | Journal | Initial evidence | Lane | Reconnaissance question | State |
|---:|---|---|---|---|---|
| 1 | Northwestern University Law Review | 373 donation records limited to 2008-2018; no normalized scraper holdings; repeated main-site seed failures | A | Which historical archive route exposes article-level metadata and files, and where did the layout change? | paused_pdf_access |
| 2 | William & Mary Law Review | One normalized scraped record appears misattributed; publication-scoped Digital Commons route is present but uncollected | B | Can `wmlr` be enumerated completely without crossing into sibling collections? | paused_pdf_access |
| 3 | U.C. Irvine Law Review | Alias-resolved combined row has 246 articles, nine scraped, spanning 2011-2026; two candidate routes remain | C | Which collection/API records belong to `ucilr`, how should the name variant resolve, and is the law-school route useful or stale? | paused_waf |
| 4 | University of Pennsylvania Journal of Business Law | No normalized combined records; Quartex now has one article-quality smoke | A | Which of the custom site, law-school page, and repository is authoritative and complete? | smoke_review |
| 5 | Yale Journal on Regulation | Registry rank 29, no matched normalized holdings, and an existing WordPress seed | D | Can the print and Bulletin scopes be reached with permitted access, and what article/PDF structure is actually exposed? | paused_waf |
| 5 | Connecticut Law Review | 30 normalized records, all from 2023-2026; archive route has repeated adapter blockers | A | How are older print volumes represented, and are their files article-level or issue-level? | queued |
| 6 | Iowa Law Review | 587 combined records begin in 2008; scraper route has repeated adapter blockers | A | Which archive eras and article routes cover the pre-2008 backfile? | queued |
| 7 | Emory Law Journal | 390 combined records, primarily 2008-2018 donations; `elj` route has no run history | B | Can OAI or publication pages enumerate the complete `elj` set and primary files? | queued |
| 8 | Seton Hall Law Review | 361 donation records from 2008-2018; `shlr` route has no run history | B | Can the `shlr` collection be bounded and enumerated independently of sibling publications? | queued |
| 9 | Tulane Law Review | 410 donation records from 2008-2018; only 16 host PDFs appear in scraper work data | A | Does the current site expose the full backfile, or must eras be joined across routes? | queued |
| 10 | UC Davis Law Review | 391 donation records from 2008-2018; journal site is recorded as WAF-blocked | D | Is there a permitted alternate repository or canonical route, and what precisely blocks the current one? | queued |

Allowed states are `queued`, `recon_in_progress`, `recon_review`, `ready_to_implement`,
`implementing`, `smoke_review`, `ready_to_collect`, `collecting`, `reconcile`, `complete`, and
`paused_<reason>`. The dossier is the source for state evidence; this table is the human-readable
queue.

## Multi-Agent Workflow

### Roles

- **Coordinator:** owns prioritization, assigns exactly one dossier owner, prevents duplicate
  probes, reviews evidence, and alone promotes a journal between gates.
- **Publication reconnaissance agent:** explores one publication's routes within a small request
  budget and writes the dossier. It does not modify scraper code or run bulk downloads.
- **Platform specialist:** develops reusable platform knowledge, especially Digital Commons, but
  preserves publication-scoped evidence and output.
- **Implementer:** converts an approved dossier into sitemap, routing, adapter, and fixture changes.
- **Reviewer/operator:** reproduces the smoke result, checks scope and metadata, and authorizes a
  bounded collection run.

One person or agent may fill several roles, but a dossier should not pass review solely on its
author's assertion. Each dossier has one owner at a time. Other agents contribute findings through
the owner to avoid concurrent edits and incompatible conclusions.

### Reconnaissance loop

1. Coordinator freezes the journal identity, aliases, publication slug, candidate routes, and
   request budget.
2. Agent checks redirects, platform signals, `robots.txt`, and access barriers before exploration.
3. Agent samples at minimum the oldest accessible issue, one middle issue, and the newest issue,
   plus at least three representative article pages when article pages exist.
4. Agent records observed selectors/API fields, URL templates, pagination, termination rules,
   historical layout eras, file-delivery behavior, and missingness. Generated/hash selectors are
   not acceptable evidence.
5. Agent tests only enough file responses to identify delivery type and validate content type or
   PDF signature. It does not start a collection run.
6. Agent assigns a reconnaissance verdict and submits the dossier for review.
7. Reviewer reproduces representative paths and either promotes the dossier or records the exact
   evidence gap.

Keep loops short. The onboarding discipline permits at most three improvement iterations per
host before recording a blocker or partial result. Probe politely, in small batches, and stop when
the evidence no longer improves.

## Dossier Contract

Canonical dossiers live at:

`data/reference/journal_recon/<journal-slug>.json`

Use one file per publication, including for Digital Commons. JSON is the canonical machine-readable
record; tracker and sitemap rows are projections used by their existing workflows. Every dossier
must validate against a versioned schema before implementation begins. The first implementation
milestone includes adding that schema and validator.

Required top-level sections:

| Section | Required content |
|---|---|
| `$schema`, `schema_version` | Dossier contract reference and version |
| `journal` | Canonical name, aliases, registry references, canonical URL, and publication path/slug scope |
| `reconnaissance` | Researcher, state, checked timestamp, sampling policy, and timestamped URL evidence |
| `scope` | Journal-scoped inclusion/exclusion patterns and treatment of supplements and whole-issue PDFs |
| `site_map` | Entry points plus the archive -> volume -> issue -> article -> file graph, URL patterns, pagination termination, and historical eras |
| `metadata_fields` | For each field: selector/API key, page level, example, normalization, missingness, confidence, and fallback |
| `file_delivery` | Direct/iframe/viewer/redirect/API/tokenized behavior, request prerequisites, final content checks, file granularity |
| `access` | Robots decision by route, HTTP evidence, rate policy, login/paywall/WAF/JS findings, permitted next action |
| `verdict` | Status, rationale, evidence references, blockers, next action, and non-binding implementation hint |

The metadata inventory covers, when available:

- article title;
- repeated authors in display order;
- abstract;
- publication date and normalized year;
- volume and issue;
- first/last page or page range;
- DOI;
- preferred citation;
- article/record URL; and
- final primary-file URL.

For every field, distinguish observed absence from an untested selector. Confidence is
`high`, `medium`, or `low` and must be justified. Preserve raw examples alongside normalization
rules so later code can be reviewed without refetching the site.

The route graph may be represented as ordered route nodes and transitions. It must answer:

- how enumeration starts;
- how every volume/issue/article is reached;
- whether pagination or resumption tokens are complete;
- how the crawl knows it is done;
- what changes across historical eras; and
- which transition yields the primary article PDF.

Reconnaissance verdicts are:

- `ready`: route, scope, metadata, and file delivery are sufficiently proven for implementation;
- `metadata_only`: records can be enumerated but primary files cannot permissibly be acquired;
- `needs_headless`: public content requires browser execution and a bounded headless design;
- `blocked_waf`, `blocked_login`, or `blocked_paywall`;
- `stale_route`: the registered route has migrated or no longer represents the journal; and
- `incomplete`: more evidence is required, with a specific next probe.

## Delivery Phases and Acceptance Gates

### Phase 0: Normalize and select

- Refresh normalized holdings, year spans, source mix, registry rows, and scrape failure history.
- Reject path-label false positives and investigate suspicious journal-name aliases.
- Freeze publication scope and first-wave order.

**Gate 0 — Eligible:** canonical journal identity and publication boundary are known; the coverage
gap is supported by normalized evidence; an owner and route budget are assigned.

### Phase 1: Reconnaissance

- Execute the small, read-only reconnaissance loop.
- Produce the complete dossier and representative samples.
- Classify access and file granularity before proposing implementation.

**Gate 1 — Evidence complete:** oldest/middle/newest eras are sampled or their absence is
explained; route termination is known; metadata fields and file delivery are evidenced; robots and
scope decisions are recorded; verdict is not merely inferred from platform branding.

### Phase 2: Design review

- Reproduce representative routes.
- Prefer reusable platform behavior only where publication boundaries remain explicit.
- Select sitemap seed, adapter family, pagination mechanism, and fixture plan.

**Gate 2 — Ready to implement:** reviewer accepts the dossier; unresolved risks are bounded; the
implementation cannot enumerate sibling publications by accident.

### Phase 3: Implement onboarding

- Add or update the curated sitemap and exact host routing before smoke.
- Add adapter/selector logic and representative fixtures/tests.
- Update the onboarding tracker with exact selectors, pagination evidence, and next action.
- Do not leave a low-confidence `registry_*.json` seed as the successful active sitemap.

**Gate 3 — Runnable:** the intended adapter is selected rather than an unmapped or accidental
generic fallback; listing, pagination, metadata, and file-resolution tests pass.

### Phase 4: Smoke and quality review

Run explicit one-target onboarding smoke with a bounded candidate budget and temporary output.
Verify the artifact on disk rather than relying on a run status.

**Gate 4 — Smoke pass:** at least one valid PDF exists on disk; title is non-empty; at least two
of author, volume, and year are non-empty; adapter, seed, report path, artifact path/size, and
metadata presence are recorded. A PDF with title but only one of those three fields is a
`SOFT_FAIL` and may be recorded as `active_partial_metadata`; no valid PDF, no title, an unmapped
adapter, or an access blocker is a `HARD_FAIL`. Neither result is a full acquisition pass.

### Phase 5: Controlled collection

- Start with one issue, then one volume or a small year range.
- Enforce rate, retry, size, and candidate limits.
- Validate HTTP status, final URL, content type, `%PDF` signature, duplicate hash, and document
  class.
- Review outliers before widening the range.

**Gate 5 — Ready to expand:** the bounded batch has no scope leakage; article/issue classification
and normalized journal attribution meet threshold; metadata and file failure rates are understood;
resume and termination behavior are proven.

### Phase 6: Reconcile coverage

- Merge run evidence into normalized holdings reporting.
- Compare expected versus observed volumes, issues, years, and article counts.
- Separate genuinely unavailable material from scraper failures.
- Update queue state and record follow-up eras or routes.

**Gate 6 — Complete for stated scope:** the dossier names the covered span and explicit
exceptions; successful records are article-qualified and journal-attributed; generated reports are
refreshed; remaining gaps have owners or documented terminal reasons.

## Digital Commons Lane

Digital Commons is a platform project with publication-scoped outputs. Platform detection should
handoff to this lane rather than to a broad generic host crawl.

For each publication:

1. establish the exact publication slug and sibling exclusions;
2. inspect `robots.txt` for the specific collection, OAI, item, and file paths;
3. compare publication pages, publication-scoped sitemap routes, and OAI-PMH sets;
4. capture OAI field mapping for title, repeated creators, abstract, date, identifier, DOI,
   primary file, and supplements;
5. prove complete resumption-token handling and a stable termination condition;
6. resolve item pages to primary files without treating supplements, cover pages, or whole issues
   as articles;
7. record redirect, cookie, referer, login, subscription, or 403 behavior separately for discovery
   and file delivery; and
8. test against fixtures from at least two repositories before declaring behavior reusable.

Repository-wide OAI enumeration is out of scope by default. A successful metadata harvest does not
prove that PDF delivery is permitted or functional. Conversely, an accessible file URL does not
prove that the item belongs to the target publication. Both gates must pass.

The platform milestone is complete when the three first-wave collections (`wmlr`, `elj`, and
`shlr`) can each be enumerated independently, their pagination/resumption terminates, and their
primary-file outcomes and access states are reported without cross-collection leakage.

## Safety, Robots, and Request Discipline

- Fetch `robots.txt` before exploration and record the user-agent and route-level decision in the
  dossier. Recheck before collection because rules can change.
- Use a descriptive project user-agent where permitted. Apply a conservative per-host delay;
  selector probing defaults to two seconds between requests.
- Keep reconnaissance batches small (normally no more than ten URLs at a time and no more than 50
  per invocation) and cache evidence instead of repeatedly fetching unchanged pages.
- Respect `Retry-After`, exponential backoff, timeouts, and a per-host failure cutoff.
- Do not log credentials, cookies, signed tokens, or private URLs in tracked artifacts.
- Stop on login, paywall, CAPTCHA, or sustained 403/429/WAF evidence. Record the blocker and seek an
  authorized alternate route or network decision.
- Do not disguise the client, rotate endpoints, or use a browser to defeat an access control.
  `needs_headless` is appropriate only for publicly accessible, JavaScript-rendered navigation.
- Inspect a minimal file response during reconnaissance; perform downloads only in smoke or an
  authorized bounded collection run.
- Treat broad production commands as unsafe until seed lifecycle filtering is repaired; paused and
  todo sitemap entries can currently remain runnable.

## Artifacts and Ownership

| Artifact | Role | Owner | Update point |
|---|---|---|---|
| `docs/JOURNAL_RECONNAISSANCE_ROADMAP.md` | Queue, policy, milestones, and metrics | Coordinator | Wave review |
| `data/reference/journal_recon/<slug>.json` | Canonical per-publication reconnaissance evidence | Dossier owner; reviewer approves | Phases 1-2 and material route change |
| `data/reference/onboarding-selector-pagination-tracker.csv` | Latest operational selector/pagination and smoke projection | Implementer/operator | Phases 3-4 |
| `offprint/sitemaps/<slug>.json` | Executable curated seed/configuration | Implementer | Phase 3 |
| Adapter registry/code and fixtures | Routing and extraction behavior | Implementer; code reviewer approves | Phase 3 |
| Temporary smoke reports/PDFs | Reproducible onboarding evidence, not tracked corpus | Operator | Phase 4 |
| Local run records and normalized coverage reports | Collection and reconciliation evidence | Collection operator/catalog maintainer | Phases 5-6 |

Do not duplicate the full dossier into sitemap notes. Link or cite the dossier and project only the
fields needed at runtime. The tracker keeps the latest operational row per host; historical and
publication-specific reasoning belongs in the dossier and version control.

## Milestones

### M0 — Contract and queue

- [x] Add a versioned JSON schema and validator for the dossier contract.
- [x] Create the `data/reference/journal_recon/` directory with a short README/template.
- [x] Generate a normalized priority report that rejects path-label counts as journal identities.
- [ ] Assign owners and reviewers for the ten first-wave publications.

### M1 — First three dossiers

- [x] Northwestern University Law Review dossier reviewed.
- [x] William & Mary Law Review dossier reviewed.
- [x] U.C. Irvine Law Review dossier reviewed.
- [x] Record schema and process revisions learned from the pilot before scaling the wave.

Pilot review on 2026-07-14 produced three distinct operational outcomes:

- Northwestern exposes structured metadata for volumes 105-120, but the sampled PDF route returns
  HTTP 403 and the earlier archive remains unresolved.
- William & Mary's publication HTML enumerates volumes 1-67 with a stable issue layout from 1957
  through 2026, but its sampled PDF route also returns HTTP 403.
- UCI's law-school page is an identity/handoff page, not the WordPress archive described by the
  existing sitemap; the eScholarship collection, item, and candidate file routes are WAF-blocked.

The dossier contract is now version 1.1.0: file-session requirements are tri-state so a blocked
probe does not create a false claim that cookies, JavaScript, a referer, or authentication are or
are not required. The Digital Commons runtime also accepts `all_issues_only`, which preserves a
reviewed publication-HTML route without silently widening to robots-disallowed OAI or a generic
HTML crawl. These changes do not promote any pilot to collection readiness.
The bounded response classification and stop policy are maintained in
[Digital Commons file-access diagnostics](DIGITAL_COMMONS_FILE_ACCESS.md).

The next reviewed dossier identified Penn's Quartex repository as the authoritative Journal of
Business Law route. Its publication-scoped archive advertises 746 records across volumes 1-28
(1998-2026), and a current detail page exposes rich metadata plus a successful public PDF-init
response. A bounded collection smoke downloaded a valid PDF but selected a Keyboard Shortcuts
masthead; a direct article-detail smoke then returned HTTP 200 without a PDF candidate. The
adapter now rejects explicit collection furniture, and the next step is to recover the current
detail-page token/API contract before another article smoke. The exact
`repository.law.upenn.edu` route is mapped to `QuartexAdapter`, preventing the generic
`repository.*` heuristic from misrouting it as Digital Commons.

### M2 — Digital Commons proof

- [ ] `wmlr`, `elj`, and `shlr` enumerate independently.
- [ ] Resumption/pagination termination and primary-file resolution have fixtures.
- [ ] Cross-publication leakage test passes.

### M3 — Custom-site first wave

- [ ] Northwestern, Penn Journal of Business Law, Connecticut, Iowa, and Tulane pass dossier review.
- [ ] At least one representative historical-era fixture exists for each distinct layout.
- [ ] Each implemented journal reaches smoke review with explicit adapter evidence.

### M4 — Barrier lane

- [ ] UC Davis has a dated barrier or alternate-route dossier.
- [ ] WAF/login/paywall states have a consistent evidence and retry policy.
- [ ] No blocked target is retried automatically by a broad run.

### M5 — Bounded acquisition and reconciliation

- [ ] Each ready journal completes a one-issue and one-volume/year bounded run.
- [ ] Document-class and journal-attribution samples pass review.
- [ ] Coverage reports are refreshed and remaining gaps are stated by year/volume.

## Metrics

Report metrics by publication and platform, not only as corpus-wide totals:

- priority journals at each workflow state;
- median days from `queued` to reviewed dossier, smoke pass, and bounded collection;
- dossier review pass rate and number of evidence-revision cycles;
- percentage of dossiers covering oldest/middle/newest eras;
- publication-scoped enumeration completeness: expected versus observed volumes/issues/items;
- smoke PASS, SOFT_FAIL, and HARD_FAIL counts by blocker class;
- metadata completeness for title, author, year, volume, issue, abstract, pages, and DOI;
- primary-file resolution rate and valid-PDF rate;
- article-qualified rate and rates of issue PDFs, front matter, mastheads, supplements, and invalid
  files;
- duplicate rate by content hash and cross-publication attribution conflicts;
- request/429/403/timeout rate by host and mean requests per accepted item; and
- net new unique article-qualified PDFs and newly covered years, with source provenance.

Do not use raw downloaded-file count as the sole success metric. A successful wave improves both
coverage and confidence in identity, scope, and metadata.

## Known Repository Contradictions and Required Repairs

These are roadmap inputs, not reasons to relax the gates:

1. **Status is informational at runtime.** `offprint/seed_catalog.py::filter_active()` currently
   returns every sitemap entry, so paused and todo records may be runnable. Repair lifecycle
   filtering or require explicit target-file operation before any broad run.
2. **Registry rows are not unique journal identities.** Several first-wave journals have duplicate
   rows with different platform labels, sitemap files, or URLs. Select a canonical publication
   identity in the dossier and reconcile registry rows deliberately.
3. **`active` does not mean adapter-ready.** Worklist history includes active seeds with repeated
   `todo_adapter_blocked` or `seed_failure` outcomes. Promotion requires observed routing and smoke
   evidence.
4. **Digital Commons lifecycle is inconsistent.** The onboarding workflow hard-gates Digital
   Commons to specialist adapter work, while many Digital Commons registry/sitemap rows are already
   marked active with no publication-scoped run evidence. The specialist lane supersedes status
   assumptions.
5. **Host counts can mask collection gaps.** eScholarship and multi-publication Digital Commons
   hosts can appear well covered while an individual journal has zero attributable items. Coverage
   must be joined on a proven publication identifier.
6. **Holdings labels are not normalized journal names.** URL segments and repository slugs appear
   beside genuine journals in the generated holdings report. Prioritization must use the normalized
   catalog plus manual identity review.
7. **Workflow paths have drifted.** Some onboarding/probing instructions refer to `sitemaps/`,
   `references/`, and `probe_results/` relative to an older working directory, while tracked files
   currently live under `offprint/sitemaps/` and `data/reference/` inside the repository. Commands
   and skills should be aligned before automation depends on them.
8. **Probe history and current state have different semantics.** Selector probing appends JSONL
   observations, while the onboarding tracker keeps a latest row per host. The dossier should retain
   versioned publication-level reasoning; projections must not erase it.
9. **Quality-gate wording has drifted.** Existing onboarding guidance differs on what constitutes a
   hard metadata failure. This roadmap uses one operational rule: PASS requires valid PDF, title,
   and two of author/volume/year; PDF plus title and only one field is SOFT_FAIL; missing PDF/title,
   blocked routing/access, or zero useful metadata is HARD_FAIL.
10. **Local holdings are not clean-checkout facts.** Generated holdings report local run records
    and may lag current corpus reconciliation. Every acquisition wave must record its baseline date
    and regenerate evidence before claiming improvement.

## Immediate Implementation Sequence

1. Implement the dossier schema, validator, template, and queue-state checks.
2. Produce the Northwestern dossier as the custom-site pilot.
3. Produce the William & Mary dossier as the Digital Commons pilot.
4. Produce the U.C. Irvine dossier as the dual-route and attribution pilot.
5. Review all three together; revise the contract once before assigning the remaining first wave.
6. Implement and smoke only dossiers that pass Gate 2.
7. Run bounded acquisitions, reconcile normalized coverage, and then reprioritize the queue.

This sequencing deliberately learns from three different failure classes before scaling: custom
historical layouts, repository-scoped enumeration, and host-to-publication attribution.
