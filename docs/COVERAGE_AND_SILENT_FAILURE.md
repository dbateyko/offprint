# Coverage and silent failure

Notes for anyone — human or agent — adding journals to this corpus. Written
2026-08-30 after a session that wasted several hundred requests on journals we
already held and found four separate tools that were installed, green, and doing
nothing.

## 1. Runs that collect a fraction and exit 0

Four distinct mechanisms were found in one day, each stopping a run early while
reporting success:

- `_iter_dspace_pdf_candidates` treated any falsy `_get_json` as end-of-results.
  `_get_json` collapses every failure into `None`, so one transient 429 after
  page 0 ended pagination permanently. Yale finished with 20 of 259 items.
- `TulaneLawReviewOnlineAdapter.discover_pdfs` read only the seed index page and
  never followed the Squarespace `rel="next"` pager: 20 of ~39.
- `--max-consecutive-seed-failures-per-domain` defaults to 3 and counts *empty*
  results as failures. Fordham's pre-2000 volumes are metadata stubs with no
  PDFs, so three in a row tripped the breaker and skipped the remaining 158
  issue pages.
- Fordham's WordPress REST pagination is broken upstream: `/wp-json/wp/v2/
  issuescategory?page=N` reports 164 terms but returns only 84 unique ids across
  four pages. Anything enumerating that way under-collects silently.

**What to do.** Declare `navigation.expected_pdfs` in every new seed and let the
completeness gate in `run_pipeline` check the run against it. Never guess the
number: an expected count that is too LOW is worse than none, because a
truncated run then compares favourably and passes. If the real total is not
known, leave it out — the gate reports "unverifiable", which is the honest
state.

## 2. Coverage is per journal, never per host

`corpus/scraped` is laid out by host, and one host serves many journals. Host
presence is wrong in both directions and cost three duplicate crawls in one day:

- Fordham Law Review read as uncovered while 3,241 of its PDFs were held under
  `ir.lawnet.fordham.edu`.
- `journals.library.columbia.edu` holds 2,012 PDFs of which none were the
  Columbia Business Law Review, so the same shortcut would have hidden a real gap.

Filenames do not rescue it either. A filename check reported "0 duplicates" for
CBLR while 1,121 of its PDFs sat on disk named `...columbuslrev...`, because the
OJS galley names share nothing with the citation-style ones. Two sources for one
journal never agree on filenames.

**What to do, before any crawl:**

```bash
python scripts/quality/check_seed_overlap.py --seed offprint/sitemaps/<seed>.json
python scripts/quality/coverage_by_journal.py          # what is actually missing
python scripts/quality/find_multisource_journals.py --only-risky
```

`check_seed_overlap` answers by journal name via
`artifacts/attribution_index.json`, which is what onboarding actually asks. A
multi-source flag is not automatically a skip: paired sources often split by era
(a bepress backfile against a site carrying recent volumes), so compare volume
ranges before deciding.

Rebuild the index after collecting anything:

```bash
python scripts/quality/build_attribution_index.py --backfill
```

Staging counts as collected. Unpromoted work is invisible to `corpus/scraped`,
and Georgetown Law Journal read as uncollected with 304 of its PDFs sitting in a
staging run.

## 2b. Filenames are not identity; hashes are

Promotion SHA-256-deduplicates against the corpus, and that is the only test
that actually holds. Two numbers from the 2026-08-30 promotion:

- The CR-CL crawl promoted 11 net-new against 228 duplicates.
- A targeted harvester that fetched exactly the 120 files a *filename* check had
  called missing promoted **0 net-new against 116 duplicates**. The corpus
  already held identical bytes under different names.

So `check_seed_overlap`'s filename comparison is a coarse screen, useful only to
catch the obvious case. It over-reports gaps whenever a source renames files, and
the by-journal lookup in the attribution index is the better first question.
Before a targeted re-fetch of "missing" files, hash-check them against the corpus
rather than trusting names.

## 3. Do not disable the guards

Every run launched in that session carried `--no-skip-well-covered-seeds`,
copied from an older script without thinking. That flag exists to prevent
re-crawling covered ground, and it was off on all of them. Only pass it when you
have a specific reason.

## 4. Test tools against real data, not against your assumptions

Four tools were installed, passing their tests, and inert:

- The completeness gate read `payload["summary"]["seeds"]`. `run_orchestrator`
  returns run-level counters with no `seeds` key — the per-seed detail is in
  `stats.json` in the run directory. The gate returned `False` and printed
  nothing on every real run for a day.
- It also compared each `start_url` against `expected_pdfs`, which counts a
  *journal*. Penn's seeds are 753 Detail pages sharing one total, so a complete
  run would have reported 753 short seeds.
- `check_seed_overlap` compared filenames only, and said "0 duplicates" for a
  journal we largely held.
- The Quartex enumerator never set `journal_name`, so the by-journal lookup had
  no name to search and reported zero holdings for any Penn journal.

None were caught by unit tests, because those were written against assumed
interfaces. All four surfaced by running the tool against a finished run or a
known-duplicate journal — a case with an answer known in advance. Budget for
that check explicitly; a regression test that passes against both the broken and
the fixed code manufactures confidence rather than providing it.

## 5. Politeness

Check `robots.txt` first, honour `Crawl-delay`, and run one job per host — a
second concurrent run doubles the rate on someone's server. Two specifics worth
knowing:

- Python's `urllib.robotparser` is first-match-wins. RFC 9309 says the LONGEST
  matching rule wins, and the difference is not academic: `openyls.law.yale.edu`
  writes `Allow: /server/api` before `Disallow: /server/api/`, so urllib reports
  a disallowed endpoint as crawlable. `scripts/quality/backfill_expected_counts.py`
  carries a correct matcher (longest-match, wildcards, matched against
  path+query).
- Bursts trip WAFs that steady crawling does not. A 3–6s cadence got our IP
  captcha-walled at `hofstralawreview.org` (SiteGround, HTTP 202 redirecting to
  `/.well-known/sgcaptcha/`) after ~20 downloads. Back off and wait; do not work
  around a challenge.

## 6. Known access blocks

bepress `/cgi/viewcontent.cgi` returns Cloudflare 403 platform-wide. Confirmed
at Northwestern, Hofstra and Vanderbilt. Of 244 genuinely uncollected active
journals, 42 are unreachable for this reason, including the three largest
remaining prizes (JCLC 6,735 articles, Vanderbilt 4,132, Hofstra backfile
2,216). Enumeration still works, so seed them `active_enumeration_only` with the
article count. This is an out-of-band access request, not an engineering task —
see `DIGITAL_COMMONS_RESPECTFUL_ACQUISITION.md`.
