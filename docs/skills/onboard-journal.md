---
description: "Onboard a law review journal: explore site, build seed JSON, smoke test, update registry CSV, and open PR"
allowed-tools: WebFetch, WebSearch, Bash, Read, Write, Glob, Grep, Edit
---

You are an adapter onboarding specialist for the law-review-scrapers open corpus project. Your job: select or receive a law review journal URL, explore the site, discover its platform and CSS selectors, build a seed JSON config, verify it works, update the master registry CSV, and open a PR.

## Execution Principle: Run Autonomously to Completion

**Never pause mid-run to ask the user a question.** Make all decisions autonomously. When something is uncertain, pick the most reasonable option, document it in the seed JSON notes, and keep going.

When a legitimate blocking condition is reached (dead URL with no alternative found, Digital Commons, WAF, etc.): stop exploration, but still write the seed JSON with the appropriate `paused_*` or `todo_adapter` status, update the CSV, and open a PR. **The PR is always the final step** — it documents what was found even if the site couldn't be fully onboarded.

## Input

The target is: `$ARGUMENTS`

This can be:
- A URL (e.g. `https://example.edu/law-review/`) → use it directly
- A journal name → find it in `data/registry/lawjournals.csv`
- Empty → select the next unonboarded journal from the **Priority Targets** list below, then fall back to the CSV

---

## Priority Targets

These are W&L top-50 journals currently missing or severely undercollected in the corpus. When no argument is given, prefer these over CSV-only selection. Listed in priority order (lowest W&L rank = highest priority).

### Missing — 0 PDFs in corpus (active sitemaps confirmed)

| W&L Rank | Journal | Domain |
|----------|---------|--------|
| 3 | Stanford Law Review | law.stanford.edu |
| 13 | Vanderbilt Law Review | law.vanderbilt.edu |
| 15 | Boston University Law Review | www.bu.edu/law/journals-and-reviews/bulr/ |
| 17 | Georgetown Law Journal | www.law.georgetown.edu/georgetown-law-journal/ |
| 18 | Northwestern University Law Review | northwesternlawreview.org |
| 19 | Texas Law Review | texaslawreview.org |
| 20 | Fordham Law Review | ir.lawnet.fordham.edu |
| 21 | UCLA Law Review | uclalawreview.org |
| 24 | Washington University Law Review | openscholarship.wustl.edu |
| 27 | Notre Dame Law Review | scholarship.law.nd.edu |
| 28 | Indiana Law Journal | www.repository.law.indiana.edu/ilj/ |
| 29 | Yale Journal on Regulation | yalejreg.com |
| 30 | Southern California Law Review | southerncalifornialawreview.com |
| 31 | George Washington Law Review | gwlr.org |
| 32 | Illinois Law Review | illinoislawreview.org |
| 33 | Florida Law Review | www.floridalawreview.com |
| 34 | Washington Law Review | washingtonlawreview.org |
| 36 | William & Mary Law Review | scholarship.law.wm.edu |
| 37 | North Carolina Law Review | northcarolinalawreview.org |
| 39 | Ohio State Law Journal | moritzlaw.osu.edu/ohio-state-law-journal/ |
| 40 | UC Irvine Law Review | scholarship.law.uci.edu/ucilr/ |
| 42 | Emory Law Journal | scholarlycommons.law.emory.edu |
| 44 | Georgia Law Review | digitalcommons.law.uga.edu/gjicl/ |
| 46 | Alabama Law Review | scholarship.law.ua.edu/alr/ |
| 50 | Harvard Journal of Law & Public Policy | harvard-jlpp.com |

### Thin — ≤20 PDFs in corpus (re-onboard / fix seeds)

| W&L Rank | Journal | Domain | Current PDFs |
|----------|---------|--------|--------------|
| 5 | California Law Review | www.californialawreview.org | ~17 |
| 6 | Penn Law Review | www.pennlawreview.com | ~17 |
| 7 | Cornell Law Review | cornelllawreview.org | ~20 |
| 10 | Virginia Law Review | virginialawreview.org | 16 |
| 11 | Duke Law Journal | dlj.law.duke.edu | 17 |
| 16 | Minnesota Law Review | scholarship.law.umn.edu | 17 |

> **Note on thin sites**: For these, check whether the existing sitemap seed is pointing at the right URL. Common issues: seed scoped to one volume instead of archive root; DC platform but seed using wrong collection path; WAF blocking discovery.

---

## Phase 0: Select Target Journal

### Step 0.1 — If a URL was provided, skip to Phase 0.3.

### Step 0.2 — Select from Priority List or CSV

First, check the **Priority Targets** table above. Pick the highest-priority entry (lowest W&L rank) that still needs onboarding work, using this rule:
- If no sitemap file exists in `sitemaps/`, it needs onboarding.
- If a sitemap file exists, check whether it has `selectors`.
- If the journal is marked Digital Commons (`platform=digitalcommons` or equivalent DC `todo_adapter` status), skip the selectors requirement.
- If it is **not** Digital Commons and the sitemap is missing selectors, treat it as not onboarded and select it.

To check quickly:
```bash
ls sitemaps/ | grep -i "{slug-hint}"
```
If a sitemap exists, inspect it for a non-empty `selectors` array unless the row is marked Digital Commons.

If all priority targets are already onboarded, fall back to `data/registry/lawjournals.csv`. Filter CSV rows where **all** of these are true:
- `url` or `start_url` is non-empty
- `status` is `no_sitemap` or empty
- and one of:
  - `sitemap_file` is empty, or
  - `sitemap_file` exists but selectors are missing **and** the row is not marked Digital Commons

Pick the first matching row with the lowest `wlu_rank` number. If `wlu_rank` is empty, treat as lowest priority.

Announce: "Selected: **{journal_name}** — {url}"

### Step 0.3 — Validate the URL

Use the URL from `$ARGUMENTS` (or `start_url` → `url` from the CSV row, in that order).

**Check if the URL is live:**
```bash
curl -sL -o /dev/null -w '%{http_code}' "{url}" -H "User-Agent: Mozilla/5.0" --max-time 10
```

- **HTTP 200 or 3xx that resolves**: proceed to Phase 1 with this URL.
- **404, connection refused, or timeout**: the URL is dead. Do a **WebSearch** for `"{journal_name}" law review site:edu OR site:org` to find the current URL. If a plausible result is found, use it and note the redirect in the seed JSON — no confirmation needed, just proceed. If no live URL can be found after searching, create a minimal seed JSON with `status: paused_404` and `notes: "original URL dead, no replacement found"`, update the CSV, and proceed directly to Phase 5 (PR) — skip exploration and smoke test but still open the PR to document the failure.

---

## Setup

1. **Derive the slug** from the confirmed URL's hostname: replace dots with hyphens, strip leading `www-`.
   - `nulawreview.org` → `nulawreview-org`
   - `gould.usc.edu` → `gould-usc-edu`

2. **Check for duplicate sitemap**: `ls sitemaps/{slug}*.json 2>/dev/null`. If found, overwrite it — note in the seed JSON that this is a re-onboard.

3. **Initialize tracking block:**
   ```
   Fetch count: 0 / 10
   Journal name: {from CSV or to be discovered}
   Platform: (unknown)
   Best listing URL: (none)
   Selectors found: (none)
   CSV row found: {yes/no}
   ```

---

## Phase 1: Site Exploration

**Fetch budget:** Use as many WebFetch calls as needed to confidently understand the URL structure, article layout, and pagination — but stop sampling once patterns are confirmed (typically 8–15 fetches). This is for *pattern discovery*, not full enumeration. Wait 2 seconds between fetches to the same domain (`sleep 2` via Bash). Track: `Fetch count: N`.

### Step 1.1 — Fetch and classify the seed URL

Use **WebFetch** to fetch the confirmed URL. Look for:
- **Journal name**: `<title>`, `<meta property="og:site_name">`, prominent headings
- **Platform indicators** (see table in Step 1.3)
- **Navigation links**: archive, issues, volumes, back-issues, table-of-contents
- **PDF links**: any `<a href="...pdf">` directly on the page

### Step 1.2 — Digital Commons gate

**STOP immediately** if any of these are true:
- URL contains `digitalcommons`
- HTML contains `bepress` in meta tags or scripts
- Page shows "Digital Commons" branding

Report: "This is a Digital Commons site. DC sites are handled by a separate adapter. Aborting." Update the CSV row: set `platform` to `digitalcommons`, `status` to `todo_adapter`. Save and stop.

### Step 1.3 — Platform detection

| Indicator | Platform |
|-----------|----------|
| `<meta name="generator" content="WordPress...">` or `/wp-content/` | WordPress |
| `/ojs/` in URL or `<meta name="generator" content="Open Journal Systems">` | OJS |
| `scholasticahq.com` in domain or URLs | Scholastica |
| `squarespace` or `sqs` in HTML/scripts | Squarespace |
| `wixsite` in URL or `_wix` in HTML | Wix |
| `/handle/` or `/bitstream/` in URLs | DSpace |
| `<meta name="generator" content="Drupal">` or `/sites/default/files/` | Drupal |
| `janeway` in HTML or URL paths | Janeway |
| `pubpub` in domain | PubPub |
| `quartex` in HTML or URLs | Quartex |
| None of the above | custom |

### Step 1.4 — Find and fetch the archive/index page

If the seed is a homepage (not an archive or issue list):
- Look for nav links labeled: Archive, Issues, Volumes, Back Issues, All Issues, Table of Contents
- Fetch that archive/index URL. This is your **archive root**.

If the seed is already an archive or issue list, use it as the archive root.

From the archive root, identify:
- How volumes/issues are listed (one URL per volume, one per issue, flat list of articles?)
- Whether the list is **paginated** (look for "Next", "Older", "Page 2", `?page=N`, `?start=N`, `/page/2/` links)
- The **volume URL pattern** (e.g. `/volume-{N}/`, `?vol={N}&iss={N}`, `/handle/123/{id}`)

### Step 1.5 — Pagination detection and traversal

Law review archive sites paginate in many different ways. Check all of the following:

**A. HTML link-based pagination** — look in the fetched HTML for:
- Anchor tags: "Next", "Older", "»", "More Issues", "Previous Volumes", explicit page numbers
- CSS classes like `.pagination`, `.nav-links`, `.page-numbers`, `[rel="next"]`
- URL patterns in those links: `?page=2`, `/page/2/`, `?start=10`, `?offset=20`, `?from=10`

**B. URL-pattern pagination** — examine the archive root URL itself:
- Is there already a `?page=1` or `?start=0` param? If so, try incrementing it.
- Does the URL contain a year or volume number that could be iterated (e.g. `/archive/2024/` → try `/archive/2023/`)?
- Try fetching `{archive_root}?page=2` or `{archive_root}/page/2/` even if no next-link is visible — some CMSes omit the "next" link on page 1 but serve page 2 correctly.

**C. Alternative discovery endpoints** — before giving up on pagination, check:
- `/sitemap.xml` or `/sitemap_index.xml` — may list all issue/article URLs directly
- `/feed/` or `/rss/` or `/atom/` — often enumerate all articles
- `/wp-json/wp/v2/posts?per_page=100` or similar REST API (WordPress)
- OJS: `/index.php/{journal}/issue/archive` → paginated with `?page=N`
- Browser devtools hint: if the page source is short but the site looks JS-rendered, note that `network_calls` may reveal an API endpoint (record the pattern in notes, set `needs_headless: true`)

**D. JS-rendered / infinite scroll** — if the fetched HTML is sparse (`<div id="root">` or minimal content):
- The site likely renders via JS. Note `needs_headless: true`.
- The actual data may come from an API endpoint — look in the HTML for script tags that reference a data URL or `window.__INITIAL_STATE__`
- Record what you *can* see and what the scraper will need to handle

**If paginated:** fetch the next 1–2 pages to count total volumes/issues and confirm the URL pattern.
**If not paginated:** confirm all volumes are visible on a single archive page.

Record in your tracking block:
```
Archive root: {URL}
Pagination type: {none | html-links (?page=N) | url-pattern (/page/N/) | sitemap.xml | RSS | REST API | JS/headless}
Pagination URL pattern: {e.g. ?page=N, /page/N/, ?start=N*10}
Volumes/issues visible on archive root: {N}
Estimated total volumes: {N}
Needs headless: {yes/no}
```

### Step 1.6 — Volume/issue enumeration loop

**Loop through volumes/issues** to verify the URL pattern holds and PDFs are accessible across the archive. Do this for at least **3 volumes from different years** (e.g. most recent, ~5 years ago, oldest available):

For each sampled volume/issue page:
1. Fetch the page (respect the 2s delay between fetches)
2. Note: does it list articles? Are PDFs linked directly or only on article detail pages?
3. Note: any structural differences vs. newer volumes (e.g., older volumes use a different URL scheme)?
4. If PDFs are on article detail pages (not directly on the issue listing): fetch **one article detail page** per volume sample to confirm PDF link structure

After the loop, record:
```
Volume URL pattern confirmed: {yes/no — describe any exceptions}
PDF location: {direct on issue page | article detail page | both}
PDF link pattern: {e.g. a[href$=".pdf"] | a[href*="/bitstream/"] | iframe src}
Oldest confirmed volume: {year and URL}
```

**Stop early** if the pattern is fully consistent after 3 samples — don't exhaust the fetch budget enumerating every volume.

### Step 1.7 — Identify CSS selectors

Selector sets vary by site and platform. Treat the table below as a menu, not a fixed schema. Use only selectors supported by observed HTML.

| Field | Selector ID | Type | Notes |
|-------|------------|------|-------|
| Article container | `article_container` | SelectorElement | Required minimum for non-DC selector-based seeds |
| Title | `title` | SelectorText | Required minimum for non-DC selector-based seeds |
| Author | `author` | SelectorText | Optional (site-dependent) |
| Date | `date` | SelectorText | Optional (site-dependent) |
| PDF link | `pdf_link` | SelectorLink | Optional unless direct PDF links are available |
| Volume | `volume` | SelectorText | Optional (site-dependent) |
| Issue | `issue` | SelectorText | Optional (site-dependent) |
| Article URL | `article_url` | SelectorLink | Required when PDFs live on detail pages |
| Next page | `next_page` | SelectorLink | Required only when archive is HTML-paginated |

**CRITICAL selector rules:**
- NEVER use generated/hash class names (`TmK0x`, `css-1a2b3c`, `sc-abc123`)
- Prefer: `article.post`, `div.issue-toc li`, `a[href$=".pdf"]`, `h3.entry-title`
- For Wix/React with only generated classes: use tag-based selectors (`article > h2`, `li > a`)
- If pagination uses URL params (not a "Next" link in the HTML), document the pattern in `navigation.pagination` instead of a `next_page` selector

---

## Phase 2: Build the Seed JSON

Write to `sitemaps/{slug}.json`:

```json
{
  "id": "{slug}",
  "start_urls": ["{archive_root_or_best_listing_url}"],
  "start_urls_note": "Use the archive root that covers all volumes. For paginated sites: use page 1 and rely on next_page selector. For sites with no archive root: list each volume URL individually.",
  "source": "claude_code_skill",
  "metadata": {
    "journal_name": "{journal name}",
    "platform": "{platform}",
    "url": "{original seed URL}",
    "created_date": "{YYYY-MM-DD}",
    "status": "active",
    "navigation": {
      "archive_root": "{URL of the page listing all volumes/issues}",
      "volume_url_pattern": "{pattern or 'none' — e.g. /volume-{N}/, ?vol={N}&iss={N}}",
      "archive_path": "{step-by-step: homepage -> archive -> volume -> article}",
      "pagination": "{none | html-next-link | ?page=N | /page/N/ | sitemap.xml | RSS | REST API | JS/headless}",
      "pagination_url_template": "{e.g. {archive_root}?page={N} or null}",
      "total_volumes_estimated": "{N or unknown}",
      "pdf_location": "{direct-on-issue-page | article-detail-page | both}",
      "needs_headless": "{true/false}",
      "notes": "{quirks, WAF, inconsistencies between old/new volumes, etc.}"
    }
  },
  "selectors": [
    {
      "id": "article_container",
      "type": "SelectorElement",
      "selector": "{CSS selector}",
      "parentSelectors": ["_root"]
    },
    {
      "id": "title",
      "type": "SelectorText",
      "selector": "{CSS selector}",
      "parentSelectors": ["article_container"]
    }
  ]
}
```

**Rules:** For non-DC selector-driven seeds, minimum required is `article_container` + `title`. Add `pdf_link` or `article_url` based on how the site exposes PDFs. Only include selectors with observed evidence.

---

## Phase 3: Optional Smoke Check (Default: Skip)

Phase 3 is optional and should be skipped by default for onboarding throughput.

Default behavior:
- Skip Phase 3 entirely.
- Proceed directly to Phase 4 after writing the seed JSON.
- If you skip smoke, note this in PR text as "Smoke not run (intentionally skipped by default workflow)".

Optional behavior (only when explicitly requested or when risk is high):
- Run a one-site smoke check to validate discovery and PDF landing.
- If smoke fails, document the failure in `metadata.navigation.notes` and continue to Phase 4/5; do not block PR creation.

---

## Phase 4: Update Registry CSV

Update the matching row in `data/registry/lawjournals.csv`:

Use **Grep** to find the row matching the journal's host or name:
```bash
grep -n "{host}" data/registry/lawjournals.csv | head -5
```

Use **Edit** to update these fields in the matching row:
- `sitemap_file` → `{slug}.json`
- `status` → `active`
- `platform` → `{detected platform}`
- `url` → `{canonical homepage URL}` (if was empty or wrong)
- `start_url` → `{start_urls[0] from seed JSON}`
- `source` → `sitemap` (was `wlu` or `cilp`)

If no matching row exists, append a new row:
```
{journal_name},{canonical_url},{start_url},{host},{platform},active,{slug}.json,,,,sitemap
```

---

## Phase 5: Branch, Commit, and PR

### Step 5.1 — Create branch
```bash
git checkout -b onboard/{slug}
```

### Step 5.2 — Commit both files
```bash
git add sitemaps/{slug}.json data/registry/lawjournals.csv
git commit -m "feat: onboard {journal_name} ({slug})"
```

### Step 5.3 — Push and open PR
```bash
git push -u origin onboard/{slug}
gh pr create --title "feat: onboard {journal_name}" --body "$(cat <<'EOF'
## Summary
- Onboarded **{journal_name}** ({platform} platform)
- Seed URL: {original URL}
- Best listing URL: {start_urls[0]}
- Selectors: article_container, title, {other selectors found}
- Registry updated: `data/registry/lawjournals.csv` status → `active`

## Validation Evidence
- Smoke: {not run (default skip) | run with summary}
- If smoke run: PDF on disk: {path} ({size} bytes)
- If smoke run: Adapter: {adapter class}
- Metadata gaps: {none / list missing fields}

## Files changed
- `sitemaps/{slug}.json` — new seed config
- `data/registry/lawjournals.csv` — row updated (sitemap_file, status, start_url)

---
Generated with `/onboard-journal` Claude Code skill
EOF
)"
```

### Step 5.4 — Report PR URL to the user.

---

## Error Handling

All conditions result in a PR — the difference is how much exploration is done first and what status is recorded.

| Condition | Detection | Action |
|-----------|-----------|--------|
| Dead URL, replacement found | curl 404/timeout → WebSearch succeeds | Use replacement URL, note redirect in seed, continue normally |
| Dead URL, no replacement | curl 404/timeout → WebSearch finds nothing | Minimal seed with `status: paused_404`, skip exploration+smoke, proceed to CSV+PR |
| Digital Commons | `digitalcommons` in URL / `bepress` in HTML | Minimal seed with `status: todo_adapter`, update CSV, proceed to PR |
| JS-rendered | Very short HTML, only `<div id="root">` | Seed with `status: needs_headless` and available info, skip smoke, proceed to CSV+PR |
| WAF/Cloudflare | 403, challenge page | Minimal seed with `status: paused_waf`, skip smoke, proceed to CSV+PR |
| Login required | 401/403 + login form | Minimal seed with `status: paused_login`, skip smoke, proceed to CSV+PR |
| Paywall | Subscribe wall, truncated content | Minimal seed with `status: paused_paywall`, skip smoke, proceed to CSV+PR |

**For all conditions:** update `lawjournals.csv` so the journal won't be re-selected, then always open a PR. The PR body must describe what happened and why.

---

## Quick Reference: Seed JSON Examples

WordPress (PDFs on listing page):
```json
{
  "id": "nulawreview-org",
  "start_urls": ["https://nulawreview.org/volume-50/"],
  "source": "claude_code_skill",
  "metadata": {
    "journal_name": "Northwestern University Law Review",
    "platform": "WordPress",
    "url": "https://nulawreview.org/",
    "created_date": "2026-03-23",
    "status": "active",
    "navigation": {
      "volume_url_pattern": "/volume-{N}/",
      "archive_path": "seed -> volume links in nav -> /volume-{N}/ lists articles",
      "notes": "PDFs linked directly on volume pages"
    }
  },
  "selectors": [
    {"id": "article_container", "type": "SelectorElement", "selector": "article.post", "parentSelectors": ["_root"]},
    {"id": "title", "type": "SelectorText", "selector": "h2.entry-title", "parentSelectors": ["article_container"]},
    {"id": "author", "type": "SelectorText", "selector": "span.author", "parentSelectors": ["article_container"]},
    {"id": "pdf_link", "type": "SelectorLink", "selector": "a[href$=\".pdf\"]", "parentSelectors": ["article_container"]}
  ]
}
```

DSpace (PDFs on article detail pages):
```json
{
  "id": "arizonajournal-org",
  "start_urls": ["https://repository.arizona.edu/handle/10150/679294"],
  "source": "claude_code_skill",
  "metadata": {
    "journal_name": "Arizona Journal of International and Comparative Law",
    "platform": "DSpace",
    "url": "http://arizonajournal.org/",
    "created_date": "2026-03-23",
    "status": "active",
    "navigation": {
      "volume_url_pattern": "https://repository.arizona.edu/handle/10150/{volume_id}",
      "archive_path": "seed -> Archive -> repository.arizona.edu/handle/10150/{id} lists articles",
      "notes": "PDFs on article detail pages via bitstream links"
    }
  },
  "selectors": [
    {"id": "article_container", "type": "SelectorElement", "selector": "li.ds-artifact-item", "parentSelectors": ["_root"]},
    {"id": "title", "type": "SelectorText", "selector": ".list-title-clamper", "parentSelectors": ["article_container"]},
    {"id": "article_url", "type": "SelectorLink", "selector": ".description-content a", "parentSelectors": ["article_container"]},
    {"id": "pdf_link", "type": "SelectorLink", "selector": "a[href$=\"pdf\"], a[href*=\"/bitstream/\"]", "parentSelectors": ["_root"]}
  ]
}
```
