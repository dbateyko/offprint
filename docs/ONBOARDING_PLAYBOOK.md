# Law-Review Sitemap Onboarding Playbook

A paste-ready brief for an LLM agent (Gemini, Claude, etc.) onboarding new
law-review sitemaps into this corpus pipeline. Distilled from a session of
running `/onboard-journal` across ~10 journals and reconciling 300+ registry
rows. Read this **before** the formal skill spec at
`skills/onboard-journal/SKILL.md` (or
`/home/dbateyko/.claude/skills/onboard-journal/SKILL.md`).

---

## Where to start

1. Read `data/registry/lawjournals.csv`. Filter to rows where:
   - `status` ∈ {`no_sitemap`, `todo_seed`, `todo_subseed`, ""}
   - `sitemap_file` is empty
   - `wlu_rank` is non-empty
2. Sort by `wlu_rank` ascending. Onboard in that order.
3. **Before exploring, check what already exists**: `ls offprint/sitemaps/ | grep -i {host_slug}`. Half the candidates are already done — the registry is the unreliable side.
4. Inspect any existing seed file before assuming it works:
   ```bash
   python3 -c "import json; d=json.load(open('PATH')); print(d.get('start_urls'), len(d.get('selectors',[])))"
   ```
   Many seed files are empty stubs (no `start_urls`, no `selectors`). Pointing the CSV at them looks like progress but isn't.

## Hard rules learned the slow way

### Digital Commons gate (non-negotiable)
If the host contains `digitalcommons` OR the HTML has `bepress` in meta tags or scripts: **stop**. Write a stub seed with `status: todo_adapter` and `platform: digitalcommons`, update the CSV, and exit. A bulk DigitalCommonsBaseAdapter handles all DC sites centrally — onboarding them one-by-one is wasted work.

### Sub-publication suffix guard
Sub-publications need their **own** seed. Never point them at the parent journal's sitemap. Recognized suffixes:
`Online`, `Discourse`, `Arguendo`, `Forum`, `Sidebar`, `En Banc`, `Supplement`, `Companion`, `Bulletin`, `Reflection`, `Circuit Review`, `Et Cetera`, `Commentaries`, `Blog`.

When matching: if the CSV journal name has any of these, the candidate seed must reference the same suffix in its filename, `journal_name`, or `start_url`. Otherwise reject.

### Generic URL slugs are not distinctive
Don't disambiguate on these — they're noise:
`/category/`, `/ojs/`, `/books/`, `/law_reviews/`, `/archive/`, `/current-issue/`, `/issues/`, `/index.php/`, `/journals/`, `/publications/`.

### Adapter registration is required
After writing the seed, **register the host** in `offprint/adapters/registry.py`. Without it, smoke fails with `UnmappedAdapter`. Both the bare host AND the `www.` variant. Look for the existing `register_many([...], WordPressAcademicBaseAdapter)` block (or the equivalent for other adapter classes).

### Adapter limitations to know about
- `WordPressAcademicBaseAdapter` often grabs the section H2 or page `<title>` instead of the per-article title. **This is an adapter limitation, not a seed problem.** The seed selectors are usually correct but the adapter ignores them. Don't chase it — pass with what you get.
- Volume/issue often encoded in CSS classes: `tag-volume-N`, `volume-volume-N`, `vol-N`, `volumeandissue-vol-N-no-M`. `_extract_volume_issue` in `wordpress_academic_base.py` reads these as a fallback (added in this session). Themes with different conventions still need work.

### Smoke test must download a real PDF
A passing curl HEAD proves nothing. Run:
```bash
echo "{start_url}" > /tmp/smoke.txt
python scripts/smoke_one_pdf_per_site.py \
  --sitemaps-dir offprint/sitemaps \
  --target-file /tmp/smoke.txt \
  --out-dir /tmp/onboard_pdfs \
  --report-dir /tmp/onboard_report \
  --max-workers 1 --max-depth 2 \
  --playwright-headless
```
Verify a `.pdf` file actually appears in `/tmp/onboard_pdfs/`. Quality gate: ≥1 PDF on disk + non-empty title + ≥2 of {authors, volume, year}.

### Dead URLs
If `curl` returns 404 or times out, **don't give up immediately** — `WebSearch` for the journal name. Many journals have moved (e.g. Ohio State Business LJ moved from `moritzlaw.osu.edu/osblj` to `kb.osu.edu`). Note the redirect in the seed.

## Seed JSON shape (minimum viable)

```json
{
  "id": "{slug}",
  "start_urls": ["{archive_root}"],
  "source": "claude_code_skill",
  "metadata": {
    "journal_name": "...",
    "platform": "wordpress|ojs|scholastica|squarespace|drupal|dspace|...",
    "url": "{canonical homepage}",
    "created_date": "YYYY-MM-DD",
    "status": "active",
    "navigation": {
      "archive_root": "...",
      "pagination": "none | html-next-link | ?page=N | /page/N/ | sitemap.xml | RSS | REST API | JS/headless",
      "pdf_location": "direct-on-issue-page | article-detail-page",
      "needs_headless": false,
      "notes": "..."
    }
  },
  "selectors": [
    {"id": "article_container", "type": "SelectorElement", "selector": "...", "parentSelectors": ["_root"]},
    {"id": "title", "type": "SelectorText", "selector": "...", "parentSelectors": ["article_container"]},
    {"id": "pdf_link", "type": "SelectorLink", "selector": "a[href$=\".pdf\"]", "parentSelectors": ["article_container"]}
  ]
}
```

**Selector rules**: never use generated/hash class names (`TmK0x`, `css-1a2b3c`, `sc-abc123`). Prefer `article.post`, `div.issue-toc li`, `a[href$=".pdf"]`, `h3.entry-title`. For Wix/React with only generated classes, use tag-based selectors.

## Pagination patterns by platform

| Platform | Pattern |
|---|---|
| WordPress | `/page/N/` (often) or `?paged=N` |
| OJS | `/index.php/{journal}/issue/archive` paginated `?page=N` |
| DSpace 7 | REST: `/server/api/discover/search/objects?scope={community-uuid}` (use `/collections/{uuid}` URL form, not `/handle/...`) |
| Scholastica | `window.JOURNAL` extraction; flat archive at `/articles` |
| Squarespace | Often `?offset=N` |

Always look for `/sitemap.xml` first as a shortcut — many sites enumerate every article URL there.

## Status conventions

| Status | Meaning |
|---|---|
| `active` | Smoke passed |
| `active_partial_metadata` | Smoke passed but adapter loses some fields |
| `todo_adapter` | Digital Commons site, deferred to bulk DC adapter |
| `todo_subseed` | Sub-publication of an onboarded flagship; needs distinct sitemap |
| `todo_seed` | Real journal needing fresh sitemap |
| `paused_waf` / `paused_login` / `paused_paywall` / `paused_404` | Blocked |

## Parallel onboarding

When running multiple onboardings concurrently:
- Each agent **only** writes its sitemap JSON + edits `adapters/registry.py`.
- An orchestrator handles the CSV update + commit centrally to avoid merge conflicts on `lawjournals.csv` (3 agents writing to it simultaneously WILL clobber each other's changes).
- Each agent reports back its proposed CSV row updates as a key:value list; orchestrator applies them in one pass.

## Git flow

- Repo on `main`, often diverged from `origin`. Don't push or open PRs unless asked. Commit locally with concise `feat:`/`chore:` messages.
- Don't bundle pre-existing untracked WIP into your onboarding commit; stage only the seed JSON, `adapters/registry.py`, and the CSV row.
- Watch for working-tree truncations of `lawjournals.csv` (~1400 rows is the canonical size; if you see ~500, check `git diff` before committing — someone may have accidentally truncated it in another session).

## Meta-lesson

The registry CSV and the on-disk sitemaps drift apart constantly. **Run a sync pass first** (read every sitemap JSON, propose CSV updates) before picking an onboarding target — otherwise you waste effort onboarding journals that are already done.

A reference sync script lives in the session log; the conservative version is:
- Build host → sitemap-metadata index from `offprint/sitemaps/*.json` (skip `registry_*.json` stubs).
- For each CSV row, match by exact `journal_name` first, then by unique host.
- Fill empty CSV fields (`sitemap_file`, `platform`, `host`, `url`).
- Upgrade `no_sitemap`/`todo_adapter` → `active` only if the seed has `status: active`.
- Never overwrite a populated `sitemap_file` unless the file is missing.
- Never downgrade an `active` status.
