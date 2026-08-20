# TOC-driven boundary solver — build and first results, 2026-08-07

Answers the design question left open in `ISSUE_SPLITTER_HANDOFF_2026-08-07.md` §6:
replace the running-head spine with a document-level matching solver in which the
contents listing is the specification, the printed folio stream is the locator,
and the running head is corroboration only.

**Status:** built, tested, measured on two held-out sets and swept over 604
compilation candidates. Not wired into `run_issue_split`; nothing has been split.

## What was built

| Path | What |
|---|---|
| `offprint/pdf_footnotes/toc_solver.py` | the solver (~900 lines) |
| `tests/test_toc_solver.py` | 21 tests over a synthetic issue + the known traps |
| `scripts/processing/run_toc_solver.py` | batch runner → one evidence-ledger row per document |
| `scripts/processing/evaluate_toc_solver.py` | whole-issue scoring |
| `offprint/pdf_footnotes/issue_boundary_gold.jsonl` | 9 hand-read issues (**contaminated**, see below) |
| `offprint/pdf_footnotes/issue_boundary_gold_v2.jsonl` | 4 hand-read issues (uncontaminated) |

Pipeline, per document:

1. **Structured contents entries** — printed start page, title, author, section
   type. Contents pages are found by *listing score* (rows shaped like entries,
   minus rows that are numbered section headings), not by a `CONTENTS` heading:
   Chapman heads its real listing `ARTICLES` and prints `TABLE OF CONTENTS` only
   inside its first article, so an anchor-driven search picks the wrong page and
   splits one article into its own sections.
2. **Column-aware row reconstruction** — contents listings are typeset in
   columns and every extractor linearises them differently. UConn's comes out as
   `Katherine` / `85` / `INSURANCE ERA: RISK,` / `Hempstead`. Lines are re-joined
   by vertical overlap and sorted by x, which recovers the printed row. Applied
   to the listing only; applying it to body text would merge a two-column
   journal's columns into nonsense.
3. **Layout-mode detection** — Chapman prints the page number on an entry's last
   row, American University on its first. Both readings are built and the one
   recovering more entries wins. Assuming one convention swallows the first entry
   of every listing using the other.
4. **Global folio fit** — `printed = physical + offset` by consensus over every
   folio candidate, scored by the **longest consecutive run** of agreeing pages,
   not by vote count. A volume number in the head votes for a constant offset
   too; it does not increment with the page, so its run is 1.
5. **Monotonic maximum-score assignment** — DP with a prefix maximum over the
   previous row, so boundaries are decided jointly.
6. **Per-entry margin** — the DP is re-run with each chosen page forbidden. A
   boundary the document does not actually determine has a margin near zero
   whatever its own score is.
7. **Three-way emission with an evidence ledger.**

Scoring, per (entry, page): folio agreement +3.0; offset-implied page +1.5;
title similarity ×3.0 when ≥0.55; author surname in the top region +2.5; opening
appearance +1.5; head transition within 0–2 pages **+0.75**; continuation prose
−3.0; roster or in-piece contents −3.0.

## Emission policy

Per boundary, not per document. "The contents listing counts about as many
entries as the head rule found" does not appear anywhere — agreement is required
entry by entry:

- **auto** — every substantive boundary has `folio + (title | author)` or
  `title + author + opening appearance`, **and** margin ≥ 2.0, **and** a folio
  offset was fitted.
- **review** — every boundary has at least one strong signal, but at least one
  rests on a single signal or a thin margin.
- **abstain** — anything else: no usable listing, a boundary landing on
  continuation prose, no feasible monotonic assignment.

A running-head transition is never sufficient on its own — `test_head_transition_alone_never_authorises_a_split` pins this.

## Results

### Held-out set v2 (uncontaminated — the number to quote)

4 issues, 2 real compilations and 2 single articles, from domains disjoint from
both the running-head rule development and the v1 gold. The solver was not
modified after scoring it.

| tier | issues emitted | boundary precision | boundary recall | issues fully correct | corrupt children | non-compilations split |
|---|---|---|---|---|---|---|
| `auto` | 0 / 4 | — | — | — | 0 | 0 / 2 |
| `auto`+`review` | 1 / 4 | **1.000** | **1.000** | 1 / 1 | **0** of 8 | 0 / 2 |

The one emitted issue (Clinical Law Review 30:1, 229 pp, 8 pieces) was placed
exactly right at every boundary. `www.jlep.net` was missed — abstained with
`no_usable_toc`, a recall loss, not a bad cut.

### Held-out set v1 (contaminated — read as an upper bound)

9 issues. **Three contents-parser bugs were found and fixed against this set**
(masthead rows parsing as entries, the layout-mode assumption, an over-broad
enumerator filter), so these figures are optimistic and are reported as
development diagnostics, not as a held-out measurement.

| tier | issues emitted | boundary precision | boundary recall | issues fully correct | corrupt children | non-compilations split |
|---|---|---|---|---|---|---|
| `auto` | 1 / 9 | 1.000 | 1.000 | 1 / 1 | 0 of 3 | 0 / 5 |
| `auto`+`review` | 5 / 9 | 0.931 | 0.964 | 3 / 5 | 2 of 29 | 1 / 5 |

Both corrupt children come from one file: `tilj.org/tilj-59n3-text-cavallaro.pdf`,
a single article that the `review` tier proposed splitting at pages 2 and 25.
At `auto` it is correctly refused.

On the four real compilations in v1, the assignment is **exactly right at every
boundary** — including `law.howard.edu` p11/27/53/105/139, the pages the previous
brief hand-verified, and Chapman p12, where the head-derived approach needs a
per-journal back-off and gets it wrong.

### Corpus sweep

604 compilation candidates (≤3 per domain, 252 domains, size-deduped) out of
11,035 `doc_type: issue_compilation` sidecars:

| status | documents | children | domains |
|---|---|---|---|
| `auto` | 36 (6.0%) | 215 | 30 |
| `review` | 81 (13.4%) | 559 | 63 |
| `abstain` | 487 (80.6%) | — | — |

Abstention reasons: `no_usable_toc` 182, `insufficient_evidence` 133,
`too_few_pages` 131, `no_feasible_assignment` 41.

Per-boundary strong-signal combinations across the 215 `auto` children:
`folio+title+author` 135, `folio+title` 43, `folio+author` 19, `title+author` 18.
Median folio support on `auto` documents is 0.97 — the offset is a consequence of
the whole page stream, not of one lucky page.

## What this does and does not settle

**Settled.** The architecture works and it is checkable. Boundaries are placed
directly on opening pages, so the back-off problem does not exist: there is no
offset to get wrong, and Southwest-style per-article variation cannot arise. Both
held-out sets produced zero corrupt children at `auto`, and zero non-compilations
split at `auto` across 7 single-article files including three that carry their own
table of contents — the failure mode that would turn one article into a dozen
corrupt children.

**Not settled.**

- **Coverage is low.** 6% at `auto`. Half of all abstentions are
  `no_usable_toc`; some of those files genuinely have no listing (NYU's *Women &
  Law*, JLEP) and some have one the parser cannot yet read.
- **The gold is far too small to certify the 99.9% bar you named.** 4
  uncontaminated issues and 8 boundaries bound nothing tightly. At 215 children
  per 604 candidates, a corpus-scale `auto` run is on the order of 4,000 children;
  99.9% precision means ≤4 bad cuts, and nothing here measures at that
  resolution. Getting there needs hundreds of issues, and the labels have to stop
  being single-pass LLM readings — the same circularity trap §7 of the handoff
  flagged applies to this work unchanged.
- **`review` is not safe to emit unattended.** It is right about the boundaries
  it proposes far more often than not, but it split a single article in v1. It is
  a queue, and its size (81 documents / 559 children per 604 candidates) is well
  within reach of local open-weight adjudication.
- **Residual defects visible in the sweep.** Some `auto` documents emit a junk
  first child from a masthead row that survives the masthead guard
  (`jost.syr.edu`: first entry titled `Science & Technology Law Volume 36…`), and
  one entry took a year as its printed page (`printed 1974`). These produce a bad
  *first* child, not a mid-article cut, but they are real and unfixed.

## Reproducing

```bash
cd offprint
python -m pytest tests/test_toc_solver.py -q

python scripts/processing/run_toc_solver.py \
  --pdf-list <(grep -rl '"doc_type": "issue_compilation"' ../corpus/scraped --include='*.text.json' \
               | sed 's/\.text\.json$//') \
  --out ledger.jsonl --root ../corpus/scraped --workers 10

python scripts/processing/evaluate_toc_solver.py \
  --gold offprint/pdf_footnotes/issue_boundary_gold_v2.jsonl --root ../corpus/scraped
# add --include-review to score the review tier as if emitted
```

---

# Follow-up: abstention diagnosis and gold-source check, same day

## 1. The candidate set is mostly not compilations

`doc_type: issue_compilation` over-triggers far worse than "treat as a candidate
set" suggests. Of the 604 sampled files:

- **131 abstained as `too_few_pages`** — median length **4 pages**. Zero of them
  carry three or more plausible openings. These are not compilations by any
  reading.
- An independent opening census (the v3-style detector used as a *census* tool,
  not a splitter: ≥3 detected openings ≥4 pages apart) calls **282 of 604 (47%)**
  plausibly compilations — and that is an over-count, because a 180-page single
  article has many section-opening pages. Hand-reading 20 `no_usable_toc` files
  put the real rate in that bucket at **35%**.

**Coverage restated over plausible compilations rather than raw candidates:
`auto` 12%, `auto`+`review` 37%** (was 6% / 19% against the raw candidate set).
97% of `auto` documents and 85% of `review` documents are plausible compilations,
so the solver is not emitting on junk.

## 2. Where the missed compilations actually are

Hand-read of 20 `no_usable_toc` files (≥40pp): **13 correct abstentions** (single
articles, a USDA agricultural census, a UN report, a legislative history), **7
real compilations missed**:

| cause | n | fixable? |
|---|---|---|
| scanned, no extractable text (`digital-commons.usnwc.edu` ×2) | 2 | needs OCR — out of scope here |
| OCR mojibake (`supremecourthistory.org`) | 1 | no |
| no issue-level listing exists (`chicagounbound.uchicago.edu` whole-issue PDF) | 1 | no |
| **readable listing the parser failed on** (`jlep.net`, `southernlawjournal.com`, `cjil.law.uconn.edu`) | **3** | **yes** |

So roughly 40% of missed compilations in this bucket are parser-fixable; the rest
need OCR or have nothing to read.

## 3. `insufficient_evidence` is the in-article-TOC failure, and it is one fix

All 8 sampled `insufficient_evidence` files are **single articles whose own table
of contents was parsed as the issue listing**. Entries read `B. Hypothesis Two:
Exclusion of Critical Sc…`, `a. Settlement Effects`, `List of Figures and
Tables`. The solver reaches the right answer (abstain) but by the wrong route:
the downstream continuation-prose guard catches it, not the listing filter.

`_ENUMERATOR_RE` only tests the *start* of a title, and row-merging pushes the
enumerator into the middle (`Recently B. Hypothesis Two: …`). The fix is to judge
the listing as a whole — a listing whose entries are dominated by section-heading
shapes anywhere in the string, or which recovers no authors at all, is an
in-article contents page. This is the same defence that prevents the worst
available failure (one article → a dozen corrupt children), so it is worth
hardening even though the current outcome is already correct.

## 4. The free-gold idea is dead — checked and closed

The hope was that cross-source records could supply boundary gold mechanically,
with no LLM labelling: Anna's Archive metadata carries `start_page`/`end_page`
for every record.

```
aa records with start_page AND end_page ........ 18,166 (100% of aa)
donation records with start_page ...............      0 (0%)
aa issue-groups with >=3 paginated articles
    and a real volume+issue ....................     94
scraped containers in containers_to_split
    with a real journal+volume+issue ...........    465
--> scraped containers matching an aa roster ...      0
```

Zero. Not a join bug: the AA paginated metadata covers **6 journals**, the
scraped containers cover **25**, and the intersection is **empty** — no overlap at
journal level, let alone volume or issue. Donation records carry no pagination at
all.

**Consequence: there is no mechanical route to boundary gold in the current
catalog.** Any gold at the scale needed to certify a precision bar has to be
labelled, and the labels must not be a single LLM pass — the circularity trap
from the previous handoff applies to this work unchanged.

Incidental: `catalog/article_inventory/containers_to_split.parquet` (1,996 rows,
548 scraped, 1,367 `needs_split`) is a far better-curated candidate set than the
11,035-file sidecar census, and carries journal/volume/issue. Future runs should
start there.

---

# Follow-up 2: hardening the listing filter, and the adjudicator scope

## Four changes

The diagnosis said the in-article contents page was the dominant confusion. Four
changes went in; the third and fourth exist only because the first two, measured,
were wrong in ways the gold sets caught.

1. **Whole-listing in-article filter** (`_listing_is_in_article`). A listing is
   rejected when ≥25% of its entries are *both* malformed (title starting
   mid-phrase in lower case, or carrying an embedded section enumerator) *and*
   unauthored.

   The first version judged on shape alone — fragments ≥0.25 or enumerators ≥0.20
   or no authors at all. It looked perfect on the twelve files used to pick the
   thresholds, and the sweep then showed it demoting **six** documents out of
   `auto`, of which **five were real compilations**. Real listings wrap their
   titles too, and row reconstruction can split a wrapped title across anchors
   (`for the Average Worker  Lisa A. Nagele-Piazza`). The author is what tells
   the two apart. The standalone no-author rule was dropped as well: it killed
   `www.fclj.org`'s annual review, which lists case names with no authors and is
   a real multi-piece document.

2. **Folio-range veto**. The listing's numbers must fall inside the document's
   observed folio range; otherwise abstain with `toc_outside_folio_range`. This
   is what makes TOC + folio mutually *checkable* rather than merely combined.

3. **The listing arbitrates between competing folio streams**
   (`_offset_agreeing_with_toc`). Digital Commons stamps a sequential page number
   on every page of a scanned issue. That stamp never skips, so it wins the
   consensus fit outright — `nsuworks.nova.edu/Vol._38_2C_Number_3.pdf` fits
   offset −1 at support 1.00 against the journal's real folios 387–523 at 0.857 —
   and every contents entry then belongs to no page in the document. Among
   candidate offsets with real support (≥8 pages and ≥20%), the one placing the
   most entries on a page that actually prints their number wins.

4. **The listing must sit in the front matter** — within the first
   `max(15, 15% of pages)`. `tilj.org/tilj-59n3-text-cavallaro.pdf` puts an
   ICC-convictions table on page 18 of 30; parsed as a listing it splits a single
   article in two. This was the last surviving corrupt-child case, and it came
   back twice under earlier versions of changes 1 and 3 before this fix.

## Effect

Held-out gold, unchanged solver after scoring:

| set | tier | emitted | precision | recall | corrupt children | non-compilations split |
|---|---|---|---|---|---|---|
| v2 (uncontaminated) | `auto` | 0/4 | — | — | 0 | 0/2 |
| v2 | `auto`+`review` | 1/4 | 1.000 | 1.000 | 0 of 8 | 0/2 |
| v1 (contaminated) | `auto` | 1/9 | 1.000 | 1.000 | 0 of 3 | 0/5 |
| v1 | `auto`+`review` | 4/9 | **1.000** (was 0.931) | 0.964 | **0** of 27 (was 2 of 29) | **0**/5 (was 1) |

**The `review` tier is now clean on every gold issue** — 35 of 35 boundaries
correct across both sets, no corrupt children, no single article split.

Sweep over the same 604 candidates, coverage measured against the 282 plausible
compilations:

| | `auto` | `auto`+`review` | children |
|---|---|---|---|
| before | 12% | 37% | 774 |
| after | 12% | 34% | 704 |

The hardening costs 9% of children and buys the elimination of the only known
corrupt-child cases. `insufficient_evidence` fell 133 → 61 and
`no_feasible_assignment` 41 → 9, both absorbed into `no_usable_toc` (182 → 286) —
the solver now declines to *read* in-article listings rather than reading them
and being rescued downstream.

## Adjudicator scope (measured, not estimated)

From the sweep, per `review` document: **7.3 boundaries, of which 40% are the
weak ones** needing a decision. The rest already carry two independent strong
signals and are only in `review` because a sibling boundary is weak.

Strong-signal combinations across `review` boundaries: `folio+title` 112,
`folio+title+author` 98, `title+author` 79, `folio` alone 78, `folio+author` 54,
`author` alone 37, `title` alone 35, none 1.

So the adjudicator is not a page classifier. Its job is narrow: **given a
boundary that has one strong signal and a thin margin, confirm or reject the
proposed opening page.** Extrapolated to the corpus (~1,500 `review` documents),
that is ~11,000 boundaries of which ~4,400 need a decision — one prompt each,
against a page of text plus the ledger's signal summary. Comfortably a local
open-weight run on the 2×3090; GPU-gated against the census/labeling queue.

Still true: nothing is wired into `run_issue_split`, and the gold remains too
small (13 issues, 4 uncontaminated, all single-pass LLM labels) to certify any
precision bar.

---

# Follow-up 3: the real denominator, and the adjudicator harness

## Coverage against `containers_to_split.parquet`

The `doc_type` sidecar census was never the right population. Re-run over the
curated container list — 548 scraped PDF containers, 491 resolvable on disk, 205
after size-dedupe:

| status | documents | share | children | domains |
|---|---|---|---|---|
| `auto` | 21 | 10% | 145 (median 6/doc) | 8 |
| `review` | 108 | 53% | 713 (median 5/doc) | 10 |
| `abstain` | 76 | 37% | — | — |

**`auto`+`review` covers 63% of curated containers**, against 34% measured on the
noisy `doc_type` set. Same solver, same day — the difference is entirely the
denominator. Abstentions here are `insufficient_evidence` 39, `no_usable_toc` 32,
`no_feasible_assignment` 3, `toc_outside_folio_range` 2; the `too_few_pages`
bucket that dominated the sidecar census is absent, as it should be.

Caveat: this set is concentrated in ~10 journals, so it is a better denominator
but a narrower one. Coverage on journals outside it is unmeasured.

## Adjudicator harness

| Path | What |
|---|---|
| `scripts/processing/build_adjudication_queue.py` | `review` boundaries → blind queue |
| `scripts/processing/adjudicate_boundaries.py` | run against local vLLM, then score |

`Assignment` now records `runner_up_page` — where an entry goes when its chosen
page is forbidden, which the margin DP already computes. That is the boundary an
adjudicator actually has to choose between, so it is stored rather than
recomputed.

**The queue is blind.** Each item names the piece from the contents listing and
shows a window of candidate pages in document order with the top of each page's
text. It does not say which page the solver chose, mark the runner-up, or quote
the margin — `test_the_prompt_never_reveals_the_solver_choice` pins this.
Agreement measured under anchoring is not evidence, which is the lesson §7 of the
handoff paid for.

Built from the container run: **351 items across 108 documents** — exactly the
weak-boundary count predicted (49% of 713 `review` boundaries). Median 7
candidate pages per item, ~1,000 tokens per prompt, ~0.3M tokens for the whole
queue. Minutes on one 3090, not hours.

The runner follows `offprint-data-ops/labeling/annotate_gold_27b.py`: `OpenAI`
client against `localhost:8000/v1`, `response_format` json_schema (vLLM ignores
legacy `guided_json`), temperature 0, thinking disabled, resumable. Scoring
reports agreement, disagreement, `none_of_these`, and which documents have every
weak boundary confirmed.

**Not yet run — needs vLLM up, which contends for the same two GPUs as the census
and labeling queues.** Both GPUs were idle at the time of writing (15 MiB used,
0% utilisation, no vLLM/labeling/census process).

One defect the first rendered prompt exposed: on `www.law.nyu.edu` NYU Annual
Survey, the layout-mode detection folded the author into the title
(`...A RETURN TO FIRST PRINCIPLES Mark A. Perry and Rachel S. Brass`, author
field empty). The adjudicator still sees the author text, so the item is usable,
but the entry parse is wrong and `author` as a scoring signal is lost for that
listing.

---

# Follow-up 4: the adjudicator ran, and found a solver bug

Served `Qwen3.5-9B` (bf16, ~17.7 GiB) on one 3090 via vLLM. First attempt OOMed
during CUDA-graph capture at `--gpu-memory-utilization 0.85`; `--enforce-eager`
with 0.93 fits and leaves GPU 1 free. Total GPU time for everything below was
well under an hour.

## The control is what makes the numbers readable

Running the blind queue on `review`-tier weak boundaries gave **74.0% agreement
with the solver** (253/342). On its own that number says nothing: it is
consistent with a good adjudicator catching a bad solver, a bad adjudicator
disagreeing with a good solver, or both being noisy.

So the same blind task was run over a **control set**: all 145 boundaries from
the 21 `auto` documents. Those carry two independent strong signals and a fat
margin, so they are near-certainly right, and agreement on them measures the
*adjudicator*.

| set | boundaries | agreement |
|---|---|---|
| control (`auto`, near-certainly correct) | 145 | **98.6%** (143/145) |
| `review` weak boundaries | 342 | **74.0%** (253/342) |

The adjudicator is accurate. The 26-point gap is mostly the solver.

## Reading the disagreements: two classes, and only one favours the model

**Class A — the solver was wrong, one page late.** `btlj.org` repeatedly: p147
carries `CYBERCRIMES & MISDEMEANORS / A REEVALUATION... / By Reid Skibell†` and
p148 opens `910 / BERKELEY TECHNOLOGY LAW JOURNAL / [Vol. 18:909`. The solver
chose p148 because `[Vol. 18:909` **matched the contents entry's printed start
page 909** — the volume:page citation in the running head was being read as a
folio. It appears on every continuation page of the article, and because a
law-review opening page often prints no folio at all, the boundary landed one
page late. That is exactly the off-by-one this whole design exists to prevent,
reintroduced through the folio channel.

**Class B — the model was wrong, it picked the contents page.**
`annualsurveyofamericanlaw.org`: the model chose the `SUMMARY OF CONTENTS` page
because it "shows the display title". It does — the contents listing repeats
every title.

So neither party is uniformly right, and **the adjudicator must be used as a gate
(confirm-only), never as a relocator.** Class B means it will also wrongly reject
some correct boundaries.

## Fixing class A

`_VOL_PAGE_RE` now strips `Vol. 18:909` / `[Vol. 24:975` constructions before
folio candidates are read off a line. Effect on the 205 curated containers:

| | `auto` | `review` | `abstain` | children | weak boundaries |
|---|---|---|---|---|---|
| before | 21 | 108 | 76 | 858 | 351 of 713 |
| after | 21 | 85 | 99 | 698 | 227 of 553 |

Boundaries moved in **49 documents**. Coverage fell — 23 documents dropped from
`review` to `abstain` — because their folio "evidence" was this artefact and they
never had real folio support. Both gold sets are unchanged (precision 1.000, zero
corrupt children); 281 tests pass.

Re-adjudicating the corrected queue:

| | items | agreement | documents fully confirmed |
|---|---|---|---|
| before fix | 342 answered | 74.0% | 57 of 108 |
| after fix | 222 answered | **87.4%** | 60 of 85 |

Fewer boundaries need a decision, and the ones that remain agree far more often.

## What this leaves

- The `review` tier is **not** safe to emit unattended even now: 12.6% of its
  weak boundaries still disagree with an adjudicator that is 98.6% accurate on
  easy cases.
- Confirm-only gating currently promotes **60 of 85** `review` documents.
- ~5 items per run fail JSON parsing when the model's `evidence` string runs past
  `max_tokens`; raising it to 400 fixed most but not all. Constrain the field
  length rather than the token budget.
- The control-set method is the reusable part. Any future adjudicator, model, or
  prompt should be scored against known-good boundaries before its disagreements
  are believed.
