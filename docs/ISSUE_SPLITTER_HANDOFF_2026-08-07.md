# Issue-splitter handoff — 2026-08-07

**For:** whoever picks up law-review issue splitting next.
**From:** an agent that spent a session authoring per-domain running-head rules, then
measured the thing those rules are graded by and found the grader is the weak link.
**Status of code:** `issue_splitter.py` is UNMODIFIED. Two data files were added/edited.

---

## 1. The question you are being handed

Splitting an issue-compilation PDF into child articles currently works like this:

```
per-domain running-head regex  ->  key changes  ->  boundary = (key change page - 1)
                               ->  looks_like_article_opening() gates the result
                               ->  <60% of boundaries passing = discard whole document
```

I extended that to 12 more domains and it works, in a narrow way. But two findings
say the architecture is wrong, and you should decide whether to keep patching it or
replace the spine:

1. **The running head is a lagging proxy.** It is recto-only in most journals, it
   arrives 0–2 pages after the real boundary, and the required back-off differs per
   journal *and sometimes per article within one issue*. This is not a tuning problem;
   it is a consequence of deriving position from the wrong signal.
2. **The gate is close to chance.** `looks_like_article_opening` measures
   **precision 0.508, recall 1.000** on a 138-page hand-labelled set. It never rejects
   a correct boundary and accepts about half of all continuation pages. So a rule
   emitting *entirely wrong* boundaries still scores ~0.5 against a 0.6 bar.

The decision in front of you is in §6.

---

## 2. Established facts (with evidence)

| Claim | Evidence |
|---|---|
| `looks_like_article_opening` is a one-sided filter | 138 labelled pages: TP=32 FP=31 FN=0. Precision 0.508 / recall 1.000 overall; 0.615 / 1.000 on the decisive stratum |
| Its false positives are mostly *unstripped running heads* | 17 of 31 FPs vanish by detecting heads via repetition instead of regex (v2 below), with zero new false negatives |
| Positive corroboration helps a lot | Requiring a byline or `Introduction` cue near the top (v3) → precision **0.865**, recall still **1.000**, FP 31 → 5 |
| The required back-off is per-journal | 2 for recto-title journals, 1 when the head resumes on page 2, 0 when a Word slug prints on every page including the opening |
| …and sometimes per-article | `www.swlaw.edu/27-sw-j-int-l-l-full-issue.pdf`: back-off 1 is right for some articles in the file, back-off 2 for others. No single value is correct |
| The validator cannot arbitrate offsets | Back-off 0 scores **1.00** for `law.howard.edu` and `sc.edu`, where 0 is definitely wrong. I built per-file offset auto-tuning against the validator and **abandoned it** for this reason |
| `doc_type: "issue_compilation"` already exists in every `.text.json` sidecar | Per-domain counts 32/27/28/30/31/37/23/42/26/17/16/16 — this is where the "N issues" numbers in task briefs come from |
| …but it over-triggers | It is page-count driven. `nyulawreview-73-6-golove` and `09-52uclalrev12004-2005` are both flagged and are both single ~150pp articles. Treat as a **candidate set**, not ground truth |
| Sidecars cannot drive a page splitter | `.text.json` is a flat blob, **0 page breaks**. Per-page pypdf extraction is required. Note liteparse text differs visibly (`Jour nal`, `olume 20`, `reversed_word_order_suspected`) — rules tuned on pypdf text are not tuned on what production reads downstream |

### Yield of the rules as they stand

Largest deduped files per domain, with the per-domain back-off applied:

| Domain | files split / sampled | articles | back-off |
|---|---|---|---|
| `www.texenrls.org` | 17 / 30 | 90 | 2 |
| `www.cschs.org` | 9 / 22 | 69 | 2 |
| `www.swlaw.edu` | 5 / 25 | 29 | 1 |
| `sc.edu` | 2 / 9 | 12 | 1 |
| `law.howard.edu` | 2 / 4 | 8 | 2 |
| `loynolawreview.org` | 1 / 20 | 7 | 0 |
| `journals.library.columbia.edu` | 1 | 3 | 2 |

Non-compilation domains resolved: `bclawreview.bc.edu` (every large PDF is one long
Annual Survey with a constant head, not a compilation), `nyulawreview.org`,
`www.uclalawreview.org` (per-article PDFs only), `www.yjil.yale.edu` (2-page mastheads
plus Colombian truth-commission and UN reports — not law-review content).
`www.abdn.ac.uk` is better served by the legacy TOC parser than by any head rule.

---

## 3. Artifacts

| Path | What |
|---|---|
| `offprint/offprint/pdf_footnotes/issue_head_rules_batch3.json` | 12 domains. 7 `pattern`, 4 `single_article_domain`, 1 inert `legacy_toc_domain`. Carries `boundary_backoff_pages` |
| `offprint/offprint/pdf_footnotes/issue_opening_gold.jsonl` | **New.** 138 labelled pages, 7 domains / 8 files. `_meta` records rubric, sampling, and the recall blind spot |
| `offprint/offprint/pdf_footnotes/issue_splitter.py` | Carries the per-domain back-off (see §4). Also `looks_like_article_opening`, the detector this brief measures |

`issue_head_rules.json` and `issue_head_rules_batch2.json` were off-limits to the
author of this brief; they have since been merged and are the file the splitter
actually loads. Merge new batches with
`offprint/scripts/processing/merge_issue_head_rules.py`, which refuses any entry not
explicitly marked usable rather than enabling it by default.

### Gold set composition

138 pages across 7 domains / 8 files: 32 opening, 103 continuation, 3 front matter.
Stratified — "candidate" =
pages the rule would emit at any back-off 0/1/2 (90 pages); "random" = uniform random
(48). Rubric fixed before reading; labelling done blind to the function's output.

---

## 4. Per-domain back-off — SHIPPED 2026-08-07, this section is history

This brief was written against a tree where the back-off was hardcoded at one page and
described the patch as pending. It has since landed in `boundaries_from_domain_rule`,
with two differences from the sketch below: the value is clamped to 0–3, and the merge
script normalises this brief's `boundary_backoff_pages` onto the shipped `back_off` key,
so batch3's values take effect as written.

```python
# as shipped
back_off = max(0, min(int(rule.get("back_off", 1)), 3))
starts.append(max(1, index - back_off))
```

Values were then calibrated per domain against the largest deduplicated issues, changing
a domain only where the default passed strictly fewer of them:
`feslr.com` 0 → 24 articles, `www.cschs.org` 2 → 33, `journals.law.harvard.edu` 0 → 73,
`btlj.org` 65 → 68. The `www.texenrls.org` risk this section used to flag is resolved —
it runs at back-off 2 and its boundaries land on display-title pages.

Do not auto-tune this value against the validator's opening share. See §7; the validator
scores 0.508 precision, and a back-off of 0 can score a perfect 1.00 where 0 is wrong.

---

## 5. The three running-head families

Everything I met is one of these. A generic detector for the three would likely cover
far more ground per unit effort than more per-domain regexes.

1. **Title-in-recto-head** (btlj, texenrls, cschs, swlaw, sc.edu, howard).
   Recto `<year>] <Title> <page>` or `✯ <Title> <page>` or `<Season> <Year> <Title> <page>`;
   verso is the journal name. Opening page has a display title and **no head**. Back-off 2.
2. **Word production slug** (loyno, administrativelawreview, cilj, houston).
   `(3) GARVEY.DOCX (DO NOT DELETE) 3/12/25 6:14 AM` on every page. Key = author
   surname. If the slug prints on the opening page too, back-off 0; if suppressed
   there, back-off 1.
3. **Folio + issue banner** (howard).
   Opening pages carry `<folio>` / `2025 Vol. 68 No. 3`; continuation pages carry a
   *different* head (`Howard Law Journal` / `<folio> [vol. 68:3`). The banner is a
   **positive marker of an opening**, not a running head — see the trap in §7.

---

## 6. Open design questions — the actual thinking to do

**Q1. Invert the architecture?**
The opening page (folio + display title + starred byline + `Introduction` + footnote 1)
is a near-journal-independent signal, and it is what I used as ground truth every single
time I judged a boundary. Head-derived boundaries are a proxy for it. If the opening
detector proposes and the head key merely corroborates, the entire back-off problem
disappears — there is no offset to get wrong. Counter-argument: v3 precision is 0.865,
not enough to propose unaided; and my recall figure does not cover openings the rules
never proposed (§7). What would it take to trust it as a proposer?

**Q2. Chase precision in the classifier, or in signal agreement?**
The residual 5 FPs are four named categories and ~3 rules from zero. But that would be
fitting to the 138 pages I tuned on, with no held-out set. The alternative is
triangulation: head-key change + opening appearance + TOC printed start + folio solver.
Independent signals fail in uncorrelated ways, and crucially TOC+folio is
**checkable** — the folio printed on the target page must equal the TOC's number. TOC
refs parsed in 10 of 11 validated compilations, and `sequence_solver.py` already exists
in the package for the folio half. My naive folio grab hit 93–100% of pages but mostly
grabbed the wrong number (low consecutive-run counts); it needs a global affine fit
`printed = physical + c`, not a per-page regex.

**Q3. Should the splitter abstain?**
100% precision is only reachable if the system may decline. Cost is asymmetric: a
mid-article cut yields two corrupt documents that enter the citation graph as real
(the shape of the 2026-07-22 header-contamination incident); a missed boundary just
leaves a compilation unsplit, which is the status quo. `skip_reason` already supports
declining. What precision bar justifies emitting at all? At ~30k child documents,
95% precision is ~1,500 corrupt children.

**Q4. What is LLM labelling for here?**
Not a per-page detector at corpus scale — 200k PDFs is tens of millions of pages. But
the heuristic's profile (**recall 1.000**, precision 0.5) is exactly a good first-stage
proposer, and adjudicating only *candidate* pages is ~10 per compilation, i.e. tens of
thousands of classifications: feasible on the local 2×3090 with an open-weight model.
Fits the standing policy (no priced-API runs without approval; default local vLLM);
needs GPU coordination with the census/labeling runs.

**Q5. How do we get honest gold?** See the circularity trap in §7.

---

## 7. Traps — these cost me real time, or nearly did

**The gold set was labelled by an LLM (me).** Scoring an LLM detector against it
measures self-consistency, not correctness. Get human adjudication on a subset, or run
independent multi-pass labelling and report agreement, before leaning on these numbers.
My labels embed judgment calls that move the figures: in-piece TOC pages → continuation;
department pieces (e.g. telj `AIR QUALITY` developments) → opening. Single pass. The
project already assumes a ~2–3% gold noise floor.

**Recall 1.000 is narrower than it sounds.** All 32 openings landed in the candidate
stratum; the 48 random pages contained none (expected ~1.7 at a ~3.5% base rate). So it
means *"never rejects a correct boundary the rules proposed"* — **not** *"finds every
opening in the corpus."* Openings the rules never propose are unsampled. Closing this
needs positives sampled independently of the rules, which a TOC/folio pass would give
for free.

**Do not auto-tune the back-off against the validator.** I built it; it picks back-off 0
for howard and sc.edu on a 1.00 tie, and 0 is wrong for both. Tie-breaking to the
smallest offset does not save it.

**Do not judge a boundary from its first three lines.** A reviewer marked
`law.howard.edu` `pattern_rejected` on 2026-08-07 for exactly this: they read
`<folio>` / `2025 Vol. 68 No. 3` as a running head. It is the opening-page banner and
appears *only* on opening pages. Reading the full page settles it — p27/p53/p105 each
carry a display title then a starred byline (`Kenneth B. Nunn*`,
`Migueyli Aisha Duran*`, `Summer Durant*`) then `Introduction` then footnote 1, while
p28/p54/p106 open `Howard Law Journal` / `<folio> [vol. 68:3` and resume mid-sentence.
I re-enabled it with that evidence recorded in the rule's notes. **Someone else was
editing `batch3.json` concurrently — check `git diff` before assuming a change is yours.**

**Sample from the `doc_type` census, not by file size.** I sampled by size and wasted
time on nyu/ucla/yjil. It landed on the same candidate set by luck, because
compilations are large — do not count on that.

**Duplicates inflate everything.** `llr-711-full-issue-archive*.pdf` is **16
byte-identical copies** of one issue. Dedupe by sha256 (`deduplicate_pdf_paths` already
exists) before counting anything.

---

## 8. Prototype detectors (not wired in — inline here because my scratchpad is session-scoped)

Measured on the 138-page gold: **v1 P=0.508 R=1.000 → v2 P=0.696 R=1.000 → v3 P=0.865 R=1.000.**

```python
import re
from collections import Counter
from offprint.pdf_footnotes import issue_splitter as S

def _norm(l):
    l = re.sub(r'\d+', ' ', l); l = re.sub(r'[^A-Za-z ]+', ' ', l)
    return re.sub(r'\s+', ' ', l).strip().upper()

def repeated_heads(page_texts, min_count=3):
    """A running head REPEATS across a document; a display title does not.
    This is the whole v2 idea, and it needs no per-domain regex."""
    c = Counter()
    for t in page_texts:
        for l in S._clean_lines(t)[:2]:
            n = _norm(l)
            if len(n) >= 6:
                c[n] += 1
    return {k for k, v in c.items() if v >= min_count}

def _body(page_text, heads):
    b = []
    for l in S._clean_lines(page_text):
        if not b and (S._is_running_head_line(l) or _norm(l) in heads):
            continue
        b.append(l)
    return b

def opening_v2(page_text, heads):
    body = _body(page_text, heads)
    if not body: return False
    run = 0
    while run < min(4, len(body)) and S._is_shouted(body[run]): run += 1
    if run and run < len(body) and S._is_resumed_prose(body[run]): return False
    f = body[0]
    if S._SECTION_LINE_RE.match(f): return True
    if len(f) < 8 or len(f) > 140 or not S._has_wordlike_content(f): return False
    if S._looks_like_sentence_prose(f): return False
    return S._upper_share(f) >= 0.6 or (f[:1].isupper() and not f.endswith((',', '-', ';')))

# v3 adds positive corroboration. NOTE the known bug: this BYLINE pattern also
# matches a lone all-caps word, so "CONCLUSION" counts as a byline - that is 2 of
# the 5 residual false positives. Require >=2 tokens or a trailing */dagger/"BY ".
BYLINE = re.compile(r"^(?:BY\s+)?[A-Z][A-Za-zÀ-ÿ.'’\-]+(?:\s+[A-Z][A-Za-zÀ-ÿ.'’\-]+){0,4}\s*[\*†‡0-9]?\s*$")
CUE = re.compile(r'^(?:I\.\s*)?(?:INTRODUCTION|ABSTRACT)', re.I)

def opening_v3(page_text, heads):
    if not opening_v2(page_text, heads): return False
    body = _body(page_text, heads)[:9]
    return (any(BYLINE.match(l) and len(l) < 60 for l in body)
            or any(CUE.match(l) for l in body))
```

### The 5 residual false positives, and why each survives

| gold id | page | cause | fix |
|---|---|---|---|
| 42, 69 | loyno p164, sc.edu p40 | `CONCLUSION` matches BYLINE | require ≥2 tokens or trailing `*`/`†`/`BY ` |
| 99 | swlaw p5 | faculty roster; name lines match BYLINE | front-matter rule: page that is *mostly* bylines is a roster |
| 62, 130 | howard p107, columbia p214 | in-piece TOC; `I. Introduction` cue fires with no title above it | require the cue to sit *below* a title line, not replace it |

Hold these back until there is a held-out set — otherwise you are fitting to the
sample you are measuring on.

---

## 9. My opinion (clearly marked as opinion)

Build the folio solver next, not more rules. It is the only step that converts this
work from eyeball-validated to **checkable**, it yields boundary positives sampled
independently of the head rules (which is exactly what the gold set is missing), and
`sequence_solver.py` is already sitting in the package. Then re-measure v2/v3 against a
gold set that includes rule-invisible openings, and only then decide whether the
opening detector is strong enough to become the proposer.

`issue_head_rules_batch3.json` is a good local optimum and should be kept — it is
hand-validated and it is the only thing that works today. It just will not scale to
hundreds of domains at roughly an hour of authoring each.

---

## 10. Reproducing the measurements

Everything below needs `warnings.filterwarnings('ignore')`,
`logging.getLogger('pypdf').setLevel(logging.CRITICAL)`, `PdfReader(path, strict=False)`,
and per-page `extract_text()`. Cache the page text — the cschs volumes are 600+ pages
and re-extraction dominates runtime.

- **Score the detector:** load `issue_opening_gold.jsonl`, re-extract each `(file, page)`,
  compare `looks_like_article_opening` (and the v2/v3 prototypes above) against `label`,
  treating `F` as non-opening. Report the candidate stratum separately — it is the
  operationally decisive one.
- **Yield sweep:** apply the §4 patch, then run `infer_law_review_boundaries` over the
  largest basename-deduped PDFs per domain and count `ok` results.
- **Candidate census:** `grep -l '"doc_type": "issue_compilation"' corpus/scraped/<domain>/*.text.json`
