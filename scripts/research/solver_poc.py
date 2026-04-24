#!/usr/bin/env python3
"""Proof-of-concept: global sequence solver for footnote extraction from LiteParse.

Replaces the current stack of local classifiers + rejection regexes + outlier trimmers
with a two-phase pipeline:
  (1) collect all plausible label candidates from raw textItems
  (2) solve for the longest monotonically-increasing sequence with small deltas and
      forward-progressing spatial positions, weighted by per-candidate confidence

Run against a pre-selected 40-doc test set and compare to the candidate-ensemble
baseline (liteparse_only_1k.json).
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

LIT_CLI = "/home/dbateyko/.nvm/versions/node/v22.16.0/bin/lit"

# ---------- Phase 1: label candidate extraction ----------

# Allow a single period-or-bracket trailer OR a repeated-period trailer ("2..").
# Keep the tail tight — mixed punctuation or commas break things (they match stray
# citation digits like "1, 2" which are not labels).
LABEL_DIGIT_RE = re.compile(r"^(\d{1,4})(?:[\.\)\]]|\.{2,})?$")

# Line-start label: same tight trailer as above.
LABEL_START_RE = re.compile(r"^\s*(\d{1,4})(?:[\.\)\]]|\.{2,})?(?:\s|$)")

CITATION_SIGNAL_RE = re.compile(
    r"\b(?:See|Id\.|Ibid\.|Cf\.|But see|Compare|E\.g\.|Accord|Supra|Infra|"
    r"U\.S\.|F\.\s*(?:2d|3d|4th|Supp)|S\.\s*Ct\.|L\.\s*Ed\.|Cir\.|"
    r"[A-Z][A-Za-z\.]*\s+v\.\s+)",
    re.IGNORECASE,
)


@dataclass
class LabelCandidate:
    page: int
    y: float
    x: float
    font_size: float
    digit_value: int
    text: str
    # Features computed at collection time
    is_pure_digit: bool = False   # textItem text is exactly the digit (superscript-style)
    has_punct: bool = False       # "N." or "N)" form
    left_margin: bool = False     # in leftmost ~25% of page width
    smaller_font: bool = False    # font smaller than page median
    # Context (filled post-collection): citation signal in following text items on same line
    citation_nearby: bool = False
    # Cluster feature: how many other candidates on the same page share a similar x-start
    # (within ±20 px) and are within ±80 y-distance. Footnote blocks are clustered;
    # isolated superscripts in body text are not.
    cluster_peers: int = 0
    # Position in page: 0.0 = top, 1.0 = bottom. Footnote blocks sit low.
    y_rel: float = 0.0
    # Substantive text: the line has enough English-looking content to be a real
    # footnote start (not a page number, running head, or stray superscript).
    substantive_text: bool = False


@dataclass
class PageData:
    page: int
    width: float
    height: float
    items: list[dict]  # raw textItems
    median_font: float


def load_lit_json(pdf_path: str) -> list[PageData]:
    out = subprocess.run(
        [LIT_CLI, "parse", "--format", "json", "--no-ocr", "-q", "-o", "/dev/stdout", pdf_path],
        capture_output=True, timeout=180,
    )
    if out.returncode != 0:
        raise RuntimeError(f"lit failed: {out.stderr[:500]!r}")
    try:
        doc = json.loads(out.stdout)
    except Exception as e:
        raise RuntimeError(f"lit stdout parse failed: {e}")
    pages = []
    for p in doc.get("pages") or []:
        items = [it for it in (p.get("textItems") or []) if (it.get("text") or "").strip()]
        fonts = [float(it.get("fontSize") or 0) for it in items if it.get("fontSize")]
        med = sorted(fonts)[len(fonts) // 2] if fonts else 10.0
        pages.append(
            PageData(
                page=int(p.get("page") or p.get("pageNum") or 0),
                width=float(p.get("width") or 0),
                height=float(p.get("height") or 0),
                items=items,
                median_font=med,
            )
        )
    return pages


def line_text_starting_at(page: PageData, anchor: dict, tol: float = 2.5) -> str:
    """Concatenate textItems on the same y-band as anchor, sorted by x, starting at anchor."""
    y = float(anchor.get("y") or 0)
    same_line = [
        it for it in page.items
        if abs(float(it.get("y") or 0) - y) <= tol
    ]
    same_line.sort(key=lambda it: float(it.get("x") or 0))
    anchor_x = float(anchor.get("x") or 0)
    parts = []
    for it in same_line:
        if float(it.get("x") or 0) >= anchor_x - 0.5:
            parts.append((it.get("text") or "").strip())
    return " ".join(p for p in parts if p)


def detect_repeating_header_texts(pages: list[PageData]) -> set[tuple[int, str]]:
    """Return set of (y_band, text) pairs that recur across >=40% of pages.
    Caller filters individual textItems against this set — NOT entire y-bands,
    since note labels often share a y-band with running-header text on other pages.
    """
    from collections import defaultdict
    text_by_yband: dict[tuple[int, str], int] = defaultdict(int)
    total = len(pages)
    for p in pages:
        seen = set()
        for it in p.items:
            t = (it.get("text") or "").strip().lower()
            y = int(round(float(it.get("y") or 0) / 10.0))
            key = (y, t)
            if key not in seen:
                text_by_yband[key] += 1
                seen.add(key)
    thresh = max(3, int(total * 0.4))
    return {k for k, c in text_by_yband.items() if c >= thresh}


def collect_candidates(pages: list[PageData]) -> list[LabelCandidate]:
    recurring_texts = detect_repeating_header_texts(pages)
    candidates: list[LabelCandidate] = []
    for p in pages:
        if not p.items:
            continue
        # Left margin threshold: 25% of page width (notes labels usually left-aligned)
        left_thresh = p.width * 0.25 if p.width else 150.0
        for it in p.items:
            text = (it.get("text") or "").strip()
            if not text:
                continue
            y = float(it.get("y") or 0)
            x = float(it.get("x") or 0)
            fs = float(it.get("fontSize") or 0)
            # Exclude top 4% and bottom 3% of page
            if p.height and (y / p.height < 0.04 or y / p.height > 0.97):
                continue
            y_band = int(round(y / 10.0))
            # Only filter THIS exact text if it recurs at THIS y_band across pages.
            if (y_band, text.lower()) in recurring_texts:
                continue
            # Must look like a label: pure digit OR digit+punct OR digit at line start
            # Two candidate shapes:
            #   (a) pure digit textItem (superscript-style): whole text is 1-4 digits
            #   (b) line starting with digit + punct (inline note marker)
            m_pure = LABEL_DIGIT_RE.match(text)
            m_start = LABEL_START_RE.match(text)
            digit_value = None
            is_pure_digit = False
            has_punct = False
            if m_pure:
                digit_value = int(m_pure.group(1))
                is_pure_digit = True
                has_punct = text != m_pure.group(1)
            elif m_start:
                digit_value = int(m_start.group(1))
                has_punct = bool(re.match(r"^\d{1,4}[\.\)\]]", text))
            else:
                continue
            if digit_value is None or digit_value < 1 or digit_value > 1500:
                continue
            # Confidence features
            smaller_font = bool(fs and fs < p.median_font * 0.95)
            left_margin = bool(x <= left_thresh)
            # Citation-signal lookahead: does text on this line after the marker contain
            # a citation indicator? (Strong positive signal that it's a real note start.)
            line_text = line_text_starting_at(p, it)
            citation_nearby = bool(CITATION_SIGNAL_RE.search(line_text))
            # Substantive English text: tokens after the label suggest a real note
            # start ("6 Asking friends and acquaintances..."), not a page number or
            # isolated stray digit. Require >=4 alphabetic tokens after the label,
            # each >=2 chars, with at least one uppercase-initial (proper names /
            # sentence start) to avoid false-positives on word-fragment junk.
            tokens = line_text.split()
            post = tokens[1:] if tokens else []
            alpha_tokens = [t for t in post if len(t) >= 2 and any(c.isalpha() for c in t)]
            has_upper_initial = any(t[:1].isupper() for t in alpha_tokens[:6])
            substantive_text = len(alpha_tokens) >= 4 and has_upper_initial
            y_rel = (y / p.height) if p.height else 0.5
            candidates.append(LabelCandidate(
                page=p.page, y=y, x=x, font_size=fs, digit_value=digit_value, text=text,
                is_pure_digit=is_pure_digit, has_punct=has_punct,
                left_margin=left_margin, smaller_font=smaller_font,
                citation_nearby=citation_nearby,
                y_rel=y_rel,
                substantive_text=substantive_text,
            ))
    # Sort by spatial order: page, y, x
    candidates.sort(key=lambda c: (c.page, c.y, c.x))
    # Compute cluster_peers: for each candidate, count other candidates on the same
    # page within ±20 x and ±80 y. Footnote entries sit in a dense block; isolated
    # body superscripts do not.
    by_page: dict[int, list[LabelCandidate]] = {}
    for c in candidates:
        by_page.setdefault(c.page, []).append(c)
    for page_cands in by_page.values():
        for c in page_cands:
            c.cluster_peers = sum(
                1 for o in page_cands
                if o is not c and abs(o.x - c.x) <= 20 and abs(o.y - c.y) <= 80
            )
    return candidates


# ---------- Phase 2: sequence solver ----------

def candidate_score(c: LabelCandidate) -> float:
    """Local confidence that this candidate is a real note-label start."""
    s = 0.0
    # Strong positive: label-with-punctuation ("259.") is a canonical footnote start form.
    if c.has_punct:
        s += 2.0
    # Supporting geometry
    if c.left_margin:
        s += 1.0
    if c.smaller_font:
        s += 0.5
    # Cite signal on the line is strong evidence of a real footnote entry
    if c.citation_nearby:
        s += 2.0
    # Substantive English text following the label is also strong evidence — catches
    # footnotes that start with prose ("6 Asking friends...") rather than a citation.
    elif c.substantive_text:
        s += 1.5
    # Clustered candidates = dense footnote block. Isolated superscripts get penalized.
    if c.cluster_peers >= 3:
        s += 1.5
    elif c.cluster_peers >= 1:
        s += 0.5
    else:
        # Isolated pure-digit with no cluster is usually a body superscript reference,
        # a page number, or a volume number. Sharply reduce its weight.
        if c.is_pure_digit and not c.has_punct:
            s -= 1.0
    # Bottom-of-page bias: footnote blocks sit in the lower half.
    if c.y_rel >= 0.5:
        s += 0.5
    elif c.y_rel < 0.2:
        # Top of page is almost never a footnote start (could be running head or body)
        s -= 1.0
    # Pure-digit + small font without any other signal is suspicious (stray superscript).
    if c.is_pure_digit and not c.has_punct and not c.citation_nearby and c.cluster_peers == 0:
        s -= 1.0
    # Never let a candidate have zero or negative influence; floor at tiny positive.
    return max(s, 0.1)


def gap_penalty(delta: int) -> float:
    if delta <= 0:
        return 10_000.0
    if delta == 1:
        return 0.0
    if delta == 2:
        return 0.5
    if delta == 3:
        return 1.5
    if delta <= 5:
        return 4.0
    return 4.0 + (delta - 5) * 2.0


def solve_sequence(candidates: list[LabelCandidate]) -> list[int]:
    """Return indices of selected candidates forming the best monotone sequence."""
    n = len(candidates)
    if n == 0:
        return []
    # dp[i] = best score of a path ending at candidate i
    dp = [0.0] * n
    parent = [-1] * n
    base = [candidate_score(c) for c in candidates]
    for i in range(n):
        dp[i] = base[i]
        ci = candidates[i]
        # Try extending from each j < i
        for j in range(i):
            cj = candidates[j]
            if cj.digit_value >= ci.digit_value:
                continue
            # Spatial progression: j must come before i (already sorted, so guaranteed)
            delta = ci.digit_value - cj.digit_value
            # Heavy penalty for backward y within same page
            spatial_pen = 0.0
            if cj.page == ci.page and ci.y < cj.y - 0.5:
                spatial_pen = 5.0
            cand = dp[j] + base[i] - gap_penalty(delta) - spatial_pen
            if cand > dp[i]:
                dp[i] = cand
                parent[i] = j
    # Backtrack from best endpoint. Also favor endpoints with highest digit (longer seq).
    best_i = max(range(n), key=lambda i: (dp[i], candidates[i].digit_value))
    path = []
    cur = best_i
    while cur != -1:
        path.append(cur)
        cur = parent[cur]
    path.reverse()
    return path


# ---------- Evaluation ----------

def trim_tail_outliers(labels: list[int]) -> list[int]:
    """Drop isolated high-end labels that are likely citation years or volumes."""
    import statistics as st
    if len(labels) < 4:
        return labels
    out = list(labels)
    while len(out) >= 4:
        top, second = out[-1], out[-2]
        jump = top - second
        deltas = [out[i+1] - out[i] for i in range(len(out) - 2)]
        if not deltas:
            break
        med = max(1, int(st.median(deltas)))
        if jump >= max(8, 5 * med) and jump > (top * 0.15):
            out.pop()
        else:
            break
    return out


def gap_fill_pass(cands: list[LabelCandidate], selected_idx: list[int]) -> list[int]:
    """Second pass: for each missing label in the current sequence, try to find
    a candidate at that digit value. Prefer candidates that fit spatially between
    the neighbors; otherwise accept the highest-scoring candidate whose page/y
    position is within a reasonable expansion of the bracket (±2 pages).
    """
    if not selected_idx:
        return selected_idx
    selected_set = set(selected_idx)
    # Map digit_value -> list of candidate indices
    by_digit: dict[int, list[int]] = {}
    for i, c in enumerate(cands):
        by_digit.setdefault(c.digit_value, []).append(i)
    ordered = sorted(selected_idx, key=lambda i: (cands[i].page, cands[i].y, cands[i].x))

    def strictly_between(cm: LabelCandidate, ca: LabelCandidate, cb: LabelCandidate) -> bool:
        after_a = (cm.page > ca.page) or (cm.page == ca.page and cm.y >= ca.y - 0.5)
        before_b = (cm.page < cb.page) or (cm.page == cb.page and cm.y <= cb.y + 0.5)
        return after_a and before_b

    def near_bracket(cm: LabelCandidate, ca: LabelCandidate, cb: LabelCandidate) -> bool:
        # Allow ±2 pages slack around the bracket — handles PDFs whose note
        # column position drifts slightly or whose neighbors are themselves
        # imperfectly placed.
        return (ca.page - 2) <= cm.page <= (cb.page + 2)

    for a, b in zip(ordered, ordered[1:]):
        ca, cb = cands[a], cands[b]
        for missing_digit in range(ca.digit_value + 1, cb.digit_value):
            options = [i for i in by_digit.get(missing_digit, []) if i not in selected_set]
            if not options:
                continue
            # Prefer candidates strictly between; fall back to near-bracket.
            strict = [i for i in options if strictly_between(cands[i], ca, cb)]
            near = [i for i in options if near_bracket(cands[i], ca, cb)]
            pool = strict or near
            if not pool:
                continue
            # Pick the one with the highest score
            best = max(pool, key=lambda i: candidate_score(cands[i]))
            # Only admit if score is plausibly real (avoid pure noise)
            if candidate_score(cands[best]) >= 1.0:
                selected_set.add(best)
    return sorted(selected_set)


def evaluate(pdf_path: str) -> dict:
    t0 = time.time()
    try:
        pages = load_lit_json(pdf_path)
    except Exception as e:
        return {"pdf": pdf_path, "error": str(e)[:200], "elapsed": time.time() - t0}
    cands = collect_candidates(pages)
    # Cap: typical law review caps around page_count * 8; allow slack.
    max_plausible = max(120, len(pages) * 10)
    cands = [c for c in cands if c.digit_value <= max_plausible]
    path = solve_sequence(cands)
    # Gap-fill pass
    path = gap_fill_pass(cands, path)
    selected = [cands[i] for i in path]
    # Sort selected by spatial order (page, y, x) to get linear reading order
    selected.sort(key=lambda c: (c.page, c.y, c.x))
    labels_spatial = [c.digit_value for c in selected]
    # Trim tail outliers
    labels = trim_tail_outliers(sorted(labels_spatial))
    status = "empty"
    expected = None
    gaps: list[int] = []
    if labels and len(labels) >= 3 and labels[0] == 1:
        lo, hi = labels[0], labels[-1]
        expected = (lo, hi)
        found = set(labels)
        gaps = [n for n in range(lo, hi + 1) if n not in found]
        status = "valid" if not gaps else ("valid_with_gaps" if len(gaps) <= max(2, int(0.02 * (hi - lo + 1))) else "invalid")
    elif labels:
        # Too few labels, or sequence doesn't start at 1 — degenerate, call empty.
        status = "empty"
    return {
        "pdf": pdf_path,
        "pages": len(pages),
        "candidates": len(cands),
        "selected": len(selected),
        "status": status,
        "expected": expected,
        "gaps": gaps[:10],
        "gap_count": len(gaps),
        "labels_sample": labels[:10] + ["..."] + labels[-5:] if len(labels) > 15 else labels,
        "elapsed": round(time.time() - t0, 2),
    }


def main():
    test_set = json.loads(Path("/tmp/solver_poc_docs.json").read_text())
    results = {"clean": [], "failure": []}
    for bucket in ("clean", "failure"):
        for i, d in enumerate(test_set[bucket]):
            r = evaluate(d["pdf"])
            r["relative"] = d["relative"]
            r["baseline_status"] = d["baseline_status"]
            r["baseline_candidate"] = d["baseline_candidate"]
            bl = d.get("baseline", {})
            r["baseline_expected"] = bl.get("expected_range")
            r["baseline_notes"] = bl.get("notes") or bl.get("found_labels")
            r["baseline_gaps"] = bl.get("gaps")
            results[bucket].append(r)
            print(f"[{bucket} {i+1}/{len(test_set[bucket])}] {d['relative']}: solver={r['status']}({r.get('gap_count')}) baseline={d['baseline_status']} elapsed={r['elapsed']}s")

    # Summary
    print("\n\n=== CLEAN SET (20 baseline-valid docs) ===")
    agree = sum(1 for r in results["clean"] if r["status"] == "valid")
    partial = sum(1 for r in results["clean"] if r["status"] == "valid_with_gaps")
    fail = sum(1 for r in results["clean"] if r["status"] == "invalid")
    empty = sum(1 for r in results["clean"] if r["status"] == "empty")
    err = sum(1 for r in results["clean"] if "error" in r)
    print(f"  solver valid: {agree}/20  valid_with_gaps: {partial}  invalid: {fail}  empty: {empty}  error: {err}")

    print("\n=== FAILURE SET (20 baseline-invalid/empty docs) ===")
    rescued = sum(1 for r in results["failure"] if r["status"] == "valid")
    partial = sum(1 for r in results["failure"] if r["status"] == "valid_with_gaps")
    still_bad = sum(1 for r in results["failure"] if r["status"] == "invalid")
    empty = sum(1 for r in results["failure"] if r["status"] == "empty")
    err = sum(1 for r in results["failure"] if "error" in r)
    print(f"  solver valid (rescue): {rescued}/20  valid_with_gaps: {partial}  invalid: {still_bad}  empty: {empty}  error: {err}")

    Path("/tmp/solver_poc_results.json").write_text(json.dumps(results, indent=2, default=str))
    print(f"\nDetails written to /tmp/solver_poc_results.json")


if __name__ == "__main__":
    main()
