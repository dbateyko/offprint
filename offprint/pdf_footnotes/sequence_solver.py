"""Global sequence solver for footnote extraction from liteparse layouts.

Treats footnote extraction as:
  (1) collect all plausible numeric-label candidates from raw textItems,
  (2) solve for the longest monotonically-increasing sequence with small deltas
      and forward-progressing spatial positions, weighted by per-candidate
      confidence,
  (3) gap-fill the selected sequence from the remaining candidates.

The solver returns per-page y-split points: everything below the first selected
label's y on a page is classified as note lines; everything above as body. This
matches the ExtractedPage body/notes contract used by the rest of the pipeline.

See offprint/pdf_footnotes/README.md for architectural context and benchmark
numbers vs. the heuristic candidate ensemble.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

LABEL_DIGIT_RE = re.compile(r"^(\d{1,4})(?:[\.\)\]]|\.{2,})?$")
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
    is_pure_digit: bool = False
    has_punct: bool = False
    left_margin: bool = False
    smaller_font: bool = False
    citation_nearby: bool = False
    cluster_peers: int = 0
    y_rel: float = 0.0
    substantive_text: bool = False


@dataclass
class _PageData:
    page: int
    width: float
    height: float
    items: list[dict[str, Any]]
    median_font: float


def _pages_from_layouts(layouts: list) -> list[_PageData]:
    result: list[_PageData] = []
    for layout in layouts:
        items = list(getattr(layout, "raw_items", ()) or [])
        fonts = [
            float(it.get("fontSize") or 0) for it in items if it.get("fontSize")
        ]
        med = sorted(fonts)[len(fonts) // 2] if fonts else 10.0
        result.append(
            _PageData(
                page=int(getattr(layout, "page_number", 0) or 0),
                width=float(getattr(layout, "width", 0.0) or 0.0),
                height=float(getattr(layout, "height", 0.0) or 0.0),
                items=items,
                median_font=med,
            )
        )
    return result


def _line_text_starting_at(page: _PageData, anchor: dict, tol: float = 2.5) -> str:
    y = float(anchor.get("y") or 0)
    same_line = [it for it in page.items if abs(float(it.get("y") or 0) - y) <= tol]
    same_line.sort(key=lambda it: float(it.get("x") or 0))
    anchor_x = float(anchor.get("x") or 0)
    parts: list[str] = []
    for it in same_line:
        if float(it.get("x") or 0) >= anchor_x - 0.5:
            parts.append((it.get("text") or "").strip())
    return " ".join(p for p in parts if p)


def _item_label_component(it: dict[str, Any]) -> tuple[str, bool] | None:
    text = (it.get("text") or "").strip()
    m = LABEL_DIGIT_RE.match(text)
    if not m:
        return None
    digits = m.group(1)
    return digits, text != digits


def _item_end_x(it: dict[str, Any]) -> float:
    return float(it.get("x") or 0) + float(it.get("width") or 0)


def _items_are_tightly_adjacent(items: list[dict[str, Any]]) -> bool:
    for left, right in zip(items, items[1:]):
        left_x = float(left.get("x") or 0)
        right_x = float(right.get("x") or 0)
        if right_x < left_x - 0.5:
            return False
        fs = max(float(left.get("fontSize") or 0), float(right.get("fontSize") or 0), 8.0)
        gap = right_x - _item_end_x(left)
        if gap > max(4.0, fs * 0.55):
            return False
        if not float(left.get("width") or 0) and right_x - left_x > fs * 1.3:
            return False
    return True


def _has_previous_line_text(page: _PageData, first: dict[str, Any], tol: float = 2.5) -> bool:
    first_x = float(first.get("x") or 0)
    y = float(first.get("y") or 0)
    for it in page.items:
        if it is first:
            continue
        text = (it.get("text") or "").strip()
        if not text:
            continue
        if abs(float(it.get("y") or 0) - y) > tol:
            continue
        if _item_end_x(it) <= first_x - 1.0:
            return True
    return False


def _has_note_text_after(page: _PageData, last: dict[str, Any], font_size: float) -> bool:
    y = float(last.get("y") or 0)
    line_after = [
        it
        for it in page.items
        if abs(float(it.get("y") or 0) - y) <= 2.5
        and float(it.get("x") or 0) >= _item_end_x(last) - 0.5
        and (it.get("text") or "").strip()
    ]
    line_after.sort(key=lambda it: float(it.get("x") or 0))
    if not line_after:
        return False
    first_after_gap = float(line_after[0].get("x") or 0) - _item_end_x(last)
    if first_after_gap > max(55.0, font_size * 6.0):
        return False
    text_after = " ".join((it.get("text") or "").strip() for it in line_after)
    if CITATION_SIGNAL_RE.search(text_after):
        return True
    tokens = text_after.split()
    alpha_tokens = [t for t in tokens[:8] if len(t) >= 2 and any(c.isalpha() for c in t)]
    return len(alpha_tokens) >= 3 and any(t[:1].isupper() for t in alpha_tokens[:4])


def _synthesize_split_label_candidates(
    page: _PageData,
    recurring_texts: set[tuple[int, str]],
    max_label: int,
) -> list[LabelCandidate]:
    """Create candidates for labels split across adjacent digit textItems."""
    valid_items: list[dict[str, Any]] = []
    for it in page.items:
        text = (it.get("text") or "").strip()
        if not text:
            continue
        y = float(it.get("y") or 0)
        if page.height and (y / page.height < 0.04 or y / page.height > 0.97):
            continue
        y_band = int(round(y / 10.0))
        if (y_band, text.lower()) in recurring_texts:
            continue
        if _item_label_component(it) is None:
            continue
        valid_items.append(it)

    left_thresh = page.width * 0.25 if page.width else 150.0
    synthesized: list[LabelCandidate] = []
    seen: set[tuple[int, int, int, int]] = set()
    for first in valid_items:
        first_y = float(first.get("y") or 0)
        first_band = int(round(first_y / 10.0))
        same_line = [
            it
            for it in valid_items
            if int(round(float(it.get("y") or 0) / 10.0)) == first_band
            and abs(float(it.get("y") or 0) - first_y) <= 2.5
        ]
        same_line.sort(key=lambda it: float(it.get("x") or 0))
        try:
            start = next(i for i, it in enumerate(same_line) if it is first)
        except StopIteration:
            continue
        for size in (2, 3):
            components = same_line[start : start + size]
            if len(components) != size:
                continue
            parsed = [_item_label_component(it) for it in components]
            if any(part is None for part in parsed):
                continue
            digit_parts = [part[0] for part in parsed if part is not None]
            punct_flags = [part[1] for part in parsed if part is not None]
            if any(punct_flags[:-1]):
                continue
            combined_text = "".join(digit_parts)
            if len(combined_text) <= 1 or combined_text.startswith("0"):
                continue
            digit_value = int(combined_text)
            if digit_value < 1 or digit_value > max_label:
                continue
            if not _items_are_tightly_adjacent(components):
                continue
            x = float(first.get("x") or 0)
            fs = float(first.get("fontSize") or 0)
            y_rel = (first_y / page.height) if page.height else 0.5
            smaller_font = bool(fs and fs < page.median_font * 0.95)
            at_line_start = not _has_previous_line_text(page, first)
            left_margin = bool(x <= left_thresh)
            note_text_after = _has_note_text_after(page, components[-1], fs)
            if not (
                note_text_after
                or (at_line_start and left_margin and (smaller_font or y_rel >= 0.35))
            ):
                continue
            key = (page.page, int(round(first_y * 10)), int(round(x * 10)), digit_value)
            if key in seen:
                continue
            seen.add(key)
            line_text = _line_text_starting_at(page, first)
            citation_nearby = bool(CITATION_SIGNAL_RE.search(line_text))
            tokens = line_text.split()
            post = tokens[size:] if len(tokens) >= size else []
            alpha_tokens = [t for t in post if len(t) >= 2 and any(c.isalpha() for c in t)]
            has_upper_initial = any(t[:1].isupper() for t in alpha_tokens[:6])
            substantive_text = len(alpha_tokens) >= 4 and has_upper_initial
            synthesized.append(
                LabelCandidate(
                    page=page.page,
                    y=first_y,
                    x=x,
                    font_size=fs,
                    digit_value=digit_value,
                    text=combined_text,
                    is_pure_digit=not punct_flags[-1],
                    has_punct=punct_flags[-1],
                    left_margin=left_margin,
                    smaller_font=smaller_font,
                    citation_nearby=citation_nearby,
                    y_rel=y_rel,
                    substantive_text=substantive_text,
                )
            )
    return synthesized


def _detect_repeating_header_texts(pages: list[_PageData]) -> set[tuple[int, str]]:
    """(y_band, text) pairs that recur across >=40 % of pages.

    Callers filter individual textItems against this set — NOT entire y-bands —
    since note labels may share a y-band with running-header text on other
    pages (the most common failure mode if filtering is too coarse).
    """
    from collections import defaultdict

    text_by_yband: dict[tuple[int, str], int] = defaultdict(int)
    total = len(pages)
    for p in pages:
        seen: set[tuple[int, str]] = set()
        for it in p.items:
            t = (it.get("text") or "").strip().lower()
            y = int(round(float(it.get("y") or 0) / 10.0))
            key = (y, t)
            if key not in seen:
                text_by_yband[key] += 1
                seen.add(key)
    thresh = max(3, int(total * 0.4))
    return {k for k, c in text_by_yband.items() if c >= thresh}


def _collect_candidates(pages: list[_PageData]) -> list[LabelCandidate]:
    recurring_texts = _detect_repeating_header_texts(pages)
    max_synthetic_label = max(120, len(pages) * 10)
    candidates: list[LabelCandidate] = []
    for p in pages:
        if not p.items:
            continue
        left_thresh = p.width * 0.25 if p.width else 150.0
        for it in p.items:
            text = (it.get("text") or "").strip()
            if not text:
                continue
            y = float(it.get("y") or 0)
            x = float(it.get("x") or 0)
            fs = float(it.get("fontSize") or 0)
            if p.height and (y / p.height < 0.04 or y / p.height > 0.97):
                continue
            y_band = int(round(y / 10.0))
            if (y_band, text.lower()) in recurring_texts:
                continue
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
            smaller_font = bool(fs and fs < p.median_font * 0.95)
            left_margin = bool(x <= left_thresh)
            line_text = _line_text_starting_at(p, it)
            citation_nearby = bool(CITATION_SIGNAL_RE.search(line_text))
            tokens = line_text.split()
            post = tokens[1:] if tokens else []
            alpha_tokens = [t for t in post if len(t) >= 2 and any(c.isalpha() for c in t)]
            has_upper_initial = any(t[:1].isupper() for t in alpha_tokens[:6])
            substantive_text = len(alpha_tokens) >= 4 and has_upper_initial
            y_rel = (y / p.height) if p.height else 0.5
            candidates.append(
                LabelCandidate(
                    page=p.page,
                    y=y,
                    x=x,
                    font_size=fs,
                    digit_value=digit_value,
                    text=text,
                    is_pure_digit=is_pure_digit,
                    has_punct=has_punct,
                    left_margin=left_margin,
                    smaller_font=smaller_font,
                    citation_nearby=citation_nearby,
                    y_rel=y_rel,
                    substantive_text=substantive_text,
                )
            )
        candidates.extend(
            _synthesize_split_label_candidates(p, recurring_texts, max_synthetic_label)
        )
    candidates.sort(key=lambda c: (c.page, c.y, c.x))
    # Cluster peers: candidates on same page within ±20 x and ±80 y.
    by_page: dict[int, list[LabelCandidate]] = {}
    for c in candidates:
        by_page.setdefault(c.page, []).append(c)
    for page_cands in by_page.values():
        for c in page_cands:
            c.cluster_peers = sum(
                1
                for o in page_cands
                if o is not c and abs(o.x - c.x) <= 20 and abs(o.y - c.y) <= 80
            )
    return candidates


def _candidate_score(c: LabelCandidate) -> float:
    s = 0.0
    if c.has_punct:
        s += 2.0
    if c.left_margin:
        s += 1.0
    if c.smaller_font:
        s += 0.5
    if c.citation_nearby:
        s += 2.0
    elif c.substantive_text:
        s += 1.5
    if c.cluster_peers >= 3:
        s += 1.5
    elif c.cluster_peers >= 1:
        s += 0.5
    else:
        if c.is_pure_digit and not c.has_punct:
            s -= 1.0
    if c.y_rel >= 0.5:
        s += 0.5
    elif c.y_rel < 0.2:
        s -= 1.0
    if c.is_pure_digit and not c.has_punct and not c.citation_nearby and c.cluster_peers == 0:
        s -= 1.0
    return max(s, 0.1)


def _gap_penalty(delta: int) -> float:
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


def _solve_sequence(candidates: list[LabelCandidate]) -> list[int]:
    n = len(candidates)
    if n == 0:
        return []
    dp = [0.0] * n
    parent = [-1] * n
    base = [_candidate_score(c) for c in candidates]
    for i in range(n):
        dp[i] = base[i]
        ci = candidates[i]
        for j in range(i):
            cj = candidates[j]
            if cj.digit_value >= ci.digit_value:
                continue
            delta = ci.digit_value - cj.digit_value
            spatial_pen = 0.0
            if cj.page == ci.page and ci.y < cj.y - 0.5:
                spatial_pen = 5.0
            cand = dp[j] + base[i] - _gap_penalty(delta) - spatial_pen
            if cand > dp[i]:
                dp[i] = cand
                parent[i] = j
    best_i = max(range(n), key=lambda i: (dp[i], candidates[i].digit_value))
    path: list[int] = []
    cur = best_i
    while cur != -1:
        path.append(cur)
        cur = parent[cur]
    path.reverse()
    return path


def _trim_tail_outliers(labels: list[int]) -> list[int]:
    import statistics as _stat

    if len(labels) < 4:
        return labels
    out = list(labels)
    while len(out) >= 4:
        top, second = out[-1], out[-2]
        jump = top - second
        deltas = [out[i + 1] - out[i] for i in range(len(out) - 2)]
        if not deltas:
            break
        med = max(1, int(_stat.median(deltas)))
        if jump >= max(8, 5 * med) and jump > (top * 0.15):
            out.pop()
        else:
            break
    return out


def _gap_fill(cands: list[LabelCandidate], selected_idx: list[int]) -> list[int]:
    if not selected_idx:
        return selected_idx
    selected_set = set(selected_idx)
    by_digit: dict[int, list[int]] = {}
    for i, c in enumerate(cands):
        by_digit.setdefault(c.digit_value, []).append(i)
    ordered = sorted(selected_idx, key=lambda i: (cands[i].page, cands[i].y, cands[i].x))

    def strictly_between(cm, ca, cb):
        after_a = (cm.page > ca.page) or (cm.page == ca.page and cm.y >= ca.y - 0.5)
        before_b = (cm.page < cb.page) or (cm.page == cb.page and cm.y <= cb.y + 0.5)
        return after_a and before_b

    def near_bracket(cm, ca, cb):
        return (ca.page - 2) <= cm.page <= (cb.page + 2)

    # Layout signal for the column-consistency fallback: what's the median x of
    # the selected path? Real law-review footnote columns are at a consistent
    # x-position across the doc; high-scoring candidates at the same x should
    # be trusted even when far from the current bracket.
    selected_xs = sorted(cands[i].x for i in selected_idx)
    median_x = selected_xs[len(selected_xs) // 2] if selected_xs else 0.0

    def aligned_column(cm) -> bool:
        # Within 15 px of the path's median x, AND the candidate is at the left
        # margin, AND it has cluster peers (other candidates nearby on its page).
        return (
            abs(cm.x - median_x) <= 15.0
            and cm.left_margin
            and cm.cluster_peers >= 2
        )

    for a, b in zip(ordered, ordered[1:]):
        ca, cb = cands[a], cands[b]
        for missing_digit in range(ca.digit_value + 1, cb.digit_value):
            options = [i for i in by_digit.get(missing_digit, []) if i not in selected_set]
            if not options:
                continue
            strict = {i for i in options if strictly_between(cands[i], ca, cb)}
            near = {i for i in options if near_bracket(cands[i], ca, cb)}
            # Column-aligned fallback: accept a high-scoring candidate anywhere
            # in the doc if it sits in the same x-column as the rest of the
            # selected sequence and has cluster peers. Captures cases where the
            # real footnote column jumps pages (multi-article PDFs, appendices)
            # and the DP's path didn't reach it.
            column_anywhere = {
                i for i in options
                if aligned_column(cands[i]) and _candidate_score(cands[i]) >= 4.5
            }
            pool = strict | near | column_anywhere
            if not pool:
                continue
            best = max(pool, key=lambda i: _candidate_score(cands[i]))
            if _candidate_score(cands[best]) >= 1.0:
                selected_set.add(best)
    return sorted(selected_set)


@dataclass(frozen=True)
class SolverResult:
    # Per-page y cutoff — note lines are at y >= cutoff (or entire page is body
    # when no cutoff is set for that page).
    page_cutoffs: dict[int, float]
    # Selected candidates' digit values (for diagnostics + ordinality check).
    selected_labels: list[int]
    # Selected candidates themselves (for downstream NoteRecord construction).
    selected_candidates: tuple[LabelCandidate, ...]
    # Raw candidate count (for diagnostics).
    candidate_count: int


def solve_document(layouts: list) -> SolverResult:
    """Run the full pipeline (collect → solve → gap-fill → trim) on liteparse
    layouts and return the per-page body/notes cutoff positions plus the
    selected label candidates (for downstream consumers that want to bypass
    the heuristic segmenter and use solver decisions as authoritative).
    """
    pages = _pages_from_layouts(layouts)
    cands = _collect_candidates(pages)
    empty = SolverResult(
        page_cutoffs={}, selected_labels=[], selected_candidates=(), candidate_count=0
    )
    if not cands:
        return empty
    max_plausible = max(120, len(pages) * 10)
    cands = [c for c in cands if c.digit_value <= max_plausible]
    path = _solve_sequence(cands)
    path = _gap_fill(cands, path)
    selected = [cands[i] for i in sorted(path)]
    selected.sort(key=lambda c: (c.page, c.y, c.x))
    labels_spatial = [c.digit_value for c in selected]
    trimmed = set(_trim_tail_outliers(sorted(labels_spatial)))
    final = [c for c in selected if c.digit_value in trimmed]
    if len(final) < 3:
        return SolverResult(
            page_cutoffs={},
            selected_labels=[],
            selected_candidates=(),
            candidate_count=len(cands),
        )
    first_label = final[0].digit_value
    # Accept sequences starting at 1 unconditionally. Accept sequences starting
    # at 2 when the sequence is long and dense (label 1's textItem was lost to
    # a rendering artifact — '1', '1.', etc. — but the rest of the sequence is
    # clearly real). Guarded against scattered stray "2" cases by requiring
    # ≥5 labels and ≥80% density.
    if first_label > 2:
        return SolverResult(
            page_cutoffs={},
            selected_labels=[],
            selected_candidates=(),
            candidate_count=len(cands),
        )
    if first_label == 2:
        unique_labels = sorted({c.digit_value for c in final})
        span = unique_labels[-1] - unique_labels[0] + 1
        density = len(unique_labels) / max(span, 1)
        if len(unique_labels) < 5 or density < 0.80:
            return SolverResult(
                page_cutoffs={},
                selected_labels=[],
                selected_candidates=(),
                candidate_count=len(cands),
            )
    page_cutoffs: dict[int, float] = {}
    for c in final:
        if c.page not in page_cutoffs or c.y < page_cutoffs[c.page]:
            page_cutoffs[c.page] = c.y
    return SolverResult(
        page_cutoffs=page_cutoffs,
        selected_labels=sorted(set(c.digit_value for c in final)),
        selected_candidates=tuple(final),
        candidate_count=len(cands),
    )


def build_note_records(layouts: list, result: SolverResult):
    """Given solver output, produce (notes, author_notes, ordinality_report)
    directly — bypassing the heuristic segmenter's re-validation.

    The solver already knows which labels exist and where; the segmenter's
    label-rejection rules (`_is_likely_false_positive`, etc.) tend to reject
    labels the solver accepted, producing disagreement. This function trusts
    the solver's decisions and constructs NoteRecord objects whose text is
    the concatenation of lines lying in each label's spatial span.

    Returns (notes, author_notes, ordinality_report). author_notes is always
    empty here — author-note (asterisk/dagger) handling stays in the segmenter
    path and runs as a supplementary pass.
    """
    from .schema import NoteRecord, OrdinalityReport

    if not result.selected_candidates:
        return [], [], None

    # Map page_number → layout for efficient lookup of lines.
    by_page: dict[int, Any] = {
        int(getattr(layout, "page_number", 0) or 0): layout for layout in layouts
    }
    ordered = list(result.selected_candidates)
    notes: list[NoteRecord] = []
    for idx, cand in enumerate(ordered):
        next_cand = ordered[idx + 1] if idx + 1 < len(ordered) else None
        # Collect all lines that lie spatially within [cand, next_cand).
        collected: list[Any] = []
        for pn in sorted(by_page.keys()):
            if pn < cand.page:
                continue
            if next_cand is not None and pn > next_cand.page:
                break
            layout = by_page[pn]
            for line in getattr(layout, "lines", ()):  # ExtractedLine
                top = float(getattr(line, "top", 0.0) or 0.0)
                # Start of the span: exclude lines strictly above cand on cand's page.
                if pn == cand.page and top < cand.y - 0.5:
                    continue
                # End of the span: exclude lines at-or-below next_cand's y on
                # next_cand's page.
                if next_cand is not None:
                    if pn == next_cand.page and top >= next_cand.y - 0.5:
                        continue
                collected.append(line)
        text_parts = [(getattr(ln, "text", "") or "").strip() for ln in collected]
        # Strip the leading label glyph from the first line if present (the
        # textItem for the label itself is already clustered into this line's
        # text; we don't want "6 6 Asking..." if the glyph was on its own).
        if text_parts:
            lead = text_parts[0]
            # Try to drop a leading "N", "N.", "N)", "N]" token if it matches
            # the candidate's digit value.
            lead_tokens = lead.split()
            if lead_tokens:
                first = lead_tokens[0].rstrip(".)]")
                # .isdigit() accepts Unicode superscripts (e.g. '³') that
                # int() rejects; restrict to ASCII digits to avoid ValueError.
                if first.isascii() and first.isdigit() and int(first) == cand.digit_value:
                    text_parts[0] = " ".join(lead_tokens[1:])
        text = " ".join(part for part in text_parts if part).strip()
        page_end = int(collected[-1].page_number) if collected else cand.page
        notes.append(
            NoteRecord(
                ordinal=idx + 1,
                label=str(cand.digit_value),
                note_type="footnote",
                text=text,
                page_start=int(cand.page),
                page_end=page_end,
                confidence=0.95,
                features={"source": "sequence_solver"},
            )
        )

    # Build ordinality report directly from selected labels.
    labels = sorted(set(c.digit_value for c in ordered))
    if not labels:
        return notes, [], None
    lo, hi = labels[0], labels[-1]
    expected_set = set(range(lo, hi + 1))
    gaps = sorted(expected_set - set(labels))
    gap_ratio = len(gaps) / max(len(expected_set), 1)
    if not gaps:
        status = "valid"
    elif len(gaps) <= max(2, int(0.02 * (hi - lo + 1))):
        status = "valid_with_gaps"
    else:
        status = "invalid"
    ordinality = OrdinalityReport(
        status=status,
        expected_range=(lo, hi),
        actual_sequence=labels,
        gaps=gaps,
        gap_tolerance=0,
        tolerance_exceeded=status == "invalid",
        gap_ratio=gap_ratio,
    )
    return notes, [], ordinality
