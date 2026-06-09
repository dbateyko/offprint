from __future__ import annotations

import logging
import os
import re
import shutil
import statistics
import tempfile
from pathlib import Path
from dataclasses import dataclass, field, replace
from typing import Any, Sequence

PARSER_MODES = {
    "balanced",
    "pdfplumber_only",
    "pypdf_only",
    "docling_only",
    "liteparse_only",
    "footnote_optimized",
}
_NOTE_LIKE_RE = re.compile(
    r"^\s*(?:\[\^\d{1,4}\]:|\d{1,4}[\]\)\.,:;-]?\s+|[ivxlcdm]{1,7}[\]\)\.,:;-]?\s+|[*†‡§¶]\s+)",
    re.IGNORECASE,
)
_SEPARATOR_ARABIC_NOTE_RE = re.compile(r"^\s*\d{1,4}[\]\)\.,:;-]\s+")
_LEGAL_FOOTNOTE_SIGNAL_RE = re.compile(
    r"\b(?:v\.|u\.s\.|f\.\d|s\. ct\.|id\.|supra|infra|§)\b",
    re.IGNORECASE,
)
_STRICT_FOOTNOTE_SIGNAL_RE = re.compile(
    r"(?<![A-Za-z])(?:"
    r"see|see also|but see|cf\.|compare|accord|contra|id\.|ibid\.|supra|infra|"
    r"u\.s\.|f\.\s?(?:2d|3d|supp)|s\.\s?ct\.|l\.\s?ed\.|u\.s\.c\.|c\.f\.r\.|"
    r"restatement|e\.g\.|§"
    r")(?![A-Za-z])",
    re.IGNORECASE,
)
_OPENDATALOADER_EXCLUDED_TYPES = {"header", "footer"}
_FRONTMATTER_TEXT_RE = re.compile(
    r"\b(?:table of contents|contents|editorial board|masthead|inside cover|volume|issue)\b",
    re.IGNORECASE,
)
_SHORT_ID_NOTE_RE = re.compile(
    r"^\s*(?:\d{1,3}\s+)?(?:id\.?|ibid\.?)(?:\s+at\s+\d+(?:[-–]\d+)?)?\.?\s*$",
    re.IGNORECASE,
)
_RUNNING_HEAD_JOURNAL_CITE_RE = re.compile(
    r"^\s*(?:\d{1,4}\s+)?[A-Za-z][A-Za-z.\s&'-]{2,80}"
    r"L\.\s*Rev\.\s+\d{2,4}\s+\(\d{4}\)(?:\s+\d{2,4})?\s*$",
    re.IGNORECASE,
)
_SECTION_HEADING_LINE_RE = re.compile(
    r"^\s*(?:"
    r"(?:[IVXLCDM]{1,8}|[A-Z]|\d{1,3})[\.\)]\s+)?"
    r"[A-Z][A-Z0-9\s,.'&:-]{8,120}"
    r"$"
)
_OPENING_TOKENS = {
    "a",
    "an",
    "at",
    "but",
    "cf",
    "compare",
    "for",
    "id",
    "ibid",
    "in",
    "of",
    "on",
    "see",
    "the",
    "to",
}


def suppress_pypdf_noise() -> None:
    """Reduce verbose parser noise that otherwise floods run logs."""
    for name in ("pypdf", "pypdf._reader", "pypdf._cmap"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.ERROR)
        logger.propagate = False


@dataclass
class ExtractedLine:
    text: str
    page_number: int
    top: float = 0.0
    bottom: float = 0.0
    font_size: float | None = None
    source: str = "text"

    def to_dict(self) -> dict:
        d: dict = {"text": self.text, "page_number": self.page_number, "source": self.source}
        if self.top != 0.0:
            d["top"] = self.top
        if self.bottom != 0.0:
            d["bottom"] = self.bottom
        if self.font_size is not None:
            d["font_size"] = self.font_size
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "ExtractedLine":
        return cls(
            text=d.get("text", ""),
            page_number=d.get("page_number", 0),
            top=d.get("top", 0.0),
            bottom=d.get("bottom", 0.0),
            font_size=d.get("font_size"),
            source=d.get("source", "text"),
        )


@dataclass
class ExtractedPage:
    page_number: int
    width: float = 0.0
    height: float = 0.0
    body_lines: list[ExtractedLine] = field(default_factory=list)
    note_lines: list[ExtractedLine] = field(default_factory=list)
    raw_text: str = ""
    source: str = "text"

    def to_dict(self) -> dict:
        return {
            "page_number": self.page_number,
            "width": self.width,
            "height": self.height,
            "body_lines": [line.to_dict() for line in self.body_lines],
            "note_lines": [line.to_dict() for line in self.note_lines],
            "raw_text": self.raw_text,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ExtractedPage":
        return cls(
            page_number=d.get("page_number", 0),
            width=d.get("width", 0.0),
            height=d.get("height", 0.0),
            body_lines=[ExtractedLine.from_dict(ld) for ld in d.get("body_lines", [])],
            note_lines=[ExtractedLine.from_dict(ld) for ld in d.get("note_lines", [])],
            raw_text=d.get("raw_text", ""),
            source=d.get("source", "text"),
        )


@dataclass
class ExtractedDocument:
    pdf_path: str
    pages: list[ExtractedPage] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    parser: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def total_text_chars(self) -> int:
        return sum(len(page.raw_text or "") for page in self.pages)

    @property
    def page_count(self) -> int:
        return len(self.pages)

    def to_dict(self) -> dict:
        return {
            "pdf_path": self.pdf_path,
            "pages": [page.to_dict() for page in self.pages],
            "warnings": list(self.warnings),
            "parser": self.parser,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ExtractedDocument":
        return cls(
            pdf_path=d.get("pdf_path", ""),
            pages=[ExtractedPage.from_dict(p) for p in d.get("pages", [])],
            warnings=d.get("warnings", []),
            parser=d.get("parser", ""),
            metadata=dict(d.get("metadata") or {}),
        )


@dataclass(frozen=True)
class _LiteparsePageLayout:
    page_number: int
    width: float
    height: float
    raw_text: str
    lines: tuple[ExtractedLine, ...]
    # Raw textItems (dicts with keys: text, x, y, width, height, fontSize) so
    # candidates that need glyph-level precision (e.g. the sequence_solver) can
    # work directly on liteparse output without re-parsing.
    raw_items: tuple[dict, ...] = ()
    # Optional pdfplumber-derived vector separator y-position. LiteParse exposes
    # text boxes, not PDF line/rect drawing primitives, so this lets the solver
    # use the visible footnote rule without switching parsers.
    separator_y: float | None = None


_REPORTER_SPACED_RE = re.compile(r"\b([A-Z])\.\s+(\d)\s+([a-z])\b")

# Liteparse-specific: kerning-split legal words where a single leading character
# is separated from the rest by a space artifact (e.g. "s upra" → "supra",
# "i d." → "Id.", "i nfra" → "infra", "i bid" → "ibid").
_LITEPARSE_KERNING_SPLIT_RE = re.compile(
    r"\b([sS])\s+(upra)\b"
    r"|\b([iI])\s+(d\.)\b"
    r"|\b([iI])\s+(nfra)\b"
    r"|\b([iI])\s+(bid\.?)\b"
    r"|\b([cC])\s+(f\.)\b"
    r"|\b([iI])\s+(d\b)"
)

# Liteparse-specific: small-caps mangling where the first (full-size) letter of
# each word is separated by a space from the rest (smaller-cap glyphs).
# Matches a single uppercase letter followed by a space and 3+ uppercase letters
# (or 2 uppercase + punctuation like "EV.").  The 3-char minimum avoids false
# positives with real words ("I AM", "A IS").
# E.g. "C ONSUMER" → "CONSUMER", "P IERCE" → "PIERCE", "R EV." → "REV.".
_LITEPARSE_SMALLCAPS_SPLIT_RE = re.compile(
    r"(?<![A-Za-z])([A-Z]) ([A-Z]{3,}|[A-Z]{2}(?=[.\-',]))"
)


def _normalize_docling_text(text: str) -> str:
    """Clean systematic spacing artifacts produced by Docling's OCR/layout engine.

    Applied at extraction time so all downstream consumers (segmenter, eval)
    receive clean text without needing their own fixups.
    """
    if not text:
        return text
    # Remove spaces before punctuation: "946 ." → "946.", "1309 , 1313" → "1309, 1313"
    text = re.sub(r"\s+([.,;:!?\)\]\}])", r"\1", text)
    # Remove spaces after opening parens/brackets: "( 2003 )" → "(2003 )"
    text = re.sub(r"([\(\[\{])\s+", r"\1", text)
    # Fix reporter citation spacing: "A. 2 d" → "A.2d", "F. 3 d" → "F.3d", etc.
    text = _REPORTER_SPACED_RE.sub(r"\1.\2\3", text)
    # Fix star-page spacing: "* 3" → "*3" (only after whitespace or start of string)
    text = re.sub(r"(?<=\s)\*\s+(\d)", r"*\1", text)
    text = re.sub(r"^\*\s+(\d)", r"*\1", text)
    # Fix ordinal spacing: "7 th" → "7th", "1 st" → "1st", "2 nd" → "2nd", "3 rd" → "3rd"
    text = re.sub(r"(\d)\s+(st|nd|rd|th)\b", r"\1\2", text)
    # Fix hyphen/dash spacing: "1315 -16" → "1315-16", "1699 -n" → "1699-n"
    text = re.sub(r"(\w)\s+(-)\s*(\w)", r"\1\2\3", text)
    text = re.sub(r"(\w)\s*(-)\s+(\w)", r"\1\2\3", text)
    # Fix slash spacing: "2004 / 25" → "2004/25"
    text = re.sub(r"(\w)\s+(/)\s*(\w)", r"\1\2\3", text)
    text = re.sub(r"(\w)\s*(/)\s+(\w)", r"\1\2\3", text)
    # Collapse double spaces
    text = re.sub(r"  +", " ", text)
    return text.strip()


def _normalize_liteparse_text(text: str) -> str:
    """Clean kerning-split word artifacts produced by liteparse's character-level layout engine.

    liteparse occasionally separates a single leading character from the rest of a
    word due to PDF character spacing/kerning (e.g. "s upra" → "supra", "i d." → "Id.").
    Applied at extraction time so downstream citation-cue regex and segmenter receive
    clean text — mirrors the role of _normalize_docling_text for docling output.
    """
    if not text:
        return text

    def _rejoin(m: re.Match) -> str:
        # Exactly one group pair will be non-None; reconstruct the joined word.
        parts = [g for g in m.groups() if g is not None]
        return "".join(parts)

    text = _LITEPARSE_KERNING_SPLIT_RE.sub(_rejoin, text)
    # Small-caps repair: rejoin a single uppercase letter separated from an
    # uppercase word fragment (e.g. "C ONSUMER" → "CONSUMER", "P IERCE" → "PIERCE").
    text = _LITEPARSE_SMALLCAPS_SPLIT_RE.sub(r"\1\2", text)
    # Collapse any double spaces introduced by the merge
    text = re.sub(r"  +", " ", text)
    return text.strip()


def _join_word_text(parts: Sequence[str]) -> str:
    text = " ".join(part.strip() for part in parts if part and part.strip())
    if not text:
        return ""
    for token in [",", ".", ";", ":", "?", "!", ")", "]", "}"]:
        text = text.replace(f" {token}", token)
    for token in ["(", "[", "{"]:
        text = text.replace(f"{token} ", token)
    return " ".join(text.split())


def _line_from_text(text: str, page_number: int, source: str = "text") -> list[ExtractedLine]:
    lines: list[ExtractedLine] = []
    for raw in (text or "").splitlines():
        line = " ".join(raw.split())
        if line:
            lines.append(ExtractedLine(text=line, page_number=page_number, source=source))
    return lines


def _normalize_token(token: str) -> str:
    return re.sub(r"^[^\w]+|[^\w]+$", "", token or "").lower()


def _line_looks_reversed(text: str) -> bool:
    tokens = [token for token in (text or "").split() if token.strip()]
    if _NOTE_LIKE_RE.match(text or "") and len(tokens) >= 2:
        tokens = tokens[1:]
    if len(tokens) < 4:
        return False
    first = _normalize_token(tokens[0])
    last = _normalize_token(tokens[-1])
    if not last or last not in _OPENING_TOKENS:
        return False
    if not first or first in _OPENING_TOKENS:
        return False
    alpha_tokens = [token for token in tokens if any(ch.isalpha() for ch in token)]
    if len(alpha_tokens) < 4:
        return False
    early_tokens = alpha_tokens[:3]
    has_sentence_like_lead = any(
        any(ch.isdigit() for ch in token)
        or any(ch in token for ch in ".,;:()[]")
        or token[:1].islower()
        for token in early_tokens
    )
    if not has_sentence_like_lead:
        return False
    title_like_ratio = sum(token[:1].isupper() for token in alpha_tokens) / max(
        len(alpha_tokens), 1
    )
    return title_like_ratio < 0.8


def _reversed_word_order_suspected(lines: list[ExtractedLine]) -> bool:
    candidates = [line for line in lines if _NOTE_LIKE_RE.match(line.text or "")]
    if len(candidates) < 2:
        return False
    suspicious = sum(1 for line in candidates if _line_looks_reversed(line.text))
    return suspicious >= 2 and suspicious / max(len(candidates), 1) >= 0.4


def _low_font_variance(lines: list[ExtractedLine]) -> bool:
    sizes = [float(line.font_size) for line in lines if line.font_size is not None]
    if len(sizes) < 6:
        return False
    return max(sizes) - min(sizes) <= 1.25


def _merge_smallcaps_textitems(words: list[dict]) -> list[dict]:
    """Merge single-uppercase-letter textItems with their adjacent word fragment.

    Small-caps fonts in PDFs render the first letter at full size and subsequent
    letters as smaller uppercase glyphs. liteparse emits these as separate
    textItems with different fontSize values, which causes the first letter to
    end up on a different line (due to y-position variance) and get dropped.

    This function detects a single uppercase letter that is horizontally
    adjacent to an uppercase word fragment (possibly at a different y due to
    font-size difference), and merges them into one textItem so they cluster
    onto the same line.
    """
    if len(words) < 2:
        return words

    consumed: set[int] = set()
    merged: list[dict] = []

    for i, item in enumerate(words):
        if i in consumed:
            continue
        txt = str(item.get("text", "")).strip()
        if len(txt) != 1 or not txt.isupper():
            merged.append(item)
            continue

        # Find the best right-adjacent candidate: closest item whose x0 is
        # near this item's x1 and whose y overlaps (within tolerance).
        item_x1 = float(item.get("x1", 0.0))
        item_top = float(item.get("top", 0.0))
        item_bottom = float(item.get("bottom", 0.0))
        item_size = float(item.get("size", 0.0) or 0.0)

        best_j: int | None = None
        best_gap = float("inf")
        for j, cand in enumerate(words):
            if j == i or j in consumed:
                continue
            cand_txt = str(cand.get("text", "")).strip()
            if not cand_txt or not cand_txt[0].isupper():
                continue
            cand_x0 = float(cand.get("x0", 0.0))
            cand_top = float(cand.get("top", 0.0))
            cand_bottom = float(cand.get("bottom", 0.0))
            cand_size = float(cand.get("size", 0.0) or 0.0)

            # Must be to the right and horizontally close.
            gap = cand_x0 - item_x1
            if gap < -2.0:  # allow tiny overlap
                continue
            max_size = max(item_size, cand_size, 6.0)
            if gap > max_size * 0.6:
                continue

            # y-ranges must overlap or be very close (small-caps baseline shift).
            y_tolerance = max_size * 0.5
            if item_top > cand_bottom + y_tolerance or cand_top > item_bottom + y_tolerance:
                continue

            # Prefer font-size difference (small-caps hallmark) or very tight gap.
            sizes_differ = (
                item_size > 0 and cand_size > 0 and abs(item_size - cand_size) > 0.5
            )
            if not sizes_differ and gap >= 1.0:
                continue

            if gap < best_gap:
                best_gap = gap
                best_j = j

        if best_j is not None:
            nxt = words[best_j]
            nxt_txt = str(nxt.get("text", "")).strip()
            nxt_size = float(nxt.get("size", 0.0) or 0.0)
            consumed.add(best_j)
            merged.append({
                "text": txt + nxt_txt,
                "x0": float(item.get("x0", 0.0)),
                "x1": float(nxt.get("x1", 0.0)),
                "top": min(item_top, float(nxt.get("top", 0.0))),
                "bottom": max(item_bottom, float(nxt.get("bottom", 0.0))),
                "size": max(item_size, nxt_size) if item_size and nxt_size else (
                    item_size or nxt_size
                ),
            })
        else:
            merged.append(item)

    return merged


def _cluster_words_to_lines(words: list[dict], page_number: int) -> list[ExtractedLine]:
    if not words:
        return []
    column_split = _detect_word_column_split(words)
    if column_split is not None:
        left_words = [
            word
            for word in words
            if _word_x_center(word) < column_split
        ]
        right_words = [
            word
            for word in words
            if _word_x_center(word) >= column_split
        ]
        if left_words and right_words:
            return _cluster_words_to_lines_single_column(
                left_words, page_number=page_number
            ) + _cluster_words_to_lines_single_column(right_words, page_number=page_number)
    return _cluster_words_to_lines_single_column(words, page_number=page_number)


def _word_x_center(word: dict) -> float:
    return (float(word.get("x0", 0.0)) + float(word.get("x1", 0.0))) / 2.0


def _detect_word_column_split(words: list[dict]) -> float | None:
    """Find the x of a two-column gutter, or None for a single-column page.

    Two detectors, first hit wins:
      1. center-gap — a large gap between adjacent sorted word x-centers. Fires
         when the gutter leaves a clean break in the *centers* distribution.
      2. projection-profile — the x in the central band crossed by almost no
         word span (a near-empty vertical gutter). This catches DENSE two-column
         scans where word centers fill the page so no single center-to-center
         gap is large enough for (1). That was the failure mode scrambling
         two-column scanned law reviews: undetected columns got y-banded into
         interleaved lines ("…the Oilspill Act had been passed Ollspill Act
         received surprisingly little public unanimously by both the House…").
    """
    return (
        _detect_column_split_center_gap(words)
        or _detect_column_split_projection(words)
    )


def _detect_column_split_center_gap(words: list[dict]) -> float | None:
    if len(words) < 12:
        return None

    centers = sorted(_word_x_center(word) for word in words)
    if not centers:
        return None
    x_span = centers[-1] - centers[0]
    if x_span < 250.0:
        return None

    gaps = [
        (centers[idx + 1] - centers[idx], idx)
        for idx in range(len(centers) - 1)
    ]
    positive_gaps = [gap for gap, _idx in gaps if gap > 0.0]
    if not positive_gaps:
        return None

    largest_gap, split_idx = max(gaps, key=lambda item: item[0])
    median_gap = statistics.median(positive_gaps)
    if largest_gap < max(55.0, median_gap * 4.0):
        return None

    left_count = split_idx + 1
    right_count = len(centers) - left_count
    min_side = max(4, int(len(centers) * 0.20))
    if left_count < min_side or right_count < min_side:
        return None

    split_x = (centers[split_idx] + centers[split_idx + 1]) / 2.0
    if not (centers[0] + x_span * 0.30 <= split_x <= centers[0] + x_span * 0.70):
        return None
    return split_x


# Projection-profile column detection. A two-column page has a near-empty
# vertical band (the gutter) in its central third that almost no word's
# [x0, x1] span crosses; a single-column page's centre is full of text, so the
# central-band coverage minimum stays high and this returns None. Conservative
# by design (high min-word count, deep-gutter + balanced-sides + few-straddlers
# guards) so it never splits a genuine single-column page.
_PROJECTION_MIN_WORDS = 40
_PROJECTION_GUTTER_COV_FRAC = 0.12    # gutter coverage <= 12% of central-band peak
_PROJECTION_MAX_STRADDLE_FRAC = 0.03  # <= 3% of words may cross the gutter


def _detect_column_split_projection(words: list[dict]) -> float | None:
    spans = [
        (float(w.get("x0", 0.0)), float(w.get("x1", 0.0))) for w in words
    ]
    spans = [(x0, x1) for x0, x1 in spans if x1 > x0]
    if len(spans) < _PROJECTION_MIN_WORDS:
        return None

    centers = sorted((x0 + x1) / 2.0 for x0, x1 in spans)
    if centers[-1] - centers[0] < 250.0:
        return None

    left_edge = min(x0 for x0, _ in spans)
    right_edge = max(x1 for _, x1 in spans)
    width = right_edge - left_edge
    if width < 250.0:
        return None

    # Word-span coverage at 1-unit resolution via a difference array.
    n = int(width) + 2
    delta = [0] * (n + 1)
    for x0, x1 in spans:
        a = max(0, int(x0 - left_edge))
        b = min(n, int(x1 - left_edge))
        if b <= a:
            b = a + 1
        delta[a] += 1
        delta[b] -= 1
    cov = [0] * n
    run = 0
    for i in range(n):
        run += delta[i]
        cov[i] = run

    lo = int(width * 0.30)
    hi = int(width * 0.70)
    if hi <= lo:
        return None
    gutter_rel = min(range(lo, hi), key=lambda i: cov[i])
    band_peak = max(cov[lo:hi]) or 1
    if cov[gutter_rel] > max(2, _PROJECTION_GUTTER_COV_FRAC * band_peak):
        return None

    split_x = float(left_edge + gutter_rel)
    left_count = sum(1 for c in centers if c < split_x)
    right_count = len(centers) - left_count
    min_side = max(4, int(len(centers) * 0.20))
    if left_count < min_side or right_count < min_side:
        return None

    straddle = sum(1 for x0, x1 in spans if x0 < split_x < x1)
    if straddle > max(3, int(len(spans) * _PROJECTION_MAX_STRADDLE_FRAC)):
        return None
    return split_x


def _group_words_by_y_band(words: list[dict], *, tolerance: float = 2.5) -> list[list[dict]]:
    sorted_words = sorted(
        words, key=lambda word: (float(word.get("top", 0.0)), float(word.get("x0", 0.0)))
    )
    bins: list[list[dict]] = []
    for word in sorted_words:
        top = float(word.get("top", 0.0))
        if not bins:
            bins.append([word])
            continue
        prev_top = float(statistics.median(float(item.get("top", 0.0)) for item in bins[-1]))
        if abs(top - prev_top) <= tolerance:
            bins[-1].append(word)
        else:
            bins.append([word])
    return bins


def _text_fidelity_score_for_word_pages(word_pages: Sequence[list[dict]]) -> float | None:
    """Cheap coordinate proxy for text fidelity after column-aware clustering.

    ExtractedLine currently stores line text and y/font metadata, not per-token
    x positions. That means exact per-note "first 10 words are x-monotonic"
    scoring is not available once notes are segmented. Instead, score page
    y-bands before line construction: a band is good when it is single-column,
    or when a detected two-column split lets clustering handle each side
    independently. This conservatively penalizes only obvious cross-column
    mixing risk and remains deterministic/cheap.
    """
    good = 0
    total = 0
    for words in word_pages:
        page_words = [word for word in words if str(word.get("text", "")).strip()]
        if len(page_words) < 2:
            continue
        split = _detect_word_column_split(page_words)
        for band in _group_words_by_y_band(page_words):
            if len(band) < 2:
                continue
            total += 1
            if split is None:
                if not _y_band_has_unresolved_column_gap(band):
                    good += 1
                continue
            left = [word for word in band if _word_x_center(word) < split]
            right = [word for word in band if _word_x_center(word) >= split]
            if left and right:
                # The main line builder clusters each side separately, so a
                # same-y two-column band should not be emitted as one mixed line.
                good += 1
            else:
                good += 1
    if total == 0:
        return None
    return round(good / total, 4)


def _y_band_has_unresolved_column_gap(words: list[dict]) -> bool:
    centers = sorted(_word_x_center(word) for word in words)
    if len(centers) < 4:
        return False
    x_span = centers[-1] - centers[0]
    if x_span < 250.0:
        return False
    gaps = [(centers[idx + 1] - centers[idx], idx) for idx in range(len(centers) - 1)]
    positive_gaps = [gap for gap, _idx in gaps if gap > 0.0]
    if not positive_gaps:
        return False
    largest_gap, split_idx = max(gaps, key=lambda item: item[0])
    median_gap = statistics.median(positive_gaps)
    if largest_gap < max(55.0, median_gap * 4.0):
        return False
    return split_idx + 1 >= 2 and len(centers) - (split_idx + 1) >= 2


def _raw_items_to_word_dicts(raw_items: Sequence[dict]) -> list[dict]:
    words: list[dict] = []
    for item in raw_items:
        text = str(item.get("text", "") or "").strip()
        if not text:
            continue
        x = float(item.get("x", 0.0) or 0.0)
        y = float(item.get("y", 0.0) or 0.0)
        width = float(item.get("width", 0.0) or 0.0)
        height = float(item.get("height", 0.0) or 0.0)
        if height <= 0.0:
            height = float(item.get("fontSize", 0.0) or 0.0)
        words.append(
            {
                "text": text,
                "x0": x,
                "x1": x + width,
                "top": y,
                "bottom": y + height if height > 0.0 else y,
                "size": float(item.get("fontSize", 0.0) or 0.0) or None,
            }
        )
    return words


def _cluster_words_to_lines_single_column(
    words: list[dict], page_number: int
) -> list[ExtractedLine]:
    lines: list[ExtractedLine] = []
    for grouped_words in _group_words_by_y_band(words):
        grouped_words = sorted(grouped_words, key=lambda word: float(word.get("x0", 0.0)))
        texts = [str(word.get("text", "")).strip() for word in grouped_words]
        line_text = _join_word_text(texts)
        if not line_text:
            continue
        tops = [float(word.get("top", 0.0)) for word in grouped_words]
        bottoms = [float(word.get("bottom", 0.0)) for word in grouped_words]
        sizes = [
            float(word.get("size", 0.0)) for word in grouped_words if word.get("size") is not None
        ]
        lines.append(
            ExtractedLine(
                text=line_text,
                page_number=page_number,
                top=min(tops) if tops else 0.0,
                bottom=max(bottoms) if bottoms else 0.0,
                font_size=statistics.median(sizes) if sizes else None,
                source="text",
            )
        )
    return lines


def _clone_line(line: ExtractedLine) -> ExtractedLine:
    return ExtractedLine(
        text=line.text,
        page_number=line.page_number,
        top=line.top,
        bottom=line.bottom,
        font_size=line.font_size,
        source=line.source,
    )


def _clone_lines(lines: list[ExtractedLine] | tuple[ExtractedLine, ...]) -> list[ExtractedLine]:
    return [_clone_line(line) for line in lines]


def _find_footnote_separator(page: Any) -> float | None:
    """
    Search for horizontal lines or thin rectangles that typically separate
    body text from footnotes in law reviews.
    """
    height = float(getattr(page, "height", 0.0) or 0.0)
    width = float(getattr(page, "width", 0.0) or 0.0)
    if not height or not width:
        return None

    candidates: list[float] = []

    # Search explicit lines
    lines = getattr(page, "lines", [])
    for line in lines:
        # Must be horizontal and thin
        if abs(line.get("top", 0) - line.get("bottom", 0)) < 2.0:
            # Must be in the bottom 60% of the page
            if line.get("top", 0) > height * 0.4:
                # Must span at least 15% of page width
                line_width = abs(line.get("x1", 0) - line.get("x0", 0))
                if line_width > width * 0.15:
                    candidates.append(float(line.get("top")))

    # Search thin rectangles
    rects = getattr(page, "rects", [])
    for rect in rects:
        # Debugging showed USC/CUNY use rects with h=0.5 or 0.7
        if float(rect.get("height", 0)) < 1.1:
            if float(rect.get("top", 0)) > height * 0.4:
                rect_width = float(rect.get("width", 0))
                # Law review separators are often short (144.0) but consistent
                if rect_width > width * 0.1:
                    candidates.append(float(rect.get("top")))

    if not candidates:
        return None

    # If multiple, pick the one that is closest to the bottom but likely valid.
    # We sort by 'top' descending (closest to bottom first).
    return max(candidates)


def _find_pdfplumber_separator_priors(
    pdf_path: str,
    *,
    page_numbers: set[int] | None = None,
) -> dict[int, float]:
    """Return page_number -> visible footnote-rule y positions from pdfplumber.

    LiteParse gives us excellent textItem coordinates but not PDF vector
    drawing primitives. A geometry-only pdfplumber pass is cheap and lets the
    LiteParse solver incorporate explicit horizontal rules / thin rectangles
    when a PDF provides them.
    """
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return {}

    priors: dict[int, float] = {}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for idx, page in enumerate(pdf.pages, start=1):
                if page_numbers is not None and idx not in page_numbers:
                    continue
                sep_y = _find_footnote_separator(page)
                if sep_y is not None:
                    priors[idx] = sep_y
    except Exception:
        return {}
    return priors


def _liteparse_page_has_lower_note_probe(layout: _LiteparsePageLayout) -> bool:
    height = float(layout.height or 0.0)
    if not height:
        return False
    lower_lines = [
        line
        for line in layout.lines
        if height * 0.42 <= float(line.top or 0.0) <= height * 0.97
    ]
    note_like = [
        line
        for line in lower_lines
        if _SEPARATOR_ARABIC_NOTE_RE.match((line.text or "").strip())
    ]
    if len(note_like) >= 2:
        return True
    return any(_STRICT_FOOTNOTE_SIGNAL_RE.search(line.text or "") for line in lower_lines)


def _separator_supported_by_liteparse(
    layout: _LiteparsePageLayout,
    separator_y: float,
) -> bool:
    height = float(layout.height or 0.0)
    if not height:
        return False
    sep_y = float(separator_y)
    if sep_y < height * 0.42 or sep_y > height * 0.88:
        return False

    below = [
        line
        for line in layout.lines
        if sep_y - 2.0 <= float(line.top or 0.0) <= min(height * 0.97, sep_y + 260.0)
    ]
    note_like = [
        line
        for line in below
        if _SEPARATOR_ARABIC_NOTE_RE.match((line.text or "").strip())
    ]
    if not note_like:
        return False
    first_note_y = min(float(line.top or 0.0) for line in note_like)
    if first_note_y - sep_y > 85.0:
        return False

    fonts = sorted(
        float(line.font_size or 0.0)
        for line in layout.lines
        if float(line.font_size or 0.0) > 0.0
    )
    median_font = fonts[len(fonts) // 2] if fonts else 0.0
    small_note_lines = sum(
        1
        for line in note_like
        if median_font and float(line.font_size or 0.0) < median_font * 0.97
    )
    legal_signal = any(
        _STRICT_FOOTNOTE_SIGNAL_RE.search(line.text or "")
        or _LEGAL_FOOTNOTE_SIGNAL_RE.search(line.text or "")
        for line in below
    )
    return len(note_like) >= 2 and (small_note_lines > 0 or legal_signal)


def _env_flag_enabled(name: str) -> bool:
    raw = (os.getenv(name, "0") or "0").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _find_font_transition(lines: list[ExtractedLine]) -> float | None:
    """
    Analyze font size distribution to find where body text ends and footnotes begin.
    """
    if not lines:
        return None

    sizes = [line.font_size for line in lines if line.font_size is not None]
    if len(sizes) < 10:
        return None

    median_size = statistics.median(sizes)
    # Most law review body text is 10-12pt, footnotes 8-10pt.
    # We look for a consistent drop below 92% of median.
    threshold = median_size * 0.92

    # Look for a vertical point where most lines below it are small
    # and lines above it are large.
    candidate_cutoffs = []
    for i in range(int(len(lines) * 0.4), len(lines) - 2):
        above = lines[:i]
        below = lines[i:]

        above_sizes = [line.font_size for line in above if line.font_size is not None]
        below_sizes = [line.font_size for line in below if line.font_size is not None]

        if not above_sizes or not below_sizes:
            continue

        avg_above = sum(above_sizes) / len(above_sizes)
        avg_below = sum(below_sizes) / len(below_sizes)

        # If we see a significant drop
        if avg_above > median_size * 0.98 and avg_below < threshold:
            candidate_cutoffs.append(lines[i].top)

    if candidate_cutoffs:
        return candidate_cutoffs[0]
    return None


def _adaptive_low_variance_cutoff(lines: list[ExtractedLine], page_height: float) -> float:
    """
    Estimate a less conservative footnote cutoff for low-variance scans.

    We start from the default 72% region and scan upward only when the bottom
    region lacks note-like signals. This reduces missed footnotes on scanned
    pages where body/note font sizes are nearly identical.
    """
    if page_height <= 0:
        return 0.0

    candidate_ratios = (0.72, 0.68, 0.64, 0.60, 0.56)
    for ratio in candidate_ratios:
        cutoff = page_height * ratio
        below = [line for line in lines if line.top >= cutoff]
        if len(below) < 3:
            continue
        note_like = sum(1 for line in below if _NOTE_LIKE_RE.match(line.text or ""))
        legal_cue = sum(1 for line in below if _LEGAL_FOOTNOTE_SIGNAL_RE.search(line.text or ""))
        signal_density = (note_like + (0.5 * legal_cue)) / max(len(below), 1)
        if note_like >= 2 and signal_density >= 0.2:
            return cutoff
    return page_height * 0.60


def _classify_lines(
    lines: list[ExtractedLine],
    page_height: float,
    manual_cutoff: float | None = None,
    note_cutoff_ratio: float | None = None,
) -> tuple[list[ExtractedLine], list[ExtractedLine], bool]:
    if not lines:
        return [], [], False

    sizes = [line.font_size for line in lines if line.font_size is not None]
    median_size = statistics.median(sizes) if sizes else None
    low_font_variance = _low_font_variance(lines)

    body_lines: list[ExtractedLine] = []
    note_lines: list[ExtractedLine] = []

    # Priority 1: Manual separator detected by page line/rect
    # Priority 2: Detected font transition
    # Priority 3: Fixed 72% heuristic fallback
    if manual_cutoff is not None:
        note_cutoff = manual_cutoff
    elif note_cutoff_ratio is not None and page_height:
        note_cutoff = page_height * note_cutoff_ratio
    else:
        transition = _find_font_transition(lines)
        if transition is not None:
            note_cutoff = transition
        else:
            if low_font_variance:
                note_cutoff = _adaptive_low_variance_cutoff(lines, page_height)
            else:
                note_cutoff = page_height * 0.72 if page_height else 0.0

    for line in lines:
        in_bottom_region = bool(page_height and line.top >= note_cutoff)
        smaller_font = bool(
            not low_font_variance
            and median_size
            and line.font_size
            and line.font_size < (median_size * 0.9)
        )
        if in_bottom_region or smaller_font:
            note_lines.append(line)
        else:
            body_lines.append(line)

    if not body_lines and note_lines:
        # Prevent pathological classifications on OCR-heavy pages.
        body_lines = note_lines[:]
        note_lines = []

    return body_lines, note_lines, low_font_variance


def _extract_with_pdfplumber(
    pdf_path: str, *, note_cutoff_ratio: float | None = None
) -> ExtractedDocument | None:
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return None

    pages: list[ExtractedPage] = []
    warnings: list[str] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            low_font_variance_detected = False
            reversed_word_order_detected = False
            word_pages: list[list[dict]] = []
            for idx, page in enumerate(pdf.pages, start=1):
                width = float(getattr(page, "width", 0.0) or 0.0)
                height = float(getattr(page, "height", 0.0) or 0.0)

                words: list[dict] = []
                try:
                    words = (
                        page.extract_words(
                            use_text_flow=True,
                            keep_blank_chars=False,
                            extra_attrs=["size"],
                        )
                        or []
                    )
                except Exception:
                    warnings.append(f"word extraction failed on page {idx}")
                word_pages.append(words)

                raw_text = ""
                try:
                    raw_text = page.extract_text() or ""
                except Exception:
                    warnings.append(f"text extraction failed on page {idx}")

                lines = _cluster_words_to_lines(words, page_number=idx)
                if not lines:
                    lines = _line_from_text(raw_text, page_number=idx)

                # Look for horizontal separator line
                manual_cutoff = _find_footnote_separator(page)

                body_lines, note_lines, page_low_font_variance = _classify_lines(
                    lines,
                    page_height=height,
                    manual_cutoff=manual_cutoff,
                    note_cutoff_ratio=note_cutoff_ratio,
                )
                low_font_variance_detected = low_font_variance_detected or page_low_font_variance
                reversed_word_order_detected = (
                    reversed_word_order_detected or _reversed_word_order_suspected(note_lines)
                )
                pages.append(
                    ExtractedPage(
                        page_number=idx,
                        width=width,
                        height=height,
                        body_lines=body_lines,
                        note_lines=note_lines,
                        raw_text=raw_text,
                        source="text",
                    )
                )
    except Exception:
        return None

    if low_font_variance_detected:
        warnings.append("low_font_variance_detected")
    if reversed_word_order_detected:
        warnings.append("reversed_word_order_suspected")

    metadata: dict[str, Any] = {}
    text_fidelity_score = _text_fidelity_score_for_word_pages(word_pages)
    if text_fidelity_score is not None:
        metadata["text_fidelity_score"] = text_fidelity_score
        metadata["text_fidelity_score_method"] = "page_y_band_column_proxy_v1"

    return ExtractedDocument(pdf_path=pdf_path, pages=pages, warnings=warnings, metadata=metadata)


def _extract_with_pypdf(pdf_path: str) -> ExtractedDocument | None:
    try:
        suppress_pypdf_noise()
        from pypdf import PdfReader  # type: ignore
    except Exception:
        return None

    pages: list[ExtractedPage] = []
    warnings: list[str] = []

    try:
        reader = PdfReader(pdf_path)
        for idx, page in enumerate(reader.pages, start=1):
            try:
                raw_text = page.extract_text() or ""
            except Exception:
                raw_text = ""
                warnings.append(f"pypdf text extraction failed on page {idx}")
            body_lines = _line_from_text(raw_text, page_number=idx)
            pages.append(
                ExtractedPage(
                    page_number=idx,
                    body_lines=body_lines,
                    note_lines=[],
                    raw_text=raw_text,
                    source="text",
                )
            )
    except Exception:
        return None

    return ExtractedDocument(pdf_path=pdf_path, pages=pages, warnings=warnings, parser="pypdf")


def _extract_with_docling(pdf_path: str) -> ExtractedDocument | None:
    try:
        from docling.document_converter import DocumentConverter  # type: ignore
    except Exception:
        return None

    try:
        converter = DocumentConverter()
        result = converter.convert(pdf_path)
        if not result or not result.document:
            return None

        # Try structured JSON export first for better footnote extraction
        doc_dict = None
        try:
            doc_dict = result.document.export_to_dict()
        except Exception:
            pass

        if doc_dict and "texts" in doc_dict:
            return _parse_docling_dict(pdf_path, doc_dict)

        # Fallback to markdown parsing
        markdown = result.document.export_to_markdown()
        if not markdown.strip():
            return ExtractedDocument(
                pdf_path=pdf_path,
                pages=[],
                warnings=["docling returned empty content"],
                parser="docling",
            )

        return _parse_docling_markdown(pdf_path, markdown)
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


_LITEPARSE_DASH_CHARS = frozenset("-–—―_‒⸺⸻~")


def _is_dash_only_line(text: str) -> bool:
    non_space = [c for c in (text or "") if not c.isspace()]
    if len(non_space) < 6:
        return False
    return sum(1 for c in non_space if c in _LITEPARSE_DASH_CHARS) / len(non_space) >= 0.9


def _find_liteparse_dash_separator(
    lines: list[ExtractedLine], *, page_height: float
) -> float | None:
    """Return the top-y of a dashed-glyph separator line when one appears in
    the bottom half of the page. Some law review PDFs render the footnote
    separator as a run of dash glyphs (rather than a vector line), which
    liteparse surfaces as an ordinary text item."""
    if page_height <= 0:
        return None
    for line in lines:
        if not _is_dash_only_line(line.text or ""):
            continue
        if float(line.top) / page_height < 0.4:
            continue
        return float(line.top)
    return None


def _bimodal_font_split(
    lines: list[ExtractedLine], *, page_height: float
) -> float | None:
    """Detect a bimodal font-size distribution per page and return the top-y
    boundary above which body text lies and below which footnotes lie."""
    sized = [ln for ln in lines if ln.font_size is not None]
    if len(sized) < 10 or page_height <= 0:
        return None
    from collections import Counter
    buckets = Counter(int(round(float(ln.font_size))) for ln in sized)
    if len(buckets) < 2:
        return None
    total = len(sized)
    significant = sorted(s for s, c in buckets.items() if c / total >= 0.12)
    if len(significant) < 2:
        return None
    small_size = float(significant[0])
    big_size = float(significant[-1])
    if big_size <= 0 or small_size / big_size > 0.9:
        return None
    threshold = (big_size + small_size) / 2.0
    sorted_lines = sorted(sized, key=lambda ln: float(ln.top), reverse=True)
    best_y: float | None = None
    small_count = 0
    total_region = 0
    for line in sorted_lines:
        total_region += 1
        is_small = line.font_size is not None and line.font_size <= threshold
        if is_small:
            small_count += 1
        if total_region >= 4 and (small_count / total_region) >= 0.70:
            if is_small:
                best_y = float(line.top)
        elif total_region >= 4:
            break
    if best_y is None or small_count < 3:
        return None
    if best_y / page_height < 0.4:
        return None
    return best_y


def _low_variance_density_split(
    lines: list[ExtractedLine], *, page_height: float
) -> float | None:
    """Find a footnote boundary when font size cannot separate body and notes."""
    if len(lines) < 6 or page_height <= 0:
        return None
    ordered = sorted(lines, key=lambda line: float(line.top))
    for idx, line in enumerate(ordered):
        rel_top = float(line.top) / page_height
        if rel_top < 0.50:
            continue
        below = ordered[idx:]
        if len(below) < 3 or len(below) > max(5, int(len(ordered) * 0.60)):
            continue
        first_three = [" ".join((ln.text or "").split()) for ln in below[:3]]
        if not any(_NOTE_LIKE_RE.match(text) for text in first_three):
            continue
        note_like = sum(1 for ln in below if _NOTE_LIKE_RE.match(" ".join((ln.text or "").split())))
        legal_cue = sum(1 for ln in below if _LEGAL_FOOTNOTE_SIGNAL_RE.search(ln.text or ""))
        short_id = sum(1 for ln in below if _SHORT_ID_NOTE_RE.match(ln.text or ""))
        signal = note_like + (0.6 * legal_cue) + (0.8 * short_id)
        density = signal / max(len(below), 1)
        if (note_like >= 2 and density >= 0.45) or (note_like >= 1 and legal_cue >= 2 and density >= 0.55):
            return float(line.top)
    return None


def _marker_density_split(
    lines: list[ExtractedLine], *, page_height: float
) -> float | None:
    if len(lines) < 6 or page_height <= 0:
        return None

    ordered = sorted(lines, key=lambda line: float(line.top))
    for idx, line in enumerate(ordered):
        rel_top = float(line.top) / page_height
        if rel_top < 0.45:
            continue
        below = ordered[idx:]
        if len(below) < 2 or len(below) > max(6, int(len(ordered) * 0.70)):
            continue
        note_like = 0
        legal_cue = 0
        for candidate in below:
            text = " ".join((candidate.text or "").split())
            if _NOTE_LIKE_RE.match(text):
                note_like += 1
            if _LEGAL_FOOTNOTE_SIGNAL_RE.search(text):
                legal_cue += 1
        density = (note_like + (0.5 * legal_cue)) / max(len(below), 1)
        if note_like >= 2 and density >= 0.35:
            return float(line.top)
    return None


def _pattern_density_strict_split(
    lines: list[ExtractedLine], *, page_height: float
) -> float | None:
    if len(lines) < 8 or page_height <= 0:
        return None
    ordered = sorted(lines, key=lambda line: float(line.top))
    for idx, line in enumerate(ordered):
        rel_top = float(line.top) / page_height
        if rel_top < 0.40:
            continue
        window = ordered[idx : idx + 8]
        below = ordered[idx:]
        if len(window) < 5 or len(below) > max(8, int(len(ordered) * 0.75)):
            continue
        strong = 0
        starts = 0
        for candidate in window:
            text = " ".join((candidate.text or "").split())
            if _NOTE_LIKE_RE.match(text):
                starts += 1
                strong += 1
            elif _STRICT_FOOTNOTE_SIGNAL_RE.search(text):
                strong += 1
        if strong >= 5 and starts >= 2:
            return float(line.top)
    return None


def _line_is_body_like_for_liberal_notes(
    line: ExtractedLine,
    *,
    page_height: float,
    median_size: float | None,
) -> bool:
    text = " ".join((line.text or "").split())
    if not text:
        return True
    rel_top = float(line.top) / page_height if page_height > 0 else 0.0
    if rel_top >= 0.92 and re.fullmatch(r"\d{1,4}", text):
        return True
    if _RUNNING_HEAD_JOURNAL_CITE_RE.match(text):
        return True
    if _SECTION_HEADING_LINE_RE.match(text):
        return True
    if (
        median_size
        and line.font_size is not None
        and float(line.font_size) >= median_size * 1.12
        and not _NOTE_LIKE_RE.match(text)
        and not _STRICT_FOOTNOTE_SIGNAL_RE.search(text)
    ):
        return True
    return False


def _liberal_notes_split(
    lines: list[ExtractedLine], *, page_height: float
) -> tuple[list[ExtractedLine], list[ExtractedLine], bool]:
    if len(lines) < 4 or page_height <= 0:
        return [], [], False

    sizes = [float(line.font_size) for line in lines if line.font_size is not None]
    median_size = statistics.median(sizes) if sizes else None
    body: list[ExtractedLine] = []
    notes: list[ExtractedLine] = []
    for line in lines:
        rel_top = float(line.top) / page_height if page_height > 0 else 0.0
        if rel_top < 0.40 or _line_is_body_like_for_liberal_notes(
            line, page_height=page_height, median_size=median_size
        ):
            body.append(line)
        else:
            notes.append(line)

    if not body or len(notes) < 2:
        return [], [], False
    note_like = sum(1 for line in notes if _NOTE_LIKE_RE.match(" ".join((line.text or "").split())))
    signal = sum(1 for line in notes if _STRICT_FOOTNOTE_SIGNAL_RE.search(line.text or ""))
    if note_like < 1 and signal < 2:
        return [], [], False
    if len(notes) > max(6, int(len(lines) * 0.85)):
        return [], [], False
    return body, notes, True


def _split_lines_at_y(
    lines: list[ExtractedLine], sep_y: float
) -> tuple[list[ExtractedLine], list[ExtractedLine]]:
    body = [ln for ln in lines if float(ln.top) < sep_y]
    notes = [
        ln
        for ln in lines
        if float(ln.top) >= sep_y and not _is_dash_only_line(ln.text or "")
    ]
    return body, notes


def _classify_liteparse_note_candidates(
    lines: list[ExtractedLine], *, page_height: float
) -> tuple[list[ExtractedLine], list[ExtractedLine], bool]:
    """
    LiteParse-specific classifier for separating body and footnote lines.

    Prefers two deterministic rails when the page supports them:
      1. **Dashed-glyph separator** — some PDFs render the footnote rule as
         a row of dashes, which liteparse emits as a text item.
      2. **Bimodal font split** — per-page k=2 clustering on font size; the
         smaller cluster defines the footnote region when both clusters carry
         significant mass and their ratio is <= 0.9.

    Falls back to position + citation-signal scoring when neither rail
    applies (homogeneous pages, title pages, low-variance scans).
    """
    if not lines or page_height <= 0:
        return [], [], False

    # --- Pre-filter: drop running heads and frontmatter on opening pages ---
    filtered: list[ExtractedLine] = []
    for line in lines:
        text = " ".join((line.text or "").split())
        if not text:
            continue
        if line.page_number <= 3:
            if _RUNNING_HEAD_JOURNAL_CITE_RE.match(text):
                continue
            if _FRONTMATTER_TEXT_RE.search(text):
                continue
        filtered.append(line)

    if not filtered:
        return [], [], False

    low_variance = _low_font_variance(filtered)

    # --- Deterministic rails ---
    sep_y = _find_liteparse_dash_separator(filtered, page_height=page_height)
    if sep_y is None:
        sep_y = _bimodal_font_split(filtered, page_height=page_height)
    if sep_y is None and low_variance:
        sep_y = _low_variance_density_split(filtered, page_height=page_height)
    if sep_y is not None:
        body_det = [ln for ln in filtered if float(ln.top) < sep_y]
        notes_det = [
            ln
            for ln in filtered
            if float(ln.top) >= sep_y and not _is_dash_only_line(ln.text or "")
        ]
        has_note_signal = any(
            _NOTE_LIKE_RE.match(" ".join((ln.text or "").split()))
            or _LEGAL_FOOTNOTE_SIGNAL_RE.search(ln.text or "")
            for ln in notes_det
        )
        if (
            body_det
            and notes_det
            and len(notes_det) <= max(4, int(len(filtered) * 0.8))
            and (has_note_signal or len(notes_det) >= 3)
        ):
            return body_det, notes_det, True

    # --- Scoring fallback ---
    sizes = [line.font_size for line in filtered if line.font_size is not None]
    median_size = statistics.median(sizes) if sizes else None

    notes: list[ExtractedLine] = []
    for line in filtered:
        text = " ".join((line.text or "").split())
        rel_top = (line.top / page_height) if page_height > 0.0 else 0.0

        note_like = bool(_NOTE_LIKE_RE.match(text))
        legal_cue = bool(_LEGAL_FOOTNOTE_SIGNAL_RE.search(text))
        short_id = bool(_SHORT_ID_NOTE_RE.match(text))

        # Font size ratio: smaller font is a strong note signal (mirrors _classify_lines).
        smaller_font = bool(
            not low_variance
            and median_size
            and line.font_size is not None
            and line.font_size < (median_size * 0.9)
        )

        # In the bottom third of the page, a smaller font alone is sufficient.
        if smaller_font and rel_top >= 0.67:
            notes.append(line)
            continue

        score = 0.0
        if note_like:
            score += 2.2
        if legal_cue:
            score += 1.4
        if short_id:
            score += 0.8
        if smaller_font:
            score += 0.6
        if rel_top >= 0.82:
            score += 1.0
        elif rel_top >= 0.72:
            score += 0.7
        elif rel_top >= 0.62:
            score += 0.3

        if note_like and rel_top >= 0.60 and score >= 2.4:
            notes.append(line)
            continue
        if legal_cue and rel_top >= 0.74 and len(text) >= 24 and score >= 2.1:
            notes.append(line)
            continue
        if short_id and rel_top >= 0.82:
            notes.append(line)

    if notes and len(notes) <= max(4, int(len(filtered) * 0.7)):
        note_ids = {id(line) for line in notes}
        body = [line for line in filtered if id(line) not in note_ids]
        if body:
            return body, notes, True

    return [], [], False


def _classify_liteparse_candidate_lines(
    lines: list[ExtractedLine],
    *,
    page_height: float,
    candidate_name: str,
    note_cutoff_ratio: float | None = None,
) -> tuple[list[ExtractedLine], list[ExtractedLine], bool]:
    # Parsimonious core: use only the most robust heuristics.
    if candidate_name == "default":
        return _classify_liteparse_note_candidates(lines, page_height=page_height)
    if candidate_name == "marker_density":
        sep_y = _marker_density_split(lines, page_height=page_height)
        if sep_y is not None:
            body, notes = _split_lines_at_y(lines, sep_y)
            if body and notes:
                return body, notes, True
        return [], [], False
    if candidate_name == "liberal_notes":
        return _liberal_notes_split(lines, page_height=page_height)
    if candidate_name == "bottom_72":
        # Standard 1-inch (approx 72px) footer zone
        body, notes, _low_variance = _classify_lines(
            lines, page_height=page_height, note_cutoff_ratio=0.72
        )
        return body, notes, True
    
    # Generic fallback
    return _classify_lines(
        lines,
        page_height=page_height,
        note_cutoff_ratio=note_cutoff_ratio,
    )


_LITEPARSE_CANDIDATE_NAMES = (
    "default",
    "marker_density",
    "liberal_notes",
    "bottom_72",
    "sequence_solver",
)


def _load_liteparse_page_layouts(pdf_path: str) -> list[_LiteparsePageLayout] | None:
    try:
        from liteparse import LiteParse  # type: ignore
    except Exception:
        return None

    cli_path = (os.getenv("LITEPARSE_CLI_PATH", "") or "").strip() or None
    if cli_path is None:
        nvm_bin = (os.getenv("NVM_BIN", "") or "").strip()
        if nvm_bin:
            lit_candidate = Path(nvm_bin) / "lit"
            if lit_candidate.is_file():
                cli_path = str(lit_candidate)
        if cli_path is None and nvm_bin:
            npx_candidate = Path(nvm_bin) / "npx"
            if npx_candidate.is_file():
                cli_path = f"{npx_candidate} @llamaindex/liteparse"
        if cli_path is None:
            lit_path = shutil.which("lit")
            if lit_path:
                cli_path = lit_path
        if cli_path is None:
            npx_path = shutil.which("npx")
            if npx_path:
                cli_path = f"{npx_path} @llamaindex/liteparse"
    timeout_raw = (os.getenv("LITEPARSE_TIMEOUT_SECONDS", "") or "").strip()
    try:
        timeout_seconds = float(timeout_raw) if timeout_raw else 180.0
    except Exception:
        timeout_seconds = 180.0

    try:
        parser = LiteParse(cli_path=cli_path)
        result = parser.parse(
            pdf_path,
            ocr_enabled=False,
            timeout=max(30.0, timeout_seconds),
        )
    except Exception:
        return None

    layouts: list[_LiteparsePageLayout] = []

    for parsed_page in list(getattr(result, "pages", []) or []):
        page_no = int(getattr(parsed_page, "pageNum", 0) or 0)
        if page_no <= 0:
            continue
        width = float(getattr(parsed_page, "width", 0.0) or 0.0)
        height = float(getattr(parsed_page, "height", 0.0) or 0.0)
        raw_text = str(getattr(parsed_page, "text", "") or "")

        words: list[dict[str, float | str]] = []
        for item in list(getattr(parsed_page, "textItems", []) or []):
            text = " ".join(str(getattr(item, "text", "") or "").split())
            if not text:
                continue
            x = float(getattr(item, "x", 0.0) or 0.0)
            y = float(getattr(item, "y", 0.0) or 0.0)
            item_width = float(getattr(item, "width", 0.0) or 0.0)
            item_height = float(getattr(item, "height", 0.0) or 0.0)
            if item_height <= 0.0:
                item_height = float(getattr(item, "fontSize", 0.0) or 0.0)
            bottom = y + item_height if item_height > 0.0 else y
            words.append(
                {
                    "text": text,
                    "x0": x,
                    "x1": x + item_width,
                    "top": y,
                    "bottom": bottom,
                    "size": float(getattr(item, "fontSize", 0.0) or 0.0) or None,
                }
            )

        words = _merge_smallcaps_textitems(words)
        lines = _cluster_words_to_lines(words, page_number=page_no)
        for line in lines:
            line.source = "liteparse"
            line.text = _normalize_liteparse_text(line.text)
        if not lines:
            lines = _line_from_text(raw_text, page_number=page_no, source="liteparse")
        if not height:
            height = max((line.bottom for line in lines), default=0.0)
        if not width:
            width = max(
                (float(word.get("x1", 0.0) or 0.0) for word in words),
                default=0.0,
            )

        raw_items: list[dict] = []
        for item in list(getattr(parsed_page, "textItems", []) or []):
            t = " ".join(str(getattr(item, "text", "") or "").split())
            if not t:
                continue
            raw_items.append({
                "text": t,
                "x": float(getattr(item, "x", 0.0) or 0.0),
                "y": float(getattr(item, "y", 0.0) or 0.0),
                "width": float(getattr(item, "width", 0.0) or 0.0),
                "height": float(getattr(item, "height", 0.0) or 0.0),
                "fontSize": float(getattr(item, "fontSize", 0.0) or 0.0),
            })
        layouts.append(
            _LiteparsePageLayout(
                page_number=page_no,
                width=width,
                height=height,
                raw_text=raw_text,
                lines=tuple(lines),
                raw_items=tuple(raw_items),
            )
        )

    if _env_flag_enabled("OFFPRINT_LITEPARSE_SEPARATOR_PRIORS"):
        probe_pages = {
            layout.page_number
            for layout in layouts
            if _liteparse_page_has_lower_note_probe(layout)
        }
        separator_priors = (
            _find_pdfplumber_separator_priors(pdf_path, page_numbers=probe_pages)
            if probe_pages
            else {}
        )
        updated_layouts: list[_LiteparsePageLayout] = []
        for layout in layouts:
            sep_y = separator_priors.get(layout.page_number)
            if sep_y is not None and _separator_supported_by_liteparse(layout, sep_y):
                updated_layouts.append(replace(layout, separator_y=sep_y))
            else:
                updated_layouts.append(layout)
        layouts = updated_layouts

    return layouts


def _build_liteparse_candidate_document(
    pdf_path: str,
    layouts: list[_LiteparsePageLayout],
    *,
    candidate_name: str,
    note_cutoff_ratio: float | None = None,
) -> ExtractedDocument:
    text_fidelity_score = _text_fidelity_score_for_word_pages(
        [_raw_items_to_word_dicts(layout.raw_items) for layout in layouts]
    )
    separator_prior_pages = sum(1 for layout in layouts if layout.separator_y is not None)
    base_metadata: dict[str, Any] = {}
    if text_fidelity_score is not None:
        base_metadata["text_fidelity_score"] = text_fidelity_score
        base_metadata["text_fidelity_score_method"] = "page_y_band_column_proxy_v1"
    if separator_prior_pages:
        base_metadata["liteparse_separator_prior_pages"] = separator_prior_pages

    # The sequence_solver candidate is global: it reasons over all pages' raw
    # textItems at once, picks label positions that form the longest
    # monotonically-increasing sequence, and returns per-page y cutoffs.
    if candidate_name == "sequence_solver":
        from .sequence_solver import solve_document, build_note_records

        result = solve_document(layouts, pdf_path=pdf_path)
        precomputed_notes, precomputed_author_notes, precomputed_ordinality = build_note_records(
            layouts, result
        )
        pages: list[ExtractedPage] = []
        warnings: list[str] = []
        low_font_variance_detected = False
        reversed_word_order_detected = False
        page_rails: dict[str, int] = {}
        for layout in layouts:
            lines = _clone_lines(layout.lines)
            cutoff = result.page_cutoffs.get(layout.page_number)
            if cutoff is None:
                body_lines = list(lines)
                note_lines: list[ExtractedLine] = []
                page_rails["no_split"] = page_rails.get("no_split", 0) + 1
            else:
                body_lines = [ln for ln in lines if float(ln.top) < cutoff - 0.5]
                note_lines = [ln for ln in lines if float(ln.top) >= cutoff - 0.5]
                page_rails["solver_split"] = page_rails.get("solver_split", 0) + 1
            low_font_variance_detected = low_font_variance_detected or _low_font_variance(lines)
            reversed_word_order_detected = (
                reversed_word_order_detected or _reversed_word_order_suspected(note_lines)
            )
            pages.append(
                ExtractedPage(
                    page_number=layout.page_number,
                    width=layout.width,
                    height=layout.height,
                    body_lines=body_lines,
                    note_lines=note_lines,
                    raw_text=layout.raw_text,
                    source="liteparse",
                )
            )
        if not pages:
            warnings.append("liteparse_no_pages")
        if low_font_variance_detected:
            warnings.append("low_font_variance_detected")
        if reversed_word_order_detected:
            warnings.append("reversed_word_order_suspected")
        metadata: dict[str, Any] = {
            **base_metadata,
            "liteparse_candidate": candidate_name,
            "liteparse_page_rails": page_rails,
            "sequence_solver_selected_labels": list(result.selected_labels),
            "sequence_solver_candidate_count": result.candidate_count,
        }
        # Stash precomputed NoteRecord + OrdinalityReport so the selector can
        # bypass segmenter re-validation for the solver candidate. The solver's
        # label decisions are authoritative; running _is_likely_false_positive
        # etc. over solver output just re-rejects valid labels.
        if precomputed_notes is not None and precomputed_ordinality is not None:
            metadata["sequence_solver_precomputed"] = {
                "notes": precomputed_notes,
                "author_notes": precomputed_author_notes,
                "ordinality": precomputed_ordinality,
            }
        return ExtractedDocument(
            pdf_path=pdf_path,
            pages=pages,
            warnings=warnings,
            parser="liteparse",
            metadata=metadata,
        )

    pages: list[ExtractedPage] = []
    warnings: list[str] = []
    low_font_variance_detected = False
    reversed_word_order_detected = False
    page_rails: dict[str, int] = {}

    for layout in layouts:
        lines = _clone_lines(layout.lines)
        body_lines, note_lines, used_custom = _classify_liteparse_note_candidates(
            lines, page_height=layout.height
        ) if candidate_name == "default" else _classify_liteparse_candidate_lines(
            lines,
            page_height=layout.height,
            candidate_name=candidate_name,
            note_cutoff_ratio=note_cutoff_ratio,
        )
        rail_name = candidate_name if used_custom else "classify_lines"
        if not used_custom:
            body_lines, note_lines, page_low_font_variance = _classify_lines(
                lines,
                page_height=layout.height,
                note_cutoff_ratio=note_cutoff_ratio,
            )
        else:
            page_low_font_variance = _low_font_variance(lines)

        low_font_variance_detected = low_font_variance_detected or page_low_font_variance
        reversed_word_order_detected = (
            reversed_word_order_detected or _reversed_word_order_suspected(note_lines)
        )
        page_rails[rail_name] = page_rails.get(rail_name, 0) + 1
        pages.append(
            ExtractedPage(
                page_number=layout.page_number,
                width=layout.width,
                height=layout.height,
                body_lines=body_lines,
                note_lines=note_lines,
                raw_text=layout.raw_text,
                source="liteparse",
            )
        )

    if not pages:
        warnings.append("liteparse_no_pages")
    if low_font_variance_detected:
        warnings.append("low_font_variance_detected")
    if reversed_word_order_detected:
        warnings.append("reversed_word_order_suspected")
    return ExtractedDocument(
        pdf_path=pdf_path,
        pages=pages,
        warnings=warnings,
        parser="liteparse",
        metadata={
            **base_metadata,
            "liteparse_candidate": candidate_name,
            "liteparse_page_rails": page_rails,
        },
    )


# Single-character ASCII glyphs that appear standalone in cmap-mangled output
# (e.g. Alegreya font with a broken cmap remaps digit/marker glyphs to '!', '#',
# '$', '%', '*', '+'). These never legitimately appear as 1000s of standalone
# tokens in body text. '&' is excluded (legitimate ampersand usage).
_LITEPARSE_CMAP_MARKER_GLYPHS = frozenset("!#$%*+")
# Latin accented letters that signal a non-English document (Spanish, French,
# Portuguese, German). If many tokens contain these, the doc is likely fine —
# liteparse handles diacritics correctly, the language model is just confused.
_LITEPARSE_NON_ENGLISH_RE = re.compile(
    r"[áéíóúñÁÉÍÓÚÑüÜçÇßàâêîôûÀÂÊÎÔÛèÈùÙ]"
)
# C1 control chars + Unicode replacement char — always pathological.
_LITEPARSE_CTRL_REPL_RE = re.compile(r"[-�]")


def _detect_liteparse_font_pathology(
    layouts: list[_LiteparsePageLayout],
) -> str | None:
    """Detect liteparse layouts exhibiting font-encoding pathologies.

    Returns the warning slug ``"liteparse_font_pathology"`` if a pathology is
    detected, else ``None``. Designed to be conservative — it requires multiple
    corroborating signals and bails out for non-English docs (Spanish/French/
    German) where the model's confusion is linguistic, not font-related.

    Pathologies caught:
      * No-text: ≥3 pages and 0 raw textItems extracted across the doc
        (image-scan or fully-broken cmap with no extractable text).
      * Cmap-mangled: ≥0.5% of raw textItems are single-char ASCII marker
        glyphs ('!', '#', '$', '%', '*', '+') which appear when fonts like
        Alegreya have broken cmap tables.
      * Control/replacement chars: ≥1% of textItems contain U+0080-009F or
        U+FFFD (always pathological, never legitimate text).

    Safeguard: if ≥1% of tokens contain Latin accented letters, the doc is
    treated as non-English and never flagged.
    """
    if not layouts:
        return None
    n_pages = len(layouts)
    if n_pages < 3:
        return None
    items_total = 0
    accented_count = 0
    cmap_marker_count = 0
    ctrl_repl_count = 0
    for layout in layouts:
        for item in layout.raw_items or ():
            text = str(item.get("text") or "")
            if not text:
                continue
            items_total += 1
            if _LITEPARSE_NON_ENGLISH_RE.search(text):
                accented_count += 1
            if len(text) == 1 and text in _LITEPARSE_CMAP_MARKER_GLYPHS:
                cmap_marker_count += 1
            if _LITEPARSE_CTRL_REPL_RE.search(text):
                ctrl_repl_count += 1
    # Pathology A: no extractable text at all across a multi-page doc.
    if items_total == 0:
        return "liteparse_font_pathology"
    # Safeguard: non-English doc — don't fire.
    if (accented_count / items_total) >= 0.01:
        return None
    # Pathology B: cmap-mangled marker glyphs above 2% of all tokens. The
    # threshold is tuned empirically on holdout 1K — single-char marker glyphs
    # appear at <1% in healthy English law-review docs but at 3-20% in cmap-
    # mangled outputs (e.g. mjlr.org Alegreya font footnote markers).
    if (cmap_marker_count / items_total) >= 0.02:
        return "liteparse_font_pathology"
    # Pathology C: control/replacement chars above 1% of all tokens.
    if (ctrl_repl_count / items_total) >= 0.01:
        return "liteparse_font_pathology"
    return None


def extract_liteparse_candidate_documents(
    pdf_path: str, *, note_cutoff_ratio: float | None = None
) -> list[ExtractedDocument]:
    layouts = _load_liteparse_page_layouts(pdf_path)
    if layouts is None:
        return []
    pathology_warning = _detect_liteparse_font_pathology(layouts)
    candidates = [
        _build_liteparse_candidate_document(
            pdf_path,
            layouts,
            candidate_name=name,
            note_cutoff_ratio=note_cutoff_ratio,
        )
        for name in _LITEPARSE_CANDIDATE_NAMES
    ]
    if pathology_warning is not None:
        # Annotate every candidate with the warning so downstream selectors
        # and ocr_fallback_recommended() can route the doc accordingly. The
        # extraction itself is preserved — this is metadata only.
        for doc in candidates:
            if pathology_warning not in doc.warnings:
                doc.warnings.append(pathology_warning)
    return candidates


def _extract_with_liteparse(
    pdf_path: str, *, note_cutoff_ratio: float | None = None
) -> ExtractedDocument | None:
    candidates = extract_liteparse_candidate_documents(
        pdf_path, note_cutoff_ratio=note_cutoff_ratio
    )
    if not candidates:
        return None
    return candidates[0]


_MERGED_NOTE_SPLIT_RE = re.compile(r"(?<=[.;:)\]\"\u201d\u2019'])\s+(?=\d{1,4}\s+[A-Z])")


def _split_merged_note_lines(text: str) -> list[str]:
    """Split a docling note block that contains multiple embedded note labels.

    Example: "25 Id. 26 See Air Line Pilots..." → ["25 Id.", "26 See Air Line Pilots..."]

    Docling occasionally merges consecutive short notes into a single text block.
    This splitter detects sentence-ending punctuation followed by a new numeric
    label + uppercase start and splits them into separate note strings.
    """
    if not text or not text.strip():
        return []
    parts = _MERGED_NOTE_SPLIT_RE.split(text)
    return [p.strip() for p in parts if p.strip()]


def _parse_docling_dict(pdf_path: str, doc_dict: dict) -> ExtractedDocument:
    """
    Parse Docling's structured JSON export, organizing text by page with
    explicit footnote labels preserved.
    """
    texts = doc_dict.get("texts", [])
    warnings: list[str] = []

    # Group items by page
    pages_data: dict[int, dict] = {}

    for item in texts:
        label = item.get("label", "text")
        text = _normalize_docling_text((item.get("text") or item.get("orig") or "").strip())
        if not text:
            continue

        # Extract page number from provenance
        prov = item.get("prov", [])
        page_no = prov[0].get("page_no", 1) if prov else 1

        if page_no not in pages_data:
            pages_data[page_no] = {"body_lines": [], "note_lines": [], "raw_parts": []}

        page_data = pages_data[page_no]
        page_data["raw_parts"].append(text)

        # Create ExtractedLine
        extracted = ExtractedLine(text=text, page_number=page_no, source="docling_json")

        # Docling labels: "footnote", "text", "section_header", "page_header", etc.
        if label == "footnote":
            # Split merged note blocks (e.g. "25 Id. 26 See...")
            split_texts = _split_merged_note_lines(text)
            if len(split_texts) > 1:
                for st in split_texts:
                    split_line = ExtractedLine(text=st, page_number=page_no, source="docling_json")
                    page_data["note_lines"].append(split_line)
                    page_data["body_lines"].append(split_line)
            else:
                page_data["note_lines"].append(extracted)
                page_data["body_lines"].append(extracted)  # Also in body for context
        elif label not in ("page_header", "page_footer", "furniture"):
            page_data["body_lines"].append(extracted)

    # Convert to ExtractedPage objects
    pages: list[ExtractedPage] = []
    for page_no in sorted(pages_data.keys()):
        data = pages_data[page_no]
        pages.append(
            ExtractedPage(
                page_number=page_no,
                body_lines=data["body_lines"],
                note_lines=data["note_lines"],
                raw_text="\n".join(data["raw_parts"]),
                source="docling_json",
            )
        )

    if not pages:
        warnings.append("docling_json_no_pages")

    return ExtractedDocument(
        pdf_path=pdf_path,
        pages=pages,
        warnings=warnings,
        parser="docling",
    )


def _parse_docling_markdown(pdf_path: str, markdown: str) -> ExtractedDocument:
    """
    Fallback parser for Docling markdown output (legacy behavior).
    """
    body_lines: list[ExtractedLine] = []
    note_lines: list[ExtractedLine] = []
    for raw in markdown.splitlines():
        line = _normalize_docling_text(" ".join(raw.split()).strip())
        if not line:
            continue
        extracted = ExtractedLine(text=line, page_number=1, source="text")
        body_lines.append(extracted)
        if _NOTE_LIKE_RE.match(line):
            note_lines.append(extracted)

    page = ExtractedPage(
        page_number=1,
        body_lines=body_lines,
        note_lines=note_lines,
        raw_text=markdown,
        source="text",
    )
    return ExtractedDocument(pdf_path=pdf_path, pages=[page], warnings=[], parser="docling")


def _annotate_parser(document: ExtractedDocument, parser: str) -> ExtractedDocument:
    if parser and document.parser != parser:
        document.parser = parser
    return document


def extract_document_text(
    pdf_path: str,
    parser_mode: str = "balanced",
    *,
    note_cutoff_ratio: float | None = None,
) -> ExtractedDocument:
    mode = (parser_mode or "balanced").strip().lower()
    if mode not in PARSER_MODES:
        mode = "balanced"
    if note_cutoff_ratio is not None and not (0.40 <= note_cutoff_ratio <= 0.90):
        note_cutoff_ratio = None

    if mode == "footnote_optimized":
        # liteparse exclusively. If liteparse can't read the PDF, return an
        # empty ExtractedDocument so downstream code can route to OCR rather
        # than producing low-quality fallback output.
        if note_cutoff_ratio is None:
            document = _extract_with_liteparse(pdf_path)
        else:
            document = _extract_with_liteparse(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
        if document is not None:
            return _annotate_parser(document, "liteparse")
        return ExtractedDocument(
            pdf_path=pdf_path,
            pages=[],
            warnings=["liteparse_returned_none"],
        )

    if mode == "pypdf_only":
        document = _extract_with_pypdf(pdf_path)
        if document is not None:
            return _annotate_parser(document, "pypdf")
        return ExtractedDocument(
            pdf_path=pdf_path,
            pages=[],
            warnings=["pypdf parser unavailable or failed in pypdf_only mode"],
            parser="pypdf",
        )

    if mode == "docling_only":
        document = _extract_with_docling(pdf_path)
        if document is not None:
            return _annotate_parser(document, "docling")
        if note_cutoff_ratio is None:
            fallback = _extract_with_pdfplumber(pdf_path)
        else:
            fallback = _extract_with_pdfplumber(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
        if fallback is not None:
            fallback.warnings.append("docling unavailable; used pdfplumber fallback")
            return _annotate_parser(fallback, "pdfplumber")
        fallback = _extract_with_pypdf(pdf_path)
        if fallback is not None:
            fallback.warnings.append("docling unavailable; used pypdf fallback")
            return _annotate_parser(fallback, "pypdf")
        return ExtractedDocument(
            pdf_path=pdf_path,
            pages=[],
            warnings=["No PDF parser available (install docling/pdfplumber/pypdf)"],
            parser="docling",
        )

    if mode == "liteparse_only":
        if note_cutoff_ratio is None:
            document = _extract_with_liteparse(pdf_path)
        else:
            document = _extract_with_liteparse(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
        if document is not None:
            return _annotate_parser(document, "liteparse")
        if note_cutoff_ratio is None:
            fallback = _extract_with_pdfplumber(pdf_path)
        else:
            fallback = _extract_with_pdfplumber(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
        if fallback is not None:
            fallback.warnings.append("liteparse unavailable; used pdfplumber fallback")
            return _annotate_parser(fallback, "pdfplumber")
        fallback = _extract_with_pypdf(pdf_path)
        if fallback is not None:
            fallback.warnings.append("liteparse unavailable; used pypdf fallback")
            return _annotate_parser(fallback, "pypdf")
        return ExtractedDocument(
            pdf_path=pdf_path,
            pages=[],
            warnings=["No PDF parser available (install liteparse/pdfplumber/pypdf)"],
            parser="liteparse",
        )

    if mode == "pdfplumber_only":
        if note_cutoff_ratio is None:
            document = _extract_with_pdfplumber(pdf_path)
        else:
            document = _extract_with_pdfplumber(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
        if document is not None:
            return _annotate_parser(document, "pdfplumber")
        fallback = _extract_with_pypdf(pdf_path)
        if fallback is not None:
            fallback.warnings.append("pdfplumber unavailable; used pypdf fallback")
            return _annotate_parser(fallback, "pypdf")
        return ExtractedDocument(
            pdf_path=pdf_path,
            pages=[],
            warnings=["No PDF parser available (install pdfplumber or pypdf)"],
        )

    # balanced: fast parser first, quality fallback is handled by pipeline.
    document = _extract_with_pypdf(pdf_path)
    if document is not None:
        return _annotate_parser(document, "pypdf")
    if note_cutoff_ratio is None:
        fallback = _extract_with_pdfplumber(pdf_path)
    else:
        fallback = _extract_with_pdfplumber(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
    if fallback is not None:
        fallback.warnings.append("pypdf unavailable; used pdfplumber fallback")
        return _annotate_parser(fallback, "pdfplumber")
    return ExtractedDocument(
        pdf_path=pdf_path,
        pages=[],
        warnings=["No PDF parser available (install pdfplumber or pypdf)"],
    )


def ocr_fallback_recommended(document: ExtractedDocument, note_count: int) -> bool:
    if not document.pages:
        return True
    if document.total_text_chars < 600:
        return True
    if note_count == 0:
        return True
    if "reversed_word_order_suspected" in document.warnings:
        return True
    if "low_font_variance_detected" in document.warnings:
        return True
    if "liteparse_font_pathology" in document.warnings:
        return True
    return False
