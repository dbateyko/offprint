from __future__ import annotations

import logging
import os
import re
import shutil
import statistics
import tempfile
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Sequence

PARSER_MODES = {
    "balanced",
    "pdfplumber_only",
    "pypdf_only",
    "docling_only",
    "opendataloader_only",
    "liteparse_only",
    "footnote_optimized",
}
_NOTE_LIKE_RE = re.compile(
    r"^\s*(?:\[\^\d{1,4}\]:|\d{1,4}[\]\)\.,:;-]?\s+|[ivxlcdm]{1,7}[\]\)\.,:;-]?\s+|[*†‡§¶]\s+)",
    re.IGNORECASE,
)
_LEGAL_FOOTNOTE_SIGNAL_RE = re.compile(
    r"\b(?:v\.|u\.s\.|f\.\d|s\. ct\.|id\.|supra|infra|§)\b",
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
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ExtractedDocument":
        return cls(
            pdf_path=d.get("pdf_path", ""),
            pages=[ExtractedPage.from_dict(p) for p in d.get("pages", [])],
            warnings=d.get("warnings", []),
            parser=d.get("parser", ""),
        )


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
    sorted_words = sorted(
        words, key=lambda word: (float(word.get("top", 0.0)), float(word.get("x0", 0.0)))
    )
    bins: list[list[dict]] = []
    tolerance = 2.5
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

    lines: list[ExtractedLine] = []
    for grouped_words in bins:
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

    return ExtractedDocument(pdf_path=pdf_path, pages=pages, warnings=warnings)


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


def _walk_opendataloader_elements(node: Any, out: list[dict[str, Any]]) -> None:
    if isinstance(node, dict):
        content = " ".join(str(node.get("content", "") or "").split())
        page_no = node.get("page number")
        if content and isinstance(page_no, int):
            out.append(node)
        for value in node.values():
            if isinstance(value, (dict, list)):
                _walk_opendataloader_elements(value, out)
        return
    if isinstance(node, list):
        for item in node:
            _walk_opendataloader_elements(item, out)


def _classify_opendataloader_note_candidates(
    line_items: list[tuple[ExtractedLine, str]], page_height: float
) -> tuple[list[ExtractedLine], list[ExtractedLine], bool]:
    filtered = [
        (line, item_type)
        for line, item_type in line_items
        if item_type not in _OPENDATALOADER_EXCLUDED_TYPES
    ]
    if not filtered:
        return [], [], False

    lines = [line for line, _item_type in filtered]
    sizes = [float(line.font_size) for line in lines if line.font_size is not None]
    median_size = statistics.median(sizes) if sizes else None

    scored: list[tuple[ExtractedLine, float]] = []
    for line, item_type in filtered:
        text = (line.text or "").strip()
        score = 0.0
        note_like = bool(_NOTE_LIKE_RE.match(text))
        legal_cue = bool(_LEGAL_FOOTNOTE_SIGNAL_RE.search(text))
        rel_top = (line.top / page_height) if page_height > 0.0 else 0.0

        if note_like:
            score += 2.2
        if legal_cue:
            score += 1.2
        if len(text) >= 60:
            score += 0.2

        if rel_top >= 0.82:
            score += 2.0
        elif rel_top >= 0.72:
            score += 1.2
        elif rel_top >= 0.62 and note_like:
            score += 0.6

        if (
            median_size is not None
            and line.font_size is not None
            and line.font_size < median_size * 0.92
        ):
            score += 1.0

        if item_type in {"list item", "caption"}:
            score += 0.3
        if item_type in {"heading", "table", "table row", "table cell", "image"}:
            score -= 1.0
        if len(text) < 10 and not note_like:
            score -= 1.0

        # Guard against first-page frontmatter and running heads.
        if line.page_number <= 3:
            if _FRONTMATTER_TEXT_RE.search(text) and not legal_cue:
                score -= 2.0
            if rel_top < 0.55:
                score -= 1.8
            if item_type == "heading":
                score -= 1.2
            if (
                note_like
                and re.match(r"^\s*(\d{2,4})\s+[A-Z]", text)
                and not legal_cue
                and rel_top < 0.45
            ):
                score -= 2.5

        scored.append((line, score))

    notes: list[ExtractedLine] = []
    for line, score in scored:
        text = (line.text or "").strip()
        note_like = bool(_NOTE_LIKE_RE.match(text))
        legal_cue = bool(_LEGAL_FOOTNOTE_SIGNAL_RE.search(text))
        rel_top = (line.top / page_height) if page_height > 0.0 else 0.0
        is_note = score >= 3.0 or (note_like and score >= 2.4)
        if line.page_number <= 3 and is_note:
            # First pages are noisy; require stronger evidence.
            if legal_cue:
                notes.append(line)
                continue
            if note_like and rel_top >= 0.82 and len(text) >= 25:
                notes.append(line)
                continue
            continue
        if is_note:
            notes.append(line)

    if notes and len(notes) <= max(3, int(len(lines) * 0.65)):
        note_ids = {id(line) for line in notes}
        body = [line for line in lines if id(line) not in note_ids]
        if body:
            return body, notes, True

    return [], [], False


def _classify_liteparse_note_candidates(
    lines: list[ExtractedLine], *, page_height: float
) -> tuple[list[ExtractedLine], list[ExtractedLine], bool]:
    """
    LiteParse-specific classifier for separating body and footnote lines.

    liteparse provides dense spatial text lines with per-word font sizes captured
    by _cluster_words_to_lines.  Unlike pdfplumber we have no access to physical
    separator lines/rects, so classification relies on:
      1. Running-head / frontmatter pre-filter for pages 1-3.
      2. Font size ratio: lines whose median font size is < 90 % of the page
         median are likely footnotes regardless of vertical position.
      3. Position + citation-signal scoring: note-like markers and legal cues in
         the bottom 40 % of the page.
      4. Pathological guard: if no body lines remain, fall back to _classify_lines.
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

    # --- Font size median for ratio check ---
    sizes = [line.font_size for line in filtered if line.font_size is not None]
    median_size = statistics.median(sizes) if sizes else None
    low_variance = _low_font_variance(filtered)

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


def _extract_with_liteparse(
    pdf_path: str, *, note_cutoff_ratio: float | None = None
) -> ExtractedDocument | None:
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

    pages: list[ExtractedPage] = []
    warnings: list[str] = []
    low_font_variance_detected = False
    reversed_word_order_detected = False

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

        body_lines, note_lines, used_custom = _classify_liteparse_note_candidates(
            lines, page_height=height
        )
        if used_custom:
            page_low_font_variance = _low_font_variance(lines)
        else:
            body_lines, note_lines, page_low_font_variance = _classify_lines(
                lines,
                page_height=height,
                note_cutoff_ratio=note_cutoff_ratio,
            )

        low_font_variance_detected = low_font_variance_detected or page_low_font_variance
        reversed_word_order_detected = (
            reversed_word_order_detected or _reversed_word_order_suspected(note_lines)
        )
        pages.append(
            ExtractedPage(
                page_number=page_no,
                width=width,
                height=height,
                body_lines=body_lines,
                note_lines=note_lines,
                raw_text=raw_text,
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
    )


def _parse_opendataloader_json(
    pdf_path: str,
    payload: dict[str, Any],
    *,
    note_cutoff_ratio: float | None = None,
) -> ExtractedDocument:
    pages_data: dict[int, list[dict[str, Any]]] = {}
    elements: list[dict[str, Any]] = []
    _walk_opendataloader_elements(payload, elements)
    for item in elements:
        page_no = int(item.get("page number", 1))
        pages_data.setdefault(page_no, []).append(item)

    pages: list[ExtractedPage] = []
    warnings: list[str] = []
    low_font_variance_detected = False
    reversed_word_order_detected = False

    for page_no in sorted(pages_data):
        items = pages_data[page_no]
        lines: list[ExtractedLine] = []
        line_items: list[tuple[ExtractedLine, str]] = []
        raw_parts: list[str] = []
        explicit_note_lines: list[ExtractedLine] = []
        max_x = 0.0
        max_y = 0.0

        sortable: list[tuple[float, float, int, dict[str, Any]]] = []
        for _idx, item in enumerate(items):
            bbox = item.get("bounding box") or []
            if isinstance(bbox, list) and len(bbox) == 4:
                x0 = _safe_float(bbox[0]) or 0.0
                y0 = _safe_float(bbox[1]) or 0.0
                x1 = _safe_float(bbox[2]) or 0.0
                y1 = _safe_float(bbox[3]) or 0.0
                sortable.append((-y1, x0, _idx, item))
                max_x = max(max_x, x0, x1)
                max_y = max(max_y, y0, y1)
            else:
                sortable.append((float("inf"), float("inf"), _idx, item))

        for _k1, _k2, _k3, item in sorted(sortable):
            item_type = str(item.get("type", "") or "").strip().lower()
            if item_type in _OPENDATALOADER_EXCLUDED_TYPES:
                continue
            text = " ".join(str(item.get("content", "") or "").split())
            if not text:
                continue
            raw_parts.append(text)
            bbox = item.get("bounding box") or []
            top_from_top = 0.0
            bottom_from_top = 0.0
            if isinstance(bbox, list) and len(bbox) == 4 and max_y > 0.0:
                y0 = _safe_float(bbox[1]) or 0.0
                y1 = _safe_float(bbox[3]) or 0.0
                top_from_top = max(0.0, max_y - y1)
                bottom_from_top = max(top_from_top, max_y - y0)

            line = ExtractedLine(
                text=text,
                page_number=page_no,
                top=top_from_top,
                bottom=bottom_from_top,
                font_size=_safe_float(item.get("font size")),
                source="opendataloader_json",
            )
            # Short "Id./Ibid." lines on opening pages are high-noise and are
            # treated as frontmatter leakage by benchmark gating.
            if line.page_number <= 3 and _SHORT_ID_NOTE_RE.match(text):
                continue
            if line.page_number <= 3 and _RUNNING_HEAD_JOURNAL_CITE_RE.match(text):
                continue
            lines.append(line)
            line_items.append((line, item_type))
            if item_type in {"footnote", "endnote"}:
                if line.page_number <= 3 and _RUNNING_HEAD_JOURNAL_CITE_RE.match(text):
                    continue
                explicit_note_lines.append(line)

        if explicit_note_lines:
            body_lines = lines
            note_lines = explicit_note_lines
            page_low_font_variance = _low_font_variance(lines)
        else:
            body_lines, note_lines, used_custom = _classify_opendataloader_note_candidates(
                line_items, page_height=max_y
            )
            if used_custom:
                page_low_font_variance = _low_font_variance(lines)
            else:
                body_lines, note_lines, page_low_font_variance = _classify_lines(
                    lines, page_height=max_y, note_cutoff_ratio=note_cutoff_ratio
                )

        low_font_variance_detected = low_font_variance_detected or page_low_font_variance
        reversed_word_order_detected = (
            reversed_word_order_detected or _reversed_word_order_suspected(note_lines)
        )
        pages.append(
            ExtractedPage(
                page_number=page_no,
                width=max_x,
                height=max_y,
                body_lines=body_lines,
                note_lines=note_lines,
                raw_text="\n".join(raw_parts),
                source="opendataloader_json",
            )
        )

    if not pages:
        warnings.append("opendataloader_json_no_pages")
    if low_font_variance_detected:
        warnings.append("low_font_variance_detected")
    if reversed_word_order_detected:
        warnings.append("reversed_word_order_suspected")

    return ExtractedDocument(
        pdf_path=pdf_path,
        pages=pages,
        warnings=warnings,
        parser="opendataloader",
    )


def _extract_with_opendataloader(
    pdf_path: str, *, note_cutoff_ratio: float | None = None
) -> ExtractedDocument | None:
    try:
        import opendataloader_pdf  # type: ignore
    except Exception:
        return None

    try:
        with tempfile.TemporaryDirectory(prefix="opendataloader_pdf_") as output_dir:
            # Disable C2 JIT compiler to prevent SIGSEGV in PhaseIdealLoop on JDK 21.
            # JAVA_TOOL_OPTIONS is read by the JVM before its own command-line args.
            _prev_jtopt = os.environ.get("JAVA_TOOL_OPTIONS")
            os.environ["JAVA_TOOL_OPTIONS"] = "-XX:TieredStopAtLevel=1"
            try:
                opendataloader_pdf.convert(
                    input_path=pdf_path,
                    output_dir=output_dir,
                    format="json",
                    quiet=True,
                )
            finally:
                if _prev_jtopt is None:
                    os.environ.pop("JAVA_TOOL_OPTIONS", None)
                else:
                    os.environ["JAVA_TOOL_OPTIONS"] = _prev_jtopt
            stem = Path(pdf_path).stem
            default_json = Path(output_dir) / f"{stem}.json"
            if default_json.exists():
                payload_path = default_json
            else:
                candidates = sorted(Path(output_dir).glob("*.json"))
                if not candidates:
                    return None
                payload_path = candidates[0]
            with open(payload_path, "r", encoding="utf-8") as handle:
                import json

                payload = json.load(handle)
        if not isinstance(payload, dict):
            return None
        return _parse_opendataloader_json(pdf_path, payload, note_cutoff_ratio=note_cutoff_ratio)
    except Exception:
        logging.getLogger(__name__).debug(
            "opendataloader failed for %s", pdf_path, exc_info=True
        )
        return None


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
        # liteparse first for robust structured parsing, then fallback to
        # existing deterministic parsers.
        if note_cutoff_ratio is None:
            document = _extract_with_liteparse(pdf_path)
        else:
            document = _extract_with_liteparse(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
        if document is not None:
            return _annotate_parser(document, "liteparse")
        if note_cutoff_ratio is None:
            document = _extract_with_pdfplumber(pdf_path)
        else:
            document = _extract_with_pdfplumber(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
        if document is not None:
            document.warnings.append("liteparse unavailable; used pdfplumber fallback")
            return _annotate_parser(document, "pdfplumber")
        fallback = _extract_with_pypdf(pdf_path)
        if fallback is not None:
            fallback.warnings.append("pdfplumber unavailable; used pypdf fallback")
            return _annotate_parser(fallback, "pypdf")
        return ExtractedDocument(
            pdf_path=pdf_path,
            pages=[],
            warnings=["No PDF parser available (install liteparse, pdfplumber, or pypdf)"],
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

    if mode == "opendataloader_only":
        if note_cutoff_ratio is None:
            document = _extract_with_opendataloader(pdf_path)
        else:
            document = _extract_with_opendataloader(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
        if document is not None:
            return _annotate_parser(document, "opendataloader")
        if note_cutoff_ratio is None:
            fallback = _extract_with_pdfplumber(pdf_path)
        else:
            fallback = _extract_with_pdfplumber(pdf_path, note_cutoff_ratio=note_cutoff_ratio)
        if fallback is not None:
            fallback.warnings.append("opendataloader unavailable; used pdfplumber fallback")
            return _annotate_parser(fallback, "pdfplumber")
        fallback = _extract_with_pypdf(pdf_path)
        if fallback is not None:
            fallback.warnings.append("opendataloader unavailable; used pypdf fallback")
            return _annotate_parser(fallback, "pypdf")
        return ExtractedDocument(
            pdf_path=pdf_path,
            pages=[],
            warnings=["No PDF parser available (install opendataloader-pdf/pdfplumber/pypdf)"],
            parser="opendataloader",
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
    if "low_font_variance_detected" in document.warnings and note_count < 5:
        return True
    return False
