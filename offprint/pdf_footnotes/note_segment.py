from __future__ import annotations

import re
import statistics
from dataclasses import dataclass

from eyecite import get_citations
from eyecite.models import FullCaseCitation, FullLawCitation

from .schema import AUTHOR_MARKERS, AuthorNote, NoteChunk, NoteRecord, OrdinalityReport
from .text_extract import ExtractedDocument, ExtractedLine, ExtractedPage

NOTE_START_RE = re.compile(
    r"^\s*(?P<label>(?:\d{1,4}|[ivxlcdm]{1,7}|[*†‡§¶]))[\]\)\.,:;-]?\s+(?P<text>.+)$",
    re.IGNORECASE,
)
ENDNOTE_HEADING_RE = re.compile(r"^\s*(notes|endnotes|footnotes)\s*$", re.IGNORECASE)

# Roman numerals that are too likely to be section headings (case-insensitive)
SECTION_HEADING_ROMANS = frozenset({"i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x"})

# Minimum text length after label to be considered a real footnote
# (unless it matches a known short citation pattern)
MIN_FOOTNOTE_TEXT_LENGTH = 15

# Short citation patterns that are valid even if under MIN_FOOTNOTE_TEXT_LENGTH
# These are common legal short forms like "Id.", "Id. at 123", "Ibid.", etc.
SHORT_CITATION_RE = re.compile(
    r"^\s*(?:Id\.?|Ibid\.?|Idem\.?)(?:\s+at\s+\*?\d+(?:[\-–—]\d+)?)?\.?\s*$",
    re.IGNORECASE,
)

# Pattern for text that's likely just a continuation/citation fragment
CONTINUATION_FRAGMENT_RE = re.compile(
    r"^\s*"
    r"(?:"
    r"\(?[\d,\s\-–—]+\)?\.?\s*$"  # Just numbers/parens like "(1989)." or "490-92"
    r"|"
    r"at\s+\d"  # Starts with "at 123"
    r"|"
    r"\d+[\-–—]\d+\s*\(?(?:19|20)\d{2}\)?\.?\s*$"  # Page range with year like "369-70 (2010)."
    r"|"
    r"\d+,\s*\d+\s*\([^)]+\)"  # Multiple page refs like "1224, 1234 (D. Kan. 2011)"
    r"|"
    r"\(\d+(?:st|nd|rd|th)\s+Cir\."  # Circuit court ref like "(3d Cir."
    r"|"
    # Reporter abbreviation at start — "U.S. at 493", "F.2d 885", "S. Ct. at 1234"
    r"(?:U\.S\.?|F\.\s*(?:2d|3d|Supp\.?\s*(?:2d|3d)?)|S\.\s*Ct\.?)\s+(?:at\s+)?\d" r")",
    re.IGNORECASE,
)

# Pattern for text that starts with a number followed by court reference (common citation fragment)
# e.g., "303 (3d Cir. 1984)" or "747 (1986)"
# Note: Legal citations use "3d" not "3rd" for Third Circuit
COURT_CITATION_FRAGMENT_RE = re.compile(
    r"^\s*\d+\s*\(\s*(?:\d+(?:st|nd|d|rd|th)\s+Cir\.|(?:19|20)\d{2})",
    re.IGNORECASE,
)

# Pattern for text that looks like a real footnote start (has a citation verb/structure)
REAL_FOOTNOTE_START_RE = re.compile(
    r"(?:See|Id\.|Cf\.|But see|Compare|E\.g\.,|Accord|Note|Supra|Infra|"
    r"[A-Z][a-z]+\s+v\.\s+)",  # Case name like "Smith v."
    re.IGNORECASE,
)
# ALL CAPS title — must NOT be case-insensitive (that defeats the purpose)
_ALL_CAPS_TITLE_RE = re.compile(r"[A-Z][A-Z\s]{2,}[,.]")
_TOC_DOT_LEADER_RE = re.compile(r"\.{3,}")
_TOC_TRAILING_PAGE_RE = re.compile(r"\b\d{1,3}\s*$")
_TABLE_OF_CONTENTS_RE = re.compile(r"\btable of contents\b", re.IGNORECASE)
_FOOTNOTE_SEPARATOR_RE = re.compile(r"^[\-\u2010\u2011\u2012\u2013\u2014\u2015_]{10,}$")
_EMBEDDED_NOTE_MARKER_RE = re.compile(r"(?<!\w)(\d{1,4})[\]\)\.,:;-]?\s+(?=[A-Z])")
_STATUTE_START_RE = re.compile(r"^\s*\d+\s+(?:U\.S\.C\.|C\.F\.R\.)", re.IGNORECASE)


def _find_body_separator_cutoff(
    page: ExtractedPage, font_drop_ratio: float = 0.90
) -> float | None:
    """Infer where body text likely transitions into footnote-like lines."""
    candidates: list[float] = []

    font_sizes = [
        float(line.font_size)
        for line in page.body_lines
        if line.font_size is not None and float(line.font_size) > 0.0
    ]
    if font_sizes:
        median_size = float(statistics.median(font_sizes))
        small_line_tops = [
            float(line.top)
            for line in page.body_lines
            if line.font_size is not None
            and float(line.font_size) > 0.0
            and float(line.font_size) < (median_size * font_drop_ratio)
            and float(line.top) > 0.0
        ]
        if len(small_line_tops) >= 2:
            candidates.append(min(small_line_tops))
    else:
        page_height = float(page.height or 0.0)
        if page_height > 0.0:
            for line in page.body_lines:
                text = _clean_line_text(line.text)
                marker = _validated_marker_match(
                    text, strict_label_filter=True, max_label=max(120, len(page.body_lines) * 6)
                )
                if marker is None:
                    continue
                top = float(line.top or 0.0)
                if top <= 0.0:
                    continue
                if (top / page_height) > 0.65:
                    candidates.append(top)

    if not candidates:
        return None
    return min(candidates)


def _rescue_from_body_lines(
    doc: ExtractedDocument, found_notes: list[NoteRecord]
) -> list[NoteRecord]:
    """Recover missed numbered notes that were emitted into body lines."""

    endnote_pages = _collect_endnote_start_pages(doc)
    min_endnote_page = min(endnote_pages) if endnote_pages else float("inf")
    existing_labels = {int(n.label) for n in found_notes if n.label.isdigit()}
    max_label = max(120, doc.page_count * 6)

    accept_labels: set[int] = set()
    if len(found_notes) >= 3:
        numeric_labels = sorted({int(n.label) for n in found_notes if n.label.isdigit()})
        if numeric_labels:
            gaps = set(range(min(numeric_labels), max(numeric_labels) + 1)) - set(numeric_labels)
            trailing = {max(numeric_labels) + i for i in range(1, 6)}
            accept_labels = gaps | trailing

    def _looks_like_heading(text: str) -> bool:
        words = text.split()
        return sum(1 for w in words[:8] if w.isupper() and len(w) > 2) >= 3

    rescued: list[NoteRecord] = []
    for page in doc.pages:
        page_num = page.page_number
        if page_num >= min_endnote_page:
            continue

        separator = _find_body_separator_cutoff(page)
        lines = page.body_lines
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            text = _clean_line_text(line.text)
            marker = _validated_marker_match(text, strict_label_filter=True, max_label=max_label)
            if marker is None:
                idx += 1
                continue
            label = marker.group("label")
            if not label.isdigit():
                idx += 1
                continue
            numeric = int(label)
            if numeric in existing_labels:
                idx += 1
                continue
            rest = marker.group("text").strip()
            if _looks_like_heading(rest):
                idx += 1
                continue

            rescuable = False
            if separator is not None and float(line.top or 0.0) >= separator:
                rescuable = True
            elif numeric in accept_labels:
                rescuable = True
            if not rescuable:
                idx += 1
                continue

            text_parts = [rest] if rest else []
            segments = [NoteChunk(page=page_num, text=rest, source=getattr(line, "source", "text"))]
            scan_idx = idx + 1
            while scan_idx < len(lines):
                next_line = lines[scan_idx]
                next_text = _clean_line_text(next_line.text)
                if not next_text:
                    scan_idx += 1
                    continue
                if _validated_marker_match(
                    next_text, strict_label_filter=True, max_label=max_label
                ):
                    break
                if _likely_continuation(next_text):
                    text_parts.append(next_text)
                    segments.append(
                        NoteChunk(
                            page=page_num,
                            text=next_text,
                            source=getattr(next_line, "source", "text"),
                        )
                    )
                scan_idx += 1

            merged_text = " ".join(part for part in text_parts if part).strip()
            if merged_text:
                rescued.append(
                    NoteRecord(
                        ordinal=0,
                        label=str(numeric),
                        note_type="footnote",
                        text=merged_text,
                        page_start=page_num,
                        page_end=page_num,
                        segments=segments,
                        quality_flags=["body_rescue"],
                    )
                )
                existing_labels.add(numeric)
            idx = scan_idx

    return rescued


DEFAULT_GAP_TOLERANCE = 2
DEFAULT_GAP_RATIO_THRESHOLD = 0.35


def _text_starts_with_citation_fragment(text: str) -> bool:
    """
    Use eyecite to detect if text starts with a citation fragment.

    A citation fragment is text like "303 (3d Cir. 1984)" that looks like
    part of a legal citation but isn't a complete citation. These occur
    when Docling incorrectly splits a citation across lines.

    Returns True if the text appears to be a citation fragment (not a real footnote).
    """
    # Check first ~100 chars for citations
    sample = text[:100].strip()
    if not sample:
        return False

    # If text starts with a number followed by parens with year/court,
    # it's likely a continuation fragment
    if COURT_CITATION_FRAGMENT_RE.match(sample):
        # Use eyecite to verify - if no full citation found, it's a fragment
        cites = get_citations(sample)
        # If eyecite finds a full citation starting near the beginning, it's valid
        for cite in cites:
            if isinstance(cite, (FullCaseCitation, FullLawCitation)):
                span = cite.span()
                if span[0] < 10:  # Citation starts near the beginning
                    return False  # It's a real citation, not a fragment
        return True  # No valid citation found - it's a fragment

    return False


def _is_likely_false_positive(
    label: str,
    text: str,
    strict_label_filter: bool = False,
    max_label: int = 600,
) -> bool:
    """
    Check if a matched note is likely a false positive.

    Uses eyecite for robust legal citation detection to filter out
    citation fragments that were incorrectly split by the PDF parser.

    Returns True if the match should be rejected.

    Args:
        max_label: Hard cap on numeric label values. Labels above this are rejected
            as likely page numbers from citations. Defaults to 600 for direct calls;
            document segmentation applies an adaptive cap based on page count.
    """
    label_lower = label.lower()
    text_stripped = text.strip()
    has_citation_signal = bool(REAL_FOOTNOTE_START_RE.search(text_stripped))
    has_caps_title = bool(_ALL_CAPS_TITLE_RE.search(text_stripped))
    has_strong_signal = has_citation_signal or has_caps_title
    words = text_stripped.split()
    text_lower = text_stripped.lower()

    # Table-of-contents style entries (dot leaders and trailing page numbers).
    if _TABLE_OF_CONTENTS_RE.search(text_stripped):
        return True
    if (
        _TOC_DOT_LEADER_RE.search(text_stripped)
        and _TOC_TRAILING_PAGE_RE.search(text_stripped)
        and not has_strong_signal
    ):
        return True
    if (
        _TOC_TRAILING_PAGE_RE.search(text_stripped)
        and len(words) <= 12
        and not SHORT_CITATION_RE.match(text_stripped)
    ):
        return True

    if (
        strict_label_filter
        and not label.isdigit()
        and not is_author_marker(label)
        and len(label_lower) <= 3
        and not SHORT_CITATION_RE.match(text_stripped)
        and not has_strong_signal
    ):
        return True

    # Reject short Roman numerals that are likely section headings
    if label_lower in SECTION_HEADING_ROMANS:
        # Allow if the text looks like a real footnote (has citation markers)
        if has_citation_signal:
            return False  # Likely a real footnote
        # Otherwise reject short ones or ones that look like section titles
        if len(text_stripped) < 60:
            return True
        # Check if text looks like a section title (starts with all caps word)
        words = text_stripped.split()
        if words and words[0].isupper() and len(words[0]) > 2:
            return True
        upper_tokens = sum(1 for token in words[:8] if token.isupper() and len(token) > 2)
        if upper_tokens >= 2:
            return True
        return False  # Long text, might be a real footnote

    # Reject if text is too short to be a real footnote
    # UNLESS it's a recognized short citation form like "Id." or "Id. at 123"
    if len(text_stripped) < MIN_FOOTNOTE_TEXT_LENGTH:
        if not SHORT_CITATION_RE.match(text_stripped) and not has_citation_signal:
            return True

    # Reject if text looks like just a continuation fragment
    if CONTINUATION_FRAGMENT_RE.match(text_stripped):
        if not _STATUTE_START_RE.match(text_stripped):
            return True

    # Short-document citation-fragment noise often leaks into note regions as
    # malformed starts like "U.S.C. ...", "(West) ...", or year/chapter lines.
    if label.isdigit() and not has_strong_signal:
        label_num = int(label)
        if label_num <= 25:
            if text_stripped[:1].islower():
                return True
            if text_lower.startswith(("u.s.c.", "c.f.r.")):
                return True
            if text_lower.startswith("(west)") or text_lower.startswith("west)"):
                return True
            if re.match(r"^\(?\d{4},\s*ch\.", text_lower):
                return True
            if re.match(r"^\(?\d{4}\),", text_lower):
                return True
            if re.match(r"^\d+\s*&\s*n\.\d+", text_lower):
                return True
        if text_stripped.startswith("(") and "codified at" in text_lower:
            return True

    # Use eyecite to detect citation fragments
    # e.g., "303 (3d Cir. 1984)" or "747 (1986)" without a full citation
    if _text_starts_with_citation_fragment(text_stripped):
        return True

    # Reject high numbers that are unlikely to be real footnote labels.
    # Segmentation uses an adaptive max_label to prevent short-document noise
    # while still allowing large note counts for long-form articles.
    if label.isdigit():
        label_num = int(label)
        if label_num > max_label:
            # Almost certainly a page number from a citation, not a footnote label.
            return True
        elif label_num > max(400, max_label - 200):
            # High but possible — require a substantive citation signal.
            sample = text_stripped[:50]
            if not (REAL_FOOTNOTE_START_RE.search(sample) or _ALL_CAPS_TITLE_RE.search(sample)):
                return True

    return False


def is_author_marker(label: str) -> bool:
    """Check if label is an author attribution marker (*, †, ‡, etc.)."""
    return label.strip() in AUTHOR_MARKERS


def validate_ordinality(
    footnote_numbers: list[int],
    gap_tolerance: int = DEFAULT_GAP_TOLERANCE,
) -> OrdinalityReport:
    """
    Validate footnote sequence with permissive gap tolerance.

    Args:
        footnote_numbers: List of numeric footnote labels (already parsed as ints).
        gap_tolerance: Maximum number of missing footnotes allowed before marking invalid.

    Returns:
        OrdinalityReport with status, gaps, and tolerance info.

    Examples:
        [1, 2, 3, 4]     → status="valid"
        [1, 3, 4, 5]     → status="valid_with_gaps", gaps=[2]
        [1, 2, 5, 6, 10] → status="invalid", tolerance_exceeded=True
    """
    if not footnote_numbers:
        return OrdinalityReport(
            status="valid",
            expected_range=(0, 0),
            actual_sequence=[],
            gaps=[],
            gap_tolerance=gap_tolerance,
            tolerance_exceeded=False,
        )

    sorted_nums = sorted(set(footnote_numbers))
    min_n, max_n = sorted_nums[0], sorted_nums[-1]
    expected = set(range(min_n, max_n + 1))
    actual = set(sorted_nums)
    gaps = sorted(expected - actual)

    tolerance_exceeded = len(gaps) > gap_tolerance
    if not gaps:
        status = "valid"
    elif tolerance_exceeded:
        status = "invalid"
    else:
        status = "valid_with_gaps"

    return OrdinalityReport(
        status=status,
        expected_range=(min_n, max_n),
        actual_sequence=sorted_nums,
        gaps=gaps,
        gap_tolerance=gap_tolerance,
        tolerance_exceeded=tolerance_exceeded,
    )


@dataclass
class _OpenNote:
    label: str
    note_type: str
    text_parts: list[str]
    segments: list[NoteChunk]
    page_start: int
    page_end: int

    def append(self, page: int, text: str, source: str) -> None:
        clean_text = " ".join(text.split())
        if not clean_text:
            return
        self.text_parts.append(clean_text)
        self.segments.append(NoteChunk(page=page, text=clean_text, source=source))
        self.page_end = max(self.page_end, page)

    def to_record(self, ordinal: int) -> NoteRecord:
        merged = " ".join(self.text_parts)
        merged = re.sub(r"\s+", " ", merged).strip()
        merged = re.sub(r"(?<=\w)-\s+(?=[a-z])", "", merged)
        return NoteRecord(
            ordinal=ordinal,
            label=self.label,
            note_type=self.note_type,
            text=merged,
            page_start=self.page_start,
            page_end=self.page_end,
            segments=list(self.segments),
        )


def _clean_line_text(text: str) -> str:
    return " ".join((text or "").split())


def _marker_match(text: str) -> re.Match[str] | None:
    """Raw regex match - use _validated_marker_match for filtered results."""
    candidate = _clean_line_text(text)
    if not candidate:
        return None
    return NOTE_START_RE.match(candidate)


def _validated_marker_match(
    text: str,
    *,
    strict_label_filter: bool = False,
    max_label: int = 600,
) -> re.Match[str] | None:
    """
    Match note start pattern with false positive filtering.

    Returns None if the match is likely a section heading, continuation fragment,
    or other false positive.
    """
    match = _marker_match(text)
    if not match:
        return None

    label = match.group("label")
    rest = match.group("text")

    if _is_likely_false_positive(
        label, rest, strict_label_filter=strict_label_filter, max_label=max_label
    ):
        return None

    return match


def _likely_continuation(text: str) -> bool:
    value = _clean_line_text(text)
    if not value:
        return False
    if _marker_match(value):
        return False
    if len(value) <= 2:
        return False
    return True


def _record_looks_toc_like(note: NoteRecord) -> bool:
    text = _clean_line_text(note.text)
    if not text:
        return False
    # TOC leaks are concentrated in opening pages; avoid over-filtering valid
    # citations that contain ellipses later in long-form articles.
    if int(getattr(note, "page_start", 0) or 0) > 3:
        return False
    if _TABLE_OF_CONTENTS_RE.search(text):
        return True
    if (
        _TOC_DOT_LEADER_RE.search(text)
        and len(text.split()) <= 120
        and bool(_TOC_TRAILING_PAGE_RE.search(text))
    ):
        return True
    if (
        _TOC_TRAILING_PAGE_RE.search(text)
        and len(text.split()) <= 14
        and not REAL_FOOTNOTE_START_RE.search(text)
    ):
        return True
    return False


def _finalize_open_note(open_note: _OpenNote | None, notes: list[_OpenNote]) -> _OpenNote | None:
    if open_note is not None and open_note.text_parts:
        notes.append(open_note)
    return None


def _split_embedded_note_markers(
    text: str,
    *,
    strict_label_filter: bool,
    max_label: int,
) -> tuple[str, list[tuple[str, str]]]:
    """Split continuation text that embeds additional numeric note markers.

    Returns `(continuation_text, markers)` where markers are `(label, rest)` tuples
    suitable for creating new notes.
    """
    value = _clean_line_text(text)
    if not value:
        return "", []

    marker_positions: list[int] = []
    for match in _EMBEDDED_NOTE_MARKER_RE.finditer(value):
        pos = int(match.start())
        if pos <= 0:
            continue
        candidate = value[pos:].strip()
        validated = _validated_marker_match(
            candidate, strict_label_filter=strict_label_filter, max_label=max_label
        )
        if validated is None:
            continue
        label = str(validated.group("label") or "")
        if not label.isdigit():
            continue
        marker_positions.append(pos)

    if not marker_positions:
        return value, []

    continuation = value[: marker_positions[0]].strip()
    chunks: list[tuple[str, str]] = []
    for idx, start in enumerate(marker_positions):
        end = marker_positions[idx + 1] if idx + 1 < len(marker_positions) else len(value)
        candidate = value[start:end].strip()
        validated = _validated_marker_match(
            candidate, strict_label_filter=strict_label_filter, max_label=max_label
        )
        if validated is None:
            continue
        chunks.append((str(validated.group("label")), str(validated.group("text"))))
    return continuation, chunks


def _iter_note_lines(document: ExtractedDocument) -> list[ExtractedLine]:
    lines: list[ExtractedLine] = []
    for page in document.pages:
        note_lines = page.note_lines
        separator_idx: int | None = None
        for index, line in enumerate(note_lines):
            text = _clean_line_text(line.text)
            if _FOOTNOTE_SEPARATOR_RE.match(text):
                separator_idx = index
                break
        if separator_idx is not None:
            note_lines = note_lines[separator_idx + 1 :]

        for line in note_lines:
            if _clean_line_text(line.text):
                lines.append(line)
    return lines


def _collect_endnote_start_pages(document: ExtractedDocument) -> set[int]:
    starts: set[int] = set()
    page_count = len(document.pages)
    for page in document.pages:
        for line in page.body_lines:
            if ENDNOTE_HEADING_RE.match(_clean_line_text(line.text)):
                starts.add(page.page_number)

    if starts:
        return starts

    if page_count < 2:
        return starts

    def _has_headingless_endnote_sequence(body_lines: list[str]) -> bool:
        numeric_markers: list[int] = []
        for raw_line in body_lines:
            marker = _marker_match(raw_line)
            if marker is None:
                continue
            label = marker.group("label")
            if label.isdigit():
                numeric_markers.append(int(label))

        if len(numeric_markers) < 3:
            return False

        # Require an explicit sequence that starts near 1 to avoid dense citation
        # sections (which often have high labels) from being misclassified.
        first_window = numeric_markers[:4]
        if 1 not in first_window:
            return False

        expected = 1
        run = 0
        for value in numeric_markers:
            if value < expected:
                continue
            if value == expected:
                run += 1
                expected += 1
                continue
            break
        return run >= 3

    for page in document.pages[max(page_count - 2, 0) :]:
        body_lines = [
            _clean_line_text(line.text) for line in page.body_lines if _clean_line_text(line.text)
        ]
        if len(body_lines) < 5:
            continue
        if _has_headingless_endnote_sequence(body_lines):
            starts.add(page.page_number)
            break
    return starts


def _extract_endnotes(
    document: ExtractedDocument,
    *,
    strict_label_filter: bool = False,
    max_label: int = 600,
) -> tuple[list[NoteRecord], list[str]]:
    warnings: list[str] = []
    start_pages = _collect_endnote_start_pages(document)
    if not start_pages:
        return [], warnings

    open_note: _OpenNote | None = None
    built: list[_OpenNote] = []

    for page in document.pages:
        if page.page_number < min(start_pages):
            continue
        for line in page.body_lines:
            text = _clean_line_text(line.text)
            if not text:
                continue
            marker = _validated_marker_match(
                text, strict_label_filter=strict_label_filter, max_label=max_label
            )
            if marker:
                open_note = _finalize_open_note(open_note, built)
                label = marker.group("label")
                rest = marker.group("text")
                open_note = _OpenNote(
                    label=label,
                    note_type="endnote",
                    text_parts=[],
                    segments=[],
                    page_start=page.page_number,
                    page_end=page.page_number,
                )
                open_note.append(page=page.page_number, text=rest, source=line.source)
                continue
            if open_note and _likely_continuation(text):
                open_note.append(page=page.page_number, text=text, source=line.source)

    open_note = _finalize_open_note(open_note, built)
    records = [entry.to_record(ordinal=index + 1) for index, entry in enumerate(built)]
    if records:
        warnings.append("endnotes_detected")
    return records, warnings


def segment_document_notes(document: ExtractedDocument) -> tuple[list[NoteRecord], list[str]]:
    """
    Original segmentation function for backward compatibility.
    Returns (notes, warnings) where notes includes both numbered and author marker notes.
    """
    notes, author_notes, ordinality, warnings = segment_document_notes_extended(document)
    # Merge author notes back into notes for backward compatibility
    all_notes = list(notes)
    for an in author_notes:
        all_notes.append(
            NoteRecord(
                ordinal=0,
                label=an.marker,
                note_type="author_note",
                text=an.text,
                page_start=an.page,
                page_end=an.page,
            )
        )
    return all_notes, warnings


def segment_document_notes_extended(
    document: ExtractedDocument,
    gap_tolerance: int = DEFAULT_GAP_TOLERANCE,
    *,
    strict_label_filter: bool = False,
) -> tuple[list[NoteRecord], list[AuthorNote], OrdinalityReport | None, list[str]]:
    """
    Extended segmentation that separates author notes and computes ordinality.

    Returns:
        (notes, author_notes, ordinality_report, warnings)
        - notes: List of numbered footnotes/endnotes
        - author_notes: List of author attribution notes (*, †, etc.)
        - ordinality_report: Validation of footnote sequence
        - warnings: List of warning strings
    """
    warnings: list[str] = []
    lines = _iter_note_lines(document)
    # Adaptive cap: permissive for long documents while filtering implausible
    # high labels on short PDFs where citation fragments are common.
    max_label = max(120, document.page_count * 6)

    open_note: _OpenNote | None = None
    parsed: list[_OpenNote] = []

    for line in lines:
        text = _clean_line_text(line.text)
        marker = _validated_marker_match(
            text, strict_label_filter=strict_label_filter, max_label=max_label
        )
        if marker:
            open_note = _finalize_open_note(open_note, parsed)
            label = marker.group("label")
            rest = marker.group("text")
            open_note = _OpenNote(
                label=label,
                note_type="footnote",
                text_parts=[],
                segments=[],
                page_start=line.page_number,
                page_end=line.page_number,
            )
            cont, embedded = _split_embedded_note_markers(
                rest, strict_label_filter=strict_label_filter, max_label=max_label
            )
            if cont:
                open_note.append(page=line.page_number, text=cont, source=line.source)
            for embedded_label, embedded_rest in embedded:
                open_note = _finalize_open_note(open_note, parsed)
                open_note = _OpenNote(
                    label=embedded_label,
                    note_type="footnote",
                    text_parts=[],
                    segments=[],
                    page_start=line.page_number,
                    page_end=line.page_number,
                )
                open_note.append(page=line.page_number, text=embedded_rest, source=line.source)
            continue

        if open_note and _likely_continuation(text):
            cont, embedded = _split_embedded_note_markers(
                text, strict_label_filter=strict_label_filter, max_label=max_label
            )
            if cont:
                open_note.append(page=line.page_number, text=cont, source=line.source)
            for embedded_label, embedded_rest in embedded:
                open_note = _finalize_open_note(open_note, parsed)
                open_note = _OpenNote(
                    label=embedded_label,
                    note_type="footnote",
                    text_parts=[],
                    segments=[],
                    page_start=line.page_number,
                    page_end=line.page_number,
                )
                open_note.append(page=line.page_number, text=embedded_rest, source=line.source)

    open_note = _finalize_open_note(open_note, parsed)

    records = [entry.to_record(ordinal=index + 1) for index, entry in enumerate(parsed)]

    # Allow documents that rely on endnotes in body region.
    endnotes, endnote_warnings = _extract_endnotes(
        document, strict_label_filter=strict_label_filter, max_label=max_label
    )
    warnings.extend(endnote_warnings)

    combined = [note for note in (records + endnotes) if not _record_looks_toc_like(note)]
    combined.sort(key=lambda note: (note.page_start, note.ordinal))
    for index, record in enumerate(combined, start=1):
        record.ordinal = index

    rescue_notes = _rescue_from_body_lines(document, combined)
    if rescue_notes:
        existing_combined_labels = {n.label for n in combined}
        for rescued in rescue_notes:
            if rescued.label not in existing_combined_labels:
                combined.append(rescued)
                existing_combined_labels.add(rescued.label)
        warnings.append("body_lines_rescue_used")
        combined.sort(key=lambda note: (note.page_start, note.ordinal))
        for index, record in enumerate(combined, start=1):
            record.ordinal = index

    # Separate author notes from numbered notes
    author_notes: list[AuthorNote] = []
    numbered_notes: list[NoteRecord] = []

    for note in combined:
        if is_author_marker(note.label):
            author_notes.append(AuthorNote(marker=note.label, text=note.text, page=note.page_start))
        else:
            numbered_notes.append(note)

    # Re-number after separating author notes
    for index, record in enumerate(numbered_notes, start=1):
        record.ordinal = index

    # Check for non-monotonic labels (existing behavior)
    numeric_labels = [int(note.label) for note in numbered_notes if note.label.isdigit()]
    if numeric_labels and any(b < a for a, b in zip(numeric_labels, numeric_labels[1:])):
        warnings.append("non_monotonic_labels")
        for record in numbered_notes:
            if "non_monotonic_labels" not in record.quality_flags:
                record.quality_flags.append("non_monotonic_labels")

    # Compute ordinality report for numeric footnotes
    ordinality_report: OrdinalityReport | None = None
    if numeric_labels:
        ordinality_report = validate_ordinality(numeric_labels, gap_tolerance=gap_tolerance)
        if ordinality_report.tolerance_exceeded:
            warnings.append("ordinality_invalid")
        elif ordinality_report.gaps:
            warnings.append("ordinality_gaps")

    return numbered_notes, author_notes, ordinality_report, warnings


def segment_notes_from_text(
    text: str,
    *,
    gap_tolerance: int = DEFAULT_GAP_TOLERANCE,
    strict_label_filter: bool = False,
) -> tuple[list[NoteRecord], list[AuthorNote], OrdinalityReport | None, list[str]]:
    """Extract footnotes from plain text without spatial/PDF info.

    Creates a synthetic :class:`ExtractedDocument` with all lines as body_lines
    (no spatial footnote region), then delegates to existing endnote detection
    and regex-based marker matching.  Useful for re-processing already-extracted
    fulltext without re-parsing PDFs.
    """
    lines = [
        ExtractedLine(text=line, page_number=1, source="text")
        for line in text.splitlines()
        if line.strip()
    ]
    page = ExtractedPage(
        page_number=1,
        body_lines=lines,
        note_lines=[],
        raw_text=text,
        source="text",
    )
    doc = ExtractedDocument(pdf_path="<text>", pages=[page], warnings=[], parser="text")
    return segment_document_notes_extended(
        doc,
        gap_tolerance=gap_tolerance,
        strict_label_filter=strict_label_filter,
    )
