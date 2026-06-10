"""OCR-specific footnote extractor for image-only law-review scans.

Image-only scans (no text layer) are recovered by olmOCR into plain *page* text
that, unlike liteparse output, carries **no spatial footnote region**.  Each page
is laid out as:

    <body prose, with inline superscript refs rendered as ".49", "potent.50", ...>
    <footnote block: "49 <citation> ...", "50 <citation> ...", ...>

The standard segmenter (:func:`segment_document_notes_extended`) keys off the
spatially-tagged ``note_lines`` that liteparse produces, so feeding it raw OCR
text yields either zero notes (everything in ``body_lines``) or body prose
absorbed into footnote text (everything in ``note_lines``).

This module reconstructs the body/footnote split that the segmenter expects.  It
detects each page's footnote block as the bottom suffix that begins at the first
*leading-labelled* line from which the labelled lines ascend — body prose carries
footnote refs only *inline* ("approvals.49"), never as a leading label, so the
first leading-labelled line is the block start on a footnote page.  Those lines
become ``note_lines``; the prose above becomes ``body_lines``.  Detection is
per-page and counter-free, so a footnote block that olmOCR drops on one page
(it sometimes does) does not desync the pages around it.

Note-building (continuation joining, embedded-marker splitting, ordinality) is
then delegated to the existing, well-tested segmenter — this module only solves
the OCR-specific *region reconstruction*.  Gaps that remain in the recovered
1..max sequence are blocks olmOCR never emitted (an OCR-recall limit), surfaced
as :attr:`OcrFootnoteResult.dropped_labels`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from .note_segment import (
    AuthorNote,
    NoteRecord,
    OrdinalityReport,
    _strip_ocr_repository_artifacts,
    segment_document_notes_extended,
)
from .text_extract import ExtractedDocument, ExtractedLine, ExtractedPage

DEFAULT_GAP_TOLERANCE = 4

# A footnote-block line: "49 Text...", "49. Text", "49) Text".
_LABEL_LINE = re.compile(r"^\s*(\d{1,4})[.)]?\s+(\S.*)$")
# A table-of-contents entry: "Title ........... 77" — never a footnote.
_TOC_LEADER = re.compile(r"\.{4,}\s*\d{1,4}\s*$")


def _leading_label(line: str) -> int | None:
    """Leading footnote label of a line, or None (TOC leaders excluded)."""
    if _TOC_LEADER.search(line):
        return None
    m = _LABEL_LINE.match(line)
    return int(m.group(1)) if m else None


@dataclass
class OcrFootnoteResult:
    notes: list[NoteRecord]
    author_notes: list[AuthorNote]
    ordinality: OrdinalityReport | None
    warnings: list[str]
    # Diagnostics: footnote labels seen as inline refs in the body but never
    # recovered as a footnote block — i.e. blocks olmOCR dropped.
    dropped_labels: list[int] = field(default_factory=list)
    pages_with_block: int = 0


def _page_texts(pages: Iterable[Any]) -> list[tuple[int, str]]:
    """Normalize OCR sidecar page records to (page_number, text)."""
    out: list[tuple[int, str]] = []
    for r in pages:
        if isinstance(r, dict):
            if r.get("type") == "doc":
                continue
            pg = int(r.get("page") or r.get("page_number") or (len(out) + 1))
            text = r.get("text") or r.get("markdown") or ""
        else:  # (page, text) tuple
            pg, text = int(r[0]), r[1] or ""
        out.append((pg, text))
    return out


def _ascending_score(labels: list[int], gap_tolerance: int) -> float:
    """Fraction of consecutive label steps that ascend (footnote blocks ~1.0)."""
    if len(labels) < 2:
        return 1.0
    good = sum(1 for a, b in zip(labels, labels[1:]) if b > a)
    return good / (len(labels) - 1)


def _split_page(lines: list[str], gap_tolerance: int) -> tuple[list[str], list[str]]:
    """Split one page's lines into (body_lines, note_lines).

    Counter-free: the footnote block is the bottom suffix that begins at the first
    *leading-labelled* line from which the labelled lines ascend.  Body prose
    carries footnote refs only *inline* (mid-line, e.g. "approvals.49"), never as
    a leading label, so the first leading-labelled line is the block start on a
    footnote page.  A leading number that opens a non-ascending run (a stray body
    number, a numbered heading) is rejected, keeping it in the body.
    """
    labelled = [(i, _leading_label(ln)) for i, ln in enumerate(lines)]
    labelled = [(i, v) for i, v in labelled if v is not None]
    if not labelled:
        return lines, []

    # Candidate block start = earliest labelled line whose following labelled run
    # is predominantly ascending. Scanning earliest-first keeps the whole block.
    for si, (i, _v) in enumerate(labelled):
        run = [v for _, v in labelled[si:]]
        if _ascending_score(run, gap_tolerance) >= 0.75:
            return lines[:i], lines[i:]
    # No ascending run anywhere — treat the whole page as body.
    return lines, []


def extract_ocr_footnotes(
    pages: Iterable[Any],
    *,
    gap_tolerance: int = DEFAULT_GAP_TOLERANCE,
    strict_label_filter: bool = False,
) -> OcrFootnoteResult:
    """Extract footnotes from olmOCR page records of an image-only scan."""
    page_texts = _page_texts(pages)

    extracted_pages: list[ExtractedPage] = []
    pages_with_block = 0

    for pg, text in page_texts:
        raw_lines = []
        for ln in text.splitlines():
            s = _strip_ocr_repository_artifacts(ln.strip())
            if s.strip():
                raw_lines.append(s)
        if not raw_lines:
            continue

        body, note = _split_page(raw_lines, gap_tolerance)
        if note:
            pages_with_block += 1

        extracted_pages.append(
            ExtractedPage(
                page_number=pg,
                body_lines=[
                    ExtractedLine(text=ln, page_number=pg, source="ocr") for ln in body
                ],
                note_lines=[
                    ExtractedLine(text=ln, page_number=pg, source="ocr") for ln in note
                ],
                raw_text=text,
                source="ocr",
            )
        )

    doc = ExtractedDocument(
        pdf_path="<ocr>", pages=extracted_pages, warnings=[], parser="ocr"
    )
    notes, author_notes, ordinality, warnings = segment_document_notes_extended(
        doc, gap_tolerance=gap_tolerance, strict_label_filter=strict_label_filter
    )

    # Footnote labels are unique by definition. The spatial segmenter can emit a
    # label twice when a block line is also re-found by its endnote/body scan;
    # collapse to one record per label, keeping the longest (most complete) text.
    by_label: dict[str, NoteRecord] = {}
    deduped: list[NoteRecord] = []
    for n in notes:
        prev = by_label.get(n.label)
        if prev is None:
            by_label[n.label] = n
            deduped.append(n)
        elif len(n.text or "") > len(prev.text or ""):
            deduped[deduped.index(prev)] = n
            by_label[n.label] = n
    deduped.sort(key=lambda n: (n.page_start, n.ordinal))
    for idx, rec in enumerate(deduped, start=1):
        rec.ordinal = idx
    notes = deduped

    # The extractor recovers essentially every footnote block present in the OCR
    # text, so the remaining gaps in the recovered 1..max sequence are blocks
    # olmOCR failed to emit (an OCR-recall limit, not an extraction one).
    recovered = {int(n.label) for n in notes if n.label.isdigit()}
    dropped: list[int] = []
    if recovered:
        dropped = sorted(set(range(1, max(recovered) + 1)) - recovered)

    return OcrFootnoteResult(
        notes=notes,
        author_notes=author_notes,
        ordinality=ordinality,
        warnings=warnings,
        dropped_labels=dropped,
        pages_with_block=pages_with_block,
    )
