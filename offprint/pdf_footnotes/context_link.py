from __future__ import annotations

import re

from .schema import NoteRecord
from .text_extract import ExtractedDocument, ExtractedLine

SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _split_sentences(text: str) -> list[str]:
    normalized = " ".join((text or "").split())
    if not normalized:
        return []
    parts = [part.strip() for part in SENTENCE_SPLIT_RE.split(normalized) if part.strip()]
    return parts or [normalized]


def _label_regex(label: str) -> re.Pattern[str]:
    escaped = re.escape(label)
    return re.compile(
        rf"(?:\b{escaped}\b|(?<=\w){escaped}(?=\W)|(?<=\W){escaped}(?=\w))",
        re.IGNORECASE,
    )


def _candidate_lines(
    page_lines: list[ExtractedLine], label: str
) -> tuple[list[ExtractedLine], list[ExtractedLine]]:
    label_re = _label_regex(label)
    all_non_empty = [line for line in page_lines if line.text and line.text.strip()]
    matched = [line for line in all_non_empty if label_re.search(line.text)]
    return matched, all_non_empty


def attach_context_sentence(note: NoteRecord, document: ExtractedDocument) -> None:
    page_number = note.page_start
    page = next((item for item in document.pages if item.page_number == page_number), None)
    if page is None or not page.body_lines:
        note.context_sentence = ""
        note.context_page = 0
        note.quality_flags.append("missing_context")
        return

    matched, all_lines = _candidate_lines(page.body_lines, note.label)

    chosen_line: ExtractedLine
    context_page = page_number
    if matched:
        chosen_line = matched[-1]
    else:
        # Prior-page fallback: check the preceding page before giving up
        prev_page = next(
            (item for item in document.pages if item.page_number == page_number - 1), None
        )
        if prev_page and prev_page.body_lines:
            prev_matched, _prev_all = _candidate_lines(prev_page.body_lines, note.label)
            if prev_matched:
                chosen_line = prev_matched[-1]
                context_page = page_number - 1
            else:
                chosen_line = all_lines[-1]
                note.quality_flags.append("ambiguous_context")
        else:
            # Fallback to the nearest available body text when marker is lost in OCR/text extraction.
            chosen_line = all_lines[-1]
            note.quality_flags.append("ambiguous_context")

    sentences = _split_sentences(chosen_line.text)
    if not sentences:
        note.context_sentence = chosen_line.text.strip()
    else:
        note.context_sentence = sentences[-1]
    note.context_page = context_page


def attach_context_batch(notes: list[NoteRecord], document: ExtractedDocument) -> None:
    for note in notes:
        attach_context_sentence(note, document)
