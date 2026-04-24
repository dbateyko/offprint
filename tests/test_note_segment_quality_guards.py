from __future__ import annotations

from offprint.pdf_footnotes.note_segment import (
    _collect_endnote_start_pages,
    segment_document_notes_extended,
)
from offprint.pdf_footnotes.text_extract import ExtractedDocument, ExtractedLine, ExtractedPage


def _doc_with_note_lines(lines: list[str]) -> ExtractedDocument:
    return ExtractedDocument(
        pdf_path="/tmp/test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                note_lines=[ExtractedLine(text=line, page_number=1, source="text") for line in lines],
            )
        ],
        parser="test",
    )


def _doc_with_body_lines(lines: list[str]) -> ExtractedDocument:
    return ExtractedDocument(
        pdf_path="/tmp/test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                body_lines=[ExtractedLine(text=line, page_number=1, source="text") for line in lines],
                note_lines=[],
            )
        ],
        parser="test",
    )


def test_rejects_non_canonical_roman_word_labels() -> None:
    document = _doc_with_note_lines(
        [
            "civil rights litigation continues in this paragraph and should not become a label.",
            "1. See valid note.",
        ]
    )
    notes, _author_notes, _ordinality, _warnings = segment_document_notes_extended(document)

    assert [n.label for n in notes] == ["1"]


def test_strips_repository_banner_from_note_text() -> None:
    document = _doc_with_note_lines(
        [
            "1. For a similar view, see Morgenbesser.",
            (
                "Florida Law Review, Vol. 14, Iss. 4 [1962], Art. 8 "
                "https://scholarship.law.ufl.edu/flr/vol14/iss4/8"
            ),
        ]
    )
    notes, _author_notes, _ordinality, _warnings = segment_document_notes_extended(document)

    assert len(notes) == 1
    text = notes[0].text.lower()
    assert "florida law review, vol." not in text
    assert "scholarship.law.ufl.edu" not in text


def test_relaxed_embedded_marker_split_on_long_ocrish_line() -> None:
    long_prefix = " ".join(["context"] * 40)
    document = _doc_with_note_lines(
        [
            f"1. {long_prefix} 2 See second authority for this claim.",
        ]
    )
    notes, _author_notes, _ordinality, _warnings = segment_document_notes_extended(document)

    assert [n.label for n in notes] == ["1", "2"]
    assert notes[1].text.startswith("See second authority")


def test_embedded_expected_next_marker_split_on_short_lowercase_line() -> None:
    document = _doc_with_note_lines(
        [
            "45. Existing authority closes here 46 dollar. Additionally, markets dominate.",
        ]
    )
    notes, _author_notes, _ordinality, _warnings = segment_document_notes_extended(document)

    assert [n.label for n in notes] == ["45", "46"]
    assert notes[0].text == "Existing authority closes here"
    assert notes[1].text.startswith("dollar. Additionally")


def test_embedded_marker_does_not_split_unexpected_number() -> None:
    document = _doc_with_note_lines(
        [
            "45. Existing authority cites 42 U.S.C. section 1983 before closing.",
        ]
    )
    notes, _author_notes, _ordinality, _warnings = segment_document_notes_extended(document)

    assert [n.label for n in notes] == ["45"]
    assert "42 U.S.C." in notes[0].text


def test_rejects_roman_labels_as_note_starts() -> None:
    document = _doc_with_note_lines(
        [
            "XIII. This is a section header-like OCR fragment, not a footnote marker.",
            "1. See valid numeric note.",
        ]
    )
    notes, _author_notes, _ordinality, _warnings = segment_document_notes_extended(document)

    assert [n.label for n in notes] == ["1"]


def test_caps_note_text_growth_to_prevent_runaway_merges() -> None:
    huge_chunk = " ".join(["longtext"] * 2000)
    document = _doc_with_note_lines(
        [
            "1. Start of note.",
            huge_chunk,
            huge_chunk,
            huge_chunk,
            "2. Next note starts here.",
        ]
    )
    notes, _author_notes, _ordinality, _warnings = segment_document_notes_extended(document)

    assert [n.label for n in notes] == ["1", "2"]
    first = notes[0]
    assert len(first.text) <= 8000
    assert "note_max_chars_truncated" in first.quality_flags


def test_headingless_endnotes_not_detected_from_sparse_numeric_body_lines() -> None:
    document = _doc_with_body_lines(
        [
            "In this article, 1 U.S.C. section numbers and 2 policy points are discussed.",
            "The framework has 3 parts and 4 implications.",
            "Conclusion and outlook for future work.",
            "Additional narrative text that should not be treated as endnotes.",
            "References are discussed in prose, not as line-starting footnotes.",
            "Final paragraph text.",
        ]
    )
    starts = _collect_endnote_start_pages(document)
    assert starts == set()


def test_headingless_endnotes_detected_when_marker_lines_are_dense() -> None:
    document = ExtractedDocument(
        pdf_path="/tmp/test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                body_lines=[
                    ExtractedLine(text="Intro paragraph on page one.", page_number=1, source="text"),
                    ExtractedLine(text="More body text.", page_number=1, source="text"),
                ],
                note_lines=[],
            ),
            ExtractedPage(
                page_number=2,
                body_lines=[
                    ExtractedLine(text="1. First endnote line.", page_number=2, source="text"),
                    ExtractedLine(text="2. Second endnote line.", page_number=2, source="text"),
                    ExtractedLine(text="3. Third endnote line.", page_number=2, source="text"),
                    ExtractedLine(text="4. Fourth endnote line.", page_number=2, source="text"),
                    ExtractedLine(text="5. Fifth endnote line.", page_number=2, source="text"),
                    ExtractedLine(text="6. Sixth endnote line.", page_number=2, source="text"),
                    ExtractedLine(text="7. Seventh endnote line.", page_number=2, source="text"),
                    ExtractedLine(text="Additional continuation text.", page_number=2, source="text"),
                ],
                note_lines=[],
            ),
        ],
        parser="test",
    )
    starts = _collect_endnote_start_pages(document)
    assert starts == {2}


def test_ignores_early_notes_heading_as_endnote_start() -> None:
    document = ExtractedDocument(
        pdf_path="/tmp/test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                body_lines=[ExtractedLine(text="Introduction", page_number=1, source="text")],
                note_lines=[],
            ),
            ExtractedPage(
                page_number=2,
                body_lines=[
                    ExtractedLine(text="NOTES", page_number=2, source="text"),
                    ExtractedLine(text="This is a section heading in body text.", page_number=2, source="text"),
                ],
                note_lines=[],
            ),
            ExtractedPage(
                page_number=3,
                body_lines=[ExtractedLine(text="More body analysis.", page_number=3, source="text")],
                note_lines=[],
            ),
            ExtractedPage(
                page_number=4,
                body_lines=[ExtractedLine(text="Conclusion.", page_number=4, source="text")],
                note_lines=[],
            ),
        ],
        parser="test",
    )
    starts = _collect_endnote_start_pages(document)
    assert starts == set()


def test_accepts_late_notes_heading_as_endnote_start() -> None:
    document = ExtractedDocument(
        pdf_path="/tmp/test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                body_lines=[ExtractedLine(text="Introduction", page_number=1, source="text")],
                note_lines=[],
            ),
            ExtractedPage(
                page_number=2,
                body_lines=[ExtractedLine(text="Body text", page_number=2, source="text")],
                note_lines=[],
            ),
            ExtractedPage(
                page_number=3,
                body_lines=[ExtractedLine(text="More body text", page_number=3, source="text")],
                note_lines=[],
            ),
            ExtractedPage(
                page_number=4,
                body_lines=[ExtractedLine(text="NOTES", page_number=4, source="text")],
                note_lines=[],
            ),
        ],
        parser="test",
    )
    starts = _collect_endnote_start_pages(document)
    assert starts == {4}
