from __future__ import annotations

from offprint.pdf_footnotes.note_segment import validate_ordinality
from offprint.pdf_footnotes.pipeline import (
    FootnoteProfile,
    _hydrate_or_segment_document_notes,
    _liteparse_candidate_score,
    _promote_liteparse_body_gap_markers,
    _select_liteparse_candidate_document,
)
from offprint.pdf_footnotes.schema import NoteRecord, OrdinalityReport
from offprint.pdf_footnotes.text_extract import ExtractedDocument, ExtractedLine, ExtractedPage


def _note(label: int, page: int = 1) -> NoteRecord:
    return NoteRecord(
        ordinal=label,
        label=str(label),
        note_type="footnote",
        text=f"See authority for note {label}.",
        page_start=page,
        page_end=page,
    )


def _doc(name: str, note_lines: list[str], body_lines: list[str] | None = None) -> ExtractedDocument:
    return ExtractedDocument(
        pdf_path="/tmp/test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                height=800,
                body_lines=[
                    ExtractedLine(text=text, page_number=1, top=top, source="liteparse")
                    for top, text in enumerate(body_lines or [], start=100)
                ],
                note_lines=[
                    ExtractedLine(text=text, page_number=1, top=top, source="liteparse")
                    for top, text in enumerate(note_lines, start=620)
                ],
                source="liteparse",
            )
        ],
        parser="liteparse",
        metadata={"liteparse_candidate": name},
    )


def test_liteparse_candidate_score_penalizes_duplicate_heavy_valid_stream() -> None:
    clean_notes = [_note(1), _note(2), _note(3)]
    duplicate_notes = [_note(1), _note(1), _note(2), _note(2), _note(3), _note(3)]
    clean_ord = validate_ordinality([1, 2, 3], gap_tolerance=0)
    duplicate_ord = validate_ordinality([1, 1, 2, 2, 3, 3], gap_tolerance=0)

    clean_score, clean_metrics = _liteparse_candidate_score(clean_notes, clean_ord, [])
    duplicate_score, duplicate_metrics = _liteparse_candidate_score(
        duplicate_notes,
        duplicate_ord,
        [],
    )

    assert clean_metrics["duplicate_numeric_labels"] == 0
    assert duplicate_metrics["duplicate_numeric_labels"] == 3
    assert clean_score > duplicate_score


def test_body_marker_promotion_moves_gap_label_from_body_to_notes() -> None:
    document = _doc(
        "default",
        note_lines=[
            "1. See first authority.",
            "3. See third authority.",
        ],
        body_lines=[
            "2. See second authority that was classified as body.",
        ],
    )
    notes = [_note(1), _note(3)]
    ordinality = validate_ordinality([1, 3], gap_tolerance=0)

    promoted = _promote_liteparse_body_gap_markers(
        document,
        notes,
        ordinality,
        strict_label_filter=True,
    )

    assert promoted is not None
    assert "liteparse_body_marker_promotion_used" in promoted.warnings
    assert [line.text for line in promoted.pages[0].body_lines] == []
    assert any(line.text.startswith("2.") for line in promoted.pages[0].note_lines)


def test_select_liteparse_candidate_attaches_scores_metadata() -> None:
    weak = _doc(
        "weak",
        note_lines=[
            "1. See first authority.",
            "3. See third authority.",
        ],
    )
    strong = _doc(
        "strong",
        note_lines=[
            "1. See first authority.",
            "2. See second authority.",
            "3. See third authority.",
        ],
    )

    selected = _select_liteparse_candidate_document(
        [weak, strong],
        profile_for=lambda _doc: FootnoteProfile(gap_tolerance=0, strict_label_filter=True),
    )

    assert selected is not None
    assert selected.metadata["liteparse_selected_candidate"] == "strong"
    assert "strong" in selected.metadata["liteparse_candidate_scores"]
    assert "liteparse_candidate_selected=strong" in selected.warnings


def test_select_liteparse_candidate_prefers_valid_over_higher_scoring_gappy_stream() -> None:
    gappy = _doc(
        "gappy",
        note_lines=[
            "1. See first authority.",
            "3. See third authority.",
            "4. See fourth authority.",
        ],
    )
    valid_with_duplicates = _doc(
        "valid_with_duplicates",
        note_lines=[
            "1. See first authority.",
            "2. See second authority.",
            "2. See duplicate second authority.",
            "3. See third authority.",
            "4. See fourth authority.",
        ],
    )

    selected = _select_liteparse_candidate_document(
        [gappy, valid_with_duplicates],
        profile_for=lambda _doc: FootnoteProfile(gap_tolerance=2, strict_label_filter=True),
    )

    assert selected is not None
    assert selected.metadata["liteparse_selected_candidate"] == "valid_with_duplicates"


def test_hydrate_prefers_precomputed_solver_payload_over_segmenter() -> None:
    """When the selector attached sequence_solver_precomputed metadata, the
    hydrator must return those notes directly instead of re-running the
    segmenter. A downstream segmenter pass would re-apply
    _is_likely_false_positive over solver-accepted labels and produce
    "selected=258, notes=4"-style disagreement — the whole point of the
    precomputed path is that the solver's decisions are authoritative.
    """
    precomputed_notes = [_note(1), _note(2), _note(3), _note(4), _note(5)]
    ordinality = OrdinalityReport(
        status="valid",
        expected_range=(1, 5),
        actual_sequence=[1, 2, 3, 4, 5],
        gaps=[],
        gap_tolerance=0,
        tolerance_exceeded=False,
    )

    # Build a document where the segmenter would find something different
    # (note lines whose markers are shaped to be treated as false-positive).
    doc = _doc(
        "sequence_solver",
        note_lines=[
            "99. Garbled text that the segmenter would struggle with.",
        ],
    )
    doc.metadata = dict(doc.metadata or {})
    doc.metadata["sequence_solver_precomputed"] = {
        "notes": precomputed_notes,
        "author_notes": [],
        "ordinality": ordinality,
    }

    notes, author_notes, returned_ordinality, warnings = _hydrate_or_segment_document_notes(
        doc,
        profile=FootnoteProfile(gap_tolerance=0, strict_label_filter=True),
    )

    assert len(notes) == 5
    assert [n.label for n in notes] == ["1", "2", "3", "4", "5"]
    assert returned_ordinality is ordinality
    assert "sequence_solver_segmenter_bypassed" in warnings
    assert author_notes == []


def test_hydrate_falls_back_to_segmenter_without_precomputed_payload() -> None:
    doc = _doc(
        "default",
        note_lines=[
            "1. See first authority.",
            "2. See second authority.",
            "3. See third authority.",
        ],
    )
    # No sequence_solver_precomputed key on metadata.

    notes, _author_notes, ordinality, warnings = _hydrate_or_segment_document_notes(
        doc,
        profile=FootnoteProfile(gap_tolerance=0, strict_label_filter=True),
    )

    assert len(notes) == 3
    assert ordinality is not None
    assert "sequence_solver_segmenter_bypassed" not in warnings
