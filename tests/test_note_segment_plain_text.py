from offprint.pdf_footnotes.note_segment import segment_notes_from_text


def test_plain_text_explicit_notes_heading_is_not_discarded_as_page_one():
    text = """A sufficiently developed article body discusses doctrine and policy.
It contains ordinary prose rather than a numbered list.

NOTES
1. First supporting authority, 1 U.S. 2 (1900).
2. Second supporting authority, 2 U.S. 3 (1901).
"""

    notes, author_notes, ordinality, warnings = segment_notes_from_text(text)

    assert [note.label for note in notes] == ["1", "2"]
    assert "First supporting authority" in notes[0].text
    assert author_notes == []
    assert ordinality is not None and ordinality.status == "valid"
    assert "endnotes_detected" in warnings


def test_plain_text_without_explicit_note_region_remains_conservative():
    text = "Body paragraph.\n1. A numbered body list item.\n2. Another body list item."

    notes, _, ordinality, _ = segment_notes_from_text(text)

    assert notes == []
    assert ordinality is None
