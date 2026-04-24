from __future__ import annotations

from types import SimpleNamespace

from offprint.pdf_footnotes.pipeline import _note_confidence, _note_sequence_quality_score
from offprint.pdf_footnotes.schema import NoteRecord


def _make_note(*, quality_flags: list[str] | None = None) -> NoteRecord:
    return NoteRecord(
        ordinal=1,
        label="1",
        note_type="footnote",
        text="This note is long enough to exercise the scoring path.",
        page_start=1,
        page_end=1,
        context_body="context",
        quality_flags=list(quality_flags or []),
    )


def test_note_confidence_ignores_non_monotonic_quality_flag() -> None:
    baseline = _make_note()
    flagged = _make_note(quality_flags=["non_monotonic_labels"])

    assert _note_confidence(flagged) == _note_confidence(baseline)


def test_note_sequence_quality_score_ignores_non_monotonic_warning() -> None:
    notes = [_make_note()]
    ordinality = SimpleNamespace(status="valid", gaps=[])

    baseline = _note_sequence_quality_score(notes, ordinality, warnings=[])
    flagged = _note_sequence_quality_score(
        notes,
        ordinality,
        warnings=["non_monotonic_labels"],
    )

    assert flagged == baseline
