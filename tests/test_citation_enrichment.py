from __future__ import annotations

import pytest

from offprint.pdf_footnotes.citation_enrichment import enrich_note, extract_citations
from offprint.pdf_footnotes.schema import NoteRecord


def _make_note(text: str) -> NoteRecord:
    return NoteRecord(
        ordinal=1,
        label="1",
        note_type="footnote",
        text=text,
        page_start=1,
        page_end=1,
        features={"existing": "kept"},
    )


def _require_eyecite() -> None:
    pytest.importorskip("eyecite")


def test_extract_full_case_citation_with_metadata() -> None:
    _require_eyecite()

    citations = extract_citations("See Smith v. Jones, 123 U.S. 456 (2020).")

    assert len(citations) == 1
    citation = citations[0]
    assert citation["kind"] == "FullCaseCitation"
    assert citation["text"] == "123 U.S. 456"
    assert citation["span"] == [20, 32]
    assert citation["volume"] == "123"
    assert citation["reporter"] == "U.S."
    assert citation["page"] == "456"
    assert citation["year"] == "2020"
    assert citation["plaintiff"] == "Smith"
    assert citation["defendant"] == "Jones"


def test_extract_id_citation() -> None:
    _require_eyecite()

    citations = extract_citations("Id. at 460.")

    assert len(citations) == 1
    assert citations[0]["kind"] == "IdCitation"
    assert citations[0]["text"] == "Id."
    assert citations[0]["span"] == [0, 10]
    assert citations[0]["pin_cite"] == "at 460"


def test_extract_short_form_case_citation_when_supported() -> None:
    _require_eyecite()

    citations = extract_citations("See Smith, 123 U.S. at 460.")

    assert len(citations) == 1
    assert citations[0]["kind"] == "ShortCaseCitation"
    assert citations[0]["text"] == "123 U.S. at 460"
    assert citations[0]["volume"] == "123"
    assert citations[0]["reporter"] == "U.S."
    assert citations[0]["page"] == "460"
    assert citations[0]["pin_cite"] == "460"
    assert citations[0]["antecedent_guess"] == "Smith"


def test_extract_statute_citation() -> None:
    _require_eyecite()

    citations = extract_citations("See 42 U.S.C. § 1983.")

    assert len(citations) == 1
    assert citations[0]["kind"] == "FullLawCitation"
    assert citations[0]["text"] == "42 U.S.C. § 1983"
    assert citations[0]["title"] == "42"
    assert citations[0]["reporter"] == "U.S.C."
    assert citations[0]["section"] == "1983"


def test_extract_no_citations_returns_empty_list() -> None:
    assert extract_citations("This footnote has no legal citation.") == []


def test_enrich_note_returns_copy_and_preserves_existing_features() -> None:
    _require_eyecite()
    note = _make_note("See Smith v. Jones, 123 U.S. 456 (2020).")

    enriched = enrich_note(note)

    assert enriched is not note
    assert note.features == {"existing": "kept"}
    assert enriched.features["existing"] == "kept"
    assert enriched.features["citations"][0]["kind"] == "FullCaseCitation"


def test_extract_citations_gracefully_handles_eyecite_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import offprint.pdf_footnotes.citation_enrichment as citation_enrichment

    def failing_get_citations(text: str) -> list[object]:
        raise RuntimeError("eyecite failed")

    monkeypatch.setattr(citation_enrichment, "_load_get_citations", lambda: failing_get_citations)

    assert citation_enrichment.extract_citations("123 U.S. 456") == []
