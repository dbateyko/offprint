from __future__ import annotations

from offprint.pdf_footnotes.doc_policy import DocSignals, classify_pdf


def _signals(
    *,
    page_count: int,
    footnote_marker_count: int = 0,
    citation_pattern_count: int = 0,
    first_page_text: str = "",
) -> DocSignals:
    return DocSignals(
        page_count=page_count,
        first_page_text=first_page_text,
        citation_pattern_count=citation_pattern_count,
        footnote_marker_count=footnote_marker_count,
        metadata_article_fields=False,
        garbled_text=False,
    )


def test_doc_policy_excludes_short_docs_without_footnote_markers() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/domain.example/cover.pdf",
        domain="domain.example",
        platform_family="custom_unknown",
        signals=_signals(page_count=3, footnote_marker_count=0),
        doc_policy="article_only",
        rules={},
    )

    assert decision.doc_type == "frontmatter"
    assert decision.include is False
    assert "short_doc_without_footnotes" in decision.reason_codes


def test_doc_policy_excludes_non_article_filename_markers() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/domain.example/fall-symposium-program.pdf",
        domain="domain.example",
        platform_family="custom_unknown",
        signals=_signals(page_count=12, footnote_marker_count=0),
        doc_policy="article_only",
        rules={},
    )

    assert decision.doc_type == "other"
    assert decision.include is False
    assert "non_article_filename" in decision.reason_codes


def test_doc_policy_marks_long_roundtable_as_issue_compilation() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/domain.example/constitutional-law-roundtable.pdf",
        domain="domain.example",
        platform_family="custom_unknown",
        signals=_signals(page_count=37, footnote_marker_count=0),
        doc_policy="article_only",
        rules={},
    )

    assert decision.doc_type == "issue_compilation"
    assert decision.include is False
    assert "roundtable_filename" in decision.reason_codes
