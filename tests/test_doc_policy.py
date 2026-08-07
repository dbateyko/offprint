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


# ---- Residual-triage rules (added 2026-05-02) ----


def test_doc_policy_excludes_vla_resource_handout() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/www.yjil.org/vla-resource-family-law-in-australia-separation.pdf",
        domain="www.yjil.org",
        platform_family="custom_unknown",
        # Mimic a 4pp handout that scrapes 1 footnote (a phone number) plus
        # enough citation tokens to look article-like.
        signals=_signals(
            page_count=4, footnote_marker_count=2, citation_pattern_count=2
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "other"
    assert decision.include is False
    assert "non_article_filename" in decision.reason_codes


def test_doc_policy_excludes_tabla_de_citacion() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/derecho.uprrp.edu/tabla-de-citaci-n-3.pdf",
        domain="derecho.uprrp.edu",
        platform_family="custom_unknown",
        signals=_signals(page_count=4),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "other"
    assert "non_article_filename" in decision.reason_codes


def test_doc_policy_excludes_remix_art_contest_source_list() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/copyrightsociety.org/list-of-all-source-material-used-cc-open-culture-remix-art-contest-2022.pdf",
        domain="copyrightsociety.org",
        platform_family="custom_unknown",
        signals=_signals(page_count=7, footnote_marker_count=10),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "other"


def test_doc_policy_excludes_acuta_journal_compilation() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/digitalcommons.unl.edu/acutajournal-vol-23.pdf",
        domain="digitalcommons.unl.edu",
        platform_family="digital_commons",
        signals=_signals(page_count=53, footnote_marker_count=20),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "other"


def test_doc_policy_excludes_seasonal_alumni_magazine() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/www.lawschool.cornell.edu/2011-fall.pdf",
        domain="www.lawschool.cornell.edu",
        platform_family="custom_unknown",
        signals=_signals(
            page_count=82,
            footnote_marker_count=12,
            citation_pattern_count=3,
            first_page_text="CORNELL LAW FORUM\nFall 2011\nA History of the School's International Programs",
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "other"
    assert "seasonal_alumni_magazine" in decision.reason_codes


def test_doc_policy_seasonal_filename_alone_does_not_disqualify_short_article() -> None:
    """Guard: a real article whose filename happens to contain a season word
    (e.g. '2024-fall-symposium-introduction.pdf') must NOT be excluded
    just because of the filename — the rule requires either ≥30 pp OR a
    magazine title-text marker."""
    decision = classify_pdf(
        pdf_path="/tmp/example.com/2024-fall.pdf",
        domain="example.com",
        platform_family="custom_unknown",
        signals=_signals(
            page_count=20,
            footnote_marker_count=15,
            citation_pattern_count=10,
            first_page_text="ARTICLE TITLE\nBy Jane Smith\n\nThis Article argues...",
        ),
        doc_policy="article_only",
        rules={},
    )
    # Guard does not fire (no magazine marker, < 30pp), so falls through
    # to the article_like branch.
    assert decision.doc_type == "article"


def test_doc_policy_excludes_bepress_book_reviews_coversheet() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/ir.law.utk.edu/viewcontent.cgi-65987.pdf",
        domain="ir.law.utk.edu",
        platform_family="digital_commons",
        signals=_signals(
            page_count=9,
            footnote_marker_count=4,
            citation_pattern_count=2,
            first_page_text="Book Reviews\n\nTennessee Law Review\nVolume 6\nIssue 1\nArticle 7",
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "frontmatter"
    assert "bepress_coversheet_title" in decision.reason_codes


def test_doc_policy_excludes_bepress_contents_coversheet() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/ir.law.utk.edu/viewcontent.cgi-da3bb.pdf",
        domain="ir.law.utk.edu",
        platform_family="digital_commons",
        signals=_signals(
            page_count=4,
            footnote_marker_count=3,
            first_page_text="Contents\n\nVolume 12 Issue 2 Article 1",
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "frontmatter"


def test_doc_policy_excludes_court_filing() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/digitalcommons.nyls.edu/some-filing.pdf",
        domain="digitalcommons.nyls.edu",
        platform_family="digital_commons",
        signals=_signals(
            page_count=15,
            footnote_marker_count=10,
            citation_pattern_count=8,
            first_page_text="In the United States Court of Appeals\n\nPetition for Rehearing or Rehearing En Banc",
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "other"
    assert "court_filing_title" in decision.reason_codes


def test_doc_policy_excludes_panel_transcript() -> None:
    body = (
        "Symposium on Constitutional Law\n\n"
        "MR. GHIDONI: Thank you, Professor.\n"
        "PROFESSOR CLUTE: I disagree on the standing issue.\n"
        "MR. GHIDONI: Let me address that.\n"
        "JUDGE SMITH: Counsel, please proceed.\n"
    )
    decision = classify_pdf(
        pdf_path="/tmp/digitalcommons.law.uga.edu/symposium-panel.pdf",
        domain="digitalcommons.law.uga.edu",
        platform_family="digital_commons",
        signals=_signals(
            page_count=11,
            footnote_marker_count=3,
            citation_pattern_count=2,
            first_page_text=body,
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "other"
    assert "panel_transcript" in decision.reason_codes


def test_doc_policy_does_not_misclassify_normal_article_with_one_quoted_speaker() -> None:
    """Guard: an article that quotes one speaker ("MR. SMITH said") must not
    trip the panel-transcript rule. Threshold is ≥3 distinct tagged turns."""
    body = (
        "ARTICLE TITLE\nBy Jane Doe\n\n"
        "Introduction. The defendant argued through counsel: MR. SMITH: 'I object.' "
        "The court overruled."
    )
    decision = classify_pdf(
        pdf_path="/tmp/example.com/normal-article.pdf",
        domain="example.com",
        platform_family="custom_unknown",
        signals=_signals(
            page_count=30,
            footnote_marker_count=20,
            citation_pattern_count=10,
            first_page_text=body,
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "article"


# ---- OCR-gate tests (added 2026-05-02) ----


def test_doc_policy_routes_low_text_density_to_needs_ocr() -> None:
    """A scanned-image PDF with no usable text layer should be routed to
    needs_ocr rather than counted as an article-bucket failure."""
    decision = classify_pdf(
        pdf_path="/tmp/example.com/scanned-article.pdf",
        domain="example.com",
        platform_family="custom_unknown",
        signals=DocSignals(
            page_count=20,
            first_page_text="",  # text layer empty
            citation_pattern_count=0,
            footnote_marker_count=0,
            metadata_article_fields=False,
            garbled_text=False,
            text_density_per_page=15.0,  # well below 200
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "needs_ocr"
    assert decision.include is False
    assert "low_text_density" in decision.reason_codes


def test_doc_policy_does_not_route_short_doc_to_needs_ocr() -> None:
    """3pp covers/squibs are caught by other rules first; the OCR gate
    requires page_count >= 4 to avoid stealing legitimate frontmatter
    classifications."""
    decision = classify_pdf(
        pdf_path="/tmp/example.com/short-cover.pdf",
        domain="example.com",
        platform_family="custom_unknown",
        signals=DocSignals(
            page_count=3,
            first_page_text="",
            citation_pattern_count=0,
            footnote_marker_count=0,
            metadata_article_fields=False,
            garbled_text=False,
            text_density_per_page=10.0,
        ),
        doc_policy="article_only",
        rules={},
    )
    # short_doc_without_footnotes fires first
    assert decision.doc_type == "frontmatter"


def test_doc_policy_does_not_route_clean_text_layer_to_needs_ocr() -> None:
    """A normal article with a clean text layer (>200 cpp) must stay
    classified as `article`, even if other signals are weak."""
    decision = classify_pdf(
        pdf_path="/tmp/example.com/normal.pdf",
        domain="example.com",
        platform_family="custom_unknown",
        signals=DocSignals(
            page_count=30,
            first_page_text="ARTICLE TITLE\nBy Jane Doe\n\nThis Article argues...",
            citation_pattern_count=10,
            footnote_marker_count=20,
            metadata_article_fields=False,
            garbled_text=False,
            text_density_per_page=2400.0,
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "article"
    assert decision.include is True


def test_doc_policy_legacy_density_none_does_not_misroute() -> None:
    """Backward-compat: when caller doesn't probe density (legacy code path),
    text_density_per_page is None and the OCR gate must NOT fire."""
    decision = classify_pdf(
        pdf_path="/tmp/example.com/legacy.pdf",
        domain="example.com",
        platform_family="custom_unknown",
        signals=DocSignals(
            page_count=30,
            first_page_text="Body text with citations Smith v. Jones",
            citation_pattern_count=5,
            footnote_marker_count=10,
            metadata_article_fields=False,
            garbled_text=False,
            text_density_per_page=None,
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "article"


def test_doc_policy_strong_frontmatter_takes_precedence_over_ocr_gate() -> None:
    """A document caught by a stronger rule (e.g. masthead filename) must
    stay classified by that rule even if its text density is low."""
    decision = classify_pdf(
        pdf_path="/tmp/example.com/masthead.pdf",
        domain="example.com",
        platform_family="custom_unknown",
        signals=DocSignals(
            page_count=10,
            first_page_text="",
            citation_pattern_count=0,
            footnote_marker_count=0,
            metadata_article_fields=False,
            garbled_text=False,
            text_density_per_page=5.0,
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "other"
    assert "non_article_filename" in decision.reason_codes


def test_should_include_doc_excludes_needs_ocr_from_all_policies() -> None:
    """needs_ocr docs must be excluded from article_only and
    include_issue_compilations; only `all` includes them."""
    from offprint.pdf_footnotes.doc_policy import should_include_doc
    assert not should_include_doc("needs_ocr", "article_only")
    assert not should_include_doc("needs_ocr", "include_issue_compilations")
    assert should_include_doc("needs_ocr", "all")


def test_doc_policy_routes_stetson_volume_paths_to_other():
    """Legacy manually-imported Stetson PDFs nested under
    `www.stetson.edu/Volume NN/` should be routed to `other` regardless of
    their other signals."""
    decision = classify_pdf(
        pdf_path="/mnt/data/corpus/scraped/www.stetson.edu/Volume 44/article.pdf",
        domain="www.stetson.edu",
        platform_family="custom_unknown",
        signals=_signals(
            page_count=30,
            footnote_marker_count=20,
            citation_pattern_count=10,
            first_page_text="ARTICLE TITLE\nBy Jane Doe\n\nAbstract...",
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "other"
    assert "legacy_import_path" in decision.reason_codes
    assert decision.include is False


def test_doc_policy_does_not_skip_normal_stetson_path():
    """Stetson PDFs at the canonical flat-by-domain depth should still be
    classifiable as articles. (Future-proofing: when WAF is bypassed and
    Stetson is properly scraped, files will land at the expected location.)"""
    decision = classify_pdf(
        pdf_path="/mnt/data/corpus/scraped/www.stetson.edu/normal-article.pdf",
        domain="www.stetson.edu",
        platform_family="custom_unknown",
        signals=_signals(
            page_count=30,
            footnote_marker_count=20,
            citation_pattern_count=10,
            first_page_text="ARTICLE TITLE\nBy Jane Doe\n\nAbstract...",
        ),
        doc_policy="article_only",
        rules={},
    )
    assert decision.doc_type == "article"


# ---- Article self-TOC vs issue TOC (added 2026-08-06) ----
#
# `toc_marker` produced 99% of issue_compilation classifications in the
# 16-119pp bands and a hand-read sample of those was 0% real compilations:
# long articles open with their own contents outline. `footnote_marker_count`
# cannot guard this, because a page that IS a TOC has no footnote markers.

_ARTICLE_SELF_TOC = """
EMINENT DOMAIN LAW AND "JUST" COMPENSATION
Ashley Mas
TABLE OF CONTENTS
INTRODUCTION .................................................... 370
I. BACKGROUND ................................................. 372
A. The Just Compensation Mandate ....................... 372
II. THE SUBSTANTIAL IMPAIRMENT DOCTRINE .................... 380
III. ANALYSIS ................................................... 390
CONCLUSION ...................................................... 399
"""

_ISSUE_TOC = """
University of Hawai'i Law Review
Volume 33 / Number 1 / Winter 2010
TABLE OF CONTENTS
Remedies for the Wrongly Deported
Rachel E. Rosenbloom 139
Regression by Progression
Helia Garrido Hull 193
The Jones Act Fish Farmer
Timothy E. Steigelman 223
"""


def test_doc_policy_keeps_article_that_opens_with_its_own_contents() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/domain.example/mas-36-1-3.pdf",
        domain="domain.example",
        platform_family="custom_unknown",
        signals=_signals(page_count=32, first_page_text=_ARTICLE_SELF_TOC),
        doc_policy="article_only",
        rules={},
    )

    assert decision.doc_type == "article"
    assert decision.include is True
    assert "article_self_toc" in decision.reason_codes


def test_doc_policy_still_flags_a_real_issue_contents_page() -> None:
    decision = classify_pdf(
        pdf_path="/tmp/domain.example/33-u-haw-l-rev-issue-1.pdf",
        domain="domain.example",
        platform_family="custom_unknown",
        signals=_signals(page_count=88, first_page_text=_ISSUE_TOC),
        doc_policy="article_only",
        rules={},
    )

    assert decision.doc_type == "issue_compilation"
    assert "toc_marker" in decision.reason_codes


def test_article_self_toc_survives_pypdf_collapsing_the_toc_to_one_line() -> None:
    """pypdf often returns a whole contents block as a single unbroken line."""
    collapsed = (
        "THE SEC AND CRYPTOASSETS Carol Goforth* TABLE OF CONTENTS "
        "I.  Introduction ......... 56 II.  When do the Securities Laws Apply? "
        "..... 58 III.  How Has the SEC Said These Rules Apply? ..... 62"
    )
    decision = classify_pdf(
        pdf_path="/tmp/domain.example/116172-sec-and-cryptoassets.pdf",
        domain="domain.example",
        platform_family="custom_unknown",
        signals=_signals(page_count=52, first_page_text=collapsed),
        doc_policy="article_only",
        rules={},
    )

    assert decision.doc_type == "article"


def test_article_self_toc_tolerates_glyph_code_artifacts() -> None:
    """Some producers emit `I./G3/G3INTRODUCTION` between numeral and heading."""
    glyphy = (
        "LOOKING AT THE LANHAM ACT\nRebecca Tushnet\nTABLE OF CONTENTS\n"
        "I./G3/G3INTRODUCTION ......... 862/G3\n"
        "II./G3/G3TRADEMARK ........... 865/G3\n"
        "III./G3/G3CONCLUSION ......... 899/G3\n"
    )
    decision = classify_pdf(
        pdf_path="/tmp/domain.example/4143-looking-at-the-lanham-act.pdf",
        domain="domain.example",
        platform_family="custom_unknown",
        signals=_signals(page_count=57, first_page_text=glyphy),
        doc_policy="article_only",
        rules={},
    )

    assert decision.doc_type == "article"


def test_abstract_heading_marks_an_article_opening() -> None:
    text = (
        "TRADEMARK CONFUSION SIMPLIFIED\nDaryl Lim\n\nABSTRACT\n"
        "Multifactor tests are challenging for judges to apply consistently.\n"
        "... table of contents follows later in the document ...\n"
    )
    decision = classify_pdf(
        pdf_path="/tmp/domain.example/0005-37-2-lim-web.pdf",
        domain="domain.example",
        platform_family="custom_unknown",
        signals=_signals(page_count=72, first_page_text=text),
        doc_policy="article_only",
        rules={},
    )

    assert decision.doc_type == "article"


def test_long_compilations_are_unaffected_by_the_self_toc_escape() -> None:
    """>200pp still short-circuits before the TOC branch is ever reached."""
    decision = classify_pdf(
        pdf_path="/tmp/domain.example/volume-33-complete.pdf",
        domain="domain.example",
        platform_family="custom_unknown",
        signals=_signals(page_count=442, first_page_text=_ARTICLE_SELF_TOC),
        doc_policy="article_only",
        rules={},
    )

    assert decision.doc_type == "issue_compilation"
    assert "long_doc_page_count" in decision.reason_codes
