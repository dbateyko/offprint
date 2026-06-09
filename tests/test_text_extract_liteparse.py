from __future__ import annotations

from offprint.pdf_footnotes.text_extract import (
    ExtractedLine,
    _LiteparsePageLayout,
    _build_liteparse_candidate_document,
    _classify_liteparse_candidate_lines,
    _classify_liteparse_note_candidates,
    _cluster_words_to_lines,
    _detect_word_column_split,
    _low_variance_density_split,
    _text_fidelity_score_for_word_pages,
)


def _two_column_words(rows: int = 30) -> list[dict]:
    """Dense two-column page: left col 40-200, right col 240-400, gutter 200-240.

    Words tile each column contiguously (as real wrapped text does — no aligned
    intra-column gaps), with row-staggered start offsets so the only sustained
    empty vertical band is the true gutter. Word centres are continuous (no
    large center-to-center gap), the case the legacy center-gap detector misses
    — the projection profile must catch the empty gutter instead.
    """
    words: list[dict] = []
    for r in range(rows):
        top = 60.0 + r * 12.0
        jitter = (r % 3) * 4  # stagger so word boundaries don't align across rows
        for x0 in range(40 + jitter, 200, 16):  # left column, contiguous tiling
            words.append({"x0": float(x0), "x1": float(min(x0 + 16, 200)), "top": top,
                          "bottom": top + 10, "text": "w"})
        for x0 in range(240 + jitter, 400, 16):  # right column
            words.append({"x0": float(x0), "x1": float(min(x0 + 16, 400)), "top": top,
                          "bottom": top + 10, "text": "w"})
    return words


def test_projection_detects_dense_two_column_gutter() -> None:
    split = _detect_word_column_split(_two_column_words())
    assert split is not None
    # gutter sits between the columns (right edge of left col 200, left of right 240)
    assert 200.0 <= split <= 240.0


def test_projection_leaves_single_column_unsplit() -> None:
    # A single dense column spanning the page width must NOT be split.
    words = []
    for r in range(30):
        top = 60.0 + r * 12.0
        jitter = (r % 3) * 4
        for x0 in range(40 + jitter, 400, 16):
            words.append({"x0": float(x0), "x1": float(min(x0 + 16, 400)), "top": top,
                          "bottom": top + 10, "text": "w"})
    assert _detect_word_column_split(words) is None


def test_two_column_words_cluster_without_cross_column_mixing() -> None:
    # Each output line should come from one column only (monotone x within a line).
    lines = _cluster_words_to_lines(_two_column_words(), page_number=1)
    assert lines  # produced something
    # With the gutter detected, no single line should span both columns: the
    # left and right column rows are emitted as separate lines, so line count
    # is ~2x the row count rather than 1x.
    assert len(lines) >= 40


def _line(text: str, top: float, *, size: float = 10.0) -> ExtractedLine:
    return ExtractedLine(
        text=text,
        page_number=4,
        top=top,
        bottom=top + size,
        font_size=size,
        source="liteparse",
    )


def test_liteparse_low_variance_density_rail_finds_bottom_notes() -> None:
    lines = [
        _line("This paragraph introduces the article and its framework.", 90),
        _line("The court's analysis turned on institutional design.", 140),
        _line("A second paragraph develops the factual background.", 190),
        _line("The statutory analysis continues without a note marker.", 240),
        _line("The next section discusses remedies and policy costs.", 290),
        _line("The final body paragraph closes above the note band.", 340),
        _line("1 See Smith v. Jones, 123 U.S. 456 (1999).", 620),
        _line("2 Id. at 460.", 650),
        _line("3 See also Brown v. Board, 347 U.S. 483 (1954).", 680),
    ]

    body, notes, used_custom = _classify_liteparse_note_candidates(lines, page_height=800)

    assert used_custom is True
    assert [line.text for line in notes] == [
        "1 See Smith v. Jones, 123 U.S. 456 (1999).",
        "2 Id. at 460.",
        "3 See also Brown v. Board, 347 U.S. 483 (1954).",
    ]
    assert body[-1].text == "The final body paragraph closes above the note band."


def test_liteparse_low_variance_density_rail_ignores_bottom_prose() -> None:
    lines = [
        _line("This paragraph introduces the article and its framework.", 90),
        _line("The court's analysis turned on institutional design.", 140),
        _line("A second paragraph develops the factual background.", 190),
        _line("The statutory analysis continues without a note marker.", 240),
        _line("The next section discusses remedies and policy costs.", 290),
        _line("The final body paragraph closes the page.", 620),
        _line("This bottom paragraph cites Smith v. Jones in prose.", 650),
        _line("It also refers to 42 U.S.C. section 1983.", 680),
    ]

    assert _low_variance_density_split(lines, page_height=800) is None


def test_liteparse_pattern_density_strict_finds_content_only_note_band() -> None:
    lines = [
        _line("The majority opinion then turns to institutional competence.", 110),
        _line("This page includes ordinary body prose above the footnotes.", 165),
        _line("1 See Smith v. Jones, 123 U.S. 456 (1999).", 430),
        _line("2 Id. at 460.", 455),
        _line("3 See also Brown v. Board, 347 U.S. 483 (1954).", 480),
        _line("4 Cf. Roe v. Wade, 410 U.S. 113 (1973).", 505),
        _line("5 But see 42 U.S.C. section 1983.", 530),
        _line("6 Supra note 2.", 555),
    ]

    body, notes, used_custom = _classify_liteparse_candidate_lines(
        lines,
        page_height=800,
        candidate_name="pattern_density_strict",
    )

    assert used_custom is True
    assert body[-1].text.startswith("This page includes")
    assert [line.text.split()[0] for line in notes] == ["1", "2", "3", "4", "5", "6"]


def test_liteparse_liberal_notes_keeps_large_heading_in_body() -> None:
    lines = [
        _line("The article begins with ordinary body text.", 120, size=11),
        _line("II. REMEDIES AND ADMINISTRATION", 340, size=14),
        _line("1 See Smith v. Jones, 123 U.S. 456 (1999).", 430, size=10),
        _line("2 Id. at 460.", 460, size=10),
        _line("3 See also Brown v. Board, 347 U.S. 483 (1954).", 490, size=10),
    ]

    body, notes, used_custom = _classify_liteparse_candidate_lines(
        lines,
        page_height=800,
        candidate_name="liberal_notes",
    )

    assert used_custom is True
    assert "II. REMEDIES AND ADMINISTRATION" in [line.text for line in body]
    assert [line.text.split()[0] for line in notes] == ["1", "2", "3"]


def _word(text: str, x0: float, top: float) -> dict[str, float | str]:
    return {
        "text": text,
        "x0": x0,
        "x1": x0 + 30,
        "top": top,
        "bottom": top + 10,
        "size": 10.0,
    }


def test_cluster_words_sorts_words_by_x_within_line() -> None:
    words = [
        _word("world", 100, 100),
        _word("Hello", 50, 101),
    ]

    lines = _cluster_words_to_lines(words, page_number=1)

    assert [line.text for line in lines] == ["Hello world"]


def test_cluster_words_keeps_clear_columns_separate_on_same_y_band() -> None:
    words = [
        _word("1", 50, 600),
        _word("See", 85, 600),
        _word("left", 120, 600),
        _word("note.", 160, 600),
        _word("continues", 55, 622),
        _word("with", 120, 622),
        _word("more", 160, 622),
        _word("text.", 205, 622),
        _word("2", 360, 600),
        _word("See", 395, 600),
        _word("right", 430, 600),
        _word("note.", 480, 600),
        _word("continues", 365, 622),
        _word("with", 430, 622),
        _word("more", 470, 622),
        _word("text.", 515, 622),
    ]

    lines = _cluster_words_to_lines(words, page_number=1)

    assert [line.text for line in lines] == [
        "1 See left note.",
        "continues with more text.",
        "2 See right note.",
        "continues with more text.",
    ]


def test_cluster_words_keeps_dense_spanning_line_single_column() -> None:
    words = [
        _word("This", 50, 600),
        _word("footnote", 95, 600),
        _word("runs", 140, 600),
        _word("across", 185, 600),
        _word("the", 230, 600),
        _word("middle", 275, 600),
        _word("without", 320, 600),
        _word("a", 365, 600),
        _word("column", 410, 600),
        _word("gap.", 455, 600),
    ]

    lines = _cluster_words_to_lines(words, page_number=1)

    assert [line.text for line in lines] == [
        "This footnote runs across the middle without a column gap."
    ]


def test_text_fidelity_score_uses_column_proxy_for_coordinate_pages() -> None:
    words = [
        _word("1", 50, 600),
        _word("See", 85, 600),
        _word("left", 120, 600),
        _word("note.", 160, 600),
        _word("continues", 55, 622),
        _word("with", 120, 622),
        _word("more", 160, 622),
        _word("text.", 205, 622),
        _word("2", 360, 600),
        _word("See", 395, 600),
        _word("right", 430, 600),
        _word("note.", 480, 600),
        _word("continues", 365, 622),
        _word("with", 430, 622),
        _word("more", 470, 622),
        _word("text.", 515, 622),
    ]

    assert _text_fidelity_score_for_word_pages([words]) == 1.0


def test_text_fidelity_score_penalizes_unresolved_cross_column_band() -> None:
    words = [
        _word("1", 50, 600),
        _word("See", 85, 600),
        _word("left", 120, 600),
        _word("note.", 160, 600),
        _word("2", 360, 600),
        _word("See", 395, 600),
        _word("right", 430, 600),
        _word("note.", 480, 600),
    ]

    assert _text_fidelity_score_for_word_pages([words]) == 0.0


def test_liteparse_candidate_document_attaches_text_fidelity_score_metadata() -> None:
    words = [
        _word("Body", 50, 100),
        _word("text.", 90, 100),
        _word("1", 50, 600),
        _word("See", 85, 600),
        _word("left", 120, 600),
        _word("note.", 160, 600),
        _word("continues", 55, 622),
        _word("with", 120, 622),
        _word("more", 160, 622),
        _word("text.", 205, 622),
        _word("2", 360, 600),
        _word("See", 395, 600),
        _word("right", 430, 600),
        _word("note.", 480, 600),
        _word("continues", 365, 622),
        _word("with", 430, 622),
        _word("more", 470, 622),
        _word("text.", 515, 622),
    ]
    raw_items = tuple(
        {
            "text": word["text"],
            "x": word["x0"],
            "y": word["top"],
            "width": float(word["x1"]) - float(word["x0"]),
            "height": float(word["bottom"]) - float(word["top"]),
            "fontSize": word["size"],
        }
        for word in words
    )
    layout = _LiteparsePageLayout(
        page_number=1,
        width=600,
        height=800,
        raw_text="Body text.\n1 See left note.\n2 See right note.",
        lines=tuple(_cluster_words_to_lines(words, page_number=1)),
        raw_items=raw_items,
    )

    document = _build_liteparse_candidate_document(
        "fixture.pdf",
        [layout],
        candidate_name="default",
    )

    assert document.metadata["text_fidelity_score"] == 1.0
    assert document.metadata["text_fidelity_score_method"] == "page_y_band_column_proxy_v1"
