"""Tests for the sequence-solver footnote extractor."""
from __future__ import annotations

from offprint.pdf_footnotes.note_segment import validate_ordinality
from offprint.pdf_footnotes.sequence_solver import (
    LabelCandidate,
    SolverResult,
    _collect_candidates,
    _pages_from_layouts,
    build_note_records,
    solve_document,
)
from offprint.pdf_footnotes.text_extract import _LiteparsePageLayout, ExtractedLine


def _mk_line(text: str, page: int, top: float, font_size: float = 9.0) -> ExtractedLine:
    return ExtractedLine(
        text=text, page_number=page, top=top, bottom=top + 10, font_size=font_size
    )


def _mk_layout(page: int, lines, items) -> _LiteparsePageLayout:
    return _LiteparsePageLayout(
        page_number=page,
        width=612.0,
        height=792.0,
        raw_text="",
        lines=tuple(lines),
        raw_items=tuple(items),
    )


def _candidate_values(layouts) -> list[int]:
    return [c.digit_value for c in _collect_candidates(_pages_from_layouts(layouts))]


def test_build_note_records_constructs_note_spans_from_selected_candidates():
    # Two pages, three footnotes. Page 1 body ends at y=400; notes start at y=500.
    layout1 = _mk_layout(
        1,
        lines=[
            _mk_line("Body text on page 1 about something important", 1, 200, 11),
            _mk_line("1. See, e.g., Smith v. Jones, 123 U.S. 456 (2020).", 1, 500, 9),
            _mk_line("Continuation of note 1 text here.", 1, 513, 9),
            _mk_line("2. Id. at 460.", 1, 527, 9),
        ],
        items=[
            {"text": "1.", "x": 72, "y": 500, "width": 10, "height": 10, "fontSize": 9},
            {"text": "2.", "x": 72, "y": 527, "width": 10, "height": 10, "fontSize": 9},
            {"text": "Body", "x": 100, "y": 200, "width": 20, "height": 10, "fontSize": 11},
        ],
    )
    layout2 = _mk_layout(
        2,
        lines=[
            _mk_line("More body text about the topic continuing", 2, 200, 11),
            _mk_line("3. See Brown v. Board, 347 U.S. 483 (1954).", 2, 600, 9),
        ],
        items=[
            {"text": "3.", "x": 72, "y": 600, "width": 10, "height": 10, "fontSize": 9},
            {"text": "Body", "x": 100, "y": 200, "width": 20, "height": 10, "fontSize": 11},
        ],
    )

    result = solve_document([layout1, layout2])
    assert result.selected_labels == [1, 2, 3]

    notes, author_notes, ordinality = build_note_records([layout1, layout2], result)
    assert ordinality is not None
    assert ordinality.status == "valid"
    assert ordinality.expected_range == (1, 3)
    assert ordinality.gaps == []
    assert len(notes) == 3
    assert notes[0].label == "1"
    assert notes[0].page_start == 1
    assert notes[1].label == "2"
    assert notes[2].label == "3"
    assert notes[2].page_start == 2
    # The leading "1." / "2." / "3." glyph should be stripped from the text.
    assert not notes[0].text.startswith("1.")
    assert not notes[1].text.startswith("2.")
    assert not notes[2].text.startswith("3.")
    # Author notes unused by solver path.
    assert author_notes == []


def test_solve_document_accepts_dense_start_at_2_when_label_1_is_lost():
    """Label 1 sometimes renders as a corrupted glyph ('1' → "'", a dash, or
    gets merged into a larger token) and drops out of the candidate pool.
    When the remaining sequence starts at 2 and is dense + long, accept it.
    """
    pages = []
    # 8 footnotes starting at 2 across 2 pages, densely packed at y=500+
    layout_lines_p1 = [
        _mk_line("Body text on page 1", 1, 100, 11),
    ]
    layout_items_p1 = [
        {"text": "Body", "x": 72, "y": 100, "width": 20, "height": 10, "fontSize": 11},
    ]
    y = 500
    for lbl in range(2, 6):  # 2, 3, 4, 5 on page 1
        layout_lines_p1.append(
            _mk_line(f"{lbl}. See some citation text here with words.", 1, y, 9)
        )
        layout_items_p1.append(
            {"text": f"{lbl}.", "x": 72, "y": y, "width": 10, "height": 10, "fontSize": 9}
        )
        layout_items_p1.append(
            {"text": "See", "x": 90, "y": y, "width": 20, "height": 10, "fontSize": 9}
        )
        layout_items_p1.append(
            {"text": "some citation text here with words.",
             "x": 115, "y": y, "width": 200, "height": 10, "fontSize": 9}
        )
        y += 13
    pages.append(_mk_layout(1, layout_lines_p1, layout_items_p1))

    layout_lines_p2 = [_mk_line("Body text on page 2", 2, 100, 11)]
    layout_items_p2 = [
        {"text": "Body", "x": 72, "y": 100, "width": 20, "height": 10, "fontSize": 11},
    ]
    y = 400
    for lbl in range(6, 10):  # 6, 7, 8, 9 on page 2
        layout_lines_p2.append(
            _mk_line(f"{lbl}. See another citation here with more words.", 2, y, 9)
        )
        layout_items_p2.append(
            {"text": f"{lbl}.", "x": 72, "y": y, "width": 10, "height": 10, "fontSize": 9}
        )
        layout_items_p2.append(
            {"text": "See", "x": 90, "y": y, "width": 20, "height": 10, "fontSize": 9}
        )
        layout_items_p2.append(
            {"text": "another citation here with more words.",
             "x": 115, "y": y, "width": 200, "height": 10, "fontSize": 9}
        )
        y += 13
    pages.append(_mk_layout(2, layout_lines_p2, layout_items_p2))

    result = solve_document(pages)
    assert result.selected_labels, "solver should not abstain on a dense start-at-2 sequence"
    assert result.selected_labels[0] == 2
    assert result.selected_labels[-1] == 9
    # Contiguous, so no gaps
    assert result.selected_labels == list(range(2, 10))


def test_collect_candidates_synthesizes_split_label_166():
    layout = _mk_layout(
        1,
        lines=[_mk_line("1 66 See authority explaining the point.", 1, 500, 9)],
        items=[
            {"text": "1", "x": 72, "y": 500, "width": 5, "height": 10, "fontSize": 9},
            {"text": "66", "x": 78, "y": 500, "width": 11, "height": 10, "fontSize": 9},
            {"text": "See", "x": 96, "y": 500, "width": 18, "height": 10, "fontSize": 9},
            {"text": "authority explaining the point.", "x": 118, "y": 500, "width": 150, "height": 10, "fontSize": 9},
        ],
    )
    # The synthetic-label cap follows max(120, page_count * 10), so include
    # enough pages to make label 166 plausible for the document.
    empty_tail = [_mk_layout(page, [], []) for page in range(2, 18)]

    values = _candidate_values([layout, *empty_tail])

    assert 166 in values


def test_solve_document_uses_split_166_to_close_sequence_gap():
    pages = []
    label = 1
    for page in range(1, 18):
        lines = [_mk_line(f"Body text page {page}", page, 80, 11)]
        items = [
            {"text": "Body", "x": 72, "y": 80, "width": 24, "height": 10, "fontSize": 11}
        ]
        for offset in range(10):
            if label > 167:
                break
            y = 200 + offset * 13
            if label == 166:
                lines.append(_mk_line("1 66 See authority explaining the point.", page, y, 9))
                items.extend(
                    [
                        {"text": "1", "x": 72, "y": y, "width": 5, "height": 10, "fontSize": 9},
                        {"text": "66", "x": 78, "y": y, "width": 11, "height": 10, "fontSize": 9},
                        {"text": "See", "x": 96, "y": y, "width": 18, "height": 10, "fontSize": 9},
                        {
                            "text": "authority explaining the point.",
                            "x": 118,
                            "y": y,
                            "width": 150,
                            "height": 10,
                            "fontSize": 9,
                        },
                    ]
                )
            else:
                lines.append(_mk_line(f"{label}. See authority explaining the point.", page, y, 9))
                items.extend(
                    [
                        {
                            "text": f"{label}.",
                            "x": 72,
                            "y": y,
                            "width": max(10, len(str(label)) * 6 + 3),
                            "height": 10,
                            "fontSize": 9,
                        },
                        {"text": "See", "x": 96, "y": y, "width": 18, "height": 10, "fontSize": 9},
                        {
                            "text": "authority explaining the point.",
                            "x": 118,
                            "y": y,
                            "width": 150,
                            "height": 10,
                            "fontSize": 9,
                        },
                    ]
                )
            label += 1
        pages.append(_mk_layout(page, lines, items))

    result = solve_document(pages)

    assert result.selected_labels == list(range(1, 168))


def test_collect_candidates_synthesizes_split_label_87():
    layout = _mk_layout(
        1,
        lines=[_mk_line("8 7 See authority explaining the point.", 1, 500, 9)],
        items=[
            {"text": "8", "x": 72, "y": 500, "width": 5, "height": 10, "fontSize": 9},
            {"text": "7", "x": 78, "y": 500, "width": 5, "height": 10, "fontSize": 9},
            {"text": "See", "x": 90, "y": 500, "width": 18, "height": 10, "fontSize": 9},
            {"text": "authority explaining the point.", "x": 112, "y": 500, "width": 150, "height": 10, "fontSize": 9},
        ],
    )

    values = _candidate_values([layout])

    assert 87 in values


def test_collect_candidates_does_not_synthesize_separated_or_implausible_digits():
    different_y = _mk_layout(
        1,
        lines=[],
        items=[
            {"text": "8", "x": 72, "y": 500, "width": 5, "height": 10, "fontSize": 9},
            {"text": "7", "x": 78, "y": 506, "width": 5, "height": 10, "fontSize": 9},
        ],
    )
    large_gap = _mk_layout(
        1,
        lines=[],
        items=[
            {"text": "8", "x": 72, "y": 500, "width": 5, "height": 10, "fontSize": 9},
            {"text": "7", "x": 120, "y": 500, "width": 5, "height": 10, "fontSize": 9},
            {"text": "See", "x": 132, "y": 500, "width": 18, "height": 10, "fontSize": 9},
        ],
    )
    implausible = _mk_layout(
        1,
        lines=[],
        items=[
            {"text": "9", "x": 72, "y": 500, "width": 5, "height": 10, "fontSize": 9},
            {"text": "99", "x": 78, "y": 500, "width": 11, "height": 10, "fontSize": 9},
            {"text": "See", "x": 96, "y": 500, "width": 18, "height": 10, "fontSize": 9},
        ],
    )

    assert 87 not in _candidate_values([different_y])
    assert 87 not in _candidate_values([large_gap])
    assert 999 not in _candidate_values([implausible])


def test_gap_fill_prefers_high_score_column_aligned_over_low_score_near_bracket():
    """Regression: the gap-fill pool used to short-circuit on the first
    non-empty sublist (strict→near→column_anywhere), which meant a low-score
    `near` candidate would shadow a high-score column-aligned candidate on a
    different page. Now all three sources union into a single pool and the
    highest score wins.
    """
    import offprint.pdf_footnotes.sequence_solver as ss

    # Selected path: 3 candidates at x=144 on page 2, then 1 at x=144 on
    # page 5, forming a dense column at x=144. Gap: label 4.
    selected_cands = [
        LabelCandidate(page=2, y=500, x=144, font_size=6, digit_value=1, text="1.",
                       has_punct=True, left_margin=True, cluster_peers=3),
        LabelCandidate(page=2, y=513, x=144, font_size=6, digit_value=2, text="2.",
                       has_punct=True, left_margin=True, cluster_peers=3),
        LabelCandidate(page=2, y=527, x=144, font_size=6, digit_value=3, text="3.",
                       has_punct=True, left_margin=True, cluster_peers=3),
        LabelCandidate(page=2, y=540, x=144, font_size=6, digit_value=5, text="5.",
                       has_punct=True, left_margin=True, cluster_peers=3),
    ]
    # Label 4: low-score candidate on a near-bracket page (p3) AND
    # high-score candidate column-aligned (x=144, peers=3) on a far page (p9).
    # Without the union fix, near would shadow column_anywhere.
    low_near = LabelCandidate(
        page=3, y=100, x=400, font_size=10, digit_value=4, text="4",
        is_pure_digit=True, has_punct=False, left_margin=False,
        cluster_peers=0, citation_nearby=False, substantive_text=False,
    )
    high_far_aligned = LabelCandidate(
        page=9, y=600, x=144, font_size=6, digit_value=4, text="4.",
        has_punct=True, left_margin=True, cluster_peers=3,
        citation_nearby=True, substantive_text=True,
    )
    cands = selected_cands + [low_near, high_far_aligned]
    selected_idx = [0, 1, 2, 3]  # 1, 2, 3, 5
    result = ss._gap_fill(cands, selected_idx)
    result_digits = sorted(cands[i].digit_value for i in result)
    assert 4 in result_digits
    # Of the two 4-candidates, the high-score column-aligned one should have
    # won; check by inspecting which cand index made it in.
    added = set(result) - set(selected_idx)
    assert added == {cands.index(high_far_aligned)}


def test_solve_document_rejects_short_start_at_2_sequence():
    """A short sequence starting at 2 (3 labels) should be rejected — too
    likely to be a stray or non-article artifact."""
    layout_lines = [
        _mk_line("Body", 1, 100, 11),
        _mk_line("2. first note", 1, 500, 9),
        _mk_line("3. second note", 1, 513, 9),
        _mk_line("4. third note", 1, 527, 9),
    ]
    layout_items = [
        {"text": "Body", "x": 72, "y": 100, "width": 20, "height": 10, "fontSize": 11},
        {"text": "2.", "x": 72, "y": 500, "width": 10, "height": 10, "fontSize": 9},
        {"text": "3.", "x": 72, "y": 513, "width": 10, "height": 10, "fontSize": 9},
        {"text": "4.", "x": 72, "y": 527, "width": 10, "height": 10, "fontSize": 9},
    ]
    layout = _mk_layout(1, layout_lines, layout_items)
    result = solve_document([layout])
    # len < 5 → abstain even though density is 1.0
    assert result.selected_labels == []


def test_build_note_records_empty_when_solver_abstains():
    layout = _mk_layout(
        1,
        lines=[_mk_line("Just body text", 1, 200, 11)],
        items=[{"text": "Body", "x": 100, "y": 200, "width": 20, "height": 10, "fontSize": 11}],
    )
    result = solve_document([layout])
    notes, author_notes, ordinality = build_note_records([layout], result)
    # No candidates → solver abstains → empty output.
    assert notes == []
    assert author_notes == []
    assert ordinality is None


def test_build_note_records_records_ordinality_status_with_gaps():
    # Selected candidates with a gap at label 2.
    cand1 = LabelCandidate(page=1, y=500, x=72, font_size=9, digit_value=1, text="1.")
    cand3 = LabelCandidate(page=1, y=527, x=72, font_size=9, digit_value=3, text="3.")
    cand4 = LabelCandidate(page=1, y=540, x=72, font_size=9, digit_value=4, text="4.")
    result = SolverResult(
        page_cutoffs={1: 500.0},
        selected_labels=[1, 3, 4],
        selected_candidates=(cand1, cand3, cand4),
        candidate_count=3,
    )
    layout = _mk_layout(
        1,
        lines=[
            _mk_line("1. Note one text.", 1, 500, 9),
            _mk_line("3. Note three text.", 1, 527, 9),
            _mk_line("4. Note four text.", 1, 540, 9),
        ],
        items=[],
    )
    notes, _, ordinality = build_note_records([layout], result)
    assert ordinality is not None
    assert ordinality.expected_range == (1, 4)
    assert ordinality.gaps == [2]
    # 1 gap on a short sequence falls under the floor of max(2, 2% of range)
    # → valid_with_gaps rather than invalid. Longer-sequence gap behavior is
    # covered implicitly by the 1K benchmark.
    assert ordinality.status == "valid_with_gaps"
    assert len(notes) == 3
    assert [n.label for n in notes] == ["1", "3", "4"]


def test_validate_ordinality_ratio_relief_keeps_long_sparse_gaps_as_valid_with_gaps():
    """A 400-note article with 5 gaps (~1 %) used to flip to "invalid" because
    the absolute gap_tolerance=2 was exceeded. The ratio-relief branch keeps
    such docs in valid_with_gaps — their extraction quality is obviously high.
    """
    nums = list(range(1, 401))
    # Remove 5 arbitrary labels to simulate minor extraction losses.
    for missing in (17, 112, 203, 298, 355):
        nums.remove(missing)
    report = validate_ordinality(nums, gap_tolerance=2)
    assert len(report.gaps) == 5
    assert report.gap_ratio < 0.02
    assert report.tolerance_exceeded is True
    assert report.status == "valid_with_gaps"


def test_validate_ordinality_high_ratio_stays_invalid():
    """A 20-note sequence with 10 gaps (50 %) should remain invalid —
    ratio-relief only rescues low-density-gap cases."""
    nums = [1, 2, 5, 6, 9, 10, 13, 14, 17, 18]
    report = validate_ordinality(nums, gap_tolerance=2)
    assert report.gap_ratio > 0.35
    assert report.status == "invalid"


def test_validate_ordinality_clean_sequence_still_valid():
    """Ratio relief must not demote strict-valid sequences."""
    report = validate_ordinality(list(range(1, 50)), gap_tolerance=2)
    assert report.status == "valid"
    assert report.gaps == []
