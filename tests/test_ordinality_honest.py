"""Regression tests pinning the *honest* ordinality bucketing.

The strict-valid metric must mean "zero gaps" by default — near-miss sequences
belong in valid_with_gaps, not promoted into strict-valid. These tests guard
against silently re-enabling the cosmetic-gap promotion that inflated the
headline quality number.
"""

from offprint.pdf_footnotes.note_segment import (
    COSMETIC_GAP_MAX,
    COSMETIC_GAP_MIN_NOTES,
    validate_ordinality,
)


def test_zero_gap_is_strict_valid():
    assert validate_ordinality(list(range(1, 21))).status == "valid"


def test_near_miss_is_not_strict_valid_by_default():
    # 18 notes missing label 4 (1 gap, >=10-note stream): historically promoted
    # to strict "valid". The honest default keeps it in valid_with_gaps.
    labels = [n for n in range(1, 19) if n != 4]
    rep = validate_ordinality(labels, gap_tolerance=2)
    assert rep.gaps == [4]
    assert rep.status == "valid_with_gaps", (
        "near-miss must NOT count as strict-valid by default"
    )


def test_promotion_opt_in_reproduces_old_behavior():
    labels = [n for n in range(1, 19) if n != 4]
    rep = validate_ordinality(labels, gap_tolerance=2, promote_cosmetic_gaps=True)
    assert rep.status == "valid", "explicit opt-in should restore promotion"


def test_short_sequence_never_promoted_even_when_opted_in():
    # Below COSMETIC_GAP_MIN_NOTES: promotion does not apply regardless.
    labels = [1, 3, 4]  # 3 notes, 1 gap
    rep = validate_ordinality(labels, gap_tolerance=2, promote_cosmetic_gaps=True)
    assert len(labels) < COSMETIC_GAP_MIN_NOTES
    assert rep.status == "valid_with_gaps"


def test_too_many_gaps_over_ratio_relief_is_invalid():
    # A short stream where gaps dominate the span -> invalid (ratio relief does
    # not rescue it).
    labels = [1, 10]  # span 10, 8 gaps, ratio 0.8 >> 0.15
    rep = validate_ordinality(labels, gap_tolerance=2)
    assert rep.status == "invalid"


def test_cosmetic_constants_are_what_the_metric_assumes():
    # Pin the knobs so a silent change to them trips this test.
    assert COSMETIC_GAP_MAX == 2
    assert COSMETIC_GAP_MIN_NOTES == 10
