from __future__ import annotations

from offprint.pdf_footnotes.ocr_footnote_extract import (
    _leading_label,
    _split_page,
    extract_ocr_footnotes,
)


def test_leading_label_skips_toc_leaders() -> None:
    # Real footnote block line.
    assert _leading_label("49 See infra Part III.") == 49
    assert _leading_label("12. Smith v. Jones, 1 U.S. 1 (2020).") == 12
    # Table-of-contents entry (dotted leader + page number) is not a footnote.
    assert _leading_label("Understanding Agency Deference ............ 77") is None
    # Body prose with no leading number.
    assert _leading_label("The agency approved the drug.") is None


def test_split_page_separates_body_from_bottom_block() -> None:
    page = [
        "The FDA approved the drug despite limited data.49 The agency later",
        "revoked that approval.50",
        "49 See 21 U.S.C. § 355.",
        "50 Id. at § 355(d).",
    ]
    body, note = _split_page(page, gap_tolerance=4)
    assert body == page[:2]
    assert note == page[2:]


def test_split_page_all_body_when_no_block() -> None:
    page = ["Pure prose with an inline ref.51", "More prose continuing the point."]
    body, note = _split_page(page, gap_tolerance=4)
    assert body == page
    assert note == []


def _ocr_pages() -> list[dict]:
    """A 4-page scan: TOC, two footnote pages, and one page whose block olmOCR
    dropped (inline refs present, no bottom block)."""
    return [
        {"type": "doc", "pdf_path": "x.pdf", "page_count": 4},
        # p1: table of contents — must not yield notes.
        {"type": "page", "page": 1, "text": (
            "TABLE OF CONTENTS\n"
            "Introduction ............................................. 1\n"
            "I. Background ............................................ 5\n"
        )},
        # p2: body + footnote block 1-3.
        {"type": "page", "page": 2, "text": (
            "The statute is ambiguous.1 Courts have long deferred to the agency.2 "
            "That deference is now contested.3\n"
            "1 Chevron U.S.A., Inc. v. NRDC, 467 U.S. 837 (1984).\n"
            "2 See id. at 842-43.\n"
            "3 Loper Bright Enters. v. Raimondo, 603 U.S. 369 (2024).\n"
        )},
        # p3: block 4-5 dropped by OCR (inline refs in body, no bottom block).
        {"type": "page", "page": 3, "text": (
            "The agency issued new guidance.4 It then revised the rule.5 "
            "The change drew comment.\n"
        )},
        # p4: body + footnote block 6-7 (sequence resumes after the dropped block).
        {"type": "page", "page": 4, "text": (
            "Petitioners challenged the rule.6 The court remanded.7\n"
            "6 See 5 U.S.C. § 706.\n"
            "7 Motor Vehicle Mfrs. Ass'n v. State Farm, 463 U.S. 29 (1983).\n"
        )},
    ]


def test_extract_recovers_blocks_and_flags_ocr_drops() -> None:
    res = extract_ocr_footnotes(_ocr_pages())
    labels = sorted(int(n.label) for n in res.notes if n.label.isdigit())
    # Recovers every block olmOCR emitted: 1,2,3,6,7 (4,5 were dropped upstream).
    assert labels == [1, 2, 3, 6, 7]
    # The TOC did not leak in as notes, and there are no duplicate labels.
    assert len(labels) == len(set(labels))
    # The dropped block is surfaced as an OCR-recall gap, not silently lost.
    assert res.dropped_labels == [4, 5]
    assert res.pages_with_block == 2


def test_extract_footnote_text_excludes_body_prose() -> None:
    res = extract_ocr_footnotes(_ocr_pages())
    note1 = next(n for n in res.notes if n.label == "1")
    # The note carries its citation, not the body sentence that referenced it.
    assert "Chevron" in note1.text
    assert "statute is ambiguous" not in note1.text


def test_extract_handles_empty_input() -> None:
    res = extract_ocr_footnotes([{"type": "doc"}])
    assert res.notes == []
    assert res.dropped_labels == []
