"""Tests for the TOC-driven boundary solver.

The fixtures are synthetic issues built from the layouts that actually appear in
the corpus: a contents listing with dot leaders, a folio stream offset from the
physical page, running heads that arrive a page or two late, and the traps that
cost the previous splitter precision (in-piece contents pages, faculty rosters,
a head that shouts like a display title).
"""

from __future__ import annotations

import pytest

from offprint.pdf_footnotes import toc_solver as T


# ---------------------------------------------------------------------------
# Synthetic issue
# ---------------------------------------------------------------------------

ARTICLES = [
    ("Remedies for the Wrongly Deported", "Rachel E. Rosenbloom", 139, 20),
    ("Monuments, Law and Cultural Transformation", "Kenneth B. Nunn", 159, 24),
    ("The Case Against Juvenile Confinement", "Migueyli Aisha Duran", 183, 18),
]
OFFSET = 134  # printed = physical + OFFSET; article 1 opens on physical page 5


def _toc_page() -> str:
    lines = ["HOWARD LAW JOURNAL", "TABLE OF CONTENTS", "ARTICLES"]
    for title, author, printed, _ in ARTICLES:
        lines.append(f"{title} . . . . . . . . . . . . . {author} {printed}")
    return "\n".join(lines)


def _opening_page(title: str, author: str, printed: int) -> str:
    return "\n".join(
        [
            title,
            author + "*",
            "Introduction",
            "The question this Article takes up is one the courts have avoided for",
            "three decades, and the answer bears directly on the remedy available.",
            "1. See infra Part II.",
            str(printed),
        ]
    )


def _continuation_page(short_title: str, printed: int, recto: bool) -> str:
    head = f"2025] {short_title} {printed}" if recto else f"{printed} HOWARD LAW JOURNAL [vol. 68:3"
    return "\n".join(
        [
            head,
            "relocating and taking on a new allegiance to a legal order that had",
            "already declined to recognise the claim, which is the point the court",
            "missed when it read the statute as jurisdictional in character.",
            str(printed),
        ]
    )


def build_issue(extra_pages: dict[int, str] | None = None) -> list[T.Page]:
    """Front matter, contents, then three articles with late-arriving heads."""
    texts = ["COVER", "MASTHEAD\nEditor in Chief\nJane Q. Editor", _toc_page(), ""]
    for title, author, printed, length in ARTICLES:
        texts.append(_opening_page(title, author, printed))
        short = title.upper()[:30]
        for step in range(1, length):
            texts.append(_continuation_page(short, printed + step, recto=step % 2 == 1))
    if extra_pages:
        for index, text in extra_pages.items():
            texts[index - 1] = text
    return T.pages_from_texts(texts)


# ---------------------------------------------------------------------------
# Contents parsing
# ---------------------------------------------------------------------------


def test_toc_entries_carry_title_author_and_section():
    entries = T.parse_toc_entries(build_issue())
    assert [entry.printed_page for entry in entries] == [139, 159, 183]
    assert entries[0].title == "Remedies for the Wrongly Deported"
    assert entries[0].author == "Rachel E. Rosenbloom"
    assert entries[0].section == "ARTICLES"
    assert entries[1].surnames == ["Nunn"]


def test_front_matter_entries_are_dropped():
    pages = T.pages_from_texts(
        [
            "CONTENTS\n"
            "Table of Contents . . . . . . . . . . . 3\n"
            "Editorial Board . . . . . . . . . . . 5\n"
            "Remedies for the Wrongly Deported . . . . . Rachel E. Rosenbloom 139\n"
            "Monuments and Law . . . . . . . . . . Kenneth B. Nunn 159"
        ]
    )
    entries = T.parse_toc_entries(pages)
    assert [entry.printed_page for entry in entries] == [139, 159]


def test_non_monotonic_entries_are_reduced_to_an_increasing_run():
    pages = T.pages_from_texts(
        [
            "CONTENTS\n"
            "Remedies for the Wrongly Deported . . . . . Rachel E. Rosenbloom 139\n"
            "Stray Line Amended in 2019 . . . . . . . . . 12\n"
            "Monuments and Law . . . . . . . . . . Kenneth B. Nunn 159\n"
            "The Case Against Confinement . . . . . Migueyli Aisha Duran 183"
        ]
    )
    entries = T.parse_toc_entries(pages)
    assert [entry.printed_page for entry in entries] == [139, 159, 183]


def test_qualified_section_headers_are_not_read_as_authors():
    assert T._section_name("SPECIAL GUEST REMARKS") == "SPECIAL GUEST REMARKS"
    assert T._section_name("STUDENT NOTES") == "STUDENT NOTES"
    assert T._section_name("Rachel E. Rosenbloom") == ""


# ---------------------------------------------------------------------------
# Folio stream
# ---------------------------------------------------------------------------


def test_offset_is_fitted_from_the_incrementing_stream():
    evidence = T.build_page_evidence(build_issue())
    fit = T.estimate_folio_offset(evidence, start_index=5)
    assert fit.offset == OFFSET
    assert fit.support > 0.9
    assert fit.longest_run >= 20


def test_a_constant_number_does_not_win_the_offset_fit():
    """A volume number in every head votes for an offset too -- and loses.

    It does not increment with the physical page, so its run length is 1 while
    the true folio stream runs the length of the issue. This is the difference
    between selecting a number because it is a number and selecting the stream.
    """
    texts = [f"HOWARD LAW JOURNAL 68\nbody text for page {index}\n{index + 100}" for index in range(1, 61)]
    evidence = T.build_page_evidence(T.pages_from_texts(texts))
    fit = T.estimate_folio_offset(evidence, start_index=1)
    assert fit.offset == 100


def test_weak_folio_stream_is_reported_as_no_offset():
    texts = ["nothing numeric here at all, just prose that runs on" for _ in range(40)]
    evidence = T.build_page_evidence(T.pages_from_texts(texts))
    fit = T.estimate_folio_offset(evidence, start_index=1)
    assert fit.offset is None


# ---------------------------------------------------------------------------
# Assignment
# ---------------------------------------------------------------------------


def test_solver_places_every_article_on_its_opening_page():
    result = T.solve(build_issue())
    assert result.status == "auto"
    assert result.start_pages == [5, 25, 49]
    assert all("folio" in assignment.signals.strong for assignment in result.assignments)


def test_assignment_is_monotonic_and_beats_the_late_running_head():
    """The head for article 2 first appears on physical page 26, not 25.

    A head-derived splitter must back off by one here; the solver never forms an
    offset at all, because it selects the opening page directly.
    """
    result = T.solve(build_issue())
    pages = result.start_pages
    assert pages == sorted(pages)
    assert pages[1] == 25


def test_in_piece_contents_page_does_not_attract_an_entry():
    inline_toc = "\n".join(
        [
            "160",
            "I. Introduction . . . . . . . . . . . . . 161",
            "II. The Statutory Background . . . . . . 167",
            "III. The Remedy . . . . . . . . . . . . . 173",
            "IV. Conclusion . . . . . . . . . . . . . 179",
        ]
    )
    result = T.solve(build_issue(extra_pages={6: inline_toc}))
    assert 6 not in result.start_pages


def test_faculty_roster_page_is_penalised():
    roster = "\n".join(
        [
            "FACULTY",
            "Danielle R. Holley",
            "Kenneth B. Nunn",
            "Sherrilyn A. Ifill",
            "Rachel E. Rosenbloom",
            "Simeon M. Spencer",
            "Summer E. Durant",
            "Migueyli A. Duran",
            "Eric H. Holder",
        ]
    )
    evidence = T.build_page_evidence(T.pages_from_texts([roster]))
    assert evidence[0].is_roster


def test_continuation_prose_is_a_negative_signal():
    page = T.pages_from_texts([_continuation_page("REMEDIES FOR THE WRONGLY", 145, recto=True)])
    evidence = T.build_page_evidence(page)
    assert evidence[0].continues_prose


# ---------------------------------------------------------------------------
# Emission policy
# ---------------------------------------------------------------------------


def test_abstains_without_a_usable_contents_listing():
    texts = ["body text that just runs on and on for this page" for _ in range(60)]
    result = T.solve(T.pages_from_texts(texts))
    assert result.status == "abstain"
    assert result.reason == "no_usable_toc"


def test_abstains_on_short_documents():
    assert T.solve(T.pages_from_texts(["a", "b"])).status == "abstain"


def test_head_transition_alone_never_authorises_a_split():
    """Strip the folio, title and author evidence; only the head remains."""
    signals = T.Signals(head_transition=True, opening=True)
    assert signals.strong == []
    assignment = T.Assignment(
        entry=T.TocEntry(printed_page=139, title="Remedies", author=""),
        page=5,
        score=1.0,
        signals=signals,
        margin=99.0,
    )
    status, reason = T._emission_policy([assignment], T.FolioFit(0, 1.0, 0.0, 40, 40), 2.0)
    assert status == "abstain"
    assert reason.startswith("insufficient_evidence")


def test_single_strong_signal_routes_to_review_not_auto():
    signals = T.Signals(folio=True)
    assignment = T.Assignment(
        entry=T.TocEntry(printed_page=139, title="Remedies", author=""),
        page=5,
        score=3.0,
        signals=signals,
        margin=99.0,
    )
    status, _ = T._emission_policy([assignment], T.FolioFit(0, 1.0, 0.0, 40, 40), 2.0)
    assert status == "review"


def test_low_margin_routes_to_review():
    signals = T.Signals(folio=True, title_similarity=0.9, author=True)
    assignment = T.Assignment(
        entry=T.TocEntry(printed_page=139, title="Remedies", author="Rosenbloom"),
        page=5,
        score=9.0,
        signals=signals,
        margin=0.4,
    )
    status, reason = T._emission_policy([assignment], T.FolioFit(0, 1.0, 0.0, 40, 40), 2.0)
    assert status == "review"
    assert "margin" in reason


def test_ledger_records_every_signal():
    ledger = T.solve(build_issue()).ledger()
    assert ledger["folio"]["offset"] == OFFSET
    first = ledger["assignments"][0]
    assert set(first["signals"]) >= {
        "folio",
        "offset_implied",
        "title_similarity",
        "author",
        "opening",
        "head_transition",
        "continuation",
        "roster_or_toc",
        "strong",
    }
    assert first["physical_page"] == 5


@pytest.mark.parametrize(
    "title,expected",
    [
        ("Remedies for the Wrongly Deported . . . . . Rachel E. Rosenbloom", "Rachel E. Rosenbloom"),
        ("Monuments, Law and Cultural Transformation. . . . Kenneth B. Nunn", "Kenneth B. Nunn"),
        ("A Title With No Author At All . . . . . . . . .", ""),
    ],
)
def test_label_splits_on_dot_leaders(title: str, expected: str):
    assert T._split_label(title)[1] == expected


# ---------------------------------------------------------------------------
# In-article contents pages
# ---------------------------------------------------------------------------


def _entries(*pairs: tuple[int, str, str]) -> list[T.TocEntry]:
    return [T.TocEntry(printed_page=page, title=title, author=author) for page, title, author in pairs]


def test_real_listing_is_not_flagged_as_in_article():
    entries = _entries(
        (139, "Remedies for the Wrongly Deported", "Rachel E. Rosenbloom"),
        (159, "Monuments, Law and Cultural Transformation", "Kenneth B. Nunn"),
        (183, "The Case Against Juvenile Confinement", "Migueyli Aisha Duran"),
    )
    assert not T._listing_is_in_article(entries)


def test_fragment_titles_flag_an_in_article_contents_page():
    """Row reconstruction leaves wrapped section headings starting mid-phrase."""
    entries = _entries(
        (93, "Recently B. Hypothesis Two: Exclusion of Critical Scholars", ""),
        (96, "to Race C. Hypothesis Three: The Big or Defining Debate", ""),
        (100, "in the United States Have Focused on Issues Other than Race", ""),
    )
    assert T._listing_is_in_article(entries)


def test_embedded_enumerators_flag_an_in_article_contents_page():
    entries = _entries(
        (14, "List of Figures and Tables List of Figures Figure 1. Overview", ""),
        (22, "Figure 2. Synfuel production routes", ""),
        (31, "Figure 3. E-fuel projects in Chile", ""),
    )
    assert T._listing_is_in_article(entries)


def test_a_middle_initial_is_not_read_as_an_enumerator():
    """`Eric H. Holder` must not look like section H."""
    entries = _entries(
        (461, "Remarks of the Honorable Eric H. Holder, Third Annual Lecture", "Eric H. Holder Jr"),
        (475, "Reviving the Promise of the 14th Amendment", "Sherrilyn Ifill"),
    )
    assert not T._listing_is_in_article(entries)


def test_an_authorless_listing_of_well_formed_titles_is_kept():
    """Absence of authors is not on its own evidence of an in-article listing.

    `www.fclj.org/67-3-2-communications-law-annual-review.pdf` lists case names
    with no authors and is a real multi-piece document. Only titles that are
    BOTH malformed (fragment or enumerator) and unauthored condemn a listing.
    """
    entries = _entries(*[(index * 10, f"Some Well Formed Title Number {index}", "") for index in range(1, 7)])
    assert not T._listing_is_in_article(entries)


def test_wrapped_titles_in_a_real_listing_survive_the_filter():
    """A real listing wraps its titles too; the author is what saves them."""
    entries = _entries(
        (1, "Unaffordable Justice: The High Cost of Mandatory Arbitration", "Nicole Ehrlich"),
        (39, "for the Average Worker", "Lisa A. Nagele-Piazza"),
        (69, "How Development Can Be Expanded", "Kevin M. Walsh"),
        (95, "of a Recuperating Music Industry", "Jacob A. Epstein"),
    )
    assert not T._listing_is_in_article(entries)


def test_a_short_listing_with_no_authors_is_kept():
    """A symposium programme may legitimately omit authors."""
    entries = _entries(
        (139, "Remedies for the Wrongly Deported", ""),
        (159, "Monuments, Law and Cultural Transformation", ""),
    )
    assert not T._listing_is_in_article(entries)


def test_listing_whose_numbers_are_outside_the_folio_stream_is_refused():
    """A data table inside an article can parse as a listing; its numbers cannot.

    The article runs printed pp. 500-540. A table of years parses as entries
    starting on printed pages 3 and 5 -- numbers that appear nowhere in the
    document's folio stream, so the listing is not this document's contents.
    """
    texts = [
        "\n".join(
            [
                f"2024] SOME ARTICLE {499 + index}",
                "prose that continues from the previous page and runs the full measure here",
                str(499 + index),
            ]
        )
        for index in range(1, 42)
    ]
    texts[20] = "\n".join(
        [
            "Table 2. Convictions by Year",
            "Kenya Crimes Against Humanity 3",
            "Mali War Crimes Against Humanity 5",
        ]
    )
    result = T.solve(T.pages_from_texts(texts))
    assert result.status == "abstain"
    assert result.start_pages == []


def test_contents_listing_arbitrates_between_competing_folio_streams():
    """A Digital Commons page stamp is a perfect stream and wins the raw fit.

    Every page of a scanned issue carries both a DC stamp (1..N) and the
    journal's real folio (387..). The stamp never skips, so consensus picks it
    (`nsuworks.nova.edu/Vol._38_2C_Number_3.pdf`: offset -1 at support 1.00
    against the real folios at 0.857) and every contents entry then belongs to
    no page in the document. The listing breaks the tie.
    """
    texts = [
        "\n".join([f"{index}", f"NOVA LAW REVIEW {index + 386}", "body prose", f"{index + 386}"])
        for index in range(1, 61)
    ]
    evidence = T.build_page_evidence(T.pages_from_texts(texts))
    # The raw fit picked the stamp; both offsets are on the candidate list.
    stamp_won = T.FolioFit(
        offset=0, support=1.0, runner_up_support=0.857, longest_run=60, n_pages=60,
        method="consensus_run", candidates=[(0, 60, 60), (386, 52, 52)],
    )
    entries = [
        T.TocEntry(printed_page=387, title="The Disruptive Politics of Energy", author="Thomas O. McGarity"),
        T.TocEntry(printed_page=407, title="Traditionally Structured Utilities", author="Joseph P. Tomain"),
        T.TocEntry(printed_page=427, title="Phasing Out Fossil Fuels", author="David M. Driesen"),
    ]
    chosen = T._offset_agreeing_with_toc(stamp_won, entries, evidence)
    assert chosen.offset == 386
    assert chosen.method == "toc_selected_stream"


def test_toc_arbitration_leaves_a_single_stream_alone():
    evidence = T.build_page_evidence(build_issue())
    fit = T.estimate_folio_offset(evidence, start_index=5)
    entries = T.parse_toc_entries(build_issue())
    assert T._offset_agreeing_with_toc(fit, entries, evidence).offset == fit.offset
