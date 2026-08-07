from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from offprint.pdf_footnotes.issue_splitter import (
    deduplicate_pdf_paths,
    infer_article_boundaries,
    load_candidates_from_tsv,
)


def test_infer_article_boundaries_from_toc_page_numbers() -> None:
    pages = ["Cover", "TABLE OF CONTENTS\nFirst Article\nPage 1\nSecond Article\nPage 5\nThird Article\nPage 9"]
    pages.extend([""] * 10)
    pages[2] = "1\nFIRST ARTICLE\nJane Author\nABSTRACT\nBody"
    pages[6] = "5\nSECOND ARTICLE\nJohn Author\nABSTRACT\nBody"
    pages[10] = "9\nTHIRD ARTICLE\nAlex Author\nABSTRACT\nBody"

    inference = infer_article_boundaries(pages)

    assert inference.ok
    assert [(b.start_page, b.end_page) for b in inference.boundaries] == [(3, 6), (7, 10), (11, 12)]
    assert inference.boundaries[0].method == "toc_page_refs+printed_page_number"
    assert inference.boundaries[0].title_guess == "FIRST ARTICLE"


def test_deduplicate_pdf_paths_by_sha256(tmp_path) -> None:
    first = tmp_path / "first.pdf"
    duplicate = tmp_path / "duplicate.pdf"
    other = tmp_path / "other.pdf"
    first.write_bytes(b"same-content")
    duplicate.write_bytes(b"same-content")
    other.write_bytes(b"different-content")

    result = deduplicate_pdf_paths([first, duplicate, other])

    assert [item.path for item in result.unique] == [first, other]
    assert len(result.duplicates) == 1
    assert result.duplicates[0].path == duplicate
    assert result.duplicates[0].duplicate_of == first
    assert result.duplicates[0].sha256 == result.unique[0].sha256


def test_infer_article_boundaries_heading_fallback() -> None:
    pages = ["Cover"]
    pages += ["TITLE OF ARTICLE ONE\nAuthor Name\nABSTRACT\nBody text"] + ["filler"] * 5
    pages += ["TITLE OF ARTICLE TWO\nAnother Author\nABSTRACT\nBody text"] + ["filler"] * 4
    pages += ["TITLE OF ARTICLE THREE\nThird Author\nABSTRACT\nBody text"] + ["filler"] * 2

    inference = infer_article_boundaries(pages)

    assert inference.ok
    assert inference.method == "heading_fallback+abstract"
    assert len(inference.boundaries) >= 2
    assert inference.boundaries[0].start_page < inference.boundaries[1].start_page


def test_load_candidates_from_tsv() -> None:
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        pdf = root / "one.pdf"
        pdf.write_bytes(b"%PDF-1.4\n%fake")
        tsv = root / "candidates.tsv"
        tsv.write_text(
            "domain\tpdf_path\tsha256\tpages\theuristics\tpriority\n"
            f"example.org\t{pdf}\tdeadbeef\t123\tfilename:token\t5\n",
            encoding="utf-8",
        )
        rows = load_candidates_from_tsv(tsv)
        assert len(rows) == 1
        assert rows[0].path == pdf
        assert rows[0].domain == "example.org"
        assert rows[0].provided_sha256 == "deadbeef"


def test_load_candidates_from_tsv_issue_filters() -> None:
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        good = root / "vol-41-full-issue.pdf"
        bad = root / "table-of-contents.pdf"
        good.write_bytes(b"%PDF-1.4\n%good")
        bad.write_bytes(b"%PDF-1.4\n%bad")
        tsv = root / "candidates.tsv"
        tsv.write_text(
            "domain\tpdf_path\tsha256\tpages\theuristics\tpriority\n"
            f"example.org\t{good}\t\t300\tfilename:strong_issue_token,pages:>120\t9\n"
            f"example.org\t{bad}\t\t4\tfilename:token\t8\n",
            encoding="utf-8",
        )
        rows = load_candidates_from_tsv(tsv, issue_only=True, min_priority=7.5)
        assert len(rows) == 1
        assert rows[0].path == good


# ---- US law-review boundary inference (added 2026-08-06) ----

from offprint.pdf_footnotes.issue_splitter import (  # noqa: E402
    article_start_pages_from_heads,
    head_signature,
    infer_law_review_boundaries,
    parse_toc_printed_starts,
)


def _hawaii_issue_pages(n_pages: int = 40) -> list[str]:
    """Verso pages name the current article's start page: `Vol. 32:<start>`.

    Pages 3 and 21 are the article openings this fixture asserts on, so they
    carry a display title and author rather than a running head over body text
    -- which is what a real opening page looks like, and what the boundary
    validator requires.
    """
    pages = ["University of Hawai'i Law Review\nVolume 32 / Number 1", "TABLE OF CONTENTS"]
    for page in range(3, n_pages + 1):
        start = 1 if page < 21 else 21
        if page in (3, 21):
            pages.append("SOME ARTICLE TITLE\nBy Jane Author\nbody text")
        elif page % 2 == 0:
            pages.append(f"University of Hawai'i Law Review / Vol. 32:{start}\nbody text")
        else:
            pages.append("2010 / SOME ARTICLE TITLE\nbody text")
    return pages


def test_head_signature_strips_folios_so_pages_compare_equal():
    assert head_signature("2010 / TXTING WHL DRVNG 361") == head_signature(
        "2010 / TXTING WHL DRVNG 377"
    )


def test_article_start_pages_come_from_the_vol_start_running_head():
    starts = article_start_pages_from_heads(_hawaii_issue_pages())

    assert len(starts) == 2
    assert starts[1] > starts[0]


def test_infer_law_review_boundaries_splits_on_vol_start():
    inference = infer_law_review_boundaries(_hawaii_issue_pages())

    assert inference.ok
    assert inference.method.startswith("running_head_vol_start")
    assert len(inference.boundaries) == 2
    assert inference.boundaries[0].end_page == inference.boundaries[1].start_page - 1
    assert inference.boundaries[-1].end_page == 40


def test_change_detection_alone_never_emits_boundaries():
    """It put boundaries mid-article at a 4-page period on real volumes."""
    pages = ["front matter", "contents"]
    for page in range(3, 41):
        # Header displaced every fourth page, as a full-page footnote run does.
        header = "filler line" if page % 4 == 0 else "2022] NEO-SEGREGATION IN MINNESOTA"
        pages.append(f"{header}\nbody text for page {page}")

    inference = infer_law_review_boundaries(pages)

    assert not inference.ok
    assert inference.skip_reason in {
        "change_signal_only_unverified",
        "no_running_head_signal",
    }


def test_parse_toc_reads_us_style_trailing_page_numbers():
    toc = [
        "TABLE OF CONTENTS\n"
        "Remedies for the Wrongly Deported\n"
        "Rachel E. Rosenbloom .................... 139\n"
        "Regression by Progression\n"
        "Helia Garrido Hull ...................... 193\n"
        "The Jones Act Fish Farmer\n"
        "Timothy E. Steigelman .................. 223\n"
    ]

    assert parse_toc_printed_starts(toc) == [139, 193, 223]


def test_parse_toc_ignores_prose_that_happens_to_end_in_a_number():
    prose = ["The statute was amended in 2019\nand the court agreed in 2021\n"]

    assert parse_toc_printed_starts(prose) == []


def test_single_article_yields_no_boundaries():
    """60-70pp BTLJ offprints are single articles with one constant head."""
    pages = ["MISUSE OR FAIR USE\nAuthor Name"] + [
        ("MISUSE OR FAIR USE" if page % 2 else "BERKELEY TECHNOLOGY LAW JOURNAL")
        + f"\nbody {page}"
        for page in range(2, 61)
    ]

    assert not infer_law_review_boundaries(pages).ok


# ---- Per-domain head rules (added 2026-08-06) ----

import re  # noqa: E402

import pytest  # noqa: E402

from offprint.pdf_footnotes.issue_splitter import (  # noqa: E402
    article_keys_for_pages,
    boundaries_from_domain_rule,
    load_head_rules,
)

_TITLE_HEAD_RULE = {
    "kind": "pattern",
    "head_lines": "first",
    "article_key_patterns": [r"^\d{4}\]\s+(?P<key>[^\d\s].{0,58}?)(?:\s+\d+)?\s*$"],
}


def _title_head_issue() -> list[str]:
    """Recto `2022] <TITLE> <page>`; verso is the journal name and never matches."""
    pages = ["FRONT MATTER", "TABLE OF CONTENTS"]
    for page in range(3, 41):
        title = "FIRST ARTICLE TITLE" if page < 21 else "SECOND ARTICLE TITLE"
        if page % 2:
            pages.append(f"2022] {title} {page}\nbody")
        else:
            pages.append(f"{page} EXAMPLE LAW REVIEW [Vol. 22\nbody")
    return pages


def test_unmatched_pages_inherit_the_previous_article_key():
    """Verso pages never match; they must not read as a new article."""
    keys = article_keys_for_pages(_title_head_issue(), _TITLE_HEAD_RULE)

    assert keys[4] == keys[5]
    assert len({key for key in keys if key}) == 2


def test_domain_rule_backs_the_boundary_off_by_one_page():
    """The article's own first page shows a display title, not a running head,
    so the key does not change until page two. Without the back-off every child
    would lose the page its title and author are read from."""
    starts = boundaries_from_domain_rule(_title_head_issue(), _TITLE_HEAD_RULE)

    assert 20 in starts and 21 not in starts


def test_domain_rule_suppresses_short_phantom_runs():
    """A one-page head flip is a degraded head, not a one-page article."""
    pages = [f"2022] STEADY TITLE {page}\nbody" for page in range(1, 31)]
    pages[14] = "2022] MOMENTARY GLITCH 15\nbody"

    starts = boundaries_from_domain_rule(pages, _TITLE_HEAD_RULE)

    assert all(
        second - first >= 4 for first, second in zip(starts, starts[1:])
    ), starts


def test_key_normalisation_ignores_case_and_punctuation_flips():
    """OCR flips `The`/`the` and drops periods; neither is an article boundary."""
    rule = {
        "kind": "pattern",
        "head_lines": "first",
        "article_key_patterns": [r"^(?P<key>.+?)\s*$"],
    }
    keys = article_keys_for_pages(["PARKS V. COOPER", "Parks v Cooper"], rule)

    assert keys[0] == keys[1]


def test_single_article_domains_are_never_split():
    rules = {
        "domains": {"albertalawreview.com": {"kind": "single_article_domain"}}
    }
    inference = infer_law_review_boundaries(
        _hawaii_issue_pages(), "albertalawreview.com", rules
    )

    assert not inference.ok
    assert inference.skip_reason == "single_article_domain"


def test_shipped_head_rules_are_loadable_and_well_formed():
    rules = load_head_rules()

    assert rules["domains"], "issue_head_rules.json should ship with domains"
    for domain, rule in rules["domains"].items():
        assert rule.get("kind") in {"pattern", "single_article_domain"}, domain
        for pattern in rule.get("article_key_patterns") or []:
            assert "(?P<key>" in pattern, f"{domain}: pattern has no `key` group"
            re.compile(pattern)  # must not raise


# ---- Article-opening validation (added 2026-08-06) ----

from offprint.pdf_footnotes.issue_splitter import (  # noqa: E402
    looks_like_article_opening,
    validate_boundary_starts,
)


def test_continuation_page_is_not_an_article_opening():
    """Drop the running head and a continuation page resumes in lower case."""
    page = (
        "10  Harvard Journal of Law & Public Policy  [Vol. 40\n"
        "relocating and taking on a new allegiance to the firm that hired them"
    )

    assert not looks_like_article_opening(page)


def test_display_title_page_is_an_article_opening():
    page = (
        "THE CONSTITUTIONALIZATION OF TECHNOLOGY LAW SYMPOSIUM\n"
        "CONSTITUTIONAL BOUNDS OF DATABASE PROTECTION\n"
        "By Yochai Benkler\n"
    )

    assert looks_like_article_opening(page)


def test_junk_page_is_not_an_article_opening():
    assert not looks_like_article_opening("!")
    assert not looks_like_article_opening("")


def test_validate_boundary_starts_drops_failures_and_reports_share():
    pages = [
        "OPENING TITLE ONE\nBy Someone",
        "body text continues here",
        "12 Example Law Review [Vol. 3\nmid-sentence continuation of the argument",
        "OPENING TITLE TWO\nBy Another",
    ]

    kept, share = validate_boundary_starts(pages, [1, 3, 4])

    assert kept == [1, 4]
    assert share == pytest.approx(2 / 3)


def test_document_is_discarded_when_most_boundaries_are_not_openings():
    """A pattern latched onto body text cuts every few pages; refuse it all."""
    pages = ["OPENING TITLE\nBy Someone"] + [
        f"{n} Example Law Review [Vol. 3\ncontinuing the sentence from before"
        for n in range(2, 41)
    ]

    inference = infer_law_review_boundaries(
        pages,
        "ex.org",
        {
            "domains": {
                "ex.org": {
                    "kind": "pattern",
                    "head_lines": "first",
                    "article_key_patterns": [r"^(?P<key>\d+) Example"],
                }
            }
        },
    )

    assert not inference.ok
    assert inference.skip_reason.startswith("boundaries_not_article_openings")


@pytest.mark.parametrize(
    "name,page,expected",
    [
        # Continuation whose next word is capitalised. Scanning past the first
        # body line used to let these through.
        (
            "proper noun continuation",
            "712 Harvard Journal of Law & Public Policy [Vol. 42\n"
            "Masterpiece Cakeshop  as well. Indeed,",
            False,
        ),
        (
            "photo caption mid-article",
            "808 Harvard Journal of Law & Public Policy [Vol. 42\n"
            "“SEDER—Albert Yakus (left), president",
            False,
        ),
        # Shouted short title + folio is a running head, not a display title.
        (
            "letter-spaced title-folio head",
            "Do Not Delete  1/8/2015  10:15 AM\nCENTERED 1 0 3\n"
            "to the issuance of an appellate court",
            False,
        ),
        (
            "title-folio head over letter-spaced body",
            "Do Not Delete  1/8/2015  10:14 AM\nDRIVEN 2 5\n"
            "6 8 .  I n t e r v i e w  b y",
            False,
        ),
        (
            "real opening under journal head and folio",
            "ATLANTIC LAW JOURNAL, VOLUME 20\n35\nNOT TOO HOT AND NOT TOO COLD: A",
            True,
        ),
        (
            "section opening",
            "872 Harvard Journal of Law & Public Policy [Vol. 42\nINTRODUCTION\n"
            "With the Supreme Court",
            True,
        ),
        ("issue cover page", "LAW REVIEW:\nThe First Fifty Years\nof", True),
    ],
)
def test_article_opening_detector_on_real_pages(name, page, expected):
    assert looks_like_article_opening(page) is expected, name


# ---- Continuation pages that shipped as children (audited 2026-08-07) ----
#
# Every page below is the FIRST page of a child PDF written by the
# 20260806T164717Z run. All of them are mid-article continuations: the boundary
# was wrong, and the detector passed them because the page's running head was
# not recognised as one and its own shouted text read as a display title.


@pytest.mark.parametrize(
    "name,page,expected",
    [
        (
            # jlep.net/v8-3 child a04. The extractor letter-spaces small caps,
            # so `J OURNAL OF` and `[V OL. 8:3` matched no head pattern.
            "letter-spaced journal head",
            "550 J OURNAL OF LAW, ECONOMICS & POLICY [V OL. 8:3\n"
            "exponents—the free choice of legal counsel and the right to determine\n"
            "one’s litigation strategy.  The Dexia case study illustrates that applying",
            False,
        ),
        (
            # regentuniversitylawreview.com/211 child a01. A shouted short
            # title with no folio: a display title as far as the old patterns
            # were concerned.
            "short-title head without a folio",
            "SACRIFICING MOTHERHOOD\n"
            "more particularly, whether the child can have two mothers.8 None of\n"
            "those approaches, however, give proper constitutional deference to the",
            False,
        ),
        (
            # hawaiilawreview.com/43-2. `<year> / <TITLE> <folio>` heads start
            # with a digit, so the title-folio pattern never matched them.
            "year-slash running head",
            "2021 / IMPLEMENTING PASH AND ITS PROGENY WITHIN DLNR 421\n"
            "diverse and often opposing viewpoints advocate for their interests.2 Not\n"
            "everyone even agrees on what are traditional and customary practices of",
            False,
        ),
        (
            # Same journal, but the resumed prose starts with a capital, so a
            # lower-case-first-word test alone would not catch it.
            "year-slash head over capitalised prose",
            "2021 / PASH: NO ONE LEGACY\n"
            "This, to me, is the most important, most concrete result of PASH and its\n"
            "progeny for the future of Hawai‘i.",
            False,
        ),
        (
            # administrativelawreview.org/63-2. The Word production slug shouts
            # too, and left in place it hid the real head on the line below.
            "production slug above the real head",
            "4RATHREV1.DOCX 5/26/2011 5:18 PM\n"
            "350 ADMINISTRATIVE LAW REVIEW [63:2\n"
            "spending agency resources on any particular site in that state.",
            False,
        ),
        (
            # jost.syr.edu/jost-volume-39. `V ol. 39` again defeats a spaced
            # pattern; the folio sits on its own line underneath.
            "letter-spaced Vol. head with folio line",
            "V ol. 39 SYRACUSE J. SCI. & TECH. L. Hung\n72\n"
            "prevailing limitations. The complex nature of legal language and the",
            False,
        ),
        (
            # jlep.net/volume-42: a scanned page whose text is debris.
            "OCR debris under a recognised head",
            "400 JOURNAL OF LAW, ECONOMICS & POLICY [VOL. 4:2\nI-'- ' 'S\ntCz",
            False,
        ),
        # --- and the openings the fix must not start rejecting ---
        (
            "display title over a lower-case author line",
            "A HOPELESS CASE?: ESCAPING THE PROOF\n"
            "PITFALL IN POWER-DEPENDENT PARADIGMS*\n"
            "e. christi cunningham †",
            True,
        ),
        (
            "department opening whose body starts under the title",
            "BOOKS RECEIVED\n"
            "The books listed below have been received by High Technology Law Journal over\n"
            "the past year. The books are cataloged by subject and are listed alphabetically",
            True,
        ),
        (
            "production slug above a display title",
            "1091_1118_ALMELING_WEB_110612 (DO NOT DELETE) 11/6/2012 5:27 PM\n"
            "SEVEN REASONS WHY TRADE SECRETS ARE\n"
            "INCREASINGLY IMPORTANT",
            True,
        ),
        (
            "tribute opening with prose two lines down",
            "ACKNOWLEDGMENT\nDEAN TREVOR MORRISON\n"
            "I am speechless, which is a first. I have been a part of these",
            True,
        ),
        (
            "title-case display title and author",
            "The Globalized District Court\nJudge Nancy Gertner\"",
            True,
        ),
        (
            # columbialawreview.org/clr-126n1. Too short to be a title, but a
            # divider sits between articles and never inside one.
            "section divider page",
            "83\nNOTES",
            True,
        ),
    ],
)
def test_article_opening_detector_on_shipped_children(name, page, expected):
    assert looks_like_article_opening(page) is expected, name


def test_hawaii_issue_with_mid_article_boundaries_is_discarded():
    """43 U. Haw. L. Rev. 2: half the head-derived starts land mid-article.

    Dropping those starts alone would merge them into the preceding child, so
    the whole document has to go.
    """
    pages = ["COVER", "TABLE OF CONTENTS"]
    for page in range(3, 41):
        start = 1 if page < 21 else 21
        if page % 2 == 0:
            pages.append(f"University of Hawai'i Law Review / Vol. 32:{start}\nbody text")
        else:
            pages.append(
                "2021 / IMPLEMENTING PASH AND ITS PROGENY WITHIN DLNR 421\n"
                "diverse and often opposing viewpoints advocate for their interests.2 Not"
            )

    inference = infer_law_review_boundaries(pages)

    assert not inference.ok
    assert inference.skip_reason.startswith("boundaries_not_article_openings")
