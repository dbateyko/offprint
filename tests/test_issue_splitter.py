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
