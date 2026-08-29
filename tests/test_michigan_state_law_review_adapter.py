"""Regression tests for the Michigan State Law Review adapter.

MSLR hosts no PDFs itself -- every article file is a Google Drive object linked
from the journal's own Squarespace pages.  These tests pin (a) that the
archive-page walk pairs each Drive link with its heading-derived issue and its
following author line, and (b) that the origin gate rejects third-party PDF
hrefs.  The two rejected URLs below are the real links that leaked into an OJS
journal in the 2026-08-24 scope-leak incident.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from offprint.adapters.michigan_state_law_review import MichiganStateLawReviewAdapter
from offprint.adapters.registry import pick_adapter_for

FIXTURE = Path(__file__).parent / "fixtures" / "michigan_state_law_review_past_issues.html"
PAGE_URL = "https://www.michiganstatelawreview.org/past-issues-1"


@pytest.fixture()
def results():
    adapter = MichiganStateLawReviewAdapter()
    soup = BeautifulSoup(FIXTURE.read_text(encoding="utf-8"), "html.parser")
    return list(adapter._walk_archive_page(soup, PAGE_URL, set()))


def test_only_drive_links_are_discovered(results) -> None:
    assert len(results) == 3
    for result in results:
        assert result.pdf_url.startswith(
            "https://drive.usercontent.google.com/download?id="
        ), result.pdf_url


@pytest.mark.parametrize(
    "url",
    [
        "https://www.katowice.sa.gov.pl/container/opinion.pdf",
        "https://lirias.kuleuven.be/retrieve/123456.pdf",
        "https://harvardlawreview.org/wp-content/uploads/2020/01/other.pdf",
    ],
)
def test_third_party_pdfs_are_rejected(results, url: str) -> None:
    assert url not in {r.pdf_url for r in results}
    assert MichiganStateLawReviewAdapter._accept_article_url(url) is False


def test_issue_and_author_are_attached(results) -> None:
    first = results[0]
    assert first.metadata["title"].startswith("Reg BI+")
    assert first.metadata["authors"] == ["James Fallows Tierney"]
    assert first.metadata["volume"] == "2024"
    assert first.metadata["issue"] == "4"
    assert first.metadata["section"] == "Articles"
    assert first.metadata["journal"] == "Michigan State Law Review"

    last = results[-1]
    assert last.metadata["volume"] == "2023"
    assert last.metadata["issue"] == "1"
    assert last.metadata["authors"] == ["Jane Doe"]


def test_drive_view_urls_are_rewritten_to_direct_download() -> None:
    direct = MichiganStateLawReviewAdapter._direct_pdf_url(
        "https://drive.google.com/file/d/1ZBXJ1Eh0WrPKhaguP_YUA_8EnIlAs01d/view?usp=sharing"
    )
    assert direct == (
        "https://drive.usercontent.google.com/download"
        "?id=1ZBXJ1Eh0WrPKhaguP_YUA_8EnIlAs01d&export=download"
    )


def test_registry_routes_michigan_state_hosts() -> None:
    for url in (
        "https://www.michiganstatelawreview.org/past-issues-1",
        "https://michiganstatelawreview.org/current-vol-20252026",
    ):
        assert isinstance(pick_adapter_for(url), MichiganStateLawReviewAdapter)
