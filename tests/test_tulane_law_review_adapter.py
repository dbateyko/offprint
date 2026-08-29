"""Regression tests for the Tulane Law Review Online adapter.

Tulane's online companion hosts its PDFs same-origin as Squarespace ``/s/``
assets.  GenericAdapter would also find them, but its candidate test accepts
any host's ``.pdf`` href; these tests pin that this adapter does not, so a
footnote link to a third-party PDF cannot be recorded as a Tulane article.
The two rejected foreign URLs are the real links from the 2026-08-24 OJS
scope-leak incident.
"""

from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from offprint.adapters.registry import pick_adapter_for
from offprint.adapters.tulane_law_review import TulaneLawReviewOnlineAdapter

SEED = "https://www.tulanelawreview.org/tlr-online"
POST = "https://www.tulanelawreview.org/tlr-online/in-defense-of-dirt-9hkts"

POST_HTML = """
<html><body>
  <h1>In Defense of Dirt</h1>
  <a href="/s/01-95OEnglehartfinal.pdf">Download PDF</a>
  <p>See <a href="https://www.katowice.sa.gov.pl/container/opinion.pdf">a Polish court opinion</a>
     and <a href="https://lirias.kuleuven.be/retrieve/123456.pdf">a Leuven paper</a>.</p>
</body></html>
"""

INDEX_HTML = """
<html><body>
  <a href="/tlr-online/in-defense-of-dirt-9hkts">In Defense of Dirt</a>
  <a href="/tlr-online/tag/Title+VII">Title VII</a>
  <a href="/tlr-online/category/Notes">Notes</a>
  <a href="/pub/volume95/issue5/the-rule-of-law-by-design">A print article</a>
  <a href="https://otherjournal.example.org/tlr-online/spoof">Off-origin lookalike</a>
</body></html>
"""


@pytest.fixture()
def adapter() -> TulaneLawReviewOnlineAdapter:
    return TulaneLawReviewOnlineAdapter()


def test_only_same_origin_pdfs_are_accepted(adapter) -> None:
    soup = BeautifulSoup(POST_HTML, "html.parser")
    found = adapter._same_origin_pdfs(soup, POST, SEED)
    assert found == ["https://www.tulanelawreview.org/s/01-95OEnglehartfinal.pdf"]


@pytest.mark.parametrize(
    "url",
    [
        "https://www.katowice.sa.gov.pl/container/opinion.pdf",
        "https://lirias.kuleuven.be/retrieve/123456.pdf",
        "https://harvardlawreview.org/wp-content/uploads/2020/01/other.pdf",
    ],
)
def test_third_party_pdfs_are_rejected(adapter, url: str) -> None:
    assert adapter._accept_pdf_url(url, SEED) is False


def test_index_link_scope(adapter) -> None:
    soup = BeautifulSoup(INDEX_HTML, "html.parser")
    urls = [u for u, _ in adapter._post_links(soup, SEED)]
    assert urls == ["https://www.tulanelawreview.org/tlr-online/in-defense-of-dirt-9hkts"]


@pytest.mark.parametrize(
    "pdf_url,expected",
    [
        ("https://www.tulanelawreview.org/s/01-95OEnglehartfinal.pdf", "95"),
        ("https://www.tulanelawreview.org/s/92onlineKaufman7.pdf", "92"),
        ("https://www.tulanelawreview.org/s/02-100O-Bankenfinal.pdf", "100"),
    ],
)
def test_volume_from_filename(adapter, pdf_url: str, expected: str) -> None:
    assert adapter._volume_from_filename(pdf_url) == expected


def test_registry_routes_tulane_hosts() -> None:
    for url in (SEED, "https://tulanelawreview.org/tlr-online"):
        assert isinstance(pick_adapter_for(url), TulaneLawReviewOnlineAdapter)


def test_page_title_skips_the_squarespace_date_heading(adapter) -> None:
    # Squarespace blog posts render a date <h1> before the article title.
    html = """
    <html><body>
      <h1>July 5, 2026</h1>
      <h1 class="entry-title">In Defense of Dirt</h1>
    </body></html>
    """
    assert adapter._page_title(BeautifulSoup(html, "html.parser")) == "In Defense of Dirt"
