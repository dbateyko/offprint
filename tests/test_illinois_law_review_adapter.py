"""Regression tests for the University of Illinois Law Review adapter.

The journal splits PDF hosting between its vanity WordPress origin and a
companion campus install.  These tests pin the origin gate: the companion host
is accepted only for real upload paths, and unrelated third-party PDFs (the
"reference-list link is not an article" failure mode) stay rejected.
"""

from __future__ import annotations

import pytest

from offprint.adapters.illinois_law_review import IllinoisLawReviewAdapter
from offprint.adapters.registry import pick_adapter_for


@pytest.fixture()
def adapter() -> IllinoisLawReviewAdapter:
    return IllinoisLawReviewAdapter(
        base_url="https://illinoislawreview.org",
        journal_name="University of Illinois Law Review",
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://illinoislawreview.org/wp-content/uploads/2026/08/Heller-1.pdf",
        "https://illinoislawreview.org/wp-content/uploads/1949/01/Carlson.pdf",
        "https://illinoislawrev.web.illinois.edu/wp-content/uploads/2018/09/McCaffrey.pdf",
        "https://publish.illinois.edu/lawreview/files/2019/01/Smith.pdf",
    ],
)
def test_accepts_journal_hosted_pdfs(adapter: IllinoisLawReviewAdapter, url: str) -> None:
    assert adapter._is_preferred_pdf_url(url) is True


@pytest.mark.parametrize(
    "url",
    [
        # Third-party PDFs cited in footnotes must never be attributed to ILR.
        "https://www.katowice.sa.gov.pl/container/some-opinion.pdf",
        "https://lirias.kuleuven.be/retrieve/123456.pdf",
        "https://harvardlawreview.org/wp-content/uploads/2020/01/other.pdf",
        # Right host family, wrong tenant/path: publish.illinois.edu is a
        # multi-tenant campus WordPress shared by many units.
        "https://publish.illinois.edu/someotherjournal/files/2019/01/Smith.pdf",
        # Companion host but not an uploads path.
        "https://illinoislawrev.web.illinois.edu/assets/brochure.pdf",
        # Companion host, non-PDF.
        "https://illinoislawrev.web.illinois.edu/wp-content/uploads/2018/09/page.html",
    ],
)
def test_rejects_offsite_and_out_of_scope_pdfs(
    adapter: IllinoisLawReviewAdapter, url: str
) -> None:
    assert adapter._is_preferred_pdf_url(url) is False


def test_registry_routes_illinois_hosts_to_the_adapter() -> None:
    for url in (
        "https://illinoislawreview.org/print/",
        "https://www.illinoislawreview.org/print/vol-2026-no-4/voting-at-work/",
    ):
        assert isinstance(pick_adapter_for(url), IllinoisLawReviewAdapter)
