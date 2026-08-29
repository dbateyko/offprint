"""Origin gating for GenericAdapter PDF discovery.

A reference-list link is not an article. Before this gate, ``direct_pdf_url``
accepted any ``.pdf`` href on any host, and the embedded-PDF sweep accepted any
absolute ``.pdf`` URL found in page script/JSON. Either path would record a
third-party PDF cited in a footnote as an article of the seeded journal -- the
2026-08-24 OJS scope-leak shape, in the repo's most widely used adapter.

The two off-host URLs below are the real ones from that incident.
"""

import pytest

from offprint.adapters.generic import GenericAdapter


LEAKED = [
    "https://www.katowice.sa.gov.pl/container/some-judgment.pdf",
    "https://lirias.kuleuven.be/retrieve/123456/paper.pdf",
]


@pytest.fixture
def adapter():
    return GenericAdapter()


@pytest.mark.parametrize("url", LEAKED)
def test_real_leaked_urls_are_rejected(adapter, url):
    assert not adapter._pdf_host_in_scope(url, "journals.muni.cz")


def test_same_origin_pdf_is_accepted(adapter):
    assert adapter._pdf_host_in_scope(
        "https://journals.muni.cz/mujlt/article/download/1/2.pdf", "journals.muni.cz"
    )


def test_www_variant_of_seed_host_is_accepted(adapter):
    assert adapter._pdf_host_in_scope(
        "https://www.example.edu/a.pdf", "example.edu"
    )
    assert adapter._pdf_host_in_scope(
        "https://example.edu/a.pdf", "www.example.edu"
    )


def test_unrelated_host_sharing_a_prefix_is_rejected(adapter):
    """removeprefix, not lstrip: 'wwe.com' must not collapse to 'e.com'."""
    assert not adapter._pdf_host_in_scope("https://wwe.com/a.pdf", "e.com")


def test_institutional_repository_is_allowed(adapter):
    assert adapter._pdf_host_in_scope(
        "https://scholarship.law.duke.edu/cgi/viewcontent.cgi?article=1", "dlj.law.duke.edu"
    )


def test_empty_and_relative_urls_are_rejected(adapter):
    assert not adapter._pdf_host_in_scope("", "example.edu")
    assert not adapter._pdf_host_in_scope("/local/file.pdf", "example.edu")
