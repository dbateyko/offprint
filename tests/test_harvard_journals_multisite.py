"""Publication-scope regression tests for the Harvard journals multisite host.

``journals.law.harvard.edu`` hosts many Harvard journals as WordPress Multisite
subsites sharing one origin, so a same-origin check is NOT a publication-scope
check.  These tests pin the two holes that let another publication's PDF be
attributed to the seeded journal:

* ``WordPressAcademicBaseAdapter._is_preferred_pdf_url`` accepts *any*
  same-origin URL containing ``/wp-content/uploads/`` regardless of seed scope;
* ``_url_matches_seed_scope``'s ``allow_siblings`` escape (triggered by a
  ``/category/`` seed prefix, as in ``/ilj/category/archives/``) accepts any
  same-origin path containing ``/volume-``, ``/issue-``, ``/article/`` etc.

Both are closed by ``HarvardJournalsMultisiteAdapter``.
"""

import pytest

from offprint.adapters import pick_adapter_for
from offprint.adapters.harvard_journals_multisite import HarvardJournalsMultisiteAdapter
from offprint.adapters.wordpress_academic_base import WordPressAcademicBaseAdapter

HOST = "https://journals.law.harvard.edu"


@pytest.mark.parametrize(
    "seed_url,slug",
    [
        (f"{HOST}/ilj/category/archives/", "ilj"),
        (f"{HOST}/ilj/", "ilj"),
        (f"{HOST}/jol/archive/", "jol"),
        (f"{HOST}/lpr/hlprvolumes/", "lpr"),
    ],
)
def test_supported_slugs_route_to_multisite_adapter(seed_url, slug):
    adapter = pick_adapter_for(seed_url)
    assert isinstance(adapter, HarvardJournalsMultisiteAdapter)
    assert adapter.slug == slug
    # base_url must carry the subsite path: REST and Jetpack sitemap endpoints
    # live under it, and the network root site has zero posts.
    assert adapter.base_url == f"{HOST}/{slug}"
    assert adapter.domain == "journals.law.harvard.edu"


def test_unlisted_harvard_journals_keep_their_existing_adapter():
    adapter = pick_adapter_for(f"{HOST}/crcl/")
    assert isinstance(adapter, WordPressAcademicBaseAdapter)
    assert not isinstance(adapter, HarvardJournalsMultisiteAdapter)


@pytest.mark.parametrize(
    "url",
    [
        # Another Harvard journal's uploads directory: same origin, wrong journal.
        f"{HOST}/crcl/wp-content/uploads/sites/85/2020/01/Some-CRCL-Article.pdf",
        f"{HOST}/jlpp/wp-content/uploads/sites/87/2021/03/JLPP-Article.pdf",
        # Another journal's issue page.
        f"{HOST}/crcl/volume-55-issue-1/",
        # Reference-list citations seen in real ILJ/LPR/JOL post bodies.
        "https://www.supremecourt.gov/opinions/21pdf/21a244_hgci.pdf",
        "https://dn790000.ca.archive.org/0/items/foo/bar.pdf",
        "https://fairplayforkids.org/wp-content/uploads/2022/04/designing_for_disorder.pdf",
    ],
)
def test_out_of_publication_pdfs_are_rejected(url):
    adapter = HarvardJournalsMultisiteAdapter(slug="ilj")
    assert adapter._is_preferred_pdf_url(url) is False
    assert adapter._url_matches_seed_scope(url) is False


@pytest.mark.parametrize(
    "url",
    [
        f"{HOST}/ilj/wp-content/uploads/sites/84/HILJ_601_2_Chen.pdf",
        f"{HOST}/ilj/wp-content/uploads/sites/84/2013/10/HILJ_54-2_Mann.pdf",
    ],
)
def test_in_publication_pdfs_are_accepted(url):
    adapter = HarvardJournalsMultisiteAdapter(slug="ilj")
    assert adapter._is_preferred_pdf_url(url) is True


def test_base_adapter_would_have_leaked_cross_journal_uploads():
    """Guards the reason this subclass exists: the base gate is origin-only."""
    base = WordPressAcademicBaseAdapter(base_url=HOST)
    leaked = f"{HOST}/crcl/wp-content/uploads/sites/85/2020/01/Some-CRCL-Article.pdf"
    assert base._is_preferred_pdf_url(leaked) is True


def test_legacy_ilj_issue_permalinks_are_traversable():
    """Legacy ILJ issues are ``issue_50-2`` / ``issue_50-2_brewster`` slugs."""
    from bs4 import BeautifulSoup

    adapter = HarvardJournalsMultisiteAdapter(slug="ilj")
    html = (
        '<div class="entry-content">'
        f'<a href="{HOST}/ilj/2009/06/issue_50-2/">Volume 50, Issue 2</a>'
        f'<a href="{HOST}/ilj/2009/06/issue_50-2_brewster/">Brewster</a>'
        f'<a href="{HOST}/crcl/2009/06/issue_44-2/">Other journal</a>'
        "</div>"
    )
    found = adapter._find_volume_issue_links(BeautifulSoup(html, "lxml"), f"{HOST}/ilj/")
    assert f"{HOST}/ilj/2009/06/issue_50-2/" in found
    assert f"{HOST}/ilj/2009/06/issue_50-2_brewster/" in found
    assert all("/crcl/" not in u for u in found)


@pytest.mark.parametrize(
    "seed_url",
    [
        f"{HOST}/ilj/online",
        f"{HOST}/ilj/online/",
        f"{HOST}/lpr/online-articles/",
    ],
)
def test_online_companion_seeds_keep_previous_routing(seed_url):
    """Sub-publication seeds must not be widened to the whole print subtree."""
    adapter = pick_adapter_for(seed_url)
    assert not isinstance(adapter, HarvardJournalsMultisiteAdapter)
