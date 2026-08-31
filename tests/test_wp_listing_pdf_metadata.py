"""Per-PDF metadata on flat archive listings.

Harvard CR-CL publishes its whole run as 315 links on one /archive/ page. Before
this, every one of them inherited the page's own metadata, so all 315 records
landed with title "Archive" and a single shared author.
"""
from bs4 import BeautifulSoup

from offprint.adapters.wordpress_academic_base import WordPressAcademicBaseAdapter


def _adapter():
    adapter = WordPressAcademicBaseAdapter()
    adapter.domain = "journals.law.harvard.edu"
    return adapter


def _extract(html):
    return _adapter()._extract_listing_pdf_metadata(
        BeautifulSoup(html, "html.parser"), "https://journals.law.harvard.edu/crcl/archive/"
    )


def test_anchor_text_becomes_title_and_trailing_byline_is_parsed():
    html = ('<p><strong><a href="/crcl/uploads/shah.pdf">'
            'Envisioning a Protective Administrative Law Framework</a></strong> by Bijal Shah</p>')
    meta = _extract(html)
    assert len(meta) == 1
    entry = next(iter(meta.values()))
    assert entry["title"] == "Envisioning a Protective Administrative Law Framework"
    assert entry["authors"] == ["Bijal Shah"]


def test_each_pdf_gets_its_own_title():
    html = ("".join(
        f'<p><strong><a href="/u/{n}.pdf">Distinct Article Title Number {n}</a></strong>'
        f' by Author {n}</p>' for n in range(5)))
    meta = _extract(html)
    assert len(meta) == 5
    assert len({m["title"] for m in meta.values()}) == 5


def test_label_anchors_are_not_mistaken_for_titles():
    # A bare "PDF"/"Download" link names nothing; the page-level title must stand.
    for label in ("PDF", "Download", "Full Text", "here"):
        assert _extract(f'<p>Some heading <a href="/a.pdf">{label}</a></p>') == {}


def test_block_with_two_pdfs_is_left_alone():
    # Ambiguous: a title cannot be attributed to either link.
    html = ('<p><a href="/a.pdf">Some Long Article Title Here</a>'
            '<a href="/b.pdf">Another Long Article Title</a></p>')
    assert _extract(html) == {}


def test_missing_byline_still_yields_a_title():
    meta = _extract('<p><a href="/a.pdf">An Article With No Stated Byline</a></p>')
    entry = next(iter(meta.values()))
    assert entry["title"] == "An Article With No Stated Byline"
    assert "authors" not in entry


def test_an_unrecognised_listing_donates_no_metadata_at_all():
    """The safety net beneath per-PDF extraction.

    `_extract_listing_pdf_metadata` handles listing shapes it recognises. When
    it does not recognise one, the page-level <title> and byline must NOT be
    stamped onto every PDF instead: that is how 264 distinct Harvard articles
    came to share the title "Archive" and one author's name. A blank beats a
    false attribution.
    """
    html = "<html><head><title>Archive</title><body>" + "".join(
        f'<div><a href="/crcl/uploads/a{i}.pdf">Download</a></div>' for i in range(12)
    ) + "</body></html>"
    adapter = _adapter()
    results = list(adapter._extract_pdfs_from_article(
        BeautifulSoup(html, "html.parser"),
        "https://journals.law.harvard.edu/crcl/archive/",
        "Archive",
    ))
    assert results, "the PDFs themselves must still be discovered"
    for result in results:
        assert not result.metadata.get("title")
        assert not result.metadata.get("authors")


def test_a_real_article_page_still_gets_its_title():
    """One or two PDFs on a page is an article, not a listing."""
    html = ('<html><head><title>A Real Article</title><body>'
            '<a href="/crcl/uploads/x.pdf">PDF</a>'
            '<a href="/crcl/uploads/x-appendix.pdf">Appendix PDF</a>'
            '</body></html>')
    adapter = _adapter()
    results = list(adapter._extract_pdfs_from_article(
        BeautifulSoup(html, "html.parser"),
        "https://journals.law.harvard.edu/crcl/2020/01/a-real-article/",
        "A Real Article",
    ))
    assert results
    assert any(r.metadata.get("title") for r in results)


def test_anchor_text_is_not_welded_at_the_seam():
    """`get_text(strip=True)` produced "Fee-Shifting Shortcutsby Maureen Carroll"."""
    html = ('<h2 class="entry-title"><a href="/crcl/2020/01/fee-shifting/">'
            '<span>Fee-Shifting Shortcuts</span><span>by Maureen Carroll</span>'
            '</a></h2>')
    soup = BeautifulSoup(html, "html.parser")
    link = soup.select_one("h2.entry-title a")
    assert link.get_text(" ", strip=True) == "Fee-Shifting Shortcuts by Maureen Carroll"
    assert link.get_text(strip=True) == "Fee-Shifting Shortcutsby Maureen Carroll"
