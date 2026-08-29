"""Regression tests for the law.stanford.edu journal-archive adapter.

Every journal on law.stanford.edu (STLR, SJCRCL, SJLBF, SELJ, SLPR, ...) renders
its FacetWP archive with the same Schema.org ScholarlyArticle markup, so the
adapter's schema path is shared. These tests pin the two properties that make
that sharing safe: discovery is origin-gated, and each result carries the owning
publication's name.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from offprint.adapters.stanford_stlr import StanfordSTLRAdapter

SJCRCL_ARCHIVE = (
    "https://law.stanford.edu/stanford-journal-of-civil-rights-civil-liberties-sjcrcl"
    "/sjcrcl-archive/"
)


def _article(pdf_href: str, title: str = "A Note", publisher: str = "Stanford Journal of Civil Rights & Civil Liberties") -> str:
    return f"""
    <article itemscope itemtype="https://schema.org/ScholarlyArticle">
      <div class="li-left-wrap">
        <h2 class="li-title" itemprop="name headline">
          <a href="https://law.stanford.edu/publications/a-note/" itemprop="url sameAs">{title}</a>
        </h2>
        <ul class="li-meta">
          <li itemprop="author" itemscope itemtype="https://schema.org/Person">
            <span itemprop="name">Jane Doe</span>
          </li>
        </ul>
        <ul class="li-meta">
          <li><time itemprop="datePublished dateCreated" datetime="2025-08-12">August 12, 2025</time></li>
          <li><ul class="ptp-attachments">
            <li><a href="{pdf_href}" itemprop="url workExample">Download</a></li>
          </ul></li>
        </ul>
      </div>
      <div class="li-right-wrap">
        <ul class="ptp-meta li-meta">
          <li class="ptp-publisher" itemprop="isPartOf" itemscope itemtype="https://schema.org/Periodical">
            <span itemprop="name">{publisher}</span>
          </li>
          <li itemprop="isPartOf" itemscope itemtype="https://schema.org/PublicationVolume">
            <span itemprop="volumeNumber">Volume 21</span>
            <span itemprop="issueNumber">Issue 2</span>
          </li>
        </ul>
      </div>
    </article>
    """


def _page(*articles: str) -> str:
    return "<html><body>" + "".join(articles) + "</body></html>"


def _adapter(pages: dict) -> StanfordSTLRAdapter:
    adapter = StanfordSTLRAdapter()
    adapter.session = None  # never used: _get_page is stubbed
    adapter._get_page = lambda url: BeautifulSoup(pages.get(url, "<html></html>"), "html.parser")
    adapter._is_likely_pdf_url = lambda url: url.lower().endswith(".pdf")
    return adapter


def test_archive_yields_schema_metadata_with_owning_publication():
    pages = {SJCRCL_ARCHIVE: _page(_article("https://law.stanford.edu/wp-content/uploads/2025/08/note.pdf"))}
    results = list(_adapter(pages).discover_pdfs(SJCRCL_ARCHIVE))

    assert len(results) == 1
    meta = results[0].metadata
    assert meta["title"] == "A Note"
    assert meta["authors"] == ["Jane Doe"]
    assert meta["volume"] == "Volume 21"
    assert meta["issue"] == "Issue 2"
    # Publication scope is auditable downstream: host != journal on law.stanford.edu.
    assert meta["publisher"] == "Stanford Journal of Civil Rights & Civil Liberties"
    assert meta["journal"] == meta["publisher"]


def test_offsite_pdf_links_are_rejected_by_the_origin_gate():
    """A cited third-party PDF must never be attributed to the journal."""
    pages = {
        SJCRCL_ARCHIVE: _page(
            _article("https://lirias.kuleuven.be/retrieve/12345.pdf"),
            _article("https://www.katowice.sa.gov.pl/orzeczenie.pdf"),
            _article("https://law.stanford.edu.evil.example/spoof.pdf"),
            _article("https://law.stanford.edu/wp-content/uploads/2025/08/real.pdf"),
        )
    }
    results = list(_adapter(pages).discover_pdfs(SJCRCL_ARCHIVE))

    assert [r.pdf_url for r in results] == [
        "https://law.stanford.edu/wp-content/uploads/2025/08/real.pdf"
    ]


def test_unpinned_seed_walks_facetwp_pages_until_empty():
    pdf = "https://law.stanford.edu/wp-content/uploads/2025/08/note.pdf"
    pages = {
        SJCRCL_ARCHIVE: _page(_article(pdf)),
        SJCRCL_ARCHIVE + "?_paged=2": _page(_article(pdf)),
        SJCRCL_ARCHIVE + "?_paged=3": "<html><body></body></html>",
    }
    results = list(_adapter(pages).discover_pdfs(SJCRCL_ARCHIVE))

    assert len(results) == 2
    assert results[1].page_url == SJCRCL_ARCHIVE + "?_paged=2"


def test_seed_that_pins_a_page_does_not_walk_pagination():
    """Range-expanded seeds ("?_paged=[1-14]") let the orchestrator own paging."""
    pdf = "https://law.stanford.edu/wp-content/uploads/2025/08/note.pdf"
    pages = {
        SJCRCL_ARCHIVE + "?_paged=1": _page(_article(pdf)),
        SJCRCL_ARCHIVE + "?_paged=2": _page(_article(pdf)),
    }
    results = list(_adapter(pages).discover_pdfs(SJCRCL_ARCHIVE + "?_paged=1"))

    assert len(results) == 1


def test_page_without_schema_markup_falls_back_to_wordpress(monkeypatch):
    calls = []

    class _FakeWP:
        def __init__(self, session=None):
            pass

        def discover_pdfs(self, seed_url, max_depth=0):
            calls.append(seed_url)
            return iter(())

    import offprint.adapters.wordpress_academic_base as wab

    monkeypatch.setattr(wab, "WordPressAcademicBaseAdapter", _FakeWP)

    seed = "https://law.stanford.edu/stanford-environmental-law-journal-selj/selj-archive/"
    list(_adapter({seed: "<html><body><p>no schema here</p></body></html>"}).discover_pdfs(seed))

    assert calls == [seed]
