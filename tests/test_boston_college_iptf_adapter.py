from __future__ import annotations

from dataclasses import dataclass

from bs4 import BeautifulSoup

from offprint.adapters.boston_college_iptf import BostonCollegeIPTFAdapter


@dataclass
class FakeResponse:
    text: str
    url: str
    status_code: int = 200


class FakeSession:
    def __init__(self, pages: dict[str, FakeResponse]):
        self.pages = pages
        self.calls: list[str] = []

    def get(self, url: str, **kwargs) -> FakeResponse:
        self.calls.append(url)
        return self.pages[url]


ARCHIVE_URL = "https://sites.bc.edu/iptf/"
ARTICLE_URL = "https://sites.bc.edu/iptf/2026/06/26/reclaiming-identity/"
WORK_URL = "https://lira.bc.edu/works/publication-article/0jw28-de474"


def test_discovers_substantive_iptf_article_through_lira() -> None:
    archive = f"""
    <nav data-pagination-max-pages="1"></nav>
    <article class="post"><h2 class="entry-title"><a href="{ARTICLE_URL}">
      Reclaiming Identity: A Quasi-Intellectual Property Framework
    </a></h2></article>
    """
    article = f"""
    <article><h1>Reclaiming Identity</h1><div class="yuki-article-content">
      <p>Emma Pulkowski</p><a href="{WORK_URL}">Read full text here</a>
    </div></article>
    """
    work = """
    <main><h1>Reclaiming Identity: A Quasi-Intellectual Property Framework</h1>
      <h2>Intellectual Property and Technology Forum</h2>
      <div><h2 id="creators-list">Authors:</h2><div><ul>
        <li><button>Pulkowski, Emma</button></li>
      </ul></div></div>
      <p>Published: <time datetime="2026-06-24">Jun 24, 2026</time></p>
      <a download href="/downloads/0jw28-de474/Pulkowski_ReclaimingIdentity.pdf">PDF</a>
    </main>
    """
    session = FakeSession(
        {
            ARCHIVE_URL: FakeResponse(archive, ARCHIVE_URL),
            ARTICLE_URL: FakeResponse(article, ARTICLE_URL),
            WORK_URL: FakeResponse(work, WORK_URL),
        }
    )

    result = next(BostonCollegeIPTFAdapter(session=session).discover_pdfs(ARCHIVE_URL))

    assert result.pdf_url == (
        "https://lira.bc.edu/downloads/0jw28-de474/Pulkowski_ReclaimingIdentity.pdf"
    )
    assert result.metadata["title"].startswith("Reclaiming Identity")
    assert result.metadata["authors"] == ["Pulkowski, Emma"]
    assert result.metadata["year"] == "2026"
    assert result.metadata["date"] == "2026-06-24"
    assert result.extraction_path == "iptf_post_to_lira_work"


def test_accepts_current_lira_work_sc_redirect_path() -> None:
    current_work_url = "https://lira.bc.edu/work/sc/c1ee995d-ba0f-4698-a66b-fc5bfe0a76ce"
    article = f'<article><a href="{current_work_url}">Read Full Text Here</a></article>'

    assert (
        BostonCollegeIPTFAdapter._lira_work_url(
            BeautifulSoup(article, "lxml"), ARTICLE_URL
        )
        == current_work_url
    )


def test_excludes_blog_staff_and_masthead_cards_without_following_them() -> None:
    archive = """
    <nav data-pagination-max-pages="1"></nav>
    <article class="post category-blog-post"><h2 class="entry-title">
      <a href="/iptf/2026/01/01/blog/">BLOG POST: A technology update</a>
    </h2></article>
    <article class="post"><h2 class="entry-title">
      <a href="/iptf/2025/10/17/staff/">IPTF Journal Staff 2025-26</a>
    </h2></article>
    <article class="post"><h2 class="entry-title">
      <a href="/iptf/2025/09/01/masthead/">Masthead</a>
    </h2></article>
    """
    session = FakeSession({ARCHIVE_URL: FakeResponse(archive, ARCHIVE_URL)})

    results = list(BostonCollegeIPTFAdapter(session=session).discover_pdfs(ARCHIVE_URL))

    assert results == []
    assert session.calls == [ARCHIVE_URL]


def test_stops_immediately_on_403_or_429_listing() -> None:
    for status in (403, 429):
        session = FakeSession({ARCHIVE_URL: FakeResponse("blocked", ARCHIVE_URL, status)})
        adapter = BostonCollegeIPTFAdapter(session=session)

        assert list(adapter.discover_pdfs(ARCHIVE_URL)) == []
        assert session.calls == [ARCHIVE_URL]


def test_rejects_seed_outside_iptf_publication_scope() -> None:
    session = FakeSession({})

    results = list(
        BostonCollegeIPTFAdapter(session=session).discover_pdfs(
            "https://sites.bc.edu/unrelated/"
        )
    )

    assert results == []
    assert session.calls == []
