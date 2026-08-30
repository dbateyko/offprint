from __future__ import annotations

import json
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
COMMUNITY_URL = (
    "https://dashboard.lira.bc.edu/api/communities/"
    "intellectual-property-and-technology-forum"
)
COMMUNITY_ID = "443588c8-3460-434f-8175-b302b38a85b6"
RECORDS_URL = (
    f"https://dashboard.lira.bc.edu/api/communities/{COMMUNITY_ID}/records"
    "?size=100&page=1"
)


def _wordpress_calls(session: "FakeSession") -> list[str]:
    return [url for url in session.calls if "sites.bc.edu" in url]


def _lira_record(
    record_id: str,
    title: str,
    filename: str,
    *,
    journal_title: str = "Boston College Intellectual Property and Technology Forum",
) -> dict:
    return {
        "id": record_id,
        "metadata": {
            "title": title,
            "publication_date": "2026-06-24",
            "creators": [{"person_or_org": {"name": "Pulkowski, Emma"}}],
        },
        "custom_fields": {
            "journal:journal": {"title": journal_title, "volume": "2026", "pages": "1-20"}
        },
        "files": {
            "enabled": True,
            "entries": {
                filename: {
                    "key": filename,
                    "ext": "pdf",
                    "mimetype": "application/pdf",
                    "access": {"hidden": False},
                }
            }
        },
    }


def _community_session(records: list[dict]) -> "FakeSession":
    return FakeSession(
        {
            COMMUNITY_URL: FakeResponse(json.dumps({"id": COMMUNITY_ID}), COMMUNITY_URL),
            RECORDS_URL: FakeResponse(
                json.dumps({"hits": {"total": len(records), "hits": records}}),
                RECORDS_URL,
            ),
        }
    )


def test_lira_community_route_is_primary_and_yields_every_record() -> None:
    records = [
        _lira_record("0jw28-de474", "Reclaiming Identity", "Pulkowski_Reclaiming Identity.pdf"),
        _lira_record("5vhce-p5y15", "Intelligent Agents and Copyright", "INTELLIGENT_AGENTS.pdf"),
    ]
    session = _community_session(records)

    results = list(BostonCollegeIPTFAdapter(session=session).discover_pdfs(ARCHIVE_URL))

    assert [r.pdf_url for r in results] == [
        "https://lira.bc.edu/downloads/0jw28-de474/Pulkowski_Reclaiming%20Identity.pdf",
        "https://lira.bc.edu/downloads/5vhce-p5y15/INTELLIGENT_AGENTS.pdf",
    ]
    assert results[0].extraction_path == "lira_iptf_community_record"
    assert results[0].metadata["authors"] == ["Pulkowski, Emma"]
    assert results[0].metadata["year"] == "2026"
    assert results[0].metadata["volume"] == "2026"
    # The WordPress archive must not be touched when the repository route works.
    assert _wordpress_calls(session) == []


def test_lira_route_rejects_records_belonging_to_another_journal() -> None:
    records = [
        _lira_record(
            "aaaaa-bbbbb",
            "Some Other Journal Article",
            "OTHER.pdf",
            journal_title="Boston College Law Review",
        ),
        _lira_record("ccccc-ddddd", "Masthead", "MASTHEAD.pdf"),
    ]
    session = _community_session(records)

    results = list(BostonCollegeIPTFAdapter(session=session).discover_pdfs(ARCHIVE_URL))

    assert results == []


def test_lira_403_stops_discovery_without_wordpress_fallback() -> None:
    session = FakeSession(
        {COMMUNITY_URL: FakeResponse("blocked", COMMUNITY_URL, 403)}
    )
    adapter = BostonCollegeIPTFAdapter(session=session)

    assert list(adapter.discover_pdfs(ARCHIVE_URL)) == []
    assert adapter._stop_discovery is True
    assert _wordpress_calls(session) == []


def test_wordpress_fallback_caps_article_fetches() -> None:
    cards = "".join(
        f'<article class="post"><h2 class="entry-title">'
        f'<a href="/iptf/2020/01/{n:02d}/post-{n}/">Article {n}</a></h2></article>'
        for n in range(1, 81)
    )
    pages = {ARCHIVE_URL: FakeResponse(f'<nav data-pagination-max-pages="1"></nav>{cards}', ARCHIVE_URL)}
    for n in range(1, 81):
        url = f"https://sites.bc.edu/iptf/2020/01/{n:02d}/post-{n}/"
        pages[url] = FakeResponse("<article><p>no repository link</p></article>", url)
    session = FakeSession(pages)

    results = list(BostonCollegeIPTFAdapter(session=session).discover_pdfs(ARCHIVE_URL))

    assert results == []
    post_fetches = [url for url in session.calls if "/post-" in url]
    assert len(post_fetches) == BostonCollegeIPTFAdapter.MAX_WORDPRESS_ARTICLE_FETCHES

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
    assert _wordpress_calls(session) == [ARCHIVE_URL]


def test_stops_immediately_on_403_or_429_listing() -> None:
    for status in (403, 429):
        session = FakeSession({ARCHIVE_URL: FakeResponse("blocked", ARCHIVE_URL, status)})
        adapter = BostonCollegeIPTFAdapter(session=session)

        assert list(adapter.discover_pdfs(ARCHIVE_URL)) == []
        assert _wordpress_calls(session) == [ARCHIVE_URL]


def test_rejects_seed_outside_iptf_publication_scope() -> None:
    session = FakeSession({})

    results = list(
        BostonCollegeIPTFAdapter(session=session).discover_pdfs(
            "https://sites.bc.edu/unrelated/"
        )
    )

    assert results == []
    assert session.calls == []
