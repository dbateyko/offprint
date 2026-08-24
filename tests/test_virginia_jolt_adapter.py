from __future__ import annotations

from bs4 import BeautifulSoup

from offprint.adapters.virginia_jolt import VirginiaJOLTAdapter


class FakeResponse:
    def __init__(self, text: str, url: str, content_type: str = "text/html", status: int = 200):
        self.text = text
        self.url = url
        self.status_code = status
        self.headers = {"Content-Type": content_type}

    def close(self):
        pass


class FakeSession:
    def __init__(self, pages: dict[str, FakeResponse]):
        self.pages = pages

    def get(self, url, **kwargs):
        return self.pages[url]


def test_archive_traverses_each_volume_and_deduplicates_downloads():
    archive = "https://www.vjolt.org/archives"
    volume_one = "https://www.vjolt.org/volume-1"
    volume_two = "https://www.vjolt.org/volume-2"
    shared_pdf = "https://virginia.box.com/shared/static/abc.pdf"
    second_pdf = "https://virginia.box.com/shared/static/def.pdf"
    session = FakeSession(
        {
            archive: FakeResponse(
                '<a href="/volume-2">Volume 2</a><a href="/volume-1">Volume 1</a>', archive
            ),
            volume_one: FakeResponse(
                f'<div class="menu-item-title"><a href="{shared_pdf}">Title One - Alice Author</a></div>',
                volume_one,
            ),
            volume_two: FakeResponse(
                f'<a href="{shared_pdf}">duplicate</a>'
                f'<div class="menu-item-title"><a href="{second_pdf}">Title Two - Bob Writer</a></div>',
                volume_two,
            ),
        }
    )

    results = list(VirginiaJOLTAdapter(session=session).discover_pdfs(archive))

    assert [result.pdf_url for result in results] == [shared_pdf, second_pdf]
    assert results[0].metadata["title"] == "Title One"
    assert results[0].metadata["authors"] == ["Alice Author"]
    assert results[0].metadata["volume"] == "1"
    assert results[1].metadata["volume"] == "2"


def test_card_layout_extracts_title_author_issue_and_volume():
    url = "https://www.vjolt.org/vol25"
    pdf = "https://www.vjolt.org/s/v25i5Frye.pdf"
    html = f"""
    <div class="image-card">
      <h4>Volume 25, Issue 5, Spring Issue</h4>
      <h3>Brian L. Frye, Karl Marx, Literary Landlord</h3>
      <a href="{pdf}">Access This Article</a>
    </div>
    """
    result = next(
        VirginiaJOLTAdapter(session=FakeSession({url: FakeResponse(html, url)})).discover_pdfs(url)
    )

    assert result.metadata["title"] == "Karl Marx, Literary Landlord"
    assert result.metadata["authors"] == ["Brian L. Frye"]
    assert result.metadata["volume"] == "25"
    assert result.metadata["issue"] == "5"


def test_extensionless_squarespace_pdf_is_resolved_and_dead_link_is_skipped():
    url = "https://www.vjolt.org/vol28"
    live = "https://www.vjolt.org/s/28-Va-JL.Tech-4-2025-Final"
    dead = "https://www.vjolt.org/s/28-missing"
    resolved = "https://static1.squarespace.com/static/article"
    html = f'<h4><a href="{live}">Driven to Collude</a></h4><a href="{dead}">Missing</a>'
    session = FakeSession(
        {
            url: FakeResponse(html, url),
            live: FakeResponse("", resolved, "application/pdf"),
            dead: FakeResponse("not found", dead, status=404),
        }
    )

    results = list(VirginiaJOLTAdapter(session=session).discover_pdfs(url))

    assert len(results) == 1
    assert results[0].pdf_url == resolved
    assert results[0].metadata["title"] == "Driven to Collude"
    assert results[0].metadata["volume"] == "28"


def test_archive_ignores_non_volume_navigation():
    adapter = VirginiaJOLTAdapter()
    soup = BeautifulSoup(
        '<a href="/volume-1-copy">alias</a><a href="/volume-1">one</a>'
        '<a href="/past-volumes">past</a><a href="/volume-editorial-board">board</a>',
        "lxml",
    )

    assert adapter._archive_volume_pages(soup, "https://www.vjolt.org/archives") == [
        "https://www.vjolt.org/volume-1"
    ]
