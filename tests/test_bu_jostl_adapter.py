from __future__ import annotations

from dataclasses import dataclass

from offprint.adapters.bu_jostl import BostonUniversityJOSTLAdapter


@dataclass
class FakeResponse:
    text: str
    url: str
    status_code: int = 200


class FakeSession:
    def __init__(self, pages: dict[str, str]):
        self.pages = pages

    def get(self, url: str, **kwargs) -> FakeResponse:
        return FakeResponse(self.pages[url], url)


ARCHIVE_URL = "https://www.bu.edu/jostl/archives/"
ISSUE_URL = "https://www.bu.edu/jostl/archives/vol-27-1-winter-2021/"


def test_discovers_collapsible_issue_pages_and_article_metadata() -> None:
    archive = f"""
    <article><a href="{ISSUE_URL}">Vol. 27.1 - Winter 2021</a></article>
    <div class="bu_collapsible_section">
      <a href="/jostl/archives/winter-2009/">Vol. 15.1 - Winter 2009</a>
    </div>
    """
    issue = """
    <article><h1>Vol. 27.1 - Winter 2021</h1>
      <p><strong><a href="/jostl/files/2021/06/1-Altman.pdf">
        1. What a Hybrid Analysis Teaches Us
      </a></strong></p>
      <p>MICAH ALTMAN, ALONI COHEN</p>
    </article>
    """
    winter = """
    <article><h1>Vol. 15.1 - Winter 2009</h1>
      <p><a href="/jostl/files/2015/02/Caudill_WEB_151.pdf">Arsenic and Old Chemistry</a>
      <br>by David S. Caudill</p>
    </article>
    """
    adapter = BostonUniversityJOSTLAdapter(
        session=FakeSession({ARCHIVE_URL: archive, ISSUE_URL: issue,
                             "https://www.bu.edu/jostl/archives/winter-2009/": winter})
    )

    results = list(adapter.discover_pdfs(ARCHIVE_URL))

    assert len(results) == 2
    assert results[0].metadata == {
        "volume": "27",
        "date": "2021",
        "issue": "1",
        "title": "What a Hybrid Analysis Teaches Us",
        "authors": ["MICAH ALTMAN, ALONI COHEN"],
        "source_url": ISSUE_URL,
        "url": ISSUE_URL,
    }
    assert results[1].metadata["authors"] == ["David S. Caudill"]


def test_embedded_issue_pdf_and_google_drive_preview_are_supported() -> None:
    issue = """
    <article><h1>Volume 29.1 - Winter - 2023</h1>
      <iframe src="https://drive.google.com/file/d/abc_123/preview"></iframe>
    </article>
    """
    adapter = BostonUniversityJOSTLAdapter(session=FakeSession({ISSUE_URL: issue}))

    result = next(adapter.discover_pdfs(ISSUE_URL))

    assert result.pdf_url == (
        "https://drive.usercontent.google.com/download?id=abc_123&export=download"
    )
    assert result.metadata["title"].endswith("Volume 29 Issue 1")
    assert result.metadata["volume"] == "29"
    assert result.metadata["date"] == "2023"
    assert result.metadata["document_type"] == "issue_compilation"


def test_correct_seed_is_jostl_archives_not_jstl_archive() -> None:
    assert ARCHIVE_URL == "https://www.bu.edu/jostl/archives/"
    assert "/jstl/" not in ARCHIVE_URL


def test_duplicate_empty_anchor_does_not_replace_titled_anchor() -> None:
    issue = """
    <article><h1>Vol. 27.1 - Winter 2021</h1>
      <p><a href="/jostl/files/5-Howard.pdf"></a></p>
      <p><a href="/jostl/files/5-Howard.pdf">5. Frand, Rand, and the Problem at Hand</a>
      <br>by Samuel Howard</p>
    </article>
    """
    adapter = BostonUniversityJOSTLAdapter(session=FakeSession({ISSUE_URL: issue}))

    result = next(adapter.discover_pdfs(ISSUE_URL))

    assert result.metadata["title"] == "Frand, Rand, and the Problem at Hand"
    assert result.metadata["authors"] == ["Samuel Howard"]
