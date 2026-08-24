from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from offprint.adapters.base import DiscoveryResult
from offprint.adapters.ohio_state_technology import OhioStateTechnologyLawJournalAdapter


class StubOhioAdapter(OhioStateTechnologyLawJournalAdapter):
    def _get_json(
        self, url: str, *, params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        assert url.endswith(f"/{self.COMMUNITY_UUID}/collections")
        return {
            "_embedded": {
                "collections": [
                    {
                        "id": "issue-one",
                        "name": "I/S: Volume 1, Issue 2-3 (Spring/Summer 2005)",
                    },
                    {
                        "id": "issue-two",
                        "name": "OSTLJ: Volume 20, Issue 1 (Spring 2024)",
                    },
                ]
            },
            "page": {"number": 0, "totalPages": 1},
        }

    def _iter_dspace_pdf_candidates(
        self, seed_url: str, *, scope_id: Optional[str] = None
    ) -> Iterable[DiscoveryResult]:
        if scope_id == "issue-one":
            yield DiscoveryResult(
                page_url="https://kb.osu.edu/items/front",
                pdf_url="https://kb.osu.edu/front.pdf",
                metadata={"title": "Front Matter with Masthead"},
            )
            yield DiscoveryResult(
                page_url="https://kb.osu.edu/items/article-one",
                pdf_url="https://kb.osu.edu/article-one.pdf",
                metadata={"title": "A Useful Article", "authors": ["A. Author"]},
            )
        elif scope_id == "issue-two":
            yield DiscoveryResult(
                page_url="https://kb.osu.edu/items/article-two",
                pdf_url="https://kb.osu.edu/article-two.pdf",
                metadata={"title": "A Newer Article", "year": "2023"},
            )


class EmptyIssueStubOhioAdapter(StubOhioAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.visited_scopes: list[str] = []

    def _iter_dspace_pdf_candidates(
        self, seed_url: str, *, scope_id: Optional[str] = None
    ) -> Iterable[DiscoveryResult]:
        self.visited_scopes.append(str(scope_id))
        return iter(())


class Response:
    def __init__(self, *, text: str = "", payload=None) -> None:
        self.text = text
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class FallbackSession:
    def __init__(self) -> None:
        self.calls = []

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        if "cdx/search/cdx" in url:
            return Response(
                payload=[
                    ["timestamp", "original", "digest"],
                    [
                        "20180123223109",
                        "http://moritzlaw.osu.edu/students/groups/is/files/2017/02/"
                        "ISjournalv12no2-5-Hunt.pdf",
                        "digest-one",
                    ],
                    [
                        "20180125051938",
                        "http://moritzlaw.osu.edu/students/groups/is/files/2017/11/"
                        "0-13-2-Cover.pdf",
                        "digest-two",
                    ],
                    [
                        "20180125051943",
                        "https://unrelated.example/files/not-ostlj.pdf",
                        "digest-three",
                    ],
                ]
            )
        return Response(
            text="""
            <li class="bux-journal-feed__item">
              <a href="/sites/default/files/2026-07/article.pdf">
                <div class="bux-journal-feed__item-content-author">Paul Taylor</div>
                <h3 class="bux-journal-feed__item-content-title">A Real Article</h3>
              </a>
            </li>
            <li class="bux-journal-feed__item">
              <a href="https://equity.osu.edu/unrelated.pdf">
                <h3 class="bux-journal-feed__item-content-title">Policy Furniture</h3>
              </a>
            </li>
            """
        )


class FallbackOhioAdapter(OhioStateTechnologyLawJournalAdapter):
    def _iter_issue_collections(self, seed_url: str):
        return iter(())


def test_issue_metadata_handles_year_ranges() -> None:
    adapter = OhioStateTechnologyLawJournalAdapter()

    assert adapter._issue_metadata("I/S: Volume 1, Issue 2-3 (Spring/Summer 2005)") == {
        "volume": "1",
        "issue": "2-3",
        "year": "2005",
    }
    assert adapter._issue_metadata("I/S: Volume 1, Issue 1 (Winter 2004/2005)") == {
        "volume": "1",
        "issue": "1",
        "year": "2005",
    }


def test_discovers_each_issue_collection_filters_furniture_and_enriches_metadata() -> None:
    results = list(
        StubOhioAdapter().discover_pdfs(
            "https://kb.osu.edu/communities/a3767fe3-6fcd-5776-bbe7-44d144fb641a"
        )
    )

    assert [result.pdf_url for result in results] == [
        "https://kb.osu.edu/article-one.pdf",
        "https://kb.osu.edu/article-two.pdf",
    ]
    assert results[0].metadata == {
        "title": "A Useful Article",
        "authors": ["A. Author"],
        "journal": "Ohio State Technology Law Journal",
        "volume": "1",
        "issue": "2-3",
        "year": "2005",
    }
    # Item-level metadata remains authoritative when present.
    assert results[1].metadata["year"] == "2023"
    assert results[1].metadata["volume"] == "20"


def test_stops_after_two_empty_issue_scopes_without_unscoped_fallback() -> None:
    adapter = EmptyIssueStubOhioAdapter()

    adapter._iter_moritz_live_articles = lambda: iter(())  # type: ignore[method-assign]
    adapter._iter_wayback_legacy_articles = lambda: iter(())  # type: ignore[method-assign]
    assert list(adapter.discover_pdfs("https://kb.osu.edu/handle/1811/72602")) == []
    assert adapter.visited_scopes == ["issue-one", "issue-two"]


def test_fallback_is_bounded_to_live_and_legacy_ostlj_publication_paths() -> None:
    session = FallbackSession()
    results = list(
        FallbackOhioAdapter(session=session).discover_pdfs(
            "https://kb.osu.edu/handle/1811/72602"
        )
    )

    assert [result.extraction_path for result in results] == [
        "moritz_ostlj_live_articles",
        "wayback_ostlj_legacy_pdf_inventory",
    ]
    assert results[0].metadata["authors"] == ["Paul Taylor"]
    assert results[0].metadata["year"] == "2026"
    assert results[1].metadata["volume"] == "12"
    assert results[1].metadata["issue"] == "2"
    cdx_params = session.calls[-1][1]["params"]
    assert cdx_params["url"] == "moritzlaw.osu.edu/students/groups/is/files/*"
    assert cdx_params["filter"] == ["statuscode:200", "mimetype:application/pdf"]


def test_archived_download_uses_exact_cdx_replay(monkeypatch, tmp_path) -> None:
    adapter = FallbackOhioAdapter(session=FallbackSession())
    results = list(adapter.discover_pdfs("https://kb.osu.edu/handle/1811/72602"))
    archived = results[1]
    captured = {}

    def fake_download(url: str, out_dir: str, **kwargs):
        captured["url"] = url
        return str(tmp_path / "article.pdf")

    monkeypatch.setattr(adapter, "_download_with_generic", fake_download)
    adapter.download_pdf(archived.pdf_url, str(tmp_path))

    assert captured["url"] == (
        "https://web.archive.org/web/20180123223109id_/"
        "http://moritzlaw.osu.edu/students/groups/is/files/2017/02/"
        "ISjournalv12no2-5-Hunt.pdf"
    )
