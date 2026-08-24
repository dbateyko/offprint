from __future__ import annotations

from offprint.adapters.base import DiscoveryResult
from offprint.adapters.digital_commons_origin_fetch import (
    DigitalCommonsOriginFetchAdapter,
    MichiganTechnologyLawReviewAdapter,
    MinnesotaJLSTAdapter,
)


def result(title: str, page_url: str) -> DiscoveryResult:
    return DiscoveryResult(
        page_url=page_url,
        pdf_url=(
            "https://scholarship.law.umn.edu/cgi/viewcontent.cgi?"
            "article=1583&context=mjlst&type=pdf"
        ),
        metadata={"title": title, "authors": "Example Author"},
    )


def test_publication_root_comes_from_context_parameter() -> None:
    root = DigitalCommonsOriginFetchAdapter._publication_root(
        "https://repository.law.umich.edu/cgi/viewcontent.cgi?"
        "article=1231&context=mttlr&type=pdf"
    )

    assert root == "https://repository.law.umich.edu/mttlr/"


def test_discovery_filters_furniture_and_adds_volume_issue(monkeypatch) -> None:
    furniture = result(
        "Volume 27, Issue 1 Front Matter",
        "https://scholarship.law.umn.edu/mjlst/vol27/iss1/1",
    )
    article = result(
        "Private Standards as Liability Shields",
        "https://scholarship.law.umn.edu/mjlst/vol27/iss1/3",
    )
    monkeypatch.setattr(
        "offprint.adapters.digital_commons_issue_article_hop."
        "DigitalCommonsIssueArticleHopAdapter.discover_pdfs",
        lambda self, seed_url, max_depth=0: iter([furniture, article]),
    )

    discovered = list(MinnesotaJLSTAdapter().discover_pdfs("https://example.test"))

    assert [item.metadata["title"] for item in discovered] == [
        "Private Standards as Liability Shields"
    ]
    assert discovered[0].metadata["volume"] == "27"
    assert discovered[0].metadata["issue"] == "1"


def test_download_writes_stable_context_article_filename(monkeypatch, tmp_path) -> None:
    DigitalCommonsOriginFetchAdapter._next_delivery_at = 0.0
    monkeypatch.setattr(
        MinnesotaJLSTAdapter,
        "_fetch_pdf_from_origin",
        staticmethod(lambda publication_root, pdf_url: b"%PDF-test"),
    )
    adapter = MinnesotaJLSTAdapter()

    path = adapter.download_pdf(
        "https://scholarship.law.umn.edu/cgi/viewcontent.cgi?"
        "article=1583&context=mjlst&type=pdf",
        str(tmp_path),
    )

    assert path == str(tmp_path / "mjlst-1583.pdf")
    assert (tmp_path / "mjlst-1583.pdf").read_bytes() == b"%PDF-test"
    assert adapter.last_download_meta["download_method"] == "playwright_page_origin_fetch"


def test_download_reuses_existing_valid_pdf_without_delivery_slot(
    monkeypatch, tmp_path
) -> None:
    output = tmp_path / "mjlst-1583.pdf"
    output.write_bytes(b"%PDF-existing")
    monkeypatch.setattr(
        MinnesotaJLSTAdapter,
        "_fetch_pdf_from_origin",
        staticmethod(lambda publication_root, pdf_url: (_ for _ in ()).throw(AssertionError)),
    )
    adapter = MinnesotaJLSTAdapter()

    path = adapter.download_pdf(
        "https://scholarship.law.umn.edu/cgi/viewcontent.cgi?"
        "article=1583&context=mjlst&type=pdf",
        str(tmp_path),
    )

    assert path == str(output)
    assert adapter.last_download_meta["download_method"] == "existing_valid_pdf"
    assert adapter.last_download_meta["skipped_duplicate"] is True


def test_delivery_pacing_is_shared_across_repository_subclasses(monkeypatch) -> None:
    clock = {"now": 100.0}
    sleeps = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(
        "offprint.adapters.digital_commons_origin_fetch.time.monotonic",
        lambda: clock["now"],
    )
    monkeypatch.setattr(
        "offprint.adapters.digital_commons_origin_fetch.time.sleep", fake_sleep
    )
    DigitalCommonsOriginFetchAdapter._next_delivery_at = 0.0

    MinnesotaJLSTAdapter._wait_for_delivery_slot()
    MichiganTechnologyLawReviewAdapter._wait_for_delivery_slot()

    assert sleeps == [DigitalCommonsOriginFetchAdapter.DELIVERY_INTERVAL_SECONDS]
    assert DigitalCommonsOriginFetchAdapter._next_delivery_at == 112.0


def test_waf_cooldown_delays_the_next_repository(monkeypatch) -> None:
    clock = {"now": 200.0}
    sleeps = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(
        "offprint.adapters.digital_commons_origin_fetch.time.monotonic",
        lambda: clock["now"],
    )
    monkeypatch.setattr(
        "offprint.adapters.digital_commons_origin_fetch.time.sleep", fake_sleep
    )
    DigitalCommonsOriginFetchAdapter._next_delivery_at = 0.0

    MinnesotaJLSTAdapter._apply_waf_cooldown()
    MichiganTechnologyLawReviewAdapter._wait_for_delivery_slot()

    assert sleeps == [DigitalCommonsOriginFetchAdapter.WAF_COOLDOWN_SECONDS]
    assert DigitalCommonsOriginFetchAdapter._next_delivery_at == 271.0
