from __future__ import annotations

import pytest

from offprint.adapters.base import DiscoveryResult
from offprint.adapters.depaul_jatip import DePaulJATIPAdapter
from offprint.adapters.digital_commons_origin_fetch import DigitalCommonsOriginFetchAdapter


def discovery(page_url: str, pdf_url: str, title: str) -> DiscoveryResult:
    return DiscoveryResult(
        page_url=page_url,
        pdf_url=pdf_url,
        metadata={"title": title, "authors": ["Example Author"]},
    )


def test_discovery_is_limited_to_jatip_and_filters_furniture(monkeypatch) -> None:
    results = [
        discovery(
            "https://via.library.depaul.edu/jatip/vol36/iss1/1/",
            "https://via.library.depaul.edu/cgi/viewcontent.cgi?article=1684&context=jatip&type=pdf",
            "Front Matter",
        ),
        discovery(
            "https://via.library.depaul.edu/jatip/vol36/iss1/2/",
            "https://via.library.depaul.edu/cgi/viewcontent.cgi?article=1688&context=jatip&type=pdf",
            "Systemic Failure and Synthetic Abuse",
        ),
        discovery(
            "https://via.library.depaul.edu/law-review/vol75/iss1/1/",
            "https://via.library.depaul.edu/cgi/viewcontent.cgi?article=9999&context=law-review&type=pdf",
            "Unrelated DePaul Law Review Article",
        ),
    ]
    monkeypatch.setattr(
        "offprint.adapters.digital_commons_origin_fetch."
        "DigitalCommonsOriginFetchAdapter.discover_pdfs",
        lambda self, seed_url, max_depth=0: iter(results),
    )

    discovered = list(
        DePaulJATIPAdapter().discover_pdfs(
            "https://via.library.depaul.edu/jatip/all_issues.html"
        )
    )

    assert [item.metadata["title"] for item in discovered] == [
        "Systemic Failure and Synthetic Abuse"
    ]


def test_rejects_host_root_and_other_publication_seeds() -> None:
    adapter = DePaulJATIPAdapter()

    with pytest.raises(ValueError, match="/jatip publication seed"):
        list(adapter.discover_pdfs("https://via.library.depaul.edu/"))
    with pytest.raises(ValueError, match="/jatip publication seed"):
        list(adapter.discover_pdfs("https://via.library.depaul.edu/law-review/"))


def test_download_rejects_non_jatip_context(tmp_path) -> None:
    with pytest.raises(ValueError, match="outside context=jatip"):
        DePaulJATIPAdapter().download_pdf(
            "https://via.library.depaul.edu/cgi/viewcontent.cgi?"
            "article=9999&context=law-review&type=pdf",
            str(tmp_path),
        )


def test_first_delivery_failure_halts_later_requests(monkeypatch, tmp_path) -> None:
    calls = []
    DePaulJATIPAdapter._delivery_halted = False
    monkeypatch.setattr(
        DigitalCommonsOriginFetchAdapter,
        "download_pdf",
        lambda self, pdf_url, out_dir, **kwargs: calls.append(pdf_url),
    )
    monkeypatch.setattr(DePaulJATIPAdapter, "_apply_waf_cooldown", lambda self: None)
    adapter = DePaulJATIPAdapter()
    url = (
        "https://via.library.depaul.edu/cgi/viewcontent.cgi?"
        "article=1688&context=jatip&type=pdf"
    )

    assert adapter.download_pdf(url, str(tmp_path)) is None
    assert adapter.download_pdf(url, str(tmp_path)) is None

    assert calls == [url]
    assert adapter.last_download_meta["waf_action"] == "halt_publication_lane"
    DePaulJATIPAdapter._delivery_halted = False
