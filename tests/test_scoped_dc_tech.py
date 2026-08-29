from __future__ import annotations

import pytest

from offprint.adapters.base import DiscoveryResult
from offprint.adapters.digital_commons_origin_fetch import DigitalCommonsOriginFetchAdapter
from offprint.adapters.scoped_dc_tech import CaseJOLTIAdapter, SeattleJTEILAdapter


def result(page: str, pdf: str, title: str) -> DiscoveryResult:
    return DiscoveryResult(page_url=page, pdf_url=pdf, metadata={"title": title})


@pytest.mark.parametrize(
    "adapter,seed,slug",
    [
        (
            SeattleJTEILAdapter,
            "https://digitalcommons.law.seattleu.edu/sjteil/all_issues.html",
            "sjteil",
        ),
        (
            CaseJOLTIAdapter,
            "https://scholarlycommons.law.case.edu/jolti/all_issues.html",
            "jolti",
        ),
    ],
)
def test_discovery_is_exactly_publication_scoped(monkeypatch, adapter, seed, slug) -> None:
    host = adapter.HOST
    discovered = [
        result(
            f"https://{host}/{slug}/vol1/iss1/1/",
            f"https://{host}/cgi/viewcontent.cgi?article=1001&context={slug}",
            "A Substantive Article",
        ),
        result(
            f"https://{host}/{slug}/vol1/iss1/2/",
            f"https://{host}/cgi/viewcontent.cgi?article=1002&context={slug}",
            "Masthead 2025-2026",
        ),
        result(
            f"https://{host}/other/vol1/iss1/1/",
            f"https://{host}/cgi/viewcontent.cgi?article=1003&context=other",
            "Other Journal",
        ),
    ]
    monkeypatch.setattr(
        DigitalCommonsOriginFetchAdapter,
        "discover_pdfs",
        lambda self, seed_url, max_depth=0: iter(discovered),
    )

    items = list(adapter().discover_pdfs(seed))

    assert [item.metadata["title"] for item in items] == ["A Substantive Article"]


def test_rejects_cross_publication_download(tmp_path) -> None:
    with pytest.raises(ValueError, match="outside context=sjteil"):
        SeattleJTEILAdapter().download_pdf(
            "https://digitalcommons.law.seattleu.edu/cgi/viewcontent.cgi?"
            "article=9999&context=sjsj",
            str(tmp_path),
        )
