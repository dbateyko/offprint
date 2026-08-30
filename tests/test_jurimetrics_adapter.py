from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from offprint.adapters.jurimetrics import JurimetricsAdapter


def test_extract_issue_urls_is_bounded_to_season_issue_pages() -> None:
    soup = BeautifulSoup(
        """
        <div class="aba-article-content">
          <a href="/groups/science_technology/resources/jurimetrics/2026-spring/">Spring</a>
          <a href="/groups/science_technology/resources/jurimetrics/2025-fall/">Fall</a>
          <a href="/groups/science_technology/resources/jurimetrics/">Home</a>
          <a href="/renew/">Renew</a>
        </div>
        """,
        "lxml",
    )

    urls = JurimetricsAdapter._extract_issue_urls(
        soup,
        "https://www.americanbar.org/groups/science_technology/resources/jurimetrics/issue-archive/",
    )

    assert urls == [
        "https://www.americanbar.org/groups/science_technology/resources/jurimetrics/2026-spring/",
        "https://www.americanbar.org/groups/science_technology/resources/jurimetrics/2025-fall/",
    ]


def test_extract_issue_metadata_has_volume_issue_and_year() -> None:
    soup = BeautifulSoup("<title>Jurimetrics: Spring 2026 — Volume 65, Issue 3</title>", "lxml")

    metadata = JurimetricsAdapter._extract_issue_metadata(
        soup,
        "https://www.americanbar.org/groups/science_technology/resources/jurimetrics/2026-spring/",
    )

    assert metadata["volume"] == "65"
    assert metadata["issue"] == "3"
    assert metadata["year"] == "2026"


def test_wayback_fallback_uses_latest_exact_pdf_capture(monkeypatch) -> None:
    class Response:
        def __init__(self, *, rows=None, content=b""):
            self._rows = rows
            self.content = content

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return self._rows

    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        if "cdx/search" in url:
            return Response(
                rows=[
                    ["timestamp", "original"],
                    ["20250101", "https://example.test/old.pdf"],
                    ["20260101", "https://example.test/new.pdf"],
                ]
            )
        return Response(content=b"%PDF-archived")

    monkeypatch.setattr("offprint.adapters.jurimetrics.requests.get", fake_get)

    payload, replay_url = JurimetricsAdapter._fetch_pdf_from_wayback(
        "https://example.test/issue.pdf"
    )

    assert payload == b"%PDF-archived"
    assert replay_url == (
        "https://web.archive.org/web/20260101id_/https://example.test/new.pdf"
    )
    assert calls[-1][0] == replay_url


def test_rejects_truncated_linearized_pdf() -> None:
    payload = b"%PDF-1.7\n1 0 obj<</Linearized 1/L 3111355>>\n" + (b"x" * 1_048_000)

    with pytest.raises(ValueError, match="received .* of 3111355 bytes"):
        JurimetricsAdapter._validate_complete_pdf_payload(payload)
