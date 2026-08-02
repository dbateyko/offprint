import json
from datetime import datetime, timezone

from offprint.digital_commons_gather import (
    AdaptivePacer,
    GatherItem,
    canonical_dc_url,
    fair_round_robin,
    gather_id_for_url,
    load_success_ids,
    parse_retry_after,
    PersistentDigitalCommonsBrowser,
)


def test_canonical_dc_url_ignores_synthetic_download_parameters():
    base = "https://Example.edu/cgi/viewcontent.cgi?article=12&context=lr"
    assert canonical_dc_url(base + "&type=pdf") == canonical_dc_url(base + "&download=1")
    assert gather_id_for_url(base + "&type=pdf") == gather_id_for_url(base)


def test_fair_round_robin_interleaves_domains():
    items = [
        GatherItem(str(i), domain, f"https://{domain}/p/{i}", f"https://{domain}/f/{i}")
        for i, domain in enumerate(["a.edu", "a.edu", "b.edu", "b.edu", "c.edu"])
    ]
    assert [item.domain for item in fair_round_robin(items)] == [
        "a.edu",
        "b.edu",
        "c.edu",
        "a.edu",
        "b.edu",
    ]


def test_adaptive_pacer_decreases_only_after_success_threshold():
    pacer = AdaptivePacer(
        start_delay_seconds=60,
        min_delay_seconds=10,
        successes_before_decrease=2,
    )
    pacer.record_success()
    assert pacer.delay_seconds == 60
    pacer.record_success()
    assert pacer.delay_seconds == 30
    pacer.record_pressure()
    assert pacer.delay_seconds == 60
    pacer.record_pressure(300)
    assert pacer.delay_seconds == 300


def test_load_success_ids_only_resumes_completed_downloads(tmp_path):
    path = tmp_path / "attempts.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"gather_id": "ok", "status": "downloaded"}),
                json.dumps({"gather_id": "retry", "status": "deferred"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    assert load_success_ids(path) == {"ok"}


def test_parse_retry_after_supports_seconds_and_http_date():
    now = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
    assert parse_retry_after("120", now=now) == 120
    assert parse_retry_after("Sun, 02 Aug 2026 12:05:00 GMT", now=now) == 300
    assert parse_retry_after("not-a-date", now=now) == 0


def test_download_link_lookup_never_falls_back_to_wrong_article():
    class FakeLink:
        def __init__(self, href):
            self.href = href

        def get_attribute(self, name):
            return self.href if name == "href" else None

    class FakeLocators:
        def __init__(self, links):
            self.links = links

        def count(self):
            return len(self.links)

        def nth(self, index):
            return self.links[index]

    class FakePage:
        def locator(self, selector):
            if selector == 'a[href*="viewcontent.cgi"]':
                return FakeLocators(
                    [FakeLink("https://example.edu/cgi/viewcontent.cgi?article=1&context=lr")]
                )
            return FakeLocators([])

    browser = PersistentDigitalCommonsBrowser()
    browser._page = FakePage()
    requested = "https://example.edu/cgi/viewcontent.cgi?article=2&context=lr&type=pdf"
    assert browser._find_download_link(requested) is None


def test_landing_metadata_is_captured_for_article_pages_only():
    class FakePage:
        def content(self):
            return (
                '<html><head><meta name="citation_title" content="Article Title">'
                '<meta name="citation_publication_date" content="2024-01-01">'
                "</head></html>"
            )

        def title(self):
            return "Fallback"

    browser = PersistentDigitalCommonsBrowser()
    browser._page = FakePage()
    article = GatherItem(
        "id",
        "example.edu",
        "https://example.edu/lr/vol1/iss1/1/",
        "https://example.edu/cgi/viewcontent.cgi?article=1&context=lr",
        dc_source="siteindex",
    )
    issue = GatherItem(
        "issue",
        "example.edu",
        "https://example.edu/lr/vol1/iss1/",
        "https://example.edu/cgi/viewcontent.cgi?article=2&context=lr",
        dc_source="all_issues",
    )
    assert browser._extract_landing_metadata(article)["title"] == "Article Title"
    assert browser._extract_landing_metadata(article)["year"] == "2024"
    assert browser._extract_landing_metadata(issue) == {}
