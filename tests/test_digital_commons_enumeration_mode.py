from contextlib import ExitStack
from unittest.mock import Mock, patch

from bs4 import BeautifulSoup

from offprint.adapters.base import DiscoveryResult
from offprint.adapters.digital_commons_issue_article_hop import (
    DigitalCommonsIssueArticleHopAdapter,
)


def test_all_issues_only_uses_publication_html_without_wider_enumerators():
    adapter = DigitalCommonsIssueArticleHopAdapter()
    adapter.configure_dc(enum_mode="all_issues_only")
    expected = DiscoveryResult(
        page_url="https://example.edu/lr/vol1/iss1/1/",
        pdf_url="https://example.edu/cgi/viewcontent.cgi?article=1&context=lr",
        metadata={"title": "Example"},
    )
    adapter._discover_via_all_issues = Mock(return_value=iter([expected]))

    with ExitStack() as stack:
        discover_oai = stack.enter_context(
            patch("offprint.adapters.digital_commons_base.discover_oai")
        )
        discover_sitemap = stack.enter_context(
            patch("offprint.adapters.digital_commons_base.discover_sitemap")
        )
        discover_html = stack.enter_context(
            patch("offprint.digital_commons_enumerator.discover_html")
        )
        results = list(adapter.discover_pdfs("https://example.edu/lr/"))

    assert results == [expected]
    assert results[0].source_adapter == "DigitalCommonsIssueArticleHopAdapter"
    assert results[0].extraction_path == "all_issues"
    discover_oai.assert_not_called()
    discover_sitemap.assert_not_called()
    discover_html.assert_not_called()


def test_all_issues_only_does_not_fall_back_to_unbounded_html_crawl():
    adapter = DigitalCommonsIssueArticleHopAdapter()
    adapter.configure_dc(enum_mode="all_issues_only")
    adapter._discover_via_all_issues = Mock(return_value=iter(()))

    with patch("offprint.digital_commons_enumerator.discover_html") as discover_html:
        assert list(adapter.discover_pdfs("https://example.edu/lr/")) == []

    discover_html.assert_not_called()


def test_safe_diagnostic_forces_single_transparent_non_browser_configuration():
    adapter = DigitalCommonsIssueArticleHopAdapter()

    adapter.configure_dc(
        ua_profiles=["browser", "transparent"],
        use_curl_cffi=True,
        waf_browser_fallback=True,
        session_rotate_threshold=300,
        max_attempts_per_profile=3,
        safe_diagnostic=True,
    )

    assert adapter.dc_ua_profiles == ["transparent"]
    assert adapter.dc_use_curl_cffi is False
    assert adapter.dc_waf_browser_fallback is False
    assert adapter.dc_session_rotate_threshold == 0
    assert adapter.dc_max_attempts_per_profile == 1


def test_issue_hop_preserves_article_landing_page_for_referer():
    adapter = DigitalCommonsIssueArticleHopAdapter()
    issue_url = "https://example.edu/lr/vol1/iss1/"
    adapter._get_page = Mock(
        return_value=BeautifulSoup(
            """
            <html><h1>Volume 1, Issue 1 (2020)</h1>
              <div class="doc">
                <p>Article metadata</p>
                <p><a href="/lr/vol1/iss1/1/">A Useful Article</a></p>
                <p class="pdf"><a href="/cgi/viewcontent.cgi?article=1&amp;context=lr">PDF</a></p>
                <span class="auth">Ada Author</span>
              </div>
            </html>
            """,
            "lxml",
        )
    )

    results = list(adapter._process_issue_page(issue_url))

    assert len(results) == 1
    assert results[0].page_url == "https://example.edu/lr/vol1/iss1/1/"
    assert results[0].metadata["url"] == results[0].page_url
