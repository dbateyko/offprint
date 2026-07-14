from unittest.mock import Mock, patch

from offprint.adapters.digital_commons_issue_article_hop import (
    DigitalCommonsIssueArticleHopAdapter,
)
from offprint.digital_commons_download import download_pdf_dc
from offprint.pipeline.normalization import _normalize_adapter_config, _seed_dc_overrides


class _AllowRobots:
    def is_allowed(self, session, url, user_agent, *, timeout):
        return True


class _Cookies:
    def keys(self):
        return ["original_referer"]


class _Response:
    status_code = 403
    content = b"<html><title>403 Forbidden</title></html>"
    url = "https://example.edu/cgi/viewcontent.cgi?article=1&context=lr&type=pdf"
    cookies = _Cookies()
    headers = {"Content-Type": "text/html", "Server": "Bepress"}


def test_safe_single_profile_403_is_access_denied_with_diagnostics(tmp_path):
    session = Mock()
    session.get.return_value = _Response()

    result = download_pdf_dc(
        session=session,
        pdf_url=_Response.url,
        out_dir=str(tmp_path),
        referer="https://example.edu/lr/vol1/iss1/1/",
        ua_profiles=["transparent"],
        robots_cache=_AllowRobots(),
        max_attempts_per_profile=1,
        use_curl_cffi=False,
        min_domain_delay_ms=0,
        max_domain_delay_ms=0,
    )

    assert session.get.call_count == 1
    assert result["error_type"] == "access_denied"
    assert result["download_status_class"] == "access_denied"
    assert result["blocked_reason"] == "generic_403"
    assert result["response_body_size"] == len(_Response.content)
    assert len(result["response_body_sha256"]) == 64
    assert result["response_cookie_names"] == ["original_referer"]


def test_generic_403_does_not_trigger_browser_fallback(tmp_path):
    adapter = DigitalCommonsIssueArticleHopAdapter()
    adapter.configure_dc(waf_browser_fallback=True)
    outcome = {
        "ok": False,
        "error_type": "access_denied",
        "message": "HTTP status 403 without a recognized challenge marker",
        "status_code": 403,
        "content_type": "text/html",
        "download_status_class": "access_denied",
        "blocked_reason": "generic_403",
        "robots_allowed": True,
    }

    with patch("offprint.adapters.digital_commons_base.download_pdf_dc", return_value=outcome):
        adapter._download_single_with_browser = Mock()
        result = adapter.download_pdf(
            "https://diagnostic.example/cgi/viewcontent.cgi?article=1&context=lr&type=pdf",
            str(tmp_path),
            referer="https://diagnostic.example/lr/vol1/iss1/1/",
        )

    assert result is None
    adapter._download_single_with_browser.assert_not_called()
    assert adapter.last_download_meta["error_type"] == "access_denied"


def test_safe_diagnostic_sitemap_config_is_normalized_and_coerced():
    adapter_config = _normalize_adapter_config(
        {
            "dc": {
                "dc_safe_diagnostic": "true",
                "dc_max_attempts_per_profile": "1",
                "dc_waf_browser_fallback": "false",
            }
        },
        file_label="journal.json",
    )

    overrides = _seed_dc_overrides(
        {"adapter_config": adapter_config, "sitemap_file": "journal.json"}
    )

    assert overrides == {
        "safe_diagnostic": True,
        "max_attempts_per_profile": 1,
        "waf_browser_fallback": False,
    }
