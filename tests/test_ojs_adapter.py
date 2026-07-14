from __future__ import annotations

from offprint.adapters.ojs import OJSAdapter, _normalize_galley_download_url


def test_normalize_rewrites_galley_view_to_download() -> None:
    assert (
        _normalize_galley_download_url(
            "https://epubs.utah.edu/index.php/jlrel/article/view/1147/841"
        )
        == "https://epubs.utah.edu/index.php/jlrel/article/view/1147/841".replace(
            "/view/", "/download/"
        )
    )


def test_normalize_is_idempotent_on_download_url() -> None:
    url = "https://epubs.utah.edu/index.php/jlrel/article/download/1147/841"
    assert _normalize_galley_download_url(url) == url


def test_normalize_leaves_non_galley_urls_untouched() -> None:
    # Landing page (no /<id>/<galley>) and a plain .pdf must not be rewritten.
    assert (
        _normalize_galley_download_url("https://x.org/index.php/j/article/view/1147")
        == "https://x.org/index.php/j/article/view/1147"
    )
    assert _normalize_galley_download_url("https://x.org/files/abc.pdf") == (
        "https://x.org/files/abc.pdf"
    )
    # A "view" elsewhere in the path must not be touched.
    assert _normalize_galley_download_url("https://x.org/view/article/9/9") == (
        "https://x.org/view/article/9/9"
    )


def test_normalize_preserves_query_and_fragment() -> None:
    assert (
        _normalize_galley_download_url(
            "https://x.org/index.php/j/article/view/12/34?download=1"
        )
        == "https://x.org/index.php/j/article/view/12/34?download=1".replace(
            "/view/", "/download/"
        )
    )


class _FakeResp:
    def __init__(self, status_code: int, content_type: str) -> None:
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}


def test_download_pdf_failure_sets_specific_error_type(monkeypatch) -> None:
    """A galley viewer that returns text/html must not masquerade as 'unknown'."""
    adapter = OJSAdapter()
    seen: dict[str, str] = {}

    def fake_get(url: str, *a, **k):
        seen["url"] = url
        return _FakeResp(200, "text/html; charset=utf-8")

    monkeypatch.setattr(adapter, "_get", fake_get)
    result = adapter.download_pdf(
        "https://epubs.utah.edu/index.php/jlrel/article/view/1147/841", "/tmp"
    )
    assert result is None
    # download_pdf normalized the viewer URL to the raw download route before GET.
    assert seen["url"].endswith("/article/download/1147/841")
    assert adapter.last_download_meta["error_type"] == "ojs_viewer_not_pdf"
