from __future__ import annotations

from urllib.parse import urlparse

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


def test_article_link_sweep_rejects_offsite_pdfs() -> None:
    """OJS reference lists hyperlink third-party PDFs; they are not this journal.

    Regression for a scope leak seen live on journals.muni.cz (2026-08-24): the
    fallback link sweep in _process_article had no origin check, so a court
    judgment PDF and a KU Leuven repository PDF linked from an article's
    references were queued for download and recorded as MUJLT articles.
    """
    from offprint.adapters.ojs import OJSAdapter

    article_html = """
    <html><head>
      <meta name="citation_title" content="A Real Article"/>
      <meta name="citation_pdf_url"
            content="https://journals.muni.cz/mujlt/article/download/111/222"/>
    </head><body>
      <a href="https://journals.muni.cz/mujlt/article/view/111/222">PDF</a>
      <a href="https://www.katowice.sa.gov.pl/container/orzeczenia/V_ACa_546-11.pdf">judgment</a>
      <a href="https://lirias.kuleuven.be/bitstream/123/1/unfair_practices_en.pdf">study</a>
    </body></html>
    """

    class _Resp:
        status_code = 200
        text = article_html
        headers = {"Content-Type": "text/html"}
        url = "https://journals.muni.cz/mujlt/article/view/111"

    adapter = OJSAdapter()
    adapter._get = lambda url: _Resp()  # type: ignore[assignment]
    adapter._head_is_pdf = lambda url: True  # type: ignore[assignment]

    results = list(
        adapter._process_article(
            "https://journals.muni.cz/mujlt/article/view/111",
            set(),
            {},
            "journals.muni.cz",
        )
    )

    hosts = {urlparse(r.pdf_url).netloc for r in results}
    assert hosts == {"journals.muni.cz"}, f"off-host PDFs leaked in: {hosts}"
    assert any("/article/download/111/222" in r.pdf_url for r in results)


def test_galley_download_url_drops_trailing_file_id():
    """OJS issue pages link one galley two ways; both must normalize alike.

    /article/download/<article>/<galley> and .../<galley>/<file> are the same PDF.
    Left distinct, the longer form misses the seen_pdfs check, so every article is
    fetched twice and the duplicate carries only issue-level metadata (no title).
    """
    from offprint.adapters.ojs import _normalize_galley_download_url as norm

    short = "https://x.org/index.php/CBLR/article/download/14950/8159"
    assert norm(short + "/44632") == short
    assert norm(short + "/44632/") == short
    assert norm(short) == short                      # idempotent
    # the viewer form still rewrites, and also loses the file id
    assert norm("https://x.org/index.php/CBLR/article/view/14950/8159") == short
    # unrelated URLs are untouched
    assert norm("https://x.org/a/b.pdf") == "https://x.org/a/b.pdf"
    assert norm("") == ""
