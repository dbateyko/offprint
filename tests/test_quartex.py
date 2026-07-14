from bs4 import BeautifulSoup

from offprint.adapters.quartex import QuartexAdapter


def test_quartex_rejects_keyboard_shortcuts_furniture():
    adapter = QuartexAdapter(session=object())
    soup = BeautifulSoup("<html><h1>Keyboard Shortcuts</h1></html>", "lxml")

    assert adapter._is_non_article_record(soup, {"title": "Keyboard Shortcuts"})


def test_quartex_keeps_unsigned_article_candidate():
    adapter = QuartexAdapter(session=object())
    soup = BeautifulSoup(
        "<html><h1>Regulatory Change and the Rule of Law</h1>"
        "<p>Article</p><div>Keyboard Shortcuts</div></html>",
        "lxml",
    )

    assert not adapter._is_non_article_record(
        soup, {"title": "Regulatory Change and the Rule of Law"}
    )


def test_quartex_prefers_document_heading_and_inline_metadata():
    adapter = QuartexAdapter(session=object())
    soup = BeautifulSoup(
        '<html><h2 class="modal__heading">Keyboard Shortcuts</h2>'
        '<h1 class="document-viewer__heading">Article Title</h1>'
        '<script>summaryMetadata: [{"name":"Author","value":"A. Author"},'
        '{"name":"Date","value":"April 03 2026"},'
        '{"name":"Volume/Issue","tags":["JBL 27.4"]},'
        '{"name":"Preferred Citation","value":"27 U. Pa. J. Bus. L. 1051 (2026)"}]</script></html>',
        "lxml",
    )

    metadata = adapter._extract_metadata(soup, "https://example.test/Documents/Detail/1")

    assert metadata["title"] == "Article Title"
    assert metadata["authors"] == ["A. Author"]
    assert metadata["year"] == "2026"
    assert metadata["volume_issue"] == "JBL 27.4"
    assert metadata["volume"] == "27"
    assert metadata["issue"] == "4"
