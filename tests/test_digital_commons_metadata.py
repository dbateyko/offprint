from bs4 import BeautifulSoup

from offprint.digital_commons_enumerator import _extract_article_page_metadata


def test_extract_article_page_metadata_prefers_structured_bepress_fields():
    soup = BeautifulSoup(
        """
        <html><head>
          <title>Presentation title by Somebody Else</title>
          <meta name="citation_title" content="A Structured Article Title">
          <meta name="citation_author" content="Ada Author">
          <meta name="citation_author" content="Ben Writer">
          <meta name="citation_publication_date" content="2021-05-01">
          <meta name="citation_journal_title" content="Example Law Review">
          <meta name="citation_volume" content="42">
          <meta name="citation_issue" content="3">
          <meta name="citation_firstpage" content="101">
          <meta name="citation_lastpage" content="155">
          <meta name="citation_pdf_url"
                content="https://example.edu/cgi/viewcontent.cgi?article=1234&amp;context=elr">
          <meta name="dc.identifier" content="https://doi.org/10.1234/example.5">
        </head><body>
          <div id="recommended_citation"><span class="citation">
            <em>A Presentation-Layer Title</em> (2021).
          </span></div>
        </body></html>
        """,
        "lxml",
    )

    metadata = _extract_article_page_metadata(
        soup,
        "https://example.edu/elr/vol42/iss3/5/",
        "Presentation title by Somebody Else",
    )

    assert metadata["title"] == "A Structured Article Title"
    assert metadata["authors"] == ["Ada Author", "Ben Writer"]
    assert metadata["year"] == "2021"
    assert metadata["journal"] == "Example Law Review"
    assert metadata["volume"] == "42"
    assert metadata["issue"] == "3"
    assert metadata["start_page"] == "101"
    assert metadata["end_page"] == "155"
    assert metadata["doi"] == "10.1234/example.5"
    assert metadata["dc_article_id"] == "1234"
    assert metadata["dc_context"] == "elr"
