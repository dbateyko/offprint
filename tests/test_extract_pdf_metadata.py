from scripts.processing.extract_pdf_metadata import _build_metadata_patch


def test_native_extraction_replaces_explicit_ostlj_title_placeholder() -> None:
    patch = _build_metadata_patch(
        current_meta={"title": "OSTLJ archived article: 3 Granick"},
        extracted={"title": "Faking It: Calculating Loss in Computer Crime Sentencing"},
        overwrite_policy="fill_gaps_only",
    )

    assert patch["title"] == "Faking It: Calculating Loss in Computer Crime Sentencing"


def test_native_extraction_preserves_real_publisher_title() -> None:
    patch = _build_metadata_patch(
        current_meta={"title": "Publisher Supplied Title"},
        extracted={"title": "Less Reliable Extracted Title"},
        overwrite_policy="fill_gaps_only",
    )

    assert "title" not in patch
