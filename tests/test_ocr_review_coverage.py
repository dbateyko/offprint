from offprint.pdf_footnotes.pipeline import BatchConfig, _derive_ocr_review_reasons


def test_supported_ocr_backend_is_the_default() -> None:
    assert BatchConfig(pdf_root=".").ocr_backend == "olmocr"


def test_partial_native_text_layer_is_queued_for_ocr() -> None:
    reasons = _derive_ocr_review_reasons(
        parser_used="liteparse",
        warnings=[],
        ordinality_status="valid",
        note_count=1,
        ocr_used=False,
        native_extract_empty=False,
        native_page_count=12,
        native_text_page_count=1,
    )

    assert reasons == ["native_text_page_coverage_low"]


def test_normal_native_text_coverage_does_not_trigger_ocr() -> None:
    reasons = _derive_ocr_review_reasons(
        parser_used="liteparse",
        warnings=[],
        ordinality_status="valid",
        note_count=10,
        ocr_used=False,
        native_extract_empty=False,
        native_page_count=12,
        native_text_page_count=11,
    )

    assert reasons == []
