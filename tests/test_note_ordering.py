from __future__ import annotations

import json
from pathlib import Path

from offprint.pdf_footnotes.pipeline import _write_jsonl_sidecar_atomic
from offprint.pdf_footnotes.schema import AuthorNote, NoteRecord, SidecarDocument


def _make_note(label: str, text: str) -> NoteRecord:
    return NoteRecord(
        ordinal=1,
        label=label,
        note_type="footnote",
        text=text,
        page_start=1,
        page_end=1,
    )


def test_sidecar_document_to_dict_orders_numeric_labels_before_lexical_labels() -> None:
    document = SidecarDocument(
        source_pdf_path="/tmp/example.pdf",
        pdf_sha256=None,
        extractor_version="test",
        created_at="2026-04-09T00:00:00Z",
        dependency_versions={},
        document_confidence=0.5,
        warnings=[],
        features_preset="legal",
        notes=[
            _make_note("10", "ten"),
            _make_note("b", "bee"),
            _make_note("2", "two"),
            _make_note("a", "aye"),
            _make_note("1", "one"),
        ],
        author_notes=[AuthorNote(marker="*", text="author", page=1)],
    )

    payload = document.to_dict()

    assert list(payload["notes"].keys()) == ["1", "2", "10", "a", "b"]


def test_write_jsonl_sidecar_atomic_sorts_dict_note_labels(tmp_path: Path) -> None:
    payload = {
        "source_pdf_path": "/tmp/example.pdf",
        "pdf_sha256": None,
        "extractor_version": "test",
        "created_at": "2026-04-09T00:00:00Z",
        "dependency_versions": {},
        "document_confidence": 0.5,
        "warnings": [],
        "features_preset": "legal",
        "author_notes": [],
        "notes": {
            "10": {"text": "ten", "page_start": 1, "page_end": 1, "_qc": {"confidence": 1.0}},
            "b": {"text": "bee", "page_start": 1, "page_end": 1, "_qc": {"confidence": 1.0}},
            "2": {"text": "two", "page_start": 1, "page_end": 1, "_qc": {"confidence": 1.0}},
            "a": {"text": "aye", "page_start": 1, "page_end": 1, "_qc": {"confidence": 1.0}},
            "1": {"text": "one", "page_start": 1, "page_end": 1, "_qc": {"confidence": 1.0}},
        },
    }
    path = tmp_path / "sidecar.jsonl"

    try:
        _write_jsonl_sidecar_atomic(str(path), payload)
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    finally:
        if path.exists():
            path.unlink()

    footnote_labels = [row["label"] for row in rows if row["type"] == "footnote"]
    assert footnote_labels == ["1", "2", "10", "a", "b"]
