import gzip
import json
from pathlib import Path
from typing import Optional

from scripts.export_footnote_dataset import export_dataset, main


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def read_gzip_jsonl(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def sidecar(
    *,
    status: str = "valid",
    source_pdf_path: str = "/tmp/example.edu/article.pdf",
    notes: Optional[dict] = None,
    metadata: Optional[dict] = None,
) -> dict:
    return {
        "source_pdf_path": source_pdf_path,
        "pdf_sha256": "abc123",
        "doc_type": "article",
        "doc_policy": "article_only",
        "platform_family": "ojs",
        "document_metadata": metadata or {"title": "A Title", "year": "2024"},
        "ordinality": {
            "status": status,
            "expected_range": [1, 2],
            "gaps": [],
            "actual_sequence": [1, 2],
        },
        "notes": notes
        if notes is not None
        else {
            "2": {"text": "Second", "page_start": 2, "page_end": 2, "_qc": {"confidence": 0.8}},
            "1": {"text": "First", "page_start": 1, "page_end": 1, "features": {"urls": ["https://x.test"]}},
        },
    }


def test_export_json_sidecars_and_manifest_counts(tmp_path):
    root = tmp_path / "sidecars"
    write_json(root / "example.edu" / "article.pdf.footnotes.json", sidecar())
    write_json(
        root / "example.edu" / "invalid.pdf.footnotes.json",
        sidecar(status="invalid", source_pdf_path="/tmp/example.edu/invalid.pdf"),
    )

    out = tmp_path / "out.jsonl.gz"
    manifest_out = tmp_path / "manifest.json"
    manifest = export_dataset(
        sidecar_root=root,
        out_path=out,
        manifest_path=manifest_out,
        include_statuses={"valid", "valid_with_gaps"},
        min_notes=1,
        journal_domain=None,
    )

    records = read_gzip_jsonl(out)
    assert len(records) == 1
    record = records[0]
    assert record["source_pdf_sha256"] == "abc123"
    assert record["journal_domain"] == "example.edu"
    assert record["doc_policy"] == {
        "doc_type": "article",
        "include": True,
        "platform_family": "ojs",
        "raw_policy": "article_only",
    }
    assert record["article"]["title"] == "A Title"
    assert record["ordinality"] == {
        "status": "valid",
        "expected_range": [1, 2],
        "gaps": [],
        "solver_selected_labels": [1, 2],
    }
    assert [note["label"] for note in record["notes"]] == ["1", "2"]
    assert record["notes"][0]["features"] == {"urls": ["https://x.test"]}
    assert record["notes"][1]["confidence"] == 0.8

    saved_manifest = json.loads(manifest_out.read_text(encoding="utf-8"))
    assert saved_manifest == manifest
    assert manifest["records_written"] == 1
    assert manifest["notes_written"] == 2
    assert manifest["counts_by_status"] == {"invalid": 1, "valid": 1}
    assert manifest["counts_by_domain"] == {"example.edu": 2}
    assert manifest["dropped_reasons"] == {"status_not_included": 1}


def test_export_jsonl_sidecar_and_filters(tmp_path):
    root = tmp_path / "sidecars"
    jsonl = root / "journal.test" / "article.pdf.footnotes.jsonl"
    jsonl.parent.mkdir(parents=True)
    rows = [
        {
            "type": "metadata",
            "source_pdf_path": "/tmp/journal.test/article.pdf",
            "source_pdf_sha256": "def456",
            "journal_domain": "journal.test",
            "doc_type": "article",
            "doc_policy": {"doc_type": "article", "platform_family": "custom", "include": True},
            "platform_family": "custom",
            "article": {"title": "From JSONL"},
            "ordinality": {"status": "valid_with_gaps", "expected_range": [1, 3], "gaps": [2]},
        },
        {"type": "footnote", "label": "1", "text": "Only note", "page_start": 3, "page_end": 4},
    ]
    jsonl.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    out = tmp_path / "out.jsonl.gz"
    manifest_out = tmp_path / "manifest.json"
    manifest = export_dataset(
        sidecar_root=root,
        out_path=out,
        manifest_path=manifest_out,
        include_statuses={"valid_with_gaps"},
        min_notes=1,
        journal_domain={"journal.test"},
    )

    records = read_gzip_jsonl(out)
    assert len(records) == 1
    assert records[0]["source_pdf_sha256"] == "def456"
    assert records[0]["article"] == {"title": "From JSONL"}
    assert records[0]["notes"][0]["ordinal"] == 1
    assert records[0]["ordinality"]["solver_selected_labels"] == []
    assert manifest["included_reasons"] == {"included": 1}


def test_min_notes_domain_and_duplicate_drops(tmp_path):
    root = tmp_path / "sidecars"
    write_json(
        root / "keep.edu" / "article.pdf.footnotes.json",
        sidecar(source_pdf_path="/tmp/keep.edu/article.pdf"),
    )
    duplicate_jsonl = root / "keep.edu" / "article.pdf.footnotes.jsonl"
    duplicate_jsonl.write_text(
        json.dumps(
            {
                "type": "metadata",
                "source_pdf_path": "/tmp/keep.edu/article.pdf",
                "ordinality": {"status": "valid"},
                "notes": {},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    write_json(
        root / "drop.edu" / "article.pdf.footnotes.json",
        sidecar(source_pdf_path="/tmp/drop.edu/article.pdf"),
    )
    write_json(
        root / "keep.edu" / "short.pdf.footnotes.json",
        sidecar(source_pdf_path="/tmp/keep.edu/short.pdf", notes={"1": {"text": "Short"}}),
    )

    out = tmp_path / "out.jsonl.gz"
    manifest = export_dataset(
        sidecar_root=root,
        out_path=out,
        manifest_path=tmp_path / "manifest.json",
        include_statuses={"valid"},
        min_notes=2,
        journal_domain={"keep.edu"},
    )

    records = read_gzip_jsonl(out)
    assert [record["source_pdf_path"] for record in records] == ["/tmp/keep.edu/article.pdf"]
    assert manifest["dropped_reasons"] == {
        "below_min_notes": 1,
        "domain_filter": 1,
        "duplicate_sidecar": 1,
    }


def test_cli_help(capsys):
    try:
        main(["--help"])
    except SystemExit as exc:
        assert exc.code == 0
    assert "--sidecar-root" in capsys.readouterr().out
