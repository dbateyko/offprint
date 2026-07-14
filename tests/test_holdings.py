from __future__ import annotations

import csv
import json
from pathlib import Path

from offprint.holdings import load_holdings, render_summary


def _write_registry(path: Path) -> None:
    path.parent.mkdir(parents=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["journal_name", "url", "host", "platform", "status", "sitemap_file"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "journal_name": "Example Law Review",
                "url": "https://repository.example.edu/elr/",
                "host": "repository.example.edu",
                "platform": "digital_commons",
                "status": "active",
                "sitemap_file": "example.json",
            }
        )


def test_holdings_deduplicate_and_infer_journal(tmp_path: Path) -> None:
    _write_registry(tmp_path / "data/registry/lawjournals.csv")
    run = tmp_path / "artifacts/runs/run-1"
    run.mkdir(parents=True)
    pdf = tmp_path / "artifacts/pdfs/example.pdf"
    pdf.parent.mkdir(parents=True)
    pdf.write_bytes(b"%PDF-1.4\n")
    row = {
        "ok": True,
        "domain": "repository.example.edu",
        "pdf_url": "https://repository.example.edu/cgi/viewcontent.cgi?article=1&context=elr",
        "local_path": "artifacts/pdfs/example.pdf",
        "pdf_sha256": "abc123",
        "retrieved_at": "2026-07-01T00:00:00Z",
        "metadata": {"title": "An Example Article", "authors": ["A. Author"], "year": "2024"},
    }
    (run / "records.jsonl").write_text(
        json.dumps(row) + "\n" + json.dumps(row) + "\n", encoding="utf-8"
    )

    holdings, invalid = load_holdings(tmp_path, tmp_path / "artifacts/runs")

    assert invalid == 0
    assert len(holdings) == 1
    assert holdings[0].journal == "Example Law Review"
    assert holdings[0].authors == "A. Author"
    assert holdings[0].file_present is True
    summary = render_summary(holdings, invalid)
    assert "| Example Law Review | 1 | 1 | 1 | 2024 | repository.example.edu |" in summary


def test_holdings_supplement_filesystem_only_pdfs(tmp_path: Path) -> None:
    _write_registry(tmp_path / "data/registry/lawjournals.csv")
    pdf_dir = tmp_path / "artifacts/pdfs/repository.example.edu"
    pdf_dir.mkdir(parents=True)
    (pdf_dir / "historical.pdf").write_bytes(b"%PDF-1.4\n")

    holdings, invalid = load_holdings(tmp_path, tmp_path / "artifacts/runs")

    assert invalid == 0
    assert len(holdings) == 1
    assert holdings[0].journal == "Example Law Review"
    assert holdings[0].context == "filesystem"
    assert holdings[0].file_present is True
