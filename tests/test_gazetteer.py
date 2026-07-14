from __future__ import annotations

import csv
import json
from pathlib import Path

from offprint.gazetteer import build_snapshot, main, render_markdown


def _write_registry(path: Path) -> None:
    path.parent.mkdir(parents=True)
    fields = [
        "journal_name",
        "url",
        "host",
        "platform",
        "status",
        "sitemap_file",
        "source",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "journal_name": "Example Law Review",
                    "url": "https://law.example.edu/review",
                    "host": "law.example.edu",
                    "platform": "digital_commons",
                    "status": "active",
                    "sitemap_file": "example.json",
                    "source": "sitemap",
                },
                {
                    "journal_name": "Second Journal",
                    "url": "https://second.example.org",
                    "host": "second.example.org",
                    "platform": "WordPress",
                    "status": "no_sitemap",
                    "sitemap_file": "",
                    "source": "wlu",
                },
            ]
        )


def _write_sitemap(path: Path) -> None:
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "id": "example",
                "start_urls": ["https://law.example.edu/review/issues"],
                "metadata": {
                    "journal_name": "Example Law Review",
                    "platform": "Digital Commons",
                },
            }
        ),
        encoding="utf-8",
    )


def test_snapshot_counts_and_normalizes_platforms(tmp_path: Path) -> None:
    registry = tmp_path / "data/registry/lawjournals.csv"
    sitemaps = tmp_path / "offprint/sitemaps"
    _write_registry(registry)
    _write_sitemap(sitemaps / "example.json")

    snapshot = build_snapshot(registry, sitemaps)

    assert snapshot.registry_rows == 2
    assert snapshot.registry_hosts == 2
    assert snapshot.registry_with_sitemap == 1
    assert snapshot.sitemap_files == 1
    assert snapshot.sitemap_statuses == {"active (inferred)": 1}
    assert snapshot.registry_platforms == {"Digital Commons": 1, "WordPress": 1}

    rendered = render_markdown(snapshot)
    assert "| Journal registry rows | 2 |" in rendered
    assert "active (inferred)" in rendered


def test_check_mode_detects_stale_snapshot(tmp_path: Path) -> None:
    _write_registry(tmp_path / "data/registry/lawjournals.csv")
    _write_sitemap(tmp_path / "offprint/sitemaps/example.json")

    assert main(["--repo-root", str(tmp_path)]) == 0
    assert main(["--repo-root", str(tmp_path), "--check"]) == 0

    output = tmp_path / "docs/generated/GAZETTEER_SNAPSHOT.md"
    output.write_text("stale\n", encoding="utf-8")
    assert main(["--repo-root", str(tmp_path), "--check"]) == 1
