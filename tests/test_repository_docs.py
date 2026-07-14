from __future__ import annotations

from pathlib import Path

from scripts.quality.check_markdown_links import find_broken_links, local_links


def test_local_links_ignores_remote_urls_and_anchors() -> None:
    text = "[local](guide.md) [section](#part) [web](https://example.org) ![logo](logo.png)"
    assert list(local_links(text)) == ["guide.md", "logo.png"]


def test_find_broken_links_reports_missing_targets(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("[missing](docs/nope.md)\n", encoding="utf-8")
    assert find_broken_links(tmp_path, ["README.md"]) == [
        "README.md: missing link target docs/nope.md"
    ]


def test_find_broken_links_accepts_existing_targets(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/guide.md").write_text("# Guide\n", encoding="utf-8")
    (tmp_path / "README.md").write_text("[guide](docs/guide.md)\n", encoding="utf-8")
    assert find_broken_links(tmp_path, ["README.md"]) == []
