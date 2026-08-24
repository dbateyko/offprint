from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

from pypdf import PdfReader, PdfWriter


SCRIPT = Path(__file__).parents[1] / "scripts/processing/split_adjudicated_issue_plans.py"
SPEC = importlib.util.spec_from_file_location("split_adjudicated_issue_plans", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_sha_pinned_split_preserves_parent_and_honors_explicit_end(tmp_path: Path) -> None:
    source = tmp_path / "issue.pdf"
    writer = PdfWriter()
    for _ in range(8):
        writer.add_blank_page(width=612, height=792)
    with source.open("wb") as handle:
        writer.write(handle)
    source_bytes = source.read_bytes()
    source_sha = hashlib.sha256(source_bytes).hexdigest()

    plan = tmp_path / "plan.json"
    plan.write_text(
        json.dumps(
            {
                "schema": "offprint.issue_split.adjudicated_plan.v1",
                "parents": [
                    {
                        "parent_pdf": str(source),
                        "parent_sha256": source_sha,
                        "parent_page_count": 8,
                        "domain": "example.test",
                        "children": [
                            {"start_page": 2, "title": "One", "author": "A"},
                            {
                                "start_page": 5,
                                "end_page": 7,
                                "title": "Two",
                                "author": "B",
                            },
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = MODULE.split_plan(plan, tmp_path / "children", tmp_path / "runs")

    assert summary["parents_split"] == 1
    assert summary["children_written"] == 2
    assert summary["failures"] == 0
    assert source.read_bytes() == source_bytes
    children = sorted((tmp_path / "children").rglob("*.pdf"))
    assert [len(PdfReader(str(path)).pages) for path in children] == [3, 3]
    assert all(path.with_suffix(".split.json").exists() for path in children)
