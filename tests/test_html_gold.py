from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_script(name: str):
    path = Path(__file__).parents[1] / "scripts" / "quality" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


build_html_gold = _load_script("build_html_gold")
score_note_text = _load_script("score_note_text")


def test_extract_html_footnotes_uses_evaluation_note_shape() -> None:
    html = """
    <ol class="footnotes">
      <li id="fn-1"><sup>1</sup> See Smith v. Jones.<a class="footnote-backref">↩</a></li>
      <li id="fn-2"><span class="fn-label">2</span> Id. at 10.</li>
    </ol>
    """
    notes, selector = build_html_gold.extract_html_footnotes(
        html, "https://www.yalelawjournal.org/article/example"
    )

    assert selector == ".footnotes li"
    assert notes == [
        {"label": "1", "note_type": "footnote", "text": "See Smith v. Jones."},
        {"label": "2", "note_type": "footnote", "text": "Id. at 10."},
    ]


def test_text_prf_normalizes_whitespace_and_line_break_hyphenation() -> None:
    tp, fp, fn = score_note_text.text_prf(
        "Constitutional interpretation is difficult.",
        "Constitutional inter-\npretation   is difficult.",
    )

    assert (tp, fp, fn) == (5, 0, 0)


def test_score_gold_five_document_regression_floor(tmp_path: Path) -> None:
    domains = [
        "yalelawjournal.org",
        "yalelawjournal.org",
        "harvardlawreview.org",
        "harvardlawreview.org",
        "californialawreview.org",
    ]
    documents = []
    for index, domain in enumerate(domains, start=1):
        pdf = tmp_path / f"doc-{index}.pdf"
        notes = {
            "1": {"text": "A normalized footnote about constitutional law."},
            "2": {"text": "See Smith v. Jones, 123 U.S. 456."},
        }
        Path(f"{pdf}.footnotes.json").write_text(json.dumps({"notes": notes}))
        documents.append(
            {
                "source_pdf_path": str(pdf),
                "html_gold": {"domain": domain, "html_url": f"https://{domain}/article/{index}"},
                "notes": [
                    {"label": label, "note_type": "footnote", "text": note["text"]}
                    for label, note in notes.items()
                ],
            }
        )
    gold = tmp_path / "gold.json"
    gold.write_text(json.dumps({"documents": documents}))

    report = score_note_text.score_gold(gold)

    assert len(report["documents"]) == 5
    assert len(report["domains"]) == 3
    assert report["summary"]["f1"] >= 0.99
