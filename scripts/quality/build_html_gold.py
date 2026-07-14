#!/usr/bin/env python3
"""Build evaluation.py-compatible footnote gold from article HTML/PDF pairs."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag

LABEL_RE = re.compile(r"(?:footnote|fn|note)[-_:]?(\d+)$", re.IGNORECASE)
LEADING_LABEL_RE = re.compile(r"^\s*(\d{1,4})(?:[.)\]]|\s)+")

DOMAIN_SELECTORS: dict[str, tuple[str, ...]] = {
    "yalelawjournal.org": (
        "div[id^='footnote_']",
        ".article-footnotes li",
        ".footnotes li",
        "li[id^='footnote-']",
        "li[id^='fn']",
    ),
    "harvardlawreview.org": (
        ".article-footnotes li",
        ".footnotes li",
        "li[id^='fn']",
    ),
    "californialawreview.org": (
        "li:has(> a[id^='footnote-'])",
        ".footnotes li",
        ".entry-content li[id^='fn']",
        "li[id^='footnote-']",
    ),
    "columbialawreview.org": ("cite.footnote",),
    "houstonlawreview.org": (".footnotes li", "li.footnote-item"),
}

GENERIC_SELECTORS = (
    "ol.footnotes > li",
    "section.footnotes li",
    "div.footnotes li",
    "li[id^='footnote-']",
    "li[id^='fn-']",
    "li[id^='fn_']",
    "div[id^='footnote-']",
    "p[id^='footnote-']",
)


def _domain(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def _label_for_node(node: Tag) -> str | None:
    for key in ("data-footnote-number", "data-note", "value"):
        value = str(node.get(key) or "").strip()
        if value.isdigit():
            return str(int(value))
    node_id = str(node.get("id") or "").strip()
    match = LABEL_RE.search(node_id)
    if match:
        return str(int(match.group(1)))
    for class_name in node.get("class") or ():
        match = re.fullmatch(r"footnote-(\d+)", str(class_name))
        if match:
            return str(int(match.group(1)))
    anchor = node.select_one("a[id^='footnote-']")
    if anchor:
        match = re.fullmatch(r"footnote-(\d+)", str(anchor.get("id") or ""))
        if match:
            return str(int(match.group(1)))
    marker = node.select_one("sup, .footnote-number, .fn-label")
    marker_text = marker.get_text(" ", strip=True) if marker else ""
    if marker_text.isdigit():
        return str(int(marker_text))
    match = LEADING_LABEL_RE.match(node.get_text(" ", strip=True))
    return str(int(match.group(1))) if match else None


def _clean_note_text(node: Tag, label: str) -> str:
    fragment = BeautifulSoup(str(node), "html.parser")
    root = fragment.find()
    if root is None:
        return ""
    full_note = root.select_one("p.FootNote")
    if full_note is not None:
        root = full_note
    for removable in root.select(
        "a.footnote-backref, a[rev='footnote'], a[href^='#fnref'], "
        ".footnote-number, .fn-label, .aside-footnote-count, "
        ".footnote-aside-show, button"
    ):
        removable.decompose()
    marker = root.select_one("sup")
    if marker and marker.get_text(" ", strip=True).strip(".()[] ") == label:
        marker.decompose()
    text = " ".join(root.get_text(" ", strip=True).split())
    text = re.sub(rf"^\s*{re.escape(label)}(?:[.)\]]|\s)+", "", text, count=1)
    return text.strip()


def extract_html_footnotes(html: str, html_url: str) -> tuple[list[dict], str]:
    """Extract numbered notes and return (notes, selector_used)."""
    soup = BeautifulSoup(html, "html.parser")
    domain = _domain(html_url)
    selectors = DOMAIN_SELECTORS.get(domain, ()) + GENERIC_SELECTORS
    for selector in selectors:
        nodes = soup.select(selector)
        notes: list[dict] = []
        seen: set[str] = set()
        for node in nodes:
            label = _label_for_node(node)
            if label is None or label in seen:
                continue
            text = _clean_note_text(node, label)
            if not text:
                continue
            seen.add(label)
            notes.append({"label": label, "note_type": "footnote", "text": text})
        notes.sort(key=lambda note: int(note["label"]))
        if notes:
            return notes, selector
    return [], ""


def _load_pairs(path: Path) -> list[dict[str, str]]:
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _load_existing(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("documents", []) if isinstance(payload, dict) else payload
    return {str(row["source_pdf_path"]): row for row in rows}


def build_gold(
    pairs: list[dict[str, str]],
    *,
    output: Path,
    timeout: float = 30,
    min_notes: int = 1,
) -> dict:
    documents = _load_existing(output)
    errors: list[dict[str, str]] = []
    session = requests.Session()
    session.headers["User-Agent"] = "Offprint HTML gold builder/0.1"
    for pair in pairs:
        html_url = str(pair.get("html_url") or pair.get("article_html_url") or "").strip()
        pdf_path = str(pair.get("pdf_path") or pair.get("source_pdf_path") or "").strip()
        if not html_url or not pdf_path:
            errors.append({"html_url": html_url, "pdf_path": pdf_path, "error": "missing_pair"})
            continue
        try:
            response = session.get(html_url, timeout=timeout)
            response.raise_for_status()
            notes, selector = extract_html_footnotes(response.text, html_url)
            if len(notes) < min_notes:
                raise ValueError(f"only {len(notes)} notes found")
            documents[pdf_path] = {
                "source_pdf_path": pdf_path,
                "notes": notes,
                "html_gold": {
                    "html_url": html_url,
                    "domain": _domain(html_url),
                    "selector": selector,
                },
            }
        except Exception as exc:
            errors.append({"html_url": html_url, "pdf_path": pdf_path, "error": str(exc)})
    payload = {
        "schema": "offprint.evaluation.v1",
        "documents": sorted(documents.values(), key=lambda row: row["source_pdf_path"]),
        "errors": errors,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", type=Path, required=True, help="CSV or JSONL pair manifest")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--min-notes", type=int, default=1)
    args = parser.parse_args()
    payload = build_gold(
        _load_pairs(args.pairs), output=args.out, timeout=args.timeout, min_notes=args.min_notes
    )
    print(f"wrote {len(payload['documents'])} gold documents to {args.out}")
    print(f"errors: {len(payload['errors'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
