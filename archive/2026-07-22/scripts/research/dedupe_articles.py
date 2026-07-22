#!/usr/bin/env python3
"""Find likely duplicate article PDFs and write a canonical manifest."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import string
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


DEFAULT_PDF_ROOT = Path("/mnt/shared_storage/law-review-corpus/corpus/scraped")
DEFAULT_OUTPUT = Path("artifacts/datasets/canonical_pdfs.csv")
FIRST_TEXT_CHARS = 500
VERSION_SUFFIX_RE = re.compile(r"-\d+$")


@dataclass(frozen=True)
class PdfRecord:
    path: Path
    journal_domain: str
    sha256: str
    title_key: str | None = None
    first_text_hash: str | None = None
    page_count: int | None = None
    mtime: float = 0.0
    size: int = 0


@dataclass
class DuplicateGroup:
    canonical: PdfRecord
    duplicates: list[PdfRecord]
    reasons: list[str] = field(default_factory=list)

    @property
    def group_size(self) -> int:
        return 1 + len(self.duplicates)


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[Path, Path] = {}

    def add(self, item: Path) -> None:
        self.parent.setdefault(item, item)

    def find(self, item: Path) -> Path:
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, a: Path, b: Path) -> None:
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a == root_b:
            return
        self.parent[max(root_a, root_b)] = min(root_a, root_b)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKC", value).casefold()
    table = str.maketrans({ch: " " for ch in string.punctuation})
    return " ".join(value.translate(table).split())


def first_text_hash(text: str) -> str | None:
    normalized = normalize_text(text)
    if not normalized:
        return None
    return hashlib.sha256(normalized[:FIRST_TEXT_CHARS].encode("utf-8")).hexdigest()


def versionless_stem(path: Path) -> str:
    return VERSION_SUFFIX_RE.sub("", path.stem)


def journal_domain_for(pdf_root: Path, path: Path) -> str:
    try:
        rel = path.relative_to(pdf_root)
    except ValueError:
        return path.parent.name
    return rel.parts[0] if len(rel.parts) > 1 else path.parent.name


def sidecar_candidates(path: Path) -> list[Path]:
    return [
        Path(f"{path}.text.json"),
        Path(f"{path}.metadata.json"),
        Path(f"{path}.json"),
        Path(f"{path}.footnotes.json"),
        Path(f"{path}.footnotes.jsonl"),
    ]


def _iter_json_values(path: Path) -> Iterable[dict[str, Any]]:
    try:
        if path.suffix == ".jsonl":
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        data = json.loads(line)
                        if isinstance(data, dict):
                            yield data
            return
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return
    if isinstance(data, dict):
        yield data
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item


def _string_at(data: dict[str, Any], keys: Iterable[tuple[str, ...]]) -> str | None:
    for key_path in keys:
        current: Any = data
        for key in key_path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        if isinstance(current, str) and current.strip():
            return current.strip()
    return None


def _int_at(data: dict[str, Any], keys: Iterable[tuple[str, ...]]) -> int | None:
    for key_path in keys:
        current: Any = data
        for key in key_path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        if isinstance(current, int):
            return current
        if isinstance(current, str) and current.isdigit():
            return int(current)
    return None


def read_sidecar_metadata(path: Path) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for sidecar in sidecar_candidates(path):
        if not sidecar.exists():
            continue
        for data in _iter_json_values(sidecar):
            title = _string_at(
                data,
                [
                    ("title",),
                    ("article_title",),
                    ("article", "title"),
                    ("metadata", "title"),
                    ("metadata", "article_title"),
                ],
            )
            text = _string_at(data, [("text",), ("full_text",), ("article", "text")])
            domain = _string_at(
                data,
                [
                    ("journal_domain",),
                    ("domain",),
                    ("metadata", "journal_domain"),
                    ("metadata", "domain"),
                ],
            )
            page_count = _int_at(
                data,
                [("page_count",), ("metadata", "page_count"), ("article", "page_count")],
            )
            if title and not metadata.get("title"):
                metadata["title"] = title
            if text and not metadata.get("text"):
                metadata["text"] = text
            if domain and not metadata.get("journal_domain"):
                metadata["journal_domain"] = domain
            if page_count is not None and metadata.get("page_count") is None:
                metadata["page_count"] = page_count
    return metadata


def build_record(path: Path, pdf_root: Path) -> PdfRecord:
    stat = path.stat()
    metadata = read_sidecar_metadata(path)
    title = metadata.get("title")
    text = metadata.get("text")
    title_key = normalize_text(title) if isinstance(title, str) and title.strip() else None
    text_hash = first_text_hash(text) if isinstance(text, str) and text.strip() else None
    domain = metadata.get("journal_domain") or journal_domain_for(pdf_root, path)
    return PdfRecord(
        path=path,
        journal_domain=str(domain),
        sha256=sha256_file(path),
        title_key=title_key,
        first_text_hash=text_hash,
        page_count=metadata.get("page_count"),
        mtime=stat.st_mtime,
        size=stat.st_size,
    )


def iter_pdf_paths(pdf_root: Path) -> Iterable[Path]:
    yield from sorted(p for p in pdf_root.rglob("*.pdf") if p.is_file())


def scan_pdf_root(pdf_root: Path) -> list[PdfRecord]:
    return [build_record(path, pdf_root) for path in iter_pdf_paths(pdf_root)]


def canonical_record(records: Iterable[PdfRecord]) -> PdfRecord:
    return min(
        records,
        key=lambda r: (
            -r.mtime,
            -r.size,
            -(r.page_count or 0),
            str(r.path),
        ),
    )


def group_duplicates(records: list[PdfRecord]) -> list[DuplicateGroup]:
    uf = UnionFind()
    indexes: dict[tuple[str, str], list[PdfRecord]] = {}

    def add_key(record: PdfRecord, reason: str, value: str) -> None:
        key = (reason, value)
        indexes.setdefault(key, []).append(record)

    for record in records:
        uf.add(record.path)
        add_key(record, "sha256", record.sha256)
        if record.title_key and record.first_text_hash:
            add_key(
                record,
                "normalized_title_first_text_hash",
                f"{record.title_key}\0{record.first_text_hash}",
            )
        suffix_base = versionless_stem(record.path)
        if suffix_base != record.path.stem:
            add_key(
                record,
                "journal_domain_versionless_stem",
                f"{record.journal_domain}\0{suffix_base}",
            )
        elif any(
            sibling != record.path and versionless_stem(sibling) == suffix_base
            for sibling in record.path.parent.glob(f"{record.path.stem}-*.pdf")
        ):
            add_key(
                record,
                "journal_domain_versionless_stem",
                f"{record.journal_domain}\0{suffix_base}",
            )

    for key, key_records in indexes.items():
        if len(key_records) < 2:
            continue
        first = key_records[0]
        for record in key_records[1:]:
            uf.union(first.path, record.path)

    grouped: dict[Path, list[PdfRecord]] = {}
    by_path = {record.path: record for record in records}
    for path in by_path:
        grouped.setdefault(uf.find(path), []).append(by_path[path])

    result: list[DuplicateGroup] = []
    for group_records in grouped.values():
        if len(group_records) < 2:
            continue
        canonical = canonical_record(group_records)
        duplicates = sorted(
            (record for record in group_records if record.path != canonical.path),
            key=lambda r: str(r.path),
        )
        reasons = sorted(
            {
                reason
                for key, key_records in indexes.items()
                if len(key_records) >= 2
                for reason in [key[0]]
                if {r.path for r in key_records}.issubset({r.path for r in group_records})
            }
        )
        result.append(DuplicateGroup(canonical, duplicates, reasons))

    return sorted(result, key=lambda group: str(group.canonical.path))


def write_csv(groups: list[DuplicateGroup], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["canonical_pdf", "duplicate_pdfs", "reason", "group_size"],
        )
        writer.writeheader()
        for group in groups:
            writer.writerow(
                {
                    "canonical_pdf": str(group.canonical.path),
                    "duplicate_pdfs": "|".join(str(r.path) for r in group.duplicates),
                    "reason": "|".join(group.reasons),
                    "group_size": group.group_size,
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect duplicate article PDFs and emit canonical_pdfs.csv."
    )
    parser.add_argument(
        "--pdf-root",
        type=Path,
        default=DEFAULT_PDF_ROOT,
        help=f"PDF root to scan (default: {DEFAULT_PDF_ROOT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"CSV output path (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    records = scan_pdf_root(args.pdf_root)
    groups = group_duplicates(records)
    write_csv(groups, args.output)
    duplicate_count = sum(group.group_size - 1 for group in groups)
    print(
        f"Wrote {len(groups)} duplicate groups "
        f"({duplicate_count} duplicate PDFs) to {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
