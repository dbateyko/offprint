import csv
import json
import os
from pathlib import Path

from scripts.research.dedupe_articles import (
    build_record,
    canonical_record,
    group_duplicates,
    scan_pdf_root,
    versionless_stem,
    write_csv,
)


def _pdf(path: Path, content: bytes, mtime: int = 1_700_000_000) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    os.utime(path, (mtime, mtime))
    return path


def _text_sidecar(path: Path, **payload):
    Path(f"{path}.text.json").write_text(json.dumps(payload), encoding="utf-8")


def test_groups_exact_sha256_and_picks_newest_canonical(tmp_path):
    root = tmp_path / "scraped"
    older = _pdf(root / "journal.test" / "article.pdf", b"%PDF same", mtime=10)
    newer = _pdf(root / "journal.test" / "article-copy.pdf", b"%PDF same", mtime=20)

    groups = group_duplicates(scan_pdf_root(root))

    assert len(groups) == 1
    assert groups[0].canonical.path == newer
    assert [r.path for r in groups[0].duplicates] == [older]
    assert groups[0].reasons == ["sha256"]
    assert groups[0].group_size == 2


def test_groups_normalized_title_and_first_text_hash(tmp_path):
    root = tmp_path / "scraped"
    first = _pdf(root / "journal.test" / "a.pdf", b"%PDF one", mtime=10)
    second = _pdf(root / "journal.test" / "b.pdf", b"%PDF two", mtime=11)
    text = "This is the same opening text. " * 40
    _text_sidecar(first, title="A Title: With Punctuation!", text=text, page_count=10)
    _text_sidecar(second, metadata={"title": "a title with punctuation"}, text=text, page_count=12)

    groups = group_duplicates(scan_pdf_root(root))

    assert len(groups) == 1
    assert groups[0].canonical.path == second
    assert groups[0].reasons == ["normalized_title_first_text_hash"]


def test_groups_version_suffix_siblings_by_domain_and_base_stem(tmp_path):
    root = tmp_path / "scraped"
    base = _pdf(root / "journal.test" / "vermeule-a.pdf", b"%PDF base", mtime=10)
    suffixed = _pdf(root / "journal.test" / "vermeule-a-3.pdf", b"%PDF revised", mtime=20)
    other_domain = _pdf(root / "other.test" / "vermeule-a-3.pdf", b"%PDF other", mtime=30)

    groups = group_duplicates(scan_pdf_root(root))

    assert versionless_stem(suffixed) == "vermeule-a"
    assert len(groups) == 1
    assert groups[0].canonical.path == suffixed
    assert [r.path for r in groups[0].duplicates] == [base]
    assert other_domain not in [groups[0].canonical.path, groups[0].duplicates[0].path]
    assert groups[0].reasons == ["journal_domain_versionless_stem"]


def test_canonical_ties_on_size_then_page_count_then_path(tmp_path):
    root = tmp_path / "scraped"
    small = _pdf(root / "journal.test" / "small.pdf", b"%PDF x", mtime=10)
    large = _pdf(root / "journal.test" / "large.pdf", b"%PDF much larger", mtime=10)
    _text_sidecar(small, page_count=99)
    _text_sidecar(large, page_count=1)

    records = [build_record(small, root), build_record(large, root)]

    assert canonical_record(records).path == large


def test_write_csv_uses_required_columns(tmp_path):
    root = tmp_path / "scraped"
    first = _pdf(root / "journal.test" / "x.pdf", b"%PDF same", mtime=10)
    second = _pdf(root / "journal.test" / "x-2.pdf", b"%PDF same", mtime=20)
    output = tmp_path / "canonical_pdfs.csv"

    groups = group_duplicates(scan_pdf_root(root))
    write_csv(groups, output)

    rows = list(csv.DictReader(output.open(encoding="utf-8")))
    assert rows == [
        {
            "canonical_pdf": str(second),
            "duplicate_pdfs": str(first),
            "reason": "journal_domain_versionless_stem|sha256",
            "group_size": "2",
        }
    ]
