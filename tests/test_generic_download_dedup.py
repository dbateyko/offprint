"""Regression tests for re-download dedup in GenericAdapter.download_pdf.

The scraper renamed re-downloads that collided with an existing filename to
``foo-2.pdf``, ``foo-3.pdf``, ... even when the freshly fetched bytes were
identical to the file already on disk. That accreted redundant copies (one
essay was stored 9x) and stamped many distinct filenames with one PDF's
metadata. ``download_pdf`` must now hash-compare a ``-N`` variant against the
existing base file and skip writing it when identical.
"""

from __future__ import annotations

import os

import requests

from offprint.adapters import generic as generic_mod
from offprint.adapters.generic import GenericAdapter

_PDF_A = b"%PDF-1.4\n1 0 obj\nMartin Dickinson tribute\n%%EOF\n"
_PDF_B = b"%PDF-1.4\n1 0 obj\nA genuinely different article\n%%EOF\n"


class _FakeStreamResp:
    def __init__(self, content: bytes, status: int = 200, ctype: str = "application/pdf"):
        self._content = content
        self.status_code = status
        self.headers = requests.structures.CaseInsensitiveDict({"Content-Type": ctype})

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, chunk_size: int = 65536):
        yield self._content


class _FakeSession:
    def __init__(self, content: bytes):
        self._content = content

    def get(self, url, **kwargs):
        return _FakeStreamResp(self._content)


def _patch_precheck(monkeypatch):
    # HEAD pre-validation and TLS-verify decisions are irrelevant to dedup.
    monkeypatch.setattr(generic_mod, "pre_validate_pdf_url", lambda *a, **k: True)
    monkeypatch.setattr(generic_mod, "request_verify_for_url", lambda *a, **k: True)


def test_identical_redownload_is_skipped(tmp_path, monkeypatch):
    _patch_precheck(monkeypatch)
    out_dir = str(tmp_path)
    existing = os.path.join(out_dir, "essay.pdf")
    with open(existing, "wb") as f:
        f.write(_PDF_A)

    adapter = GenericAdapter(session=_FakeSession(_PDF_A))
    result = adapter.download_pdf("https://x.org/files/essay.pdf", out_dir)

    # Returns the pre-existing canonical file; no -2 variant is written.
    assert result == existing
    assert not os.path.exists(os.path.join(out_dir, "essay-2.pdf"))
    assert adapter.last_download_meta["ok"] is True
    assert adapter.last_download_meta["skipped_duplicate"] is True
    # Directory still holds exactly one copy.
    assert sorted(os.listdir(out_dir)) == ["essay.pdf"]


def test_different_content_still_written_as_variant(tmp_path, monkeypatch):
    _patch_precheck(monkeypatch)
    out_dir = str(tmp_path)
    existing = os.path.join(out_dir, "essay.pdf")
    with open(existing, "wb") as f:
        f.write(_PDF_A)

    adapter = GenericAdapter(session=_FakeSession(_PDF_B))
    result = adapter.download_pdf("https://x.org/files/essay.pdf", out_dir)

    # Genuinely different bytes must not be dropped: keep the -2 variant.
    assert result == os.path.join(out_dir, "essay-2.pdf")
    assert os.path.exists(result)
    assert adapter.last_download_meta["skipped_duplicate"] is False
    assert sorted(os.listdir(out_dir)) == ["essay-2.pdf", "essay.pdf"]


def test_first_download_writes_base_name(tmp_path, monkeypatch):
    _patch_precheck(monkeypatch)
    out_dir = str(tmp_path)

    adapter = GenericAdapter(session=_FakeSession(_PDF_A))
    result = adapter.download_pdf("https://x.org/files/essay.pdf", out_dir)

    assert result == os.path.join(out_dir, "essay.pdf")
    assert adapter.last_download_meta["skipped_duplicate"] is False


def test_existing_identical_sibling_helper(tmp_path):
    adapter = GenericAdapter(session=_FakeSession(_PDF_A))
    out_dir = str(tmp_path)
    base = os.path.join(out_dir, "foo.pdf")
    with open(base, "wb") as f:
        f.write(_PDF_A)
    variant = os.path.join(out_dir, "foo-3.pdf")
    with open(variant, "wb") as f:
        f.write(_PDF_A)

    import hashlib

    sha = hashlib.sha256(_PDF_A).hexdigest()
    # A -3 variant identical to the base resolves to the base file.
    assert adapter._existing_identical_sibling(variant, out_dir, sha) == base
    # A file with no dedup suffix has no sibling to dedup against.
    assert adapter._existing_identical_sibling(base, out_dir, sha) is None
    # Missing sha short-circuits.
    assert adapter._existing_identical_sibling(variant, out_dir, None) is None
