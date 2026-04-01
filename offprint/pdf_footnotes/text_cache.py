"""Text extraction cache — gzip-compressed JSON sidecars for ExtractedDocument.

Avoids redundant PDF parsing when both the footnote pipeline and HF builder
process the same corpus.  Cache path: ``{pdf_path}.text_cache.json.gz``.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import tempfile
from typing import Any

from .text_extract import ExtractedDocument

log = logging.getLogger(__name__)

_CACHE_SUFFIX = ".text_cache.json.gz"
_CACHE_VERSION = 1


class TextExtractionCache:
    """Transparent read/write cache for :class:`ExtractedDocument`."""

    def __init__(self, cache_dir: str | None = None, enabled: bool = True) -> None:
        self.cache_dir = cache_dir
        self.enabled = enabled
        self._hits = 0
        self._misses = 0

    @property
    def hits(self) -> int:
        return self._hits

    @property
    def misses(self) -> int:
        return self._misses

    def _cache_path(self, pdf_path: str) -> str:
        if self.cache_dir:
            basename = os.path.basename(pdf_path) + _CACHE_SUFFIX
            return os.path.join(self.cache_dir, basename)
        return pdf_path + _CACHE_SUFFIX

    def get(self, pdf_path: str) -> ExtractedDocument | None:
        if not self.enabled:
            return None
        cache_path = self._cache_path(pdf_path)
        if not os.path.exists(cache_path):
            self._misses += 1
            return None
        # Staleness check: cache must be newer than the PDF
        try:
            pdf_mtime = os.path.getmtime(pdf_path)
            cache_mtime = os.path.getmtime(cache_path)
            if cache_mtime < pdf_mtime:
                self._misses += 1
                return None
        except OSError:
            self._misses += 1
            return None
        try:
            with gzip.open(cache_path, "rt", encoding="utf-8") as fh:
                payload: dict[str, Any] = json.load(fh)
            if payload.get("_cache_version") != _CACHE_VERSION:
                self._misses += 1
                return None
            doc = ExtractedDocument.from_dict(payload)
            # Restore original pdf_path (cache may have been created with a different abs path)
            doc.pdf_path = pdf_path
            self._hits += 1
            return doc
        except Exception:
            log.debug("Cache read failed for %s", pdf_path, exc_info=True)
            self._misses += 1
            return None

    def put(self, pdf_path: str, document: ExtractedDocument) -> None:
        if not self.enabled:
            return
        cache_path = self._cache_path(pdf_path)
        try:
            payload = document.to_dict()
            payload["_cache_version"] = _CACHE_VERSION
            parent = os.path.dirname(cache_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(dir=parent or ".", suffix=".tmp")
            try:
                # mkstemp returns a raw OS fd; close it and re-open by path for gzip/json writing.
                os.close(fd)
                with gzip.open(tmp_path, "wt", encoding="utf-8") as fh:
                    json.dump(payload, fh)
                os.replace(tmp_path, cache_path)
            except Exception:
                # Clean up temp file on failure
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception:
            log.debug("Cache write failed for %s", pdf_path, exc_info=True)
