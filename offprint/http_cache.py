from __future__ import annotations

import gzip
import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

import requests


CACHEABLE_CONTENT_TYPES = (
    "text/html",
    "application/xhtml+xml",
    "application/xml",
    "text/xml",
    "application/json",
    "text/json",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc_iso(ts: str) -> Optional[float]:
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


@dataclass
class CachedSnapshot:
    status_code: int
    headers: Dict[str, str]
    body: bytes
    retrieved_at: str
    content_type: str


class HttpSnapshotCache:
    """Persistent GET cache used for provenance snapshots and request reuse."""

    def __init__(
        self,
        cache_dir: str,
        ttl_seconds: int = 86400,
        max_bytes: int = 2_147_483_648,
        scope: Optional[str] = None,
    ):
        self.cache_dir = cache_dir
        self.ttl_seconds = max(ttl_seconds, 0)
        self.max_bytes = max(max_bytes, 0)
        self.scope = scope
        self._lock = threading.Lock()
        os.makedirs(self.cache_dir, exist_ok=True)

    def _key(self, url: str) -> str:
        return hashlib.sha1(url.encode("utf-8")).hexdigest()

    def _meta_path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.json")

    def _body_path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.body.gz")

    def _is_pdf_like_url(self, url: str) -> bool:
        lowered = url.lower()
        if lowered.endswith(".pdf") or ".pdf?" in lowered or "/pdf" in lowered:
            return True
        # Digital Commons/Bepress direct download pattern.
        if "viewcontent.cgi" in lowered and "article=" in lowered:
            return True
        return False

    def _is_cacheable_content_type(self, content_type: str) -> bool:
        normalized = (content_type or "").split(";", 1)[0].strip().lower()
        return any(normalized.startswith(prefix) for prefix in CACHEABLE_CONTENT_TYPES)

    def _can_cache(self, method: str, url: str, stream: bool = False) -> bool:
        if method.upper() != "GET":
            return False
        if stream:
            return False
        if self._is_pdf_like_url(url):
            return False
        return True

    def load(self, method: str, url: str, stream: bool = False) -> Optional[CachedSnapshot]:
        if not self._can_cache(method, url, stream=stream):
            return None
        key = self._key(url)
        meta_path = self._meta_path(key)
        body_path = self._body_path(key)
        with self._lock:
            if not os.path.exists(meta_path) or not os.path.exists(body_path):
                return None
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
            except Exception:
                return None

            retrieved_at = str(meta.get("retrieved_at", ""))
            if self.scope:
                meta_scope = str(meta.get("scope") or "")
                if meta_scope != self.scope:
                    return None
            ts = parse_utc_iso(retrieved_at)
            if ts is None or (self.ttl_seconds and time.time() - ts > self.ttl_seconds):
                self._delete_key(key)
                return None

            try:
                with gzip.open(body_path, "rb") as f:
                    body = f.read()
            except Exception:
                self._delete_key(key)
                return None

            return CachedSnapshot(
                status_code=int(meta.get("status", 0) or 0),
                headers=dict(meta.get("headers") or {}),
                body=body,
                retrieved_at=retrieved_at,
                content_type=str(meta.get("content_type") or ""),
            )

    def save(
        self, method: str, url: str, response: requests.Response, stream: bool = False
    ) -> None:
        if not self._can_cache(method, url, stream=stream):
            return
        status_code = int(response.status_code)
        if status_code >= 400:
            return
        content_type = response.headers.get("Content-Type", "")
        if not self._is_cacheable_content_type(content_type):
            return

        key = self._key(url)
        meta_path = self._meta_path(key)
        body_path = self._body_path(key)
        meta_tmp = f"{meta_path}.tmp"
        body_tmp = f"{body_path}.tmp"

        payload = {
            "url": url,
            "method": method.upper(),
            "status": status_code,
            "headers": dict(response.headers),
            "content_type": content_type,
            "retrieved_at": utc_now_iso(),
            "body_path": os.path.basename(body_path),
            "scope": self.scope or "",
        }

        with self._lock:
            try:
                with gzip.open(body_tmp, "wb") as f:
                    f.write(response.content)
                with open(meta_tmp, "w", encoding="utf-8") as f:
                    json.dump(payload, f, sort_keys=True)
                os.replace(body_tmp, body_path)
                os.replace(meta_tmp, meta_path)
            finally:
                for tmp in (meta_tmp, body_tmp):
                    if os.path.exists(tmp):
                        try:
                            os.remove(tmp)
                        except OSError:
                            pass
            self._evict_if_needed()

    def _cache_size_bytes(self) -> int:
        total = 0
        for name in os.listdir(self.cache_dir):
            path = os.path.join(self.cache_dir, name)
            if os.path.isfile(path):
                try:
                    total += os.path.getsize(path)
                except OSError:
                    pass
        return total

    def _evict_if_needed(self) -> None:
        if self.max_bytes <= 0:
            return
        total = self._cache_size_bytes()
        if total <= self.max_bytes:
            return

        candidates = []
        for name in os.listdir(self.cache_dir):
            if not name.endswith(".json"):
                continue
            meta_path = os.path.join(self.cache_dir, name)
            key = name[: -len(".json")]
            body_path = self._body_path(key)
            try:
                mtime = os.path.getmtime(meta_path)
            except OSError:
                continue
            size = 0
            for p in (meta_path, body_path):
                if os.path.exists(p):
                    try:
                        size += os.path.getsize(p)
                    except OSError:
                        pass
            candidates.append((mtime, key, size))

        candidates.sort(key=lambda item: item[0])
        for _mtime, key, size in candidates:
            if total <= self.max_bytes:
                break
            self._delete_key(key)
            total -= size

    def _delete_key(self, key: str) -> None:
        for path in (self._meta_path(key), self._body_path(key)):
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
