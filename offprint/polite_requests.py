from __future__ import annotations

import random
import threading
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict
from urllib3.util.retry import Retry

from .http_cache import HttpSnapshotCache, utc_now_iso


class PoliteRequestsSession(requests.Session):
    """requests.Session with per-request delay and retry/backoff built in."""

    def __init__(
        self,
        min_delay: float = 1.0,
        max_delay: float = 3.0,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        snapshot_cache: Optional[HttpSnapshotCache] = None,
        pool_maxsize: int = 32,
    ):
        super().__init__()
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._last_request_time = 0.0
        self._rate_lock = threading.Lock()
        self.snapshot_cache = snapshot_cache
        self._response_meta: Dict[str, Dict[str, Any]] = {}
        self._meta_lock = threading.Lock()

        from .adapters.generic import DISCOVERY_UA_PROFILES
        # Set a browser-like User-Agent to avoid simple 403 blocks
        self.headers.update(
            {
                "User-Agent": DISCOVERY_UA_PROFILES[0]
            }
        )

        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=pool_maxsize,
            pool_maxsize=pool_maxsize,
        )
        self.mount("http://", adapter)
        self.mount("https://", adapter)

    def _wait_for_rate_limit(self) -> None:
        """Sleep between requests to reduce 429s."""
        with self._rate_lock:
            delay = random.uniform(self.min_delay, self.max_delay)
            elapsed = time.time() - self._last_request_time
            if elapsed < delay:
                time.sleep(delay - elapsed)
            self._last_request_time = time.time()

    def _record_response_meta(
        self,
        url: str,
        *,
        status_code: int,
        content_type: str,
        retrieved_at: Optional[str] = None,
        from_cache: bool = False,
    ) -> None:
        with self._meta_lock:
            self._response_meta[url] = {
                "status": status_code,
                "content_type": content_type,
                "retrieved_at": retrieved_at or utc_now_iso(),
                "from_cache": from_cache,
            }

    def get_response_meta(self, url: str) -> Optional[Dict[str, Any]]:
        with self._meta_lock:
            value = self._response_meta.get(url)
            return dict(value) if value else None

    def _response_from_cache(
        self, method: str, url: str, **kwargs: Any
    ) -> Optional[requests.Response]:
        if not self.snapshot_cache:
            return None
        stream = bool(kwargs.get("stream", False))
        cached = self.snapshot_cache.load(method, url, stream=stream)
        if not cached:
            return None
        response = requests.Response()
        response.status_code = cached.status_code
        response._content = cached.body
        response.headers = CaseInsensitiveDict(cached.headers)
        response.url = url
        response.encoding = requests.utils.get_encoding_from_headers(response.headers)
        response.reason = "OK"
        response.request = requests.Request(method=method, url=url).prepare()
        self._record_response_meta(
            url,
            status_code=response.status_code,
            content_type=response.headers.get("Content-Type", ""),
            retrieved_at=cached.retrieved_at,
            from_cache=True,
        )
        return response

    def request(self, *args: Any, **kwargs: Any) -> requests.Response:
        method = str(args[0] if args else kwargs.get("method", "GET"))
        url = str(args[1] if len(args) > 1 else kwargs.get("url", ""))

        self._wait_for_rate_limit()

        cached_response = self._response_from_cache(method, url, **kwargs)
        if cached_response is not None:
            return cached_response

        response = super().request(*args, **kwargs)
        self._record_response_meta(
            url,
            status_code=response.status_code,
            content_type=response.headers.get("Content-Type", ""),
            retrieved_at=utc_now_iso(),
            from_cache=False,
        )
        if self.snapshot_cache is not None:
            stream = bool(kwargs.get("stream", False))
            self.snapshot_cache.save(method, url, response, stream=stream)
        return response

    def close(self) -> None:
        """Close session and force-close all pooled TCP connections.

        The default ``requests.Session.close()`` returns connections to the
        urllib3 pool but does **not** close the underlying sockets, leaving
        them in CLOSE_WAIT.  We explicitly call ``clear()`` on every mounted
        adapter's pool manager so the OS-level sockets are released
        immediately.
        """
        for adapter in self.adapters.values():
            try:
                adapter.poolmanager.clear()
            except Exception:
                pass
        super().close()
