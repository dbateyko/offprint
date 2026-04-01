"""Playwright-based session for scraping with rate limiting and browser simulation.

Browser pool management
-----------------------
By default every thread that calls ``_ensure_browser()`` launches its own
Chromium process.  Call ``PlaywrightSession.set_max_browsers(n)`` once before
any requests to cap the number of concurrent browser processes globally
(across all ``PlaywrightSession`` instances).  Threads that cannot acquire a
slot block until one is released by ``close()``.
"""

from __future__ import annotations

import json
import random
import threading
import time
from typing import ClassVar, Optional, Union
from dataclasses import dataclass


class HTTPError(Exception):
    """HTTP error raised by raise_for_status()."""

    def __init__(self, message: str, response: "PlaywrightResponse"):
        super().__init__(message)
        self.response = response


@dataclass
class PlaywrightResponse:
    """Mimics requests.Response interface for compatibility."""

    status_code: int
    text: str
    content: bytes
    headers: dict
    url: str

    def __bool__(self):
        return self.status_code < 400

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def iter_content(self, chunk_size=1, decode_unicode=False):
        """Iterate over response content."""
        if not self.content:
            yield b""
            return

        for i in range(0, len(self.content), chunk_size):
            yield self.content[i : i + chunk_size]

    def raise_for_status(self):
        """Raise HTTPError if status code indicates an error."""
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code} error for URL: {self.url}", self)

    def json(self):
        """Parse response text as JSON."""
        return json.loads(self.text)


class PlaywrightSession:
    """Wrapper around Playwright that mimics requests.Session interface.

    Uses threading.local to manage Playwright instances per thread, avoiding
    asyncio event loop conflicts in multi-threaded environments.

    A class-level semaphore (set via ``set_max_browsers``) caps the total
    number of concurrent Chromium processes across all instances and threads.
    """

    # ------------------------------------------------------------------
    # Class-level browser pool cap
    # ------------------------------------------------------------------
    _browser_semaphore: ClassVar[Optional[threading.Semaphore]] = None
    _max_browsers: ClassVar[int] = 0  # 0 = unlimited

    @classmethod
    def set_max_browsers(cls, n: int) -> None:
        """Cap the total number of concurrent Chromium processes.

        Must be called before any ``_ensure_browser`` calls.  Affects *all*
        ``PlaywrightSession`` instances (including adapter-created fallback
        sessions).
        """
        if n > 0:
            cls._browser_semaphore = threading.Semaphore(n)
            cls._max_browsers = n
        else:
            cls._browser_semaphore = None
            cls._max_browsers = 0

    # ------------------------------------------------------------------

    def __init__(
        self,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        headless: bool = False,
        max_retries: int = 3,
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.headless = headless
        self.max_retries = max_retries
        self._last_request_time = 0
        self._local = threading.local()

    @property
    def _playwright(self):
        return getattr(self._local, "playwright", None)

    @_playwright.setter
    def _playwright(self, val):
        self._local.playwright = val

    @property
    def _browser(self):
        return getattr(self._local, "browser", None)

    @_browser.setter
    def _browser(self, val):
        self._local.browser = val

    @property
    def _context(self):
        return getattr(self._local, "context", None)

    @_context.setter
    def _context(self, val):
        self._local.context = val

    @property
    def _page(self):
        return getattr(self._local, "page", None)

    @_page.setter
    def _page(self, val):
        self._local.page = val

    def _ensure_browser(self):
        """Lazily initialize browser on first request for current thread.

        When a browser pool cap is active, blocks until a slot is available.
        """
        if self._browser is not None:
            return

        holds = bool(getattr(self._local, "_holds_semaphore", False))
        acquired_here = False

        # Acquire a slot from the global pool (blocks if full), unless this
        # thread already holds one from a prior attempt.
        sem = PlaywrightSession._browser_semaphore
        if sem is not None and not holds:
            sem.acquire()
            self._local._holds_semaphore = True
            acquired_here = True

        try:
            from playwright.sync_api import sync_playwright

            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(headless=self.headless)
            self._context = self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            self._page = self._context.new_page()
        except Exception:
            # Ensure failed launches do not leak browser-pool slots.
            try:
                if self._page:
                    self._page.close()
            except Exception:
                pass
            self._page = None
            try:
                if self._context:
                    self._context.close()
            except Exception:
                pass
            self._context = None
            try:
                if self._browser:
                    self._browser.close()
            except Exception:
                pass
            self._browser = None
            try:
                if self._playwright:
                    self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

            if acquired_here and sem is not None:
                sem.release()
                self._local._holds_semaphore = False
            raise

    def _wait_for_rate_limit(self):
        """Wait between requests to avoid rate limiting."""
        elapsed = time.time() - self._last_request_time
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request_time = time.time()

    def _is_json_request(self, url: str, headers: Optional[dict]) -> bool:
        """Detect JSON/API requests so we can bypass DOM rendering."""
        accept = ""
        if headers:
            accept = headers.get("Accept") or headers.get("accept") or ""
        lowered = url.lower()
        return (
            "json" in accept.lower()
            or lowered.endswith(".json")
            or "/wp-json" in lowered
            or "/api/" in lowered
        )

    def _is_binary_download_request(self, url: str, headers: Optional[dict]) -> bool:
        """Detect PDF/binary endpoints that should use request API, not page navigation."""
        accept = ""
        if headers:
            accept = str(headers.get("Accept") or headers.get("accept") or "").lower()
        lowered = (url or "").lower()
        return (
            "application/pdf" in accept
            or "application/octet-stream" in accept
            or lowered.endswith(".pdf")
            or "viewcontent.cgi" in lowered
            or "/viewcontent/" in lowered
        )

    @staticmethod
    def _is_textual_content_type(content_type: str) -> bool:
        lowered = (content_type or "").lower()
        return (
            "text/" in lowered
            or "application/json" in lowered
            or "application/javascript" in lowered
            or "application/xml" in lowered
        )

    def _request_get(
        self, url: str, headers: dict, timeout_ms: float, **kwargs
    ) -> PlaywrightResponse:
        """Perform a request-context GET and normalize response shape."""
        request_kwargs = {"headers": headers, "timeout": timeout_ms}
        if kwargs.get("allow_redirects") is False:
            request_kwargs["max_redirects"] = 0

        response = self._context.request.get(url, **request_kwargs)
        response_headers = dict(response.headers)
        body = response.body() or b""
        content_type = str(
            response_headers.get("content-type")
            or response_headers.get("Content-Type")
            or ""
        )
        text = ""
        if self._is_textual_content_type(content_type):
            text = body.decode("utf-8", errors="ignore")

        return PlaywrightResponse(
            status_code=response.status,
            text=text,
            content=body,
            headers=response_headers,
            url=getattr(response, "url", url),
        )

    def get(
        self,
        url: str,
        headers: Optional[dict] = None,
        timeout: Union[int, float, tuple] = 30,
        **kwargs,
    ) -> Optional[PlaywrightResponse]:
        """Fetch a URL using Playwright, mimicking requests.Session.get()."""
        self._ensure_browser()
        headers = headers or {}

        # Handle tuple timeouts (connect, read)
        if isinstance(timeout, (tuple, list)):
            actual_timeout_ms = sum(timeout) * 1000
        else:
            actual_timeout_ms = float(timeout) * 1000

        for attempt in range(self.max_retries):
            self._wait_for_rate_limit()

            try:
                if self._is_json_request(
                    url, headers
                ) or self._is_binary_download_request(url, headers):
                    normalized = self._request_get(
                        url=url,
                        headers=headers,
                        timeout_ms=actual_timeout_ms,
                        **kwargs,
                    )
                    if normalized.status_code == 429:
                        wait_time = (2**attempt) * 10 + random.uniform(1, 5)
                        time.sleep(wait_time)
                        continue
                    return normalized

                # Set up download listener before navigation
                download_info = {"url": None}

                def handle_download(download):
                    download_info["url"] = download.url

                self._page.on("download", handle_download)

                try:
                    response = self._page.goto(
                        url, timeout=actual_timeout_ms, wait_until="networkidle"
                    )

                    # Handle SiteGround/Generic Captcha challenges
                    if (
                        response
                        and response.status == 202
                        and "captcha" in response.text().lower()
                    ):
                        time.sleep(5)
                        response = self._page.request.get(url)

                except Exception as e:
                    error_str = str(e)
                    if (
                        "Download is starting" in error_str
                        or "net::ERR_ABORTED" in error_str
                    ):
                        if ".pdf" in url.lower() or "viewcontent" in url.lower():
                            try:
                                return self._request_get(
                                    url=url,
                                    headers=headers,
                                    timeout_ms=actual_timeout_ms,
                                    **kwargs,
                                )
                            except Exception:
                                self._page.remove_listener(
                                    "download", handle_download
                                )
                                return PlaywrightResponse(
                                    status_code=0,
                                    text="",
                                    content=b"",
                                    headers={},
                                    url=download_info["url"] or url,
                                )
                    raise e
                finally:
                    try:
                        self._page.remove_listener("download", handle_download)
                    except Exception:
                        pass

                if response is None:
                    return None

                # Get page content after JavaScript execution
                content = self._page.content()

                return PlaywrightResponse(
                    status_code=response.status,
                    text=content,
                    content=content.encode("utf-8"),
                    headers=dict(response.headers),
                    url=self._page.url,
                )

            except Exception:
                if attempt < self.max_retries - 1:
                    time.sleep(2**attempt)
                    continue
                return None

        return None

    def head(
        self,
        url: str,
        headers: Optional[dict] = None,
        timeout: Union[int, float, tuple] = 30,
        **kwargs,
    ) -> Optional[PlaywrightResponse]:
        """Perform a HEAD request using Playwright's request context."""
        self._ensure_browser()
        self._wait_for_rate_limit()

        if isinstance(timeout, (tuple, list)):
            actual_timeout_ms = sum(timeout) * 1000
        else:
            actual_timeout_ms = float(timeout) * 1000

        try:
            response = self._context.request.head(url, timeout=actual_timeout_ms)
            return PlaywrightResponse(
                status_code=response.status,
                text="",
                content=b"",
                headers=dict(response.headers),
                url=url,
            )
        except Exception:
            return None

    def close(self):
        """Clean up browser resources for the current thread."""
        holds = getattr(self._local, "_holds_semaphore", False)
        if self._page:
            self._page.close()
            self._page = None
        if self._context:
            self._context.close()
            self._context = None
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._playwright:
            self._playwright.stop()
            self._playwright = None
        # Release semaphore slot AFTER browser is fully closed.
        if holds:
            sem = PlaywrightSession._browser_semaphore
            if sem is not None:
                sem.release()
            self._local._holds_semaphore = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
