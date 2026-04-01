"""Browser-based fallback for WAF-blocked PDF downloads.

This module provides browser-based PDF downloads using either:
1. Camoufox - A stealthy Firefox-based browser with anti-detection (recommended)
2. Chrome DevTools MCP - Chrome browser automation via MCP protocol

The approach:
1. Spawn a browser instance (Camoufox or Chrome)
2. Navigate to PDF URLs in a real browser context
3. Let browser auto-download PDFs to a staging directory
4. Monitor staging directory and move completed PDFs to final location

Camoufox is preferred because:
- Modified Firefox with built-in fingerprint spoofing
- Automatic device/screen/WebGL fingerprint rotation
- Works with proxies and geoip matching
- Humanized cursor movements
- No automation detection flags

Chrome DevTools MCP is an alternative that:
- Uses real Chrome browser with genuine fingerprint
- Persistent user data directory (cookies, session state)
- Requires external MCP server setup
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Literal
from urllib.parse import urlparse


def _run_in_clean_thread(func, *args, **kwargs):
    """Run a function in a thread without asyncio event loop interference.

    Playwright's sync API fails when called from a thread that has an asyncio
    event loop running. This helper runs the function in a separate thread
    that explicitly has no event loop.
    """
    result = [None]
    exception = [None]

    def worker():
        # Ensure no event loop in this thread
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.set_event_loop(None)
        except RuntimeError:
            pass  # No event loop, which is what we want

        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            exception[0] = e

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=300)  # 5 minute max timeout

    if exception[0] is not None:
        raise exception[0]
    return result[0]


# Try to import camoufox
try:
    from camoufox.sync_api import Camoufox

    CAMOUFOX_AVAILABLE = True
except ImportError:
    Camoufox = None  # type: ignore
    CAMOUFOX_AVAILABLE = False

# Try to import Playwright sync API
try:
    from playwright.sync_api import sync_playwright

    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    sync_playwright = None  # type: ignore
    PLAYWRIGHT_AVAILABLE = False

# Default Chrome download timeout
DEFAULT_BROWSER_TIMEOUT = 60
# Interval to poll for downloaded files
POLL_INTERVAL_SECONDS = 0.5
# Maximum time to wait for a single download
MAX_DOWNLOAD_WAIT_SECONDS = 120


@dataclass
class BrowserDownloadResult:
    """Result of a browser-based PDF download attempt."""

    url: str
    ok: bool
    local_path: Optional[str] = None
    error_type: str = ""
    message: str = ""
    download_time_seconds: float = 0.0
    operator_intervention_prompted: int = 0
    operator_intervention_retries: int = 0
    operator_intervention_recovered: bool = False
    operator_intervention_unresolved: bool = False


@dataclass
class OperatorInterventionConfig:
    enabled: bool = False
    scope: Literal["off", "browser_fallback_only"] = "off"
    wait_mode: Literal["prompt_enter", "off"] = "off"
    manual_retries: int = 1
    run_id: str = ""
    events_path: str = ""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_operator_config(
    config: Optional[OperatorInterventionConfig | Dict[str, Any]],
) -> Optional[OperatorInterventionConfig]:
    if config is None:
        return None
    if isinstance(config, OperatorInterventionConfig):
        return config
    if not isinstance(config, dict):
        return None
    scope = str(config.get("scope") or "off").strip().lower()
    if scope not in {"off", "browser_fallback_only"}:
        scope = "off"
    wait_mode = str(config.get("wait_mode") or "off").strip().lower()
    if wait_mode not in {"prompt_enter", "off"}:
        wait_mode = "off"
    return OperatorInterventionConfig(
        enabled=bool(config.get("enabled")),
        scope=scope,  # type: ignore[arg-type]
        wait_mode=wait_mode,  # type: ignore[arg-type]
        manual_retries=max(int(config.get("manual_retries") or 0), 0),
        run_id=str(config.get("run_id") or ""),
        events_path=str(config.get("events_path") or ""),
    )


def _append_operator_event(events_path: str, payload: Dict[str, Any]) -> None:
    if not events_path:
        return
    directory = os.path.dirname(events_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    row = dict(payload)
    row.setdefault("timestamp", _utc_now_iso())
    with open(events_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")


def _is_operator_intervention_candidate(result: BrowserDownloadResult) -> bool:
    candidate_errors = {"waf_challenge_failed", "download_timeout", "navigation_error", "not_pdf"}
    error_type = str(result.error_type or "").strip().lower()
    if error_type in candidate_errors:
        return True
    haystack = f"{error_type} {str(result.message or '').lower()}"
    keywords = ("captcha", "challenge", "verify", "login", "authentication")
    return any(keyword in haystack for keyword in keywords)


@dataclass
class MCPBrowserDownloader:
    """Chrome DevTools MCP-based browser for WAF fallback downloads.

    This class manages a Chrome browser instance via the Chrome DevTools MCP
    server for downloading PDFs that are blocked by WAF challenges.

    Usage:
        downloader = MCPBrowserDownloader(
            staging_dir="artifacts/browser_staging",
            user_data_dir="artifacts/chrome_profile",
            final_out_dir="artifacts/pdfs",
        )

        # Add URLs to download
        downloader.add_url("https://example.com/blocked.pdf", domain="example.com")

        # Process all URLs in batch
        results = downloader.process_batch()
    """

    staging_dir: str = "artifacts/browser_staging"
    user_data_dir: str = "artifacts/chrome_profile"
    final_out_dir: str = "artifacts/pdfs"
    timeout_seconds: int = DEFAULT_BROWSER_TIMEOUT
    visible: bool = True  # Show browser window for debugging

    # Internal state
    _pending_urls: List[Dict[str, Any]] = field(default_factory=list)
    _processed_urls: Set[str] = field(default_factory=set)
    _mcp_process: Optional[subprocess.Popen] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)
    operator_config: Optional[OperatorInterventionConfig] = None

    def __post_init__(self):
        """Initialize directories."""
        os.makedirs(self.staging_dir, exist_ok=True)
        os.makedirs(self.user_data_dir, exist_ok=True)
        os.makedirs(self.final_out_dir, exist_ok=True)
        self._pending_urls = []
        self._processed_urls = set()
        self._lock = threading.Lock()

    def add_url(
        self,
        url: str,
        *,
        domain: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        referer: str = "",
    ) -> None:
        """Add a URL to the pending download queue.

        Args:
            url: PDF URL to download
            domain: Domain for organizing output directory
            metadata: Optional metadata to associate with the download
            referer: Optional referer URL
        """
        with self._lock:
            if url in self._processed_urls:
                return

            if not domain:
                domain = urlparse(url).netloc or "unknown"

            self._pending_urls.append(
                {
                    "url": url,
                    "domain": domain,
                    "metadata": metadata or {},
                    "referer": referer,
                }
            )

    def get_pending_count(self) -> int:
        """Get number of URLs pending download."""
        with self._lock:
            return len(self._pending_urls)

    def _ensure_staging_empty(self) -> None:
        """Clear the staging directory before batch processing."""
        if os.path.exists(self.staging_dir):
            for item in os.listdir(self.staging_dir):
                item_path = os.path.join(self.staging_dir, item)
                try:
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                except OSError:
                    pass

    def _wait_for_download(
        self,
        *,
        expected_filename: Optional[str] = None,
        timeout: float = MAX_DOWNLOAD_WAIT_SECONDS,
    ) -> Optional[str]:
        """Wait for a file to appear in the staging directory.

        Args:
            expected_filename: Optional expected filename to look for
            timeout: Maximum seconds to wait

        Returns:
            Path to downloaded file, or None if timeout
        """
        start = time.time()
        seen_files: Set[str] = set()

        # Record initial files
        for item in os.listdir(self.staging_dir):
            seen_files.add(item)

        while (time.time() - start) < timeout:
            time.sleep(POLL_INTERVAL_SECONDS)

            current_files = set(os.listdir(self.staging_dir))
            new_files = current_files - seen_files

            for filename in new_files:
                filepath = os.path.join(self.staging_dir, filename)

                # Skip Chrome temporary download files
                if filename.endswith(".crdownload") or filename.endswith(".tmp"):
                    continue

                # Check if file is a PDF
                if filename.lower().endswith(".pdf"):
                    # Verify file is complete (not still being written)
                    try:
                        size1 = os.path.getsize(filepath)
                        time.sleep(0.2)
                        size2 = os.path.getsize(filepath)
                        if size1 == size2 and size1 > 0:
                            return filepath
                    except OSError:
                        continue

                # If not a PDF but expected, check anyway
                if expected_filename and filename == expected_filename:
                    return filepath

            seen_files = current_files

        return None

    def _move_to_final(
        self,
        staging_path: str,
        domain: str,
    ) -> str:
        """Move a downloaded file from staging to final output directory.

        Args:
            staging_path: Path in staging directory
            domain: Domain for subdirectory organization

        Returns:
            Final path where file was moved
        """
        domain_dir = os.path.join(self.final_out_dir, domain)
        os.makedirs(domain_dir, exist_ok=True)

        filename = os.path.basename(staging_path)
        final_path = os.path.join(domain_dir, filename)

        # Handle collisions
        if os.path.exists(final_path):
            base, ext = os.path.splitext(filename)
            idx = 2
            while True:
                candidate = os.path.join(domain_dir, f"{base}-{idx}{ext}")
                if not os.path.exists(candidate):
                    final_path = candidate
                    break
                idx += 1

        shutil.move(staging_path, final_path)
        return final_path

    def _download_single_url(
        self,
        url: str,
        domain: str,
        referer: str = "",
    ) -> BrowserDownloadResult:
        """Download a single URL using browser automation.

        This method should be overridden by subclasses that implement
        actual MCP browser communication. The base implementation
        provides a stub that can be used for testing.

        Args:
            url: URL to download
            domain: Domain for output organization
            referer: Optional referer header

        Returns:
            BrowserDownloadResult with outcome
        """
        start_time = time.time()

        # Clear staging before download
        self._ensure_staging_empty()

        # In a real implementation, this would:
        # 1. Send MCP tool call to navigate to the URL
        # 2. Wait for download to complete
        # 3. Move file to final location

        # For now, return a stub result indicating MCP is not available
        return BrowserDownloadResult(
            url=url,
            ok=False,
            local_path=None,
            error_type="mcp_not_implemented",
            message="Chrome DevTools MCP browser automation not yet implemented",
            download_time_seconds=time.time() - start_time,
        )

    def process_batch(
        self,
        *,
        progress_callback: Optional[callable] = None,
    ) -> List[BrowserDownloadResult]:
        """Process all pending URLs in batch.

        Args:
            progress_callback: Optional callback(current, total, url) for progress

        Returns:
            List of BrowserDownloadResult for each URL
        """
        results: List[BrowserDownloadResult] = []

        with self._lock:
            pending = list(self._pending_urls)
            self._pending_urls.clear()

        total = len(pending)
        for idx, item in enumerate(pending):
            url = item["url"]
            domain = item["domain"]
            referer = item.get("referer", "")

            if progress_callback:
                progress_callback(idx + 1, total, url)

            result = self._download_single_url(url, domain, referer)
            operator_cfg = _coerce_operator_config(getattr(self, "operator_config", None))
            prompted = 0
            retries = 0
            recovered = False
            unresolved = False
            operator_enabled = bool(
                operator_cfg
                and operator_cfg.enabled
                and operator_cfg.scope == "browser_fallback_only"
                and operator_cfg.wait_mode == "prompt_enter"
            )
            max_manual_retries = max(
                int((operator_cfg.manual_retries if operator_cfg else 0) or 0), 0
            )
            while (
                operator_enabled
                and retries < max_manual_retries
                and not bool(result.ok)
                and _is_operator_intervention_candidate(result)
            ):
                prompted += 1
                _append_operator_event(
                    str(operator_cfg.events_path if operator_cfg else ""),
                    {
                        "run_id": str(operator_cfg.run_id if operator_cfg else ""),
                        "event": "prompted",
                        "url": url,
                        "domain": domain,
                        "error_type": str(result.error_type or ""),
                        "message": str(result.message or ""),
                        "attempt": retries + 1,
                    },
                )
                print(
                    f"🛑 Operator intervention required for {domain}: "
                    f"{result.error_type or 'download_failed'} - {result.message}",
                    flush=True,
                )
                try:
                    input(
                        "    Solve captcha/login in the visible browser if needed, then press Enter to retry..."
                    )
                except EOFError:
                    print(
                        "⚠️ Operator prompt unavailable (non-interactive stdin); skipping manual retry.",
                        flush=True,
                    )
                    break
                retries += 1
                _append_operator_event(
                    str(operator_cfg.events_path if operator_cfg else ""),
                    {
                        "run_id": str(operator_cfg.run_id if operator_cfg else ""),
                        "event": "retrying",
                        "url": url,
                        "domain": domain,
                        "attempt": retries,
                    },
                )
                result = self._download_single_url(url, domain, referer)
                if bool(result.ok):
                    recovered = True
                    break
            if prompted > 0 and not bool(result.ok):
                unresolved = True
            if prompted > 0:
                _append_operator_event(
                    str(operator_cfg.events_path if operator_cfg else ""),
                    {
                        "run_id": str(operator_cfg.run_id if operator_cfg else ""),
                        "event": "recovered" if recovered else "unresolved",
                        "url": url,
                        "domain": domain,
                        "attempts_used": retries,
                        "final_error_type": str(result.error_type or ""),
                        "final_message": str(result.message or ""),
                    },
                )
            result.operator_intervention_prompted = prompted
            result.operator_intervention_retries = retries
            result.operator_intervention_recovered = recovered
            result.operator_intervention_unresolved = unresolved
            results.append(result)

            with self._lock:
                self._processed_urls.add(url)

        return results

    def close(self) -> None:
        """Clean up resources."""
        if self._mcp_process is not None:
            try:
                self._mcp_process.terminate()
                self._mcp_process.wait(timeout=5)
            except Exception:
                try:
                    self._mcp_process.kill()
                except Exception:
                    pass
            self._mcp_process = None


class ChromeMCPDownloader(MCPBrowserDownloader):
    """Chrome DevTools MCP implementation using subprocess communication.

    This class spawns the Chrome DevTools MCP server as a subprocess and
    communicates with it via stdin/stdout JSON-RPC messages.
    """

    def __init__(
        self,
        staging_dir: str = "artifacts/browser_staging",
        user_data_dir: str = "artifacts/chrome_profile",
        final_out_dir: str = "artifacts/pdfs",
        timeout_seconds: int = DEFAULT_BROWSER_TIMEOUT,
        visible: bool = True,
        mcp_server_command: Optional[List[str]] = None,
        chrome_path: Optional[str] = None,
    ):
        # Set attributes
        self.staging_dir = staging_dir
        self.user_data_dir = user_data_dir
        self.final_out_dir = final_out_dir
        self.timeout_seconds = timeout_seconds
        self.visible = visible

        # Chrome MCP specific
        self.mcp_server_command = mcp_server_command or [
            "npx",
            "--yes",
            "chrome-devtools-mcp",
        ]
        self.chrome_path = chrome_path

        # Internal state
        self._pending_urls: List[Dict[str, Any]] = []
        self._processed_urls: Set[str] = set()
        self._mcp_process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self.operator_config: Optional[OperatorInterventionConfig] = None

        # Initialize directories
        os.makedirs(self.staging_dir, exist_ok=True)
        os.makedirs(self.user_data_dir, exist_ok=True)
        os.makedirs(self.final_out_dir, exist_ok=True)

    def _start_mcp_server(self) -> bool:
        """Start the Chrome DevTools MCP server subprocess.

        Returns:
            True if server started successfully
        """
        if self._mcp_process is not None:
            return True

        try:
            env = os.environ.copy()

            # Build command with options
            cmd = list(self.mcp_server_command)
            if self.chrome_path:
                cmd.extend(["--chrome-path", self.chrome_path])
            if self.user_data_dir:
                cmd.extend(["--user-data-dir", os.path.abspath(self.user_data_dir)])
            if self.visible:
                cmd.append("--no-headless")

            self._mcp_process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                text=True,
            )

            # Wait a moment for server to initialize
            time.sleep(2)

            if self._mcp_process.poll() is not None:
                # Process exited immediately
                stderr = self._mcp_process.stderr.read() if self._mcp_process.stderr else ""
                print(f"⚠️ MCP server failed to start: {stderr}", flush=True)
                self._mcp_process = None
                return False

            return True

        except Exception as exc:
            print(f"⚠️ Failed to start MCP server: {exc}", flush=True)
            self._mcp_process = None
            return False

    def _send_mcp_request(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Send a JSON-RPC request to the MCP server.

        Args:
            method: MCP method name (e.g., "tools/call")
            params: Optional parameters

        Returns:
            Response dict or None on error
        """
        if self._mcp_process is None or self._mcp_process.stdin is None:
            return None

        request = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": method,
            "params": params or {},
        }

        try:
            request_str = json.dumps(request) + "\n"
            self._mcp_process.stdin.write(request_str)
            self._mcp_process.stdin.flush()

            # Read response (blocking)
            if self._mcp_process.stdout:
                response_str = self._mcp_process.stdout.readline()
                if response_str:
                    return json.loads(response_str)
        except Exception as exc:
            print(f"⚠️ MCP request failed: {exc}", flush=True)

        return None

    def _download_single_url(
        self,
        url: str,
        domain: str,
        referer: str = "",
    ) -> BrowserDownloadResult:
        """Download a single URL using Chrome DevTools MCP.

        Args:
            url: URL to download
            domain: Domain for output organization
            referer: Optional referer header

        Returns:
            BrowserDownloadResult with outcome
        """
        start_time = time.time()

        # Ensure MCP server is running
        if not self._start_mcp_server():
            if PLAYWRIGHT_AVAILABLE:
                fallback = PlaywrightDownloader(
                    staging_dir=self.staging_dir,
                    user_data_dir=self.user_data_dir,
                    final_out_dir=self.final_out_dir,
                    timeout_seconds=self.timeout_seconds,
                    headless=not self.visible,
                )
                fallback.operator_config = self.operator_config
                return fallback._download_single_url(url, domain, referer)
            return BrowserDownloadResult(
                url=url,
                ok=False,
                local_path=None,
                error_type="mcp_server_error",
                message="Failed to start Chrome DevTools MCP server",
                download_time_seconds=time.time() - start_time,
            )

        # Clear staging before download
        self._ensure_staging_empty()

        # Navigate to URL using MCP tool
        nav_response = self._send_mcp_request(
            "tools/call",
            {
                "name": "navigate_page",
                "arguments": {
                    "url": url,
                },
            },
        )

        if nav_response is None or nav_response.get("error"):
            error_msg = (nav_response or {}).get("error", {}).get("message", "Navigation failed")
            return BrowserDownloadResult(
                url=url,
                ok=False,
                local_path=None,
                error_type="navigation_error",
                message=error_msg,
                download_time_seconds=time.time() - start_time,
            )

        # Wait for download to appear in staging directory
        downloaded_path = self._wait_for_download(timeout=self.timeout_seconds)

        if downloaded_path is None:
            return BrowserDownloadResult(
                url=url,
                ok=False,
                local_path=None,
                error_type="download_timeout",
                message=f"Download did not complete within {self.timeout_seconds}s",
                download_time_seconds=time.time() - start_time,
            )

        # Move to final location
        try:
            final_path = self._move_to_final(downloaded_path, domain)
            return BrowserDownloadResult(
                url=url,
                ok=True,
                local_path=final_path,
                error_type="",
                message="",
                download_time_seconds=time.time() - start_time,
            )
        except Exception as exc:
            return BrowserDownloadResult(
                url=url,
                ok=False,
                local_path=None,
                error_type="filesystem_error",
                message=str(exc),
                download_time_seconds=time.time() - start_time,
            )


class PlaywrightDownloader(MCPBrowserDownloader):
    """Playwright Chromium downloader for browser-based PDF retrieval.

    This backend uses a real Chromium browser to obtain session cookies/user-agent,
    then performs the PDF request with those credentials.
    """

    def __init__(
        self,
        staging_dir: str = "artifacts/browser_staging",
        user_data_dir: str = "artifacts/playwright_profile",
        final_out_dir: str = "artifacts/pdfs",
        timeout_seconds: int = DEFAULT_BROWSER_TIMEOUT,
        headless: bool = False,
        no_sandbox: bool = True,
    ):
        self.staging_dir = staging_dir
        self.user_data_dir = user_data_dir
        self.final_out_dir = final_out_dir
        self.timeout_seconds = timeout_seconds
        self.visible = not headless
        self.headless = headless
        self.no_sandbox = no_sandbox

        self._pending_urls: List[Dict[str, Any]] = []
        self._processed_urls: Set[str] = set()
        self._mcp_process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self.operator_config: Optional[OperatorInterventionConfig] = None

        os.makedirs(self.staging_dir, exist_ok=True)
        os.makedirs(self.user_data_dir, exist_ok=True)
        os.makedirs(self.final_out_dir, exist_ok=True)

    def _download_single_url(
        self,
        url: str,
        domain: str,
        referer: str = "",
    ) -> BrowserDownloadResult:
        import requests

        start_time = time.time()
        self._ensure_staging_empty()

        if not PLAYWRIGHT_AVAILABLE:
            return BrowserDownloadResult(
                url=url,
                ok=False,
                local_path=None,
                error_type="playwright_not_available",
                message="Playwright is not installed for browser fallback",
                download_time_seconds=time.time() - start_time,
            )

        def _do_download() -> BrowserDownloadResult:
            try:
                assert sync_playwright is not None
                with sync_playwright() as p:
                    launch_args: List[str] = []
                    if self.no_sandbox:
                        launch_args.extend(["--no-sandbox", "--disable-dev-shm-usage"])
                    browser = p.chromium.launch(headless=bool(self.headless), args=launch_args)
                    context = browser.new_context(accept_downloads=True)
                    page = context.new_page()
                    if referer:
                        page.set_extra_http_headers({"Referer": referer})
                    response = page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=self.timeout_seconds * 1000,
                    )
                    if response is None:
                        page.close()
                        context.close()
                        browser.close()
                        return BrowserDownloadResult(
                            url=url,
                            ok=False,
                            local_path=None,
                            error_type="navigation_error",
                            message="No response from browser navigation",
                            download_time_seconds=time.time() - start_time,
                        )
                    page.wait_for_timeout(1200)
                    cookies = context.cookies()
                    ua = page.evaluate("() => navigator.userAgent")
                    page.close()
                    context.close()
                    browser.close()

                session = requests.Session()
                for cookie in cookies:
                    session.cookies.set(
                        cookie["name"],
                        cookie["value"],
                        domain=str(cookie.get("domain", "")).lstrip("."),
                        path=str(cookie.get("path", "/")),
                    )
                headers = {
                    "User-Agent": str(ua),
                    "Accept": "application/pdf,*/*;q=0.9",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": referer or f"https://{domain}/",
                    "Connection": "keep-alive",
                }
                resp = session.get(url, headers=headers, timeout=self.timeout_seconds)
                waf_action = str(resp.headers.get("x-amzn-waf-action", "")).strip().lower()
                if waf_action == "challenge":
                    return BrowserDownloadResult(
                        url=url,
                        ok=False,
                        local_path=None,
                        error_type="waf_challenge_failed",
                        message="Browser-acquired session still challenged by WAF",
                        download_time_seconds=time.time() - start_time,
                    )
                if resp.status_code != 200:
                    return BrowserDownloadResult(
                        url=url,
                        ok=False,
                        local_path=None,
                        error_type="http_error",
                        message=f"HTTP {resp.status_code} when fetching PDF",
                        download_time_seconds=time.time() - start_time,
                    )
                content = resp.content
                if not content.startswith(b"%PDF-"):
                    pdf_start = content.find(b"%PDF-")
                    if pdf_start == -1 or pdf_start > 1024:
                        return BrowserDownloadResult(
                            url=url,
                            ok=False,
                            local_path=None,
                            error_type="not_pdf",
                            message="Server returned non-PDF content",
                            download_time_seconds=time.time() - start_time,
                        )
                    content = content[pdf_start:]

                filename = os.path.basename(urlparse(url).path) or "document.pdf"
                if not filename.lower().endswith(".pdf"):
                    filename += ".pdf"
                staging_path = os.path.join(self.staging_dir, filename)
                with open(staging_path, "wb") as fh:
                    fh.write(content)
                final_path = self._move_to_final(staging_path, domain)
                return BrowserDownloadResult(
                    url=url,
                    ok=True,
                    local_path=final_path,
                    error_type="",
                    message="",
                    download_time_seconds=time.time() - start_time,
                )
            except Exception as exc:
                return BrowserDownloadResult(
                    url=url,
                    ok=False,
                    local_path=None,
                    error_type="navigation_error",
                    message=str(exc),
                    download_time_seconds=time.time() - start_time,
                )

        try:
            return _run_in_clean_thread(_do_download)
        except Exception as exc:
            return BrowserDownloadResult(
                url=url,
                ok=False,
                local_path=None,
                error_type="thread_error",
                message=str(exc),
                download_time_seconds=time.time() - start_time,
            )


class CamoufoxDownloader(MCPBrowserDownloader):
    """Camoufox-based browser for stealthy WAF fallback downloads.

    Camoufox is a modified Firefox browser with built-in anti-detection:
    - Automatic fingerprint spoofing (screen, WebGL, fonts, etc.)
    - Humanized cursor movements
    - GeoIP-based locale matching for proxies
    - No automation detection flags

    This is the recommended browser fallback for WAF-blocked downloads.

    Usage:
        downloader = CamoufoxDownloader(
            staging_dir="artifacts/browser_staging",
            user_data_dir="artifacts/camoufox_profile",
            final_out_dir="artifacts/pdfs",
            headless=False,  # Use True or "virtual" for headless on Linux
        )

        downloader.add_url("https://example.com/blocked.pdf", domain="example.com")
        results = downloader.process_batch()
    """

    # Additional Camoufox-specific settings (not using dataclass fields to avoid inheritance issues)
    def __init__(
        self,
        staging_dir: str = "artifacts/browser_staging",
        user_data_dir: str = "artifacts/camoufox_profile",
        final_out_dir: str = "artifacts/pdfs",
        timeout_seconds: int = DEFAULT_BROWSER_TIMEOUT,
        visible: bool = True,
        headless: bool | Literal["virtual"] = False,
        humanize: bool | float = True,
        os_target: Optional[str] = None,
        proxy: Optional[str] = None,
        geoip: Optional[str | bool] = None,
        block_images: bool = False,
        disable_coop: bool = True,
        enable_cache: bool = True,
        locale: Optional[str] = None,
    ):
        # Set attributes before calling parent (dataclass fields)
        self.staging_dir = staging_dir
        self.user_data_dir = user_data_dir
        self.final_out_dir = final_out_dir
        self.timeout_seconds = timeout_seconds
        self.visible = visible

        # Camoufox-specific
        self.headless = headless
        self.humanize = humanize
        self.os_target = os_target
        self.proxy = proxy
        self.geoip = geoip
        self.block_images = block_images
        self.disable_coop = disable_coop
        self.enable_cache = enable_cache
        self.locale = locale

        # Internal state
        self._pending_urls: List[Dict[str, Any]] = []
        self._processed_urls: Set[str] = set()
        self._mcp_process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._browser: Any = None
        self._context: Any = None
        self.operator_config: Optional[OperatorInterventionConfig] = None

        # Initialize directories
        os.makedirs(self.staging_dir, exist_ok=True)
        os.makedirs(self.user_data_dir, exist_ok=True)
        os.makedirs(self.final_out_dir, exist_ok=True)

    def _download_single_url(
        self,
        url: str,
        domain: str,
        referer: str = "",
    ) -> BrowserDownloadResult:
        """Download a single URL using Camoufox browser with cookie extraction.

        This method uses a hybrid approach:
        1. Navigate to the URL with Camoufox to solve any WAF challenges (AWS WAF, Cloudflare, etc.)
        2. Extract the session cookies (especially aws-waf-token) from the browser
        3. Use requests/curl_cffi with those cookies to download the actual PDF

        This is more reliable than trying to capture browser downloads because:
        - Firefox's built-in PDF.js viewer intercepts PDFs before Playwright sees them
        - WAF challenges set session cookies that can be reused for subsequent requests

        Args:
            url: URL to download
            domain: Domain for output organization
            referer: Optional referer header

        Returns:
            BrowserDownloadResult with outcome
        """
        import requests

        start_time = time.time()

        if not CAMOUFOX_AVAILABLE:
            if PLAYWRIGHT_AVAILABLE:
                fallback = PlaywrightDownloader(
                    staging_dir=self.staging_dir,
                    user_data_dir=self.user_data_dir,
                    final_out_dir=self.final_out_dir,
                    timeout_seconds=self.timeout_seconds,
                    headless=bool(self.headless),
                )
                fallback.operator_config = self.operator_config
                return fallback._download_single_url(url, domain, referer)
            return BrowserDownloadResult(
                url=url,
                ok=False,
                local_path=None,
                error_type="camoufox_not_available",
                message="Camoufox not installed. Install with: pip install camoufox[geoip]",
                download_time_seconds=time.time() - start_time,
            )

        # Run the entire browser workflow in a clean thread to avoid asyncio conflicts
        def _do_download():
            # Ensure browser is running (will also run in clean thread context)
            if not self._start_browser_internal():
                if PLAYWRIGHT_AVAILABLE:
                    fallback = PlaywrightDownloader(
                        staging_dir=self.staging_dir,
                        user_data_dir=self.user_data_dir,
                        final_out_dir=self.final_out_dir,
                        timeout_seconds=self.timeout_seconds,
                        headless=bool(self.headless),
                    )
                    fallback.operator_config = self.operator_config
                    return fallback._download_single_url(url, domain, referer)
                return BrowserDownloadResult(
                    url=url,
                    ok=False,
                    local_path=None,
                    error_type="browser_start_error",
                    message="Failed to start Camoufox browser",
                    download_time_seconds=time.time() - start_time,
                )

            # Clear staging before download
            self._ensure_staging_empty()

            try:
                # Create a new page
                page = self._browser.new_page()

                # Navigate to the URL to trigger WAF challenge solving
                print(f"🦊 [camoufox] Navigating to {url} to solve WAF challenge...", flush=True)
                try:
                    if referer:
                        page.set_extra_http_headers({"Referer": referer})

                    response = page.goto(
                        url, wait_until="networkidle", timeout=self.timeout_seconds * 1000
                    )

                    # Wait for any JS challenges to complete
                    page.wait_for_timeout(3000)

                    # Check if we got through the WAF
                    waf_action = response.headers.get("x-amzn-waf-action", "") if response else ""
                    # Even if WAF challenge header is present, the browser may have solved it
                    # Check the page content to see if we're viewing a PDF
                    html = page.content()
                    in_pdf_viewer = "resource://pdf.js" in html or "pdfjs" in html.lower()

                    if in_pdf_viewer:
                        print(
                            "🦊 [camoufox] PDF viewer detected - WAF challenge solved!", flush=True
                        )
                    elif waf_action == "challenge":
                        # Still in challenge mode, wait longer
                        print(
                            "🦊 [camoufox] WAF challenge active, waiting for auto-solve...",
                            flush=True,
                        )
                        page.wait_for_timeout(7000)  # Extra wait for challenge

                    # Extract cookies from browser context
                    context = page.context
                    cookies = context.cookies()

                    # Get user agent from browser
                    ua = page.evaluate("() => navigator.userAgent")

                    page.close()

                    # Check if we have WAF token
                    waf_cookies = [c for c in cookies if "waf" in c.get("name", "").lower()]
                    if waf_cookies:
                        print(
                            f"🦊 [camoufox] Extracted {len(waf_cookies)} WAF cookie(s)", flush=True
                        )

                    # Now fetch the PDF using requests with the browser cookies
                    print("🦊 [camoufox] Fetching PDF with extracted cookies...", flush=True)

                    session = requests.Session()
                    for c in cookies:
                        session.cookies.set(
                            c["name"],
                            c["value"],
                            domain=c.get("domain", "").lstrip("."),
                            path=c.get("path", "/"),
                        )

                    headers = {
                        "User-Agent": ua,
                        "Accept": "application/pdf,*/*;q=0.9",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Referer": referer or f"https://{domain}/",
                        "Connection": "keep-alive",
                    }

                    resp = session.get(url, headers=headers, timeout=self.timeout_seconds)

                    new_waf_action = resp.headers.get("x-amzn-waf-action", "")

                    if new_waf_action == "challenge":
                        return BrowserDownloadResult(
                            url=url,
                            ok=False,
                            local_path=None,
                            error_type="waf_challenge_failed",
                            message="WAF challenge cookies not accepted for PDF download",
                            download_time_seconds=time.time() - start_time,
                        )

                    if resp.status_code != 200:
                        return BrowserDownloadResult(
                            url=url,
                            ok=False,
                            local_path=None,
                            error_type="http_error",
                            message=f"HTTP {resp.status_code} when fetching PDF",
                            download_time_seconds=time.time() - start_time,
                        )

                    # Validate PDF content
                    content = resp.content
                    if not content.startswith(b"%PDF-"):
                        # Check if it's HTML (might be another challenge page)
                        if (
                            content[:100].lower().startswith(b"<!doctype")
                            or b"<html" in content[:200].lower()
                        ):
                            return BrowserDownloadResult(
                                url=url,
                                ok=False,
                                local_path=None,
                                error_type="not_pdf",
                                message="Server returned HTML instead of PDF",
                                download_time_seconds=time.time() - start_time,
                            )
                        # Some PDFs have BOM or whitespace before %PDF-
                        pdf_start = content.find(b"%PDF-")
                        if pdf_start == -1 or pdf_start > 1024:
                            return BrowserDownloadResult(
                                url=url,
                                ok=False,
                                local_path=None,
                                error_type="invalid_pdf",
                                message=f"Response does not contain valid PDF (starts with: {content[:20]})",
                                download_time_seconds=time.time() - start_time,
                            )
                        # Trim leading bytes
                        content = content[pdf_start:]

                    # Generate filename
                    filename = os.path.basename(urlparse(url).path) or "document.pdf"
                    if not filename.lower().endswith(".pdf"):
                        filename += ".pdf"

                    # Save to staging
                    staging_path = os.path.join(self.staging_dir, filename)
                    with open(staging_path, "wb") as f:
                        f.write(content)

                    # Move to final location
                    final_path = self._move_to_final(staging_path, domain)

                    print(
                        f"✅ [camoufox] Successfully downloaded {len(content)} bytes to {final_path}",
                        flush=True,
                    )

                    return BrowserDownloadResult(
                        url=url,
                        ok=True,
                        local_path=final_path,
                        error_type="",
                        message="",
                        download_time_seconds=time.time() - start_time,
                    )

                except Exception as nav_exc:
                    try:
                        page.close()
                    except Exception:
                        pass
                    return BrowserDownloadResult(
                        url=url,
                        ok=False,
                        local_path=None,
                        error_type="navigation_error",
                        message=str(nav_exc),
                        download_time_seconds=time.time() - start_time,
                    )

            except Exception as exc:
                return BrowserDownloadResult(
                    url=url,
                    ok=False,
                    local_path=None,
                    error_type="browser_error",
                    message=str(exc),
                    download_time_seconds=time.time() - start_time,
                )

        try:
            # Run in clean thread to avoid asyncio event loop conflicts
            return _run_in_clean_thread(_do_download)
        except Exception as exc:
            return BrowserDownloadResult(
                url=url,
                ok=False,
                local_path=None,
                error_type="thread_error",
                message=str(exc),
                download_time_seconds=time.time() - start_time,
            )

    def _start_browser_internal(self) -> bool:
        """Internal browser start - called from within clean thread context."""
        if self._browser is not None:
            return True

        try:
            # Configure Camoufox options based on docs
            kwargs: Dict[str, Any] = {
                "headless": self.headless,
                "humanize": self.humanize,
                "block_images": self.block_images,
                "disable_coop": self.disable_coop,
                "i_know_what_im_doing": True,  # Suppress COOP warning - we need it for Cloudflare
                "enable_cache": self.enable_cache,
            }

            if self.os_target:
                kwargs["os"] = self.os_target

            if self.proxy:
                kwargs["proxy"] = {"server": self.proxy}

            if self.geoip:
                kwargs["geoip"] = self.geoip

            if self.locale:
                kwargs["locale"] = self.locale

            if self.user_data_dir:
                os.makedirs(self.user_data_dir, exist_ok=True)
                kwargs["persistent_context"] = True
                kwargs["user_data_dir"] = self.user_data_dir

            self._browser = Camoufox(**kwargs).__enter__()
            print("🦊 Camoufox browser started", flush=True)
            return True

        except Exception as exc:
            print(f"⚠️ Failed to start Camoufox browser: {exc}", flush=True)
            self._browser = None
            return False

    def close(self) -> None:
        """Clean up browser resources."""
        if self._browser is not None:
            try:
                self._browser.__exit__(None, None, None)
            except Exception:
                pass
            self._browser = None

        super().close()


def create_browser_downloader(
    backend: Literal["camoufox", "chrome_mcp", "playwright", "auto"] = "auto",
    staging_dir: str = "artifacts/browser_staging",
    user_data_dir: str = "artifacts/browser_profile",
    final_out_dir: str = "artifacts/pdfs",
    timeout_seconds: int = DEFAULT_BROWSER_TIMEOUT,
    headless: bool | Literal["virtual"] = False,
    operator_config: Optional[OperatorInterventionConfig | Dict[str, Any]] = None,
    **kwargs,
) -> MCPBrowserDownloader:
    """Factory function to create the appropriate browser downloader.

    Args:
        backend: Browser backend to use:
            - "camoufox": Use Camoufox (recommended, requires camoufox package)
            - "chrome_mcp": Use Chrome DevTools MCP (requires external MCP server)
            - "playwright": Use local Playwright Chromium browser
            - "auto": Use Camoufox if available, otherwise Playwright
        staging_dir: Directory for temporary downloads
        user_data_dir: Directory for browser profile persistence
        final_out_dir: Final output directory for PDFs
        timeout_seconds: Download timeout
        headless: Run browser headless (True, False, or "virtual" for Xvfb on Linux)
        **kwargs: Additional backend-specific options

    Returns:
        Configured MCPBrowserDownloader instance
    """
    if backend == "auto":
        backend = "camoufox" if CAMOUFOX_AVAILABLE else "playwright"

    if backend == "camoufox":
        downloader: MCPBrowserDownloader = CamoufoxDownloader(
            staging_dir=staging_dir,
            user_data_dir=user_data_dir,
            final_out_dir=final_out_dir,
            timeout_seconds=timeout_seconds,
            headless=headless,
            humanize=kwargs.get("humanize", True),
            os_target=kwargs.get("os_target"),
            proxy=kwargs.get("proxy"),
            geoip=kwargs.get("geoip"),
            block_images=kwargs.get("block_images", False),
            disable_coop=kwargs.get("disable_coop", True),
            enable_cache=kwargs.get("enable_cache", True),
            locale=kwargs.get("locale"),
        )
    elif backend == "playwright":
        downloader = PlaywrightDownloader(
            staging_dir=staging_dir,
            user_data_dir=user_data_dir,
            final_out_dir=final_out_dir,
            timeout_seconds=timeout_seconds,
            headless=bool(headless),
            no_sandbox=bool(kwargs.get("no_sandbox", True)),
        )
    else:
        downloader = ChromeMCPDownloader(
            staging_dir=staging_dir,
            user_data_dir=user_data_dir,
            final_out_dir=final_out_dir,
            timeout_seconds=timeout_seconds,
            visible=not headless,
            chrome_path=kwargs.get("chrome_path"),
            mcp_server_command=kwargs.get("mcp_server_command"),
        )
    downloader.operator_config = _coerce_operator_config(operator_config)
    return downloader


def create_waf_retry_file(
    run_dir: str,
    waf_blocked_urls: List[Dict[str, Any]],
) -> str:
    """Create a retry file for WAF-blocked URLs.

    Args:
        run_dir: Directory for the run artifacts
        waf_blocked_urls: List of dicts with url, domain, metadata, referer

    Returns:
        Path to the created retry file
    """
    retry_path = os.path.join(run_dir, "retry_targets_waf_browser.txt")

    with open(retry_path, "w", encoding="utf-8") as f:
        for item in waf_blocked_urls:
            # Write URL and metadata as JSON line
            f.write(json.dumps(item, sort_keys=True) + "\n")

    return retry_path


def load_waf_retry_file(retry_path: str) -> List[Dict[str, Any]]:
    """Load WAF retry targets from file.

    Args:
        retry_path: Path to retry file

    Returns:
        List of dicts with url, domain, metadata, referer
    """
    targets: List[Dict[str, Any]] = []

    if not os.path.exists(retry_path):
        return targets

    with open(retry_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict) and item.get("url"):
                    targets.append(item)
            except json.JSONDecodeError:
                # If line is just a URL, wrap it
                if line.startswith("http"):
                    targets.append({"url": line})

    return targets
