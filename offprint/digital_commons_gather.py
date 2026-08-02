from __future__ import annotations

import hashlib
import json
import os
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Callable, Deque, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_retry_after(value: object, *, now: Optional[datetime] = None) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return max(float(text), 0.0)
    except ValueError:
        pass
    try:
        target = parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return max((target - current).total_seconds(), 0.0)


def canonical_dc_url(value: str) -> str:
    """Normalize DC PDF URLs without inventing download parameters."""
    parsed = urlparse(str(value or "").strip())
    query = [
        (key, val)
        for key, val in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in {"type", "download"}
    ]
    query.sort()
    return urlunparse(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, "", urlencode(query), "")
    )


def gather_id_for_url(value: str) -> str:
    return hashlib.sha1(canonical_dc_url(value).encode("utf-8")).hexdigest()[:20]


@dataclass(frozen=True)
class GatherItem:
    gather_id: str
    domain: str
    page_url: str
    pdf_url: str
    title: str = ""
    dc_context: str = ""
    dc_article_id: str = ""
    dc_source: str = ""

    @classmethod
    def from_dict(cls, payload: Dict[str, object]) -> "GatherItem":
        pdf_url = str(payload.get("pdf_url") or "").strip()
        domain = str(payload.get("domain") or urlparse(pdf_url).netloc).strip().lower()
        return cls(
            gather_id=str(payload.get("gather_id") or gather_id_for_url(pdf_url)),
            domain=domain,
            page_url=str(payload.get("page_url") or "").strip(),
            pdf_url=pdf_url,
            title=str(payload.get("title") or "").strip(),
            dc_context=str(payload.get("dc_context") or "").strip(),
            dc_article_id=str(payload.get("dc_article_id") or "").strip(),
            dc_source=str(payload.get("dc_source") or "").strip(),
        )


@dataclass
class GatherAttempt:
    gather_id: str
    domain: str
    page_url: str
    requested_pdf_url: str
    clicked_pdf_url: str
    status: str
    http_status: int
    error: str
    local_path: str
    pdf_sha256: str
    pdf_size_bytes: int
    elapsed_seconds: float
    dispatch_delay_seconds: float
    attempted_at: str
    retry_after_seconds: float = 0.0
    landing_metadata: Dict[str, object] = field(default_factory=dict)


class AdaptivePacer:
    """AIMD-style global pacing with conservative success-based decreases."""

    def __init__(
        self,
        *,
        start_delay_seconds: float = 60.0,
        min_delay_seconds: float = 10.0,
        max_delay_seconds: float = 3600.0,
        successes_before_decrease: int = 50,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.delay_seconds = max(float(start_delay_seconds), 0.0)
        self.min_delay_seconds = max(float(min_delay_seconds), 0.0)
        self.max_delay_seconds = max(float(max_delay_seconds), self.min_delay_seconds)
        self.successes_before_decrease = max(int(successes_before_decrease), 1)
        self._success_streak = 0
        self._last_dispatch: Optional[float] = None
        self._clock = clock
        self._sleeper = sleeper

    def wait(self) -> float:
        now = self._clock()
        slept = 0.0
        if self._last_dispatch is not None:
            remaining = self.delay_seconds - (now - self._last_dispatch)
            if remaining > 0:
                self._sleeper(remaining)
                slept = remaining
        self._last_dispatch = self._clock()
        return slept

    def record_success(self) -> None:
        self._success_streak += 1
        if self._success_streak < self.successes_before_decrease:
            return
        self.delay_seconds = max(self.min_delay_seconds, self.delay_seconds / 2.0)
        self._success_streak = 0

    def record_pressure(self, retry_after_seconds: Optional[float] = None) -> None:
        floor = max(float(retry_after_seconds or 0.0), self.delay_seconds * 2.0)
        self.delay_seconds = min(self.max_delay_seconds, max(self.min_delay_seconds, floor))
        self._success_streak = 0

    def record_failure(self) -> None:
        self._success_streak = 0


def fair_round_robin(items: Iterable[GatherItem]) -> Iterator[GatherItem]:
    queues: Dict[str, Deque[GatherItem]] = defaultdict(deque)
    order: List[str] = []
    for item in items:
        if item.domain not in queues:
            order.append(item.domain)
        queues[item.domain].append(item)
    active: Deque[str] = deque(order)
    while active:
        domain = active.popleft()
        queue = queues[domain]
        yield queue.popleft()
        if queue:
            active.append(domain)


class PersistentDigitalCommonsBrowser:
    """One persistent Chromium context that clicks publisher Download links."""

    COOKIE_SELECTORS = (
        "#onetrust-accept-btn-handler",
        'button:has-text("Accept All Cookies")',
        'button:has-text("Accept All")',
    )
    DOWNLOAD_SELECTORS = (
        "a#pdf",
        'a:has-text("Download PDF")',
        'a:has-text("Download")',
        'a[href*="viewcontent.cgi"]',
        'a[href$=".pdf"]',
    )

    def __init__(
        self,
        *,
        headless: bool = True,
        timeout_seconds: int = 75,
        contact_email: str = "",
        project_url: str = "",
        max_download_mib_per_second: float = 0.0,
    ):
        self.headless = bool(headless)
        self.timeout_seconds = max(int(timeout_seconds), 10)
        self.contact_email = str(contact_email).strip()
        self.project_url = str(project_url).strip()
        self.max_download_mib_per_second = max(float(max_download_mib_per_second), 0.0)
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._cdp_session = None

    def start(self) -> None:
        if self._browser is not None:
            return
        from playwright.sync_api import sync_playwright

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        extra_http_headers = {}
        if self.contact_email:
            extra_http_headers["From"] = self.contact_email
        if self.project_url:
            extra_http_headers["X-Research-Project"] = self.project_url
        self._context = self._browser.new_context(
            accept_downloads=True,
            extra_http_headers=extra_http_headers,
        )
        self._page = self._context.new_page()
        if self.max_download_mib_per_second:
            self._cdp_session = self._context.new_cdp_session(self._page)
            self._cdp_session.send("Network.enable")
            self._cdp_session.send(
                "Network.emulateNetworkConditions",
                {
                    "offline": False,
                    "latency": 0,
                    "downloadThroughput": self.max_download_mib_per_second * 1024 * 1024,
                    "uploadThroughput": -1,
                },
            )

    def close(self) -> None:
        if self._cdp_session is not None:
            try:
                self._cdp_session.detach()
            except Exception:
                pass
            self._cdp_session = None
        for obj_name in ("_page", "_context", "_browser"):
            obj = getattr(self, obj_name)
            if obj is not None:
                try:
                    obj.close()
                except Exception:
                    pass
                setattr(self, obj_name, None)
        if self._playwright is not None:
            try:
                self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    def __enter__(self) -> "PersistentDigitalCommonsBrowser":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _dismiss_cookie_banner(self) -> None:
        for selector in self.COOKIE_SELECTORS:
            locator = self._page.locator(selector).first
            try:
                if locator.count() and locator.is_visible():
                    locator.click(timeout=5000)
                    return
            except Exception:
                continue

    def _find_download_link(self, requested_pdf_url: str):
        requested = canonical_dc_url(requested_pdf_url)
        for selector in self.DOWNLOAD_SELECTORS:
            locators = self._page.locator(selector)
            try:
                count = min(locators.count(), 20)
            except Exception:
                continue
            for index in range(count):
                locator = locators.nth(index)
                try:
                    href = str(locator.get_attribute("href") or "").strip()
                except Exception:
                    continue
                if not href:
                    continue
                if canonical_dc_url(href) == requested:
                    return locator
        # Never fall back to an arbitrary Download link. Some queue records use
        # an issue landing page shared by several articles; accepting the first
        # link would silently save the wrong article under a valid gather ID.
        return None

    def _extract_landing_metadata(self, item: GatherItem) -> Dict[str, object]:
        if item.dc_source == "all_issues":
            return {}
        try:
            from bs4 import BeautifulSoup

            from offprint.digital_commons_enumerator import (
                _extract_article_page_metadata,
            )

            soup = BeautifulSoup(self._page.content(), "lxml")
            title = self._page.title()
            metadata = _extract_article_page_metadata(soup, item.page_url, title)
            return {str(key): value for key, value in metadata.items()}
        except Exception:
            # Metadata enrichment must never turn a valid public PDF into a
            # failed acquisition. Missing metadata stays explicit in the ledger.
            return {}

    @staticmethod
    def _hash_file(path: Path) -> Tuple[str, int]:
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
        return digest.hexdigest(), size

    def download(self, item: GatherItem, destination: Path) -> GatherAttempt:
        self.start()
        started = time.monotonic()
        clicked_url = ""
        landing_metadata: Dict[str, object] = {}
        captured: List[Tuple[int, str, str, float]] = []
        partial = destination.with_suffix(destination.suffix + ".part")
        destination.parent.mkdir(parents=True, exist_ok=True)

        def on_response(response) -> None:
            if (
                "viewcontent.cgi" not in str(response.url).lower()
                and ".pdf" not in str(response.url).lower()
            ):
                return
            headers = {str(k).lower(): str(v) for k, v in dict(response.headers).items()}
            captured.append(
                (
                    int(response.status),
                    str(response.url),
                    headers.get("x-amzn-waf-action", ""),
                    parse_retry_after(headers.get("retry-after", "")),
                )
            )

        try:
            response = self._page.goto(
                item.page_url,
                wait_until="domcontentloaded",
                timeout=self.timeout_seconds * 1000,
            )
            landing_status = int(response.status) if response is not None else 0
            if landing_status >= 400:
                retry_after = parse_retry_after(
                    response.headers.get("retry-after", "") if response is not None else ""
                )
                return self._failure(
                    item,
                    started,
                    landing_status,
                    "landing_http_error",
                    retry_after_seconds=retry_after,
                )
            self._dismiss_cookie_banner()
            landing_metadata = self._extract_landing_metadata(item)
            link = self._find_download_link(item.pdf_url)
            if link is None:
                return self._failure(
                    item,
                    started,
                    0,
                    "download_link_not_found",
                    landing_metadata=landing_metadata,
                )
            clicked_url = str(link.get_attribute("href") or "")
            # DC commonly uses target=_blank. Removing it makes Playwright's
            # download event deterministic and preserves the browser context.
            link.evaluate("element => element.removeAttribute('target')")
            self._page.on("response", on_response)
            with self._page.expect_download(timeout=self.timeout_seconds * 1000) as info:
                # The high-level click also waits for navigation and can time out
                # even after Chromium has started a valid attachment download.
                link.evaluate("element => element.click()")
            download = info.value
            failure = download.failure()
            if failure:
                status = captured[-1][0] if captured else 0
                retry_after = captured[-1][3] if captured else 0.0
                return self._failure(
                    item,
                    started,
                    status,
                    str(failure),
                    clicked_url,
                    retry_after_seconds=retry_after,
                    landing_metadata=landing_metadata,
                )
            download.save_as(str(partial))
            with partial.open("rb") as handle:
                if not handle.read(8).startswith(b"%PDF-"):
                    return self._failure(
                        item,
                        started,
                        200,
                        "invalid_pdf",
                        clicked_url,
                        landing_metadata=landing_metadata,
                    )
            pdf_sha256, size = self._hash_file(partial)
            os.replace(str(partial), str(destination))
            return GatherAttempt(
                gather_id=item.gather_id,
                domain=item.domain,
                page_url=item.page_url,
                requested_pdf_url=item.pdf_url,
                clicked_pdf_url=clicked_url,
                status="downloaded",
                http_status=200,
                error="",
                local_path=str(destination),
                pdf_sha256=pdf_sha256,
                pdf_size_bytes=size,
                elapsed_seconds=round(time.monotonic() - started, 3),
                dispatch_delay_seconds=0.0,
                attempted_at=utc_now_iso(),
                landing_metadata=landing_metadata,
            )
        except Exception as exc:
            status = captured[-1][0] if captured else 0
            waf_action = captured[-1][2] if captured else ""
            retry_after = captured[-1][3] if captured else 0.0
            detail = f"{type(exc).__name__}: {exc}"
            if waf_action:
                detail = f"{detail}; waf_action={waf_action}"
            return self._failure(
                item,
                started,
                status,
                detail,
                clicked_url,
                retry_after_seconds=retry_after,
                landing_metadata=landing_metadata,
            )
        finally:
            try:
                self._page.remove_listener("response", on_response)
            except Exception:
                pass
            if partial.exists():
                try:
                    partial.unlink()
                except OSError:
                    pass

    @staticmethod
    def _failure(
        item: GatherItem,
        started: float,
        status: int,
        error: str,
        clicked_url: str = "",
        retry_after_seconds: float = 0.0,
        landing_metadata: Optional[Dict[str, object]] = None,
    ) -> GatherAttempt:
        return GatherAttempt(
            gather_id=item.gather_id,
            domain=item.domain,
            page_url=item.page_url,
            requested_pdf_url=item.pdf_url,
            clicked_pdf_url=clicked_url,
            status="deferred",
            http_status=int(status or 0),
            error=error,
            local_path="",
            pdf_sha256="",
            pdf_size_bytes=0,
            elapsed_seconds=round(time.monotonic() - started, 3),
            dispatch_delay_seconds=0.0,
            attempted_at=utc_now_iso(),
            retry_after_seconds=round(max(float(retry_after_seconds), 0.0), 3),
            landing_metadata=landing_metadata or {},
        )


def load_items_jsonl(path: Path) -> List[GatherItem]:
    items: List[GatherItem] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            item = GatherItem.from_dict(json.loads(line))
            if item.domain and item.page_url and item.pdf_url:
                items.append(item)
    return items


def load_success_ids(path: Path) -> set:
    successes = set()
    if not path.exists():
        return successes
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                payload = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                continue
            if payload.get("status") == "downloaded" and payload.get("gather_id"):
                successes.add(str(payload["gather_id"]))
    return successes


def append_attempt(path: Path, attempt: GatherAttempt) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(attempt), ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
