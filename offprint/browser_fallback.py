from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class BrowserDownloadResult:
    ok: bool
    status_code: int = 0
    content_type: str = ""
    waf_action: str = ""
    error: str = ""
    body: bytes = b""


class HeadlessBrowserFallbackSession:
    """Reusable browser session for PDF fallback downloads."""

    def __init__(self, *, timeout_seconds: int = 45, headless: bool = False):
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.headless = bool(headless)
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._last_referer = ""

    def _ensure_started(self) -> None:
        if self._browser is not None:
            return
        from playwright.sync_api import sync_playwright

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=self.headless)
        self._context = self._browser.new_context(user_agent=DEFAULT_USER_AGENT)
        self._page = self._context.new_page()

    def close(self) -> None:
        try:
            if self._page is not None:
                self._page.close()
        finally:
            self._page = None
        try:
            if self._context is not None:
                self._context.close()
        finally:
            self._context = None
        try:
            if self._browser is not None:
                self._browser.close()
        finally:
            self._browser = None
        try:
            if self._playwright is not None:
                self._playwright.stop()
        finally:
            self._playwright = None

    def __enter__(self) -> "HeadlessBrowserFallbackSession":
        self._ensure_started()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def warmup(self, referer: str) -> bool:
        if not referer:
            return False
        if referer == self._last_referer:
            return True
        self._ensure_started()
        try:
            self._page.goto(
                referer,
                wait_until="domcontentloaded",
                timeout=self.timeout_seconds * 1000,
            )
            self._last_referer = referer
            return True
        except Exception:
            return False

    def _to_result(
        self,
        *,
        status_code: int,
        headers: Optional[dict],
        body: bytes,
        error: str = "",
    ) -> BrowserDownloadResult:
        lowered_headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        content_type = lowered_headers.get("content-type", "")
        waf_action = lowered_headers.get("x-amzn-waf-action", "")

        if not body:
            return BrowserDownloadResult(
                ok=False,
                status_code=status_code,
                content_type=content_type,
                waf_action=waf_action,
                error=error or "empty_browser_response",
            )

        if not body.startswith(b"%PDF-"):
            return BrowserDownloadResult(
                ok=False,
                status_code=status_code,
                content_type=content_type,
                waf_action=waf_action,
                error=error or "invalid_pdf_from_browser",
                body=body,
            )

        return BrowserDownloadResult(
            ok=True,
            status_code=status_code,
            content_type=content_type,
            waf_action=waf_action,
            body=body,
        )

    @staticmethod
    def _is_known_waf_block(result: BrowserDownloadResult) -> bool:
        lowered_waf = (result.waf_action or "").lower()
        lowered_ct = (result.content_type or "").lower()
        if lowered_waf == "challenge":
            return True
        return (
            result.status_code in {202, 401, 403}
            and "text/html" in lowered_ct
            and not result.body.startswith(b"%PDF-")
        )

    def _fetch_with_request_context(self, pdf_url: str, referer: str = "") -> BrowserDownloadResult:
        self._ensure_started()
        headers = {"Accept": "application/pdf,*/*;q=0.9"}
        if referer:
            headers["Referer"] = referer
        try:
            response = self._context.request.get(
                pdf_url,
                headers=headers,
                timeout=self.timeout_seconds * 1000,
            )
            return self._to_result(
                status_code=int(response.status),
                headers=dict(response.headers),
                body=response.body(),
            )
        except Exception as exc:
            return BrowserDownloadResult(ok=False, error=f"playwright_request_error: {exc}")

    def _response_matches_pdf(self, response, expected_url: str) -> bool:
        try:
            ctype = (response.headers.get("content-type") or "").lower()
            if "application/pdf" in ctype:
                return True
            expected = (expected_url or "").lower()
            actual = (response.url or "").lower()
            if expected and actual == expected:
                return True
            expected_path = unquote(urlparse(expected).path or "").lower()
            actual_path = unquote(urlparse(actual).path or "").lower()
            if expected_path and actual_path and expected_path == actual_path:
                return True
            if expected_path.endswith(".pdf") and actual_path.endswith(
                expected_path.split("/")[-1]
            ):
                return True
        except Exception:
            return False
        return False

    def _fetch_by_clicking_referer_link(
        self,
        pdf_url: str,
        referer: str = "",
    ) -> BrowserDownloadResult:
        if not referer:
            return BrowserDownloadResult(ok=False, error="missing_referer")
        self._ensure_started()
        try:
            self._page.goto(
                referer,
                wait_until="networkidle",
                timeout=self.timeout_seconds * 1000,
            )
        except Exception as exc:
            return BrowserDownloadResult(ok=False, error=f"referer_navigation_error: {exc}")

        expected_path = (urlparse(pdf_url).path or "").lower()
        expected_leaf = expected_path.rsplit("/", 1)[-1] if expected_path else ""
        selectors = []
        if pdf_url:
            selectors.append(f'a[href="{pdf_url}"]')
        if expected_leaf and expected_leaf.endswith(".pdf"):
            selectors.append(f'a[href*="{expected_leaf}"]')
        if "viewcontent.cgi" in (pdf_url or "").lower():
            selectors.append('a[href*="viewcontent.cgi"]')
        selectors.extend(
            [
                'a:has-text("Download PDF")',
                'a:has-text("PDF")',
                'a:has-text("Download")',
            ]
        )
        click_timeout_ms = min(4000, max(1200, int(self.timeout_seconds * 1000 * 0.08)))

        tried = set()
        for selector in selectors:
            if selector in tried:
                continue
            tried.add(selector)
            locator = self._page.locator(selector).first
            try:
                if locator.count() == 0:
                    continue
            except Exception:
                continue

            # First prefer browser download events.
            try:
                with self._page.expect_response(
                    lambda resp: self._response_matches_pdf(resp, pdf_url),
                    timeout=click_timeout_ms,
                ) as resp_info:
                    locator.click()
                response = resp_info.value
                return self._to_result(
                    status_code=int(response.status),
                    headers=dict(response.headers),
                    body=response.body(),
                )
            except Exception:
                pass

            # If no response event, try browser download event with short timeout.
            try:
                with self._page.expect_download(timeout=click_timeout_ms) as dl_info:
                    locator.click()
                download = dl_info.value
                local_path = download.path()
                if local_path:
                    body = Path(local_path).read_bytes()
                    return self._to_result(
                        status_code=200,
                        headers={"content-type": "application/pdf"},
                        body=body,
                    )
            except Exception:
                continue

        return BrowserDownloadResult(ok=False, error="pdf_link_click_not_found")

    def _fetch_with_page_navigation(self, pdf_url: str) -> BrowserDownloadResult:
        self._ensure_started()
        try:
            response = self._page.goto(
                pdf_url,
                wait_until="networkidle",
                timeout=self.timeout_seconds * 1000,
            )
            if response is None:
                return BrowserDownloadResult(ok=False, error="empty_browser_response")
            return self._to_result(
                status_code=int(response.status),
                headers=dict(response.headers),
                body=response.body(),
            )
        except Exception as exc:
            return BrowserDownloadResult(ok=False, error=f"playwright_navigation_error: {exc}")

    def fetch_pdf(self, pdf_url: str, referer: str = "") -> BrowserDownloadResult:
        # Warmup first to establish cookies/challenge state on the source page.
        self.warmup(referer)

        attempts = []

        if referer:
            clicked = self._fetch_by_clicking_referer_link(pdf_url, referer=referer)
            attempts.append(clicked)
            if clicked.ok:
                return clicked

        request_result = self._fetch_with_request_context(pdf_url, referer=referer)
        attempts.append(request_result)
        if request_result.ok:
            return request_result

        nav_result = self._fetch_with_page_navigation(pdf_url)
        attempts.append(nav_result)
        if nav_result.ok:
            return nav_result

        # Return the most informative failure (status/headers/body), else last error.
        for result in reversed(attempts):
            if result.status_code or result.content_type or result.body or result.waf_action:
                return result
        return attempts[-1] if attempts else BrowserDownloadResult(ok=False, error="no_attempts")

    def _extract_links_from_current_page(self) -> tuple[list[str], list[str]]:
        self._ensure_started()
        try:
            payload = self._page.evaluate(
                """() => {
                  const toAbs = (value) => {
                    try { return new URL(value, window.location.href).href; } catch { return ""; }
                  };
                  const pdfHints = [".pdf", "/pdf/", "viewcontent.cgi", "download", "bitstream"];
                  const navHints = ["issue", "archive", "volume", "vol-", "past-issues", "all_issues"];
                  const pdfs = [];
                  const nav = [];
                  const pushUnique = (arr, value) => {
                    if (!value) return;
                    if (!arr.includes(value)) arr.push(value);
                  };

                  document.querySelectorAll("a[href], iframe[src], embed[src], object[data]").forEach((el) => {
                    const raw = el.getAttribute("href") || el.getAttribute("src") || el.getAttribute("data") || "";
                    const href = toAbs(raw);
                    if (!href) return;
                    const lower = href.toLowerCase();
                    if (pdfHints.some((hint) => lower.includes(hint))) {
                      pushUnique(pdfs, href);
                    }
                    if (el.tagName.toLowerCase() === "a" && navHints.some((hint) => lower.includes(hint))) {
                      pushUnique(nav, href);
                    }
                  });

                  return {pdfs, nav};
                }"""
            )
        except Exception:
            return [], []

        if not isinstance(payload, dict):
            return [], []

        raw_pdfs = payload.get("pdfs") if isinstance(payload.get("pdfs"), list) else []
        raw_nav = payload.get("nav") if isinstance(payload.get("nav"), list) else []
        pdfs = [str(v).strip() for v in raw_pdfs if str(v).strip()]
        nav = [str(v).strip() for v in raw_nav if str(v).strip()]
        return pdfs, nav

    def discover_pdf_links(self, seed_url: str, max_links: int = 20) -> list[str]:
        self._ensure_started()
        limit = max(1, int(max_links or 1))
        discovered: list[str] = []
        seen: set[str] = set()

        def add_all(urls: list[str]) -> None:
            for url in urls:
                if not url or url in seen:
                    continue
                seen.add(url)
                discovered.append(url)
                if len(discovered) >= limit:
                    return

        try:
            self._page.goto(
                seed_url,
                wait_until="networkidle",
                timeout=self.timeout_seconds * 1000,
            )
            self._last_referer = seed_url
        except Exception:
            return []

        pdfs, nav_links = self._extract_links_from_current_page()
        add_all(pdfs)
        if len(discovered) >= limit:
            return discovered[:limit]

        for nav_link in nav_links[:8]:
            try:
                self._page.goto(
                    nav_link,
                    wait_until="domcontentloaded",
                    timeout=self.timeout_seconds * 1000,
                )
            except Exception:
                continue
            pdfs, _ = self._extract_links_from_current_page()
            add_all(pdfs)
            if len(discovered) >= limit:
                break

        return discovered[:limit]


def download_pdf_via_playwright(
    pdf_url: str,
    referer: str = "",
    timeout_seconds: int = 45,
    headless: bool = False,
) -> BrowserDownloadResult:
    try:
        with HeadlessBrowserFallbackSession(
            timeout_seconds=timeout_seconds,
            headless=headless,
        ) as session:
            return session.fetch_pdf(pdf_url, referer=referer)
    except Exception as exc:
        return BrowserDownloadResult(ok=False, error=f"playwright_error: {exc}")
