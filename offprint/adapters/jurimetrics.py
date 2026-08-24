from __future__ import annotations

import base64
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from ..playwright_session import PlaywrightSession


class JurimetricsAdapter(Adapter):
    """Playwright-backed adapter for ABA Jurimetrics issue archives.

    The American Bar Association pages are Cloudflare-protected for normal
    requests. A headed Playwright session can fetch both the archive page and
    issue pages reliably. Current issue pages expose an issue-compilation PDF
    through a `data-path` attribute on the download button.
    """

    ISSUE_PATH_RE = re.compile(
        r'data-path="(?P<path>/content/dam/aba/publications/Jurimetrics/[^"]+\.pdf)"',
        re.IGNORECASE,
    )
    ISSUE_URL_RE = re.compile(
        r"/groups/science_technology/(?:resources|publications)/jurimetrics/"
        r"(?:\d{4}/)?\d{4}-(?:fall|winter|spring|summer)/?$",
        re.IGNORECASE,
    )

    def __init__(self, session=None):
        super().__init__(session=session)
        self._issue_page_by_pdf: dict[str, str] = {}
        self._wayback_replay_by_pdf: dict[str, str] = {}

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        del max_depth
        # A single CDX query provides a stable, deduplicated inventory of ABA's
        # public Jurimetrics PDFs even while Cloudflare challenges the live DAM.
        try:
            archived_results = list(self._discover_wayback_pdfs())
        except Exception:
            archived_results = []
        if archived_results:
            yield from archived_results
            return

        # ABA's challenge page is cleared by headed navigation, but a second
        # navigation in the same page routinely times out. Discover the bounded
        # issue list once, then use a fresh browser session for each issue.
        archive_html = ""
        try:
            archive_payload, _ = self._fetch_latest_wayback(seed_url, mimetype="text/html")
            archive_html = archive_payload.decode("utf-8", errors="replace")
        except Exception:
            pass

        archive_soup = BeautifulSoup(archive_html, "lxml")
        issue_urls = self._extract_issue_urls(archive_soup, seed_url)
        archive_from_wayback = bool(issue_urls)
        if not issue_urls:
            with PlaywrightSession(
                headless=False, min_delay=0.2, max_delay=0.5, max_retries=1
            ) as pw:
                archive = pw.get(seed_url, timeout=45)
                if archive is None or archive.status_code >= 400:
                    return
                archive_html = archive.text
            archive_soup = BeautifulSoup(archive_html, "lxml")
            issue_urls = self._extract_issue_urls(archive_soup, seed_url)

        for issue_url in issue_urls:
            issue_html = ""
            if archive_from_wayback:
                try:
                    issue_payload, _ = self._fetch_latest_wayback(
                        issue_url, mimetype="text/html"
                    )
                    issue_html = issue_payload.decode("utf-8", errors="replace")
                except Exception:
                    pass
            pdf_url = self._extract_issue_pdf_url(issue_html)
            if not pdf_url:
                try:
                    with PlaywrightSession(
                        headless=False, min_delay=0.2, max_delay=0.5, max_retries=1
                    ) as pw:
                        issue_response = pw.get(issue_url, timeout=45)
                        if issue_response is not None and issue_response.status_code < 400:
                            issue_html = issue_response.text
                            pdf_url = self._extract_issue_pdf_url(issue_html)
                except Exception:
                    continue
            if not pdf_url:
                continue

            issue_soup = BeautifulSoup(issue_html, "lxml")
            metadata = self._extract_issue_metadata(issue_soup, issue_url)
            self._issue_page_by_pdf[pdf_url] = issue_url

            yield DiscoveryResult(
                page_url=issue_url,
                pdf_url=pdf_url,
                metadata=metadata,
                source_adapter="jurimetrics",
                extraction_path="archive_issue_download",
            )

    def _discover_wayback_pdfs(self) -> Iterable[DiscoveryResult]:
        cdx_response = self._request_with_retries(
            "https://web.archive.org/cdx/search/cdx",
            params={
                "url": "americanbar.org/content/dam/aba/publications/Jurimetrics/*",
                "output": "json",
                "filter": ["statuscode:200", "mimetype:application/pdf"],
                "fl": "timestamp,original,digest",
                "collapse": "digest",
                "limit": "10000",
            },
            timeout=60,
        )
        cdx_response.raise_for_status()
        rows = cdx_response.json()
        if len(rows) < 2:
            return
        for timestamp, original, _digest in sorted(rows[1:], reverse=True):
            replay_url = f"https://web.archive.org/web/{timestamp}id_/{original}"
            self._wayback_replay_by_pdf[original] = replay_url
            path = urlparse(original).path
            filename = Path(path).stem
            title = re.sub(r"[_-]+", " ", filename).strip().title()
            metadata = {
                "source_url": original,
                "url": original,
                "title": title or "Jurimetrics article",
                "platform": "aba_jurimetrics_wayback",
                "document_type": "article_or_issue_pdf",
            }
            year_match = re.search(r"(?:19|20)\d{2}", path)
            if year_match:
                metadata["year"] = year_match.group(0)
            yield DiscoveryResult(
                page_url=original,
                pdf_url=original,
                metadata=metadata,
                source_adapter="jurimetrics",
                extraction_path="wayback_cdx_pdf_inventory",
            )

    @classmethod
    def _extract_issue_urls(cls, soup: BeautifulSoup, seed_url: str) -> list[str]:
        issue_urls: list[str] = []
        for anchor in soup.select('.aba-article-content a[href], a[href*="/jurimetrics/"]'):
            issue_url = urljoin(seed_url, (anchor.get("href") or "").strip())
            if not cls.ISSUE_URL_RE.search(urlparse(issue_url).path):
                continue
            if issue_url not in issue_urls:
                issue_urls.append(issue_url)
        return issue_urls

    @classmethod
    def _extract_issue_pdf_url(cls, html: str) -> Optional[str]:
        match = cls.ISSUE_PATH_RE.search(html or "")
        if not match:
            return None
        return urljoin("https://www.americanbar.org", match.group("path"))

    @staticmethod
    def _extract_issue_metadata(soup: BeautifulSoup, issue_url: str) -> dict:
        metadata = {
            "source_url": issue_url,
            "url": issue_url,
            "platform": "aba_jurimetrics",
            "document_type": "issue_compilation",
        }

        title = soup.select_one("h1.group-microsite-basecontent__header__page-title") or soup.find("title")
        if title:
            metadata["title"] = title.get_text(" ", strip=True)

        volume = soup.select_one(".group-microsite-basecontent__brand-banner__volume")
        if volume:
            metadata["issue"] = volume.get_text(" ", strip=True)

        combined = " ".join(
            [str(metadata.get("title") or ""), str(metadata.get("issue") or ""), issue_url]
        )
        volume_match = re.search(r"\bVolume\s+(\d+)\b", combined, re.IGNORECASE)
        issue_match = re.search(r"\bIssue\s+(\d+)\b", combined, re.IGNORECASE)
        year_match = re.search(r"\b(20\d{2}|19\d{2})\b", combined)
        if volume_match:
            metadata["volume"] = volume_match.group(1)
        if issue_match:
            metadata["issue"] = issue_match.group(1)
        if year_match:
            metadata["year"] = year_match.group(1)

        return metadata

    def download_pdf(self, pdf_url: str, out_dir: str, **kwargs) -> Optional[str]:
        """Download through in-page fetch after ABA's browser challenge clears."""
        del kwargs
        issue_url = self._issue_page_by_pdf.get(pdf_url)
        replay_url = self._wayback_replay_by_pdf.get(pdf_url)
        if not issue_url and not replay_url:
            self._set_download_meta(
                error_type="missing_issue_context",
                message="Jurimetrics PDF download requires its discovered issue page",
                final_url=pdf_url,
                download_method="playwright_js_fetch",
            )
            return None

        try:
            if replay_url:
                replay_response = self._request_with_retries(replay_url, timeout=60)
                payload = replay_response.content
                if not payload.startswith(b"%PDF"):
                    raise ValueError("archived capture returned non-PDF content")
                download_method = "wayback_replay"
                final_url = replay_url
            else:
                assert issue_url is not None
            # The pipeline's smoke runner calls adapters from an asyncio loop.
            # Playwright's synchronous API rejects that calling thread, so keep
            # the browser lifecycle on a short-lived worker with no event loop.
                with ThreadPoolExecutor(max_workers=1) as executor:
                    try:
                        payload, final_url = executor.submit(
                            self._fetch_pdf_from_wayback, pdf_url
                        ).result()
                        download_method = "wayback_replay"
                    except Exception:
                        payload = executor.submit(
                            self._fetch_pdf_in_browser, issue_url, pdf_url
                        ).result()
                        download_method = "playwright_js_fetch"
                        final_url = pdf_url
            output_dir = Path(out_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            filename = Path(urlparse(pdf_url).path).name or "jurimetrics-issue.pdf"
            output_path = output_dir / filename
            output_path.write_bytes(payload)
            self._set_download_meta(
                ok=True,
                error_type="",
                status_code=200,
                content_type="application/pdf",
                pdf_size_bytes=len(payload),
                final_url=final_url,
                response_body_size=len(payload),
                download_method=download_method,
            )
            return str(output_path)
        except Exception as exc:
            self._set_download_meta(
                error_type="download_failed",
                message=str(exc),
                final_url=pdf_url,
                download_method="wayback_or_playwright",
            )
            return None

    @staticmethod
    def _fetch_pdf_in_browser(issue_url: str, pdf_url: str) -> bytes:
        """Fetch one challenge-protected PDF in a Playwright-only thread."""
        with PlaywrightSession(
            headless=False, min_delay=0.2, max_delay=0.5, max_retries=1
        ) as pw:
            issue_response = pw.get(issue_url, timeout=45)
            if issue_response is None or issue_response.status_code >= 400:
                raise RuntimeError("ABA issue page did not clear the browser challenge")
            data_url = pw._page.evaluate(
                """async (url) => {
                  const response = await fetch(url);
                  if (!response.ok) throw new Error(`HTTP ${response.status}`);
                  const blob = await response.blob();
                  return await new Promise((resolve, reject) => {
                    const reader = new FileReader();
                    reader.onload = () => resolve(reader.result);
                    reader.onerror = () => reject(reader.error);
                    reader.readAsDataURL(blob);
                  });
                }""",
                pdf_url,
            )
        payload = base64.b64decode(str(data_url).split(",", 1)[1])
        if not payload.startswith(b"%PDF"):
            raise ValueError("browser fetch returned non-PDF content")
        return payload

    @staticmethod
    def _fetch_pdf_from_wayback(pdf_url: str) -> tuple[bytes, str]:
        """Use an exact archived capture when Cloudflare blocks the live asset."""
        payload, replay_url = JurimetricsAdapter._fetch_latest_wayback(
            pdf_url, mimetype="application/pdf"
        )
        if not payload.startswith(b"%PDF"):
            raise ValueError("archived capture returned non-PDF content")
        return payload, replay_url

    @staticmethod
    def _fetch_latest_wayback(url: str, *, mimetype: str) -> tuple[bytes, str]:
        """Return the newest exact, successful Wayback capture for a URL."""
        cdx_response = JurimetricsAdapter._request_with_retries(
            "https://web.archive.org/cdx/search/cdx",
            params={
                "url": url,
                "output": "json",
                "filter": ["statuscode:200", f"mimetype:{mimetype}"],
                "fl": "timestamp,original",
                "limit": "20",
            },
            timeout=30,
        )
        cdx_response.raise_for_status()
        rows = cdx_response.json()
        if len(rows) < 2:
            raise RuntimeError("no archived PDF capture is available")
        timestamp, original = rows[-1]
        replay_url = f"https://web.archive.org/web/{timestamp}id_/{original}"
        replay_response = JurimetricsAdapter._request_with_retries(replay_url, timeout=60)
        payload = replay_response.content
        return payload, replay_url

    @staticmethod
    def _request_with_retries(url: str, **kwargs):
        """Retry transient Wayback connection/rate failures with bounded backoff."""
        last_error: Optional[Exception] = None
        for attempt in range(6):
            try:
                response = requests.get(url, **kwargs)
                response.raise_for_status()
                return response
            except Exception as exc:
                last_error = exc
                if attempt < 5:
                    time.sleep(2**attempt)
        assert last_error is not None
        raise last_error
