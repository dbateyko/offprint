from __future__ import annotations

import base64
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import parse_qs, urlparse

from .base import DiscoveryResult
from .digital_commons_issue_article_hop import DigitalCommonsIssueArticleHopAdapter
from ..playwright_session import PlaywrightSession


class DigitalCommonsOriginFetchAdapter(DigitalCommonsIssueArticleHopAdapter):
    """Digital Commons issue traversal with page-origin browser PDF delivery.

    Some bepress repositories serve public article pages normally but deny direct
    ``viewcontent.cgi`` requests. Loading the publication root first and fetching
    the PDF from that browser origin clears the repository's delivery challenge.
    """

    NON_ARTICLE_TITLE_RE = re.compile(
        r"^(?:student\s+)?(?:front matter|back matter|masthead|table of contents|"
        r"editorial board)\b|(?:front matter|back matter|masthead|table of contents|"
        r"editorial board)\s*$",
        re.IGNORECASE,
    )

    # The PDF delivery edge is shared by otherwise independent Digital Commons
    # repositories. A full run against Minnesota and Michigan showed that
    # parallel, per-host pacing still tripped the common WAF after 13 downloads
    # in 37 seconds. Serialize delivery across adapter instances and stay below
    # that observed rolling-window threshold.
    DELIVERY_INTERVAL_SECONDS = 6.0
    WAF_COOLDOWN_SECONDS = 65.0
    _delivery_fetch_lock = threading.Lock()
    _delivery_lock = threading.Lock()
    _next_delivery_at = 0.0

    @classmethod
    def _is_article(cls, result: DiscoveryResult) -> bool:
        title = str((result.metadata or {}).get("title") or "").strip()
        return not cls.NON_ARTICLE_TITLE_RE.search(title)

    def configure_dc(self, **kwargs) -> None:
        kwargs["enum_mode"] = "all_issues_only"
        super().configure_dc(**kwargs)

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        for result in super().discover_pdfs(seed_url, max_depth=max_depth):
            if not self._is_article(result):
                continue
            match = re.search(r"/vol(?P<volume>\d+)/iss(?P<issue>\d+)/", result.page_url)
            if match:
                result.metadata = dict(result.metadata or {})
                result.metadata.setdefault("volume", match.group("volume"))
                result.metadata.setdefault("issue", match.group("issue"))
            yield result

    @staticmethod
    def _publication_root(pdf_url: str) -> str:
        parsed = urlparse(pdf_url)
        context = (parse_qs(parsed.query).get("context") or [""])[0].strip()
        if not context:
            raise ValueError("Digital Commons PDF URL is missing its context parameter")
        return f"{parsed.scheme}://{parsed.netloc}/{context}/"

    def download_pdf(self, pdf_url: str, out_dir: str, **kwargs) -> Optional[str]:
        del kwargs
        try:
            publication_root = self._publication_root(pdf_url)
            parsed = urlparse(pdf_url)
            query = parse_qs(parsed.query)
            context = (query.get("context") or ["article"])[0]
            article = (query.get("article") or ["unknown"])[0]
            output_dir = Path(out_dir)
            output_path = output_dir / f"{context}-{article}.pdf"

            # Failsafe process attempts revisit the archive from the beginning.
            # Do not spend scarce WAF delivery slots re-fetching a stable PDF that
            # is already present and still has valid PDF magic bytes.
            if output_path.is_file() and output_path.read_bytes()[:4] == b"%PDF":
                size = output_path.stat().st_size
                self._set_download_meta(
                    ok=True,
                    error_type="",
                    status_code=200,
                    content_type="application/pdf",
                    pdf_size_bytes=size,
                    final_url=pdf_url,
                    response_body_size=size,
                    download_method="existing_valid_pdf",
                    skipped_duplicate=True,
                )
                return str(output_path)

            # Keep the reservation and request atomic so a second repository
            # cannot slip in while the first one is applying a WAF cooldown.
            with DigitalCommonsOriginFetchAdapter._delivery_fetch_lock:
                self._wait_for_delivery_slot()
                try:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        payload = executor.submit(
                            self._fetch_pdf_from_origin, publication_root, pdf_url
                        ).result()
                except Exception as exc:
                    if "HTTP 403" in str(exc):
                        self._apply_waf_cooldown()
                    raise

            output_dir.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(payload)
            self._set_download_meta(
                ok=True,
                error_type="",
                status_code=200,
                content_type="application/pdf",
                pdf_size_bytes=len(payload),
                final_url=pdf_url,
                response_body_size=len(payload),
                download_method="playwright_page_origin_fetch",
            )
            return str(output_path)
        except Exception as exc:
            self._set_download_meta(
                error_type="download_failed",
                message=str(exc),
                final_url=pdf_url,
                download_method="playwright_page_origin_fetch",
            )
            return None

    @classmethod
    def _wait_for_delivery_slot(cls) -> None:
        """Reserve one process-wide delivery slot for the shared bepress edge."""
        # Always mutate state on the base class: subclasses represent different
        # repositories but their PDF requests reach the same delivery firewall.
        owner = DigitalCommonsOriginFetchAdapter
        with owner._delivery_lock:
            now = time.monotonic()
            wait_seconds = max(0.0, owner._next_delivery_at - now)
            if wait_seconds:
                time.sleep(wait_seconds)
                now = time.monotonic()
            owner._next_delivery_at = now + owner.DELIVERY_INTERVAL_SECONDS

    @classmethod
    def _apply_waf_cooldown(cls) -> None:
        """Delay all repositories after a Digital Commons WAF response."""
        owner = DigitalCommonsOriginFetchAdapter
        with owner._delivery_lock:
            owner._next_delivery_at = max(
                owner._next_delivery_at,
                time.monotonic() + owner.WAF_COOLDOWN_SECONDS,
            )

    @staticmethod
    def _fetch_pdf_from_origin(publication_root: str, pdf_url: str) -> bytes:
        with PlaywrightSession(
            headless=False, min_delay=0.2, max_delay=0.5, max_retries=1
        ) as browser:
            root_response = browser.get(publication_root, timeout=45)
            if root_response is None or root_response.status_code >= 400:
                raise RuntimeError("publication root did not load in the browser")
            data_url = browser._page.evaluate(
                """async (url) => {
                  const response = await fetch(url, {credentials: 'include'});
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
            raise ValueError("page-origin fetch returned non-PDF content")
        return payload


class MinnesotaJLSTAdapter(DigitalCommonsOriginFetchAdapter):
    """Minnesota Journal of Law, Science & Technology delivery adapter."""


class MichiganTechnologyLawReviewAdapter(DigitalCommonsOriginFetchAdapter):
    """Michigan Technology Law Review delivery adapter."""
