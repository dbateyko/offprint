from __future__ import annotations

import re
from typing import Iterable, Optional
from urllib.parse import urljoin

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

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        with PlaywrightSession(headless=False, min_delay=0.2, max_delay=0.5, max_retries=2) as pw:
            archive = pw.get(seed_url, timeout=45)
            if archive is None or archive.status_code >= 400:
                return

            archive_soup = BeautifulSoup(archive.text, "lxml")
            seen_issue_urls: set[str] = set()

            for anchor in archive_soup.select(".aba-article-content a[href]"):
                href = (anchor.get("href") or "").strip()
                if not href:
                    continue
                issue_url = urljoin(seed_url, href)
                if issue_url in seen_issue_urls:
                    continue
                seen_issue_urls.add(issue_url)

                issue_response = pw.get(issue_url, timeout=45)
                if issue_response is None or issue_response.status_code >= 400:
                    continue

                pdf_url = self._extract_issue_pdf_url(issue_response.text)
                if not pdf_url:
                    continue

                issue_soup = BeautifulSoup(issue_response.text, "lxml")
                metadata = self._extract_issue_metadata(issue_soup, issue_url)

                yield DiscoveryResult(
                    page_url=issue_url,
                    pdf_url=pdf_url,
                    metadata=metadata,
                    source_adapter="jurimetrics",
                    extraction_path="archive_issue_download",
                )

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

        return metadata
