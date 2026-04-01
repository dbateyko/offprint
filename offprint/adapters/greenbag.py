from __future__ import annotations

import re
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


class GreenBagAdapter(Adapter):
    """Adapter for Green Bag archive tables of contents."""

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = self.session.get(url, headers=DEFAULT_HEADERS, timeout=30)
            if response.status_code >= 400:
                return None
            return BeautifulSoup(response.text, "lxml")
        except Exception:
            return None

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        soup = self._get_page(seed_url)
        if not soup:
            return

        seen: set[str] = set()
        for link in soup.find_all("a", href=True):
            raw_href = (link.get("href") or "").strip()
            if not raw_href:
                continue
            pdf_url = urljoin(seed_url, raw_href)
            if not self._is_greenbag_pdf(pdf_url):
                continue
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            metadata = self._extract_metadata(link_text=link.get_text(" ", strip=True), pdf_url=pdf_url)
            yield DiscoveryResult(page_url=seed_url, pdf_url=pdf_url, metadata=metadata)

    def _is_greenbag_pdf(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        if not parsed.netloc:
            return False
        host = parsed.netloc.lower()
        if not (host == "greenbag.org" or host == "www.greenbag.org"):
            return False
        return parsed.path.lower().endswith(".pdf")

    def _extract_metadata(self, link_text: str, pdf_url: str) -> dict:
        metadata: dict[str, str] = {
            "source_url": pdf_url,
            "url": pdf_url,
        }
        clean_title = re.sub(r"\s+", " ", (link_text or "").strip())
        if clean_title:
            metadata["title"] = clean_title

        parsed = urlparse(pdf_url)
        # Typical path shape: /v27n1/v27n1_articles_garner.pdf
        volume_issue_match = re.search(r"/v(\d+)n(\d+)/", parsed.path.lower())
        if volume_issue_match:
            metadata["volume"] = volume_issue_match.group(1)
            metadata["issue"] = volume_issue_match.group(2)
        year_match = re.search(r"\b(19|20)\d{2}\b", clean_title)
        if year_match:
            metadata["year"] = year_match.group(0)
        return metadata
