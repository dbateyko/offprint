from __future__ import annotations

import re
from collections.abc import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .utils import extract_year, parse_authors


class AveMariaLawReviewAdapter(Adapter):
    """Adapter for the Ave Maria Law Review WordPress archive page."""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def _get_soup(self, url: str) -> BeautifulSoup | None:
        try:
            response = self.session.get(url, headers=self.HEADERS, timeout=25)
        except Exception:
            return None
        if response.status_code >= 400 or not response.text:
            return None
        return BeautifulSoup(response.text, "lxml")

    @staticmethod
    def _volume_for_link(link) -> str:
        for parent in link.parents:
            node_id = parent.get("id") or ""
            match = re.search(r"tab-volume-(\d+)", node_id)
            if match:
                return match.group(1)
        return ""

    @staticmethod
    def _authors_for_link(link, title: str) -> list[str]:
        container_text = link.parent.get_text(" ", strip=True) if link.parent else ""
        if not container_text:
            return []
        tail = container_text.replace(title, "", 1).strip()
        tail = re.sub(r"^[\s\-:\u2013\u2014]+", "", tail).strip()
        if not tail:
            return []
        return parse_authors(tail)

    def discover_pdfs(
        self, seed_url: str, max_depth: int = 0
    ) -> Iterable[DiscoveryResult]:
        soup = self._get_soup(seed_url)
        if not soup:
            return

        seen: set[str] = set()
        for link in soup.select('div[id^="tab-volume-"] a[href$=".pdf"]'):
            title = link.get_text(" ", strip=True)
            if not title or title.upper() in {"MASTHEAD", "ARTICLES"}:
                continue
            pdf_url = urljoin(seed_url, link.get("href") or "")
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            metadata = {
                "title": title,
                "authors": self._authors_for_link(link, title),
                "volume": self._volume_for_link(link),
                "year": extract_year(pdf_url) or "",
                "url": pdf_url,
                "source_url": seed_url,
            }
            yield DiscoveryResult(page_url=seed_url, pdf_url=pdf_url, metadata=metadata)
