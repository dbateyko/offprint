from __future__ import annotations

from typing import Iterable, Optional

from bs4 import BeautifulSoup

from .base import Adapter
from .utils import absolutize, fetch_page, is_pdf_url


class SiteArchiveAdapterBase(Adapter):
    """Shared helpers for explicit per-site archive adapters."""

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        return fetch_page(self.session, url)

    @staticmethod
    def _iter_links(soup: Optional[BeautifulSoup], base_url: str) -> Iterable[tuple[str, str]]:
        if not soup:
            return []
        out: list[tuple[str, str]] = []
        for anchor in soup.select("a[href]"):
            href = (anchor.get("href") or "").strip()
            if not href or href.startswith("#") or href.lower().startswith("javascript:"):
                continue
            href = href.replace("\\", "/")
            url = absolutize(base_url, href)
            text = anchor.get_text(" ", strip=True)
            out.append((text, url))
        return out

    @staticmethod
    def _is_pdf_candidate(url: str, text: str = "") -> bool:
        lowered_text = (text or "").lower()
        return (
            is_pdf_url(url)
            or ("download" in lowered_text and "pdf" in lowered_text)
            or lowered_text == "pdf"
        )
