from __future__ import annotations

import re
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .wordpress_academic_base import WordPressAcademicBaseAdapter


class RutgersLawReviewAdapter(WordPressAcademicBaseAdapter):
    """Host-specific adapter for Rutgers Law Review printed-issue archives."""

    VOLUME_RE = re.compile(r"/volume-\d+", re.I)

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        response = self._request_with_retry(url, timeout=20, max_attempts=3)
        if response is None or int(response.status_code or 0) >= 400:
            return None
        return BeautifulSoup(response.text, "lxml")

    def _extract_volume_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        urls: List[str] = []
        for anchor in soup.select("a[href]"):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            full = urljoin(base_url, href)
            parsed = urlparse(full)
            if "rutgerslawreview.com" not in (parsed.netloc or "").lower():
                continue
            if self.VOLUME_RE.search(parsed.path or ""):
                urls.append(full)

        deduped: List[str] = []
        seen = set()
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            deduped.append(url)
        return deduped

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        yielded = False
        seed_soup = self._get_page(seed_url)
        if seed_soup is not None:
            for volume_url in self._extract_volume_urls(seed_soup, seed_url):
                volume_soup = self._get_page(volume_url)
                if volume_soup is None:
                    continue
                title = volume_soup.title.get_text(strip=True) if volume_soup.title else volume_url
                for row in self._extract_pdfs_from_article(volume_soup, volume_url, title):
                    yielded = True
                    row.metadata.setdefault("source_url", volume_url)
                    row.metadata.setdefault("url", volume_url)
                    row.metadata["extraction_method"] = "rutgers_law_review"
                    yield row

        if yielded:
            return

        # Fallback keeps compatibility if site theme/layout changes.
        yield from super().discover_pdfs(seed_url, max_depth=max(max_depth, 2))
