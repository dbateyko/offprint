from __future__ import annotations

import re
from typing import Iterable, Optional, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


_ARTICLE_PATH_RE = re.compile(r"/law/lawreview/issues/Archives/v(\d+)-(\d+)/[^/]+/?$", re.IGNORECASE)
_VOLUME_ISSUE_RE = re.compile(r"/v(\d+)-(\d+)/", re.IGNORECASE)


class DrexelLawReviewAdapter(Adapter):
    """Adapter for Drexel Law Review at drexel.edu/law/lawreview/.

    The original drexellawreview.org domain is offline. The journal's archive
    lives at /law/lawreview/issues/Archives/, with article landing pages of the
    form /law/lawreview/issues/Archives/vNN-N/<author-slug>/. Each landing page
    contains a "Full Article [PDF]" link pointing at an ASP.NET handler URL
    ending in .ashx (e.g. /~/media/Files/law/law review/V18-2/foo.ashx).

    The .ashx URLs return real PDF bytes; the downloader validates by magic
    bytes, so the adapter only needs to surface them as pdf_url candidates.
    """

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20)
            if resp.status_code >= 400:
                return None
            return BeautifulSoup(resp.text, "lxml")
        except Exception:
            return None

    @staticmethod
    def _is_pdf_handler_url(url: str) -> bool:
        lowered = (url or "").lower()
        return ".ashx" in lowered or lowered.endswith(".pdf")

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        soup = self._get_page(seed_url)
        if not soup:
            return

        seen_articles: Set[str] = set()
        seen_pdfs: Set[str] = set()

        # Collect article landing pages from the archive index.
        article_urls = []
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if not href:
                continue
            abs_url = urljoin(seed_url, href)
            parsed = urlparse(abs_url)
            if "drexel.edu" not in parsed.netloc:
                continue
            if not _ARTICLE_PATH_RE.search(parsed.path):
                continue
            if abs_url in seen_articles:
                continue
            seen_articles.add(abs_url)
            article_urls.append((abs_url, a.get_text(strip=True)))

        # If the seed already IS an article landing page, treat it as such.
        if not article_urls and _ARTICLE_PATH_RE.search(urlparse(seed_url).path):
            article_urls.append((seed_url, ""))

        for article_url, anchor_text in article_urls:
            for result in self._process_article_page(article_url, anchor_text):
                if result.pdf_url in seen_pdfs:
                    continue
                seen_pdfs.add(result.pdf_url)
                yield result

    def _process_article_page(
        self, article_url: str, anchor_text: str
    ) -> Iterable[DiscoveryResult]:
        soup = self._get_page(article_url)
        if not soup:
            return

        # Volume / issue from URL.
        metadata: dict = {"source_url": article_url, "url": article_url}
        m = _VOLUME_ISSUE_RE.search(article_url)
        if m:
            metadata["volume"] = m.group(1)
            metadata["issue"] = m.group(2)

        # Title: prefer page <h1>, fall back to <title>, else anchor text.
        h1 = soup.select_one("h1")
        if h1 and h1.get_text(strip=True):
            metadata["title"] = h1.get_text(strip=True)
        elif soup.title and soup.title.get_text(strip=True):
            metadata["title"] = soup.title.get_text(strip=True)
        elif anchor_text:
            metadata["title"] = anchor_text

        # Find the "Full Article [PDF]" .ashx link.
        for link in soup.select("a[href]"):
            href = link.get("href") or ""
            if not href:
                continue
            if not self._is_pdf_handler_url(href):
                continue
            pdf_url = urljoin(article_url, href)
            yield DiscoveryResult(
                page_url=article_url,
                pdf_url=pdf_url,
                metadata=dict(metadata),
            )
