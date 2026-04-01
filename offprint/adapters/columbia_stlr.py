from __future__ import annotations

from typing import Iterable, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS, GenericAdapter


class ColumbiaSTLRAdapter(Adapter):
    """Adapter tailored for Columbia's STLR (OJS) structure.

    Sitemap guidance (mapped to selectors):
    - Archive pages: /issue/archive/[N]
      - Volume cards: div.card-body → issue link: a
    - Issue page: article listings: div.article-summary
      - Title: .article-summary-title a
      - Authors: div.article-summary-authors (not stored here but parsed if needed)
      - PDF path: article-summary contains action button a.btn (to article page)
    - Article page: .pdf-download-button a → final PDF link
    """

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        super().__init__(session=session)
        self.downloader = GenericAdapter(self.session)

    def _get(self, url: str) -> Optional[requests.Response]:
        try:
            r = self.session.get(url, headers=DEFAULT_HEADERS, timeout=25)
            if r.status_code >= 400:
                return None
            return r
        except requests.RequestException:
            return None

    def _iter_issue_links_from_archive(self, archive_url: str) -> Iterable[str]:
        resp = self._get(archive_url)
        if not resp or not resp.text:
            return
        soup = BeautifulSoup(resp.text, "lxml")
        for card in soup.select("div.card-body"):
            for a in card.select("a[href]"):
                href = a.get("href")
                if not href:
                    continue
                yield urljoin(archive_url, href)

    def _iter_article_blocks(self, issue_url: str) -> Iterable[BeautifulSoup]:
        resp = self._get(issue_url)
        if not resp or not resp.text:
            return
        soup = BeautifulSoup(resp.text, "lxml")
        for art in soup.select("div.article-summary"):
            yield art

    def _article_title(self, article_block: BeautifulSoup) -> Optional[str]:
        a = article_block.select_one(".article-summary-title a")
        return a.get_text(strip=True) if a else None

    def _article_authors(self, article_block: BeautifulSoup) -> Optional[str]:
        t = article_block.select_one("div.article-summary-authors")
        return t.get_text(strip=True) if t else None

    def _article_page_link(self, article_block: BeautifulSoup, base_url: str) -> Optional[str]:
        btn = article_block.select_one("a.btn[href]")
        if not btn:
            # fallback: try the title link
            title_a = article_block.select_one(".article-summary-title a[href]")
            if not title_a:
                return None
            return urljoin(base_url, title_a["href"])
        return urljoin(base_url, btn["href"])

    def _extract_pdf_from_article_page(self, article_url: str) -> Optional[str]:
        resp = self._get(article_url)
        if not resp or not resp.text:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        # Primary selector from sitemap
        a = soup.select_one(".pdf-download-button a[href]")
        if a and a.get("href"):
            return urljoin(article_url, a["href"])  # absolute
        # Fallbacks: any link ending with pdf or containing /pdf
        for link in soup.select("a[href]"):
            href = link.get("href", "")
            absu = urljoin(article_url, href)
            if absu.lower().endswith(".pdf") or "/pdf" in absu.lower():
                return absu
        return None

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        # seed is archive page; enumerate issues, then articles, then article page → PDF
        found = False
        for issue in self._iter_issue_links_from_archive(seed_url):
            for art in self._iter_article_blocks(issue):
                article_page = self._article_page_link(art, base_url=issue)
                if not article_page:
                    continue
                pdf = self._extract_pdf_from_article_page(article_page)
                if pdf:
                    found = True
                    yield DiscoveryResult(page_url=article_page, pdf_url=pdf)

        # Fallback to the generic OJS adapter if site-specific parsing yields nothing
        if not found:
            from .ojs import OJSAdapter

            yield from OJSAdapter(session=self.session).discover_pdfs(seed_url, max_depth=max_depth)

    def download_pdf(self, pdf_url: str, out_dir: str) -> Optional[str]:
        local_path = self.downloader.download_pdf(pdf_url, out_dir)
        self.last_download_meta = dict(self.downloader.last_download_meta or {})
        return local_path
