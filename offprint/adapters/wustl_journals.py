from __future__ import annotations

from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


class WUSTLJournalsAdapter(Adapter):
    """Adapter for journals.library.wustl.edu article/galley download flows."""

    def _get(self, url: str) -> requests.Response | None:
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20)
            if resp.status_code >= 400:
                return None
            return resp
        except requests.RequestException:
            return None

    def _article_links(self, html: str, base_url: str, origin: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        found: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = urljoin(base_url, a["href"])
            parsed = urlparse(href)
            if parsed.scheme not in {"http", "https"} or parsed.netloc != origin:
                continue
            path = parsed.path.lower()
            if "/article/id/" not in path:
                continue
            normalized = href.rstrip("/") + "/"
            if normalized in seen:
                continue
            seen.add(normalized)
            found.append(normalized)
        return found

    def _extract_download_links(self, article_html: str, article_url: str) -> list[str]:
        soup = BeautifulSoup(article_html, "lxml")
        found: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = urljoin(article_url, a["href"])
            lowered = href.lower()
            if "/galley/" not in lowered:
                continue
            if "/download/" not in lowered and not lowered.endswith(".pdf"):
                continue
            if href in seen:
                continue
            seen.add(href)
            found.append(href)
        return found

    def _extract_title(self, html: str) -> str:
        soup = BeautifulSoup(html, "lxml")
        title = soup.select_one("h1")
        if title and title.get_text(strip=True):
            return title.get_text(strip=True)
        if soup.title:
            return soup.title.get_text(strip=True)
        return ""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        resp = self._get(seed_url)
        if not resp or not resp.text:
            return

        origin = urlparse(resp.url).netloc
        article_urls = self._article_links(resp.text, resp.url, origin)
        for article_url in article_urls[:200]:
            article_resp = self._get(article_url)
            if not article_resp or not article_resp.text:
                continue

            title = self._extract_title(article_resp.text)
            metadata = {
                "title": title,
                "url": article_url,
                "page_url": article_url,
                "platform": "WUSTL Journals",
                "extraction_method": "wustl_journals_adapter",
            }

            for pdf_url in self._extract_download_links(article_resp.text, article_url):
                yield DiscoveryResult(
                    page_url=article_url,
                    pdf_url=pdf_url,
                    metadata=dict(metadata),
                    extraction_path="article_galley_download",
                )

    def download_pdf(self, pdf_url: str, out_dir: str) -> str | None:
        return self._download_with_generic(pdf_url, out_dir)
