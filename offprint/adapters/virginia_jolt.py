from __future__ import annotations

import re
from typing import Iterable, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .generic import DEFAULT_HEADERS
from .jolt_base import JOLTBaseAdapter


class VirginiaJOLTAdapter(JOLTBaseAdapter):
    """Adapter for Virginia Journal of Law & Technology (www.vjolt.org).

    Handles the SquareSpace-based platform with volume pages, work pages,
    and fluid-engine content blocks.
    """

    journal_short_cite = "Va. J.L. & Tech."
    extra_pdf_url_markers = ["static1.squarespace.com"]

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        soup = self._get_page(seed_url)
        if not soup:
            return

        seen_pdf_urls: Set[str] = set()

        def _yield_unique(results: Iterable[DiscoveryResult]) -> Iterable[DiscoveryResult]:
            for result in results:
                if result.pdf_url in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(result.pdf_url)
                yield result

        if "/volume-" in seed_url:
            yield from _yield_unique(self._process_ss_volume_page(soup, seed_url))
        elif "/work" in seed_url:
            yield from _yield_unique(self._process_work_page(soup, seed_url))
        else:
            yield from _yield_unique(self._process_main_page(soup, seed_url))

    def _process_ss_volume_page(
        self, soup: BeautifulSoup, page_url: str
    ) -> Iterable[DiscoveryResult]:
        volume_match = re.search(r"/volume-(\d+)", page_url)
        volume_number = volume_match.group(1) if volume_match else ""

        for section in soup.select("div.user-items-list"):
            issue_info = ""
            title_el = section.select_one(".list-section-title p")
            if title_el:
                issue_info = title_el.get_text(strip=True)

            for item in section.select("div.list-item-content"):
                yield from self._process_article_item(item, page_url, volume_number, issue_info)

    def _process_work_page(self, soup: BeautifulSoup, page_url: str) -> Iterable[DiscoveryResult]:
        for card in soup.select("div.image-card"):
            yield from self._process_image_card(card, page_url)

    def _process_main_page(self, soup: BeautifulSoup, page_url: str) -> Iterable[DiscoveryResult]:
        seen_links: Set[str] = set()
        for block in soup.select("div.fluid-engine"):
            yield from self._process_fluid_engine_block(block, page_url)

        for link in soup.select("a[href]"):
            href = link.get("href", "")
            absolute = urljoin(page_url, href)
            if absolute in seen_links:
                continue
            seen_links.add(absolute)
            parsed = urlparse(absolute)
            if parsed.scheme not in {"http", "https"}:
                continue
            if "/s/" in parsed.path:
                resolved = self._resolve_possible_pdf_url(absolute)
                if resolved:
                    yield DiscoveryResult(
                        page_url=page_url, pdf_url=resolved, metadata={"url": resolved}
                    )
                continue
            if any(
                token in parsed.path for token in ("/volume-", "/vol", "/work", "/past-volumes")
            ):
                next_soup = self._get_page(absolute)
                if not next_soup:
                    continue
                if "/work" in parsed.path:
                    yield from self._process_work_page(next_soup, absolute)
                else:
                    yield from self._process_ss_volume_page(next_soup, absolute)

    def _process_article_item(
        self, item: BeautifulSoup, page_url: str, volume_number: str, issue_info: str
    ) -> Iterable[DiscoveryResult]:
        try:
            metadata: dict = {}
            h2 = item.select_one("h2")
            if h2:
                metadata["title"] = h2.get_text(strip=True)
            desc = item.select_one("div.list-item-content__description")
            if desc:
                text = desc.get_text(strip=True)
                if text:
                    metadata["authors"] = [text]
            if volume_number:
                metadata["volume"] = volume_number
            if issue_info:
                yr = self._extract_year(issue_info)
                if yr:
                    metadata["date"] = yr
                metadata["issue_info"] = issue_info

            pdf_link = item.select_one("a")
            if pdf_link:
                pdf_url = urljoin(page_url, pdf_link.get("href", ""))
                if pdf_url and self._is_likely_pdf_url(pdf_url):
                    metadata["url"] = pdf_url
                    self._generate_citation(metadata)
                    yield DiscoveryResult(page_url=page_url, pdf_url=pdf_url, metadata=metadata)
        except Exception:
            return

    def _process_image_card(self, card: BeautifulSoup, page_url: str) -> Iterable[DiscoveryResult]:
        try:
            metadata: dict = {}
            h4 = card.select_one("h4")
            if h4:
                issue_text = h4.get_text(strip=True)
                metadata["issue_info"] = issue_text
                v = self._extract_volume_number(issue_text)
                if v:
                    metadata["volume"] = v
                yr = self._extract_year(issue_text)
                if yr:
                    metadata["date"] = yr

            em = card.select_one("em")
            if em:
                text = em.get_text(strip=True)
                if " by " in text:
                    parts = text.split(" by ", 1)
                    metadata["title"] = parts[0].strip()
                    metadata["authors"] = [parts[1].strip()]
                else:
                    metadata["title"] = text

            pdf_link = card.select_one("a")
            if pdf_link:
                pdf_url = urljoin(page_url, pdf_link.get("href", ""))
                if pdf_url and self._is_likely_pdf_url(pdf_url):
                    metadata["url"] = pdf_url
                    self._generate_citation(metadata)
                    yield DiscoveryResult(page_url=page_url, pdf_url=pdf_url, metadata=metadata)
        except Exception:
            return

    def _process_fluid_engine_block(
        self, block: BeautifulSoup, page_url: str
    ) -> Iterable[DiscoveryResult]:
        try:
            metadata: dict = {}
            pdf_link = block.select_one("a")
            if not pdf_link:
                return
            raw_link = urljoin(page_url, pdf_link.get("href", ""))
            if not raw_link:
                return
            pdf_url = raw_link
            if not self._is_likely_pdf_url(pdf_url):
                if "/s/" in urlparse(raw_link).path:
                    resolved = self._resolve_possible_pdf_url(raw_link)
                    if not resolved:
                        return
                    pdf_url = resolved
                else:
                    return

            title_text = pdf_link.get_text(strip=True)
            if title_text:
                metadata["title"] = title_text

            author_el = block.select_one("p:nth-of-type(2)")
            if author_el:
                text = author_el.get_text(strip=True)
                if text:
                    metadata["authors"] = [text]

            for p in block.select("div.fe-block:nth-of-type(n+2) p:nth-of-type(1)"):
                yr = self._extract_year(p.get_text(strip=True))
                if yr:
                    metadata["date"] = yr
                    break

            metadata["url"] = pdf_url
            self._generate_citation(metadata)
            yield DiscoveryResult(page_url=page_url, pdf_url=pdf_url, metadata=metadata)
        except Exception:
            return

    def _resolve_possible_pdf_url(self, url: str) -> str:
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20, stream=True)
            final_url = str(resp.url or url)
            content_type = str(resp.headers.get("Content-Type") or "").lower()
            resp.close()
            if "application/pdf" in content_type:
                return final_url
            if "static1.squarespace.com" in final_url.lower():
                return final_url
        except Exception:
            return ""
        return ""
