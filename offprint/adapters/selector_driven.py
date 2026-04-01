from __future__ import annotations

from collections import defaultdict
import re
from pathlib import PurePosixPath
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .models import ArticleMetadata
from .utils import (
    absolutize,
    extract_year,
    fetch_page,
    is_pdf_url,
    parse_authors,
)


class SelectorDrivenAdapter(Adapter):
    """Adapter that uses sitemap JSON selectors for extraction."""

    def __init__(self, sitemap: Dict, session=None):
        super().__init__(session=session)
        self.sitemap = sitemap or {}
        selectors = (
            sitemap.get("selectors")
            or sitemap.get("raw_sitemap", {}).get("selectors")
            or sitemap.get("raw", {}).get("selectors")
            or []
        )
        self.selectors: List[Dict] = [s for s in selectors if isinstance(s, dict)]
        self.selector_map = {s.get("id"): s for s in self.selectors if s.get("id")}
        self.children: Dict[str, List[Dict]] = defaultdict(list)
        for sel in self.selectors:
            for parent in sel.get("parentSelectors", []):
                self.children[parent].append(sel)
        self.article_selectors: List[Dict] = [
            s
            for s in self.selectors
            if s.get("type") == "SelectorElement" and "_root" in (s.get("parentSelectors") or [])
        ]

    @staticmethod
    def _filename_stem(url: str) -> str:
        path = urlparse(url).path or ""
        stem = PurePosixPath(path).stem
        stem = re.sub(r"[_-]+", " ", stem).strip()
        return stem

    @staticmethod
    def _infer_volume_from_text(text: str) -> str:
        match = re.search(r"\bvol(?:ume)?\s*[-_ ]?\s*(\d+)\b", text, flags=re.IGNORECASE)
        return match.group(1) if match else ""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0, browser_session=None) -> Iterable[DiscoveryResult]:
        if browser_session:
            html = browser_session.get_html(seed_url)
            soup = BeautifulSoup(html, "html.parser") if html else None
        else:
            soup = fetch_page(self.session, seed_url)
        if not soup:
            return

        if not self.article_selectors:
            yield from self._process_article(soup, seed_url, parent_id="_root")
            return

        for article_selector in self.article_selectors:
            selector = article_selector.get("selector", "")
            if not selector:
                continue
            for article in soup.select(selector):
                yield from self._process_article(
                    article, seed_url, parent_id=article_selector.get("id")
                )

    def _process_article(
        self, article: BeautifulSoup, page_url: str, parent_id: Optional[str]
    ) -> Iterable[DiscoveryResult]:
        metadata = self._extract_metadata(article, page_url, parent_id=parent_id)
        pdf_url = self._extract_pdf_url(article, page_url, parent_id=parent_id)
        if not pdf_url and metadata.url:
            pdf_url = self._find_pdf_on_page(metadata.url)
        if pdf_url:
            # Fallbacks for sparse archive pages where selectors only expose file links.
            stem = self._filename_stem(pdf_url)
            if stem and not metadata.title:
                metadata.title = stem
            if stem and not metadata.volume:
                inferred_volume = self._infer_volume_from_text(stem)
                if inferred_volume:
                    metadata.volume = inferred_volume
            if stem and not metadata.date:
                inferred_year = extract_year(stem)
                if inferred_year:
                    metadata.date = inferred_year
            yield DiscoveryResult(
                page_url=metadata.source_url or page_url,
                pdf_url=pdf_url,
                metadata=metadata.to_dict(),
            )

    def _extract_pdf_url(
        self, article: BeautifulSoup, page_url: str, parent_id: Optional[str]
    ) -> Optional[str]:
        for sel in self.children.get(parent_id or "", []):
            if "pdf" in (sel.get("id") or "").lower() or sel.get("type") == "SelectorLink":
                candidate = self._extract_value(article, sel, page_url, prefer_link=True)
                if candidate and is_pdf_url(candidate):
                    return candidate
        # Fallback: any PDF-like link inside the article
        for a in article.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            url = absolutize(page_url, href)
            if is_pdf_url(url):
                return url
        return None

    def _extract_metadata(
        self, article: BeautifulSoup, page_url: str, parent_id: Optional[str]
    ) -> ArticleMetadata:
        metadata = ArticleMetadata(source_url=page_url)
        for sel in self.children.get(parent_id or "", []):
            raw_value = self._extract_value(article, sel, page_url)
            if not raw_value:
                continue
            sid = (sel.get("id") or "").lower()
            if "title" in sid and not metadata.title:
                metadata.title = raw_value
            elif "author" in sid:
                metadata.authors.extend(parse_authors(raw_value))
            elif "date" in sid or "year" in sid:
                metadata.date = extract_year(raw_value) or raw_value
            elif "volume" in sid:
                metadata.volume = raw_value
            elif "issue" in sid:
                metadata.issue = raw_value
            elif "page" in sid and not metadata.pages:
                metadata.pages = raw_value
            elif "citation" in sid and not metadata.citation:
                metadata.citation = raw_value
            elif "doi" in sid and not metadata.doi:
                metadata.doi = raw_value
            elif "abstract" in sid and not metadata.abstract:
                metadata.abstract = raw_value
            elif "url" in sid or (sel.get("type") == "SelectorLink" and not metadata.url):
                metadata.url = raw_value
            else:
                metadata.extra[sid] = raw_value

        # Deduplicate authors
        if metadata.authors:
            seen = set()
            deduped = []
            for author in metadata.authors:
                if author not in seen:
                    deduped.append(author)
                    seen.add(author)
            metadata.authors = deduped
        return metadata

    def _extract_value(
        self, article: BeautifulSoup, selector: Dict, page_url: str, prefer_link: bool = False
    ) -> Optional[str]:
        css = selector.get("selector") or ""
        if not css:
            return None
        matches = article.select(css)
        if not matches:
            return None

        sel_type = selector.get("type", "").lower()
        if sel_type == "selectorlink" or prefer_link:
            for match in matches:
                href = match.get("href") or match.get("src")
                if href:
                    return absolutize(page_url, href)
        if sel_type == "selectorelementattribute":
            attr = selector.get("extractAttribute") or selector.get("attribute") or "href"
            for match in matches:
                value = match.get(attr)
                if value:
                    if attr in {"href", "src"}:
                        return absolutize(page_url, value)
                    return value.strip()
        # Default: text content
        for match in matches:
            text = match.get_text(strip=True)
            if text:
                return text
        return None

    def _find_pdf_on_page(self, url: str) -> Optional[str]:
        soup = fetch_page(self.session, url)
        if not soup:
            return None
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            candidate = absolutize(url, href)
            if is_pdf_url(candidate):
                return candidate
        return None
