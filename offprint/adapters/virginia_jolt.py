from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Iterable, Set
from urllib.parse import unquote, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from .base import DiscoveryResult
from .generic import DEFAULT_HEADERS
from .jolt_base import JOLTBaseAdapter


class VirginiaJOLTAdapter(JOLTBaseAdapter):
    """Traverse every layout generation in the Squarespace VJOLT archive."""

    journal_short_cite = "Va. J.L. & Tech."
    extra_pdf_url_markers = ["static1.squarespace.com", "virginia.box.com/shared/static/"]

    _ARCHIVE_PATH_RE = re.compile(r"^/(?:volume-(\d+)(?:-\d+)?|vol(\d+))/?$", re.I)
    _VOLUME_RE = re.compile(r"/(?:volume-|vol)(\d+)", re.I)
    _GENERIC_LINK_TEXT = {"access this article", "click to read", "read here", "download"}

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        soup = self._get_page(seed_url)
        if not soup:
            return

        seen_pdf_urls: Set[str] = set()
        volume_pages = self._archive_volume_pages(soup, seed_url)
        if not volume_pages and self._volume_number(seed_url):
            volume_pages = [seed_url]

        for volume_url in volume_pages:
            volume_soup = soup if volume_url.rstrip("/") == seed_url.rstrip("/") else None
            if volume_soup is None:
                volume_soup = self._get_page(volume_url)
            if not volume_soup:
                continue
            for result in self._process_volume_page(volume_soup, volume_url):
                if result.pdf_url in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(result.pdf_url)
                yield result

    def _archive_volume_pages(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        """Return one archive-provided URL per volume, sorted by volume."""
        by_volume: dict[int, str] = {}
        for link in soup.select("a[href]"):
            absolute = urljoin(page_url, str(link.get("href") or ""))
            parsed = urlparse(absolute)
            if parsed.netloc.lower() not in {"vjolt.org", "www.vjolt.org"}:
                continue
            match = self._ARCHIVE_PATH_RE.match(parsed.path)
            if not match:
                continue
            volume = int(match.group(1) or match.group(2))
            by_volume.setdefault(volume, absolute)
        return [by_volume[volume] for volume in sorted(by_volume)]

    def _process_volume_page(
        self, soup: BeautifulSoup, page_url: str
    ) -> Iterable[DiscoveryResult]:
        volume = self._volume_number(page_url)

        for link in soup.select("a[href]"):
            raw_url = urljoin(page_url, str(link.get("href") or ""))
            if not self._is_article_download(raw_url):
                continue

            pdf_url = raw_url
            if not urlparse(raw_url).path.lower().endswith(".pdf"):
                pdf_url = self._resolve_possible_pdf_url(raw_url)
                if not pdf_url:
                    continue

            metadata = self._metadata_for_link(link, raw_url)
            if volume:
                metadata.setdefault("volume", volume)
            metadata["url"] = pdf_url
            metadata["source_url"] = page_url
            self._generate_citation(metadata)
            yield DiscoveryResult(page_url=page_url, pdf_url=pdf_url, metadata=metadata)

    def _is_article_download(self, url: str) -> bool:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        if host in {"vjolt.org", "www.vjolt.org"} and path.startswith("/s/"):
            return True
        if host.endswith("box.com") and (
            "/shared/static/" in path or path.startswith("/s/")
        ):
            return True
        return "static1.squarespace.com" in host and "/static/" in path

    def _metadata_for_link(self, link: Tag, pdf_url: str) -> dict:
        metadata: dict = {}
        text = " ".join(link.get_text(" ", strip=True).split())

        menu_item = link.find_parent(class_="menu-item-title")
        if menu_item:
            self._apply_title_author_text(metadata, menu_item.get_text(" ", strip=True))

        image_card = link.find_parent(class_="image-card")
        if image_card:
            issue = image_card.select_one("h4")
            if issue:
                number = self._extract_issue_number(issue.get_text(" ", strip=True))
                if number:
                    metadata["issue"] = number
            heading = image_card.select_one("h3")
            if heading:
                self._apply_title_author_text(metadata, heading.get_text(" ", strip=True))

        list_item = link.find_parent(class_="list-item-content")
        if list_item:
            heading = list_item.select_one("h2, h3, h4")
            if heading:
                metadata["title"] = heading.get_text(" ", strip=True)
            description = list_item.select_one(".list-item-content__description")
            if description:
                first_paragraph = description.select_one("p")
                author_text = (
                    first_paragraph.get_text(" ", strip=True)
                    if first_paragraph
                    else description.get_text(" ", strip=True)
                )
                if author_text:
                    metadata["authors"] = [author_text]

        fluid = link.find_parent(class_=lambda value: value and "fluid-engine" in value)
        if fluid:
            heading = link.find_parent(["h1", "h2", "h3", "h4", "h5", "h6"])
            if heading:
                metadata["title"] = heading.get_text(" ", strip=True)
            if not metadata.get("title"):
                headings = fluid.select("h1, h2, h3, h4, h5, h6")
                if len(headings) == 1:
                    metadata["title"] = headings[0].get_text(" ", strip=True)
            if text.lower() in self._GENERIC_LINK_TEXT:
                preceding = []
                for element in link.find_all_previous(["p", "h2", "h3", "h4"]):
                    if element not in fluid.descendants:
                        continue
                    value = " ".join(element.get_text(" ", strip=True).split())
                    if value:
                        preceding.append(value)
                # In modern button layouts the nearest fields are affiliation,
                # author, then title.  This preserves the page's article-level
                # labels instead of falling back to a filename.
                if len(preceding) >= 3:
                    metadata["authors"] = self._extract_authors_from_text(preceding[1])
                    metadata["title"] = preceding[2]
            for paragraph in fluid.select("p"):
                paragraph_text = paragraph.get_text(" ", strip=True)
                if re.match(r"^by\s+", paragraph_text, re.I):
                    metadata["authors"] = self._extract_authors_from_text(paragraph_text)
                    break
            year = self._extract_year(fluid.get_text(" ", strip=True))
            if year:
                metadata["date"] = year

        if text.lower() not in self._GENERIC_LINK_TEXT and len(text) > 8:
            metadata.setdefault("title", text)
        metadata.setdefault("title", self._title_from_download_url(pdf_url))
        return metadata

    def _apply_title_author_text(self, metadata: dict, raw_text: str) -> None:
        text = " ".join(raw_text.split())
        if " - " in text:
            title, author = text.rsplit(" - ", 1)
            if title:
                metadata["title"] = title.strip()
            if author:
                metadata["authors"] = [author.strip()]
            return

        # Volume 25 cards use "Author, Article title" in their H3.
        if ", " in text:
            author, title = text.split(", ", 1)
            if title:
                metadata["title"] = title.strip()
            if author:
                metadata["authors"] = [author.strip()]

    def _title_from_download_url(self, url: str) -> str:
        stem = PurePosixPath(unquote(urlparse(url).path)).name
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        stem = re.sub(r"^[vV]?\d+(?:[-_. ]*(?:Va[-_. ]*JL[-_. ]*Tech)?)?[-_. ]*", "", stem)
        stem = re.sub(r"[-_. ]*(?:final|version)(?:[-_. ].*)?$", "", stem, flags=re.I)
        return " ".join(re.sub(r"[-_+.]+", " ", stem).split()) or "Virginia JOLT article"

    def _volume_number(self, url: str) -> str:
        match = self._VOLUME_RE.search(urlparse(url).path)
        return match.group(1) if match else ""

    def _resolve_possible_pdf_url(self, url: str) -> str:
        """Resolve extensionless Squarespace downloads and direct Box PDFs."""
        try:
            response = self.session.get(
                url,
                headers=DEFAULT_HEADERS,
                timeout=30,
                stream=True,
                allow_redirects=True,
            )
            final_url = str(response.url or url)
            content_type = str(response.headers.get("Content-Type") or "").lower()
            status_code = response.status_code
            response.close()
            if status_code < 400 and "application/pdf" in content_type:
                return final_url
            if status_code < 400 and "static1.squarespace.com" in final_url.lower():
                return final_url
        except Exception:
            return ""
        return ""
