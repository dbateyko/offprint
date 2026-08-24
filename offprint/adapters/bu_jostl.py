from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase


class BostonUniversityJOSTLAdapter(SiteArchiveAdapterBase):
    """Archive adapter for BU's Journal of Science & Technology Law."""

    ISSUE_TEXT_RE = re.compile(
        r"\bVol(?:ume)?\.?\s*(?P<volume>\d+)"
        r"(?:\s*[.-]\s*(?P<issue>\d+))?.*?\b(?P<year>(?:19|20)\d{2})\b",
        re.IGNORECASE,
    )
    DRIVE_FILE_RE = re.compile(r"drive\.google\.com/file/d/([^/?#]+)", re.IGNORECASE)

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        seed_soup = self._get(seed_url)
        if not seed_soup:
            return

        issue_pages = self._issue_pages(seed_soup, seed_url)
        if not issue_pages:
            issue_pages = [(self._page_heading(seed_soup), seed_url)]

        seen: set[str] = set()
        for issue_label, issue_url in issue_pages:
            soup = seed_soup if issue_url == seed_url else self._get(issue_url)
            if not soup:
                continue
            issue_text = self._page_heading(soup) or issue_label
            issue_metadata = self._issue_metadata(issue_text)
            for element, raw_url in self._pdf_elements(soup):
                pdf_url = self._direct_pdf_url(urljoin(issue_url, raw_url))
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                metadata = dict(issue_metadata)
                if element.name == "a":
                    metadata["title"] = self._article_title(element) or self._filename_title(pdf_url)
                    authors = self._article_authors(element)
                    if authors:
                        metadata["authors"] = authors
                else:
                    metadata["title"] = self._issue_title(metadata, issue_text)
                    metadata["document_type"] = "issue_compilation"
                metadata["source_url"] = issue_url
                metadata["url"] = issue_url
                yield DiscoveryResult(
                    page_url=issue_url,
                    pdf_url=pdf_url,
                    metadata=metadata,
                    source_adapter=type(self).__name__,
                    extraction_path="archive_issue_page",
                )

    @classmethod
    def _issue_pages(cls, soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for anchor in soup.select("article a[href], .bu_collapsible_section a[href]"):
            label = " ".join(anchor.get_text(" ", strip=True).split())
            if not cls.ISSUE_TEXT_RE.search(label):
                continue
            issue_url = urljoin(base_url, anchor.get("href", "")).split("#", 1)[0]
            if "/jostl/" not in urlparse(issue_url).path or issue_url in seen:
                continue
            seen.add(issue_url)
            out.append((label, issue_url))
        return out

    @staticmethod
    def _page_heading(soup: BeautifulSoup) -> str:
        heading = soup.select_one("article h1, main h1, h1")
        return " ".join(heading.get_text(" ", strip=True).split()) if heading else ""

    @classmethod
    def _issue_metadata(cls, text: str) -> dict[str, str]:
        match = cls.ISSUE_TEXT_RE.search(text or "")
        if not match:
            return {}
        metadata = {"volume": match.group("volume"), "date": match.group("year")}
        if match.group("issue"):
            metadata["issue"] = match.group("issue")
        return metadata

    @staticmethod
    def _pdf_elements(soup: BeautifulSoup) -> list[tuple[Tag, str]]:
        out: list[tuple[Tag, str]] = []
        for selector, attribute in (
            ("a[href]", "href"),
            ("iframe[src]", "src"),
            ("embed[src]", "src"),
            ("object[data]", "data"),
        ):
            for element in soup.select(selector):
                value = (element.get(attribute) or "").strip()
                lowered = value.lower()
                if re.search(r"\.pdf(?:[?#]|$)", lowered) or "drive.google.com/file/d/" in lowered:
                    out.append((element, value))
        # A malformed BU issue page places an empty duplicate link before the
        # real titled link. Prefer metadata-bearing anchors so URL dedupe keeps
        # the useful element.
        return sorted(
            out,
            key=lambda item: (
                item[0].name != "a",
                not bool(item[0].get_text(" ", strip=True)),
            ),
        )

    @classmethod
    def _direct_pdf_url(cls, url: str) -> str:
        match = cls.DRIVE_FILE_RE.search(url)
        if match:
            return f"https://drive.usercontent.google.com/download?id={match.group(1)}&export=download"
        parsed = urlparse(url)
        if parsed.netloc == "drive.google.com" and parsed.path == "/uc":
            file_id = parse_qs(parsed.query).get("id", [""])[0]
            if file_id:
                return f"https://drive.usercontent.google.com/download?id={file_id}&export=download"
        return url

    @staticmethod
    def _article_title(anchor: Tag) -> str:
        title = " ".join(anchor.get_text(" ", strip=True).split())
        return re.sub(r"^\s*\d+[.)]\s*", "", title).strip()

    @staticmethod
    def _filename_title(pdf_url: str) -> str:
        filename = urlparse(pdf_url).path.rsplit("/", 1)[-1]
        stem = re.sub(r"(?i)\.pdf$", "", filename)
        stem = re.sub(r"(?i)(?:[-_](?:online|version|web|final))+$", "", stem)
        stem = re.sub(r"^\d+[.-]?", "", stem)
        return " ".join(stem.replace("_", " ").replace("-", " ").split())

    @staticmethod
    def _article_authors(anchor: Tag) -> list[str]:
        paragraph = anchor.find_parent("p")
        if paragraph:
            paragraph_text = " ".join(paragraph.get_text(" ", strip=True).split())
            by_match = re.search(r"\bby\s+(.+)$", paragraph_text, re.IGNORECASE)
            if by_match:
                return [by_match.group(1).strip()]

            sibling = paragraph.find_next_sibling()
            while isinstance(sibling, Tag):
                if sibling.select_one("a[href*='.pdf']"):
                    break
                candidate = " ".join(sibling.get_text(" ", strip=True).split())
                if candidate and candidate.upper() == candidate and candidate not in {"ARTICLES", "NOTES"}:
                    return [candidate]
                sibling = sibling.find_next_sibling()
        return []

    @staticmethod
    def _issue_title(metadata: dict[str, str], fallback: str) -> str:
        volume = metadata.get("volume")
        issue = metadata.get("issue")
        if volume:
            suffix = f" Issue {issue}" if issue else ""
            return f"Boston University Journal of Science & Technology Law Volume {volume}{suffix}"
        return fallback or "Boston University Journal of Science & Technology Law issue"
