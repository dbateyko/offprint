from __future__ import annotations

import copy
import re
from typing import List
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .wordpress_academic_base import WordPressAcademicBaseAdapter


class GeorgetownJNSLPAdapter(WordPressAcademicBaseAdapter):
    """Host-specific adapter for Georgetown's Journal of National Security Law & Policy."""

    JOURNAL_HOST = "nationalsecurity.law.georgetown.edu"
    JOURNAL_PAGE_RE = re.compile(r"^/journal/page/\d+/?$", re.I)
    JOURNAL_ARTICLE_RE = re.compile(r"^/journal/\d{4}/\d{2}/\d{2}/[^/]+/?$", re.I)

    WORDPRESS_SELECTORS = copy.deepcopy(WordPressAcademicBaseAdapter.WORDPRESS_SELECTORS)
    # This theme uses card overlays for article links on archive pages.
    WORDPRESS_SELECTORS["article_links"] = [
        'a.card--link[href*="/journal/"]',
        *WORDPRESS_SELECTORS["article_links"],
    ]

    def _is_valid_volume_issue_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            host = (parsed.netloc or "").lower()
            path = (parsed.path or "").lower()
            if host == self.JOURNAL_HOST and self.JOURNAL_PAGE_RE.match(path):
                return True
        except Exception:
            return False
        return super()._is_valid_volume_issue_url(url)

    def _is_valid_article_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            host = (parsed.netloc or "").lower()
            path = (parsed.path or "").lower()
            if host == self.JOURNAL_HOST and self.JOURNAL_ARTICLE_RE.match(path):
                return True
        except Exception:
            return False
        return super()._is_valid_article_url(url)

    def _find_volume_issue_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links = super()._find_volume_issue_links(soup, base_url)

        for selector in ("a.page-numbers[href]", 'link[rel="next"][href]', 'link[rel="prev"][href]'):
            try:
                for elem in soup.select(selector):
                    href = (elem.get("href") or "").strip()
                    if not href:
                        continue
                    full_url = urljoin(base_url, href)
                    if self._is_valid_volume_issue_url(full_url):
                        links.append(full_url)
            except Exception:
                continue

        deduped: List[str] = []
        seen = set()
        for link in links:
            if link in seen:
                continue
            seen.add(link)
            deduped.append(link)
        return deduped
