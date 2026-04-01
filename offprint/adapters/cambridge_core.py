from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


class CambridgeCoreAdapter(Adapter):
    """Adapter for Cambridge Core journal pages."""

    def _get(self, url: str):
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=25)
            if resp.status_code < 400 and resp.text:
                return resp
        except Exception:
            return None
        return None

    def _journal_prefix(self, seed_url: str) -> str:
        parsed = urlparse(seed_url)
        parts = [p for p in (parsed.path or "").split("/") if p]
        # /core/journals/<slug>/...
        if len(parts) >= 3 and parts[0] == "core" and parts[1] == "journals":
            return f"/core/journals/{parts[2]}"
        return ""

    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(base_url, href)
            if full in seen:
                continue
            seen.add(full)
            out.append(full)
        return out

    def _extract_metadata(self, soup: BeautifulSoup, article_url: str) -> dict:
        metadata: dict = {"source_url": article_url, "url": article_url}
        title_meta = soup.find("meta", attrs={"name": "citation_title"})
        if title_meta and title_meta.get("content"):
            metadata["title"] = str(title_meta["content"]).strip()
        elif soup.title:
            metadata["title"] = soup.title.get_text(" ", strip=True)

        authors = [
            str(m.get("content")).strip()
            for m in soup.find_all("meta", attrs={"name": "citation_author"})
            if m.get("content")
        ]
        if authors:
            metadata["authors"] = authors

        pub_date = soup.find("meta", attrs={"name": "citation_publication_date"})
        if pub_date and pub_date.get("content"):
            metadata["date"] = str(pub_date["content"]).strip()
        return metadata

    def discover_pdfs(
        self, seed_url: str, max_depth: int = 0
    ) -> Iterable[DiscoveryResult]:
        prefix = self._journal_prefix(seed_url)
        if not prefix:
            return

        seed_resp = self._get(seed_url)
        if not seed_resp:
            return
        seed_soup = BeautifulSoup(seed_resp.text, "lxml")
        seed_links = self._extract_links(seed_soup, seed_url)

        issue_urls = [
            u
            for u in seed_links
            if u.startswith(f"https://www.cambridge.org{prefix}/issue/")
            or u.startswith(f"http://www.cambridge.org{prefix}/issue/")
        ]
        if not issue_urls and f"{prefix}/issue/" in (urlparse(seed_url).path or ""):
            issue_urls = [seed_url]

        # Bound crawl size for smoke/default runs.
        max_issues = 2 if max_depth <= 0 else min(40, 4 + (max_depth * 8))
        issue_urls = list(dict.fromkeys(issue_urls))[:max_issues]

        seen_article_urls: set[str] = set()
        seen_pdf_urls: set[str] = set()
        for issue_url in issue_urls:
            issue_resp = self._get(issue_url)
            if not issue_resp:
                continue
            issue_soup = BeautifulSoup(issue_resp.text, "lxml")
            links = self._extract_links(issue_soup, issue_url)
            article_urls = [
                u
                for u in links
                if u.startswith(f"https://www.cambridge.org{prefix}/article/")
                or u.startswith(f"http://www.cambridge.org{prefix}/article/")
            ]

            for article_url in article_urls:
                if article_url in seen_article_urls:
                    continue
                seen_article_urls.add(article_url)
                article_resp = self._get(article_url)
                if not article_resp:
                    continue
                article_soup = BeautifulSoup(article_resp.text, "lxml")
                metadata = self._extract_metadata(article_soup, article_url)
                for link in self._extract_links(article_soup, article_url):
                    if (
                        "/core/services/aop-cambridge-core/content/view/" not in link
                        or ".pdf" not in link.lower()
                    ):
                        continue
                    if link in seen_pdf_urls:
                        continue
                    seen_pdf_urls.add(link)
                    yield DiscoveryResult(page_url=article_url, pdf_url=link, metadata=metadata)

