from __future__ import annotations

import re
from collections import deque
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .quartex import QuartexAdapter
from .wordpress_academic_base import WordPressAcademicBaseAdapter


class PennLawReviewAdapter(WordPressAcademicBaseAdapter):
    """Adapter for Penn Law Review's print/results index."""

    RESULTS_URL = "https://pennlawreview.com/print/results"
    MAX_RESULTS_PAGES = 200

    def discover_pdfs(self, start_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        del max_depth
        queue: deque[str] = deque([self._results_url_for_seed(start_url)])
        seen_pages: set[str] = set()
        seen_pdfs: set[str] = set()
        pages_scanned = 0

        while queue and pages_scanned < self.MAX_RESULTS_PAGES:
            page_url = queue.popleft()
            if page_url in seen_pages:
                continue
            seen_pages.add(page_url)
            pages_scanned += 1

            response = self._request_with_retry(page_url, timeout=20, max_attempts=3)
            if response is None or int(response.status_code or 0) >= 400:
                continue
            soup = BeautifulSoup(response.text, "lxml")

            for card in soup.select("div.article-card"):
                extracted = self._extract_card(card, page_url)
                raw_pdf_url = extracted.get("raw_pdf_url") or ""
                if not raw_pdf_url:
                    continue

                article_url = str(extracted.get("article_url") or page_url)
                metadata = dict(extracted.get("metadata") or {})
                metadata.setdefault("source_url", article_url)
                metadata.setdefault("url", article_url)
                metadata.setdefault("journal", "University of Pennsylvania Law Review")
                metadata.setdefault("platform", "WordPress print results")
                metadata.setdefault("extraction_method", "penn_law_review")
                metadata.setdefault("raw_pdf_url", raw_pdf_url)

                for resolved in self._resolve_pdf_candidates(raw_pdf_url, article_url):
                    if not resolved or resolved in seen_pdfs:
                        continue
                    seen_pdfs.add(resolved)
                    yield DiscoveryResult(page_url=article_url, pdf_url=resolved, metadata=metadata)

            older_link = soup.select_one("div.nav-previous a[href]")
            if not older_link:
                continue
            older_url = urljoin(page_url, older_link.get("href", ""))
            if older_url and older_url not in seen_pages and self._is_results_page(older_url):
                queue.append(older_url)

    def _results_url_for_seed(self, seed_url: str) -> str:
        parsed = urlparse(seed_url)
        if self._is_results_page(seed_url):
            return seed_url

        scheme = parsed.scheme or "https"
        host = (parsed.netloc or "pennlawreview.com").lower()
        if host.startswith("www."):
            host = host[4:]
        return f"{scheme}://{host}/print/results"

    def _is_results_page(self, url: str) -> bool:
        path = (urlparse(url).path or "").lower().rstrip("/")
        return path.endswith("/print/results") or path == "/print/results"

    def _extract_card(self, card: BeautifulSoup, page_url: str) -> Dict[str, object]:
        article_link = card.select_one("a.article-link[href]")
        article_url = urljoin(page_url, article_link.get("href", "")) if article_link else page_url

        title = ""
        title_el = card.select_one("a.article-link h2")
        if title_el:
            title = title_el.get_text(" ", strip=True)
        elif article_link:
            title = article_link.get_text(" ", strip=True)

        author_text = ""
        author_el = card.select_one(".article-author")
        if author_el:
            author_text = author_el.get_text(" ", strip=True)
        authors = self._split_author_string(author_text) if author_text else []

        issue = ""
        year = ""
        data_spans = card.select(".article-data span")
        if data_spans:
            issue = data_spans[0].get_text(" ", strip=True)
        if len(data_spans) > 1:
            year = data_spans[1].get_text(" ", strip=True)

        pdf_link = card.select_one("div.view-pdf a[href]")
        raw_pdf_url = ""
        if pdf_link:
            raw_pdf_url = urljoin(page_url, pdf_link.get("href", ""))

        metadata: Dict[str, object] = {"title": title}
        if authors:
            metadata["authors"] = authors
        if issue:
            metadata["issue"] = issue
        if year:
            metadata["year"] = year
            metadata["date"] = year

        return {
            "article_url": article_url,
            "raw_pdf_url": raw_pdf_url,
            "metadata": metadata,
        }

    def _resolve_pdf_candidates(self, candidate_url: str, article_url: str) -> List[str]:
        if not candidate_url:
            return []
        normalized = candidate_url.replace("&#038;", "&")
        lowered = normalized.lower()

        if self._is_direct_pdf_target(normalized):
            return [normalized]

        if "doi.org/" in lowered:
            doi_target = self._resolve_doi_target(normalized)
            if not doi_target:
                return []
            return self._resolve_pdf_candidates(doi_target, article_url)

        if self._is_quartex_detail_page(normalized):
            detail_pdf = self._resolve_quartex_detail_pdf(normalized)
            return [detail_pdf] if detail_pdf else []

        discovered: List[str] = []
        response = self._request_with_retry(normalized, timeout=20, max_attempts=2)
        if response is None or int(response.status_code or 0) >= 400:
            return []
        landing_url = str(getattr(response, "url", "") or normalized)
        if self._is_direct_pdf_target(landing_url):
            discovered.append(landing_url)

        soup = BeautifulSoup(response.text, "lxml")
        for meta in soup.select('meta[name="citation_pdf_url"]'):
            href = meta.get("content")
            if not href:
                continue
            resolved = urljoin(landing_url, href)
            if self._is_direct_pdf_target(resolved):
                discovered.append(resolved)

        for link in soup.select("a[href]"):
            href = link.get("href") or ""
            if not href:
                continue
            resolved = urljoin(landing_url, href)
            if self._is_direct_pdf_target(resolved):
                discovered.append(resolved)
                continue
            text = link.get_text(" ", strip=True).lower()
            if self._is_quartex_detail_page(resolved) and "download" in text:
                detail_pdf = self._resolve_quartex_detail_pdf(resolved)
                if detail_pdf:
                    discovered.append(detail_pdf)

        # Keep order deterministic while removing duplicates.
        deduped: List[str] = []
        seen: set[str] = set()
        for item in discovered:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    def _resolve_doi_target(self, doi_url: str) -> Optional[str]:
        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        try:
            head_resp = self.session.head(
                doi_url,
                headers=headers,
                allow_redirects=False,
                timeout=20,
            )
            status = int(getattr(head_resp, "status_code", 0) or 0)
            location = str(getattr(head_resp, "headers", {}).get("Location") or "").strip()
            if 300 <= status < 400 and location:
                return urljoin(doi_url, location)
        except Exception:
            pass

        try:
            get_resp = self.session.get(doi_url, headers=headers, allow_redirects=True, timeout=20)
            if int(get_resp.status_code or 0) >= 400:
                return None
            return str(getattr(get_resp, "url", "") or doi_url)
        except Exception:
            return None

    def _resolve_quartex_detail_pdf(self, detail_url: str) -> Optional[str]:
        response = self._request_with_retry(detail_url, timeout=25, max_attempts=2)
        if response is None or int(response.status_code or 0) >= 400:
            return None

        quartex = QuartexAdapter(session=self.session)
        page_html = str(response.text or "")
        api_pdf = quartex._extract_quartex_full_pdf_url(page_html)
        if api_pdf:
            return api_pdf

        soup = BeautifulSoup(page_html, "lxml")
        for candidate in quartex._extract_pdf_links(soup, detail_url):
            if self._is_direct_pdf_target(candidate):
                return candidate
        return None

    def _is_quartex_detail_page(self, url: str) -> bool:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        return host == "repository.law.upenn.edu" and path.startswith("/documents/detail/")

    def _is_direct_pdf_target(self, url: str) -> bool:
        lowered = (url or "").lower()
        path = (urlparse(url).path or "").lower()
        if path.endswith(".pdf"):
            return True
        if "viewcontent.cgi" in lowered:
            return True
        if "frontend-api.quartexcollections.com" in lowered and "/download/" in lowered:
            return True
        if re.search(r"/server/api/core/bitstreams/[^/]+/content$", path):
            return True
        return False
