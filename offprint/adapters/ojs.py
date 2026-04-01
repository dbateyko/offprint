from __future__ import annotations

from collections import deque
import os
import re
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS

OJS_NAV_HINTS = ("/issue/", "/article/")
PDF_HINTS = (".pdf", "/pdf", "galley", "pdf-download-button")
OJS_INSECURE_TLS_HOSTS = {"tlp.law.pitt.edu"}


class OJSAdapter(Adapter):
    """Adapter for OJS-based journals.

    Inventory-grade approach:
    - Prefer starting from `/issue/archive` (canonicalized seed).
    - Enumerate all issue links from archive (including pagination when present).
    - For each issue, enumerate article pages, then discover galley/PDF links.
    - Treat anchors containing PDF_HINTS as candidate PDF links; accept if HEAD shows application/pdf.
    """
    def __init__(self, session=None):
        super().__init__(session=session)
        value = str(os.getenv("LRS_ADAPTER_PLAYWRIGHT_FALLBACK", "1")).strip().lower()
        self.enable_playwright_fallback = value not in {"0", "false", "no", "off"}

    def _get(self, url: str) -> Optional[requests.Response]:
        verify_tls = self._verify_tls(url)
        try:
            # First attempt with standard requests
            r = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20, verify=verify_tls)
            if r.status_code < 400:
                return r
            
            # If forbidden (403), try Playwright fallback
            if r.status_code == 403 and self.enable_playwright_fallback:
                from ..playwright_session import PlaywrightSession, PlaywrightResponse
                with PlaywrightSession(headless=True) as pw:
                    # In some OJS sites (like Tulane), the /article/view/ URL
                    # immediately triggers a download instead of showing HTML.
                    # We catch the download event and return its URL.
                    pdf_urls = []
                    def handle_download(dl):
                        pdf_urls.append(dl.url)

                    pw._ensure_browser()
                    pw._page.on("download", handle_download)
                    resp = pw.get(url, timeout=30)
                    
                    if pdf_urls:
                        # Return a mock response that identifies as PDF
                        return PlaywrightResponse(
                            status_code=200,
                            text=f'<html><body><a href="{pdf_urls[0]}">PDF</a></body></html>',
                            content=b"",
                            headers={"Content-Type": "application/pdf"},
                            url=pdf_urls[0]
                        )
                    
                    if resp and resp.status_code < 400:
                        return resp
            return r
        except Exception:
            # Final attempt with Playwright on any error
            if self.enable_playwright_fallback:
                try:
                    from ..playwright_session import PlaywrightSession, PlaywrightResponse
                    with PlaywrightSession(headless=True) as pw:
                        pdf_urls = []
                        def handle_download(dl):
                            pdf_urls.append(dl.url)
                        pw._ensure_browser()
                        pw._page.on("download", handle_download)
                        resp = pw.get(url, timeout=30)
                        if pdf_urls:
                            return PlaywrightResponse(
                                status_code=200,
                                text=f'<html><body><a href="{pdf_urls[0]}">PDF</a></body></html>',
                                content=b"",
                                headers={"Content-Type": "application/pdf"},
                                url=pdf_urls[0]
                            )
                        if resp and resp.status_code < 400:
                            return resp
                except Exception:
                    pass
            return None

    def _head_is_pdf(self, url: str) -> bool:
        verify_tls = self._verify_tls(url)
        try:
            r = self.session.head(
                url,
                headers=DEFAULT_HEADERS,
                timeout=15,
                allow_redirects=True,
                verify=verify_tls,
            )
            ctype = r.headers.get("Content-Type", "").lower()
            return r.status_code < 400 and ("application/pdf" in ctype)
        except requests.RequestException:
            return False

    def _verify_tls(self, url: str) -> bool:
        host = (urlparse(url).hostname or "").lower()
        return host not in OJS_INSECURE_TLS_HOSTS

    def _iter_links(self, html: str, base_url: str) -> Iterable[Tuple[str, str]]:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            yield a.get_text(strip=True).lower(), urljoin(
                base_url, a["href"]
            )  # (text, absolute url)

    def _clean_metadata_value(self, value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _extract_issue_metadata(self, issue_html: str, issue_url: str) -> Dict[str, Any]:
        soup = BeautifulSoup(issue_html, "lxml")
        metadata: Dict[str, Any] = {"source_url": issue_url}

        heading = soup.select_one("h1, h2, .page-header h1, .issue h2")
        heading_text = heading.get_text(" ", strip=True) if heading else ""
        if heading_text:
            metadata["issue_title"] = heading_text

        context_text = " ".join([heading_text, issue_url]).strip()
        if context_text:
            volume_match = re.search(r"(?:vol(?:ume)?\.?\s*)(\d+)", context_text, re.IGNORECASE)
            if volume_match:
                metadata["volume"] = volume_match.group(1)

            issue_match = re.search(r"(?:no\.?|issue)\s*(\d+)", context_text, re.IGNORECASE)
            if issue_match:
                metadata["issue"] = issue_match.group(1)

            year_match = re.search(r"\b(19|20)\d{2}\b", context_text)
            if year_match:
                metadata["year"] = year_match.group(0)

        return metadata

    def _extract_article_metadata(
        self, article_html: str, article_url: str, issue_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        soup = BeautifulSoup(article_html, "lxml")
        metadata: Dict[str, Any] = dict(issue_metadata)
        metadata["url"] = article_url

        # OJS consistently renders citation_* meta tags on article pages.
        meta_values: Dict[str, List[str]] = {}
        for meta in soup.find_all("meta"):
            key = self._clean_metadata_value(meta.get("name") or meta.get("property")).lower()
            value = self._clean_metadata_value(meta.get("content"))
            if not key or not value:
                continue
            meta_values.setdefault(key, []).append(value)

        def first(*keys: str) -> str:
            for key in keys:
                values = meta_values.get(key.lower(), [])
                if values:
                    return values[0]
            return ""

        title = first("citation_title", "dc.title")
        if title:
            metadata["title"] = title

        authors = [
            self._clean_metadata_value(author)
            for author in meta_values.get("citation_author", [])
            if self._clean_metadata_value(author)
        ]
        if authors:
            metadata["authors"] = authors

        volume = first("citation_volume")
        if volume:
            metadata["volume"] = volume

        issue = first("citation_issue")
        if issue:
            metadata["issue"] = issue

        date_value = first("citation_publication_date", "citation_date", "dc.date")
        if date_value:
            metadata["date"] = date_value
            year_match = re.search(r"\b(19|20)\d{2}\b", date_value)
            if year_match:
                metadata["year"] = year_match.group(0)

        first_page = first("citation_firstpage")
        last_page = first("citation_lastpage")
        if first_page and last_page:
            metadata["pages"] = f"{first_page}-{last_page}"
        elif first_page:
            metadata["pages"] = first_page

        doi = first("citation_doi")
        if doi:
            metadata["doi"] = doi

        citation_url = first(
            "citation_abstract_html_url",
            "citation_fulltext_html_url",
            "citation_public_url",
            "dc.identifier.uri",
        )
        if citation_url:
            metadata["url"] = citation_url

        # Build a concise citation when enough components are present.
        if not metadata.get("citation"):
            citation_parts: List[str] = []
            if metadata.get("title"):
                citation_parts.append(str(metadata["title"]))
            vol = self._clean_metadata_value(metadata.get("volume"))
            iss = self._clean_metadata_value(metadata.get("issue"))
            if vol and iss:
                citation_parts.append(f"Vol. {vol}, No. {iss}")
            elif vol:
                citation_parts.append(f"Vol. {vol}")
            year = self._clean_metadata_value(metadata.get("year"))
            if year:
                citation_parts.append(f"({year})")
            if citation_parts:
                metadata["citation"] = " ".join(citation_parts)

        return metadata

    def download_pdf(
        self,
        pdf_url: str,
        out_dir: str,
        referer: str = "",
        **kwargs: Any,
    ) -> Optional[str]:
        """Download PDF using the OJS-specific _get method for Playwright fallback."""
        # Some OJS sites (Tulane, Utah) require a browser to trigger the actual PDF download
        # from an /article/view/ or /article/download/ URL.
        resp = self._get(pdf_url)
        if resp and "application/pdf" in resp.headers.get("Content-Type", "").lower():
            # If _get returned a mock PlaywrightResponse, it might already have the data
            # or the URL might be updated.
            final_url = resp.url if hasattr(resp, "url") else pdf_url
            return self._download_with_generic(final_url, out_dir, referer=referer, **kwargs)

    def _citation_pdf_url(self, article_html: str, article_url: str) -> str:
        soup = BeautifulSoup(article_html, "lxml")
        for meta in soup.find_all("meta"):
            key = self._clean_metadata_value(meta.get("name") or meta.get("property")).lower()
            if key != "citation_pdf_url":
                continue
            value = self._clean_metadata_value(meta.get("content"))
            if value:
                return urljoin(article_url, value)
        return ""

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        seed_url = seed_url.rstrip("/")
        seed_origin = urlparse(seed_url).netloc

        issue_urls: List[str] = []
        for candidate in self._candidate_archive_urls(seed_url):
            archive_pages = self._collect_archive_pages(candidate, seed_origin)
            issue_urls = self._extract_issue_urls(archive_pages, seed_origin)
            if issue_urls:
                break

        seen_pdfs: Set[str] = set()
        for issue_url in issue_urls:
            yield from self._process_issue(issue_url, seed_origin, seen_pdfs)

    def _candidate_archive_urls(self, seed_url: str) -> List[str]:
        parsed = urlparse(seed_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path.rstrip("/")

        candidates: List[str] = [seed_url]
        lowered = seed_url.lower()
        if "/issue/archive" in lowered:
            # Some OJS deployments localize archive paths under /en/issue/archive.
            if "/en/" not in lowered:
                if path:
                    candidates.append(f"{base}{path.replace('/issue/archive', '/en/issue/archive')}")
                candidates.append(f"{base}/en/issue/archive")
            # Continue through dedupe logic instead of returning early.

        if path:
            candidates.append(f"{base}{path}/issue/archive")
            candidates.append(f"{base}{path}/en/issue/archive")
        else:
            candidates.append(f"{base}/issue/archive")
            candidates.append(f"{base}/en/issue/archive")

        if "/index.php/" in path.lower():
            suffix = path.split("/index.php/", 1)[1].strip("/")
            journal_slug = suffix.split("/", 1)[0] if suffix else ""
            if journal_slug:
                candidates.append(f"{base}/index.php/{journal_slug}/issue/archive")

        deduped: List[str] = []
        seen: Set[str] = set()
        for candidate in candidates:
            normalized = candidate.rstrip("/")
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
            if len(deduped) >= 5:
                break
        return deduped

    def _collect_archive_pages(
        self, archive_url: str, origin: str, max_pages: int = 50
    ) -> List[Tuple[str, str]]:
        """Return [(url, html)] for archive pages, following archive pagination links deterministically."""
        visited: Set[str] = set()
        pages: List[Tuple[str, str]] = []
        queue: Deque[str] = deque([archive_url])

        while queue and len(pages) < max_pages:
            url = queue.popleft()
            if url in visited:
                continue
            visited.add(url)

            resp = self._get(url)
            if not resp or not resp.text:
                continue

            html = resp.text
            pages.append((url, html))

            soup = BeautifulSoup(html, "lxml")
            next_links: List[str] = []
            for a in soup.select(
                "a[rel='next'], a.next, a.nextPage, a.page-numbers.next, li.next a"
            ):
                href = a.get("href")
                if not href:
                    continue
                next_links.append(urljoin(url, href))

            # Only follow pagination within the same journal and still on issue/archive
            for nxt in sorted(set(next_links)):
                parsed = urlparse(nxt)
                if (
                    parsed.scheme in {"http", "https"}
                    and parsed.netloc == origin
                    and "issue/archive" in nxt.lower()
                ):
                    if nxt not in visited:
                        queue.append(nxt)

        return pages

    def _extract_issue_urls(self, archive_pages: List[Tuple[str, str]], origin: str) -> List[str]:
        issue_urls: Set[str] = set()
        for base_url, html in archive_pages:
            for _text, link in self._iter_links(html, base_url):
                parsed = urlparse(link)
                if parsed.scheme not in {"http", "https"} or parsed.netloc != origin:
                    continue
                link_lower = link.lower()
                if (
                    "/issue/view" in link_lower
                    or "/issue/current" in link_lower
                    or ("/volume/" in link_lower and "/issue/" in link_lower)
                ):
                    issue_urls.add(link)
        return sorted(issue_urls)

    def _process_issue(
        self, issue_url: str, origin: str, seen_pdfs: Set[str]
    ) -> Iterable[DiscoveryResult]:
        resp = self._get(issue_url)
        if not resp or not resp.text:
            return

        html = resp.text
        issue_metadata = self._extract_issue_metadata(html, issue_url)
        article_urls: Set[str] = set()

        issue_pdf_links: List[str] = []
        for _text, link in self._iter_links(html, issue_url):
            parsed = urlparse(link)
            if parsed.scheme not in {"http", "https"} or parsed.netloc != origin:
                continue

            link_lower = link.lower()
            if "/article/view" in link_lower:
                article_urls.add(link)
                continue

            # Some OJS deployments publish article landing pages as /articles/<doi>.
            if "/articles/" in link_lower and "/files/" not in link_lower:
                article_urls.add(link)
                continue

            if any(h in link_lower for h in PDF_HINTS):
                if link_lower.endswith(".pdf") or self._head_is_pdf(link):
                    issue_pdf_links.append(link)

        # Prefer article-page extraction first so we can attach richer metadata.
        for article_url in sorted(article_urls):
            yield from self._process_article(article_url, seen_pdfs, issue_metadata)

        for link in issue_pdf_links:
            if link not in seen_pdfs:
                seen_pdfs.add(link)
                yield DiscoveryResult(page_url=issue_url, pdf_url=link, metadata=issue_metadata)

    def _process_article(
        self, article_url: str, seen_pdfs: Set[str], issue_metadata: Dict[str, Any]
    ) -> Iterable[DiscoveryResult]:
        resp = self._get(article_url)
        if not resp or not resp.text:
            return

        article_metadata = self._extract_article_metadata(resp.text, article_url, issue_metadata)

        citation_pdf = self._citation_pdf_url(resp.text, article_url)
        if citation_pdf and citation_pdf not in seen_pdfs:
            if citation_pdf.lower().endswith(".pdf") or self._head_is_pdf(citation_pdf):
                seen_pdfs.add(citation_pdf)
                yield DiscoveryResult(
                    page_url=article_url,
                    pdf_url=citation_pdf,
                    metadata=dict(article_metadata),
                )

        for _text, link in self._iter_links(resp.text, article_url):
            link_lower = link.lower()
            
            # Numeric ID patterns common in OJS: /article/view/123/456 or /article/download/123/456
            is_numeric_pdf = bool(re.search(r"/article/(?:view|download)/\d+/\d+", link_lower))
            
            if not (any(h in link_lower for h in PDF_HINTS) or is_numeric_pdf):
                continue

            if link_lower.endswith(".pdf") or is_numeric_pdf or self._head_is_pdf(link):
                if link not in seen_pdfs:
                    seen_pdfs.add(link)
                    yield DiscoveryResult(page_url=article_url, pdf_url=link, metadata=dict(article_metadata))
