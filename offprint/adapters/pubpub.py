from __future__ import annotations

import html
import json
from collections.abc import Iterable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


class PubPubAdapter(Adapter):
    """Adapter for PubPub-hosted journals (pubpub.org)."""

    def _get(self, url: str):
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=25)
            if resp.status_code < 400 and resp.text:
                return resp
        except Exception:
            return None
        return None

    def _extract_pub_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        host = (urlparse(base_url).netloc or "").lower()
        links: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(base_url, href)
            parsed = urlparse(full)
            if parsed.scheme not in {"http", "https"}:
                continue
            if (parsed.netloc or "").lower() != host:
                continue
            path = parsed.path or ""
            if not path.startswith("/pub/"):
                continue
            # Canonicalize to /pub/<slug>
            slug = path.split("/")[2] if len(path.split("/")) > 2 else ""
            if not slug:
                continue
            normalized = f"{parsed.scheme}://{parsed.netloc}/pub/{slug}"
            if normalized in seen:
                continue
            seen.add(normalized)
            links.append(normalized)
        return links

    def _extract_pdf_links(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()

        def add(candidate: str) -> None:
            parsed = urlparse(candidate)
            if parsed.scheme not in {"http", "https"}:
                return
            if ".pdf" not in (parsed.path or "").lower():
                return
            # Drop obviously corrupted matches.
            if any(token in candidate for token in ('"&quot;', "{", "}", "\n", "\r")):
                return
            if len(candidate) > 400:
                return
            if candidate in seen:
                return
            seen.add(candidate)
            found.append(candidate)

        # Direct anchors are the cleanest signal on /pub/* pages.
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(page_url, href)
            add(full)

        # PubPub stores rich page state in script[data-json] payloads.
        for script in soup.select("script[data-json]"):
            raw = script.get("data-json") or ""
            if not raw:
                continue
            decoded = html.unescape(raw)
            try:
                payload = json.loads(decoded)
            except Exception:
                continue

            stack: list[object] = [payload]
            while stack:
                cur = stack.pop()
                if isinstance(cur, dict):
                    if str(cur.get("format") or "").lower() == "pdf":
                        url_value = str(cur.get("url") or "").strip()
                        if url_value:
                            add(url_value)
                    for value in cur.values():
                        if isinstance(value, (dict, list)):
                            stack.append(value)
                        elif isinstance(value, str) and value.startswith("http"):
                            add(value.strip())
                elif isinstance(cur, list):
                    stack.extend(cur)

        return found

    def _extract_metadata(self, soup: BeautifulSoup, page_url: str) -> dict:
        metadata = {"source_url": page_url, "url": page_url}
        title = ""
        title_meta = soup.find("meta", attrs={"name": "citation_title"})
        if title_meta and title_meta.get("content"):
            title = str(title_meta["content"]).strip()
        if not title:
            og_title = soup.find("meta", attrs={"property": "og:title"})
            if og_title and og_title.get("content"):
                title = str(og_title["content"]).strip()
        if not title and soup.title:
            title = soup.title.get_text(" ", strip=True)
        if title:
            metadata["title"] = title
        return metadata

    def discover_pdfs(
        self, seed_url: str, max_depth: int = 0
    ) -> Iterable[DiscoveryResult]:
        start = self._get(seed_url)
        if not start:
            return

        start_soup = BeautifulSoup(start.text, "lxml")
        pub_links = self._extract_pub_links(start_soup, seed_url)
        if not pub_links and "/pub/" in (urlparse(seed_url).path or ""):
            pub_links = [seed_url]

        # Keep smoke runs fast; expand when depth is explicitly requested.
        max_pubs = 25 if max_depth <= 0 else min(250, 25 + (max_depth * 75))
        pub_links = pub_links[:max_pubs]

        seen_pdf_urls: set[str] = set()
        for pub_url in pub_links:
            pub_resp = self._get(pub_url)
            if not pub_resp:
                continue
            soup = BeautifulSoup(pub_resp.text, "lxml")
            metadata = self._extract_metadata(soup, pub_url)
            for pdf_url in self._extract_pdf_links(soup, pub_url):
                if pdf_url in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(pdf_url)
                yield DiscoveryResult(page_url=pub_url, pdf_url=pdf_url, metadata=metadata)

