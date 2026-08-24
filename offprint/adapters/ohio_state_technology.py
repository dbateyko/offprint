from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple
from urllib.parse import unquote, urljoin, urlparse

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .dspace import DSpaceAdapter


class OhioStateTechnologyLawJournalAdapter(DSpaceAdapter):
    """DSpace adapter scoped to every OSTLJ issue collection.

    The legacy ``1811/72602`` handle resolves to a DSpace *community*, not a
    collection.  Walking its issue collections avoids both repository-wide
    fallback discovery and the 400-object ceiling in the generic DSpace path.
    """

    COMMUNITY_UUID = "a3767fe3-6fcd-5776-bbe7-44d144fb641a"
    MORITZ_ARTICLES_URL = (
        "https://moritzlaw.osu.edu/student-life/law-journals/"
        "ohio-state-technology-law-journal/ostlj-articles"
    )
    WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
    WAYBACK_SCOPE = "moritzlaw.osu.edu/students/groups/is/files/*"
    _ISSUE_RE = re.compile(
        r"Volume\s*(?P<volume>\d+)\s*,\s*Issue\s*(?P<issue>[\d-]+).*?"
        r"(?P<year>(?:19|20)\d{2})(?:\s*/\s*(?P<second_year>\d{4}))?",
        re.IGNORECASE,
    )
    _NON_ARTICLE_TITLES = re.compile(
        r"^(?:back matter|editorial board|front matter|masthead|table of contents)\b",
        re.IGNORECASE,
    )
    _NON_ARTICLE_FILES = re.compile(
        r"(?:^|[-_.])(?:cover|masthead|table[-_ ]?of[-_ ]?contents|toc|"
        r"recruiting[-_ ]?brochure|text)(?:[-_.]|$)|(?:journal|issue).*text",
        re.IGNORECASE,
    )

    def __init__(self, session=None):
        super().__init__(session=session)
        self._wayback_replay_by_pdf: Dict[str, str] = {}

    def _issue_metadata(self, collection_name: str) -> Dict[str, str]:
        match = self._ISSUE_RE.search(collection_name or "")
        if not match:
            return {}
        return {
            "volume": match.group("volume"),
            "issue": match.group("issue"),
            "year": match.group("second_year") or match.group("year"),
        }

    def _iter_issue_collections(self, seed_url: str) -> Iterator[Tuple[str, Dict[str, str]]]:
        base = "https://kb.osu.edu"
        collections_url = f"{base}/server/api/core/communities/{self.COMMUNITY_UUID}/collections"
        page = 0
        while True:
            payload = self._get_json(collections_url, params={"size": 100, "page": page})
            if not payload:
                return
            collections = (payload.get("_embedded") or {}).get("collections") or []
            if not isinstance(collections, list) or not collections:
                return
            for collection in collections:
                collection_id = str(collection.get("id") or collection.get("uuid") or "")
                if not collection_id:
                    continue
                yield collection_id, self._issue_metadata(str(collection.get("name") or ""))

            page_info: Dict[str, Any] = payload.get("page") or {}
            total_pages = int(page_info.get("totalPages") or 0)
            page += 1
            if total_pages and page >= total_pages:
                return

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        seen_pdf_urls: set[str] = set()
        consecutive_empty_collections = 0

        for collection_id, issue_metadata in self._iter_issue_collections(seed_url):
            collection_had_objects = False
            for result in self._iter_dspace_pdf_candidates(seed_url, scope_id=collection_id):
                collection_had_objects = True
                title = str((result.metadata or {}).get("title") or "").strip()
                if self._NON_ARTICLE_TITLES.match(title):
                    continue
                if result.pdf_url in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(result.pdf_url)
                result.metadata.setdefault("journal", "Ohio State Technology Law Journal")
                for key, value in issue_metadata.items():
                    result.metadata.setdefault(key, value)
                yield result

            if collection_had_objects:
                consecutive_empty_collections = 0
            else:
                consecutive_empty_collections += 1
                # A real OSTLJ issue contains records. Two empty issue scopes in
                # succession indicate an unavailable/throttled discover API;
                # stop instead of multiplying the 25-second API timeout by all
                # 50 collections.
                if consecutive_empty_collections >= 2:
                    break

        # The Knowledge Bank is occasionally unreachable before HTTP from the
        # scraper network.  Only then use the journal's two publisher-owned,
        # publication-scoped fallbacks: the current Moritz OSTLJ articles page
        # and the archived legacy ``/students/groups/is/files/`` subtree.
        if seen_pdf_urls:
            return

        for result in self._iter_moritz_live_articles():
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result

        for result in self._iter_wayback_legacy_articles():
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result

    def _iter_moritz_live_articles(self) -> Iterator[DiscoveryResult]:
        try:
            response = self.session.get(self.MORITZ_ARTICLES_URL, timeout=30)
            response.raise_for_status()
        except Exception:
            return

        soup = BeautifulSoup(response.text, "lxml")
        for item in soup.select("li.bux-journal-feed__item"):
            anchor = item.select_one('a[href*=".pdf"]')
            if anchor is None:
                continue
            pdf_url = urljoin(self.MORITZ_ARTICLES_URL, str(anchor.get("href") or ""))
            if urlparse(pdf_url).hostname != "moritzlaw.osu.edu":
                continue
            title_node = item.select_one(".bux-journal-feed__item-content-title")
            author_node = item.select_one(".bux-journal-feed__item-content-author")
            title = title_node.get_text(" ", strip=True) if title_node else ""
            if not title or self._NON_ARTICLE_TITLES.match(title):
                continue
            metadata: Dict[str, Any] = {
                "title": title,
                "journal": "Ohio State Technology Law Journal",
                "platform": "moritz_ostlj",
                "document_type": "article",
            }
            author = author_node.get_text(" ", strip=True) if author_node else ""
            if author:
                metadata["authors"] = [author]
            year_match = re.search(r"/((?:19|20)\d{2})-\d{2}/", urlparse(pdf_url).path)
            if year_match:
                metadata["year"] = year_match.group(1)
            yield DiscoveryResult(
                page_url=self.MORITZ_ARTICLES_URL,
                pdf_url=pdf_url,
                metadata=metadata,
                source_adapter="ohio_state_technology",
                extraction_path="moritz_ostlj_live_articles",
            )

    def _iter_wayback_legacy_articles(self) -> Iterator[DiscoveryResult]:
        try:
            response = self.session.get(
                self.WAYBACK_CDX_URL,
                params={
                    "url": self.WAYBACK_SCOPE,
                    "output": "json",
                    "filter": ["statuscode:200", "mimetype:application/pdf"],
                    "fl": "timestamp,original,digest",
                    "collapse": "urlkey",
                    "limit": "5000",
                },
                timeout=60,
            )
            response.raise_for_status()
            rows = response.json()
        except Exception:
            return
        if not isinstance(rows, list) or len(rows) < 2:
            return

        for row in rows[1:]:
            if not isinstance(row, list) or len(row) < 2:
                continue
            timestamp, original = str(row[0]), str(row[1])
            parsed = urlparse(original)
            if parsed.hostname != "moritzlaw.osu.edu" or not parsed.path.startswith(
                "/students/groups/is/files/"
            ):
                continue
            filename = unquote(Path(parsed.path).name)
            if not filename.lower().endswith(".pdf") or self._NON_ARTICLE_FILES.search(filename):
                continue
            replay_url = f"https://web.archive.org/web/{timestamp}id_/{original}"
            self._wayback_replay_by_pdf[original] = replay_url
            metadata = self._legacy_filename_metadata(filename)
            yield DiscoveryResult(
                page_url=original,
                pdf_url=original,
                metadata=metadata,
                source_adapter="ohio_state_technology",
                extraction_path="wayback_ostlj_legacy_pdf_inventory",
            )

    @staticmethod
    def _legacy_filename_metadata(filename: str) -> Dict[str, Any]:
        stem = Path(filename).stem
        readable = re.sub(r"[_-]+", " ", stem).strip()
        metadata: Dict[str, Any] = {
            "title": f"OSTLJ archived article: {readable}",
            "journal": "Ohio State Technology Law Journal",
            "platform": "moritz_ostlj_wayback",
            "document_type": "article",
        }
        volume_match = re.search(r"v(?:ol(?:ume)?)?\s*(\d+)\s*no\s*(\d+)", readable, re.I)
        if volume_match:
            metadata["volume"] = volume_match.group(1)
            metadata["issue"] = volume_match.group(2)
        return metadata

    def download_pdf(self, pdf_url: str, out_dir: str, **kwargs) -> Optional[str]:
        replay_url = self._wayback_replay_by_pdf.get(pdf_url)
        if replay_url:
            return self._download_with_generic(replay_url, out_dir, **kwargs)
        return self._download_with_generic(pdf_url, out_dir, **kwargs)
