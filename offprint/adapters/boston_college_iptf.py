from __future__ import annotations

import json
import re
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, quote, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase
from .utils import DEFAULT_HEADERS, request_verify_for_url


class BostonCollegeIPTFAdapter(SiteArchiveAdapterBase):
    """Publication-scoped adapter for Boston College's IPTF journal."""

    PUBLICATION_PREFIX = "/iptf/"
    LIRA_WORK_PREFIXES = ("/works/publication-article/", "/work/sc/")
    MAX_ARCHIVE_PAGES = 30

    # Boston College's LIRA repository (InvenioRDM) is the publication of record for
    # IPTF. The sites.bc.edu WordPress archive only advertises recent posts, so the
    # WordPress traversal is a fallback, never the primary route.
    LIRA_PUBLIC_HOST = "https://lira.bc.edu"
    LIRA_API_BASE = "https://dashboard.lira.bc.edu/api"
    LIRA_COMMUNITY_SLUG = "intellectual-property-and-technology-forum"
    LIRA_PAGE_SIZE = 100
    LIRA_MAX_PAGES = 20
    JOURNAL_TITLE = "boston college intellectual property and technology forum"
    # Hard cap on WordPress post fetches so a fallback traversal can never wedge a
    # seed in `discovering` for the whole stalled-seed window.
    MAX_WORDPRESS_ARTICLE_FETCHES = 60

    NON_ARTICLE_RE = re.compile(
        r"\b(?:blog post|masthead|journal staff|front matter|back matter"
        r"|table of contents|editorial board|table of authorities)\b",
        re.IGNORECASE,
    )
    CHALLENGE_RE = re.compile(
        r"(?:cf-chl-|cloudflare ray id|attention required|verify you are human)",
        re.IGNORECASE,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_discovery = False
        self._wordpress_article_fetches = 0

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        parsed_seed = urlparse(seed_url)
        if parsed_seed.netloc.lower() != "sites.bc.edu" or not parsed_seed.path.startswith(
            self.PUBLICATION_PREFIX
        ):
            return

        seen_pdfs: set[str] = set()
        for result in self._discover_via_lira_community():
            if result.pdf_url in seen_pdfs:
                continue
            seen_pdfs.add(result.pdf_url)
            yield result

        if seen_pdfs or self._stop_discovery:
            return

        # Fallback only: the repository route returned nothing at all.
        yield from self._discover_via_wordpress(parsed_seed, seen_pdfs)

    # ------------------------------------------------------------------
    # Primary route: LIRA InvenioRDM community (publication-scoped)
    # ------------------------------------------------------------------

    def _discover_via_lira_community(self) -> Iterable[DiscoveryResult]:
        community = self._fetch_json(
            f"{self.LIRA_API_BASE}/communities/{self.LIRA_COMMUNITY_SLUG}"
        )
        community_id = (community or {}).get("id") or self.LIRA_COMMUNITY_SLUG
        if not community:
            return

        seen_ids: set[str] = set()
        for page in range(1, self.LIRA_MAX_PAGES + 1):
            if self._stop_discovery:
                return
            payload = self._fetch_json(
                f"{self.LIRA_API_BASE}/communities/{community_id}/records"
                f"?size={self.LIRA_PAGE_SIZE}&page={page}"
            )
            if not payload:
                return
            hits = ((payload.get("hits") or {}).get("hits")) or []
            if not hits:
                return
            for record in hits:
                record_id = str(record.get("id") or "")
                if not record_id or record_id in seen_ids:
                    continue
                seen_ids.add(record_id)
                result = self._result_from_lira_record(record)
                if result is not None:
                    yield result
            if len(hits) < self.LIRA_PAGE_SIZE:
                return

    def _result_from_lira_record(self, record: dict) -> Optional[DiscoveryResult]:
        record_id = str(record.get("id") or "")
        metadata = record.get("metadata") or {}
        journal = ((record.get("custom_fields") or {}).get("journal:journal")) or {}

        # host != journal: accept only records this repository itself labels IPTF.
        if self._normalise(journal.get("title")) != self.JOURNAL_TITLE:
            return None

        title = " ".join(str(metadata.get("title") or "").split())
        if not title or self.NON_ARTICLE_RE.search(title):
            return None

        filename = self._pdf_filename(record)
        if not filename:
            return None

        pdf_url = f"{self.LIRA_PUBLIC_HOST}/downloads/{record_id}/{quote(filename)}"
        work_url = f"{self.LIRA_PUBLIC_HOST}/works/publication-article/{record_id}"

        authors = [
            " ".join(str((creator.get("person_or_org") or {}).get("name") or "").split())
            for creator in metadata.get("creators") or []
        ]
        authors = [author for author in authors if author]
        date = str(metadata.get("publication_date") or "").strip()

        out: dict[str, Any] = {
            "title": title,
            "source_url": work_url,
            "url": work_url,
            "repository_url": work_url,
            "journal_name": journal.get("title"),
            "document_type": "article",
        }
        if authors:
            out["authors"] = authors
        if date:
            out["date"] = date
            out["year"] = date[:4]
        for key, field in (("volume", "volume"), ("issue", "issue"), ("pages", "pages")):
            value = journal.get(field)
            if value:
                out[key] = str(value)

        return DiscoveryResult(
            page_url=work_url,
            pdf_url=pdf_url,
            metadata=out,
            source_adapter=type(self).__name__,
            extraction_path="lira_iptf_community_record",
        )

    @staticmethod
    def _pdf_filename(record: dict) -> str:
        entries = ((record.get("files") or {}).get("entries")) or {}
        for key, entry in entries.items():
            entry = entry if isinstance(entry, dict) else {}
            if entry.get("access", {}).get("hidden"):
                continue
            name = str(entry.get("key") or key)
            if name.lower().endswith(".pdf") or entry.get("mimetype") == "application/pdf":
                return name
        return ""

    @staticmethod
    def _normalise(value: object) -> str:
        return " ".join(str(value or "").split()).casefold()

    def _fetch_json(self, url: str) -> Optional[dict]:
        if self._stop_discovery:
            return None
        try:
            response = self.session.get(
                url,
                headers={**DEFAULT_HEADERS, "Accept": "application/json"},
                timeout=30,
                verify=request_verify_for_url(url),
            )
        except Exception:
            return None
        if getattr(response, "status_code", 200) in {403, 429}:
            self._stop_discovery = True
            return None
        if getattr(response, "status_code", 200) >= 400:
            return None
        text = getattr(response, "text", "") or ""
        if self.CHALLENGE_RE.search(text):
            self._stop_discovery = True
            return None
        try:
            payload = json.loads(text)
        except ValueError:
            return None
        return payload if isinstance(payload, dict) else None

    # ------------------------------------------------------------------
    # Fallback route: sites.bc.edu WordPress archive
    # ------------------------------------------------------------------

    def _discover_via_wordpress(
        self, parsed_seed, seen_pdfs: set[str]
    ) -> Iterable[DiscoveryResult]:
        archive_url = f"{parsed_seed.scheme}://{parsed_seed.netloc}{self.PUBLICATION_PREFIX}"
        first_page = self._fetch_html(archive_url)
        if not first_page:
            return

        max_pages = min(self._archive_page_count(first_page), self.MAX_ARCHIVE_PAGES)
        seen_articles: set[str] = set()

        for page_number in range(1, max_pages + 1):
            if self._stop_discovery:
                return
            listing_url = archive_url if page_number == 1 else f"{archive_url}?paged={page_number}"
            listing = first_page if page_number == 1 else self._fetch_html(listing_url)
            if not listing:
                return

            cards = listing.select("article.post")
            if not cards:
                return

            for card in cards:
                article = self._article_link(card, archive_url)
                if not article:
                    continue
                listing_title, article_url = article
                if article_url in seen_articles or self._is_non_article(listing_title, card):
                    continue
                seen_articles.add(article_url)
                if self._wordpress_article_fetches >= self.MAX_WORDPRESS_ARTICLE_FETCHES:
                    return
                self._wordpress_article_fetches += 1

                result = self._discover_article(article_url, listing_title)
                if result is None:
                    if self._stop_discovery:
                        return
                    continue

                if result.pdf_url in seen_pdfs:
                    continue
                seen_pdfs.add(result.pdf_url)
                yield result

    def _discover_article(
        self, article_url: str, listing_title: str
    ) -> Optional[DiscoveryResult]:
        article = self._fetch_html(article_url)
        if not article or self._is_non_article(listing_title, article):
            return None

        work_url = self._lira_work_url(article, article_url)
        if not work_url:
            return None
        work = self._fetch_html(work_url)
        if not work or "Intellectual Property and Technology Forum" not in work.get_text(
            " ", strip=True
        ):
            return None

        download = work.select_one('a[href*="/downloads/"][href*=".pdf"]')
        if not download:
            return None
        pdf_url = urljoin(work_url, download.get("href", ""))

        title = self._work_title(work) or listing_title
        authors = self._work_authors(work)
        published = work.select_one("time[datetime]")
        date = (published.get("datetime") or "").strip() if published else ""

        metadata: dict[str, object] = {
            "title": title,
            "source_url": article_url,
            "url": article_url,
            "repository_url": work_url,
            "document_type": "article",
        }
        if authors:
            metadata["authors"] = authors
        if date:
            metadata["date"] = date
            metadata["year"] = date[:4]

        return DiscoveryResult(
            page_url=article_url,
            pdf_url=pdf_url,
            metadata=metadata,
            source_adapter=type(self).__name__,
            extraction_path="iptf_post_to_lira_work",
        )

    def _fetch_html(self, url: str) -> Optional[BeautifulSoup]:
        if self._stop_discovery:
            return None
        try:
            response = self.session.get(
                url,
                headers=DEFAULT_HEADERS,
                timeout=20,
                verify=request_verify_for_url(url),
            )
        except Exception:
            return None

        if response.status_code in {403, 429}:
            self._stop_discovery = True
            return None
        if response.status_code >= 400:
            return None
        text = response.text or ""
        if not text.strip() or self.CHALLENGE_RE.search(text):
            self._stop_discovery = True
            return None
        return BeautifulSoup(text, "lxml")

    @classmethod
    def _archive_page_count(cls, soup: BeautifulSoup) -> int:
        nav = soup.select_one("nav[data-pagination-max-pages]")
        if nav:
            try:
                return max(1, int(nav.get("data-pagination-max-pages", "1")))
            except ValueError:
                pass
        pages = [1]
        for anchor in soup.select('a[href*="paged="]'):
            try:
                pages.append(int(parse_qs(urlparse(anchor.get("href", "")).query)["paged"][0]))
            except (KeyError, TypeError, ValueError):
                continue
        return max(pages)

    @classmethod
    def _article_link(cls, card: Tag, base_url: str) -> Optional[tuple[str, str]]:
        anchor = card.select_one("h2.entry-title a[href]")
        if not anchor:
            return None
        title = " ".join(anchor.get_text(" ", strip=True).split())
        url = urljoin(base_url, anchor.get("href", ""))
        parsed = urlparse(url)
        if parsed.netloc.lower() != "sites.bc.edu" or not parsed.path.startswith(
            cls.PUBLICATION_PREFIX
        ):
            return None
        return title, url

    @classmethod
    def _is_non_article(cls, title: str, element: Tag) -> bool:
        if cls.NON_ARTICLE_RE.search(title or ""):
            return True
        classes = {str(value).lower() for value in element.get("class", [])}
        if "category-blog-post" in classes or "tag-blog-post" in classes:
            return True
        body = element.select_one(".yuki-article-content, .entry-content")
        body_text = body.get_text(" ", strip=True)[:600] if body else ""
        return bool(re.search(r"\bnot (?:a|an) (?:published )?IPTF Journal article\b", body_text, re.I))

    @classmethod
    def _lira_work_url(cls, soup: BeautifulSoup, base_url: str) -> str:
        for anchor in soup.select('a[href*="lira.bc.edu/"]'):
            url = urljoin(base_url, anchor.get("href", ""))
            parsed = urlparse(url)
            if parsed.netloc.lower() == "lira.bc.edu" and any(
                parsed.path.startswith(prefix) for prefix in cls.LIRA_WORK_PREFIXES
            ):
                return url
        return ""

    @staticmethod
    def _work_title(soup: BeautifulSoup) -> str:
        heading = soup.select_one("main h1, h1")
        return " ".join(heading.get_text(" ", strip=True).split()) if heading else ""

    @staticmethod
    def _work_authors(soup: BeautifulSoup) -> list[str]:
        heading = soup.select_one("#creators-list")
        if not heading or not heading.parent:
            return []
        authors: list[str] = []
        for item in heading.parent.select("li button"):
            author = " ".join(item.get_text(" ", strip=True).split())
            if author and author not in authors:
                authors.append(author)
        return authors
