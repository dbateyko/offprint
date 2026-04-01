from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS


class IllinoisJLTPAdapter(Adapter):
    """Adapter for the Illinois JLTP custom Cybernautic journal archive.

    The public archive page shells out an empty listing and then hydrates
    content through `/extended/getJournalArticles` and
    `/extended/getJournalProfile`.
    """

    ALLOWED_PDF_HOSTS = {"illinoisjltp.com", "www.illinoisjltp.com"}

    def _browser_headers(self, referer: str = "") -> Dict[str, str]:
        headers = dict(DEFAULT_HEADERS)
        if referer:
            headers["Referer"] = referer
        return headers

    def _warm_archive_session(self, archive_url: str) -> bool:
        try:
            resp = self.session.get(
                archive_url,
                headers=self._browser_headers(),
                timeout=20,
            )
        except Exception:
            return False
        return bool(resp is not None and resp.status_code < 400)

    def _post_json(
        self,
        endpoint_url: str,
        *,
        referer: str,
        data: Optional[Dict[str, str]] = None,
        max_attempts: int = 3,
    ) -> Optional[dict[str, Any]]:
        headers = self._browser_headers(referer=referer)
        headers["X-Requested-With"] = "XMLHttpRequest"

        for attempt in range(max_attempts):
            try:
                response = self.session.post(
                    endpoint_url,
                    headers=headers,
                    data=data or {},
                    timeout=20,
                )
            except Exception:
                response = None

            if response is not None and response.status_code == 200:
                try:
                    return json.loads(response.text)
                except json.JSONDecodeError:
                    pass

            # Refresh the archive page before retrying to recover cookies.
            self._warm_archive_session(referer)
            time.sleep(0.5 * (attempt + 1))

        return None

    def _fetch_articles_payload(self, seed_url: str) -> Optional[dict[str, Any]]:
        archive_url = seed_url.rstrip("/") + "/"
        if not self._warm_archive_session(archive_url):
            return None

        endpoint = urljoin(archive_url, "/extended/getJournalArticles")
        payload = self._post_json(endpoint, referer=archive_url)
        if not payload or payload.get("status") != "success":
            return None
        data = payload.get("data")
        return data if isinstance(data, dict) else None

    @staticmethod
    def _split_authors(author_text: str) -> list[str]:
        if not author_text:
            return []
        normal = re.sub(r"\s+and\s+", ",", author_text.strip(), flags=re.I)
        return [part.strip() for part in normal.split(",") if part.strip()]

    @staticmethod
    def _build_article_context(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        by_slug: dict[str, dict[str, Any]] = {}

        for volume_slug, volume_bucket in payload.items():
            if volume_slug == "all" or not isinstance(volume_bucket, dict):
                continue
            for issue_slug, items in volume_bucket.items():
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    slug = str(item.get("slug") or "").strip()
                    if not slug:
                        continue
                    context = by_slug.setdefault(slug, {})
                    if volume_slug:
                        context.setdefault("volume_slug", volume_slug)
                    if issue_slug:
                        context.setdefault("issue_slug", issue_slug)
                    for key in ("volume", "issue", "title", "author", "short_description", "article_date"):
                        value = item.get(key)
                        if value not in (None, ""):
                            context.setdefault(key, value)

        return by_slug

    @classmethod
    def _is_allowed_pdf_host(cls, url: str) -> bool:
        return (urlparse(url).netloc or "").lower() in cls.ALLOWED_PDF_HOSTS

    def _resolve_pdf_url(self, candidate_url: str, referer: str) -> Optional[str]:
        headers = self._browser_headers(referer=referer)
        try:
            response = self.session.get(
                candidate_url,
                headers=headers,
                timeout=20,
                allow_redirects=False,
            )
        except Exception:
            response = None

        if response is not None and response.status_code in {301, 302, 303, 307, 308}:
            location = response.headers.get("location") or response.headers.get("Location")
            if location:
                return urljoin(candidate_url, location)
        return candidate_url

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        payload = self._fetch_articles_payload(seed_url)
        if not payload:
            return

        archive_url = seed_url.rstrip("/") + "/"
        profile_endpoint = urljoin(archive_url, "/extended/getJournalProfile")
        article_context = self._build_article_context(payload)
        seen_pdf_urls: set[str] = set()

        for raw_item in payload.get("all", []):
            if not isinstance(raw_item, dict):
                continue

            slug = str(raw_item.get("slug") or "").strip()
            if not slug:
                continue

            context = dict(article_context.get(slug, {}))
            context.update(raw_item)

            volume_value = str(context.get("volume") or "")
            volume_slug = str(context.get("volume_slug") or "")
            profile_payload = self._post_json(
                profile_endpoint,
                referer=archive_url,
                data={
                    "slug": slug,
                    "volume": volume_value,
                    "vol-slug": volume_slug,
                },
            )
            if not profile_payload or profile_payload.get("status") != "success":
                continue

            profile_html = profile_payload.get("data")
            if not isinstance(profile_html, str):
                continue
            profile_soup = BeautifulSoup(profile_html, "lxml")
            pdf_anchor = profile_soup.select_one("a.profile-btn-wrapper[href]")
            if not pdf_anchor:
                continue

            candidate_pdf_url = urljoin(archive_url, (pdf_anchor.get("href") or "").strip())
            resolved_pdf_url = self._resolve_pdf_url(candidate_pdf_url, referer=archive_url)
            if not resolved_pdf_url or not self._is_allowed_pdf_host(resolved_pdf_url):
                continue
            if resolved_pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(resolved_pdf_url)

            metadata: dict[str, Any] = {
                "title": str(context.get("title") or "").strip(),
                "source_url": archive_url,
                "platform": "illinois_jltp_api",
                "issue": str(context.get("issue") or "").strip(),
            }
            if volume_value:
                metadata["volume"] = volume_value
            article_date = str(context.get("article_date") or "").strip()
            if article_date:
                metadata["date"] = article_date.split(" ", 1)[0]

            authors = self._split_authors(str(context.get("author") or "").strip())
            if authors:
                metadata["authors"] = authors

            if volume_slug:
                metadata["url"] = urljoin(
                    archive_url,
                    f"volume/{volume_slug}/article/{slug}",
                )

            yield DiscoveryResult(
                page_url=metadata.get("url") or archive_url,
                pdf_url=resolved_pdf_url,
                metadata=metadata,
                source_adapter="illinois_jltp",
                extraction_path="json_profile_download",
            )
