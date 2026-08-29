"""Adapter for the Michigan State Law Review (Squarespace + Google Drive).

`michiganstatelawreview.org` is a Squarespace site.  It carries no PDFs of its
own: every article file is a Google Drive object linked from the journal's own
pages.  Two page shapes matter:

* ``/past-issues-1`` (alias ``/past-issues-1-1``) -- one long static page whose
  ``div.sqs-html-content`` block runs ``<h2>Issue YYYY.N</h2>`` /
  ``<h3>Articles|Comment|...</h3>`` / ``<p><a href="drive.google...">Title</a>``
  / ``<p>Author</p>`` in document order.  This covers issues 2020.1 - 2024.5.
* ``/current-vol-20252026`` -- a Squarespace blog index whose per-post pages
  each carry a single Drive link for the current volume.

Origin gate
-----------
Because the article bytes live off-origin, this adapter uses a strict
allowlist: a link is only accepted when it is a Google **Drive file** link (or
a same-origin Squarespace file) discovered on a page under the journal's own
host.  Anything else -- including a ``.pdf`` href to a third-party site that
happens to appear in a footnote or a "further reading" list -- is rejected.
This is deliberately narrower than a generic ``a[href$=".pdf"]`` sweep, which
is what leaked a foreign court's PDF into an OJS journal in 2026-08.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from .base import DiscoveryResult
from .site_archive_base import SiteArchiveAdapterBase

JOURNAL_NAME = "Michigan State Law Review"

#: Hosts allowed to serve the journal's article files.
ALLOWED_HOSTS = frozenset(
    {
        "michiganstatelawreview.org",
        "www.michiganstatelawreview.org",
        "drive.google.com",
        "drive.usercontent.google.com",
        "docs.google.com",
        "static1.squarespace.com",
    }
)

DRIVE_HOSTS = frozenset({"drive.google.com", "drive.usercontent.google.com", "docs.google.com"})

SITE_HOSTS = frozenset({"michiganstatelawreview.org", "www.michiganstatelawreview.org"})

DRIVE_FILE_RE = re.compile(r"(?:drive|docs)\.google\.com/file/d/([^/?#]+)", re.IGNORECASE)

#: "Issue 2024.4", "Issue 2024", "Volume 2021 Issue 3"
ISSUE_HEADING_RE = re.compile(
    r"(?:issue|volume)\s*(?P<year>(?:19|20)\d{2})(?:\s*[.\-]\s*(?P<issue>\d+))?",
    re.IGNORECASE,
)


class MichiganStateLawReviewAdapter(SiteArchiveAdapterBase):
    """Squarespace archive adapter that resolves Google Drive article files."""

    # ------------------------------------------------------------------ gate

    @staticmethod
    def _drive_file_id(url: str) -> str:
        match = DRIVE_FILE_RE.search(url or "")
        if match:
            return match.group(1)
        parsed = urlparse(url or "")
        if parsed.netloc.lower() in DRIVE_HOSTS and parsed.path in ("/uc", "/open", "/download"):
            return parse_qs(parsed.query).get("id", [""])[0]
        return ""

    @classmethod
    def _accept_article_url(cls, url: str) -> bool:
        """Origin gate for candidate article files."""
        try:
            host = (urlparse(url).netloc or "").lower()
        except Exception:
            return False
        if host not in ALLOWED_HOSTS:
            return False
        if host in DRIVE_HOSTS:
            return bool(cls._drive_file_id(url))
        # Same-origin / Squarespace CDN: require a real .pdf path.
        return urlparse(url).path.lower().endswith(".pdf")

    @classmethod
    def _direct_pdf_url(cls, url: str) -> str:
        file_id = cls._drive_file_id(url)
        if file_id:
            return f"https://drive.usercontent.google.com/download?id={file_id}&export=download"
        return url

    @staticmethod
    def _is_site_page(url: str) -> bool:
        return (urlparse(url or "").netloc or "").lower() in SITE_HOSTS

    # ------------------------------------------------------------- metadata

    @staticmethod
    def _issue_metadata(heading: str) -> Dict[str, str]:
        match = ISSUE_HEADING_RE.search(heading or "")
        if not match:
            return {}
        # MSLR numbers its volumes by year (e.g. "2024 MICH. ST. L. REV. 947").
        meta = {"volume": match.group("year"), "date": match.group("year")}
        if match.group("issue"):
            meta["issue"] = match.group("issue")
        return meta

    @staticmethod
    def _following_author(anchor_paragraph: Tag) -> List[str]:
        """The author line is the next sibling <p> that carries no link."""
        sibling = anchor_paragraph.find_next_sibling()
        while isinstance(sibling, Tag):
            if sibling.name in {"h1", "h2", "h3", "h4"}:
                return []
            if sibling.name == "p":
                if sibling.find("a"):
                    return []
                text = " ".join(sibling.get_text(" ", strip=True).split())
                if text and len(text) < 200:
                    return [part.strip() for part in re.split(r"\s*&\s*|\s+and\s+", text) if part.strip()]
                return []
            sibling = sibling.find_next_sibling()
        return []

    # ------------------------------------------------------------ discovery

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        if not self._is_site_page(seed_url):
            return
        soup = self._get(seed_url)
        if not soup:
            return

        seen: set[str] = set()

        # 1) Flat archive page (headings + drive links in document order).
        for result in self._walk_archive_page(soup, seed_url, seen):
            yield result

        # 2) Squarespace blog index -> per-post pages (current volume).
        for post_url, post_title in self._blog_post_links(soup, seed_url):
            post_soup = self._get(post_url)
            if not post_soup:
                continue
            for result in self._walk_archive_page(
                post_soup, post_url, seen, fallback_title=post_title
            ):
                yield result

    def _walk_archive_page(
        self,
        soup: BeautifulSoup,
        page_url: str,
        seen: set,
        fallback_title: str = "",
    ) -> Iterable[DiscoveryResult]:
        issue_meta: Dict[str, str] = {}
        section = ""
        for element in soup.find_all(["h1", "h2", "h3", "h4", "p", "li", "a"]):
            if element.name in {"h1", "h2", "h3", "h4"}:
                heading = " ".join(element.get_text(" ", strip=True).split())
                parsed = self._issue_metadata(heading)
                if parsed:
                    issue_meta = parsed
                    section = ""
                elif heading:
                    section = heading
                continue
            if element.name == "a":
                anchors: List[Tag] = [element] if not element.find_parent(["p", "li"]) else []
            else:
                anchors = element.find_all("a", href=True)
            for anchor in anchors:
                raw = (anchor.get("href") or "").strip()
                if not raw:
                    continue
                absolute = urljoin(page_url, raw)
                if not self._accept_article_url(absolute):
                    continue
                pdf_url = self._direct_pdf_url(absolute)
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                title = " ".join(anchor.get_text(" ", strip=True).split()) or fallback_title
                metadata: Dict[str, object] = dict(issue_meta)
                metadata["title"] = title
                metadata["journal"] = JOURNAL_NAME
                if section:
                    metadata["section"] = section
                container = anchor.find_parent("p")
                if container is not None:
                    authors = self._following_author(container)
                    if authors:
                        metadata["authors"] = authors
                metadata["source_url"] = page_url
                metadata["url"] = page_url
                yield DiscoveryResult(
                    page_url=page_url,
                    pdf_url=pdf_url,
                    metadata=metadata,
                    source_adapter=type(self).__name__,
                    extraction_path="squarespace_archive_drive_link",
                )

    def _blog_post_links(self, soup: BeautifulSoup, page_url: str) -> List[Tuple[str, str]]:
        """Same-origin Squarespace blog post URLs under the seed's collection."""
        seed_path = urlparse(page_url).path.rstrip("/")
        if not seed_path:
            return []
        out: List[Tuple[str, str]] = []
        seen: set = set()
        for anchor in soup.select("a[href]"):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            absolute = urljoin(page_url, href).split("#", 1)[0]
            if not self._is_site_page(absolute):
                continue
            path = urlparse(absolute).path.rstrip("/")
            # Blog posts live at <collection>/<yyyy>/<m>/<d>/<slug>.
            if not path.startswith(seed_path + "/"):
                continue
            if not re.match(rf"^{re.escape(seed_path)}/\d{{4}}/\d{{1,2}}/\d{{1,2}}/", path + "/"):
                continue
            if absolute in seen:
                continue
            seen.add(absolute)
            out.append((absolute, " ".join(anchor.get_text(" ", strip=True).split())))
        return out

    def download_pdf(self, pdf_url: str, out_dir: str, **kwargs) -> Optional[str]:
        return self._download_with_generic(self._direct_pdf_url(pdf_url), out_dir, **kwargs)
