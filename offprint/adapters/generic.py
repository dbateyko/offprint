from __future__ import annotations

import os
import re
import time
import unicodedata
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urljoin, urlparse, urlunsplit

import requests
from bs4 import BeautifulSoup, NavigableString
from collections import deque

from .base import Adapter, DiscoveryResult
from .utils import (
    compute_pdf_sha256_and_size,
    is_pdf_url,
    pre_validate_pdf_url,
    request_verify_for_url,
    validate_pdf_magic_bytes,
)

DEFAULT_HEADERS = {
    # Browser-like UA to reduce simple 403/406 blocks on journal sites.
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

DISCOVERY_UA_PROFILES = [
    "curl/8.7.1",
    f"python-requests/{requests.__version__}",
    "Wget/1.21.4",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

FALLBACK_LINK_HINTS = (
    "archive",
    "archives",
    "issue",
    "issues",
    "volume",
    "vol-",
    "past",
    "print",
    "journal",
    "content",
    "article",
    "masthead",
)


class GenericAdapter(Adapter):
    """Generic adapter: finds <a href="*.pdf"> links and downloads them.

    Depth 0: only the seed page. Depth >0: follows same-origin links up to max_depth.
    """

    def _looks_blocked_html(self, resp: requests.Response) -> bool:
        status = int(resp.status_code or 0)
        ctype = str(resp.headers.get("Content-Type") or "").lower()
        waf_action = str(resp.headers.get("x-amzn-waf-action") or "").lower()
        if waf_action == "challenge":
            return True
        if status in {401, 403, 406, 429, 503} and "text/html" in ctype:
            return True
        if status == 202 and "text/html" in ctype:
            snippet = (resp.text or "")[:1200].lower()
            return any(
                token in snippet
                for token in (
                    "captcha",
                    "cloudflare",
                    "access denied",
                    "forbidden",
                    "bot detection",
                    "challenge",
                    "checking your browser",
                )
            )
        return False

    def _get(self, url: str) -> Optional[requests.Response]:
        last_resp: Optional[requests.Response] = None
        for ua in DISCOVERY_UA_PROFILES:
            headers = dict(DEFAULT_HEADERS)
            headers["User-Agent"] = ua
            try:
                resp = self.session.get(url, headers=headers, timeout=20)
            except requests.RequestException:
                continue
            if resp is None:
                continue

            last_resp = resp
            if resp.status_code < 400 and not self._looks_blocked_html(resp):
                return resp
            # Retry with protocol-style UAs for common bot/challenge responses.
            if self._looks_blocked_html(resp):
                continue
            if resp.status_code in {401, 403, 406, 429, 503}:
                continue
            break

        if (
            last_resp is not None
            and last_resp.status_code < 400
            and not self._looks_blocked_html(last_resp)
        ):
            return last_resp
        return None

    def _iter_links(self, html: str, base_url: str) -> Iterable[tuple[str, str]]:
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            anchor_text = a.get_text(" ", strip=True)
            yield urljoin(base_url, a["href"]), anchor_text  # absolute

    def _iter_embedded_pdf_urls(self, html: str, base_url: str) -> Iterable[str]:
        """Extract PDF URLs embedded in script/JSON/markdown blobs."""

        if not html:
            return

        candidates: Set[str] = set()
        normalized = html.replace("\\/", "/")
        sources = (html, normalized)
        patterns = [
            re.compile(r"\((https?://[^)\s]+?\.pdf(?:\?[^)\s]*)?)\)", re.I),
            re.compile(r"https?://[^\"'<>\s]+?\.pdf(?:\?[^\"'<>\s]*)?", re.I),
            re.compile(r"['\"](/[^'\" ]+?\.pdf(?:\?[^'\" ]*)?)['\"]", re.I),
        ]

        for source in sources:
            for pattern in patterns:
                for match in pattern.findall(source):
                    raw = match if isinstance(match, str) else (match[0] if match else "")
                    if not raw:
                        continue
                    cleaned = str(raw).strip().strip("'").strip('"')
                    cleaned = cleaned.replace("&amp;", "&").replace("\\u002F", "/")
                    cleaned = cleaned.rstrip("\\")
                    cleaned = cleaned.rstrip(").,;")
                    if cleaned.startswith("//"):
                        cleaned = f"{urlparse(base_url).scheme}:{cleaned}"
                    absolute = urljoin(base_url, cleaned)
                    parsed = urlparse(absolute)
                    if parsed.scheme not in {"http", "https"}:
                        continue
                    if not parsed.netloc:
                        continue
                    if ".pdf" not in (parsed.path or "").lower():
                        continue
                    candidates.add(absolute)

        for candidate in sorted(candidates):
            yield candidate

    def _extract_dspace_scope_id(self, seed_url: str) -> str:
        parsed = urlparse(seed_url)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2 and parts[0] == "collections":
            return parts[1]
        return ""

    def _is_probable_pdf_endpoint(self, link: str) -> bool:
        parsed = urlparse(link)
        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()

        if is_pdf_url(link):
            return True

        if "viewcontent.cgi" in path:
            return True

        if re.search(r"/article/\d+(?:/\d+)?/?$", path):
            return True

        if path.endswith("/"):
            return any(token in query for token in ("pdf", "download", "attachment"))

        if any(token in path for token in ("/download", "/downloads/", "/files/", "/bitstream")):
            return True

        if any(token in query for token in ("pdf", "download", "attachment", "inline=false")):
            return True

        return False

    def _get_json(
        self, url: str, *, params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        try:
            resp = self.session.get(
                url,
                params=params,
                headers={**DEFAULT_HEADERS, "Accept": "application/json"},
                timeout=25,
            )
            if resp.status_code >= 400:
                return None
            return resp.json()
        except Exception:
            return None

    def _extract_dspace_handle(self, url: str) -> str:
        parsed = urlparse(url)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 4 and parts[0].lower() == "bitstream" and parts[1].lower() == "handle":
            return f"{parts[2]}/{parts[3]}"
        return ""

    def _resolve_dspace_bitstream_content_url(self, bitstream_url: str) -> str:
        handle = self._extract_dspace_handle(bitstream_url)
        if not handle:
            return ""

        parsed = urlparse(bitstream_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        discover_url = f"{base}/server/api/discover/search/objects"
        payload = self._get_json(
            discover_url,
            params={"query": f"handle:{handle}", "size": 1},
        )
        if not payload:
            return ""

        objects = (
            payload.get("_embedded", {})
            .get("searchResult", {})
            .get("_embedded", {})
            .get("objects", [])
        )
        if not isinstance(objects, list) or not objects:
            return ""

        item = (objects[0].get("_embedded") or {}).get("indexableObject") or {}
        bundles_href = ((item.get("_links") or {}).get("bundles") or {}).get("href")
        if not bundles_href:
            return ""

        bundles = self._get_json(bundles_href, params={"embed": "bitstreams", "size": 100})
        if not bundles:
            return ""

        bundles_arr = (bundles.get("_embedded") or {}).get("bundles") or []
        if not isinstance(bundles_arr, list):
            return ""

        fallback_content_href = ""
        for bundle in bundles_arr:
            bundle_name = str(bundle.get("name") or "").upper()
            bitstreams = (bundle.get("_embedded") or {}).get("bitstreams") or []
            if isinstance(bitstreams, dict):
                bitstreams = (bitstreams.get("_embedded") or {}).get("bitstreams") or []
            if not isinstance(bitstreams, list):
                continue
            for bitstream in bitstreams:
                content_href = ((bitstream.get("_links") or {}).get("content") or {}).get("href")
                if not content_href:
                    continue
                bit_name = str(bitstream.get("name") or "").lower()
                if bundle_name == "ORIGINAL" and bit_name.endswith(".pdf"):
                    return content_href
                if bundle_name == "ORIGINAL" and not fallback_content_href:
                    fallback_content_href = content_href
        return fallback_content_href

    def _retry_dspace_bitstream_download(self, pdf_url: str, path: str) -> tuple[bool, int, str]:
        resolved_content_url = self._resolve_dspace_bitstream_content_url(pdf_url)
        if not resolved_content_url:
            return False, 0, ""

        try:
            with self.session.get(
                resolved_content_url,
                headers=DEFAULT_HEADERS,
                stream=True,
                timeout=30,
            ) as r:
                status_code = int(r.status_code or 0)
                content_type = str(r.headers.get("Content-Type") or "")
                if status_code >= 400:
                    return False, status_code, content_type
                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                if validate_pdf_magic_bytes(path):
                    return True, status_code, content_type
                return False, status_code, content_type
        except Exception:
            return False, 0, ""

    def _iter_plone_pdf_variants(self, pdf_url: str) -> Iterable[str]:
        parsed = urlparse(pdf_url)
        path_lower = (parsed.path or "").lower()
        if not path_lower.endswith(".pdf"):
            return
        if "/@@download/file" in path_lower or "/@@display-file/file" in path_lower:
            return

        base_path = parsed.path.rstrip("/")
        for suffix in ("/@@download/file", "/@@display-file/file"):
            yield urlunsplit(
                (parsed.scheme, parsed.netloc, f"{base_path}{suffix}", parsed.query, "")
            )

    def _retry_plone_pdf_variant_download(
        self,
        *,
        pdf_url: str,
        path: str,
        request_headers: Dict[str, str],
    ) -> tuple[bool, int, str]:
        for candidate_url in self._iter_plone_pdf_variants(pdf_url):
            try:
                with self.session.get(
                    candidate_url,
                    headers=request_headers,
                    stream=True,
                    timeout=(15, 120),
                ) as r:
                    status_code = int(r.status_code or 0)
                    content_type = str(r.headers.get("Content-Type") or "")
                    if status_code >= 400:
                        continue
                    bytes_written = 0
                    with open(path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if chunk:
                                bytes_written += len(chunk)
                                f.write(chunk)
                    if bytes_written <= 0:
                        try:
                            os.remove(path)
                        except OSError:
                            pass
                        continue
                    if validate_pdf_magic_bytes(path):
                        return True, status_code, content_type
                    try:
                        os.remove(path)
                    except OSError:
                        pass
            except Exception:
                continue
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass
        return False, 0, ""

    def _viewcontent_signature(self, pdf_url: str) -> tuple[str, str]:
        """Extract stable identifiers from viewcontent URLs for candidate matching."""
        try:
            parsed = urlparse(pdf_url)
            params = parse_qs(parsed.query or "")
        except Exception:
            return "", ""
        article = str((params.get("article") or [""])[0] or "").strip().lower()
        context = str((params.get("context") or [""])[0] or "").strip().lower()
        return article, context

    def _iter_referer_pdf_candidates(self, referer: str) -> Iterable[str]:
        """Extract likely PDF links from the referer/article page."""
        try:
            resp = self.session.get(referer, headers=DEFAULT_HEADERS, timeout=20)
            if resp is None:
                return
            if int(resp.status_code or 0) >= 400 or not resp.text:
                return
            soup = BeautifulSoup(resp.text, "lxml")
        except Exception:
            return

        seen: Set[str] = set()
        for a in soup.find_all("a", href=True):
            href = str(a.get("href") or "").strip()
            if not href:
                continue
            abs_url = urljoin(referer, href)
            parsed = urlparse(abs_url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                continue
            lowered = abs_url.lower()
            if not (
                is_pdf_url(abs_url)
                or "viewcontent.cgi" in lowered
                or "/server/api/core/bitstreams/" in lowered
            ):
                continue
            if abs_url in seen:
                continue
            seen.add(abs_url)
            yield abs_url

    def _retry_viewcontent_from_referer(
        self,
        *,
        pdf_url: str,
        referer: str,
        out_dir: str,
        request_headers: Dict[str, str],
    ) -> tuple[Optional[str], int, str]:
        """Recover stale viewcontent links by scraping a fresh PDF URL from referer page."""
        if not referer or "viewcontent.cgi" not in (pdf_url or "").lower():
            return None, 0, ""

        original_host = (urlparse(pdf_url).netloc or "").lower()
        article_sig, context_sig = self._viewcontent_signature(pdf_url)

        def _score(candidate_url: str) -> int:
            lower = candidate_url.lower()
            parsed = urlparse(candidate_url)
            score = 0
            if (parsed.netloc or "").lower() == original_host:
                score += 20
            if lower.endswith(".pdf"):
                score += 8
            if "viewcontent.cgi" in lower:
                score += 5
            if article_sig and f"article={article_sig}" in lower:
                score += 20
            if context_sig and f"context={context_sig}" in lower:
                score += 20
            return score

        candidates = sorted(
            {
                c
                for c in self._iter_referer_pdf_candidates(referer)
                if c.strip() and c.strip() != pdf_url
            },
            key=_score,
            reverse=True,
        )
        if not candidates:
            return None, 0, ""

        for candidate in candidates[:12]:
            try:
                with self.session.get(
                    candidate,
                    headers=request_headers,
                    stream=True,
                    timeout=(15, 120),
                ) as resp:
                    status_code = int(resp.status_code or 0)
                    content_type = str(resp.headers.get("Content-Type") or "")
                    if status_code >= 400:
                        continue
                    filename = self._resolve_filename(candidate, resp.headers, out_dir)
                    path = os.path.join(out_dir, filename)
                    bytes_written = 0
                    with open(path, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=65536):
                            if chunk:
                                bytes_written += len(chunk)
                                fh.write(chunk)
                    if bytes_written <= 0:
                        try:
                            os.remove(path)
                        except OSError:
                            pass
                        continue
                    if not validate_pdf_magic_bytes(path):
                        try:
                            os.remove(path)
                        except OSError:
                            pass
                        continue
                    return path, status_code, content_type
            except Exception:
                continue
        return None, 0, ""

    def _extract_dspace_metadata(self, item: Dict[str, Any], page_url: str) -> Dict[str, Any]:
        md = item.get("metadata") or {}

        def first(key: str) -> str:
            values = md.get(key) or []
            if isinstance(values, list) and values:
                value = values[0].get("value")
                if value:
                    return str(value).strip()
            return ""

        authors: List[str] = []
        for key in ("dc.contributor.author", "dc.creator"):
            values = md.get(key) or []
            if isinstance(values, list):
                for entry in values:
                    value = (entry or {}).get("value")
                    if value:
                        authors.append(str(value).strip())
            if authors:
                break

        metadata: Dict[str, Any] = {
            "source_url": page_url,
            "url": page_url,
            "title": first("dc.title") or item.get("name") or "",
        }
        if authors:
            metadata["authors"] = [a for a in authors if a]

        date_issued = first("dc.date.issued") or first("dc.date.year")
        if date_issued:
            metadata["date"] = date_issued
            year_match = re.search(r"\b(19|20)\d{2}\b", date_issued)
            if year_match:
                metadata["year"] = year_match.group(0)

        citation = first("dc.identifier.citation")
        if citation:
            metadata["citation"] = citation
            vol_match = re.search(r"\b(\d+)\b", citation)
            if vol_match and not metadata.get("volume"):
                metadata["volume"] = vol_match.group(1)
        issue_value = first("dc.description.issue")
        if issue_value:
            metadata["issue"] = issue_value

        return metadata

    # ------------------------------------------------------------------
    # Generic metadata extraction (covers non-DSpace, non-WordPress sites)
    # ------------------------------------------------------------------

    def _extract_generic_metadata(
        self,
        soup: BeautifulSoup,
        page_url: str,
        pdf_url: str,
        anchor_tag: Any,
    ) -> Dict[str, Any]:
        """Extract metadata for a PDF link from its surrounding page context."""
        metadata: Dict[str, Any] = {}
        anchor_text = anchor_tag.get_text(" ", strip=True) if anchor_tag else ""

        # --- Strategy 1: Green Bag pattern ---
        # <em><a href="...pdf">Title</a></em>, by Author Name
        parent = anchor_tag.parent if anchor_tag else None
        if parent and parent.name in ("em", "i"):
            if anchor_text and len(anchor_text) > 5:
                metadata["title"] = anchor_text
            grandparent = parent.parent
            if grandparent:
                full_text = grandparent.get_text(" ", strip=True)
                by_match = re.search(r",\s*by\s+(.+?)(?:\.\s*$|$)", full_text, re.I)
                if by_match:
                    author_str = by_match.group(1).strip().rstrip(".")
                    if author_str:
                        metadata["authors"] = [
                            a.strip() for a in re.split(r"\s*(?:&|and)\s*", author_str) if a.strip()
                        ]

        # --- Strategy 2: <li> citation pattern (Chapman) ---
        # <li>Author Name, <a>Title</a>, 28 CHAP. L. REV. 465 (2025).</li>
        if "authors" not in metadata and anchor_tag is not None:
            li = anchor_tag.find_parent("li")
            if li:
                pre_text = ""
                for child in li.children:
                    if child is anchor_tag:
                        break
                    if hasattr(child, "descendants") and anchor_tag in child.descendants:
                        break
                    if isinstance(child, NavigableString):
                        pre_text += str(child)
                author_candidate = pre_text.strip().rstrip(",").strip()
                if (
                    author_candidate
                    and len(author_candidate.split()) >= 2
                    and author_candidate[0].isupper()
                    and len(author_candidate) < 100
                ):
                    authors = [
                        a.strip()
                        for a in re.split(r",\s*", author_candidate)
                        if a.strip() and len(a.strip().split()) >= 2
                    ]
                    if authors:
                        metadata["authors"] = authors
                if anchor_text and len(anchor_text) > 5:
                    metadata.setdefault("title", anchor_text)
                # Post-anchor text for year/citation
                post_text = ""
                found_anchor = False
                for child in li.children:
                    if found_anchor and isinstance(child, NavigableString):
                        post_text += str(child)
                    if child is anchor_tag:
                        found_anchor = True
                    elif hasattr(child, "descendants") and anchor_tag in child.descendants:
                        found_anchor = True
                year_match = re.search(r"\((\d{4})\)", post_text)
                if year_match:
                    metadata["year"] = year_match.group(1)

        # --- Strategy 3: "Author, Title" anchor text (Lewis & Clark lw_files_pdf) ---
        if "authors" not in metadata and anchor_text and "," in anchor_text:
            css_classes = " ".join(anchor_tag.get("class") or []) if anchor_tag else ""
            first_comma = anchor_text.index(",")
            candidate_author = anchor_text[:first_comma].strip()
            candidate_title = anchor_text[first_comma + 1 :].strip()
            words = candidate_author.split()
            is_name_like = (
                2 <= len(words) <= 5 and words[0][0].isupper() and len(candidate_title) > 10
            )
            if "lw_files_pdf" in css_classes or is_name_like:
                metadata["authors"] = [candidate_author]
                metadata["title"] = candidate_title

        # --- Strategy 4: plain anchor text as title fallback ---
        if "title" not in metadata and anchor_text:
            skip_prefixes = ("pdf", "download", "click", "here", "view", "full text")
            if len(anchor_text) > 5 and not anchor_text.lower().startswith(skip_prefixes):
                metadata["title"] = anchor_text

        # --- Strategy 5: preceding heading for volume/issue/year ---
        if anchor_tag is not None:
            heading_text = self._preceding_heading_text(anchor_tag)
            if heading_text:
                heading_meta = self._extract_vol_issue_year_from_heading(heading_text)
                for k, v in heading_meta.items():
                    metadata.setdefault(k, v)

        # --- Strategy 6: URL pattern extraction ---
        url_meta = self._extract_vol_issue_from_url(pdf_url)
        for k, v in url_meta.items():
            metadata.setdefault(k, v)

        # --- Strategy 7: OG/meta tags as page-level fallback ---
        og_title = soup.find("meta", property="og:title") if soup else None
        if og_title and og_title.get("content"):
            og_content = og_title["content"].strip()
            og_meta = self._extract_vol_issue_year_from_heading(og_content)
            for k, v in og_meta.items():
                metadata.setdefault(k, v)

        return metadata

    def _preceding_heading_text(self, anchor_tag: Any) -> str:
        """Return text of the nearest h1/h2/h3 before anchor_tag in DOM order."""
        for tag in anchor_tag.find_all_previous(["h1", "h2", "h3"]):
            text = tag.get_text(" ", strip=True)
            if text:
                return text
        return ""

    def _extract_vol_issue_year_from_heading(self, heading_text: str) -> Dict[str, Any]:
        """Extract volume, issue, and year from heading text like 'Spring 2025 (vol. 28, no. 3)'."""
        result: Dict[str, Any] = {}
        year_m = re.search(r"\b((?:19|20)\d{2})\b", heading_text)
        if year_m:
            result["year"] = year_m.group(1)
        vol_m = re.search(r"(?:vol(?:ume)?\.?\s*)(\d+)", heading_text, re.I)
        if vol_m:
            result["volume"] = vol_m.group(1)
        iss_m = re.search(r"(?:no(?:\.|umber)?\.?\s*|issue\s*)(\d+)", heading_text, re.I)
        if iss_m:
            result["issue"] = iss_m.group(1)
        return result

    def _extract_vol_issue_from_url(self, url: str) -> Dict[str, Any]:
        """Extract volume/issue from URL patterns like /v27n3/ or vol35_no4."""
        result: Dict[str, Any] = {}
        # Green Bag style: /v27n3/
        m = re.search(r"/v(\d+)n(\d+)/", url, re.I)
        if m:
            result["volume"] = m.group(1)
            result["issue"] = m.group(2)
            return result
        # Generic: vol35_no4 or v35-n4
        m = re.search(r"v(?:ol)?[_-]?(\d+)[_-]n(?:o)?[_-]?(\d+)", url, re.I)
        if m:
            result["volume"] = m.group(1)
            result["issue"] = m.group(2)
        return result

    def _iter_dspace_pdf_candidates(
        self, seed_url: str, *, scope_id: Optional[str] = None
    ) -> Iterable[DiscoveryResult]:
        resolved_scope = (
            scope_id if scope_id is not None else self._extract_dspace_scope_id(seed_url)
        )
        parsed = urlparse(seed_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        discover_url = f"{base}/server/api/discover/search/objects"

        seen_pdf_urls: Set[str] = set()
        page = 0
        size = 20
        max_pages = 20

        for _ in range(max_pages):
            payload = self._get_json(
                discover_url,
                params={
                    **({"scope": resolved_scope} if resolved_scope else {}),
                    "size": size,
                    "page": page,
                },
            )
            if not payload:
                break
            objects = (
                payload.get("_embedded", {})
                .get("searchResult", {})
                .get("_embedded", {})
                .get("objects", [])
            )
            if not isinstance(objects, list) or not objects:
                break

            for entry in objects:
                item = (entry.get("_embedded") or {}).get("indexableObject") or {}
                if (item.get("type") or "").lower() != "item":
                    continue
                item_id = item.get("id") or item.get("uuid")
                if not item_id:
                    continue
                bundles_href = ((item.get("_links") or {}).get("bundles") or {}).get("href")
                if not bundles_href:
                    continue

                bundles = self._get_json(bundles_href, params={"embed": "bitstreams", "size": 100})
                if not bundles:
                    continue
                bundles_arr = (bundles.get("_embedded") or {}).get("bundles") or []
                if not isinstance(bundles_arr, list):
                    continue

                page_url = f"{base}/items/{item_id}"
                metadata = self._extract_dspace_metadata(item, page_url)

                for bundle in bundles_arr:
                    bundle_name = str(bundle.get("name") or "").upper()
                    bitstreams = (bundle.get("_embedded") or {}).get("bitstreams") or []
                    if isinstance(bitstreams, dict):
                        bitstreams = (bitstreams.get("_embedded") or {}).get("bitstreams") or []
                    if not isinstance(bitstreams, list):
                        continue

                    for bitstream in bitstreams:
                        name = str(bitstream.get("name") or "").lower()
                        content_href = ((bitstream.get("_links") or {}).get("content") or {}).get(
                            "href"
                        )
                        if not content_href:
                            continue
                        if bundle_name != "ORIGINAL" and not name.endswith(".pdf"):
                            continue
                        if content_href in seen_pdf_urls:
                            continue
                        seen_pdf_urls.add(content_href)
                        yield DiscoveryResult(
                            page_url=page_url,
                            pdf_url=content_href,
                            metadata=dict(metadata),
                            extraction_path="dspace_api",
                        )
            page += 1

    def _collect_archive_probe_urls(self, seed_url: str) -> List[str]:
        parsed = urlparse(seed_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path.rstrip("/")

        probes = [
            f"{base}/archive",
            f"{base}/archives",
            f"{base}/issues",
            f"{base}/issue/archive",
            f"{base}/all_issues.html",
            f"{base}/past-issues",
            f"{base}/print-issues",
            f"{base}/current-issue",
            f"{base}/work",
            f"{base}/content",
        ]
        if path:
            probes.extend(
                [
                    f"{base}{path}/archive",
                    f"{base}{path}/archives",
                    f"{base}{path}/issues",
                    f"{base}{path}/issue/archive",
                    f"{base}{path}/all_issues.html",
                    f"{base}{path}/past-issues",
                    f"{base}{path}/print-issues",
                ]
            )

        deduped: List[str] = []
        seen: Set[str] = set()
        for candidate in probes:
            normalized = candidate.rstrip("/")
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
            if len(deduped) >= 25:
                break
        return deduped

    def _is_high_signal_link(self, link: str, anchor_text: str, seed_origin: str) -> bool:
        parsed = urlparse(link)
        if parsed.scheme not in {"http", "https"}:
            return False
        if parsed.netloc != seed_origin:
            return False
        haystack = f"{link} {anchor_text}".lower()
        return any(token in haystack for token in FALLBACK_LINK_HINTS)

    def _discover_with_params(
        self,
        *,
        seed_url: str,
        max_depth: int,
        targeted_only: bool,
        max_pages: int,
    ) -> Iterable[DiscoveryResult]:
        seen_pages: Set[str] = set()
        seen_pdf_urls: Set[str] = set()
        queue: Deque[Tuple[str, int]] = deque([(seed_url, 0)])
        seed_origin = urlparse(seed_url).netloc

        if targeted_only:
            for probe in self._collect_archive_probe_urls(seed_url):
                queue.append((probe, 1))

        while queue and len(seen_pages) < max_pages:
            page_url, depth = queue.popleft()
            if page_url in seen_pages:
                continue
            seen_pages.add(page_url)

            resp = self._get(page_url)
            if not resp or not resp.content:
                continue

            soup = BeautifulSoup(resp.text, "lxml")
            links = []
            for a in soup.find_all("a", href=True):
                anchor_text = a.get_text(" ", strip=True)
                links.append((urljoin(page_url, a["href"]), anchor_text, a))

            for link, anchor_text, a_tag in links:
                link_lower = (link or "").strip().lower()
                page_lower = (page_url or "").strip().lower()
                anchor_lower = (anchor_text or "").lower()
                same_page = link_lower.rstrip("/") == page_lower.rstrip("/")
                parsed_link = urlparse(link)
                same_origin = parsed_link.netloc == seed_origin
                anchor_pdf_hint = "pdf" in anchor_lower
                direct_pdf_url = is_pdf_url(link)
                same_origin_pdf_endpoint = same_origin and self._is_probable_pdf_endpoint(link)
                hinted_same_origin_endpoint = (
                    anchor_pdf_hint
                    and same_origin
                    and not same_page
                    and self._is_probable_pdf_endpoint(link)
                )
                looks_like_candidate = (
                    direct_pdf_url or same_origin_pdf_endpoint or hinted_same_origin_endpoint
                )
                if looks_like_candidate:
                    if link in seen_pdf_urls:
                        continue
                    seen_pdf_urls.add(link)
                    metadata = self._extract_generic_metadata(soup, page_url, link, a_tag)
                    yield DiscoveryResult(page_url=page_url, pdf_url=link, metadata=metadata)

            for embedded_pdf in self._iter_embedded_pdf_urls(resp.text, page_url):
                if embedded_pdf in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(embedded_pdf)
                url_meta = self._extract_vol_issue_from_url(embedded_pdf)
                yield DiscoveryResult(
                    page_url=page_url,
                    pdf_url=embedded_pdf,
                    extraction_path="embedded_text",
                    metadata=url_meta,
                )

            if depth < max_depth:
                for link, anchor_text, _ in links:
                    parsed = urlparse(link)
                    if parsed.scheme not in {"http", "https"} or parsed.netloc != seed_origin:
                        continue
                    if targeted_only and not self._is_high_signal_link(
                        link, anchor_text, seed_origin
                    ):
                        continue
                    queue.append((link, depth + 1))

            # DSpace 7 collection pages are JS-rendered and expose PDFs through REST APIs.
            if depth == 0:
                scope_id = self._extract_dspace_scope_id(page_url)
                looks_like_dspace_shell = (
                    "<title>DSpace</title>" in resp.text
                    or "dspace" in (resp.text or "").lower()
                    or not links
                )
                if looks_like_dspace_shell:
                    yielded = False
                    for dspace_result in self._iter_dspace_pdf_candidates(
                        page_url, scope_id=scope_id or None
                    ):
                        yielded = True
                        yield dspace_result
                    # Fallback for handle-based DSpace seeds where scope id isn't in URL.
                    if not yielded and "/handle/" in (page_url or "").lower():
                        yield from self._iter_dspace_pdf_candidates(page_url, scope_id="")

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        seen_pdf_urls: Set[str] = set()
        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=max_depth,
            targeted_only=False,
            max_pages=200,
        ):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result

        # Smoke runs often use depth=0; perform a bounded, high-signal fallback crawl.
        if seen_pdf_urls or max_depth > 0:
            return

        for result in self._discover_with_params(
            seed_url=seed_url,
            max_depth=3,
            targeted_only=True,
            max_pages=160,
        ):
            if result.pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(result.pdf_url)
            yield result

    def _classify_waf_response(
        self,
        *,
        pdf_url: str,
        status_code: int,
        content_type: str,
        waf_action: str,
    ) -> tuple[bool, str]:
        lowered_url = (pdf_url or "").lower()
        lowered_ctype = (content_type or "").lower()
        lowered_waf = (waf_action or "").lower()

        if status_code == 202 and lowered_waf == "challenge":
            return True, waf_action

        # Digital Commons and similar PDF endpoints often return 403 HTML challenge pages
        # while still being valid PDF targets.
        parsed_host = (urlparse(lowered_url).netloc or "").lower()
        digital_commons_like = (
            "viewcontent.cgi" in lowered_url
            or "digitalcommons." in parsed_host
            or parsed_host.startswith("commons.")
            or "scholarship." in parsed_host
            or "repository." in parsed_host
            or "engagedscholarship." in parsed_host
        )
        if status_code in {401, 403} and "text/html" in lowered_ctype and digital_commons_like:
            return True, waf_action or "challenge_403"

        # Some WordPress/CDN hosts return HTML challenge pages for uploaded PDFs.
        wordpress_pdf_like = "wp-content/uploads/" in lowered_url
        if (
            status_code in {401, 403}
            and "text/html" in lowered_ctype
            and (wordpress_pdf_like or lowered_url.endswith(".pdf"))
        ):
            return True, waf_action or "challenge_403"

        return False, waf_action

    def download_pdf(
        self, pdf_url: str, out_dir: str, referer: str = "", **_: Any
    ) -> Optional[str]:
        self._set_download_meta(
            ok=False,
            error_type="unknown",
            message="download not attempted",
            status_code=0,
            content_type="",
            pdf_sha256=None,
            pdf_size_bytes=None,
            waf_action="",
            download_method="requests",
        )
        try:
            if not pre_validate_pdf_url(self.session, pdf_url):
                self._set_download_meta(
                    ok=False,
                    error_type="precheck_failed",
                    message="HEAD pre-validation failed for PDF URL",
                    status_code=0,
                    content_type="",
                    pdf_sha256=None,
                    pdf_size_bytes=None,
                    waf_action="",
                    download_method="requests",
                )
                return None

            out_dir = os.fspath(out_dir)
            os.makedirs(out_dir, exist_ok=True)
            status_code = 0
            content_type = ""
            waf_action = ""
            request_headers = dict(DEFAULT_HEADERS)
            parsed_pdf = urlparse(pdf_url)
            if parsed_pdf.scheme and parsed_pdf.netloc:
                request_headers["Referer"] = f"{parsed_pdf.scheme}://{parsed_pdf.netloc}/"
            with self.session.get(
                pdf_url,
                headers=request_headers,
                stream=True,
                timeout=(15, 120),
                verify=request_verify_for_url(pdf_url),
            ) as r:
                status_code = int(r.status_code)
                content_type = r.headers.get("Content-Type", "")
                waf_action = r.headers.get("x-amzn-waf-action", "")
                is_waf, waf_action = self._classify_waf_response(
                    pdf_url=pdf_url,
                    status_code=status_code,
                    content_type=content_type,
                    waf_action=str(waf_action or ""),
                )
                if is_waf:
                    self._set_download_meta(
                        ok=False,
                        error_type="waf_challenge",
                        message="Blocked by WAF challenge; browser-mediated download required",
                        status_code=status_code,
                        content_type=content_type,
                        pdf_sha256=None,
                        pdf_size_bytes=None,
                        waf_action=str(waf_action or ""),
                        download_method="requests",
                    )
                    return None
                if r.status_code >= 400:
                    recovered, recovered_status, recovered_content_type = (
                        self._retry_plone_pdf_variant_download(
                            pdf_url=pdf_url,
                            path=os.path.join(
                                out_dir,
                                self._resolve_filename(pdf_url, r.headers, out_dir),
                            ),
                            request_headers=request_headers,
                        )
                    )
                    if recovered:
                        path = os.path.join(
                            out_dir, self._resolve_filename(pdf_url, r.headers, out_dir)
                        )
                        status_code = recovered_status or status_code
                        content_type = recovered_content_type or content_type
                        pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(path)
                        self._set_download_meta(
                            ok=True,
                            error_type="",
                            message="",
                            status_code=status_code,
                            content_type=content_type,
                            pdf_sha256=pdf_sha256,
                            pdf_size_bytes=pdf_size_bytes,
                            waf_action=str(waf_action or ""),
                            download_method="requests",
                        )
                        return path
                    self._set_download_meta(
                        ok=False,
                        error_type="http_error",
                        message=f"HTTP status {r.status_code}",
                        status_code=r.status_code,
                        content_type=content_type,
                        pdf_sha256=None,
                        pdf_size_bytes=None,
                        waf_action=str(waf_action or ""),
                        download_method="requests",
                    )
                    return None
                filename = self._resolve_filename(pdf_url, r.headers, out_dir)
                path = os.path.join(out_dir, filename)
                bytes_written = 0
                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        if chunk:
                            bytes_written += len(chunk)
                            f.write(chunk)
                if bytes_written == 0:
                    try:
                        os.remove(path)
                    except OSError:
                        pass
                    self._set_download_meta(
                        ok=False,
                        error_type="empty_response",
                        message="Download returned zero bytes",
                        status_code=status_code,
                        content_type=content_type,
                        pdf_sha256=None,
                        pdf_size_bytes=None,
                        waf_action=str(waf_action or ""),
                        download_method="requests",
                    )
                    return None

            if not validate_pdf_magic_bytes(path):
                recovered, recovered_status, recovered_content_type = (False, 0, "")
                if "/bitstream/handle/" in (pdf_url or "").lower():
                    recovered, recovered_status, recovered_content_type = (
                        self._retry_dspace_bitstream_download(pdf_url, path)
                    )
                if recovered:
                    status_code = recovered_status or status_code
                    content_type = recovered_content_type or content_type
                    pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(path)
                    self._set_download_meta(
                        ok=True,
                        error_type="",
                        message="",
                        status_code=status_code,
                        content_type=content_type,
                        pdf_sha256=pdf_sha256,
                        pdf_size_bytes=pdf_size_bytes,
                        waf_action=str(waf_action or ""),
                        download_method="requests",
                    )
                    return path
                recovered, recovered_status, recovered_content_type = (
                    self._retry_plone_pdf_variant_download(
                        pdf_url=pdf_url,
                        path=path,
                        request_headers=request_headers,
                    )
                )
                if recovered:
                    status_code = recovered_status or status_code
                    content_type = recovered_content_type or content_type
                    pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(path)
                    self._set_download_meta(
                        ok=True,
                        error_type="",
                        message="",
                        status_code=status_code,
                        content_type=content_type,
                        pdf_sha256=pdf_sha256,
                        pdf_size_bytes=pdf_size_bytes,
                        waf_action=str(waf_action or ""),
                        download_method="requests",
                    )
                    return path
                recovered_path, recovered_status, recovered_content_type = (
                    self._retry_viewcontent_from_referer(
                        pdf_url=pdf_url,
                        referer=referer,
                        out_dir=out_dir,
                        request_headers=request_headers,
                    )
                )
                if recovered_path:
                    status_code = recovered_status or status_code
                    content_type = recovered_content_type or content_type
                    pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(recovered_path)
                    self._set_download_meta(
                        ok=True,
                        error_type="",
                        message="",
                        status_code=status_code,
                        content_type=content_type,
                        pdf_sha256=pdf_sha256,
                        pdf_size_bytes=pdf_size_bytes,
                        waf_action=str(waf_action or ""),
                        download_method="requests",
                        fallback_used=True,
                    )
                    return recovered_path
                try:
                    os.remove(path)
                except OSError:
                    pass
                self._set_download_meta(
                    ok=False,
                    error_type="invalid_pdf",
                    message="Downloaded content is not a valid PDF magic header",
                    status_code=status_code,
                    content_type=content_type,
                    pdf_sha256=None,
                    pdf_size_bytes=None,
                    waf_action=str(waf_action or ""),
                    download_method="requests",
                )
                return None

            pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(path)
            self._set_download_meta(
                ok=True,
                error_type="",
                message="",
                status_code=status_code,
                content_type=content_type,
                pdf_sha256=pdf_sha256,
                pdf_size_bytes=pdf_size_bytes,
                waf_action=str(waf_action or ""),
                download_method="requests",
            )
            return path
        except requests.Timeout:
            self._set_download_meta(
                ok=False,
                error_type="timeout",
                message="Request timed out while downloading PDF",
                status_code=0,
                content_type="",
                pdf_sha256=None,
                pdf_size_bytes=None,
                waf_action="",
                download_method="requests",
            )
            return None
        except requests.RequestException as exc:
            self._set_download_meta(
                ok=False,
                error_type="network",
                message=str(exc),
                status_code=0,
                content_type="",
                pdf_sha256=None,
                pdf_size_bytes=None,
                waf_action="",
                download_method="requests",
            )
            return None
        except OSError as exc:
            self._set_download_meta(
                ok=False,
                error_type="filesystem",
                message=str(exc),
                status_code=0,
                content_type="",
                pdf_sha256=None,
                pdf_size_bytes=None,
                waf_action="",
                download_method="requests",
            )
            return None

    def _resolve_filename(
        self, pdf_url: str, headers: requests.structures.CaseInsensitiveDict, out_dir: str
    ) -> str:
        """Normalize filenames using HTTP hints and ensure uniqueness."""

        candidates = []

        # Content-Disposition hints have highest precedence
        cd = headers.get("Content-Disposition")
        if cd:
            cd_name = self._filename_from_content_disposition(cd)
            if cd_name:
                candidates.append(cd_name)

        # Fallback to URL path
        parsed = urlparse(pdf_url)
        if parsed.path:
            candidates.append(os.path.basename(parsed.path))

        # Use slugified host if everything else fails
        if not candidates:
            candidates.append(parsed.netloc or "download")

        filename = None
        for raw_candidate in candidates:
            cleaned = self._clean_filename(raw_candidate)
            if cleaned:
                filename = cleaned
                break

        if not filename:
            filename = f"download-{int(time.time())}.pdf"

        filename = self._force_pdf_extension(filename)
        filename = self._slugify_filename(filename)
        filename = self._ensure_unique_filename(filename, out_dir)

        return filename

    def _ensure_unique_filename(self, filename: str, out_dir: str) -> str:
        base, ext = os.path.splitext(filename)
        if not base:
            base = "download"
        if not ext:
            ext = ".pdf"

        candidate = f"{base}{ext}"
        counter = 2
        while os.path.exists(os.path.join(out_dir, candidate)):
            candidate = f"{base}-{counter}{ext}"
            counter += 1
        return candidate

    def _clean_filename(self, filename: str) -> str:
        filename = filename.split("?")[0].split("#")[0]
        filename = filename.strip().strip('"').strip("'")
        filename = unquote(filename)
        filename = filename.replace("/", "-").replace("\\", "-")
        return filename

    def _force_pdf_extension(self, filename: str) -> str:
        name, ext = os.path.splitext(filename)
        if ext.lower() != ".pdf":
            filename = f"{name}.pdf"
        return filename

    def _slugify_filename(self, filename: str) -> str:
        name, ext = os.path.splitext(filename)
        slug = self._slugify(name)
        if not slug:
            slug = f"download-{int(time.time())}"
        return f"{slug}{ext.lower() or '.pdf'}"

    def _slugify(self, value: str) -> str:
        value = unicodedata.normalize("NFKC", value)
        value = value.strip()
        # Replace non-word characters with hyphen
        value = re.sub(r"[^0-9A-Za-z]+", "-", value)
        value = re.sub(r"-+", "-", value)
        return value.strip("-").lower()

    def _filename_from_content_disposition(self, header_value: str) -> Optional[str]:
        parts = [part.strip() for part in header_value.split(";")]
        filename = None
        for part in parts:
            if part.lower().startswith("filename*="):
                value = part.split("=", 1)[1]
                if "''" in value:
                    _, value = value.split("''", 1)
                filename = value
                break
            if part.lower().startswith("filename="):
                filename = part.split("=", 1)[1]
                break
        if not filename:
            return None
        filename = filename.strip('"')
        filename = unquote(filename)
        return filename
