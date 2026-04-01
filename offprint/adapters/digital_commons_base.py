from __future__ import annotations

import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from ..digital_commons_download import (
    RobotsCache,
    SessionRotationManager,
    download_pdf_dc,
    parse_ua_profiles,
)
from ..digital_commons_enumerator import discover_oai, discover_sitemap, merge_dedupe
from .generic import DEFAULT_HEADERS


class DigitalCommonsBaseAdapter(Adapter):
    """Base adapter for Digital Commons (BePress) law review sites.

    Digital Commons sites follow a consistent pattern:
    1. /all_issues.html page with issue links (.issue a or h3 a)
    2. Issue pages contain article containers (div.doc)
    3. Each article has title, author, and PDF link (.pdf a)
    4. Metadata in structured paragraph elements
    """

    _waf_state_lock = threading.Lock()
    _waf_state: Dict[str, Dict[str, float]] = {}
    _subscription_state_lock = threading.Lock()
    _subscription_blocked_scopes: Set[str] = set()
    # Persistent known login/subscription-only scopes discovered via probes.
    _known_subscription_blocked_scopes: Set[str] = {
        "scholarship.law.nd.edu|ajj",
    }
    _session_rotation_manager: Optional[SessionRotationManager] = None
    _session_rotation_lock = threading.Lock()
    
    # Singleton browser downloader for fallbacks
    _browser_downloader: Optional[Any] = None
    _browser_downloader_lock = threading.Lock()
    _browser_semaphore = threading.Semaphore(2) # Limit to 2 concurrent browsers globally

    @classmethod
    def _get_session_rotation_manager(cls, threshold: int = 300) -> SessionRotationManager:
        """Get or create the shared session rotation manager."""
        with cls._session_rotation_lock:
            if cls._session_rotation_manager is None:
                cls._session_rotation_manager = SessionRotationManager(rotate_threshold=threshold)
            return cls._session_rotation_manager

    def __init__(self, session=None):
        super().__init__(session)
        # These can be overridden by subclasses for site-specific variations
        self.issue_selector = ".issue a"
        self.article_container_selector = "div.doc"
        self.pdf_link_selector = ".pdf a"
        self.title_selector = "p:nth-of-type(2) a"
        self.author_selector = ".auth"
        self.date_selector = "p.index_date"
        self.dc_enum_mode = "oai_sitemap_union"
        self.dc_use_siteindex = True
        self.dc_ua_profiles = ["browser", "transparent", "python_requests", "wget", "curl"]
        self.dc_robots_enforce = True
        self.dc_max_oai_records = 100
        self.dc_max_sitemap_urls = 100
        self.dc_download_timeout = 30
        self.dc_min_domain_delay_ms = 2000
        self.dc_max_domain_delay_ms = 4000
        self.dc_waf_fail_threshold = 3
        self.dc_waf_cooldown_seconds = 900
        self.dc_waf_browser_fallback = True
        self.dc_browser_backend = "camoufox"
        self.dc_disable_unscoped_oai_no_slug = True
        self.dc_allow_generic_fallback = False
        self.dc_session_rotate_threshold = 300
        self.dc_use_curl_cffi = True
        self._robots_cache = RobotsCache()
        self._issue_skip_urls: Set[str] = set()
        self._on_issue_complete: Optional[Callable[[str, int], None]] = None

    def configure_dc(
        self,
        *,
        enum_mode: str = "oai_sitemap_union",
        use_siteindex: bool = True,
        ua_profiles: Optional[List[str]] = None,
        robots_enforce: bool = True,
        max_oai_records: int = 0,
        max_sitemap_urls: int = 0,
        download_timeout: int = 30,
        min_domain_delay_ms: int = 2000,
        max_domain_delay_ms: int = 4000,
        waf_fail_threshold: int = 3,
        waf_cooldown_seconds: int = 900,
        disable_unscoped_oai_no_slug: bool = True,
        allow_generic_fallback: bool = False,
        session_rotate_threshold: int = 300,
        use_curl_cffi: bool = True,
    ) -> None:
        self.dc_enum_mode = enum_mode or "oai_sitemap_union"
        self.dc_use_siteindex = bool(use_siteindex)
        self.dc_ua_profiles = list(ua_profiles or self.dc_ua_profiles)
        self.dc_robots_enforce = bool(robots_enforce)
        self.dc_max_oai_records = max(int(max_oai_records or 0), 0)
        self.dc_max_sitemap_urls = max(int(max_sitemap_urls or 0), 0)
        self.dc_download_timeout = max(int(download_timeout or 30), 1)
        self.dc_min_domain_delay_ms = max(int(min_domain_delay_ms or 0), 0)
        self.dc_max_domain_delay_ms = max(int(max_domain_delay_ms or 0), 0)
        self.dc_waf_fail_threshold = max(int(waf_fail_threshold or 0), 0)
        self.dc_waf_cooldown_seconds = max(int(waf_cooldown_seconds or 0), 0)
        self.dc_disable_unscoped_oai_no_slug = bool(disable_unscoped_oai_no_slug)
        self.dc_allow_generic_fallback = bool(allow_generic_fallback)
        self.dc_session_rotate_threshold = max(int(session_rotate_threshold or 0), 0)
        self.dc_use_curl_cffi = bool(use_curl_cffi)

    def _is_unscoped_oai_allowed(self, seed_url: str) -> bool:
        if not self.dc_disable_unscoped_oai_no_slug:
            return True
        return bool(self._seed_slug(seed_url))

    @classmethod
    def _check_waf_circuit(cls, host: str) -> tuple[bool, float, bool]:
        now = time.time()
        with cls._waf_state_lock:
            entry = cls._waf_state.get(host) or {}
            open_until = float(entry.get("open_until", 0.0) or 0.0)
            if open_until and open_until > now:
                return True, max(open_until - now, 0.0), False
            if open_until:
                cls._waf_state[host] = {"streak": 0.0, "open_until": 0.0}
                return False, 0.0, True
            return False, 0.0, False

    @staticmethod
    def _journal_scope_from_url(url: str) -> str:
        parsed = urlparse(url or "")
        path = (parsed.path or "").strip("/")
        if not path:
            return ""
        return path.split("/", 1)[0].strip().lower()

    def _waf_scope_key(self, *, host: str, seed_url: str = "", referer: str = "") -> str:
        scope = self._journal_scope_from_url(seed_url) or self._journal_scope_from_url(referer)
        if not scope:
            return f"{host}|__host__"
        return f"{host}|{scope}"

    @classmethod
    def _is_subscription_blocked_scope(cls, scope_key: str) -> bool:
        with cls._subscription_state_lock:
            return scope_key in cls._subscription_blocked_scopes or scope_key in cls._known_subscription_blocked_scopes

    @classmethod
    def _mark_subscription_blocked_scope(cls, scope_key: str) -> None:
        with cls._subscription_state_lock:
            cls._subscription_blocked_scopes.add(scope_key)

    @classmethod
    def _record_waf_failure(
        cls,
        host: str,
        *,
        fail_threshold: int,
        cooldown_seconds: int,
    ) -> tuple[bool, int]:
        with cls._waf_state_lock:
            entry = cls._waf_state.setdefault(host, {"streak": 0.0, "open_until": 0.0})
            streak = int(entry.get("streak", 0.0) or 0) + 1
            entry["streak"] = float(streak)
            opened = False
            if fail_threshold > 0 and streak >= fail_threshold and cooldown_seconds > 0:
                entry["open_until"] = time.time() + float(cooldown_seconds)
                opened = True
            cls._waf_state[host] = entry
            return opened, streak

    @classmethod
    def _record_waf_success(cls, host: str) -> None:
        with cls._waf_state_lock:
            if host in cls._waf_state:
                cls._waf_state[host] = {"streak": 0.0, "open_until": 0.0}

    def configure_issue_checkpoint(
        self,
        *,
        skip_issue_urls: Optional[Set[str]] = None,
        on_issue_complete: Optional[Callable[[str, int], None]] = None,
    ) -> None:
        self._issue_skip_urls = set(skip_issue_urls or set())
        self._on_issue_complete = on_issue_complete

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a page."""
        try:
            resp = self.session.get(url, headers=DEFAULT_HEADERS, timeout=20)
            if resp.status_code >= 400:
                return None
            return BeautifulSoup(resp.text, "lxml")
        except Exception:
            return None

    def _build_oai_endpoint(self, seed_url: str) -> str:
        parsed = urlparse(seed_url)
        return f"{parsed.scheme}://{parsed.netloc}/do/oai/"

    def _seed_slug(self, seed_url: str) -> str:
        parsed = urlparse(seed_url)
        parts = [p for p in parsed.path.split("/") if p]
        if not parts:
            return ""
        # Typical Digital Commons journal seeds are https://host/<slug>/...
        return parts[0].strip().lower()

    def _request_oai_xml(self, endpoint: str, params: dict) -> Optional[ET.Element]:
        try:
            resp = self.session.get(endpoint, params=params, headers=DEFAULT_HEADERS, timeout=30)
            if resp.status_code >= 400 or not resp.content:
                return None
            return ET.fromstring(resp.content)
        except Exception:
            return None

    def _extract_oai_identifiers(self, record: ET.Element) -> list[str]:
        ns = {
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "dc": "http://purl.org/dc/elements/1.1/",
        }
        identifiers: list[str] = []
        for elem in record.findall(".//dc:identifier", ns):
            value = (elem.text or "").strip()
            if value:
                identifiers.append(value)
        return identifiers

    def _normalize_oai_pdf_url(self, pdf_url: str) -> str:
        lowered = pdf_url.lower()
        if "cgi/viewcontent.cgi" in lowered and "type=pdf" not in lowered:
            sep = "&" if "?" in pdf_url else "?"
            return f"{pdf_url}{sep}type=pdf"
        return pdf_url

    def _extract_oai_record_metadata(
        self,
        record: ET.Element,
        seed_url: str,
        landing_url: str,
    ) -> dict:
        ns = {
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "dc": "http://purl.org/dc/elements/1.1/",
        }
        metadata: dict = {"source_url": landing_url or seed_url}

        title = record.find(".//dc:title", ns)
        if title is not None and (title.text or "").strip():
            metadata["title"] = (title.text or "").strip()

        authors = [(elem.text or "").strip() for elem in record.findall(".//dc:creator", ns)]
        authors = [a for a in authors if a]
        if authors:
            metadata["authors"] = authors

        date_elem = record.find(".//dc:date", ns)
        if date_elem is not None and (date_elem.text or "").strip():
            metadata["date"] = (date_elem.text or "").strip()

        source_elem = record.find(".//dc:source", ns)
        source_text = (source_elem.text or "").strip() if source_elem is not None else ""
        if source_text:
            metadata["source"] = source_text

        if landing_url:
            metadata["url"] = landing_url

        combined = " ".join(
            [
                str(metadata.get("title") or ""),
                str(metadata.get("date") or ""),
                source_text,
                landing_url,
            ]
        )
        year_match = re.search(r"\b(19|20)\d{2}\b", combined)
        if year_match:
            metadata["year"] = year_match.group(0)

        volume_match = re.search(r"/vol(?:ume)?/?(\d+)|\bvol(?:ume)?\.?\s*(\d+)\b", combined, re.I)
        if volume_match:
            metadata["volume"] = next((g for g in volume_match.groups() if g), "")

        issue_match = re.search(
            r"/iss(?:ue)?/?(\d+)|\b(?:issue|no\.?)\s*(\d+)\b", combined, re.I
        )
        if issue_match:
            metadata["issue"] = next((g for g in issue_match.groups() if g), "")

        return metadata

    def _pick_oai_urls(
        self,
        identifiers: list[str],
        *,
        seed_url: str,
    ) -> tuple[Optional[str], Optional[str]]:
        parsed_seed = urlparse(seed_url)
        seed_host = (parsed_seed.netloc or "").lower()
        slug = self._seed_slug(seed_url)

        landing_url: Optional[str] = None
        pdf_url: Optional[str] = None
        for value in identifiers:
            parsed = urlparse(value)
            if parsed.scheme not in {"http", "https"}:
                continue
            if (parsed.netloc or "").lower() != seed_host:
                continue
            lowered = value.lower()
            in_scope = (
                (not slug)
                or (f"/{slug}/" in lowered)
                or (f"/context/{slug}/" in lowered)
                or (f"context={slug}" in lowered)
            )
            if not in_scope:
                continue

            if (
                lowered.endswith(".pdf")
                or "/viewcontent/" in lowered
                or "viewcontent.cgi" in lowered
                or "/files/" in lowered
            ):
                pdf_url = self._normalize_oai_pdf_url(value)
            elif landing_url is None:
                landing_url = value

            if pdf_url and landing_url:
                break

        return landing_url, pdf_url

    def _discover_via_oai(
        self,
        seed_url: str,
        *,
        max_pages: int = 12,
        max_records: int = 500,
    ) -> Iterable[DiscoveryResult]:
        ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        endpoint = self._build_oai_endpoint(seed_url)
        slug = self._seed_slug(seed_url)
        set_candidates = [f"publication:{slug}"] if slug else []
        if self._is_unscoped_oai_allowed(seed_url):
            set_candidates.append("")

        seen_pdf_urls: Set[str] = set()
        for set_spec in set_candidates:
            pages = 0
            yielded = 0
            token = ""
            while pages < max_pages and yielded < max_records:
                pages += 1
                if token:
                    params = {"verb": "ListRecords", "resumptionToken": token}
                else:
                    params = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
                    if set_spec:
                        params["set"] = set_spec

                root = self._request_oai_xml(endpoint, params)
                if root is None:
                    break

                records = root.findall(".//oai:record", ns)
                if not records and not token:
                    # No scoped records; try unscoped set.
                    break

                for record in records:
                    header = record.find("oai:header", ns)
                    if header is not None and (header.get("status") or "").lower() == "deleted":
                        continue

                    identifiers = self._extract_oai_identifiers(record)
                    if not identifiers:
                        continue

                    landing_url, pdf_url = self._pick_oai_urls(identifiers, seed_url=seed_url)
                    if not pdf_url or pdf_url in seen_pdf_urls:
                        continue

                    seen_pdf_urls.add(pdf_url)
                    yielded += 1
                    metadata = self._extract_oai_record_metadata(
                        record,
                        seed_url=seed_url,
                        landing_url=landing_url or seed_url,
                    )
                    yield DiscoveryResult(
                        page_url=landing_url or seed_url,
                        pdf_url=pdf_url,
                        metadata=metadata,
                        extraction_path="oai_pmh",
                    )
                    if yielded >= max_records:
                        break

                token_elem = root.find(".//oai:resumptionToken", ns)
                token = (token_elem.text or "").strip() if token_elem is not None else ""
                if not token:
                    break

            if seen_pdf_urls:
                # If scoped OAI produced candidates, skip wide unscoped crawl.
                break

    def _discover_via_all_issues(self, seed_url: str) -> Iterable[DiscoveryResult]:
        """Discover PDFs through all_issues + issue page HTML traversal."""
        seen_pdfs: Set[str] = set()
        # Step 1: Get the seed page (ideally /all_issues.html)
        soup = self._get_page(seed_url)
        if not soup:
            return

        # Step 2: Find all issue links
        issue_links = soup.select(self.issue_selector)
        if not issue_links:
            # If the seed URL isn't an all_issues page, try the common suffix once.
            if "all_issues.html" not in seed_url:
                candidate = seed_url.rstrip("/") + "/all_issues.html"
                candidate_soup = self._get_page(candidate)
                if candidate_soup:
                    candidate_links = candidate_soup.select(self.issue_selector)
                    if candidate_links:
                        seed_url = candidate
                        soup = candidate_soup
                        issue_links = candidate_links

        if not issue_links:
            # Try to find an "All Issues" link on the page.
            for a in soup.select("a[href]"):
                text = (a.get_text(" ", strip=True) or "").lower()
                href = a.get("href") or ""
                if not href:
                    continue
                if "all issues" in text or "all_issues" in href.lower():
                    candidate = urljoin(seed_url, href)
                    candidate_soup = self._get_page(candidate)
                    if candidate_soup:
                        candidate_links = candidate_soup.select(self.issue_selector)
                        if candidate_links:
                            seed_url = candidate
                            soup = candidate_soup
                            issue_links = candidate_links
                            break
        if not issue_links:
            # Last resort: broader selector for some Digital Commons templates.
            issue_links = soup.select("h3 a")

        print(f"ℹ️  Found {len(issue_links)} issue links on {seed_url}")

        seen_urls: Set[str] = set()

        for issue_link in issue_links:
            issue_url = urljoin(seed_url, issue_link.get("href", ""))
            if not issue_url or issue_url in seen_urls:
                continue
            seen_urls.add(issue_url)
            if issue_url in self._issue_skip_urls:
                continue

            # Step 3: Process each issue page
            emitted = 0
            for result in self._process_issue_page(issue_url):
                if result.pdf_url in seen_pdfs:
                    continue
                seen_pdfs.add(result.pdf_url)
                emitted += 1
                yield result
            if callable(self._on_issue_complete):
                try:
                    self._on_issue_complete(issue_url, emitted)
                except Exception:
                    pass

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        """Discover PDFs via OAI + sitemap + HTML fallback.

        When the seed is a sitemap.xml URL, we use sitemap-only mode to avoid
        duplicative HTML hop traversal. Otherwise, we try OAI + sitemap first
        and only fall back to all_issues HTML traversal if they produce nothing.
        """
        del max_depth
        adapter_name = self.__class__.__name__
        seen_urls: Set[str] = set()
        yielded_any = False
        union_merge_mode = str(os.getenv("LRS_DC_UNION_MERGE", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        
        # If seed is a sitemap.xml URL, use sitemap-only mode to avoid duplicative traversal
        seed_lower = seed_url.lower()
        is_sitemap_seed = seed_lower.endswith("sitemap.xml") or "/sitemap" in seed_lower
        effective_enum_mode = "sitemap_only" if is_sitemap_seed else self.dc_enum_mode
        
        if is_sitemap_seed and self.dc_enum_mode == "oai_sitemap_union":
            print(
                "ℹ️  [dc] Sitemap seed detected, using sitemap-only mode (skipping all_issues hop)",
                flush=True,
            )

        def _normalize_result(result: DiscoveryResult) -> DiscoveryResult:
            if not result.source_adapter:
                result.source_adapter = adapter_name
            if not result.extraction_path:
                result.extraction_path = "all_issues"
            result.metadata = dict(result.metadata or {})
            result.metadata.setdefault("dc_set_spec", "")
            if result.metadata.get("dc_source") not in {
                "oai",
                "siteindex",
                "all_issues",
                "hybrid",
            }:
                path = (result.extraction_path or "").lower()
                if "oai" in path:
                    result.metadata["dc_source"] = "oai"
                elif "siteindex" in path or "sitemap" in path:
                    result.metadata["dc_source"] = "siteindex"
                else:
                    result.metadata["dc_source"] = "all_issues"
            return result

        if effective_enum_mode == "oai_sitemap_union":
            if union_merge_mode:
                print(
                    f"ℹ️  [dc] Using merge mode for {seed_url} (set LRS_DC_UNION_MERGE=0 to stream)",
                    flush=True,
                )
                oai_results = list(
                    discover_oai(
                        seed_url,
                        session=self.session,
                        max_records=self.dc_max_oai_records,
                        timeout=self.dc_download_timeout,
                        allow_unscoped_fallback=self._is_unscoped_oai_allowed(seed_url),
                    )
                )
                sitemap_results = list(
                    discover_sitemap(
                        seed_url,
                        session=self.session,
                        use_siteindex=self.dc_use_siteindex,
                        max_urls=self.dc_max_sitemap_urls,
                        timeout=self.dc_download_timeout,
                    )
                )
                # Only run all_issues if OAI + sitemap produced nothing (avoid duplicative work)
                if oai_results or sitemap_results:
                    all_issues_results: List[DiscoveryResult] = []
                    print(
                        f"ℹ️  [dc] OAI/sitemap yielded {len(oai_results)}+{len(sitemap_results)} results, skipping all_issues traversal",
                        flush=True,
                    )
                else:
                    all_issues_results = list(self._discover_via_all_issues(seed_url))
                merged_results = merge_dedupe(oai_results, sitemap_results, all_issues_results)
                for result in merged_results:
                    dedupe_key = (result.pdf_url or result.page_url or "").strip()
                    if not dedupe_key or dedupe_key in seen_urls:
                        continue
                    seen_urls.add(dedupe_key)
                    yielded_any = True
                    yield _normalize_result(result)
            else:
                # Stream union mode: start downloading immediately as each source yields.
                # Source order preserves metadata quality preference: OAI > sitemap.
                # all_issues is only used as fallback if OAI + sitemap yield nothing.
                streams: List[Tuple[str, Iterable[DiscoveryResult]]] = [
                    (
                        "oai",
                        discover_oai(
                            seed_url,
                            session=self.session,
                            max_records=self.dc_max_oai_records,
                            timeout=self.dc_download_timeout,
                            allow_unscoped_fallback=self._is_unscoped_oai_allowed(seed_url),
                        ),
                    ),
                    (
                        "siteindex",
                        discover_sitemap(
                            seed_url,
                            session=self.session,
                            use_siteindex=self.dc_use_siteindex,
                            max_urls=self.dc_max_sitemap_urls,
                            timeout=self.dc_download_timeout,
                        ),
                    ),
                ]

                for stage_name, stream in streams:
                    stage_start = time.time()
                    stage_scanned = 0
                    stage_yielded = 0
                    print(f"🔎 [dc] {seed_url} stage={stage_name} start", flush=True)
                    for result in stream:
                        stage_scanned += 1
                        dedupe_key = (result.pdf_url or result.page_url or "").strip()
                        if not dedupe_key or dedupe_key in seen_urls:
                            continue
                        seen_urls.add(dedupe_key)
                        stage_yielded += 1
                        yielded_any = True
                        if stage_yielded % 100 == 0:
                            print(
                                f"  [dc] {seed_url} stage={stage_name} yielded={stage_yielded}",
                                flush=True,
                            )
                        yield _normalize_result(result)
                    print(
                        f"✅ [dc] {seed_url} stage={stage_name} complete "
                        f"(yielded={stage_yielded}, scanned={stage_scanned}, "
                        f"elapsed={max(time.time() - stage_start, 0.0):.1f}s)",
                        flush=True,
                    )
                
                # Only run all_issues as fallback if OAI + sitemap yielded nothing
                if not yielded_any:
                    print("ℹ️  [dc] OAI/sitemap yielded nothing, falling back to all_issues traversal", flush=True)
                    stage_name = "all_issues"
                    stage_start = time.time()
                    stage_scanned = 0
                    stage_yielded = 0
                    for result in self._discover_via_all_issues(seed_url):
                        stage_scanned += 1
                        dedupe_key = (result.pdf_url or result.page_url or "").strip()
                        if not dedupe_key or dedupe_key in seen_urls:
                            continue
                        seen_urls.add(dedupe_key)
                        stage_yielded += 1
                        yielded_any = True
                        if stage_yielded % 100 == 0:
                            print(
                                f"  [dc] {seed_url} stage={stage_name} yielded={stage_yielded}",
                                flush=True,
                            )
                        yield _normalize_result(result)
                    print(
                        f"✅ [dc] {seed_url} stage={stage_name} complete "
                        f"(yielded={stage_yielded}, scanned={stage_scanned}, "
                        f"elapsed={max(time.time() - stage_start, 0.0):.1f}s)",
                        flush=True,
                    )
        elif effective_enum_mode == "sitemap_only":
            # Sitemap-only mode: used when seed is a sitemap.xml URL
            for result in discover_sitemap(
                seed_url,
                session=self.session,
                use_siteindex=self.dc_use_siteindex,
                max_urls=self.dc_max_sitemap_urls,
                timeout=self.dc_download_timeout,
            ):
                dedupe_key = (result.pdf_url or result.page_url or "").strip()
                if not dedupe_key or dedupe_key in seen_urls:
                    continue
                seen_urls.add(dedupe_key)
                yielded_any = True
                yield _normalize_result(result)
        elif effective_enum_mode == "oai_only":
            # OAI-only mode
            for result in discover_oai(
                seed_url,
                session=self.session,
                max_records=self.dc_max_oai_records,
                timeout=self.dc_download_timeout,
                allow_unscoped_fallback=self._is_unscoped_oai_allowed(seed_url),
            ):
                dedupe_key = (result.pdf_url or result.page_url or "").strip()
                if not dedupe_key or dedupe_key in seen_urls:
                    continue
                seen_urls.add(dedupe_key)
                yielded_any = True
                yield _normalize_result(result)

        # Final resort: BFS HTML crawl if no discovery source produced results.
        if not yielded_any:
            print(f"  [dc] OAI/Sitemap empty for {seed_url}, falling back to BFS HTML crawl")
            from ..digital_commons_enumerator import discover_html
            for result in discover_html(seed_url, session=self.session):
                dedupe_key = (result.pdf_url or result.page_url or "").strip()
                if not dedupe_key or dedupe_key in seen_urls:
                    continue
                seen_urls.add(dedupe_key)
                yield _normalize_result(result)

    def _process_issue_page(self, issue_url: str) -> Iterable[DiscoveryResult]:
        """Process individual issue page to extract articles and PDFs."""
        soup = self._get_page(issue_url)
        if not soup:
            return

        # Extract issue-level metadata
        issue_metadata = self._extract_issue_metadata(soup, issue_url)

        # Find all article containers
        articles = soup.select(self.article_container_selector)

        for article in articles:
            try:
                # Extract PDF URL
                pdf_link = article.select_one(self.pdf_link_selector)
                if not pdf_link or not pdf_link.get("href"):
                    continue

                pdf_url = urljoin(issue_url, pdf_link["href"])

                # Handle Digital Commons CGI URLs - convert to direct download
                if "viewcontent.cgi" in pdf_url:
                    # Extract article ID and convert to direct download URL
                    import re

                    article_match = re.search(r"article=(\d+)", pdf_url)
                    context_match = re.search(r"context=(\w+)", pdf_url)
                    if article_match and context_match:
                        article_id = article_match.group(1)
                        context = context_match.group(1)
                        base_url = pdf_url.split("/cgi/")[0]
                        pdf_url = f"{base_url}/cgi/viewcontent.cgi?article={article_id}&context={context}&type=pdf"

                # Extract article metadata
                metadata = self._extract_article_metadata(article, issue_url, issue_metadata)

                # Add PDF filename for metadata-PDF linking
                parsed_pdf = urlparse(pdf_url)
                pdf_filename = (
                    os.path.basename(parsed_pdf.path) or f"article_{article_id}.pdf"
                    if "article_id" in locals()
                    else "document.pdf"
                )
                if not pdf_filename.endswith(".pdf"):
                    pdf_filename += ".pdf"
                metadata["pdf_filename"] = pdf_filename
                metadata["pdf_url"] = pdf_url
                metadata.setdefault("dc_source", "all_issues")
                metadata.setdefault("dc_set_spec", "")

                yield DiscoveryResult(page_url=issue_url, pdf_url=pdf_url, metadata=metadata)

            except Exception:
                # Skip malformed articles but continue processing
                continue

    def _extract_issue_metadata(self, soup: BeautifulSoup, issue_url: str) -> dict:
        """Extract issue-level metadata like volume, issue, year."""
        metadata = {}

        try:
            # Try to extract from page title or h1
            title_elem = soup.select_one("h1") or soup.select_one("title")
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                metadata["issue_title"] = title_text

                # Try to parse volume/issue from title
                vol_issue_match = re.search(r"Volume\s+(\d+),?\s*Issue\s+(\d+)", title_text, re.I)
                if vol_issue_match:
                    metadata["volume"] = vol_issue_match.group(1)
                    metadata["issue"] = vol_issue_match.group(2)

                # Try to parse year
                year_match = re.search(r"\b(19|20)\d{2}\b", title_text)
                if year_match:
                    metadata["year"] = year_match.group(0)

        except Exception:
            pass

        return metadata

    def _extract_article_metadata(
        self, article_elem: BeautifulSoup, issue_url: str, issue_metadata: dict
    ) -> dict:
        """Extract metadata from individual article element."""
        metadata = {}

        try:
            # Title - try multiple selectors
            title_elem = article_elem.select_one(self.title_selector)
            if not title_elem:
                # Fallback selectors
                title_elem = (
                    article_elem.select_one("a.doctitle")
                    or article_elem.select_one("p a")
                    or article_elem.select_one("a")
                )

            if title_elem:
                metadata["title"] = title_elem.get_text(strip=True)

            # Author(s) - usually in span elements
            author_elems = article_elem.select(self.author_selector)
            authors = []
            for author_elem in author_elems:
                author_text = author_elem.get_text(strip=True)
                if author_text and not any(
                    skip in author_text.lower() for skip in ["follow", "download", "full text"]
                ):
                    authors.append(author_text)

            if authors:
                metadata["authors"] = authors if len(authors) > 1 else authors[0]

            # Date - try multiple selectors
            date_elem = article_elem.select_one(self.date_selector) or article_elem.select_one(
                "p.date"
            )
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                if date_text.lower().startswith("date posted:"):
                    date_text = date_text.split(":", 1)[1].strip()
                metadata["date"] = date_text

            # Inherit issue-level metadata
            metadata.update(issue_metadata)

            # Add source URL for reference
            metadata["source_url"] = issue_url

            # Try to extract additional metadata from text
            all_text = article_elem.get_text()

            # Look for page numbers
            page_match = re.search(r"pp?\.\s*(\d+(?:-\d+)?)", all_text, re.I)
            if page_match:
                metadata["pages"] = page_match.group(1)

        except Exception:
            # Return partial metadata if extraction fails
            pass

        return metadata

    def _download_single_with_browser(
        self,
        *,
        pdf_url: str,
        out_dir: str,
        referer: str = "",
    ) -> Optional[str]:
        """Download a single PDF using browser automation fallback with global concurrency limit."""
        from ..mcp_browser_download import create_browser_downloader
        
        domain = (urlparse(pdf_url).netloc or "unknown").lower()
        
        # Use shared artifacts directories
        staging_dir = "artifacts/browser_staging"
        user_data_dir = f"artifacts/browser_profiles/{self.dc_browser_backend}"
        
        # Limit concurrency globally across all workers
        with self._browser_semaphore:
            # We use a Lock to ensure only one thread creates/manages the downloader instance
            with self._browser_downloader_lock:
                if self._browser_downloader is None:
                    try:
                        self._browser_downloader = create_browser_downloader(
                            backend=self.dc_browser_backend,
                            staging_dir=staging_dir,
                            user_data_dir=user_data_dir,
                            final_out_dir=out_dir,
                            timeout_seconds=self.dc_download_timeout * 3, # Give browser plenty of time
                            headless=False,
                        )
                    except Exception as e:
                        print(f"⚠️ [dc] Failed to initialize browser downloader: {e}", flush=True)
                        return None
                
                downloader = self._browser_downloader
                # Re-check/ensure directories for this specific download
                downloader.final_out_dir = out_dir
                
                try:
                    downloader.add_url(pdf_url, domain=domain, referer=referer)
                    results = downloader.process_batch()
                    
                    if results and results[0].ok:
                        return results[0].local_path
                    else:
                        msg = results[0].message if results else "Unknown error"
                        print(f"❌ [dc] Browser fallback failed for {pdf_url}: {msg}", flush=True)
                except Exception as exc:
                    print(f"⚠️ [dc] Browser fallback exception for {pdf_url}: {exc}", flush=True)
            
        return None

    def download_pdf(self, pdf_url: str, out_dir: str, **kwargs) -> Optional[str]:
        referer = str(kwargs.get("referer") or "")
        seed_url = str(kwargs.get("seed_url") or "")
        host = (urlparse(pdf_url).netloc or "unknown").lower()
        circuit_key = self._waf_scope_key(host=host, seed_url=seed_url, referer=referer)

        if self._is_subscription_blocked_scope(circuit_key):
            self._set_download_meta(
                ok=False,
                error_type="subscription_blocked",
                message=(
                    "Subscription/login wall previously detected for this journal scope; "
                    "skipping remaining PDF downloads in scope"
                ),
                status_code=403,
                content_type="text/html",
                pdf_sha256=None,
                pdf_size_bytes=None,
                waf_action="",
                download_method="requests",
                ua_profile_used="",
                robots_allowed=True,
                download_status_class="blocked_subscription",
                blocked_reason="subscription_login_scope_blocked",
                retry_after_hint=None,
            )
            return None

        circuit_open, remaining_s, circuit_closed = self._check_waf_circuit(circuit_key)
        if circuit_closed:
            print(f"🟢 [dc] WAF circuit closed for key={circuit_key}", flush=True)
        if circuit_open:
            self._set_download_meta(
                ok=False,
                error_type="waf_circuit_open",
                message=(
                    "WAF circuit is open for host; skipping request "
                    f"(cooldown_remaining={remaining_s:.1f}s)"
                ),
                status_code=403,
                content_type="",
                pdf_sha256=None,
                pdf_size_bytes=None,
                waf_action="",
                download_method="requests",
                ua_profile_used="",
                robots_allowed=True,
                download_status_class="blocked_waf",
                blocked_reason="waf_circuit_open",
                retry_after_hint=remaining_s,
            )
            return None

        # Check session rotation - proactively rotate session to avoid WAF session limits
        rotation_manager = self._get_session_rotation_manager(self.dc_session_rotate_threshold)
        if rotation_manager.should_rotate(host):
            # Clear session cookies for this host to start fresh
            if hasattr(self.session, "cookies"):
                try:
                    # Clear all cookies for this host
                    for cookie in list(self.session.cookies):
                        if host in (cookie.domain or ""):
                            self.session.cookies.clear(cookie.domain, cookie.path, cookie.name)
                except Exception:
                    # If selective clearing fails, just clear all cookies
                    self.session.cookies.clear()
            rotation_manager.reset_host(host)
            print(f"🔄 [dc] Session rotated for host={host} (threshold={self.dc_session_rotate_threshold})", flush=True)

        # Record this request
        rotation_manager.record_request(host)

        outcome = download_pdf_dc(
            session=self.session,
            pdf_url=pdf_url,
            out_dir=out_dir,
            referer=referer,
            ua_profiles=parse_ua_profiles(",".join(self.dc_ua_profiles)),
            timeout=self.dc_download_timeout,
            min_domain_delay_ms=self.dc_min_domain_delay_ms,
            max_domain_delay_ms=self.dc_max_domain_delay_ms,
            robots_enforce=self.dc_robots_enforce,
            robots_cache=self._robots_cache,
            max_attempts_per_profile=3,
            use_curl_cffi=self.dc_use_curl_cffi,
        )
        # Determine download method from ua_profile_used
        ua_profile = str(outcome.get("ua_profile_used") or "")
        download_method = "curl_cffi" if "curl_cffi" in ua_profile else "requests"
        
        if outcome.get("ok"):
            self._record_waf_success(circuit_key)
            self._set_download_meta(
                ok=True,
                error_type="",
                message="",
                status_code=int(outcome.get("status_code") or 200),
                content_type=str(outcome.get("content_type") or "application/pdf"),
                pdf_sha256=outcome.get("pdf_sha256"),
                pdf_size_bytes=outcome.get("pdf_size_bytes"),
                waf_action=str(outcome.get("waf_action") or ""),
                download_method=download_method,
                ua_profile_used=ua_profile,
                robots_allowed=bool(outcome.get("robots_allowed")),
                download_status_class=str(outcome.get("download_status_class") or "ok"),
                blocked_reason=str(outcome.get("blocked_reason") or ""),
                retry_after_hint=outcome.get("retry_after_hint"),
            )
            return outcome.get("local_path")

        error_type = str(outcome.get("error_type") or "download_failed")
        status_code = int(outcome.get("status_code") or 0)

        if error_type == "subscription_blocked":
            self._mark_subscription_blocked_scope(circuit_key)
            self._set_download_meta(
                ok=False,
                error_type="subscription_blocked",
                message=str(outcome.get("message") or "Subscription/login wall blocked PDF access"),
                status_code=status_code or 403,
                content_type=str(outcome.get("content_type") or "text/html"),
                pdf_sha256=None,
                pdf_size_bytes=None,
                waf_action=str(outcome.get("waf_action") or ""),
                download_method=download_method,
                ua_profile_used=ua_profile,
                robots_allowed=outcome.get("robots_allowed"),
                download_status_class="blocked_subscription",
                blocked_reason=str(outcome.get("blocked_reason") or "subscription_login"),
                retry_after_hint=outcome.get("retry_after_hint"),
            )
            return None
        
        # Aggressive WAF detection: any 403, 429, or known WAF error
        is_waf_block = (
            status_code in {403, 429} 
            or error_type in {"waf_challenge", "blocked_waf", "waf_circuit_open"}
            or "waf" in str(outcome.get("message", "")).lower()
        )

        if is_waf_block:
            if self.dc_waf_browser_fallback:
                print(f"🌐 [dc] WAF block ({status_code}/{error_type}) for {pdf_url}; attempting browser fallback...", flush=True)
                browser_result = self._download_single_with_browser(
                    pdf_url=pdf_url,
                    out_dir=out_dir,
                    referer=referer,
                )
                if browser_result and os.path.exists(browser_result):
                    print(f"✅ [dc] Browser fallback successful for {pdf_url}", flush=True)
                    self._record_waf_success(circuit_key)
                    self._set_download_meta(
                        ok=True,
                        error_type="",
                        message="",
                        status_code=200,
                        content_type="application/pdf",
                        pdf_sha256=None,
                        pdf_size_bytes=None,
                        waf_action="browser_fallback",
                        download_method="browser",
                        ua_profile_used=f"browser_{self.dc_browser_backend}",
                        robots_allowed=True,
                        download_status_class="ok",
                        blocked_reason="",
                        retry_after_hint=None,
                    )
                    return browser_result
                else:
                    print(f"❌ [dc] Browser fallback FAILED for {pdf_url}", flush=True)

            # Only record failure and potentially open circuit if browser fallback didn't save us
            opened, streak = self._record_waf_failure(
                circuit_key,
                fail_threshold=self.dc_waf_fail_threshold,
                cooldown_seconds=self.dc_waf_cooldown_seconds,
            )
            if opened:
                print(
                    "🛑 [dc] WAF circuit opened for "
                    f"key={circuit_key} threshold={self.dc_waf_fail_threshold} "
                    f"cooldown={self.dc_waf_cooldown_seconds}s",
                    flush=True,
                )
            outcome["waf_fail_streak"] = streak
            outcome["waf_circuit_opened"] = opened
            outcome["error_type"] = "blocked_waf"
            error_type = "blocked_waf"

        self._set_download_meta(
            ok=False,
            error_type=error_type,
            message=str(outcome.get("message") or "PDF download failed"),
            status_code=int(outcome.get("status_code") or 0),
            content_type=str(outcome.get("content_type") or ""),
            pdf_sha256=None,
            pdf_size_bytes=None,
            waf_action=str(outcome.get("waf_action") or ""),
            download_method=download_method,
            ua_profile_used=ua_profile,
            robots_allowed=outcome.get("robots_allowed"),
            download_status_class=str(outcome.get("download_status_class") or "network"),
            blocked_reason=str(outcome.get("blocked_reason") or ""),
            retry_after_hint=outcome.get("retry_after_hint"),
        )

        if self.dc_allow_generic_fallback:
            local_path = self._download_with_generic(pdf_url, out_dir, referer=referer)
            if local_path:
                return local_path
        return None
