from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from collections import deque
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

if TYPE_CHECKING:
    from .adapters.base import DiscoveryResult


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


def _new_discovery_result(**kwargs: Any):
    """Lazy import to avoid circular imports during module initialization."""
    from .adapters.base import DiscoveryResult

    return DiscoveryResult(**kwargs)


OAI_NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/",
}


def _dedupe_preserve(values: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for value in values:
        candidate = value.strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        out.append(candidate)
    return out


def _extract_urls(value: str) -> List[str]:
    urls = re.findall(r"https?://[^\s<>'\"),]+", value)
    return _dedupe_preserve(urls)


def _parse_rights_values(raw_values: List[str]) -> Dict[str, object]:
    rights_raw = _dedupe_preserve(raw_values)
    if not rights_raw:
        return {}

    rights_text_values: List[str] = []
    rights_urls: List[str] = []
    for raw in rights_raw:
        decoded = html.unescape(raw).strip()
        if not decoded:
            continue

        if "<" in decoded and ">" in decoded:
            fragment = BeautifulSoup(decoded, "lxml")
            text = " ".join(fragment.get_text(" ", strip=True).split()).strip()
            if not text:
                alt_texts = [
                    " ".join((img.get("alt") or "").split()).strip()
                    for img in fragment.select("img[alt]")
                ]
                alt_texts = [t for t in alt_texts if t]
                if alt_texts:
                    text = " | ".join(alt_texts)
            if text:
                rights_text_values.append(text)

            hrefs = [a.get("href", "").strip() for a in fragment.select("a[href]")]
            hrefs = [h for h in hrefs if h]
            rights_urls.extend(hrefs)
        else:
            rights_text_values.append(decoded)

        rights_urls.extend(_extract_urls(decoded))

    rights_urls = _dedupe_preserve(rights_urls)
    license_urls = [u for u in rights_urls if "creativecommons.org/licenses" in u.lower()]
    statement_urls = [
        u for u in rights_urls if "creativecommons.org/licenses" not in u.lower()
    ]

    metadata: Dict[str, object] = {"rights_raw": rights_raw}
    rights_text_values = _dedupe_preserve(rights_text_values)
    if rights_text_values:
        metadata["rights_text"] = (
            rights_text_values[0] if len(rights_text_values) == 1 else rights_text_values
        )
    if statement_urls:
        metadata["rights_url"] = statement_urls[0]
    if license_urls:
        metadata["license_url"] = license_urls[0]
    return metadata


def _seed_slug(seed_url: str) -> str:
    parsed = urlparse(seed_url)
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        return ""
    return parts[0].strip().lower()


def _normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    if not parsed.scheme:
        return url.strip()
    clean_query = parse_qs(parsed.query, keep_blank_values=True)
    if parsed.fragment:
        parsed = parsed._replace(fragment="")
    if clean_query:
        normalized_query = urlencode(clean_query, doseq=True)
        parsed = parsed._replace(query=normalized_query)
    return urlunparse(parsed)


def _normalize_oai_pdf_url(pdf_url: str) -> str:
    lowered = pdf_url.lower()
    if "cgi/viewcontent.cgi" in lowered and "type=pdf" not in lowered:
        sep = "&" if "?" in pdf_url else "?"
        return f"{pdf_url}{sep}type=pdf"
    return pdf_url


def _extract_oai_identifiers(record: ET.Element) -> List[str]:
    identifiers: List[str] = []
    for elem in record.findall(".//dc:identifier", OAI_NS):
        value = (elem.text or "").strip()
        if value:
            identifiers.append(value)
    return identifiers


def _pick_oai_urls(
    identifiers: List[str], *, seed_url: str
) -> Tuple[Optional[str], Optional[str]]:
    parsed_seed = urlparse(seed_url)
    seed_host = (parsed_seed.netloc or "").lower()
    slug = _seed_slug(seed_url)

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
            pdf_url = _normalize_oai_pdf_url(value)
        elif landing_url is None:
            landing_url = value

        if pdf_url and landing_url:
            break

    return landing_url, pdf_url


def _extract_oai_metadata(record: ET.Element, *, seed_url: str, landing_url: str, set_spec: str) -> Dict:
    metadata: Dict[str, object] = {"source_url": landing_url or seed_url}
    title = record.find(".//dc:title", OAI_NS)
    if title is not None and (title.text or "").strip():
        metadata["title"] = (title.text or "").strip()

    authors = [(elem.text or "").strip() for elem in record.findall(".//dc:creator", OAI_NS)]
    authors = [a for a in authors if a]
    if authors:
        metadata["authors"] = authors

    date_elem = record.find(".//dc:date", OAI_NS)
    if date_elem is not None and (date_elem.text or "").strip():
        metadata["date"] = (date_elem.text or "").strip()

    source_elem = record.find(".//dc:source", OAI_NS)
    source_text = (source_elem.text or "").strip() if source_elem is not None else ""
    if source_text:
        metadata["source"] = source_text

    rights_values = [
        (elem.text or "").strip() for elem in record.findall(".//dc:rights", OAI_NS)
    ]
    rights_values = [v for v in rights_values if v]
    if rights_values:
        metadata.update(_parse_rights_values(rights_values))

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

    issue_match = re.search(r"/iss(?:ue)?/?(\d+)|\b(?:issue|no\.?)\s*(\d+)\b", combined, re.I)
    if issue_match:
        metadata["issue"] = next((g for g in issue_match.groups() if g), "")

    metadata["dc_source"] = "oai"
    metadata["dc_set_spec"] = set_spec
    return metadata


def discover_oai(
    seed_url: str,
    *,
    session,
    max_records: int = 0,
    timeout: int = 30,
    allow_unscoped_fallback: bool = True,
) -> Iterator[DiscoveryResult]:
    parsed_seed = urlparse(seed_url)
    endpoint = f"{parsed_seed.scheme}://{parsed_seed.netloc}/do/oai/"
    slug = _seed_slug(seed_url)
    set_candidates = [f"publication:{slug}"] if slug else []
    if allow_unscoped_fallback:
        set_candidates.append("")

    seen_pdf_urls: Set[str] = set()
    max_records_limit = max(0, int(max_records or 0))
    print(
        f"🔎 [dc:oai] {seed_url} start (sets={len(set_candidates)}, max_records={max_records_limit or 'unbounded'})",
        flush=True,
    )

    for set_spec in set_candidates:
        set_label = set_spec or "unscoped"
        set_start = 0
        token = ""
        scoped_yielded = 0
        page_count = 0
        while True:
            if page_count == 0:
                set_start = 1
                print(f"  [dc:oai] set={set_label} begin", flush=True)
            page_count += 1
            if token:
                params = {"verb": "ListRecords", "resumptionToken": token}
            else:
                params = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
                if set_spec:
                    params["set"] = set_spec

            try:
                resp = session.get(endpoint, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
            except Exception:
                break
            if resp.status_code >= 400 or not resp.content:
                break

            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError:
                break

            records = root.findall(".//oai:record", OAI_NS)
            if not records and not token:
                break

            for record in records:
                header = record.find("oai:header", OAI_NS)
                if header is not None and (header.get("status") or "").lower() == "deleted":
                    continue

                identifiers = _extract_oai_identifiers(record)
                if not identifiers:
                    continue
                landing_url, pdf_url = _pick_oai_urls(identifiers, seed_url=seed_url)
                if not pdf_url:
                    continue

                normalized_pdf = _normalize_url(pdf_url)
                if normalized_pdf in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(normalized_pdf)

                scoped_yielded += 1
                metadata = _extract_oai_metadata(
                    record,
                    seed_url=seed_url,
                    landing_url=landing_url or seed_url,
                    set_spec=set_spec,
                )
                yield _new_discovery_result(
                    page_url=landing_url or seed_url,
                    pdf_url=normalized_pdf,
                    metadata=metadata,
                    extraction_path="oai_pmh",
                )

                if max_records_limit and scoped_yielded >= max_records_limit:
                    return

            token_elem = root.find(".//oai:resumptionToken", OAI_NS)
            token = (token_elem.text or "").strip() if token_elem is not None else ""
            if page_count % 10 == 0:
                print(
                    f"  [dc:oai] set={set_label} pages={page_count} yielded={scoped_yielded}",
                    flush=True,
                )
            if not token:
                break

        if set_start:
            print(
                f"✅ [dc:oai] set={set_label} complete (pages={page_count}, yielded={scoped_yielded})",
                flush=True,
            )
        if scoped_yielded:
            break


def _discover_sitemap_roots(seed_url: str, *, use_siteindex: bool) -> List[str]:
    parsed = urlparse(seed_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    slug = _seed_slug(seed_url)
    roots: List[str] = []
    if use_siteindex:
        roots.append(f"{base}/siteindex.xml")
    roots.extend([f"{base}/sitemap.xml", f"{base}/{slug}/sitemap.xml" if slug else ""])
    return [r for r in roots if r]


LAW_KEYWORDS = [
    "review", "journal", "jurist", "justice", "legal", "jurisprudence", 
    "constitution", "policy", "court", "judge", "attorney", "survey",
    "/lr/", "/hlr/", "/jplp/", "/mls/", "/cl_pubs/", "/clr/", "/sulr/", "/mulr/",
    "/alr/", "/flr/", "/vlr/", "/mlr/", "/ulr/", "/slr/", "/tlr/", "/wlr/",
    "student"
]

BLACK_KEYWORDS = [
    "facpub", "faculty", "fac_articles", "fac_books", "fac_other_pubs",
    "staff", "repository", "series", "archives", "biography", "alumni",
    "newsletter", "bulletin", "magazine", "presentation", "lecture",
    "theses", "dissertations", "etd-", "gallery", "image", "video", "audio",
    "multimedia", "curriculum", "syllabus", "portfolio", "exhibit", "collection",
    "university-wide", "institutional-repository", "orientation", "pressrelease",
    "commencement", "about", "history", "news", "event", "meeting", "meeting", 
    "awards", "institutions", "communities"
]


def _is_in_scope(url: str, *, seed_url: str) -> bool:
    parsed_seed = urlparse(seed_url)
    parsed = urlparse(url)
    if (parsed.netloc or "").lower() != (parsed_seed.netloc or "").lower():
        return False
    
    lowered = url.lower()

    # 1. Explicit Exclusions (Blacklist)
    if any(k in lowered for k in BLACK_KEYWORDS):
        return False

    # 2. Slug-based matching (High Confidence)
    slug = _seed_slug(seed_url)
    if slug:
        if (f"/{slug}/" in lowered or f"/context/{slug}/" in lowered or f"context={slug}" in lowered):
            return True

    # 3. Dynamic Journal Gathering (Low Confidence / Heuristic)
    # We strip the protocol/host before checking keywords to avoid "law" in the domain name
    # causing everything to match.
    path_only = parsed.path.lower()
    if any(k in path_only for k in LAW_KEYWORDS):
        return True

    # 4. If no slug was provided in seed, and no black/white match, default to True (broad discovery)
    if not slug:
        return True
    
    return False


def _looks_like_article_page(url: str, *, seed_url: str) -> bool:
    path = (urlparse(url).path or "").lower()
    slug = _seed_slug(seed_url)
    if re.search(r"/vol\d+/.*/iss\d+/\d+/?$", path):
        return True
    if slug and re.search(rf"/{re.escape(slug)}/vol\d+/iss\d+/\d+/?$", path):
        return True
    if slug and re.search(rf"/{re.escape(slug)}/\d+/?$", path):
        return True
    return False


def _extract_sitemap_pdf_urls(page_url: str, *, seed_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    found: List[str] = []
    slug = _seed_slug(seed_url)
    for anchor in soup.select("a[href]"):
        href = anchor.get("href") or ""
        absolute = _normalize_url(urljoin(page_url, href.replace("&amp;", "&")))
        lowered = absolute.lower()
        if "viewcontent.cgi" in lowered:
            if "type=pdf" not in lowered:
                sep = "&" if "?" in absolute else "?"
                absolute = f"{absolute}{sep}type=pdf"
            found.append(_normalize_url(absolute))
            continue
        if "/viewcontent/" in lowered and slug and f"/context/{slug}/article/" in lowered:
            found.append(_normalize_url(absolute))
            continue
        if lowered.endswith(".pdf"):
            found.append(_normalize_url(absolute))
    return found


def _split_title_and_authors_from_byline(title_text: str) -> Tuple[str, List[str]]:
    cleaned = " ".join((title_text or "").split()).strip()
    if not cleaned:
        return "", []

    # Common Digital Commons page-title pattern:
    #   "Article Title" by Author Name
    match = re.match(r'^[\"“]?(.+?)[\"”]?\s+by\s+(.+?)\s*$', cleaned, flags=re.I)
    if not match:
        return cleaned, []

    title = match.group(1).strip().strip('"').strip("'").strip("“”")
    authors_raw = match.group(2).strip()
    authors = [
        part.strip()
        for part in re.split(r"\s*(?:,| and | & )\s*", authors_raw)
        if part.strip()
    ]
    return title or cleaned, authors


def _extract_article_page_metadata(soup: BeautifulSoup, page_url: str, page_title: str) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {"source_url": page_url, "url": page_url}

    title, authors = _split_title_and_authors_from_byline(page_title)
    if title:
        metadata["title"] = title
    if authors:
        metadata["authors"] = authors

    citation_node = soup.select_one("#recommended_citation .citation")
    if citation_node:
        # Keep title from <em> where present; this is usually the clean article title.
        title_node = citation_node.select_one("em")
        if title_node:
            citation_title = " ".join(title_node.get_text(" ", strip=True).split()).strip()
            if citation_title:
                metadata["title"] = citation_title

        citation_text = " ".join(citation_node.get_text(" ", strip=True).split()).strip()
        if citation_text:
            citation_body = re.split(r"\bAvailable at:\b", citation_text, maxsplit=1, flags=re.I)[
                0
            ].strip()
            if citation_body:
                metadata["citation"] = citation_body
                year_match = re.search(r"\((19|20)\d{2}\)", citation_body)
                if year_match:
                    metadata["year"] = year_match.group(0).strip("()")

    rights_values: List[str] = []
    for meta in soup.select("meta[name][content], meta[property][content]"):
        key = ((meta.get("name") or meta.get("property") or "").strip()).lower()
        if key in {"dc.rights", "dc:rights", "dcterms.rights", "citation_rights"}:
            rights_values.append((meta.get("content") or "").strip())

    rights_block = soup.select_one("#rights")
    if rights_block:
        rights_values.append(str(rights_block))

    for link in soup.select("a[href*='creativecommons.org/licenses'], a[href*='rightsstatements.org/vocab']"):
        href = (link.get("href") or "").strip()
        if href:
            rights_values.append(href)
    for link in soup.select("link[rel~='license'][href]"):
        href = (link.get("href") or "").strip()
        if href:
            rights_values.append(href)

    rights_values = [v for v in rights_values if v]
    if rights_values:
        metadata.update(_parse_rights_values(rights_values))

    return metadata


def discover_sitemap(
    seed_url: str,
    *,
    session,
    use_siteindex: bool = True,
    max_urls: int = 0,
    timeout: int = 20,
) -> Iterator[DiscoveryResult]:
    roots = _discover_sitemap_roots(seed_url, use_siteindex=use_siteindex)
    pending = deque(roots)
    seen_maps: Set[str] = set()
    seen_pages: Set[str] = set()
    queued_article_pages: deque[str] = deque()
    fallback_page_urls: List[str] = []
    seen_pdfs: Set[str] = set()
    max_urls_limit = max(0, int(max_urls or 0))
    scanned_maps = 0
    scanned_pages = 0
    yielded_total = 0

    print(
        f"🔎 [dc:sitemap] {seed_url} start (roots={len(roots)}, max_urls={max_urls_limit or 'unbounded'})",
        flush=True,
    )

    def _enqueue_page(url: str) -> None:
        normalized = _normalize_url(url)
        if not normalized or normalized in seen_pages:
            return
        if not _is_in_scope(normalized, seed_url=seed_url):
            return
        seen_pages.add(normalized)
        if _looks_like_article_page(normalized, seed_url=seed_url):
            queued_article_pages.append(normalized)
        else:
            fallback_page_urls.append(normalized)

    def _iter_page_results(page_url: str) -> Iterator[DiscoveryResult]:
        nonlocal yielded_total
        try:
            resp = session.get(page_url, headers=DEFAULT_HEADERS, timeout=timeout)
        except Exception:
            return
        if resp.status_code >= 400 or not resp.text:
            return
        soup = BeautifulSoup(resp.text, "lxml")
        title = soup.title.get_text(strip=True) if soup.title else ""
        metadata = _extract_article_page_metadata(soup, page_url, title)
        metadata["dc_source"] = "siteindex"
        metadata["dc_set_spec"] = ""
        volume_match = re.search(r"/vol(?:ume)?/?(\d+)|\bvol(?:ume)?\.?\s*(\d+)\b", page_url, re.I)
        if volume_match:
            metadata["volume"] = next((g for g in volume_match.groups() if g), "")
        issue_match = re.search(r"/iss(?:ue)?/?(\d+)|\b(?:issue|no\.?)\s*(\d+)\b", page_url, re.I)
        if issue_match:
            metadata["issue"] = next((g for g in issue_match.groups() if g), "")
        year_match = re.search(r"\b(19|20)\d{2}\b", f"{title} {page_url}")
        if year_match:
            metadata["year"] = year_match.group(0)

        for pdf_url in _extract_sitemap_pdf_urls(page_url, seed_url=seed_url, html=resp.text):
            if pdf_url in seen_pdfs:
                continue
            seen_pdfs.add(pdf_url)
            yielded_total += 1
            yield _new_discovery_result(
                page_url=page_url,
                pdf_url=pdf_url,
                metadata=dict(metadata),
                extraction_path="siteindex_article_page",
            )

    while pending:
        sitemap_url = pending.popleft()
        if sitemap_url in seen_maps:
            continue
        seen_maps.add(sitemap_url)
        scanned_maps += 1

        try:
            resp = session.get(
                sitemap_url,
                headers={"Accept": "application/xml", **DEFAULT_HEADERS},
                timeout=timeout,
            )
        except Exception:
            continue
        if resp.status_code >= 400 or not resp.content:
            continue

        soup = BeautifulSoup(resp.content, "xml")
        if soup.find("sitemapindex"):
            for loc in soup.find_all("loc"):
                child = (loc.get_text() or "").strip()
                if child and _is_in_scope(child, seed_url=seed_url):
                    pending.append(child)
        elif soup.find("urlset"):
            for loc in soup.find_all("loc"):
                url = (loc.get_text() or "").strip()
                if not url:
                    continue
                _enqueue_page(url)

        if scanned_maps % 25 == 0:
            print(
                f"  [dc:sitemap] maps={scanned_maps} queued_articles={len(queued_article_pages)} "
                f"fallback_urls={len(fallback_page_urls)} yielded={yielded_total}",
                flush=True,
            )

        while queued_article_pages and (not max_urls_limit or scanned_pages < max_urls_limit):
            page_url = queued_article_pages.popleft()
            scanned_pages += 1
            if scanned_pages % 50 == 0:
                print(
                    f"  [dc:sitemap] article_pages_scanned={scanned_pages} yielded={yielded_total}",
                    flush=True,
                )
            for result in _iter_page_results(page_url):
                yield result

    # If no article-like pages were discovered, fall back to all in-scope sitemap URLs.
    if scanned_pages == 0 and fallback_page_urls:
        fallback_pages = sorted(fallback_page_urls)
        if max_urls_limit:
            fallback_pages = fallback_pages[:max_urls_limit]
        print(
            f"  [dc:sitemap] no article-pattern URLs found; checking {len(fallback_pages)} fallback pages",
            flush=True,
        )
        for page_url in fallback_pages:
            scanned_pages += 1
            for result in _iter_page_results(page_url):
                yield result

    print(
        f"✅ [dc:sitemap] {seed_url} complete (maps={scanned_maps}, pages={scanned_pages}, yielded={yielded_total})",
        flush=True,
    )


def discover_html(
    seed_url: str,
    *,
    session,
    max_depth: int = 2,
    timeout: int = 20,
) -> Iterator[DiscoveryResult]:
    """Crawl Digital Commons HTML as a last resort when OAI and Sitemaps fail."""
    queue: deque[Tuple[str, int]] = deque([(seed_url, 0)])
    seen_pages: Set[str] = {seed_url}
    seen_pdfs: Set[str] = set()
    slug = _seed_slug(seed_url)
    host = urlparse(seed_url).netloc

    while queue:
        url, depth = queue.popleft()
        try:
            resp = session.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
        except Exception:
            continue
        if not resp or resp.status_code >= 400 or not resp.text:
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        
        # 1. Extract PDFs from current page
        for pdf_url in _extract_sitemap_pdf_urls(url, seed_url=seed_url, html=resp.text):
            if pdf_url not in seen_pdfs:
                seen_pdfs.add(pdf_url)
                # Basic metadata from page title/URL
                title = soup.title.get_text(strip=True) if soup.title else ""
                metadata = _extract_article_page_metadata(soup, url, title)
                metadata["dc_source"] = "html_crawl"
                
                yield _new_discovery_result(
                    page_url=url,
                    pdf_url=pdf_url,
                    metadata=metadata,
                    extraction_path="html_crawl",
                )

        # 2. Enqueue more pages if within depth
        if depth < max_depth:
            for a in soup.select("a[href]"):
                href = a["href"]
                absolute = _normalize_url(urljoin(url, href))
                parsed = urlparse(absolute)
                
                if parsed.netloc != host:
                    continue
                if absolute in seen_pages:
                    continue
                
                # Scoping: must contain slug
                if slug and f"/{slug}/" not in absolute.lower() and f"context={slug}" not in absolute.lower():
                    continue
                
                # Heuristics for "crawlable" pages (issues, articles, archives)
                lowered = absolute.lower()
                is_likely_nav = any(x in lowered for x in ["/vol", "/iss", "/articles", "all_issues", "archive"])
                
                if is_likely_nav:
                    seen_pages.add(absolute)
                    queue.append((absolute, depth + 1))


def _dc_source_from_result(result: DiscoveryResult) -> str:
    meta_source = str((result.metadata or {}).get("dc_source") or "").strip().lower()
    if meta_source in {"oai", "siteindex", "all_issues"}:
        return meta_source
    path = (result.extraction_path or "").lower()
    if "oai" in path:
        return "oai"
    if "siteindex" in path or "sitemap" in path:
        return "siteindex"
    return "all_issues"


def _is_empty(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def _listify_textish(value: object) -> List[str]:
    if _is_empty(value):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if not _is_empty(v)]
    return [str(value)]


def merge_dedupe(*streams: Iterable[DiscoveryResult]) -> List[DiscoveryResult]:
    source_rank = {"oai": 3, "siteindex": 2, "all_issues": 1}
    merged: Dict[str, DiscoveryResult] = {}
    field_rank: Dict[str, Dict[str, int]] = {}
    source_sets: Dict[str, Set[str]] = {}

    for stream in streams:
        for result in stream:
            key = _normalize_url(result.pdf_url or result.page_url or "")
            if not key:
                continue
            source = _dc_source_from_result(result)
            candidate_meta = dict(result.metadata or {})
            candidate_meta.setdefault("dc_source", source)
            candidate_meta.setdefault("dc_set_spec", "")

            existing = merged.get(key)
            if existing is None:
                merged[key] = _new_discovery_result(
                    page_url=result.page_url,
                    pdf_url=_normalize_url(result.pdf_url),
                    metadata=candidate_meta,
                    source_adapter=result.source_adapter,
                    extraction_path=result.extraction_path,
                    retrieved_at=result.retrieved_at,
                    http_status=result.http_status,
                    content_type=result.content_type,
                    pdf_sha256=result.pdf_sha256,
                    pdf_size_bytes=result.pdf_size_bytes,
                )
                source_sets[key] = {source}
                field_rank[key] = {
                    meta_key: source_rank.get(source, 0)
                    for meta_key, meta_value in candidate_meta.items()
                    if not _is_empty(meta_value)
                }
                continue

            source_sets.setdefault(key, set()).add(source)
            existing_meta = dict(existing.metadata or {})
            ranks = field_rank.setdefault(key, {})
            for meta_key, meta_value in candidate_meta.items():
                if _is_empty(meta_value):
                    continue
                # Preserve evidence from multiple sources for rights-related text fields.
                if meta_key in {"rights_raw", "rights_text"}:
                    current_list = _listify_textish(existing_meta.get(meta_key))
                    candidate_list = _listify_textish(meta_value)
                    merged_list = _dedupe_preserve([*current_list, *candidate_list])
                    if merged_list:
                        existing_meta[meta_key] = merged_list[0] if len(merged_list) == 1 else merged_list
                        ranks[meta_key] = max(ranks.get(meta_key, 0), source_rank.get(source, 0))
                    continue
                current_value = existing_meta.get(meta_key)
                current_rank = ranks.get(meta_key, 0)
                candidate_rank = source_rank.get(source, 0)
                if _is_empty(current_value) or candidate_rank > current_rank:
                    existing_meta[meta_key] = meta_value
                    ranks[meta_key] = candidate_rank
            existing.metadata = existing_meta

    output: List[DiscoveryResult] = []
    for key in sorted(merged.keys()):
        result = merged[key]
        sources = source_sets.get(key, set())
        final_source = "hybrid" if len(sources) > 1 else (next(iter(sources)) if sources else "all_issues")
        result.metadata = dict(result.metadata or {})
        result.metadata["dc_source"] = final_source
        result.pdf_url = _normalize_url(result.pdf_url)
        output.append(result)
    return output
