"""Site fingerprinter for law review journal platforms.

Fetches a small number of pages from a journal website and returns a
``SiteFingerprint`` describing the detected platform, structural link
patterns, CSS selectors, pagination cues, and an adapter recommendation
ready for use with this scraper toolkit.
"""

from __future__ import annotations

import re
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from offprint.polite_requests import PoliteRequestsSession
from offprint.url_classifier import (
    DIGITAL_COMMONS_HINTS,
    classify_url,
    is_digital_commons_like,
)

if TYPE_CHECKING:
    pass  # kept for future Protocol / stub imports

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_JS_FRAME_MARKERS = re.compile(
    r'(?:id=["\'](?:root|app)["\']|ng-app|data-reactroot|<div\s+id="__next")',
    re.I,
)
_WP_MARKERS = re.compile(r"wp-content/|wp-includes/|/wp-json/", re.I)
_OJS_MARKERS = re.compile(r"/index\.php/|/issue/archive|/article/view/", re.I)
_DC_BEPRESS_META = re.compile(r"bepress_citation_", re.I)
_DSPACE_MARKERS = re.compile(r"/bitstream/|dspace", re.I)
_JANEWAY_MARKERS = re.compile(r"janeway", re.I)
_SQUARESPACE_MARKERS = re.compile(r"static\d*\.squarespace\.com", re.I)
_WIX_MARKERS = re.compile(r"static\.wixstatic\.com|wix\.com/", re.I)
_SCHOLASTICA_MARKERS = re.compile(r"scholasticahq\.com|scholastica", re.I)
_DRUPAL_MARKERS = re.compile(r"drupal|Drupal\.settings|/sites/default/files/", re.I)

_PLATFORM_TO_ADAPTER: Dict[str, str] = {
    "wordpress": "WordPressAcademicBaseAdapter",
    "digitalcommons": "DigitalCommonsIssueArticleHopAdapter",
    "ojs": "OJSAdapter",
    "scholastica": "ScholasticaBaseAdapter",
    "drupal": "DrupalAdapter",
    "squarespace": "SquarespaceAdapter",
    "wix": "WixAdapter",
    "dspace": "DSpaceAdapter",
    "janeway": "JanewayAdapter",
    "quartex": "QuartexAdapter",
    "pubpub": "PubPubAdapter",
    "unknown": "needs_custom_adapter",
}

_WAF_MARKERS = re.compile(
    r"captcha|cloudflare|access denied|ray id|just a moment|please wait",
    re.I,
)

# CSS selector candidates for article listing containers, ordered most→least specific
_ARTICLE_CONTAINER_CANDIDATES: List[str] = [
    "article.post",
    "article",
    ".views-row",
    ".article-item",
    ".entry-list li",
    ".post-list li",
    "li.article",
    ".issue-article",
    "div.article",
    "div.post",
    "div.entry",
    ".search-results li",
    ".obj_article_summary",
]

# PDF link anchor-text hints
_PDF_ANCHOR_RE = re.compile(r"\bpdf\b", re.I)


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass
class SiteFingerprint:
    """All detected information about a journal website.

    Fields are populated by ``fingerprint_site`` and serialised by
    ``fingerprint_to_dict``.
    """

    url: str
    domain: str
    platform: str
    platform_signals: Dict[str, object]
    structure: Dict[str, int]
    css_selectors: Dict[str, str]
    pagination: Dict[str, object]
    pdf_url_patterns: List[str]
    sample_links: Dict[str, List[str]]
    adapter_recommendation: str
    seed_json: Optional[Dict[str, object]]
    fetch_count: int
    errors: List[str]
    js_rendered: bool


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_domain(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    host = parsed.netloc or ""
    # Strip port if present
    return host.split(":")[0].lower()


def _normalize_url(base: str, href: str) -> str:
    try:
        joined = urllib.parse.urljoin(base, href)
        # Drop fragments
        parts = urllib.parse.urlsplit(joined)
        return urllib.parse.urlunsplit(parts._replace(fragment=""))
    except Exception:
        return ""


def _is_same_domain(url: str, domain: str) -> bool:
    host = _extract_domain(url)
    return host == domain or host.endswith("." + domain)


def _is_js_heavy(html: str) -> bool:
    if len(html.strip()) < 200:
        return True
    if _JS_FRAME_MARKERS.search(html):
        return True
    if _WAF_MARKERS.search(html[:2000]):
        return True
    return False


def _fetch_html(session: PoliteRequestsSession, url: str) -> Tuple[str, Dict[str, str], int]:
    """Return ``(html_text, response_headers, status_code)``."""
    try:
        resp = session.get(url, timeout=20, allow_redirects=True)
        headers = dict(resp.headers) if hasattr(resp, "headers") else {}
        return resp.text, headers, resp.status_code
    except Exception:
        return "", {}, 0


def _fetch_playwright(url: str) -> Tuple[str, Dict[str, str], int]:
    from offprint.playwright_session import PlaywrightSession

    pw = PlaywrightSession(min_delay=0.5, max_delay=1.5, headless=True)
    try:
        resp = pw.get(url, timeout=30)
        headers = resp.headers if isinstance(resp.headers, dict) else {}
        return resp.text, headers, resp.status_code
    except Exception:
        return "", {}, 0
    finally:
        try:
            pw.close()
        except Exception:
            pass


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def _detect_platform_signals(html: str, headers: Dict[str, str], url: str) -> Dict[str, object]:
    signals: Dict[str, object] = {}

    # --- meta generator ---
    soup = _soup(html)
    gen_meta = soup.find("meta", attrs={"name": re.compile(r"^generator$", re.I)})
    if gen_meta:
        content = gen_meta.get("content", "")
        signals["meta_generator"] = content

    # --- BePress citation meta tags ---
    bp_metas = soup.find_all("meta", attrs={"name": _DC_BEPRESS_META})
    if bp_metas:
        signals["bepress_citation_meta_count"] = len(bp_metas)

    # --- Response headers ---
    for hdr in ("X-Powered-By", "X-Drupal-Cache", "X-Generator", "X-Wix-Request-Id"):
        val = headers.get(hdr) or headers.get(hdr.lower())
        if val:
            signals[f"header_{hdr}"] = val

    squarespace_served = headers.get("X-ServedBy") or headers.get("x-servedby") or ""
    if "squarespace" in squarespace_served.lower():
        signals["header_x_servedby"] = squarespace_served

    wix_hdrs = [k for k in headers if k.lower().startswith("x-wix-")]
    if wix_hdrs:
        signals["wix_headers"] = wix_hdrs

    # --- Source patterns ---
    if _WP_MARKERS.search(html):
        signals["wp_source_marker"] = True
    if _OJS_MARKERS.search(html) or _OJS_MARKERS.search(url):
        signals["ojs_source_marker"] = True
    if _DC_BEPRESS_META.search(html):
        signals["bepress_meta_in_source"] = True
    if _DSPACE_MARKERS.search(html):
        signals["dspace_source_marker"] = True
    if _JANEWAY_MARKERS.search(html):
        signals["janeway_source_marker"] = True
    if _SQUARESPACE_MARKERS.search(html):
        signals["squarespace_source_marker"] = True
    if _WIX_MARKERS.search(html):
        signals["wix_source_marker"] = True
    if _SCHOLASTICA_MARKERS.search(html):
        signals["scholastica_source_marker"] = True
    if _DRUPAL_MARKERS.search(html):
        signals["drupal_source_marker"] = True

    # --- Digital Commons URL heuristic ---
    if is_digital_commons_like(url):
        signals["dc_url_heuristic"] = True

    return signals


def _resolve_platform(signals: Dict[str, object], url: str, domain: str) -> str:
    gen = str(signals.get("meta_generator", "")).lower()

    # Explicit generator strings take top priority
    if "wordpress" in gen:
        return "wordpress"
    if "drupal" in gen:
        return "drupal"
    if "dspace" in gen:
        return "dspace"
    if "ojs" in gen or "open journal systems" in gen:
        return "ojs"
    if "janeway" in gen:
        return "janeway"
    if "squarespace" in gen:
        return "squarespace"

    # Bepress / Digital Commons
    if signals.get("bepress_citation_meta_count") or signals.get("bepress_meta_in_source"):
        return "digitalcommons"
    if signals.get("dc_url_heuristic"):
        return "digitalcommons"

    # OJS
    if signals.get("ojs_source_marker"):
        return "ojs"
    if "/index.php/" in url:
        return "ojs"

    # WordPress
    if signals.get("wp_source_marker"):
        return "wordpress"
    if "wordpress" in str(signals.get("header_X-Powered-By", "")).lower():
        return "wordpress"

    # Drupal
    if signals.get("drupal_source_marker"):
        return "drupal"
    if signals.get("header_X-Drupal-Cache"):
        return "drupal"

    # DSpace
    if signals.get("dspace_source_marker"):
        return "dspace"

    # Janeway
    if signals.get("janeway_source_marker"):
        return "janeway"

    # Squarespace
    if signals.get("squarespace_source_marker") or signals.get("header_x_servedby"):
        return "squarespace"

    # Wix
    if signals.get("wix_source_marker") or signals.get("wix_headers"):
        return "wix"

    # Scholastica
    if signals.get("scholastica_source_marker"):
        return "scholastica"
    if "scholasticahq.com" in domain:
        return "scholastica"

    # Digital Commons via domain hints
    if any(token in domain for token in DIGITAL_COMMONS_HINTS):
        return "digitalcommons"
    if domain.startswith("ir.") or domain.startswith("dc.law."):
        return "digitalcommons"

    return "unknown"


def _classify_links(
    soup: BeautifulSoup, base_url: str, domain: str
) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
    counts: Dict[str, int] = defaultdict(int)
    samples: Dict[str, List[str]] = defaultdict(list)
    max_sample = 5

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("javascript"):
            continue
        full = _normalize_url(base_url, href)
        if not full or not _is_same_domain(full, domain):
            continue
        anchor = a.get_text(strip=True)
        category = classify_url(full, anchor)
        counts[category] += 1
        bucket = samples[category]
        if len(bucket) < max_sample:
            bucket.append(full)

    return dict(counts), dict(samples)


def _detect_pagination(soup: BeautifulSoup, base_url: str) -> Dict[str, object]:
    info: Dict[str, object] = {}

    # rel="next"
    rel_next = soup.find("link", rel="next") or soup.find("a", rel="next")
    if rel_next:
        info["rel_next"] = rel_next.get("href", "")

    # Common pagination class names
    for cls in (".pagination", ".pager", ".page-numbers", "nav.navigation", ".wp-pagenavi"):
        selector = cls
        if selector.startswith("."):
            found = soup.find(class_=selector.lstrip("."))
        else:
            parts = selector.split(".", 1)
            found = soup.find(parts[0], class_=parts[1] if len(parts) > 1 else None)
        if found:
            info["pagination_class"] = selector
            break

    # URL-based page param
    all_hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]
    for href in all_hrefs:
        if "?page=" in href or "&page=" in href:
            info["page_param"] = "page"
            break
        if re.search(r"/page/\d+", href):
            info["page_param"] = "wp_page_path"
            break

    return info


def _detect_css_selectors(soup: BeautifulSoup) -> Dict[str, str]:
    selectors: Dict[str, str] = {}

    # Article listing container
    for candidate in _ARTICLE_CONTAINER_CANDIDATES:
        tag, _, cls = candidate.partition(".")
        tag = tag or None
        search_kwargs = {}
        if cls:
            search_kwargs["class_"] = cls
        found = soup.find_all(tag or True, **search_kwargs) if cls else soup.find_all(tag)
        if len(found) >= 3:
            selectors["article_listing"] = candidate
            break

    # PDF download link selector
    for a in soup.find_all("a", href=True):
        href = a["href"]
        anchor = a.get_text(strip=True)
        if classify_url(href, anchor) == "pdf" or _PDF_ANCHOR_RE.search(anchor):
            # Build a simple selector from tag + class
            classes = a.get("class", [])
            if classes:
                selectors["pdf_link"] = f"a.{classes[0]}"
            else:
                selectors["pdf_link"] = "a[href*='.pdf']"
            break

    return selectors


def _extract_pdf_url_patterns(pdf_urls: List[str]) -> List[str]:
    """Reduce a list of PDF URLs to a small set of representative templates."""
    patterns: List[str] = []
    seen_templates: set = set()
    # Replace numeric segments to form a template
    for url in pdf_urls:
        try:
            parts = urllib.parse.urlsplit(url)
            path_template = re.sub(r"\d+", "{n}", parts.path)
            template = urllib.parse.urlunsplit(
                parts._replace(path=path_template, query="", fragment="")
            )
        except Exception:
            template = url
        if template not in seen_templates:
            seen_templates.add(template)
            patterns.append(template)
        if len(patterns) >= 5:
            break
    return patterns


def _detect_journal_title(soup: BeautifulSoup) -> str:
    # og:site_name is most reliable
    og = soup.find("meta", property="og:site_name")
    if og and og.get("content"):
        return og["content"].strip()
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        return og_title["content"].strip()
    title_tag = soup.find("title")
    if title_tag:
        raw = title_tag.get_text(strip=True)
        # Strip common suffixes like " | Law Review"
        return raw.split(" – ")[0].split(" | ")[0].strip()
    return ""


def _build_seed_json(url: str, domain: str, platform: str, title: str) -> Dict[str, object]:
    slug = re.sub(r"[^a-z0-9]+", "-", domain).strip("-")
    return {
        "id": slug,
        "start_urls": [url],
        "source": "auto_fingerprint",
        "metadata": {
            "journal_name": title or domain,
            "platform": platform,
            "url": url,
            "created_date": date.today().isoformat(),
        },
    }


def _adapter_from_registry(url: str) -> Optional[str]:
    """Return the adapter class name already registered for this URL, if any."""
    try:
        from offprint.adapters.registry import pick_adapter_for

        adapter = pick_adapter_for(url)
        if adapter is not None:
            return type(adapter).__name__ if not isinstance(adapter, type) else adapter.__name__
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fingerprint_site(
    url: str,
    max_pages: int = 10,
    use_playwright_fallback: bool = True,
) -> SiteFingerprint:
    """Fingerprint a law review journal website and recommend a scraping adapter.

    Fetches up to *max_pages* pages from *url* using ``PoliteRequestsSession``
    (and optionally a headless Playwright browser for JS-heavy sites), then
    analyses platform signals, link structure, CSS selectors, and pagination
    patterns.  Returns a fully populated ``SiteFingerprint``.

    Args:
        url: The seed URL of the journal (archive or home page).
        max_pages: Maximum number of HTTP requests to make.
        use_playwright_fallback: When ``True``, retry with headless Playwright
            if the plain-HTTP response looks JS-rendered or WAF-blocked.

    Returns:
        A ``SiteFingerprint`` dataclass.  ``errors`` is non-empty on partial
        failures; the function never raises.
    """
    errors: List[str] = []
    fetch_count = 0
    js_rendered = False

    domain = _extract_domain(url)
    if not domain:
        return SiteFingerprint(
            url=url,
            domain="",
            platform="unknown",
            platform_signals={},
            structure={},
            css_selectors={},
            pagination={},
            pdf_url_patterns=[],
            sample_links={},
            adapter_recommendation="needs_custom_adapter",
            seed_json=None,
            fetch_count=0,
            errors=["Could not parse domain from URL"],
            js_rendered=False,
        )

    session = PoliteRequestsSession(min_delay=0.5, max_delay=1.5)

    # --- Phase 1: fetch seed page ---
    seed_html, seed_headers, seed_status = _fetch_html(session, url)
    fetch_count += 1

    if seed_status == 0:
        errors.append(f"Seed fetch failed for {url}")
    elif seed_status >= 400:
        errors.append(f"Seed returned HTTP {seed_status} for {url}")

    # Playwright fallback
    if use_playwright_fallback and (not seed_html or _is_js_heavy(seed_html) or seed_status == 0):
        try:
            pw_html, pw_headers, pw_status = _fetch_playwright(url)
            if pw_html and not _is_js_heavy(pw_html):
                seed_html = pw_html
                seed_headers = pw_headers
                seed_status = pw_status
                js_rendered = True
                fetch_count += 1
        except Exception as exc:
            errors.append(f"Playwright fallback error: {exc}")

    if not seed_html:
        return SiteFingerprint(
            url=url,
            domain=domain,
            platform="unknown",
            platform_signals={},
            structure={},
            css_selectors={},
            pagination={},
            pdf_url_patterns=[],
            sample_links={},
            adapter_recommendation="needs_custom_adapter",
            seed_json=None,
            fetch_count=fetch_count,
            errors=errors or ["Empty response from seed URL"],
            js_rendered=js_rendered,
        )

    seed_soup = _soup(seed_html)

    # --- Phase 2: platform detection ---
    signals = _detect_platform_signals(seed_html, seed_headers, url)
    platform = _resolve_platform(signals, url, domain)

    # --- Phase 3: link classification on seed page ---
    counts, samples = _classify_links(seed_soup, url, domain)
    structure = {
        "archive_count": counts.get("archive", 0),
        "issue_count": counts.get("issue", 0),
        "article_count": counts.get("article", 0),
        "pdf_count": counts.get("pdf", 0),
        "other_count": counts.get("other", 0),
    }

    # Collect initial PDF samples
    all_pdf_urls: List[str] = list(samples.get("pdf", []))

    # --- Phase 4: follow 1-2 archive/issue links for deeper structure ---
    pages_remaining = max_pages - fetch_count
    follow_candidates = samples.get("archive", [])[:1] + samples.get("issue", [])[:1]

    for follow_url in follow_candidates[:2]:
        if pages_remaining <= 0:
            break
        if follow_url == url:
            continue
        try:
            html2, hdrs2, status2 = _fetch_html(session, follow_url)
            fetch_count += 1
            pages_remaining -= 1
            if html2 and status2 < 400:
                soup2 = _soup(html2)
                counts2, samples2 = _classify_links(soup2, follow_url, domain)
                # Accumulate counts
                for key in ("archive", "issue", "article", "pdf", "other"):
                    struct_key = f"{key}_count"
                    structure[struct_key] = structure.get(struct_key, 0) + counts2.get(key, 0)
                # Accumulate PDF samples
                for pu in samples2.get("pdf", []):
                    if pu not in all_pdf_urls:
                        all_pdf_urls.append(pu)
                # Update platform signals from deeper pages if still unknown
                if platform == "unknown":
                    deeper_signals = _detect_platform_signals(html2, hdrs2, follow_url)
                    signals.update({k: v for k, v in deeper_signals.items() if k not in signals})
                    platform = _resolve_platform(signals, follow_url, domain)
            else:
                errors.append(f"Follow fetch returned HTTP {status2} for {follow_url}")
        except Exception as exc:
            errors.append(f"Follow fetch error for {follow_url}: {exc}")

    # --- Phase 5: CSS selectors & pagination (from seed soup) ---
    css_selectors = _detect_css_selectors(seed_soup)
    pagination = _detect_pagination(seed_soup, url)

    # Try an issue or article page for better PDF selector detection
    if "pdf_link" not in css_selectors and pages_remaining > 0:
        article_candidates = samples.get("article", [])[:1] + samples.get("issue", [])[:2]
        for cand_url in article_candidates:
            if cand_url == url or pages_remaining <= 0:
                continue
            try:
                html_c, _, status_c = _fetch_html(session, cand_url)
                fetch_count += 1
                pages_remaining -= 1
                if html_c and status_c < 400:
                    soup_c = _soup(html_c)
                    extra_sel = _detect_css_selectors(soup_c)
                    css_selectors.update(
                        {k: v for k, v in extra_sel.items() if k not in css_selectors}
                    )
                    _, samples_c = _classify_links(soup_c, cand_url, domain)
                    for pu in samples_c.get("pdf", []):
                        if pu not in all_pdf_urls:
                            all_pdf_urls.append(pu)
                    if "pdf_link" in css_selectors:
                        break
            except Exception as exc:
                errors.append(f"CSS-probe fetch error for {cand_url}: {exc}")

    # --- Phase 6: PDF URL templates ---
    pdf_url_patterns = _extract_pdf_url_patterns(all_pdf_urls)

    # --- Phase 7: journal title ---
    journal_title = _detect_journal_title(seed_soup)

    # --- Phase 8: adapter recommendation ---
    # Check registry first for an exact or heuristic match
    registry_adapter = _adapter_from_registry(url)
    if registry_adapter and registry_adapter not in (
        "GenericAdapter",
        "Adapter",
    ):
        adapter_recommendation = registry_adapter
    else:
        adapter_recommendation = _PLATFORM_TO_ADAPTER.get(platform, "needs_custom_adapter")

    # --- Phase 9: seed JSON ---
    seed_json: Optional[Dict[str, object]] = None
    if adapter_recommendation not in ("needs_custom_adapter", "GenericAdapter"):
        seed_json = _build_seed_json(url, domain, platform, journal_title)

    # Merge samples (seed + followed pages) for caller convenience
    merged_samples: Dict[str, List[str]] = {}
    for cat in ("archive", "issue", "article", "pdf"):
        entries = list(samples.get(cat, []))
        # Add PDF URLs from all_pdf_urls for the pdf bucket
        if cat == "pdf":
            for pu in all_pdf_urls:
                if pu not in entries:
                    entries.append(pu)
        merged_samples[cat] = entries[:5]

    return SiteFingerprint(
        url=url,
        domain=domain,
        platform=platform,
        platform_signals=signals,
        structure=structure,
        css_selectors=css_selectors,
        pagination=pagination,
        pdf_url_patterns=pdf_url_patterns,
        sample_links=merged_samples,
        adapter_recommendation=adapter_recommendation,
        seed_json=seed_json,
        fetch_count=fetch_count,
        errors=errors,
        js_rendered=js_rendered,
    )


def fingerprint_to_dict(fp: SiteFingerprint) -> dict:
    """Serialise a ``SiteFingerprint`` to a JSON-safe dictionary.

    All values are coerced to JSON-compatible primitives.  The ``seed_json``
    sub-dict is embedded as-is (it is already JSON-friendly by construction).
    """
    return {
        "url": fp.url,
        "domain": fp.domain,
        "platform": fp.platform,
        "platform_signals": {
            k: (list(v) if isinstance(v, (set, tuple)) else v)
            for k, v in fp.platform_signals.items()
        },
        "structure": fp.structure,
        "css_selectors": fp.css_selectors,
        "pagination": {
            k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v)
            for k, v in fp.pagination.items()
        },
        "pdf_url_patterns": fp.pdf_url_patterns,
        "sample_links": fp.sample_links,
        "adapter_recommendation": fp.adapter_recommendation,
        "seed_json": fp.seed_json,
        "fetch_count": fp.fetch_count,
        "errors": fp.errors,
        "js_rendered": fp.js_rendered,
    }
