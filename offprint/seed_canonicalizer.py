from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .coverage_tools.live_crawl import _guess_archive_candidates, _looks_js_required
from .polite_requests import PoliteRequestsSession

_DIGITAL_COMMONS_HOST_HINTS = (
    "digitalcommons.",
    "scholarlycommons.",
    "scholarship.",
    "engagedscholarship.",
    "repository.",
    "uknowledge.",
    "via.library.",
)


def _normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return url
    if url.startswith("//"):
        url = "https:" + url
    if "://" not in url:
        url = "https://" + url
    normalized, _frag = urldefrag(url)
    return normalized


def _now_ts() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _extract_links(html: str, base_url: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    out: list[tuple[str, str]] = []
    for a in soup.select("a[href]"):
        text = a.get_text(" ", strip=True)
        href = a.get("href") or ""
        if not href:
            continue
        out.append((text, urljoin(base_url, href)))
    return out


def _contains_window_journal(html: str) -> bool:
    return "window.JOURNAL" in html or "published_article_ids" in html


def _guess_platform(final_url: str, html: str) -> str:
    host = urlparse(final_url).netloc.lower()
    lowered = html.lower()

    if any(h in host for h in _DIGITAL_COMMONS_HOST_HINTS) or "bepress" in lowered:
        return "digital_commons"
    if "/index.php/" in final_url or "pkp" in lowered or "open journal systems" in lowered:
        return "ojs"
    if (
        "wp-content" in lowered
        or "wp-json" in lowered
        or 'name="generator" content="wordpress' in lowered
    ):
        return "wordpress"
    if "scholasticahq.com" in host or _contains_window_journal(html):
        return "scholastica"
    return "unknown"


def _is_digital_commons_issue_index(html: str) -> bool:
    soup = BeautifulSoup(html, "lxml")
    return bool(soup.select(".issue a") or soup.select("h3 a"))


def _is_ojs_issue_archive(html: str) -> bool:
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").lower()
        if "issue/view" in href:
            return True
    return "pkp" in html.lower() and "issue" in html.lower()


def _is_wordpress_indexable(
    base_url: str, session: requests.Session
) -> tuple[bool, dict[str, Any]]:
    signals: dict[str, Any] = {}

    wp_api = base_url.rstrip("/") + "/wp-json/wp/v2/posts?per_page=1"
    try:
        r = session.get(wp_api, timeout=15, headers={"Accept": "application/json"})
        signals["wp_api_status"] = r.status_code
        if r.status_code == 200:
            try:
                payload = r.json()
                if isinstance(payload, list):
                    signals["wp_api_list"] = True
                    return True, signals
            except Exception:
                pass
    except Exception as e:
        signals["wp_api_error"] = f"{type(e).__name__}: {e}"

    wp_sitemap = base_url.rstrip("/") + "/wp-sitemap.xml"
    try:
        r2 = session.get(wp_sitemap, timeout=15, headers={"Accept": "application/xml"})
        signals["wp_sitemap_status"] = r2.status_code
        if r2.status_code == 200 and ("<urlset" in r2.text or "<sitemapindex" in r2.text):
            return True, signals
    except Exception as e:
        signals["wp_sitemap_error"] = f"{type(e).__name__}: {e}"

    return False, signals


def _is_scholastica_page(html: str) -> bool:
    return _contains_window_journal(html)


@dataclass
class SeedCandidate:
    url: str
    reason: str
    fetch_mode: str  # derived|requests|playwright
    valid: bool = False
    signals: dict[str, Any] = field(default_factory=dict)


@dataclass
class SeedResolution:
    journal_name: str
    original_url: str
    final_url: str | None
    domain: str | None
    platform_guess: str
    candidates: list[SeedCandidate] = field(default_factory=list)
    chosen_start_urls: list[str] = field(default_factory=list)
    js_required: bool = False
    used_fallback: bool = False
    errors: list[str] = field(default_factory=list)
    evidence_dir: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, ensure_ascii=False, sort_keys=True) + "\n"


@dataclass
class _FetchResult:
    url: str
    final_url: str | None
    status_code: int | None
    html: str | None
    mode: str
    error: str | None = None


class SeedCanonicalizer:
    def __init__(
        self,
        *,
        evidence_root: str | Path = "artifacts/seed_evidence",
        timeout_s: int = 30,
        playwright_enabled: bool = True,
    ) -> None:
        self.evidence_root = Path(evidence_root)
        self.timeout_s = timeout_s
        self.playwright_enabled = playwright_enabled

    def canonicalize(
        self,
        *,
        journal_name: str,
        original_url: str,
        session: requests.Session | None = None,
    ) -> SeedResolution:
        url = _normalize_url(original_url)
        sess = session or PoliteRequestsSession(min_delay=1.0, max_delay=3.0)

        initial_domain = urlparse(url).netloc.lower() if url else ""
        evidence_dir = (
            self.evidence_root / (initial_domain or "unknown") / "seed_canonicalization" / _now_ts()
        )
        _safe_mkdir(evidence_dir)

        resolution = SeedResolution(
            journal_name=journal_name,
            original_url=original_url,
            final_url=None,
            domain=(initial_domain or None),
            platform_guess="unknown",
            evidence_dir=str(evidence_dir),
        )

        seed_fetch = self._fetch_requests(url, sess)
        if seed_fetch.html:
            _write_text(evidence_dir / "requests_seed.html", seed_fetch.html)

        seed_html = seed_fetch.html or ""
        final_url = seed_fetch.final_url or url
        resolution.final_url = final_url
        resolution.domain = urlparse(final_url).netloc.lower() or resolution.domain
        resolution.platform_guess = _guess_platform(final_url, seed_html)

        if self.playwright_enabled and self._should_try_playwright(seed_fetch):
            pw_fetch = self._fetch_playwright(final_url, evidence_dir)
            if pw_fetch.html:
                seed_html = pw_fetch.html
                resolution.js_required = True
                resolution.platform_guess = _guess_platform(final_url, seed_html)

        candidate_urls = self._build_candidates(final_url, seed_html, resolution.platform_guess)
        chosen, candidates, errors, js_required = self._validate_and_choose(
            candidate_urls,
            platform_hint=resolution.platform_guess,
            session=sess,
        )
        resolution.candidates.extend(candidates)
        resolution.errors.extend(errors)
        resolution.js_required = resolution.js_required or js_required

        if chosen:
            resolution.chosen_start_urls = chosen
        else:
            resolution.chosen_start_urls = [final_url]
            resolution.used_fallback = True
            resolution.errors.append("No valid archive seed detected; defaulting to final_url")

        _write_text(evidence_dir / "resolution.json", resolution.to_json())
        return resolution

    def _build_candidates(
        self, final_url: str, html: str, platform_hint: str
    ) -> list[SeedCandidate]:
        out: list[SeedCandidate] = []

        out.append(SeedCandidate(url=final_url, reason="final_url", fetch_mode="requests"))

        derived: list[tuple[str, str]] = []
        if platform_hint in {"digital_commons", "unknown"}:
            if not final_url.lower().endswith("/all_issues.html"):
                derived.append(
                    (final_url.rstrip("/") + "/all_issues.html", "digital_commons_all_issues")
                )
            else:
                derived.append((final_url, "digital_commons_all_issues"))

        if platform_hint in {"ojs", "unknown"}:
            if "/issue/archive" not in final_url.lower():
                derived.append((final_url.rstrip("/") + "/issue/archive", "ojs_issue_archive"))
            else:
                derived.append((final_url, "ojs_issue_archive"))

        parsed = urlparse(final_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if platform_hint in {"wordpress", "unknown"}:
            derived.append((final_url, "wordpress_seed"))
            derived.append((origin, "wordpress_origin"))

        if platform_hint in {"scholastica", "unknown"}:
            derived.append((final_url, "scholastica_seed"))

        for u, reason in derived:
            u = _normalize_url(u)
            if u and all(c.url != u for c in out):
                out.append(SeedCandidate(url=u, reason=reason, fetch_mode="derived"))

        for cand in _guess_archive_candidates(final_url, html)[:10]:
            cand = _normalize_url(cand)
            if not cand or any(c.url == cand for c in out):
                continue
            out.append(SeedCandidate(url=cand, reason="archive_link", fetch_mode="derived"))

        return out

    def _validate_and_choose(
        self,
        candidates: list[SeedCandidate],
        *,
        platform_hint: str,
        session: requests.Session,
    ) -> tuple[list[str], list[SeedCandidate], list[str], bool]:
        errors: list[str] = []
        js_required = False
        fetch_cache: dict[str, _FetchResult] = {}

        def fetch(url: str) -> _FetchResult:
            if url in fetch_cache:
                return fetch_cache[url]
            r = self._fetch_requests(url, session)
            fetch_cache[url] = r
            return r

        def validate_one(url: str) -> tuple[bool, dict[str, Any], bool]:
            fr = fetch(url)
            if not fr.html:
                return False, {"status_code": fr.status_code, "error": fr.error}, False
            html = fr.html
            signals: dict[str, Any] = {"status_code": fr.status_code, "final_url": fr.final_url}

            if self.playwright_enabled and (
                _looks_js_required(html) or fr.status_code in {401, 403, 406}
            ):
                pw = self._fetch_playwright(url, out_dir=None)
                if pw.html:
                    html = pw.html
                    js_required_local = True
                else:
                    js_required_local = False
            else:
                js_required_local = False

            lowered_url = (fr.final_url or url).lower()
            host = urlparse(fr.final_url or url).netloc.lower()

            if platform_hint == "digital_commons" or any(
                h in host for h in _DIGITAL_COMMONS_HOST_HINTS
            ):
                ok = _is_digital_commons_issue_index(html)
                signals["digital_commons_issue_links"] = ok
                return ok, signals, js_required_local

            if (
                platform_hint == "ojs"
                or "/index.php/" in lowered_url
                or "/issue/archive" in lowered_url
            ):
                ok = _is_ojs_issue_archive(html)
                signals["ojs_issue_archive"] = ok
                return ok, signals, js_required_local

            if platform_hint == "wordpress" or "wp-content" in html.lower():
                base_url = (fr.final_url or url).rstrip("/")
                ok, wp_signals = _is_wordpress_indexable(base_url, session)
                signals.update(wp_signals)
                return ok, signals, js_required_local

            if platform_hint == "scholastica" or "scholasticahq.com" in host:
                ok = _is_scholastica_page(html)
                signals["scholastica_window_journal"] = ok
                return ok, signals, js_required_local

            pdf_count = sum(
                1 for _t, u in _extract_links(html, fr.final_url or url) if ".pdf" in u.lower()
            )
            signals["pdf_link_count"] = pdf_count
            return pdf_count >= 3, signals, js_required_local

        def rank_key(c: SeedCandidate) -> tuple[int, int]:
            reason = c.reason
            if reason.startswith("digital_commons"):
                return (0, 0)
            if reason.startswith("ojs"):
                return (1, 0)
            if reason.startswith("wordpress"):
                return (2, 0)
            if reason.startswith("scholastica"):
                return (3, 0)
            if reason == "archive_link":
                return (4, 0)
            return (5, 0)

        ordered = sorted(candidates, key=rank_key)
        for cand in ordered:
            ok, signals, jsr = validate_one(cand.url)
            cand.valid = ok
            cand.signals = signals
            js_required = js_required or jsr
            if ok:
                return [cand.url], ordered, errors, js_required

        errors.append("No candidate passed validation")
        return [], ordered, errors, js_required

    def _fetch_requests(self, url: str, session: requests.Session) -> _FetchResult:
        url = _normalize_url(url)
        if not url:
            return _FetchResult(
                url=url,
                final_url=None,
                status_code=None,
                html=None,
                mode="requests",
                error="empty URL",
            )

        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        try:
            resp = session.get(url, timeout=self.timeout_s, headers=headers, allow_redirects=True)
            status = resp.status_code
            final_url = str(resp.url)
            html = resp.text if resp.text else None
            return _FetchResult(
                url=url,
                final_url=final_url,
                status_code=status,
                html=html,
                mode="requests",
                error=None if status < 400 else f"http {status}",
            )
        except Exception as e:
            return _FetchResult(
                url=url,
                final_url=None,
                status_code=None,
                html=None,
                mode="requests",
                error=f"{type(e).__name__}: {e}",
            )

    def _should_try_playwright(self, fetch: _FetchResult) -> bool:
        if fetch.html and _looks_js_required(fetch.html):
            return True
        if fetch.status_code in {401, 403, 406, 429}:
            return True
        if fetch.html is None:
            return True
        return False

    def _fetch_playwright(self, url: str, out_dir: Path | None) -> _FetchResult:
        if not self.playwright_enabled:
            return _FetchResult(
                url=url,
                final_url=None,
                status_code=None,
                html=None,
                mode="playwright",
                error="disabled",
            )

        try:
            from playwright.sync_api import sync_playwright
        except Exception as e:
            return _FetchResult(
                url=url,
                final_url=None,
                status_code=None,
                html=None,
                mode="playwright",
                error=f"playwright not installed: {type(e).__name__}: {e}",
            )

        html_path: Path | None = None
        png_path: Path | None = None
        if out_dir is not None:
            html_path = out_dir / "playwright_seed.html"
            png_path = out_dir / "playwright_seed.png"

        try:
            with sync_playwright() as pwt:
                browser = pwt.chromium.launch(headless=False)
                context = browser.new_context(
                    viewport={"width": 1600, "height": 900}, locale="en-US"
                )
                page = context.new_page()
                resp = page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_s * 1000)
                page.wait_for_timeout(1500)
                html = page.content()
                final_url = page.url
                status_code = resp.status if resp is not None else None
                if html_path is not None and html:
                    _write_text(html_path, html)
                if png_path is not None:
                    page.screenshot(path=str(png_path), full_page=True)
                context.close()
                browser.close()
                return _FetchResult(
                    url=url,
                    final_url=final_url,
                    status_code=status_code,
                    html=html,
                    mode="playwright",
                    error=None,
                )
        except Exception as e:
            return _FetchResult(
                url=url,
                final_url=None,
                status_code=None,
                html=None,
                mode="playwright",
                error=f"{type(e).__name__}: {e}",
            )


def canonicalize_seed(
    *,
    journal_name: str,
    original_url: str,
    evidence_root: str | Path = "artifacts/seed_evidence",
    timeout_s: int = 30,
    playwright_enabled: bool = True,
    session: requests.Session | None = None,
) -> SeedResolution:
    canon = SeedCanonicalizer(
        evidence_root=evidence_root,
        timeout_s=timeout_s,
        playwright_enabled=playwright_enabled,
    )
    return canon.canonicalize(journal_name=journal_name, original_url=original_url, session=session)


def canonical_sitemap_payload(
    *,
    resolution: SeedResolution,
    source: str = "auto",
    notes: str = "",
) -> dict[str, Any]:
    parsed = urlparse(resolution.final_url or resolution.original_url)
    domain = parsed.netloc.lower()
    stable_id = re.sub(r"[^a-z0-9]+", "-", (resolution.journal_name or domain).lower()).strip("-")
    payload: dict[str, Any] = {
        "id": stable_id or domain,
        "start_urls": resolution.chosen_start_urls,
        "source": source,
        "metadata": {
            "journal_name": resolution.journal_name,
            "original_url": resolution.original_url,
            "domain": domain,
            "platform": resolution.platform_guess,
            "canonicalized_at": date.today().isoformat(),
            "evidence_dir": resolution.evidence_dir,
            "notes": notes,
        },
    }
    return payload
