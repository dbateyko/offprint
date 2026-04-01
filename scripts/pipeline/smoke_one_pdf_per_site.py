#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import threading
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

# Allow running as `python scripts/smoke_one_pdf_per_site.py` from anywhere.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.adapters import UnmappedAdapterError, pick_adapter_for
from offprint.adapters.utils import compute_pdf_sha256_and_size
from offprint.adapters.generic import DEFAULT_HEADERS, DISCOVERY_UA_PROFILES
from offprint.browser_fallback import HeadlessBrowserFallbackSession
from offprint.cli import DEFAULT_RUNS_DIR, DEFAULT_SITEMAPS_DIR
from offprint.path_policy import warn_legacy_paths
from offprint.polite_requests import PoliteRequestsSession
from offprint.seed_catalog import SEED_STATUS_ACTIVE, filter_active, load_seed_entries
from offprint.seed_quality import (
    SeedStatus,
    assess_with_dedup,
    is_retryable_failure,
)

PRINT_LOCK = threading.Lock()
WAF_FALLBACK_FROZEN = True


def _log(message: str) -> None:
    with PRINT_LOCK:
        print(message, flush=True)


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


@dataclass
class SmokeResult:
    seed_url: str
    domain: str
    adapter: str
    ok: bool
    pdf_url: Optional[str] = None
    local_path: Optional[str] = None
    discovered_candidates: int = 0
    attempted_downloads: int = 0
    failure_reason: str = ""
    failure_details: List[str] = field(default_factory=list)
    seed_status: str = SeedStatus.VALID.value
    download_method: str = "none"
    fallback_reason: str = ""
    http_status: int = 0
    content_type: str = ""
    waf_action: str = ""
    pdf_sha256: str = ""
    pdf_size_bytes: Optional[int] = None
    article_title: str = ""
    article_authors: List[str] = field(default_factory=list)
    article_volume: str = ""
    article_issue: str = ""
    article_year: str = ""
    article_citation: str = ""
    article_pages: str = ""
    article_url: str = ""
    article_metadata: Dict[str, Any] = field(default_factory=dict)
    attempt_trace: List[str] = field(default_factory=list)


class FallbackLimiter:
    def __init__(self, max_total: int, max_per_domain: int):
        self.max_total = max(0, int(max_total))
        self.max_per_domain = max(0, int(max_per_domain))
        self.total_used = 0
        self.by_domain: Counter[str] = Counter()
        self._lock = threading.Lock()

    def try_acquire(self, domain: str) -> bool:
        domain_key = (domain or "unknown").lower()
        with self._lock:
            if self.max_total and self.total_used >= self.max_total:
                return False
            if self.max_per_domain and self.by_domain[domain_key] >= self.max_per_domain:
                return False
            self.total_used += 1
            self.by_domain[domain_key] += 1
            return True


def _build_targets(seeds: List[str], per_domain: bool) -> List[Tuple[str, str]]:
    filtered: List[str] = []
    for seed in seeds:
        parsed = urlparse(seed)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        filtered.append(seed)

    if not per_domain:
        return [(seed, (urlparse(seed).netloc or "unknown").lower()) for seed in filtered]

    def _seed_score(seed_url: str) -> int:
        lowered = seed_url.lower()
        score = 0
        if "issue/archive" in lowered or "all_issues.html" in lowered:
            score += 5
        if any(token in lowered for token in ("in-print", "online-edition")):
            score += 4
        if any(
            token in lowered for token in ("archive", "archives", "past-issues", "print-issues")
        ):
            score += 3
        if any(token in lowered for token in ("law-reviews", "/journals/", "/journal/")):
            score += 2
        if any(token in lowered for token in ("issues", "volume", "vol-")):
            score += 2
        if "[" in lowered and "]" in lowered:
            score -= 3
        if "preview=true" in lowered:
            score -= 2
        return score

    best_seed_by_domain: Dict[str, str] = {}
    best_score_by_domain: Dict[str, int] = {}
    for seed in filtered:
        domain = (urlparse(seed).netloc or "unknown").lower()
        score = _seed_score(seed)
        previous = best_score_by_domain.get(domain)
        if previous is None or score > previous:
            best_score_by_domain[domain] = score
            best_seed_by_domain[domain] = seed
    return sorted(
        [(seed, domain) for domain, seed in best_seed_by_domain.items()], key=lambda x: x[1]
    )


def _load_targets_from_file(path: str) -> List[str]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    targets: List[str] = []
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        targets.append(line)
    return targets


def _apply_window(
    targets: List[Tuple[str, str]], start_index: int = 0, limit: int = 0
) -> List[Tuple[str, str]]:
    start = max(0, int(start_index or 0))
    if limit and limit > 0:
        return targets[start : start + int(limit)]
    return targets[start:]


def _load_completed_targets(report_path: str, per_domain: bool) -> set[str]:
    if not report_path:
        return set()
    path = Path(report_path)
    if not path.exists():
        raise FileNotFoundError(f"resume report not found: {report_path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    completed = set()
    for result in payload.get("results", []):
        key = str(result.get("domain") if per_domain else result.get("seed_url") or "").strip()
        if key:
            completed.add(key.lower())
    return completed


def _partition_targets(
    targets: List[Tuple[str, str]],
    *,
    per_domain: bool,
    auto_skip_invalid_seeds: bool,
) -> Tuple[List[Tuple[str, str]], List[SmokeResult]]:
    seen: set[str] = set()
    runnable: List[Tuple[str, str]] = []
    skipped: List[SmokeResult] = []

    for seed_url, domain in targets:
        dedup_key = domain if per_domain else seed_url
        assessment = assess_with_dedup(seed_url, dedup_key, seen)

        if assessment.status == SeedStatus.SKIPPED_DUPLICATE:
            skipped.append(
                SmokeResult(
                    seed_url=seed_url,
                    domain=domain,
                    adapter="unassigned",
                    ok=False,
                    failure_reason="skipped_duplicate_seed",
                    failure_details=[assessment.reason],
                    seed_status=SeedStatus.SKIPPED_DUPLICATE.value,
                )
            )
            continue

        if assessment.status == SeedStatus.SKIPPED_INVALID and auto_skip_invalid_seeds:
            skipped.append(
                SmokeResult(
                    seed_url=seed_url,
                    domain=domain,
                    adapter="unassigned",
                    ok=False,
                    failure_reason="skipped_invalid_seed",
                    failure_details=[assessment.reason],
                    seed_status=SeedStatus.SKIPPED_INVALID.value,
                )
            )
            continue

        runnable.append((seed_url, domain))

    return runnable, skipped


def _sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z._-]+", "-", value).strip("-._")
    return cleaned or "download"


def _ensure_unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix or ".pdf"
    parent = path.parent
    index = 2
    while True:
        candidate = parent / f"{stem}-{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def _save_browser_pdf(pdf_url: str, out_dir: str, domain: str, body: bytes) -> str:
    domain_dir = Path(out_dir) / domain
    domain_dir.mkdir(parents=True, exist_ok=True)

    parsed = urlparse(pdf_url)
    base_name = (
        os.path.basename(parsed.path)
        or f"download-{hashlib.sha1(pdf_url.encode('utf-8')).hexdigest()[:10]}"
    )
    if not base_name.lower().endswith(".pdf"):
        base_name += ".pdf"
    filename = _sanitize_filename(base_name)
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"

    output = _ensure_unique_path(domain_dir / filename)
    output.write_bytes(body)
    return str(output)


def _is_known_waf_browser_block(status_code: int, content_type: str, waf_action: str) -> bool:
    lowered_ct = (content_type or "").lower()
    lowered_waf = (waf_action or "").lower()
    if lowered_waf == "challenge":
        return True
    return status_code in {202, 401, 403} and "text/html" in lowered_ct


def _should_fail_fast_browser_attempt(
    *,
    status_code: int,
    content_type: str,
    waf_action: str,
    error: str,
) -> bool:
    if _is_known_waf_browser_block(status_code, content_type, waf_action):
        return True
    lowered_error = (error or "").lower()
    if any(
        token in lowered_error
        for token in (
            "empty_browser_response",
            "invalid_pdf_from_browser",
            "pdf_link_click_not_found",
            "playwright_request_error",
            "playwright_navigation_error",
            "referer_navigation_error",
        )
    ):
        return True
    return False


def _normalize_article_metadata(raw: object) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    return {}


def _coerce_authors(raw: object) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(v).strip() for v in raw if str(v).strip()]
    value = str(raw).strip()
    if not value:
        return []
    return [value]


def _extract_article_fields(metadata: Dict[str, Any]) -> Dict[str, Any]:
    title = str(metadata.get("title") or "").strip()
    volume = str(metadata.get("volume") or "").strip()
    issue = str(metadata.get("issue") or "").strip()
    citation = str(metadata.get("citation") or "").strip()
    pages = str(metadata.get("pages") or "").strip()
    article_url = str(
        metadata.get("url") or metadata.get("article_url") or metadata.get("source_url") or ""
    ).strip()
    year = str(metadata.get("year") or "").strip()
    if not year:
        date_value = str(metadata.get("date") or "").strip()
        m = re.search(r"(19|20)\d{2}", date_value)
        year = m.group(0) if m else date_value

    return {
        "article_title": title,
        "article_authors": _coerce_authors(metadata.get("authors")),
        "article_volume": volume,
        "article_issue": issue,
        "article_year": year,
        "article_citation": citation,
        "article_pages": pages,
        "article_url": article_url,
    }


def _download_one_pdf_for_site(
    *,
    seed_url: str,
    domain: str,
    out_dir: str,
    max_depth: int,
    max_candidates: int,
    min_delay: float,
    max_delay: float,
    waf_playwright_fallback: bool,
    playwright_headless: bool,
    fallback_limiter: FallbackLimiter,
) -> SmokeResult:
    session = PoliteRequestsSession(min_delay=min_delay, max_delay=max_delay)
    try:
        adapter = pick_adapter_for(seed_url, session=session, allow_generic=False)
    except UnmappedAdapterError as exc:
        return SmokeResult(
            seed_url=seed_url,
            domain=domain,
            adapter="UnmappedAdapter",
            ok=False,
            failure_reason="todo_adapter_blocked",
            failure_details=[str(exc)],
            seed_status=SeedStatus.VALID.value,
            download_method="none",
        )
    configure_dc = getattr(adapter, "configure_dc", None)
    if callable(configure_dc):
        # Smoke mode targets one successful PDF quickly; avoid exhaustive DC enumeration.
        configure_dc(
            enum_mode="oai_only",
            max_oai_records=max(max_candidates, 1),
            max_sitemap_urls=max(max_candidates, 1),
            download_timeout=30,
            min_domain_delay_ms=max(int(min_delay * 1000), 0),
            max_domain_delay_ms=max(int(max_delay * 1000), 0),
        )
    adapter_name = adapter.__class__.__name__
    errors: List[str] = []
    attempt_trace: List[str] = []

    discovered = 0
    attempted = 0
    saw_waf = False
    saw_non_waf = False
    browser_mode = False
    browser_session: Optional[HeadlessBrowserFallbackSession] = None
    last_candidate_meta: Dict[str, Any] = {}
    last_candidate_pdf_url: Optional[str] = None
    seed_probe_status = 0
    seed_probe_content_type = ""
    seed_probe_waf_action = ""
    seed_probe_blocked = False

    try:
        try:
            for probe_ua in DISCOVERY_UA_PROFILES:
                probe_headers = dict(DEFAULT_HEADERS)
                probe_headers["User-Agent"] = probe_ua
                seed_probe = session.get(seed_url, headers=probe_headers, timeout=20)
                seed_probe_status = int(seed_probe.status_code or 0)
                seed_probe_content_type = str(seed_probe.headers.get("Content-Type") or "")
                seed_probe_waf_action = str(seed_probe.headers.get("x-amzn-waf-action") or "")
                seed_body = (seed_probe.text or "")[:1200].lower()
                probe_blocked = False
                if seed_probe_waf_action.lower() == "challenge":
                    probe_blocked = True
                elif seed_probe_status in {401, 403, 429, 503}:
                    probe_blocked = True
                elif (
                    seed_probe_status == 202
                    and "text/html" in seed_probe_content_type.lower()
                    and any(
                        token in seed_body
                        for token in [
                            "captcha",
                            "cloudflare",
                            "attention required",
                            "access denied",
                            "forbidden",
                            "bot detection",
                        ]
                    )
                ):
                    probe_blocked = True
                seed_probe_blocked = probe_blocked
                if not probe_blocked:
                    break
        except Exception:
            pass

        for result in adapter.discover_pdfs(seed_url, max_depth=max_depth):
            discovered += 1
            if discovered > max_candidates:
                break

            attempted += 1
            candidate_meta = _normalize_article_metadata(getattr(result, "metadata", None))
            article_fields = _extract_article_fields(candidate_meta)
            last_candidate_meta = dict(candidate_meta)
            last_candidate_pdf_url = result.pdf_url
            if browser_mode and browser_session is not None:
                try:
                    browser_result = browser_session.fetch_pdf(
                        result.pdf_url,
                        referer=result.page_url,
                    )
                except Exception as exc:
                    errors.append(
                        f"WAF_BLOCKED_HEADLESS (status=0): browser_fallback_unavailable: {exc}"
                    )
                    break
                attempt_trace.append(
                    "playwright "
                    "reused=true "
                    f"status={browser_result.status_code} "
                    f"ctype={browser_result.content_type} "
                    f"waf={browser_result.waf_action} "
                    f"ok={browser_result.ok} "
                    f"error={browser_result.error}"
                )
                if browser_result.ok and browser_result.body.startswith(b"%PDF-"):
                    browser_path = _save_browser_pdf(
                        result.pdf_url,
                        out_dir=out_dir,
                        domain=domain,
                        body=browser_result.body,
                    )
                    pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(browser_path)
                    return SmokeResult(
                        seed_url=seed_url,
                        domain=domain,
                        adapter=adapter_name,
                        ok=True,
                        pdf_url=result.pdf_url,
                        local_path=browser_path,
                        discovered_candidates=discovered,
                        attempted_downloads=attempted,
                        seed_status=SeedStatus.VALID.value,
                        download_method="playwright",
                        fallback_reason="waf_challenge",
                        http_status=browser_result.status_code,
                        content_type=browser_result.content_type,
                        waf_action=browser_result.waf_action,
                        pdf_sha256=str(pdf_sha256 or ""),
                        pdf_size_bytes=pdf_size_bytes,
                        article_metadata=candidate_meta,
                        **article_fields,
                        attempt_trace=attempt_trace[-25:],
                    )
                errors.append(
                    "WAF_BLOCKED_HEADLESS"
                    f" (status={browser_result.status_code}): {browser_result.error}"
                )
                if _should_fail_fast_browser_attempt(
                    status_code=browser_result.status_code,
                    content_type=browser_result.content_type,
                    waf_action=browser_result.waf_action,
                    error=browser_result.error,
                ):
                    break
                continue

            local_path = adapter.download_pdf(result.pdf_url, out_dir=os.path.join(out_dir, domain))

            meta = dict(getattr(adapter, "last_download_meta", {}) or {})
            err_type = str(meta.get("error_type") or "download_failed")
            msg = str(meta.get("message") or "")
            status = int(meta.get("status_code") or 0)
            content_type = str(meta.get("content_type") or "")
            waf_action = str(meta.get("waf_action") or "")

            attempt_trace.append(
                f"requests attempt={attempted} err={err_type} status={status} "
                f"ctype={content_type} waf={waf_action}"
            )

            if local_path and os.path.exists(local_path):
                size_value = meta.get("pdf_size_bytes")
                if isinstance(size_value, str) and size_value.isdigit():
                    size_value = int(size_value)
                if not isinstance(size_value, int):
                    size_value = None
                return SmokeResult(
                    seed_url=seed_url,
                    domain=domain,
                    adapter=adapter_name,
                    ok=True,
                    pdf_url=result.pdf_url,
                    local_path=local_path,
                    discovered_candidates=discovered,
                    attempted_downloads=attempted,
                    seed_status=SeedStatus.VALID.value,
                    download_method=str(meta.get("download_method") or "requests"),
                    http_status=status,
                    content_type=content_type,
                    waf_action=waf_action,
                    pdf_sha256=str(meta.get("pdf_sha256") or ""),
                    pdf_size_bytes=size_value,
                    article_metadata=candidate_meta,
                    **article_fields,
                    attempt_trace=attempt_trace[-25:],
                )

            if status:
                errors.append(f"{err_type} (status={status}): {msg}")
            else:
                errors.append(f"{err_type}: {msg}")

            if err_type == "waf_challenge":
                saw_waf = True
                if waf_playwright_fallback:
                    if not browser_mode:
                        if not fallback_limiter.try_acquire(domain):
                            errors.append("waf_fallback_skipped: limiter_exhausted")
                            break
                        browser_mode = True
                        try:
                            browser_session = HeadlessBrowserFallbackSession(
                                timeout_seconds=12,
                                headless=playwright_headless,
                            )
                        except Exception as exc:
                            errors.append(
                                f"WAF_BLOCKED_HEADLESS (status=0): browser_fallback_init_failed: {exc}"
                            )
                            break
                        attempt_trace.append("playwright sticky_mode=true initialized=true")
                    try:
                        browser_result = browser_session.fetch_pdf(
                            result.pdf_url,
                            referer=result.page_url,
                        )
                    except Exception as exc:
                        errors.append(
                            f"WAF_BLOCKED_HEADLESS (status=0): browser_fallback_unavailable: {exc}"
                        )
                        break
                    attempt_trace.append(
                        "playwright "
                        "reused=false "
                        f"status={browser_result.status_code} "
                        f"ctype={browser_result.content_type} "
                        f"waf={browser_result.waf_action} "
                        f"ok={browser_result.ok} "
                        f"error={browser_result.error}"
                    )
                    if browser_result.ok and browser_result.body.startswith(b"%PDF-"):
                        browser_path = _save_browser_pdf(
                            result.pdf_url,
                            out_dir=out_dir,
                            domain=domain,
                            body=browser_result.body,
                        )
                        pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(browser_path)
                        return SmokeResult(
                            seed_url=seed_url,
                            domain=domain,
                            adapter=adapter_name,
                            ok=True,
                            pdf_url=result.pdf_url,
                            local_path=browser_path,
                            discovered_candidates=discovered,
                            attempted_downloads=attempted,
                            seed_status=SeedStatus.VALID.value,
                            download_method="playwright",
                            fallback_reason="waf_challenge",
                            http_status=browser_result.status_code,
                            content_type=browser_result.content_type,
                            waf_action=browser_result.waf_action,
                            pdf_sha256=str(pdf_sha256 or ""),
                            pdf_size_bytes=pdf_size_bytes,
                            article_metadata=candidate_meta,
                            **article_fields,
                            attempt_trace=attempt_trace[-25:],
                        )
                    errors.append(
                        "WAF_BLOCKED_HEADLESS"
                        f" (status={browser_result.status_code}): {browser_result.error}"
                    )
                    if _should_fail_fast_browser_attempt(
                        status_code=browser_result.status_code,
                        content_type=browser_result.content_type,
                        waf_action=browser_result.waf_action,
                        error=browser_result.error,
                    ):
                        break
                    continue
            else:
                saw_non_waf = True
                if (
                    err_type == "http_error"
                    and status in {401, 403}
                    and "DigitalCommons" in adapter_name
                ):
                    # Avoid spending 30 attempts on hosts that uniformly deny direct fetches.
                    break

        if discovered == 0:
            if seed_probe_blocked and waf_playwright_fallback:
                if not browser_mode:
                    if fallback_limiter.try_acquire(domain):
                        browser_mode = True
                        try:
                            browser_session = HeadlessBrowserFallbackSession(
                                timeout_seconds=12,
                                headless=playwright_headless,
                            )
                            attempt_trace.append("playwright sticky_mode=true initialized=true")
                        except Exception as exc:
                            errors.append(
                                f"WAF_BLOCKED_HEADLESS (status=0): browser_fallback_init_failed: {exc}"
                            )
                            browser_mode = False
                    else:
                        errors.append("waf_fallback_skipped: limiter_exhausted")

                discovered_links: List[str] = []
                if browser_mode and browser_session is not None:
                    try:
                        discovered_links = browser_session.discover_pdf_links(
                            seed_url, max_links=max_candidates
                        )
                    except Exception as exc:
                        errors.append(
                            f"WAF_BLOCKED_HEADLESS (status=0): browser_discovery_failed: {exc}"
                        )
                    attempt_trace.append(
                        f"playwright discover seed={seed_url} candidates={len(discovered_links)}"
                    )

                if discovered_links:
                    discovered = len(discovered_links)
                    saw_waf = True
                    for discovered_pdf in discovered_links:
                        attempted += 1
                        last_candidate_pdf_url = discovered_pdf
                        try:
                            browser_result = browser_session.fetch_pdf(
                                discovered_pdf,
                                referer=seed_url,
                            )
                        except Exception as exc:
                            errors.append(
                                f"WAF_BLOCKED_HEADLESS (status=0): browser_fallback_unavailable: {exc}"
                            )
                            break
                        attempt_trace.append(
                            "playwright "
                            "seed_discovery=true "
                            f"status={browser_result.status_code} "
                            f"ctype={browser_result.content_type} "
                            f"waf={browser_result.waf_action} "
                            f"ok={browser_result.ok} "
                            f"error={browser_result.error}"
                        )
                        if browser_result.ok and browser_result.body.startswith(b"%PDF-"):
                            browser_path = _save_browser_pdf(
                                discovered_pdf,
                                out_dir=out_dir,
                                domain=domain,
                                body=browser_result.body,
                            )
                            pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(browser_path)
                            return SmokeResult(
                                seed_url=seed_url,
                                domain=domain,
                                adapter=adapter_name,
                                ok=True,
                                pdf_url=discovered_pdf,
                                local_path=browser_path,
                                discovered_candidates=discovered,
                                attempted_downloads=attempted,
                                seed_status=SeedStatus.VALID.value,
                                download_method="playwright",
                                fallback_reason="seed_probe_waf",
                                http_status=browser_result.status_code,
                                content_type=browser_result.content_type,
                                waf_action=browser_result.waf_action,
                                pdf_sha256=str(pdf_sha256 or ""),
                                pdf_size_bytes=pdf_size_bytes,
                                article_metadata={},
                                attempt_trace=attempt_trace[-25:],
                            )
                        errors.append(
                            "WAF_BLOCKED_HEADLESS"
                            f" (status={browser_result.status_code}): {browser_result.error}"
                        )
                        if _should_fail_fast_browser_attempt(
                            status_code=browser_result.status_code,
                            content_type=browser_result.content_type,
                            waf_action=browser_result.waf_action,
                            error=browser_result.error,
                        ):
                            break

            if seed_probe_blocked:
                saw_waf = True
                reason = "WAF_BLOCKED_HEADLESS"
                errors.append(
                    f"seed_probe_blocked (status={seed_probe_status}, ctype={seed_probe_content_type}, waf={seed_probe_waf_action})"
                )
            else:
                reason = "no PDF candidates discovered"
        elif attempted == 0:
            reason = "no candidate downloads attempted"
        elif saw_waf and not saw_non_waf:
            reason = "WAF_BLOCKED_HEADLESS"
        else:
            reason = "all candidate downloads failed"

        last_meta = dict(getattr(adapter, "last_download_meta", {}) or {})
        fail_article_fields = _extract_article_fields(last_candidate_meta)
        return SmokeResult(
            seed_url=seed_url,
            domain=domain,
            adapter=adapter_name,
            ok=False,
            pdf_url=last_candidate_pdf_url,
            discovered_candidates=discovered,
            attempted_downloads=attempted,
            failure_reason=reason,
            failure_details=errors[-7:],
            seed_status=SeedStatus.VALID.value,
            download_method="none",
            fallback_reason="waf_challenge" if saw_waf else "",
            http_status=int(last_meta.get("status_code") or seed_probe_status or 0),
            content_type=str(last_meta.get("content_type") or seed_probe_content_type or ""),
            waf_action=str(last_meta.get("waf_action") or seed_probe_waf_action or ""),
            article_metadata=last_candidate_meta,
            **fail_article_fields,
            attempt_trace=attempt_trace[-25:],
        )

    except Exception as exc:
        fail_article_fields = _extract_article_fields(last_candidate_meta)
        return SmokeResult(
            seed_url=seed_url,
            domain=domain,
            adapter=adapter_name,
            ok=False,
            pdf_url=last_candidate_pdf_url,
            discovered_candidates=discovered,
            attempted_downloads=attempted,
            failure_reason=f"exception: {type(exc).__name__}: {exc}",
            failure_details=traceback.format_exc().splitlines()[-10:],
            seed_status=SeedStatus.VALID.value,
            download_method="none",
            article_metadata=last_candidate_meta,
            **fail_article_fields,
            attempt_trace=attempt_trace[-25:],
        )
    finally:
        try:
            if browser_session is not None:
                browser_session.close()
        except Exception:
            pass
        try:
            session.close()
        except Exception:
            pass


def _build_report(
    *,
    mode: str,
    seeds_loaded: int,
    targets_queued: int,
    results: List[SmokeResult],
    interrupted: bool,
    args: argparse.Namespace,
) -> Dict[str, object]:
    seed_status_counts = Counter(r.seed_status for r in results)
    valid_results = [r for r in results if r.seed_status == SeedStatus.VALID.value]
    passed = sum(1 for r in valid_results if r.ok)
    failed = sum(1 for r in valid_results if not r.ok)

    failure_reason_counts = Counter(r.failure_reason for r in valid_results if not r.ok)
    download_method_counts = Counter(r.download_method for r in valid_results)

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mode": mode,
        "targets": len(valid_results),
        "targets_reported": len(results),
        "targets_queued": targets_queued,
        "interrupted": interrupted,
        "passed": passed,
        "failed": failed,
        "seed_status_counts": dict(seed_status_counts),
        "failure_reason_counts": dict(failure_reason_counts),
        "download_method_counts": dict(download_method_counts),
        "config": {
            "sitemaps_dir": args.sitemaps_dir,
            "out_dir": args.out_dir,
            "max_workers": args.max_workers,
            "max_depth": args.max_depth,
            "max_candidates": args.max_candidates,
            "min_delay": args.min_delay,
            "max_delay": args.max_delay,
            "checkpoint_every": args.checkpoint_every,
            "auto_skip_invalid_seeds": args.auto_skip_invalid_seeds,
            "waf_playwright_fallback": args.waf_playwright_fallback,
            "max_browser_fallback_total": args.max_browser_fallback_total,
            "max_browser_fallback_per_domain": args.max_browser_fallback_per_domain,
            "resume_report": args.resume_report,
            "target_file": args.target_file,
            "start_index": args.start_index,
            "limit": args.limit,
        },
        "results": [
            asdict(r) for r in sorted(results, key=lambda x: (x.domain.lower(), x.seed_url.lower()))
        ],
    }


def _write_json_atomic(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp_path, path)


def _emit_retryable_targets(
    report_dir: str, stamp: str, results: List[SmokeResult]
) -> Optional[Path]:
    retryable = []
    for r in results:
        if r.seed_status != SeedStatus.VALID.value:
            continue
        if r.ok:
            continue
        if is_retryable_failure(r.failure_reason, r.failure_details):
            retryable.append((r.domain, r.seed_url))

    if not retryable:
        return None

    unique = sorted(set(retryable))
    out_path = Path(report_dir) / f"smoke_retry_targets_{stamp}.txt"
    out_path.write_text(
        "\n".join(f"{domain}\t{seed_url}" for domain, seed_url in unique) + "\n",
        encoding="utf-8",
    )
    return out_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test all sites by downloading exactly one PDF per site"
    )
    parser.add_argument("--sitemaps-dir", default=DEFAULT_SITEMAPS_DIR)
    parser.add_argument("--out-dir", default="artifacts/smoke/pdfs")
    parser.add_argument("--report-dir", default=DEFAULT_RUNS_DIR)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--max-depth", type=int, default=0)
    parser.add_argument("--max-candidates", type=int, default=30)
    parser.add_argument("--min-delay", type=float, default=0.25)
    parser.add_argument("--max-delay", type=float, default=0.75)
    parser.add_argument(
        "--per-seed",
        action="store_true",
        help="Run one-PDF smoke test per seed instead of per domain",
    )
    parser.add_argument(
        "--resume-report",
        default="",
        help=(
            "Path to a prior smoke report JSON. Completed targets from that report "
            "are skipped so only untested/unfinished targets run."
        ),
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=10,
        help="Write a checkpoint report every N completed attempted targets",
    )
    parser.add_argument(
        "--auto-skip-invalid-seeds",
        dest="auto_skip_invalid_seeds",
        action="store_true",
        default=True,
        help="Skip obvious non-journal/auth seeds and report them as skipped",
    )
    parser.add_argument(
        "--no-auto-skip-invalid-seeds",
        dest="auto_skip_invalid_seeds",
        action="store_false",
        help="Do not skip invalid seeds",
    )
    parser.add_argument(
        "--waf-playwright-fallback",
        dest="waf_playwright_fallback",
        action="store_true",
        default=False,
        help="Deprecated flag. WAF/browser fallback lane is permanently disabled.",
    )
    parser.add_argument(
        "--no-waf-playwright-fallback",
        dest="waf_playwright_fallback",
        action="store_false",
        help="No-op; WAF/browser fallback lane is permanently disabled.",
    )
    parser.add_argument(
        "--playwright-headless",
        action="store_true",
        default=False,
        help="Run Playwright in headless mode",
    )
    parser.add_argument("--max-browser-fallback-total", type=int, default=60)
    parser.add_argument("--max-browser-fallback-per-domain", type=int, default=3)
    parser.add_argument(
        "--target-file",
        default="",
        help="Optional file containing explicit seed URLs (one per line)",
    )
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def _warn_legacy_paths(args: argparse.Namespace) -> None:
    warn_legacy_paths(
        tool_name="smoke",
        values_by_arg={
            "out_dir": str(getattr(args, "out_dir", "")),
            "report_dir": str(getattr(args, "report_dir", "")),
        },
        legacy_by_arg={"out_dir": {"pdfs_smoke"}, "report_dir": {"runs"}},
    )


def main() -> None:
    args = _parse_args()
    _warn_legacy_paths(args)
    if WAF_FALLBACK_FROZEN:
        if args.waf_playwright_fallback:
            _log(
                "[smoke] Ignoring --waf-playwright-fallback: "
                "WAF/browser fallback lane is permanently disabled."
            )
        args.waf_playwright_fallback = False
        args.max_browser_fallback_total = 0
        args.max_browser_fallback_per_domain = 0

    per_domain = not args.per_seed

    if args.target_file:
        seeds = _load_targets_from_file(args.target_file)
    else:
        all_entries = load_seed_entries(args.sitemaps_dir)
        active_entries = filter_active(all_entries)
        seeds = [entry.seed_url for entry in active_entries]
        inactive_count = sum(1 for entry in all_entries if entry.status != SEED_STATUS_ACTIVE)
        if inactive_count:
            by_status: Dict[str, int] = {}
            for entry in all_entries:
                if entry.status == SEED_STATUS_ACTIVE:
                    continue
                by_status[entry.status] = by_status.get(entry.status, 0) + 1
            rendered = ", ".join(
                f"{status}={count}" for status, count in sorted(by_status.items())
            )
            _log(f"[smoke] Excluding non-active sitemap seeds: {inactive_count} ({rendered})")

    targets = _build_targets(seeds, per_domain=per_domain)
    targets = _apply_window(targets, start_index=args.start_index, limit=args.limit)

    completed_targets = _load_completed_targets(args.resume_report, per_domain=per_domain)
    if completed_targets:
        filtered = []
        for seed_url, domain in targets:
            key = domain.lower() if per_domain else seed_url.lower()
            if key in completed_targets:
                continue
            filtered.append((seed_url, domain))
        skipped = len(targets) - len(filtered)
        targets = filtered
        _log(
            f"[smoke] Resume mode enabled: skipped={skipped} "
            f"remaining={len(targets)} from report={args.resume_report}"
        )

    runnable_targets, skipped_results = _partition_targets(
        targets,
        per_domain=per_domain,
        auto_skip_invalid_seeds=args.auto_skip_invalid_seeds,
    )

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.report_dir, exist_ok=True)

    mode = "domain" if per_domain else "seed"
    _log(
        f"[smoke] Starting one-PDF smoke test in {mode} mode: "
        f"targets={len(runnable_targets)} seeds_loaded={len(seeds)} workers={args.max_workers}"
    )
    if skipped_results:
        _log(
            f"[smoke] Pre-skip targets={len(skipped_results)} "
            f"(invalid={sum(1 for r in skipped_results if r.seed_status == SeedStatus.SKIPPED_INVALID.value)}, "
            f"duplicate={sum(1 for r in skipped_results if r.seed_status == SeedStatus.SKIPPED_DUPLICATE.value)})"
        )

    results: List[SmokeResult] = list(skipped_results)
    futures = {}
    interrupted = False

    stamp = _utc_stamp()
    report_path = Path(args.report_dir) / f"smoke_one_pdf_{stamp}.json"

    limiter = FallbackLimiter(
        max_total=args.max_browser_fallback_total,
        max_per_domain=args.max_browser_fallback_per_domain,
    )

    def checkpoint() -> None:
        report = _build_report(
            mode=mode,
            seeds_loaded=len(seeds),
            targets_queued=len(runnable_targets),
            results=results,
            interrupted=interrupted,
            args=args,
        )
        _write_json_atomic(report_path, report)

    checkpoint()

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        for i, (seed_url, domain) in enumerate(runnable_targets, start=1):
            _log(f"[smoke] QUEUE [{i}/{len(runnable_targets)}] {domain} <- {seed_url}")
            fut = executor.submit(
                _download_one_pdf_for_site,
                seed_url=seed_url,
                domain=domain,
                out_dir=args.out_dir,
                max_depth=args.max_depth,
                max_candidates=args.max_candidates,
                min_delay=args.min_delay,
                max_delay=args.max_delay,
                waf_playwright_fallback=args.waf_playwright_fallback,
                playwright_headless=args.playwright_headless,
                fallback_limiter=limiter,
            )
            futures[fut] = (i, domain)

        completed = 0
        try:
            for fut in as_completed(futures):
                completed += 1
                idx, domain = futures[fut]
                try:
                    result = fut.result()
                except Exception as exc:
                    result = SmokeResult(
                        seed_url="",
                        domain=domain,
                        adapter="unknown",
                        ok=False,
                        failure_reason=f"worker exception: {exc}",
                        seed_status=SeedStatus.VALID.value,
                    )
                results.append(result)

                if result.ok:
                    _log(
                        f"[smoke] PASS [{completed}/{len(runnable_targets)}] {domain} "
                        f"adapter={result.adapter} method={result.download_method} "
                        f"pdf={result.pdf_url} file={result.local_path}"
                    )
                else:
                    _log(
                        f"[smoke] FAIL [{completed}/{len(runnable_targets)}] {domain} "
                        f"adapter={result.adapter} reason={result.failure_reason} "
                        f"(discovered={result.discovered_candidates}, attempted={result.attempted_downloads})"
                    )
                    for detail in result.failure_details[:3]:
                        _log(f"[smoke]   detail: {detail}")

                checkpoint_every = max(1, int(args.checkpoint_every))
                if completed % checkpoint_every == 0:
                    checkpoint()
        except KeyboardInterrupt:
            interrupted = True
            _log("[smoke] Interrupted by user. Writing partial report...")

    checkpoint()

    valid_results = [r for r in results if r.seed_status == SeedStatus.VALID.value]
    passed = sum(1 for r in valid_results if r.ok)
    failed = sum(1 for r in valid_results if not r.ok)

    retry_path = _emit_retryable_targets(args.report_dir, stamp, results)

    summary = (
        f"[smoke] COMPLETE pass={passed} fail={failed} total={len(valid_results)} "
        f"reported={len(results)} report={report_path}"
    )
    if retry_path:
        summary += f" retry_targets={retry_path}"
    _log(summary)

    if interrupted or failed > 0:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
