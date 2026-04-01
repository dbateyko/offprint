from __future__ import annotations

import hashlib
import os
import random
import time
import urllib.robotparser
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import requests

# Try to import curl_cffi for TLS fingerprint impersonation
# Falls back to requests if not available
try:
    from curl_cffi import requests as curl_requests
    CURL_CFFI_AVAILABLE = True
except ImportError:
    curl_requests = None  # type: ignore
    CURL_CFFI_AVAILABLE = False


PROFILE_USER_AGENTS: Dict[str, str] = {
    "browser": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "transparent": (
        "LawReviewScraper/1.0 "
        "(+https://github.com/dbateyko/law-review-scrapers; contact: opensource@example.com)"
    ),
    "python_requests": f"python-requests/{requests.__version__}",
    "wget": "Wget/1.21.4",
    "curl": "curl/8.7.1",
}

# Chrome version for curl_cffi TLS impersonation
CURL_CFFI_IMPERSONATE = "chrome124"

PROFILE_EXTRA_HEADERS: Dict[str, Dict[str, str]] = {
    "browser": {
        "Accept": "application/pdf,application/octet-stream,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-CH-UA": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Linux"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    }
}

DEFAULT_PROFILE_ORDER: List[str] = [
    "browser",
    "transparent",
    "python_requests",
    "wget",
    "curl",
]


def _validate_pdf_magic_bytes(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(8).startswith(b"%PDF-")
    except OSError:
        return False


def _compute_pdf_sha256_and_size(path: str) -> Tuple[Optional[str], Optional[int]]:
    sha256 = hashlib.sha256()
    size = 0
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                if not chunk:
                    break
                size += len(chunk)
                sha256.update(chunk)
    except OSError:
        return None, None
    return sha256.hexdigest(), size


def parse_ua_profiles(raw: Optional[str]) -> List[str]:
    if not raw:
        return list(DEFAULT_PROFILE_ORDER)
    ordered: List[str] = []
    for part in str(raw).split(","):
        key = part.strip()
        if not key:
            continue
        if key not in PROFILE_USER_AGENTS:
            continue
        if key not in ordered:
            ordered.append(key)
    if not ordered:
        return list(DEFAULT_PROFILE_ORDER)
    return ordered


@dataclass
class _RobotsEntry:
    parser: urllib.robotparser.RobotFileParser
    fetched_at: float


class RobotsCache:
    def __init__(self, ttl_seconds: int = 3600):
        self.ttl_seconds = max(int(ttl_seconds), 0)
        self._entries: Dict[str, _RobotsEntry] = {}
        self._lock = Lock()

    def _fetch(self, session: requests.Session, base_url: str, timeout: int) -> urllib.robotparser.RobotFileParser:
        parser = urllib.robotparser.RobotFileParser()
        robots_url = f"{base_url.rstrip('/')}/robots.txt"
        parser.set_url(robots_url)
        try:
            resp = session.get(robots_url, timeout=timeout)
            if resp.status_code >= 400:
                parser.parse([])
            else:
                parser.parse((resp.text or "").splitlines())
        except Exception:
            parser.parse([])
        return parser

    def is_allowed(
        self,
        session: requests.Session,
        url: str,
        user_agent: str,
        *,
        timeout: int = 10,
    ) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return True

        base_url = f"{parsed.scheme}://{parsed.netloc}"
        now = time.time()
        with self._lock:
            entry = self._entries.get(base_url)
            stale = (
                entry is None
                or (self.ttl_seconds > 0 and (now - entry.fetched_at) > self.ttl_seconds)
            )
            if stale:
                parser = self._fetch(session, base_url, timeout)
                entry = _RobotsEntry(parser=parser, fetched_at=now)
                self._entries[base_url] = entry

        agent = (user_agent or "").strip() or "*"
        try:
            return bool(entry.parser.can_fetch(agent, url))
        except Exception:
            return True


class SessionRotationManager:
    """Manages session rotation to prevent WAF session invalidation.
    
    Tracks request counts per host and proactively rotates sessions
    after a configurable threshold to avoid hitting WAF session limits.
    """
    
    def __init__(self, rotate_threshold: int = 300):
        self.rotate_threshold = max(int(rotate_threshold), 0)
        self._request_counts: Dict[str, int] = {}
        self._lock = Lock()
    
    def should_rotate(self, host: str) -> bool:
        """Check if session should be rotated for this host."""
        if self.rotate_threshold <= 0:
            return False
        with self._lock:
            count = self._request_counts.get(host, 0)
            return count >= self.rotate_threshold
    
    def record_request(self, host: str) -> int:
        """Record a request and return new count."""
        with self._lock:
            count = self._request_counts.get(host, 0) + 1
            self._request_counts[host] = count
            return count
    
    def reset_host(self, host: str) -> None:
        """Reset request count for a host after session rotation."""
        with self._lock:
            self._request_counts[host] = 0
    
    def get_count(self, host: str) -> int:
        """Get current request count for a host."""
        with self._lock:
            return self._request_counts.get(host, 0)


def _safe_filename_from_url(pdf_url: str) -> str:
    parsed = urlparse(pdf_url)
    filename = os.path.basename(unquote(parsed.path or "")).strip()
    if not filename:
        filename = "document.pdf"
    if not filename.lower().endswith(".pdf"):
        filename = f"{filename}.pdf"
    filename = filename.replace("/", "-").replace("\\", "-")
    filename = "".join(ch if ch.isalnum() or ch in {"-", "_", ".", " "} else "-" for ch in filename)
    filename = "-".join(part for part in filename.split() if part).strip("-")
    if not filename:
        filename = "document.pdf"
    return filename


def _ensure_unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    root, ext = os.path.splitext(path)
    idx = 2
    while True:
        candidate = f"{root}-{idx}{ext or '.pdf'}"
        if not os.path.exists(candidate):
            return candidate
        idx += 1


def _content_looks_like_waf(status_code: int, content_type: str, waf_action: str) -> bool:
    lowered_ct = (content_type or "").lower()
    lowered_waf = (waf_action or "").lower()
    if lowered_waf == "challenge":
        return True
    return status_code in {202, 401, 403} and "text/html" in lowered_ct


def _html_body_looks_like_waf(content_type: str, body: bytes) -> bool:
    lowered_ct = (content_type or "").lower()
    if "html" not in lowered_ct:
        return False
    snippet = (body or b"")[:4096].decode("utf-8", errors="ignore").lower()
    if not snippet:
        return False
    waf_markers = (
        "access denied",
        "forbidden",
        "challenge",
        "captcha",
        "cloudflare",
        "akamai",
        "incapsula",
        "perimeterx",
        "bot verification",
        "please enable javascript",
        "checking your browser",
        "ddos protection",
    )
    return any(marker in snippet for marker in waf_markers)


def _html_looks_like_subscription_gate(content_type: str, body: bytes, final_url: str) -> bool:
    lowered_ct = (content_type or "").lower()
    lowered_url = (final_url or "").lower()
    if "login.cgi" in lowered_url and "situation=subscription" in lowered_url:
        return True
    # Some Digital Commons journals redirect blocked PDF requests to login.cgi
    # without situation=subscription but with return_to/context/article params.
    if "login.cgi" in lowered_url and (
        "return_to=" in lowered_url or "context=" in lowered_url or "article=" in lowered_url
    ):
        return True
    if "html" not in lowered_ct:
        return False
    snippet = (body or b"")[:8192].decode("utf-8", errors="ignore").lower()
    if not snippet:
        return False
    markers = (
        "situation=subscription",
        "online access to this journal is restricted",
        "existing subscriber? log in",
        "new email address? please log in",
        "create new account",
        "you will need to create an account to complete your request",
        "forget your password",
        "remember me",
        "bepress guest access",
        "login.cgi?return_to=",
    )
    return any(marker in snippet for marker in markers)


def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
    header = resp.headers.get("Retry-After")
    if not header:
        return None
    try:
        seconds = float(header)
    except Exception:
        return None
    if seconds < 0:
        return None
    return seconds


def _retry_after_seconds_curl(headers: Dict[str, str]) -> Optional[float]:
    """Extract Retry-After from curl_cffi response headers."""
    header = headers.get("Retry-After") or headers.get("retry-after")
    if not header:
        return None
    try:
        seconds = float(header)
    except Exception:
        return None
    if seconds < 0:
        return None
    return seconds


def _download_with_curl_cffi(
    *,
    pdf_url: str,
    out_dir: str,
    referer: str = "",
    timeout: int = 30,
    min_domain_delay_ms: int = 2000,
    max_domain_delay_ms: int = 4000,
    max_attempts: int = 3,
) -> Optional[Dict[str, Any]]:
    """Attempt download using curl_cffi with Chrome TLS fingerprint impersonation.
    
    Returns None if curl_cffi is not available, otherwise returns the download result dict.
    This provides better WAF evasion by matching Chrome's TLS signature (JA3 fingerprint).
    """
    if not CURL_CFFI_AVAILABLE or curl_requests is None:
        return None
    
    min_delay = max(int(min_domain_delay_ms), 0) / 1000.0
    max_delay = max(int(max_domain_delay_ms), 0) / 1000.0
    if max_delay < min_delay:
        min_delay, max_delay = max_delay, min_delay
    
    timeout_s = max(int(timeout), 1)
    os.makedirs(out_dir, exist_ok=True)
    
    headers = dict(PROFILE_EXTRA_HEADERS.get("browser", {}))
    headers["User-Agent"] = PROFILE_USER_AGENTS["browser"]
    if referer:
        headers["Referer"] = referer
    
    last_error: Dict[str, Any] = {}
    
    for attempt in range(1, max(int(max_attempts), 1) + 1):
        if min_delay or max_delay:
            time.sleep(random.uniform(min_delay, max_delay))
        
        try:
            resp = curl_requests.get(
                pdf_url,
                headers=headers,
                timeout=timeout_s,
                impersonate=CURL_CFFI_IMPERSONATE,
                allow_redirects=True,
            )
        except Exception as exc:
            error_type = "timeout" if "timeout" in str(exc).lower() else "network"
            last_error = {
                "ok": False,
                "local_path": None,
                "error_type": error_type,
                "message": str(exc),
                "status_code": 0,
                "content_type": "",
                "waf_action": "",
                "ua_profile_used": "browser_curl_cffi",
                "robots_allowed": True,
                "download_status_class": "network",
                "blocked_reason": "",
                "retry_after_hint": None,
            }
            continue
        
        status_code = int(resp.status_code or 0)
        resp_headers = {k: v for k, v in resp.headers.items()}
        content_type = str(resp_headers.get("Content-Type") or resp_headers.get("content-type") or "")
        waf_action = str(resp_headers.get("x-amzn-waf-action") or resp_headers.get("X-Amzn-Waf-Action") or "")
        retry_after_hint = _retry_after_seconds_curl(resp_headers)
        final_url = str(getattr(resp, "url", "") or "")
        
        if status_code == 200 and resp.content:
            if _html_body_looks_like_waf(content_type, resp.content):
                last_error = {
                    "ok": False,
                    "local_path": None,
                    "error_type": "waf_challenge",
                    "message": "Blocked by WAF challenge (curl_cffi)",
                    "status_code": status_code,
                    "content_type": content_type,
                    "waf_action": waf_action,
                    "ua_profile_used": "browser_curl_cffi",
                    "robots_allowed": True,
                    "download_status_class": "blocked_waf",
                    "blocked_reason": "waf_challenge",
                    "retry_after_hint": retry_after_hint,
                }
                break  # WAF challenge - don't retry, fall back to requests
            if _html_looks_like_subscription_gate(content_type, resp.content, final_url):
                last_error = {
                    "ok": False,
                    "local_path": None,
                    "error_type": "subscription_blocked",
                    "message": "Subscription/login wall blocked PDF access (curl_cffi)",
                    "status_code": status_code,
                    "content_type": content_type,
                    "waf_action": waf_action,
                    "ua_profile_used": "browser_curl_cffi",
                    "robots_allowed": True,
                    "download_status_class": "blocked_subscription",
                    "blocked_reason": "subscription_login",
                    "retry_after_hint": retry_after_hint,
                }
                break
            
            filename = _safe_filename_from_url(pdf_url)
            output_path = _ensure_unique_path(os.path.join(out_dir, filename))
            try:
                with open(output_path, "wb") as f:
                    f.write(resp.content)
            except OSError as exc:
                last_error = {
                    "ok": False,
                    "local_path": None,
                    "error_type": "filesystem",
                    "message": str(exc),
                    "status_code": status_code,
                    "content_type": content_type,
                    "waf_action": waf_action,
                    "ua_profile_used": "browser_curl_cffi",
                    "robots_allowed": True,
                    "download_status_class": "network",
                    "blocked_reason": "",
                    "retry_after_hint": retry_after_hint,
                }
                break
            
            if _validate_pdf_magic_bytes(output_path):
                pdf_sha256, pdf_size_bytes = _compute_pdf_sha256_and_size(output_path)
                return {
                    "ok": True,
                    "local_path": output_path,
                    "pdf_sha256": pdf_sha256,
                    "pdf_size_bytes": pdf_size_bytes,
                    "status_code": status_code,
                    "content_type": content_type,
                    "waf_action": waf_action,
                    "ua_profile_used": "browser_curl_cffi",
                    "robots_allowed": True,
                    "download_status_class": "ok",
                    "blocked_reason": "",
                    "retry_after_hint": retry_after_hint,
                }
            
            try:
                os.remove(output_path)
            except OSError:
                pass
            last_error = {
                "ok": False,
                "local_path": None,
                "error_type": "invalid_pdf",
                "message": "Downloaded content failed PDF magic-byte validation (curl_cffi)",
                "status_code": status_code,
                "content_type": content_type,
                "waf_action": waf_action,
                "ua_profile_used": "browser_curl_cffi",
                "robots_allowed": True,
                "download_status_class": "invalid_pdf",
                "blocked_reason": "",
                "retry_after_hint": retry_after_hint,
            }
            break
        
        if status_code in {429, 500, 502, 503, 504}:
            last_error = {
                "ok": False,
                "local_path": None,
                "error_type": "http_error",
                "message": f"HTTP status {status_code} (curl_cffi)",
                "status_code": status_code,
                "content_type": content_type,
                "waf_action": waf_action,
                "ua_profile_used": "browser_curl_cffi",
                "robots_allowed": True,
                "download_status_class": "http_error",
                "blocked_reason": "",
                "retry_after_hint": retry_after_hint,
            }
            if attempt < max_attempts:
                sleep_for = retry_after_hint if retry_after_hint is not None else (
                    (2 ** (attempt - 1)) + random.uniform(0.0, 0.25)
                )
                time.sleep(max(sleep_for, 0.0))
                continue
            break
        
        if _content_looks_like_waf(status_code, content_type, waf_action):
            last_error = {
                "ok": False,
                "local_path": None,
                "error_type": "waf_challenge",
                "message": "Blocked by WAF challenge (curl_cffi)",
                "status_code": status_code,
                "content_type": content_type,
                "waf_action": waf_action,
                "ua_profile_used": "browser_curl_cffi",
                "robots_allowed": True,
                "download_status_class": "blocked_waf",
                "blocked_reason": "waf_challenge",
                "retry_after_hint": retry_after_hint,
            }
            break
        
        last_error = {
            "ok": False,
            "local_path": None,
            "error_type": "http_error",
            "message": f"HTTP status {status_code} (curl_cffi)",
            "status_code": status_code,
            "content_type": content_type,
            "waf_action": waf_action,
            "ua_profile_used": "browser_curl_cffi",
            "robots_allowed": True,
            "download_status_class": "http_error",
            "blocked_reason": "http_error",
            "retry_after_hint": retry_after_hint,
        }
        break
    
    return last_error if last_error else None


def download_pdf_dc(
    *,
    session: requests.Session,
    pdf_url: str,
    out_dir: str,
    referer: str = "",
    ua_profiles: Optional[Iterable[str]] = None,
    timeout: int = 30,
    min_domain_delay_ms: int = 2000,
    max_domain_delay_ms: int = 4000,
    robots_enforce: bool = True,
    robots_cache: Optional[RobotsCache] = None,
    max_attempts_per_profile: int = 3,
    use_curl_cffi: bool = True,
) -> Dict[str, Any]:
    """Download a PDF from Digital Commons with WAF resilience.
    
    Args:
        session: requests.Session for HTTP requests
        pdf_url: URL of the PDF to download
        out_dir: Output directory for the downloaded PDF
        referer: Referer header value (typically the page URL where PDF was discovered)
        ua_profiles: List of UA profile names to try in order
        timeout: Request timeout in seconds
        min_domain_delay_ms: Minimum delay between requests in milliseconds
        max_domain_delay_ms: Maximum delay between requests in milliseconds
        robots_enforce: Whether to enforce robots.txt
        robots_cache: Optional shared RobotsCache instance
        max_attempts_per_profile: Max retry attempts per UA profile
        use_curl_cffi: If True and curl_cffi is available, try it first for TLS fingerprint evasion
    
    Returns:
        Dict with download result including ok, local_path, error_type, etc.
    """
    profiles = [p for p in (ua_profiles or list(DEFAULT_PROFILE_ORDER)) if p in PROFILE_USER_AGENTS]
    if not profiles:
        profiles = list(DEFAULT_PROFILE_ORDER)

    # Try curl_cffi first if enabled and "browser" profile is in the list
    # curl_cffi provides Chrome TLS fingerprint impersonation for better WAF evasion
    if use_curl_cffi and CURL_CFFI_AVAILABLE and "browser" in profiles:
        # Check robots.txt first with browser UA
        cache = robots_cache or RobotsCache()
        browser_ua = PROFILE_USER_AGENTS["browser"]
        if not robots_enforce or cache.is_allowed(session, pdf_url, browser_ua, timeout=timeout):
            curl_result = _download_with_curl_cffi(
                pdf_url=pdf_url,
                out_dir=out_dir,
                referer=referer,
                timeout=timeout,
                min_domain_delay_ms=min_domain_delay_ms,
                max_domain_delay_ms=max_domain_delay_ms,
                max_attempts=max_attempts_per_profile,
            )
            if curl_result is not None:
                # If successful, return immediately
                if curl_result.get("ok"):
                    return curl_result
                # If WAF challenge, continue to fallback profiles (excluding browser since we tried curl_cffi)
                if curl_result.get("error_type") == "waf_challenge":
                    # Remove "browser" from profiles since curl_cffi already tried it with better TLS
                    profiles = [p for p in profiles if p != "browser"]
                    if not profiles:
                        # No fallback profiles, return the WAF error
                        return curl_result
                    # Store as last_error in case all fallbacks fail
                    last_error = curl_result
                else:
                    # For other errors (network, timeout), continue with fallback
                    pass

    cache = robots_cache or RobotsCache()
    timeout_s = max(int(timeout), 1)
    min_delay = max(int(min_domain_delay_ms), 0) / 1000.0
    max_delay = max(int(max_domain_delay_ms), 0) / 1000.0
    if max_delay < min_delay:
        min_delay, max_delay = max_delay, min_delay

    os.makedirs(out_dir, exist_ok=True)
    blocked_by_robots = True
    last_error: Dict[str, Any] = {}

    for profile in profiles:
        ua = PROFILE_USER_AGENTS[profile]
        allowed = True
        if robots_enforce:
            allowed = cache.is_allowed(session, pdf_url, ua, timeout=timeout_s)
            blocked_by_robots = blocked_by_robots and (not allowed)
        else:
            blocked_by_robots = False

        if not allowed:
            last_error = {
                "ok": False,
                "local_path": None,
                "error_type": "blocked_robots",
                "message": "Disallowed by robots.txt",
                "status_code": 0,
                "content_type": "",
                "waf_action": "",
                "ua_profile_used": profile,
                "robots_allowed": False,
                "download_status_class": "blocked_robots",
                "blocked_reason": "robots_disallow",
                "retry_after_hint": None,
            }
            continue

        blocked_by_robots = False
        for attempt in range(1, max(int(max_attempts_per_profile), 1) + 1):
            if min_delay or max_delay:
                time.sleep(random.uniform(min_delay, max_delay))
            headers = {"User-Agent": ua, "Accept": "application/pdf,*/*;q=0.9"}
            headers.update(PROFILE_EXTRA_HEADERS.get(profile, {}))
            if referer:
                headers["Referer"] = referer

            try:
                resp = session.get(pdf_url, headers=headers, timeout=timeout_s, allow_redirects=True)
            except requests.Timeout:
                last_error = {
                    "ok": False,
                    "local_path": None,
                    "error_type": "timeout",
                    "message": "Request timed out while downloading PDF",
                    "status_code": 0,
                    "content_type": "",
                    "waf_action": "",
                    "ua_profile_used": profile,
                    "robots_allowed": True,
                    "download_status_class": "network",
                    "blocked_reason": "",
                    "retry_after_hint": None,
                }
                continue
            except requests.RequestException as exc:
                last_error = {
                    "ok": False,
                    "local_path": None,
                    "error_type": "network",
                    "message": str(exc),
                    "status_code": 0,
                    "content_type": "",
                    "waf_action": "",
                    "ua_profile_used": profile,
                    "robots_allowed": True,
                    "download_status_class": "network",
                    "blocked_reason": "",
                    "retry_after_hint": None,
                }
                continue

            status_code = int(resp.status_code or 0)
            content_type = str(resp.headers.get("Content-Type") or "")
            waf_action = str(resp.headers.get("x-amzn-waf-action") or "")
            retry_after_hint = _retry_after_seconds(resp)
            final_url = str(getattr(resp, "url", "") or "")

            if status_code == 200 and resp.content:
                if _html_body_looks_like_waf(content_type, resp.content):
                    last_error = {
                        "ok": False,
                        "local_path": None,
                        "error_type": "waf_challenge",
                        "message": "Blocked by WAF challenge",
                        "status_code": status_code,
                        "content_type": content_type,
                        "waf_action": waf_action,
                        "ua_profile_used": profile,
                        "robots_allowed": True,
                        "download_status_class": "blocked_waf",
                        "blocked_reason": "waf_challenge",
                        "retry_after_hint": retry_after_hint,
                    }
                    break
                if _html_looks_like_subscription_gate(content_type, resp.content, final_url):
                    last_error = {
                        "ok": False,
                        "local_path": None,
                        "error_type": "subscription_blocked",
                        "message": "Subscription/login wall blocked PDF access",
                        "status_code": status_code,
                        "content_type": content_type,
                        "waf_action": waf_action,
                        "ua_profile_used": profile,
                        "robots_allowed": True,
                        "download_status_class": "blocked_subscription",
                        "blocked_reason": "subscription_login",
                        "retry_after_hint": retry_after_hint,
                    }
                    break
                filename = _safe_filename_from_url(pdf_url)
                output_path = _ensure_unique_path(os.path.join(out_dir, filename))
                try:
                    with open(output_path, "wb") as f:
                        f.write(resp.content)
                except OSError as exc:
                    last_error = {
                        "ok": False,
                        "local_path": None,
                        "error_type": "filesystem",
                        "message": str(exc),
                        "status_code": status_code,
                        "content_type": content_type,
                        "waf_action": waf_action,
                        "ua_profile_used": profile,
                        "robots_allowed": True,
                        "download_status_class": "network",
                        "blocked_reason": "",
                        "retry_after_hint": retry_after_hint,
                    }
                    break

                if _validate_pdf_magic_bytes(output_path):
                    pdf_sha256, pdf_size_bytes = _compute_pdf_sha256_and_size(output_path)
                    return {
                        "ok": True,
                        "local_path": output_path,
                        "pdf_sha256": pdf_sha256,
                        "pdf_size_bytes": pdf_size_bytes,
                        "status_code": status_code,
                        "content_type": content_type,
                        "waf_action": waf_action,
                        "ua_profile_used": profile,
                        "robots_allowed": True,
                        "download_status_class": "ok",
                        "blocked_reason": "",
                        "retry_after_hint": retry_after_hint,
                    }

                try:
                    os.remove(output_path)
                except OSError:
                    pass
                last_error = {
                    "ok": False,
                    "local_path": None,
                    "error_type": "invalid_pdf",
                    "message": "Downloaded content failed PDF magic-byte validation",
                    "status_code": status_code,
                    "content_type": content_type,
                    "waf_action": waf_action,
                    "ua_profile_used": profile,
                    "robots_allowed": True,
                    "download_status_class": "invalid_pdf",
                    "blocked_reason": "",
                    "retry_after_hint": retry_after_hint,
                }
                break

            if status_code in {429, 500, 502, 503, 504}:
                last_error = {
                    "ok": False,
                    "local_path": None,
                    "error_type": "http_error",
                    "message": f"HTTP status {status_code}",
                    "status_code": status_code,
                    "content_type": content_type,
                    "waf_action": waf_action,
                    "ua_profile_used": profile,
                    "robots_allowed": True,
                    "download_status_class": "http_error",
                    "blocked_reason": "",
                    "retry_after_hint": retry_after_hint,
                }
                if attempt < max_attempts_per_profile:
                    sleep_for = retry_after_hint if retry_after_hint is not None else (
                        (2 ** (attempt - 1)) + random.uniform(0.0, 0.25)
                    )
                    time.sleep(max(sleep_for, 0.0))
                    continue
                break

            if _content_looks_like_waf(status_code, content_type, waf_action):
                last_error = {
                    "ok": False,
                    "local_path": None,
                    "error_type": "waf_challenge",
                    "message": "Blocked by WAF challenge",
                    "status_code": status_code,
                    "content_type": content_type,
                    "waf_action": waf_action,
                    "ua_profile_used": profile,
                    "robots_allowed": True,
                    "download_status_class": "blocked_waf",
                    "blocked_reason": "waf_challenge",
                    "retry_after_hint": retry_after_hint,
                }
                break

            last_error = {
                "ok": False,
                "local_path": None,
                "error_type": "http_error",
                "message": f"HTTP status {status_code}",
                "status_code": status_code,
                "content_type": content_type,
                "waf_action": waf_action,
                "ua_profile_used": profile,
                "robots_allowed": True,
                "download_status_class": "http_error",
                "blocked_reason": "http_error",
                "retry_after_hint": retry_after_hint,
            }
            break

    if blocked_by_robots:
        return {
            "ok": False,
            "local_path": None,
            "error_type": "blocked_robots",
            "message": "Disallowed by robots.txt for all configured UA profiles",
            "status_code": 0,
            "content_type": "",
            "waf_action": "",
            "ua_profile_used": "",
            "robots_allowed": False,
            "download_status_class": "blocked_robots",
            "blocked_reason": "robots_disallow",
            "retry_after_hint": None,
        }

    if not last_error:
        last_error = {
            "ok": False,
            "local_path": None,
            "error_type": "download_failed",
            "message": "PDF download failed",
            "status_code": 0,
            "content_type": "",
            "waf_action": "",
            "ua_profile_used": "",
            "robots_allowed": None,
            "download_status_class": "network",
            "blocked_reason": "",
            "retry_after_hint": None,
        }
    return last_error
