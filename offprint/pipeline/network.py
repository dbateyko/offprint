import os
import random
import time
from typing import Any, Dict

from ..adapters.utils import compute_pdf_sha256_and_size, validate_pdf_magic_bytes
from .io import _append_jsonl, _utc_now_iso

def _response_meta(session: Any, url: str) -> Dict[str, Any]:
    getter = getattr(session, "get_response_meta", None)
    if callable(getter):
        value = getter(url)
        if isinstance(value, dict):
            return value
    return {}

def _classify_error(download_meta: Dict[str, Any]) -> tuple[str, str, int]:
    error_type = str(download_meta.get("error_type") or "download_failed")
    message = str(download_meta.get("message") or "PDF download failed")
    status_code = int(download_meta.get("status_code") or 0)
    if error_type == "http_error" and status_code >= 400:
        if status_code in {401, 403, 408, 429}:
            return str(status_code), message, status_code
        family = f"{status_code // 100}xx"
        return family, message, status_code
    return error_type, message, status_code

def _is_retryable(error_type: str, http_status: int) -> bool:
    if error_type in {
        "403",
        "blocked_waf",
        "filesystem",
        "precheck_failed",
        "blocked_robots",
        "subscription_blocked",
        "waf_challenge",
        "waf_circuit_open",
    }:
        return False
    if error_type == "invalid_pdf":
        return False
    if error_type in {"408", "429"}:
        return True
    if error_type in {"timeout", "network", "5xx", "4xx"}:
        return True
    if error_type == "http_error" and http_status in {0, 200, 202}:
        return True
    if http_status >= 500:
        return True
    return error_type in {"download_failed", "unknown", "http_error"}

def _download_with_retries(
    *,
    adapter: Any,
    pdf_url: str,
    out_dir: str,
    errors_path: str,
    error_context: Dict[str, Any],
    max_attempts: int = 3,
) -> Dict[str, Any]:
    referer = str(error_context.get("page_url") or "")
    seed_url = str(error_context.get("seed_url") or "")
    for attempt in range(1, max_attempts + 1):
        if referer or seed_url:
            try:
                local_path = adapter.download_pdf(
                    pdf_url,
                    out_dir=out_dir,
                    referer=referer,
                    seed_url=seed_url,
                )
            except TypeError as exc:
                if "unexpected keyword argument" not in str(exc):
                    raise
                try:
                    local_path = adapter.download_pdf(pdf_url, out_dir=out_dir, referer=referer)
                except TypeError as exc2:
                    if "unexpected keyword argument" not in str(exc2):
                        raise
                    local_path = adapter.download_pdf(pdf_url, out_dir=out_dir)
        else:
            local_path = adapter.download_pdf(pdf_url, out_dir=out_dir)
        download_meta = dict(getattr(adapter, "last_download_meta", {}) or {})

        if local_path and os.path.exists(local_path):
            if not validate_pdf_magic_bytes(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass
                download_meta = {
                    "error_type": "invalid_pdf",
                    "message": "Downloaded content failed PDF magic-byte validation",
                    "status_code": 200,
                }
            else:
                pdf_sha256 = download_meta.get("pdf_sha256")
                pdf_size_bytes = download_meta.get("pdf_size_bytes")
                if not pdf_sha256 or pdf_size_bytes is None:
                    pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(local_path)
                return {
                    "ok": True,
                    "local_path": local_path,
                    "pdf_sha256": pdf_sha256,
                    "pdf_size_bytes": pdf_size_bytes,
                    "content_type": str(download_meta.get("content_type") or "application/pdf"),
                    "http_status": int(download_meta.get("status_code") or 200),
                    "ua_profile_used": str(download_meta.get("ua_profile_used") or ""),
                    "robots_allowed": download_meta.get("robots_allowed"),
                    "download_status_class": str(
                        download_meta.get("download_status_class") or "ok"
                    ),
                    "blocked_reason": str(download_meta.get("blocked_reason") or ""),
                    "retry_after_hint": download_meta.get("retry_after_hint"),
                    "retries": attempt - 1,
                }

        error_type, message, status_code = _classify_error(download_meta)
        ua_profile_used = str(download_meta.get("ua_profile_used") or "")
        robots_allowed = download_meta.get("robots_allowed")
        blocked_reason = str(download_meta.get("blocked_reason") or "")
        retry_after_hint = download_meta.get("retry_after_hint")
        dc_source = str(error_context.get("dc_source") or "")
        blocked_class = "unknown"
        if error_type in {"waf_challenge", "waf_circuit_open", "blocked_waf"}:
            blocked_class = "waf"
        elif error_type == "blocked_robots":
            blocked_class = "robots"
        elif error_type == "subscription_blocked":
            blocked_class = "subscription"
        elif error_type in {"401", "403"}:
            blocked_class = "auth"
        error_record = {
            **error_context,
            "error_type": error_type,
            "message": message,
            "http_status": status_code,
            "attempt": attempt,
            "retries": attempt,
            "ua_profile_used": ua_profile_used,
            "robots_allowed": robots_allowed,
            "dc_source": dc_source,
            "retry_after_hint": retry_after_hint,
            "blocked_class": blocked_class,
            "blocked_reason": blocked_reason,
            "retrieved_at": _utc_now_iso(),
        }
        _append_jsonl(errors_path, error_record)

        if attempt < max_attempts and _is_retryable(error_type, status_code):
            sleep_s = (2 ** (attempt - 1)) + random.uniform(0.0, 0.25)
            time.sleep(sleep_s)
            continue

        return {
            "ok": False,
            "local_path": None,
            "pdf_sha256": None,
            "pdf_size_bytes": None,
            "content_type": str(download_meta.get("content_type") or ""),
            "http_status": status_code,
            "error_type": error_type,
            "error_message": message,
            "ua_profile_used": ua_profile_used,
            "robots_allowed": robots_allowed,
            "download_status_class": str(download_meta.get("download_status_class") or "network"),
            "blocked_reason": blocked_reason,
            "retry_after_hint": retry_after_hint,
            "retries": attempt,
        }

    return {
        "ok": False,
        "local_path": None,
        "pdf_sha256": None,
        "pdf_size_bytes": None,
        "content_type": "",
        "http_status": 0,
        "error_type": "download_failed",
        "error_message": "PDF download failed",
        "ua_profile_used": "",
        "robots_allowed": None,
        "download_status_class": "network",
        "blocked_reason": "",
        "retry_after_hint": None,
        "retries": max_attempts,
    }
