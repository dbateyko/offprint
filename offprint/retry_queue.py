from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.parse import urlparse

from .adapters import UnmappedAdapterError, pick_adapter_for
from .adapters.utils import compute_pdf_sha256_and_size, validate_pdf_magic_bytes
from .legacy_manifest import should_write_legacy_manifests
from .mcp_browser_download import (
    create_browser_downloader,
    load_waf_retry_file,
    BrowserDownloadResult,
    CAMOUFOX_AVAILABLE,
)
from .orchestrator import _append_jsonl, _classify_error, _is_retryable, _utc_now_iso
from .polite_requests import PoliteRequestsSession


def _iter_jsonl(path: str) -> Iterator[Dict[str, Any]]:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                yield payload


def _latest_retryable_errors(
    errors: Iterable[Dict[str, Any]], max_retries: int, include_waf: bool = False
) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for err in errors:
        pdf_url = str(err.get("pdf_url") or "").strip()
        if not pdf_url:
            continue
        error_type = str(err.get("error_type") or "")
        retries = int(err.get("retries") or 0)
        if retries >= max_retries:
            continue
        is_waf_error = error_type in {"waf_challenge", "waf_circuit_open", "blocked_waf"}
        if "waf" in error_type:
            is_waf_error = True
        if is_waf_error:
            if not include_waf:
                continue
        else:
            if not _is_retryable(error_type, int(err.get("http_status") or 0)):
                continue
            if error_type in {"403", "completeness_warning", "seed_failure"}:
                continue
        latest[pdf_url] = err
    return latest


def _collect_waf_errors(
    errors: Iterable[Dict[str, Any]], max_retries: int
) -> List[Dict[str, Any]]:
    """Collect WAF-blocked errors for browser fallback processing.
    
    Args:
        errors: Iterable of error records
        max_retries: Maximum retry count to include
        
    Returns:
        List of WAF error dicts suitable for browser fallback
    """
    waf_errors: List[Dict[str, Any]] = []
    seen_urls: set = set()
    
    for err in errors:
        pdf_url = str(err.get("pdf_url") or "").strip()
        if not pdf_url or pdf_url in seen_urls:
            continue
        
        error_type = str(err.get("error_type") or "")
        retries = int(err.get("retries") or 0)
        
        if retries >= max_retries:
            continue
        
        # Check if this is a WAF error
        is_waf_error = error_type in {"waf_challenge", "waf_circuit_open", "blocked_waf"}
        if "waf" in error_type.lower():
            is_waf_error = True
        
        if is_waf_error:
            seen_urls.add(pdf_url)
            waf_errors.append({
                "url": pdf_url,
                "domain": str(err.get("domain") or urlparse(pdf_url).netloc or "unknown"),
                "metadata": dict(err.get("metadata") or {}),
                "referer": str(err.get("page_url") or err.get("seed_url") or ""),
                "seed_url": str(err.get("seed_url") or err.get("page_url") or pdf_url),
                "page_url": str(err.get("page_url") or err.get("seed_url") or pdf_url),
                "source_adapter": str(err.get("source_adapter") or ""),
                "extraction_path": str(err.get("extraction_path") or "browser_fallback"),
                "retries": retries,
            })
    
    return waf_errors


def retry_waf_browser(
    run_dir: str,
    max_retries: int = 3,
    browser_backend: str = "auto",
    staging_dir: str = "artifacts/browser_staging",
    user_data_dir: str = "artifacts/browser_profile",
    timeout_seconds: int = 60,
    headless: bool = False,
    waf_retry_file: Optional[str] = None,
) -> Dict[str, Any]:
    """Retry WAF-blocked downloads using browser fallback.
    
    This function processes WAF-blocked errors from a run directory using
    a real browser (Camoufox or Chrome via MCP) to bypass WAF challenges.
    
    Args:
        run_dir: Path to run artifact directory
        max_retries: Maximum number of retries to consider
        browser_backend: Browser backend ("auto", "camoufox", "chrome_mcp")
        staging_dir: Directory for browser download staging
        user_data_dir: Browser profile directory for session persistence
        timeout_seconds: Download timeout per URL
        headless: Run browser in headless mode
        waf_retry_file: Optional path to specific WAF retry file. If None,
            collects WAF errors from errors.jsonl
            
    Returns:
        Summary dict with attempted/recovered/failed counts
    """
    manifest_path = os.path.join(run_dir, "manifest.json")
    records_path = os.path.join(run_dir, "records.jsonl")
    errors_path = os.path.join(run_dir, "errors.jsonl")
    
    if not os.path.exists(manifest_path):
        raise ValueError(f"Missing run manifest: {manifest_path}")
    
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    
    config = manifest.get("config") or {}
    out_dir = str(config.get("out_dir") or "pdfs")
    write_legacy_manifests = should_write_legacy_manifests(
        config.get("write_legacy_manifests")
    )
    
    # Collect WAF-blocked URLs
    if waf_retry_file and os.path.exists(waf_retry_file):
        waf_targets = load_waf_retry_file(waf_retry_file)
        print(f"📄 Loaded {len(waf_targets)} WAF targets from {waf_retry_file}", flush=True)
    else:
        waf_targets = _collect_waf_errors(
            _iter_jsonl(errors_path), max_retries=max_retries
        )
        # Also check for existing WAF retry file
        default_waf_file = os.path.join(run_dir, "retry_targets_waf_browser.txt")
        if os.path.exists(default_waf_file):
            file_targets = load_waf_retry_file(default_waf_file)
            # Merge, avoiding duplicates
            existing_urls = {t["url"] for t in waf_targets}
            for t in file_targets:
                if t.get("url") not in existing_urls:
                    waf_targets.append(t)
        print(f"🔍 Found {len(waf_targets)} WAF-blocked URLs to retry", flush=True)
    
    if not waf_targets:
        print("✅ No WAF-blocked URLs to retry", flush=True)
        return {"attempted": 0, "recovered": 0, "failed": 0}
    
    # Create browser downloader
    print(f"🌐 Starting browser fallback (backend={browser_backend})...", flush=True)
    
    downloader = create_browser_downloader(
        backend=browser_backend,  # type: ignore
        staging_dir=staging_dir,
        user_data_dir=user_data_dir,
        final_out_dir=out_dir,
        timeout_seconds=timeout_seconds,
        headless=headless,
    )
    
    recovered = 0
    failed = 0
    
    try:
        for idx, target in enumerate(waf_targets):
            url = target["url"]
            domain = target.get("domain") or urlparse(url).netloc or "unknown"
            metadata = target.get("metadata") or {}
            referer = target.get("referer", "")
            seed_url = target.get("seed_url", url)
            page_url = target.get("page_url", url)
            source_adapter = target.get("source_adapter", "browser_fallback")
            extraction_path = target.get("extraction_path", "browser_fallback")
            retries = int(target.get("retries") or 0)
            
            print(f"  [{idx + 1}/{len(waf_targets)}] Downloading: {url[:80]}...", flush=True)
            
            # Add URL and process immediately
            downloader.add_url(url, domain=domain, metadata=metadata, referer=referer)
            
            def progress_cb(current, total, url):
                pass  # Already printed above
            
            results = downloader.process_batch(progress_callback=progress_cb)
            
            if not results:
                failed += 1
                _append_jsonl(
                    errors_path,
                    {
                        "seed_url": seed_url,
                        "domain": domain,
                        "page_url": page_url,
                        "pdf_url": url,
                        "source_adapter": source_adapter,
                        "extraction_path": extraction_path,
                        "metadata": metadata,
                        "error_type": "browser_no_result",
                        "message": "Browser fallback returned no result",
                        "http_status": 0,
                        "attempt": retries + 1,
                        "retries": retries + 1,
                        "retrieved_at": _utc_now_iso(),
                    },
                )
                continue
            
            result = results[0]
            
            if result.ok and result.local_path and os.path.exists(result.local_path):
                # Validate PDF
                if validate_pdf_magic_bytes(result.local_path):
                    pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(result.local_path)
                    
                    metadata["pdf_relative_path"] = os.path.relpath(result.local_path, out_dir)
                    metadata["pdf_filename"] = os.path.basename(result.local_path)
                    metadata["browser_fallback"] = True
                    metadata["download_time_seconds"] = result.download_time_seconds
                    
                    record = {
                        "seed_url": seed_url,
                        "domain": domain,
                        "page_url": page_url,
                        "pdf_url": url,
                        "local_path": result.local_path,
                        "ok": True,
                        "metadata": metadata,
                        "source_adapter": source_adapter,
                        "extraction_path": extraction_path,
                        "retrieved_at": _utc_now_iso(),
                        "http_status": 200,
                        "content_type": "application/pdf",
                        "pdf_sha256": pdf_sha256,
                        "pdf_size_bytes": pdf_size_bytes,
                        "retries": retries + 1,
                        "error_type": "",
                        "error_message": "",
                        "browser_fallback": True,
                    }
                    _append_jsonl(records_path, record)
                    
                    if write_legacy_manifests:
                        legacy_manifest_path = os.path.join(
                            os.path.dirname(run_dir), f"{domain.replace(':', '_')}.jsonl"
                        )
                        _append_jsonl(
                            legacy_manifest_path,
                            {
                                "page_url": page_url,
                                "pdf_url": url,
                                "local_path": result.local_path,
                                "ok": True,
                                "metadata": metadata,
                            },
                        )
                    
                    print(f"    ✅ Recovered: {os.path.basename(result.local_path)}", flush=True)
                    recovered += 1
                    continue
                else:
                    # Invalid PDF
                    try:
                        os.remove(result.local_path)
                    except OSError:
                        pass
                    result = BrowserDownloadResult(
                        url=url,
                        ok=False,
                        local_path=None,
                        error_type="invalid_pdf",
                        message="Downloaded content failed PDF magic-byte validation",
                        download_time_seconds=result.download_time_seconds,
                    )
            
            # Failed download
            _append_jsonl(
                errors_path,
                {
                    "seed_url": seed_url,
                    "domain": domain,
                    "page_url": page_url,
                    "pdf_url": url,
                    "source_adapter": source_adapter,
                    "extraction_path": extraction_path,
                    "metadata": metadata,
                    "error_type": result.error_type or "browser_download_failed",
                    "message": result.message or "Browser download failed",
                    "http_status": 0,
                    "attempt": retries + 1,
                    "retries": retries + 1,
                    "retrieved_at": _utc_now_iso(),
                    "browser_fallback": True,
                },
            )
            print(f"    ❌ Failed: {result.error_type}: {result.message}", flush=True)
            failed += 1
    
    finally:
        downloader.close()
    
    print(f"🏁 Browser fallback complete: {recovered} recovered, {failed} failed", flush=True)
    return {"attempted": len(waf_targets), "recovered": recovered, "failed": failed}


def retry_failed(run_dir: str, max_retries: int = 3, include_waf: bool = False) -> Dict[str, Any]:
    manifest_path = os.path.join(run_dir, "manifest.json")
    records_path = os.path.join(run_dir, "records.jsonl")
    errors_path = os.path.join(run_dir, "errors.jsonl")

    if not os.path.exists(manifest_path):
        raise ValueError(f"Missing run manifest: {manifest_path}")

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    config = manifest.get("config") or {}
    out_dir = str(config.get("out_dir") or "pdfs")
    write_legacy_manifests = should_write_legacy_manifests(
        config.get("write_legacy_manifests")
    )
    min_delay = float(config.get("min_delay") or 1.0)
    max_delay = float(config.get("max_delay") or 3.0)

    candidates = _latest_retryable_errors(
        _iter_jsonl(errors_path), max_retries=max_retries, include_waf=include_waf
    )
    if not candidates:
        return {"attempted": 0, "recovered": 0, "failed": 0}

    recovered = 0
    failed = 0

    with PoliteRequestsSession(min_delay=min_delay, max_delay=max_delay) as session:
        for pdf_url, err in candidates.items():
            seed_url = str(err.get("seed_url") or err.get("page_url") or pdf_url)
            page_url = str(err.get("page_url") or seed_url)
            domain = str(err.get("domain") or urlparse(seed_url).netloc or "unknown")
            retries = int(err.get("retries") or 0)
            metadata = dict(err.get("metadata") or {})
            source_adapter = str(err.get("source_adapter") or "")
            extraction_path = str(err.get("extraction_path") or "retry_queue")

            try:
                adapter = pick_adapter_for(seed_url, session=session, allow_generic=False)
            except UnmappedAdapterError as exc:
                _append_jsonl(
                    errors_path,
                    {
                        "seed_url": seed_url,
                        "domain": domain,
                        "page_url": page_url,
                        "pdf_url": pdf_url,
                        "source_adapter": source_adapter or "retry_queue",
                        "extraction_path": extraction_path,
                        "metadata": metadata,
                        "error_type": "todo_adapter_blocked",
                        "message": str(exc),
                        "http_status": 0,
                        "attempt": retries + 1,
                        "retries": retries + 1,
                        "blocked_host": exc.host,
                        "retrieved_at": _utc_now_iso(),
                    },
                )
                failed += 1
                continue
            if not source_adapter:
                source_adapter = adapter.__class__.__name__

            local_path = adapter.download_pdf(pdf_url, os.path.join(out_dir, domain))
            download_meta = dict(getattr(adapter, "last_download_meta", {}) or {})

            ok = False
            pdf_sha256 = None
            pdf_size_bytes = None
            content_type = str(download_meta.get("content_type") or "")
            http_status = int(download_meta.get("status_code") or 0)

            if local_path and os.path.exists(local_path):
                if validate_pdf_magic_bytes(local_path):
                    ok = True
                    pdf_sha256, pdf_size_bytes = compute_pdf_sha256_and_size(local_path)
                else:
                    try:
                        os.remove(local_path)
                    except OSError:
                        pass
                    download_meta = {
                        "error_type": "invalid_pdf",
                        "message": "Downloaded content failed PDF magic-byte validation",
                        "status_code": 200,
                    }

            if ok:
                metadata["pdf_relative_path"] = os.path.relpath(local_path, out_dir)
                metadata["pdf_filename"] = os.path.basename(local_path)
                record = {
                    "seed_url": seed_url,
                    "domain": domain,
                    "page_url": page_url,
                    "pdf_url": pdf_url,
                    "local_path": local_path,
                    "ok": True,
                    "metadata": metadata,
                    "source_adapter": source_adapter,
                    "extraction_path": extraction_path,
                    "retrieved_at": _utc_now_iso(),
                    "http_status": http_status,
                    "content_type": content_type,
                    "pdf_sha256": pdf_sha256,
                    "pdf_size_bytes": pdf_size_bytes,
                    "retries": retries + 1,
                    "error_type": "",
                    "error_message": "",
                }
                _append_jsonl(records_path, record)

                if write_legacy_manifests:
                    legacy_manifest_path = os.path.join(
                        os.path.dirname(run_dir), f"{domain.replace(':', '_')}.jsonl"
                    )
                    _append_jsonl(
                        legacy_manifest_path,
                        {
                            "page_url": page_url,
                            "pdf_url": pdf_url,
                            "local_path": local_path,
                            "ok": True,
                            "metadata": metadata,
                        },
                    )
                recovered += 1
                continue

            error_type, message, status_code = _classify_error(download_meta)
            _append_jsonl(
                errors_path,
                {
                    "seed_url": seed_url,
                    "domain": domain,
                    "page_url": page_url,
                    "pdf_url": pdf_url,
                    "source_adapter": source_adapter,
                    "extraction_path": extraction_path,
                    "metadata": metadata,
                    "error_type": error_type,
                    "message": message,
                    "http_status": status_code,
                    "attempt": retries + 1,
                    "retries": retries + 1,
                    "retrieved_at": _utc_now_iso(),
                },
            )
            failed += 1

    return {"attempted": len(candidates), "recovered": recovered, "failed": failed}


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry failed downloads for a run directory")
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to artifact run directory (for example artifacts/runs/<run_id>)",
    )
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument(
        "--include-waf",
        action="store_true",
        help="Include WAF-blocked errors in standard HTTP retry",
    )
    
    # Browser fallback options
    parser.add_argument(
        "--browser-fallback",
        action="store_true",
        help="Use browser fallback for WAF-blocked downloads (instead of standard HTTP retry)",
    )
    parser.add_argument(
        "--browser-backend",
        choices=["auto", "camoufox", "chrome_mcp"],
        default="auto",
        help="Browser backend: auto (prefer camoufox), camoufox, or chrome_mcp",
    )
    parser.add_argument(
        "--browser-staging-dir",
        default="artifacts/browser_staging",
        help="Directory for browser download staging",
    )
    parser.add_argument(
        "--browser-user-data-dir",
        default="artifacts/browser_profile",
        help="Browser profile directory for session persistence",
    )
    parser.add_argument(
        "--browser-timeout",
        type=int,
        default=60,
        help="Download timeout per URL in seconds",
    )
    parser.add_argument(
        "--browser-headless",
        action="store_true",
        help="Run browser in headless mode",
    )
    parser.add_argument(
        "--waf-retry-file",
        help="Path to specific WAF retry file (default: auto-detect from run dir)",
    )
    
    args = parser.parse_args()

    if args.browser_fallback:
        # Use browser fallback for WAF-blocked URLs
        if not CAMOUFOX_AVAILABLE and args.browser_backend in ("auto", "camoufox"):
            print("⚠️ Camoufox not available. Install with: pip install camoufox[geoip]", flush=True)
            print("   Falling back to chrome_mcp backend", flush=True)
            args.browser_backend = "chrome_mcp"
        
        summary = retry_waf_browser(
            run_dir=args.run_dir,
            max_retries=args.max_retries,
            browser_backend=args.browser_backend,
            staging_dir=args.browser_staging_dir,
            user_data_dir=args.browser_user_data_dir,
            timeout_seconds=args.browser_timeout,
            headless=args.browser_headless,
            waf_retry_file=args.waf_retry_file,
        )
    else:
        # Standard HTTP retry
        summary = retry_failed(
            args.run_dir, max_retries=args.max_retries, include_waf=args.include_waf
        )
    
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
