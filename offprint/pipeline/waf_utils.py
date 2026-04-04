import json
import os
from typing import Any, Dict, List, Optional

from .io import _append_jsonl, _utc_now_iso

def _count_waf_stats(errors_path: str) -> Dict[str, Any]:
    stats = {
        "waf_blocked": 0,
        "waf_challenge": 0,
        "waf_circuit_open": 0,
        "blocked_waf": 0,
        "domains_affected": set(),
    }
    seen_urls: set[str] = set()

    if not os.path.exists(errors_path):
        stats["domains_affected"] = []
        return stats

    try:
        with open(errors_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                error_type = str(record.get("error_type") or "")
                blocked_class = str(record.get("blocked_class") or "")
                pdf_url = record.get("pdf_url") or ""
                domain = record.get("domain") or ""

                is_waf = False
                if error_type == "waf_challenge":
                    stats["waf_challenge"] += 1
                    is_waf = True
                elif error_type == "waf_circuit_open":
                    stats["waf_circuit_open"] += 1
                    is_waf = True
                elif error_type == "blocked_waf" or blocked_class == "waf":
                    stats["blocked_waf"] += 1
                    is_waf = True
                elif "waf" in error_type.lower():
                    is_waf = True

                if is_waf and pdf_url and pdf_url not in seen_urls:
                    seen_urls.add(pdf_url)
                    stats["waf_blocked"] += 1
                    if domain:
                        stats["domains_affected"].add(domain)
    except Exception:
        pass

    stats["domains_affected"] = sorted(stats["domains_affected"])
    return stats


def _count_curl_cffi_downloads(records_path: str) -> int:
    count = 0

    if not os.path.exists(records_path):
        return count

    try:
        with open(records_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if record.get("ok") and record.get("ua_profile_used") == "browser_curl_cffi":
                    count += 1
    except Exception:
        pass

    return count


def _collect_waf_blocked_urls(
    errors_path: str,
    *,
    seed_url: Optional[str] = None,
    domain: Optional[str] = None,
    exclude_urls: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    waf_blocked: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    excluded = set(exclude_urls or set())

    if not os.path.exists(errors_path):
        return waf_blocked

    try:
        with open(errors_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                error_type = str(record.get("error_type") or "")
                blocked_class = str(record.get("blocked_class") or "")

                record_seed = str(record.get("seed_url") or "")
                record_domain = str(record.get("domain") or "")
                if seed_url and record_seed != seed_url:
                    continue
                if domain and record_domain != domain:
                    continue

                if error_type in {"waf_challenge", "waf_circuit_open"} or blocked_class == "waf":
                    pdf_url = record.get("pdf_url")
                    if pdf_url and pdf_url not in seen_urls and pdf_url not in excluded:
                        seen_urls.add(pdf_url)
                        waf_blocked.append(
                            {
                                "url": pdf_url,
                                "domain": record.get("domain") or "",
                                "metadata": record.get("metadata") or {},
                                "referer": record.get("page_url") or "",
                                "seed_url": record.get("seed_url") or "",
                                "source_adapter": record.get("source_adapter") or "",
                                "extraction_path": record.get("extraction_path") or "",
                            }
                        )
    except Exception as exc:
        print(f"⚠️ Error reading WAF-blocked URLs: {exc}", flush=True)

    return waf_blocked


def _process_waf_browser_fallback(
    *,
    waf_blocked_urls: List[Dict[str, Any]],
    out_dir: str,
    run_dir: str,
    records_path: str,
    errors_path: str,
    backend: str,
    staging_dir: str,
    user_data_dir: str,
    timeout: int,
    headless: bool,
    operator_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from ..mcp_browser_download import (
        create_browser_downloader,
        create_waf_retry_file,
    )

    result = {
        "succeeded": 0,
        "failed": 0,
        "skipped": 0,
        "operator_prompted": 0,
        "operator_retried": 0,
        "operator_recovered": 0,
        "operator_unresolved": 0,
    }

    if not waf_blocked_urls:
        return result

    print(
        f"\n🌐 Browser fallback: processing {len(waf_blocked_urls)} WAF-blocked URLs...", flush=True
    )

    retry_file = create_waf_retry_file(run_dir, waf_blocked_urls)
    print(f"📝 WAF retry targets saved to: {retry_file}", flush=True)

    try:
        downloader = create_browser_downloader(
            backend=backend,
            staging_dir=staging_dir,
            user_data_dir=user_data_dir,
            final_out_dir=out_dir,
            timeout_seconds=timeout,
            headless=headless,
            operator_config=operator_config,
        )

        for item in waf_blocked_urls:
            downloader.add_url(
                item["url"],
                domain=item.get("domain", ""),
                metadata=item.get("metadata"),
                referer=item.get("referer", ""),
            )

        def progress_cb(current: int, total: int, url: str) -> None:
            print(f"  [{current}/{total}] {url[:80]}...", flush=True)

        results = downloader.process_batch(progress_callback=progress_cb)

        for idx, browser_result in enumerate(results):
            item = waf_blocked_urls[idx] if idx < len(waf_blocked_urls) else {}
            prompted = int(getattr(browser_result, "operator_intervention_prompted", 0) or 0)
            retried = int(getattr(browser_result, "operator_intervention_retries", 0) or 0)
            recovered = bool(getattr(browser_result, "operator_intervention_recovered", False))
            unresolved = bool(getattr(browser_result, "operator_intervention_unresolved", False))
            result["operator_prompted"] += prompted
            result["operator_retried"] += retried
            if recovered:
                result["operator_recovered"] += 1
            if unresolved:
                result["operator_unresolved"] += 1

            if browser_result.ok and browser_result.local_path:
                result["succeeded"] += 1

                source_adapter = str(item.get("source_adapter") or "browser_fallback")
                extraction_path = str(item.get("extraction_path") or "")
                if extraction_path:
                    extraction_path = f"{extraction_path}|browser_fallback/{backend}"
                else:
                    extraction_path = f"browser_fallback/{backend}"
                success_record = {
                    "seed_url": item.get("seed_url", ""),
                    "domain": item.get("domain", ""),
                    "page_url": item.get("referer", ""),
                    "pdf_url": browser_result.url,
                    "local_path": browser_result.local_path,
                    "ok": True,
                    "metadata": item.get("metadata", {}),
                    "source_adapter": source_adapter,
                    "extraction_path": extraction_path,
                    "retrieved_at": _utc_now_iso(),
                    "http_status": 200,
                    "content_type": "application/pdf",
                    "pdf_sha256": None, 
                    "pdf_size_bytes": None,
                    "retries": 0,
                    "error_type": "",
                    "error_message": "",
                    "dc_source": "browser_fallback",
                    "dc_set_spec": "",
                    "ua_profile_used": f"browser_fallback_{backend}",
                    "robots_allowed": True,
                    "download_status_class": "ok",
                    "blocked_reason": "",
                    "browser_download_time_seconds": browser_result.download_time_seconds,
                }
                _append_jsonl(records_path, success_record)
                print(f"    ✅ Downloaded: {browser_result.local_path}", flush=True)
            else:
                result["failed"] += 1

                source_adapter = str(item.get("source_adapter") or "browser_fallback")
                extraction_path = str(item.get("extraction_path") or "")
                if extraction_path:
                    extraction_path = f"{extraction_path}|browser_fallback/{backend}"
                else:
                    extraction_path = f"browser_fallback/{backend}"
                error_record = {
                    "seed_url": item.get("seed_url", ""),
                    "domain": item.get("domain", ""),
                    "page_url": item.get("referer", ""),
                    "pdf_url": browser_result.url,
                    "source_adapter": source_adapter,
                    "extraction_path": extraction_path,
                    "metadata": item.get("metadata", {}),
                    "error_type": f"browser_{browser_result.error_type}",
                    "message": browser_result.message,
                    "http_status": 0,
                    "attempt": 1,
                    "retries": 0,
                    "retrieved_at": _utc_now_iso(),
                    "browser_download_time_seconds": browser_result.download_time_seconds,
                }
                _append_jsonl(errors_path, error_record)
                print(
                    f"    ❌ Failed: {browser_result.error_type} - {browser_result.message}",
                    flush=True,
                )

        downloader.close()

    except Exception as exc:
        print(f"⚠️ Browser fallback error: {exc}", flush=True)
        result["skipped"] = len(waf_blocked_urls)

    print(
        f"🌐 Browser fallback complete: {result['succeeded']} succeeded, "
        f"{result['failed']} failed, {result['skipped']} skipped",
        flush=True,
    )

    return result

def _domains_with_waf_errors(errors_path: str) -> set[str]:
    stats = _count_waf_stats(errors_path)
    return set(stats.get("domains_affected", []))
