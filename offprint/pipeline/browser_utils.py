from typing import Any, Dict, Optional
from urllib.parse import urlparse

def _download_single_with_browser(
    *,
    record: Dict[str, Any],
    out_dir: str,
    backend: str,
    staging_dir: str,
    user_data_dir: str,
    timeout: int,
    headless: bool,
    operator_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    from ..mcp_browser_download import create_browser_downloader

    pdf_url = str(record.get("pdf_url") or "").strip()
    domain = str(record.get("domain") or urlparse(pdf_url).netloc or "unknown")
    referer = str(record.get("page_url") or "")

    downloader = create_browser_downloader(
        backend=backend,  # type: ignore[arg-type]
        staging_dir=staging_dir,
        user_data_dir=user_data_dir,
        final_out_dir=out_dir,
        timeout_seconds=timeout,
        headless=headless,
        operator_config=operator_config,
    )
    try:
        downloader.add_url(pdf_url, domain=domain, metadata=record.get("metadata"), referer=referer)
        results = downloader.process_batch()
    finally:
        downloader.close()

    if not results:
        return {
            "ok": False,
            "local_path": None,
            "error_type": "browser_no_result",
            "message": "Browser failed to produce a result",
        }

    res = results[0]
    return {
        "ok": res.ok,
        "local_path": res.local_path,
        "error_type": res.error_type,
        "message": res.message,
    }
