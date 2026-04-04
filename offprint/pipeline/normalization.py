import re
from datetime import datetime
from typing import Any, Dict, Optional

def _normalize_metadata(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    metadata = dict(raw or {})
    for key in [
        "title",
        "authors",
        "issue",
        "volume",
        "year",
        "pdf_filename",
        "pdf_relative_path",
    ]:
        metadata.setdefault(key, None)
    return metadata

def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(_normalize_text(v) for v in value if _normalize_text(v))
    return str(value).strip()

def _normalize_journal_name(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return text

def _extract_journal_name(metadata: Dict[str, Any]) -> str:
    for key in ("journal", "journal_name", "publication_title", "name"):
        name = _normalize_journal_name(metadata.get(key))
        if name:
            return name
    return ""

_DC_ADAPTER_CONFIG_ALIASES = {
    "enum_mode": "enum_mode",
    "dc_enum_mode": "enum_mode",
    "use_siteindex": "use_siteindex",
    "dc_use_siteindex": "use_siteindex",
    "ua_profiles": "ua_profiles",
    "dc_ua_fallback_profiles": "ua_profiles",
    "robots_enforce": "robots_enforce",
    "dc_robots_enforce": "robots_enforce",
    "max_oai_records": "max_oai_records",
    "dc_max_oai_records": "max_oai_records",
    "max_sitemap_urls": "max_sitemap_urls",
    "dc_max_sitemap_urls": "max_sitemap_urls",
    "download_timeout": "download_timeout",
    "dc_download_timeout": "download_timeout",
    "min_domain_delay_ms": "min_domain_delay_ms",
    "dc_min_domain_delay_ms": "min_domain_delay_ms",
    "max_domain_delay_ms": "max_domain_delay_ms",
    "dc_max_domain_delay_ms": "max_domain_delay_ms",
    "waf_fail_threshold": "waf_fail_threshold",
    "dc_waf_fail_threshold": "waf_fail_threshold",
    "waf_cooldown_seconds": "waf_cooldown_seconds",
    "dc_waf_cooldown_seconds": "waf_cooldown_seconds",
    "disable_unscoped_oai_no_slug": "disable_unscoped_oai_no_slug",
    "dc_disable_unscoped_oai_no_slug": "disable_unscoped_oai_no_slug",
    "allow_generic_fallback": "allow_generic_fallback",
    "dc_allow_generic_fallback": "allow_generic_fallback",
    "session_rotate_threshold": "session_rotate_threshold",
    "dc_session_rotate_threshold": "session_rotate_threshold",
    "use_curl_cffi": "use_curl_cffi",
    "dc_use_curl_cffi": "use_curl_cffi",
}

def _normalize_adapter_config(raw: Any, *, file_label: str) -> Dict[str, Any]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{file_label}: adapter_config must be an object")

    dc_raw = raw.get("dc")
    if dc_raw is None:
        return {}
    if not isinstance(dc_raw, dict):
        raise ValueError(f"{file_label}: adapter_config.dc must be an object")

    unknown_keys = sorted(k for k in dc_raw.keys() if k not in _DC_ADAPTER_CONFIG_ALIASES)
    if unknown_keys:
        allowed = ", ".join(sorted(_DC_ADAPTER_CONFIG_ALIASES.keys()))
        unknown = ", ".join(unknown_keys)
        raise ValueError(
            f"{file_label}: unsupported adapter_config.dc keys: {unknown}. "
            f"Allowed keys: {allowed}"
        )

    normalized_dc: Dict[str, Any] = {}
    for key, value in dc_raw.items():
        canonical = _DC_ADAPTER_CONFIG_ALIASES[key]
        normalized_dc[canonical] = value

    return {"dc": normalized_dc} if normalized_dc else {}

def _coerce_bool(value: Any, *, label: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{label} must be a boolean-like value")

def _coerce_int(value: Any, *, label: str, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer") from exc
    return max(parsed, minimum)

def _seed_dc_overrides(seed_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    context = seed_context if isinstance(seed_context, dict) else {}
    adapter_config = context.get("adapter_config")
    if not isinstance(adapter_config, dict):
        return {}
    dc_raw = adapter_config.get("dc")
    if not isinstance(dc_raw, dict):
        return {}

    file_label = str(context.get("sitemap_file") or context.get("seed_url") or "adapter_config.dc")
    overrides: Dict[str, Any] = {}
    for key, value in dc_raw.items():
        label = f"{file_label}.{key}"
        if key in {
            "enum_mode",
        }:
            text = str(value or "").strip()
            if not text:
                raise ValueError(f"{label} must be a non-empty string")
            overrides[key] = text
        elif key in {"ua_profiles"}:
            if isinstance(value, str):
                parsed = [p.strip() for p in value.split(",") if p.strip()]
            elif isinstance(value, list):
                parsed = [str(v).strip() for v in value if str(v).strip()]
            else:
                raise ValueError(f"{label} must be a comma-separated string or list of strings")
            if not parsed:
                raise ValueError(f"{label} must not be empty")
            overrides[key] = parsed
        elif key in {
            "use_siteindex",
            "robots_enforce",
            "disable_unscoped_oai_no_slug",
            "allow_generic_fallback",
            "use_curl_cffi",
        }:
            overrides[key] = _coerce_bool(value, label=label)
        elif key in {
            "max_oai_records",
            "max_sitemap_urls",
            "download_timeout",
            "min_domain_delay_ms",
            "max_domain_delay_ms",
            "waf_fail_threshold",
            "waf_cooldown_seconds",
            "session_rotate_threshold",
        }:
            minimum = 1 if key == "download_timeout" else 0
            overrides[key] = _coerce_int(value, label=label, minimum=minimum)
    return overrides

def _article_key(record: Dict[str, Any]) -> str:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    metadata = metadata or {}

    article_url = _normalize_text(metadata.get("url") or metadata.get("article_url"))
    if article_url:
        return f"url:{article_url.lower()}"

    doi = _normalize_text(metadata.get("doi"))
    if doi:
        return f"doi:{doi.lower()}"

    title = _normalize_text(metadata.get("title")).lower()
    volume = _normalize_text(metadata.get("volume")).lower()
    issue = _normalize_text(metadata.get("issue")).lower()
    date_text = _normalize_text(metadata.get("date") or metadata.get("year")).lower()
    authors = _normalize_text(metadata.get("authors")).lower()

    if title:
        return f"title:{title}|vol:{volume}|iss:{issue}|date:{date_text}|auth:{authors}"

    pdf_url = _normalize_text(record.get("pdf_url")).lower()
    if pdf_url:
        return f"pdf:{pdf_url}"

    page_url = _normalize_text(record.get("page_url")).lower()
    return f"page:{page_url}"

def _parse_partial_date(value: Any) -> Optional[Dict[str, Any]]:
    text = _normalize_text(value)
    if not text:
        return None
    text = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", text, flags=re.IGNORECASE)
    text = text.replace("Sept.", "Sep.").replace("Sept ", "Sep ")
    text = re.sub(r"\s+", " ", text).strip()

    normalized = text.replace("/", "-").replace(".", "-")
    full_match = re.search(r"\b((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})\b", normalized)
    if full_match:
        year, month, day = (
            int(full_match.group(1)),
            int(full_match.group(2)),
            int(full_match.group(3)),
        )
        if 1 <= month <= 12 and 1 <= day <= 31:
            return {
                "normalized": f"{year:04d}-{month:02d}-{day:02d}",
                "start_key": (year, month, day),
                "end_key": (year, month, day),
                "year": year,
            }

    ym_match = re.search(r"\b((?:19|20)\d{2})-(\d{1,2})\b", normalized)
    if ym_match:
        year, month = int(ym_match.group(1)), int(ym_match.group(2))
        if 1 <= month <= 12:
            return {
                "normalized": f"{year:04d}-{month:02d}",
                "start_key": (year, month, 1),
                "end_key": (year, month, 31),
                "year": year,
            }

    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%d %B %Y",
        "%d %b %Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %Y",
        "%b %Y",
    ):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt in {"%B %Y", "%b %Y"}:
                return {
                    "normalized": parsed.strftime("%Y-%m"),
                    "start_key": (parsed.year, parsed.month, 1),
                    "end_key": (parsed.year, parsed.month, 31),
                    "year": parsed.year,
                }
            return {
                "normalized": parsed.strftime("%Y-%m-%d"),
                "start_key": (parsed.year, parsed.month, parsed.day),
                "end_key": (parsed.year, parsed.month, parsed.day),
                "year": parsed.year,
            }
        except ValueError:
            continue

    year_match = re.search(r"\b((?:19|20)\d{2})\b", text)
    if year_match:
        year = int(year_match.group(1))
        return {
            "normalized": f"{year:04d}",
            "start_key": (year, 1, 1),
            "end_key": (year, 12, 31),
            "year": year,
        }
    return None
