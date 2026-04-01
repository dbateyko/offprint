from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .text_extract import suppress_pypdf_noise

DOC_POLICIES = {"article_only", "include_issue_compilations", "all"}
DOC_TYPES = {"article", "issue_compilation", "frontmatter", "other"}

_CITATION_RE = re.compile(
    r"(?:\b\d+\s+U\.?S\.?C\.?\b|\b\d+\s+C\.?F\.?R\.?\b|\b[A-Z][A-Za-z]+\s+v\.\s+[A-Z][A-Za-z]+|§)",
    re.IGNORECASE,
)
_FOOTNOTE_MARKER_RE = re.compile(r"(?m)^\s*\d{1,3}[\]\)\.,]?\s+")
_DC_WRAPPER_RE = re.compile(
    r"(?:"
    r"brought to you for free and open access by"
    r"|accepted for inclusion in"
    r"|repository citation"
    r"|digital commons @"
    r"|follow this and additional works at"
    r")",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DocSignals:
    page_count: int
    first_page_text: str
    citation_pattern_count: int
    footnote_marker_count: int
    metadata_article_fields: bool
    garbled_text: bool


@dataclass(frozen=True)
class DocDecision:
    doc_type: str
    include: bool
    reason_codes: list[str]
    confidence: float
    platform_family: str
    domain: str
    ocr_candidate: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "doc_type": self.doc_type,
            "include": self.include,
            "reason_codes": list(self.reason_codes),
            "confidence": self.confidence,
            "platform_family": self.platform_family,
            "domain": self.domain,
            "ocr_candidate": self.ocr_candidate,
        }


def default_rules_path() -> str:
    return str(Path(__file__).with_name("doc_type_rules.json"))


def load_rules(path: str | None = None) -> dict[str, Any]:
    rule_path = Path(path or default_rules_path())
    if not rule_path.exists():
        return {"version": "0", "overrides": {"platform": {}, "domain": {}}}
    data = json.loads(rule_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {"version": "0", "overrides": {"platform": {}, "domain": {}}}
    return data


def infer_domain(pdf_path: str, pdf_root: str | None = None) -> str:
    path_obj = Path(pdf_path)
    if pdf_root:
        try:
            rel = path_obj.relative_to(Path(pdf_root))
            if rel.parts:
                first = rel.parts[0].lower()
                if "." in first:
                    return first
        except Exception:
            pass
    for part in path_obj.parts:
        low = part.lower()
        if "." in low and not low.endswith(".pdf"):
            return low
    return "unknown"


def infer_platform_family(
    *,
    domain: str,
    source_adapter: str = "",
    platform_raw: str = "",
) -> str:
    platform = " ".join((platform_raw or "").lower().replace("_", " ").split())
    source = " ".join((source_adapter or "").lower().replace("_", " ").split())
    compact_platform = platform.replace(" ", "")
    compact_source = source.replace(" ", "")
    domain_l = (domain or "").lower()

    dc_tokens = (
        "digitalcommons",
        "academicworks",
        "scholarship",
        "scholarworks",
        "repository.",
        "engagedscholarship",
        "commons.",
    )
    if any(token in domain_l for token in dc_tokens) or "digitalcommons" in compact_source:
        return "digital_commons"
    if "wordpress" in compact_platform or "wordpress" in compact_source:
        return "wordpress"
    if "scholastica" in compact_platform or "scholastica" in compact_source:
        return "scholastica"
    if compact_platform == "ojs" or "openjournalsystems" in compact_platform or "ojs" in compact_source:
        return "ojs"
    return "custom_unknown"


def read_first_page_overview(pdf_path: str) -> tuple[int, str]:
    try:
        suppress_pypdf_noise()
        from pypdf import PdfReader  # type: ignore
    except Exception:
        return 0, ""
    try:
        reader = PdfReader(pdf_path)
        page_count = len(reader.pages)
        first = reader.pages[0].extract_text() if page_count else ""
        return page_count, (first or "")
    except Exception:
        return 0, ""


def _metadata_article_fields(metadata: dict[str, Any] | None) -> bool:
    if not isinstance(metadata, dict):
        return False
    title = str(metadata.get("title") or "").strip()
    authors = metadata.get("authors")
    citation = str(metadata.get("citation") or "").strip()
    year = str(metadata.get("year") or metadata.get("date") or metadata.get("publication_date") or "").strip()
    if not title:
        return False
    has_author = False
    if isinstance(authors, list):
        has_author = any(str(item).strip() for item in authors)
    elif isinstance(authors, str):
        has_author = bool(authors.strip())
    return has_author or bool(citation or year)


def collect_signals(first_page_text: str, page_count: int, metadata: dict[str, Any] | None) -> DocSignals:
    text = first_page_text or ""
    visible = [ch for ch in text if not ch.isspace()]
    alpha_count = sum(1 for ch in visible if ch.isalpha())
    control_count = sum(1 for ch in visible if ord(ch) < 32)
    alpha_ratio = (alpha_count / max(len(visible), 1)) if visible else 0.0
    garbled = len(text) >= 120 and (alpha_ratio < 0.2 or control_count >= 10)
    return DocSignals(
        page_count=page_count,
        first_page_text=text[:6000],
        citation_pattern_count=len(_CITATION_RE.findall(text)),
        footnote_marker_count=len(_FOOTNOTE_MARKER_RE.findall(text)),
        metadata_article_fields=_metadata_article_fields(metadata),
        garbled_text=garbled,
    )


def _matches_override(
    rules: list[dict[str, Any]],
    *,
    filename: str,
    text: str,
) -> tuple[str, str] | None:
    for raw_rule in rules:
        if not isinstance(raw_rule, dict):
            continue
        field = str(raw_rule.get("field") or "").lower()
        pattern = str(raw_rule.get("pattern") or "")
        doc_type = str(raw_rule.get("doc_type") or "")
        reason = str(raw_rule.get("reason") or "override_match")
        if field not in {"filename", "text"} or not pattern or doc_type not in DOC_TYPES:
            continue
        haystack = filename if field == "filename" else text
        try:
            if re.search(pattern, haystack, flags=re.IGNORECASE):
                return doc_type, reason
        except re.error:
            continue
    return None


def should_include_doc(doc_type: str, doc_policy: str) -> bool:
    if doc_policy == "all":
        return True
    if doc_policy == "include_issue_compilations":
        return doc_type in {"article", "issue_compilation"}
    return doc_type == "article"


def classify_pdf(
    *,
    pdf_path: str,
    domain: str,
    platform_family: str,
    signals: DocSignals,
    doc_policy: str,
    rules: dict[str, Any],
) -> DocDecision:
    text_l = signals.first_page_text.lower()
    filename_l = Path(pdf_path).name.lower()
    reason_codes: list[str] = []
    doc_type = "article"
    confidence = 0.6

    overrides = rules.get("overrides") if isinstance(rules.get("overrides"), dict) else {}
    by_platform = overrides.get("platform") if isinstance(overrides.get("platform"), dict) else {}
    by_domain = overrides.get("domain") if isinstance(overrides.get("domain"), dict) else {}

    platform_rules = by_platform.get(platform_family) if isinstance(by_platform.get(platform_family), list) else []
    domain_rules = by_domain.get(domain) if isinstance(by_domain.get(domain), list) else []
    matched = _matches_override(domain_rules + platform_rules, filename=filename_l, text=text_l)
    if matched:
        doc_type, reason = matched
        reason_codes.append(reason)
        confidence = 0.95

    strong_frontmatter = False
    if doc_type == "article":
        if re.search(r"\b(submission|guidelines?|style\s+guide|manual)\b", filename_l + " " + text_l):
            doc_type = "other"
            reason_codes.append("manual_or_guidelines")
            confidence = 0.98
            strong_frontmatter = True
        elif platform_family == "digital_commons" and signals.page_count <= 5 and _DC_WRAPPER_RE.search(
            text_l
        ):
            doc_type = "frontmatter"
            reason_codes.append("dc_repository_wrapper")
            confidence = 0.98
            strong_frontmatter = True
        elif re.search(r"\b(editorial\s*board|masthead|inside[-\s]?cover|dedication)\b", filename_l + " " + text_l):
            doc_type = "frontmatter"
            reason_codes.append("frontmatter_marker")
            confidence = 0.96
            strong_frontmatter = True
        elif "table of contents" in text_l or re.search(r"\b(toc|contents)\b", filename_l):
            doc_type = "issue_compilation" if signals.page_count > 6 else "frontmatter"
            reason_codes.append("toc_marker")
            confidence = 0.95
            strong_frontmatter = True
        elif "yearbook" in text_l or "yearbook" in filename_l:
            doc_type = "other"
            reason_codes.append("yearbook_marker")
            confidence = 0.98
            strong_frontmatter = True

    article_like = (
        signals.metadata_article_fields
        or signals.citation_pattern_count >= 2
        or signals.footnote_marker_count >= 2
        or ("abstract" in text_l and "introduction" in text_l)
    )
    if not strong_frontmatter and article_like:
        doc_type = "article"
        if "article_like" not in reason_codes:
            reason_codes.append("article_like")
        confidence = max(confidence, 0.75)

    ocr_candidate = (
        signals.page_count > 0
        and (
            len(signals.first_page_text.strip()) < 300
            or signals.garbled_text
            or (
                signals.footnote_marker_count == 0
                and signals.citation_pattern_count == 0
                and signals.metadata_article_fields
            )
        )
    )

    if doc_policy not in DOC_POLICIES:
        doc_policy = "article_only"
    include = should_include_doc(doc_type, doc_policy)
    return DocDecision(
        doc_type=doc_type,
        include=include,
        reason_codes=sorted(set(reason_codes)),
        confidence=round(float(confidence), 3),
        platform_family=platform_family,
        domain=domain,
        ocr_candidate=ocr_candidate,
    )
