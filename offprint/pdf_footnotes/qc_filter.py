from __future__ import annotations

import glob
import hashlib
import json
import os
import re
import shutil
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from offprint.adapters.utils import compute_pdf_sha256_and_size

QC_VERSION = "3"

STRONG_MARKERS = ("title-page", "masthead", "toc", "editorialboard", "frontmatter")

CITATION_RE = re.compile(
    r"(?:\b\d+\s+U\.?S\.?C\.?\b|\b\d+\s+C\.?F\.?R\.?\b|\b[A-Z][A-Za-z]+\s+v\.\s+[A-Z][A-Za-z]+|§)",
    re.IGNORECASE,
)
FOOTNOTE_MARKER_RE = re.compile(r"(?m)^\s*\d{1,3}[\]\)\.,]?\s+")

# --- Full-volume / full-issue detection ---
FULL_VOLUME_FILENAME_RE = re.compile(
    r"(?:full[-_]?issue|full[-_]?volume|complete[-_]?issue|"
    r"full[-_]?web[-_]?issue|"
    r"combined[-_]?front[-_]?matter|"
    r"\d+[-_]ar[-_]full|"
    r"fm[-_]\d{4}[-_]\d{4})",  # e.g. "15-berkeley-tech-l-j-fm-0865-1274"
    re.IGNORECASE,
)
WHOLE_ISSUE_NAMING_RE = re.compile(r"^jol\d+[-_]\d+(?:[-_]ae)?\.pdf$", re.IGNORECASE)

# --- Non-law-review content detection ---
NON_LR_FILENAME_RE = re.compile(
    r"\b(?:annual[-_]?report|strategic[-_]?plan|financial[-_]?statement|"
    r"press[-_]?release|ipcc|climate[-_]?report|"
    r"budget|audit|newsletter|bulletin|"
    r"handbook|thesis|dissertation|syllabus|curriculum|"
    r"feasibility[-_]?study|impact[-_]?report|"
    r"uscode|compendium)\b",
    re.IGNORECASE,
)
NON_LR_FILENAME_ALLOWLIST = frozenset({"book", "appendix"})


def _eyecite_count(text: str) -> int:
    """Return the number of legal citations found by eyecite.

    Returns -1 if eyecite is unavailable (treated as unknown — rules requiring
    zero cites will not fire, preserving the conservative page-count fallbacks).
    """
    try:
        from eyecite import get_citations  # type: ignore

        return len(get_citations(text))
    except Exception:
        return -1


@dataclass
class QCConfig:
    pdf_root: str
    quarantine_root: str = "artifacts/quarantine"
    manifest_out: str | None = None
    report_out: str | None = None
    dry_run: bool = False
    limit: int = 0


@dataclass
class QCSignals:
    source_pdf_path: str
    filename_tokens: list[str]
    path_tokens: list[str]
    page_count: int
    first_page_text: str
    punctuation_ratio: float
    citation_pattern_count: int
    footnote_marker_count: int
    metadata_has_article_fields: bool
    eyecite_citation_count: int = -1  # -1 = eyecite unavailable
    file_size_bytes: int = 0


@dataclass
class DomainContext:
    domain: str
    pdf_count: int
    median_size_bytes: float
    median_page_count: float


@dataclass
class QCDecision:
    source_pdf_path: str
    decision: str
    reason_codes: list[str]
    guardrail_codes: list[str]
    rule_confidence: float
    signals: dict[str, Any]


@dataclass
class QCRunResult:
    report_path: str
    manifest_path: str
    scanned: int
    excluded: int
    kept: int
    copied: int
    dry_run: bool
    rule_counts: dict[str, int] = field(default_factory=dict)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _tokenize(value: str) -> list[str]:
    tokens = re.split(r"[^A-Za-z0-9]+", value.lower())
    return [token for token in tokens if token]


def _read_pdf_text(pdf_path: str) -> tuple[int, str]:
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(pdf_path)
        page_count = len(reader.pages)
        first_page_text = ""
        if page_count > 0:
            first_page_text = reader.pages[0].extract_text() or ""
        return page_count, first_page_text
    except Exception:
        return 0, ""


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _metadata_article_fields(metadata: dict[str, Any] | None) -> bool:
    if not isinstance(metadata, dict):
        return False
    title = str(metadata.get("title") or "").strip()
    authors = metadata.get("authors")
    citation = str(metadata.get("citation") or "").strip()
    date = str(metadata.get("date") or metadata.get("year") or "").strip()

    has_support = bool(citation or date)
    if isinstance(authors, list):
        has_support = has_support or bool([item for item in authors if str(item).strip()])
    elif isinstance(authors, str):
        has_support = has_support or bool(authors.strip())

    return bool(title and has_support)


def collect_signals(pdf_path: str, metadata: dict[str, Any] | None = None) -> QCSignals:
    page_count, first_page_text = _read_pdf_text(pdf_path)
    filename_tokens = _tokenize(os.path.basename(pdf_path))
    path_tokens = _tokenize(pdf_path)

    try:
        file_size_bytes = os.path.getsize(pdf_path)
    except OSError:
        file_size_bytes = 0

    visible_chars = [ch for ch in first_page_text if not ch.isspace()]
    punct_chars = [ch for ch in visible_chars if re.match(r"[^\w]", ch)]

    citation_pattern_count = len(CITATION_RE.findall(first_page_text or ""))
    footnote_marker_count = len(FOOTNOTE_MARKER_RE.findall(first_page_text or ""))

    first_page_text_capped = (first_page_text or "")[:6000]
    return QCSignals(
        source_pdf_path=pdf_path,
        filename_tokens=filename_tokens,
        path_tokens=path_tokens,
        page_count=page_count,
        first_page_text=first_page_text_capped,
        punctuation_ratio=round(_safe_ratio(len(punct_chars), max(len(visible_chars), 1)), 4),
        citation_pattern_count=citation_pattern_count,
        footnote_marker_count=footnote_marker_count,
        metadata_has_article_fields=_metadata_article_fields(metadata),
        eyecite_citation_count=_eyecite_count(first_page_text_capped),
        file_size_bytes=file_size_bytes,
    )


def _has_filename_marker(tokens: list[str], marker: str) -> bool:
    if marker == "title-page":
        return "title" in tokens and "page" in tokens
    if marker == "editorialboard":
        return "editorialboard" in tokens or ("editorial" in tokens and "board" in tokens)
    return marker in tokens


def _filename_has_any(tokens: list[str], options: tuple[str, ...]) -> bool:
    return any(_has_filename_marker(tokens, marker) for marker in options)


def _rule_reasons(signals: QCSignals) -> list[str]:
    reasons: list[str] = []
    tokens = signals.filename_tokens
    text_lower = signals.first_page_text.lower()

    # Convenience booleans for eyecite results.
    # no_eyecite_cites is True only when eyecite ran successfully AND found 0 citations.
    # When eyecite is unavailable (count == -1) we fall back to page-count heuristics.
    ec = signals.eyecite_citation_count
    no_eyecite_cites = ec == 0  # False when ec == -1 (unknown)

    # --- Masthead ---
    # Preferred: eyecite confirms 0 citations (no page-count limit needed).
    # Fallback:  page count ≤ 3 when eyecite is unavailable.
    if _has_filename_marker(tokens, "masthead"):
        if no_eyecite_cites:
            reasons.append("masthead_no_cites")
        elif ec < 0 and signals.page_count <= 3:
            reasons.append("masthead_short")

    # --- Table of contents ---
    # Relaxed from ≤ 3 pages to ≤ 15 pages now that eyecite guards against real articles.
    toc_in_name = _has_filename_marker(tokens, "toc") or (
        _has_filename_marker(tokens, "table") and _has_filename_marker(tokens, "contents")
    )
    if toc_in_name:
        if no_eyecite_cites and signals.page_count <= 15:
            reasons.append("toc_no_cites")
        elif ec < 0 and signals.page_count <= 3:
            reasons.append("toc_short_name")

    # --- Frontmatter explicitly labeled in filename ---
    # Files named "frontmatter" are unambiguously non-article; require 0 eyecite cites
    # as a safety net for edge cases (e.g., a journal that names articles with this word).
    if _has_filename_marker(tokens, "frontmatter") and no_eyecite_cites:
        reasons.append("frontmatter_in_filename")

    # --- Cover pages ---
    # Short PDFs labeled "cover" with no legal citations.
    if "cover" in tokens and no_eyecite_cites and signals.page_count <= 3:
        reasons.append("cover_no_cites")

    # --- Editorial board ---
    editorial_in_name = _has_filename_marker(tokens, "editorialboard") or (
        _has_filename_marker(tokens, "board") and _has_filename_marker(tokens, "editors")
    )
    if editorial_in_name and signals.page_count <= 6:
        reasons.append("editorial_short")

    # --- Multi-marker frontmatter ---
    # Two or more strong frontmatter keywords in the filename is highly reliable.
    strong_count = sum(1 for marker in STRONG_MARKERS if _has_filename_marker(tokens, marker))
    if strong_count >= 2:
        reasons.append("multi_marker_frontmatter")

    # --- Single-page TOC-layout ---
    has_volume = "volume" in text_lower
    has_number = "number" in text_lower
    has_articles_or_notes = ("articles" in text_lower) or ("notes" in text_lower)
    list_like = len([line for line in signals.first_page_text.splitlines() if line.strip()]) >= 8
    if (
        signals.page_count == 1
        and has_volume
        and has_number
        and has_articles_or_notes
        and list_like
    ):
        reasons.append("frontmatter_layout_onepage")

    # --- Submission guidelines / style manuals ---
    # Requires the keyword in BOTH filename AND first-page text to avoid false positives.
    manual_in_name = (
        "manual" in tokens
        or "guidelines" in tokens
        or ("submission" in tokens and "guidelines" in tokens)
        or ("style" in tokens and "guide" in tokens)
    )
    manual_in_text = bool(
        re.search(r"\b(manual|guidelines|submission\s+guidelines|style\s+guide)\b", text_lower)
    )
    if manual_in_name and manual_in_text:
        reasons.append("manual_guideline_strict")

    return sorted(set(reasons))


def _full_volume_reasons(
    signals: QCSignals, domain_ctx: DomainContext | None
) -> list[str]:
    """Detect full-volume/full-issue PDFs that duplicate individual articles."""
    if domain_ctx is None or domain_ctx.pdf_count <= 1:
        return []

    reasons: list[str] = []
    raw_filename = os.path.basename(signals.source_pdf_path).lower()

    # Filename-based detection
    if FULL_VOLUME_FILENAME_RE.search(raw_filename) or WHOLE_ISSUE_NAMING_RE.match(raw_filename):
        reasons.append("full_volume_filename")

    # Size anomaly: PDF is >5x the domain median AND >10MB
    if (
        domain_ctx.median_size_bytes > 0
        and signals.file_size_bytes > 10_485_760
        and signals.file_size_bytes > 5 * domain_ctx.median_size_bytes
    ):
        reasons.append("full_volume_size_anomaly")

    # Page count anomaly: >150 pages when domain median is <40
    if (
        signals.page_count > 150
        and domain_ctx.median_page_count > 0
        and domain_ctx.median_page_count < 40
    ):
        reasons.append("full_volume_page_anomaly")

    return sorted(set(reasons))


def _non_lr_reasons(signals: QCSignals) -> list[str]:
    """Detect non-law-review content (reports, theses, newsletters, etc.)."""
    reasons: list[str] = []
    raw_filename = os.path.basename(signals.source_pdf_path).lower()
    tokens = signals.filename_tokens
    ec = signals.eyecite_citation_count
    no_eyecite_cites = ec == 0

    # Filename-based: match non-LR patterns, but protect book reviews / appendices
    has_allowlist_token = bool(NON_LR_FILENAME_ALLOWLIST & set(tokens))
    if NON_LR_FILENAME_RE.search(raw_filename) and not has_allowlist_token and no_eyecite_cites:
        reasons.append("non_lr_filename")

    return sorted(set(reasons))


def _guardrail_reasons(signals: QCSignals, matched_reasons: list[str]) -> list[str]:
    guardrails: list[str] = []
    text_lower = signals.first_page_text.lower()

    # Prefer eyecite for citation detection; fall back to the legacy regex count.
    ec = signals.eyecite_citation_count
    has_citations = ec >= 1 if ec >= 0 else signals.citation_pattern_count >= 2

    if signals.page_count >= 8 and has_citations:
        guardrails.append("article_like_longform")

    if signals.footnote_marker_count >= 3:
        guardrails.append("article_like_footnotes")

    if signals.metadata_has_article_fields:
        guardrails.append("metadata_article_present")

    has_toc_phrase = "table of contents" in text_lower
    has_filename_markers = bool(
        _filename_has_any(
            signals.filename_tokens, ("toc", "masthead", "editorialboard", "title-page", "frontmatter")
        )
    )
    if has_toc_phrase and not has_filename_markers and not matched_reasons:
        guardrails.append("toc_phrase_only")

    return sorted(set(guardrails))


def _rule_confidence(reasons: list[str]) -> float:
    if not reasons:
        return 0.0
    confidence_by_rule = {
        # eyecite-confirmed rules (higher confidence)
        "masthead_no_cites": 0.995,
        "toc_no_cites": 0.995,
        "frontmatter_in_filename": 0.995,
        "cover_no_cites": 0.990,
        # page-count fallback rules (used only when eyecite is unavailable)
        "masthead_short": 0.995,
        "toc_short_name": 0.990,
        # other rules
        "editorial_short": 0.990,
        "multi_marker_frontmatter": 0.995,
        "frontmatter_layout_onepage": 0.990,
        "manual_guideline_strict": 0.990,
        # full-volume rules
        "full_volume_filename": 0.985,
        "full_volume_size_anomaly": 0.980,
        "full_volume_page_anomaly": 0.980,
        # non-law-review content rules
        "non_lr_filename": 0.985,
    }
    score = max(confidence_by_rule.get(reason, 0.0) for reason in reasons)
    return round(score, 3)


def evaluate_pdf(signals: QCSignals) -> QCDecision:
    # Fast path: eyecite found at least one legal citation — almost certainly an article.
    # Skip all rule evaluation and return immediately.
    if signals.eyecite_citation_count >= 1:
        return QCDecision(
            source_pdf_path=signals.source_pdf_path,
            decision="keep",
            reason_codes=[],
            guardrail_codes=["article_like_citations"],
            rule_confidence=0.0,
            signals={
                "filename_tokens": signals.filename_tokens,
                "page_count": signals.page_count,
                "eyecite_citation_count": signals.eyecite_citation_count,
                "metadata_has_article_fields": signals.metadata_has_article_fields,
            },
        )

    reasons = _rule_reasons(signals)
    guardrails = _guardrail_reasons(signals, reasons)
    confidence = _rule_confidence(reasons)

    decision = "keep"
    if reasons and not guardrails and confidence >= 0.98:
        decision = "exclude"

    signal_payload = {
        "filename_tokens": signals.filename_tokens,
        "path_tokens": signals.path_tokens,
        "page_count": signals.page_count,
        "first_page_text": signals.first_page_text[:1200],
        "punctuation_ratio": signals.punctuation_ratio,
        "citation_pattern_count": signals.citation_pattern_count,
        "eyecite_citation_count": signals.eyecite_citation_count,
        "footnote_marker_count": signals.footnote_marker_count,
        "metadata_has_article_fields": signals.metadata_has_article_fields,
    }

    return QCDecision(
        source_pdf_path=signals.source_pdf_path,
        decision=decision,
        reason_codes=reasons,
        guardrail_codes=guardrails,
        rule_confidence=confidence,
        signals=signal_payload,
    )


def evaluate_pdf_with_context(
    signals: QCSignals, domain_ctx: DomainContext | None = None
) -> QCDecision:
    """Evaluate a PDF with domain-level context for full-volume and non-LR rules.

    Runs existing per-file rules first, then applies full-volume and non-LR
    rules that require domain context. Full-volume filename matches bypass
    the eyecite fast path (full volumes contain citations from many articles).
    """
    raw_filename = os.path.basename(signals.source_pdf_path).lower()
    has_volume_filename = bool(
        FULL_VOLUME_FILENAME_RE.search(raw_filename) or WHOLE_ISSUE_NAMING_RE.match(raw_filename)
    )

    # Run base per-file rules. If the file has eyecite citations AND no
    # volume-filename pattern, use the fast path (definitely an article).
    if signals.eyecite_citation_count >= 1 and not has_volume_filename:
        base_reasons: list[str] = []
        base_guardrails = ["article_like_citations"]
    else:
        base_reasons = _rule_reasons(signals)
        base_guardrails = _guardrail_reasons(signals, base_reasons)

    # Layer on full-volume and non-LR rules
    fv_reasons = _full_volume_reasons(signals, domain_ctx)
    nlr_reasons = _non_lr_reasons(signals)

    all_reasons = sorted(set(base_reasons + fv_reasons + nlr_reasons))

    # Full-volume guardrails
    fv_guardrails: list[str] = []
    if fv_reasons:
        # Protect short articles that look large but have many citations
        if signals.eyecite_citation_count >= 5 and signals.page_count < 50:
            fv_guardrails.append("short_with_many_cites")

    # When full-volume rules fire, suppress article_like guardrails that would
    # incorrectly protect the file (full volumes contain citations and footnotes
    # from many articles, so article_like_* signals are expected noise).
    combined_guardrails = list(base_guardrails) + fv_guardrails
    if fv_reasons:
        combined_guardrails = [
            g for g in combined_guardrails
            if g not in ("article_like_longform", "article_like_footnotes", "article_like_citations")
        ]

    all_guardrails = sorted(set(combined_guardrails))
    confidence = _rule_confidence(all_reasons)

    decision = "keep"
    if all_reasons and not all_guardrails and confidence >= 0.98:
        decision = "exclude"

    signal_payload = {
        "filename_tokens": signals.filename_tokens,
        "path_tokens": signals.path_tokens,
        "page_count": signals.page_count,
        "first_page_text": signals.first_page_text[:1200],
        "punctuation_ratio": signals.punctuation_ratio,
        "citation_pattern_count": signals.citation_pattern_count,
        "eyecite_citation_count": signals.eyecite_citation_count,
        "footnote_marker_count": signals.footnote_marker_count,
        "metadata_has_article_fields": signals.metadata_has_article_fields,
        "file_size_bytes": signals.file_size_bytes,
    }
    if domain_ctx is not None:
        signal_payload["domain_pdf_count"] = domain_ctx.pdf_count
        signal_payload["domain_median_size_bytes"] = domain_ctx.median_size_bytes
        signal_payload["domain_median_page_count"] = domain_ctx.median_page_count

    return QCDecision(
        source_pdf_path=signals.source_pdf_path,
        decision=decision,
        reason_codes=all_reasons,
        guardrail_codes=all_guardrails,
        rule_confidence=confidence,
        signals=signal_payload,
    )


def _compute_domain_contexts(
    signals_list: list[tuple[str, QCSignals]], pdf_root: str
) -> dict[str, DomainContext]:
    """Compute per-domain statistics from collected signals."""
    by_domain: dict[str, list[QCSignals]] = {}
    for abs_path, sig in signals_list:
        rel = os.path.relpath(abs_path, pdf_root)
        domain = _infer_domain(rel)
        by_domain.setdefault(domain, []).append(sig)

    contexts: dict[str, DomainContext] = {}
    for domain, sigs in by_domain.items():
        sizes = [s.file_size_bytes for s in sigs if s.file_size_bytes > 0]
        pages = [s.page_count for s in sigs if s.page_count > 0]
        contexts[domain] = DomainContext(
            domain=domain,
            pdf_count=len(sigs),
            median_size_bytes=statistics.median(sizes) if sizes else 0.0,
            median_page_count=statistics.median(pages) if pages else 0.0,
        )
    return contexts


def _compute_ruleset_hash() -> str:
    payload = {
        "version": QC_VERSION,
        "rules": [
            # eyecite-primary rules
            "masthead_no_cites",
            "toc_no_cites",
            "frontmatter_in_filename",
            "cover_no_cites",
            # page-count fallback rules (fire only when eyecite is unavailable)
            "masthead_short",
            "toc_short_name",
            # always-on rules
            "editorial_short",
            "multi_marker_frontmatter",
            "frontmatter_layout_onepage",
            "manual_guideline_strict",
            # full-volume rules
            "full_volume_filename",
            "full_volume_size_anomaly",
            "full_volume_page_anomaly",
            # non-law-review content rules
            "non_lr_filename",
        ],
        "guardrails": [
            "article_like_longform",
            "article_like_footnotes",
            "metadata_article_present",
            "toc_phrase_only",
            "short_with_many_cites",
        ],
    }
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _discover_pdfs(pdf_root: str, limit: int = 0) -> list[str]:
    found = sorted(str(path) for path in Path(pdf_root).rglob("*.pdf"))
    if limit and limit > 0:
        return found[:limit]
    return found


def _infer_domain(relative_path: str) -> str:
    parts = Path(relative_path).parts
    if not parts:
        return "unknown"
    first = parts[0]
    if "." in first:
        return first
    return "unknown"


def _copied_quarantine_path(
    pdf_path: str, pdf_root: str, quarantine_root: str, primary_reason: str
) -> str:
    rel = os.path.relpath(pdf_path, pdf_root)
    domain = _infer_domain(rel)
    filename = os.path.basename(pdf_path)
    return os.path.join(quarantine_root, primary_reason, domain, filename)


def _copy_if_needed(source: str, destination: str) -> bool:
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    if os.path.exists(destination):
        src_hash, _ = compute_pdf_sha256_and_size(source)
        dst_hash, _ = compute_pdf_sha256_and_size(destination)
        if src_hash and src_hash == dst_hash:
            return False
    shutil.copy2(source, destination)
    return True


def _write_jsonl_atomic(path: str, rows: list[dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    os.replace(tmp, path)


def _write_json_atomic(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    os.replace(tmp, path)


def default_manifest_path() -> str:
    os.makedirs("artifacts/runs", exist_ok=True)
    return os.path.join("artifacts/runs", f"pdf_qc_exclusions_{_utc_stamp()}.jsonl")


def default_report_path() -> str:
    os.makedirs("artifacts/runs", exist_ok=True)
    return os.path.join("artifacts/runs", f"pdf_qc_report_{_utc_stamp()}.json")


def latest_qc_manifest(runs_dir: str = "artifacts/runs") -> str | None:
    pattern = os.path.join(runs_dir, "pdf_qc_exclusions_*.jsonl")
    candidates = sorted(glob.glob(pattern))
    if candidates:
        return candidates[-1]
    if runs_dir == "artifacts/runs":
        legacy_pattern = os.path.join("runs", "pdf_qc_exclusions_*.jsonl")
        legacy_candidates = sorted(glob.glob(legacy_pattern))
        return legacy_candidates[-1] if legacy_candidates else None
    return None


def load_excluded_paths(manifest_path: str) -> set[str]:
    excluded: set[str] = set()
    if not manifest_path or not os.path.exists(manifest_path):
        return excluded

    with open(manifest_path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(row.get("decision") or "") != "exclude":
                continue
            source = str(row.get("source_pdf_path") or "").strip()
            if source:
                excluded.add(os.path.abspath(source))
    return excluded


def run_qc(config: QCConfig) -> QCRunResult:
    pdf_root = os.path.abspath(config.pdf_root)
    quarantine_root = os.path.abspath(config.quarantine_root)
    manifest_path = config.manifest_out or default_manifest_path()
    report_path = config.report_out or default_report_path()

    pdf_paths = _discover_pdfs(pdf_root, limit=config.limit)

    exclusion_rows: list[dict[str, Any]] = []
    rule_counts: dict[str, int] = {}
    copied = 0
    kept = 0

    seen_paths: set[str] = set()

    # --- Pass 1: collect signals for all PDFs ---
    all_signals: list[tuple[str, QCSignals]] = []

    try:
        from tqdm import tqdm  # type: ignore

        _iter1 = tqdm(pdf_paths, unit="pdf", desc="QC pass 1 (signals)", dynamic_ncols=True)
    except ImportError:
        _iter1 = iter(pdf_paths)  # type: ignore[assignment]

    for pdf_path in _iter1:
        abs_path = os.path.abspath(pdf_path)
        if abs_path in seen_paths:
            continue
        seen_paths.add(abs_path)
        signals = collect_signals(abs_path)
        all_signals.append((abs_path, signals))

    # --- Compute domain-level context ---
    domain_contexts = _compute_domain_contexts(all_signals, pdf_root)

    # --- Pass 2: evaluate with domain context and quarantine ---
    try:
        from tqdm import tqdm  # type: ignore

        _iter2 = tqdm(all_signals, unit="pdf", desc="QC pass 2 (evaluate)", dynamic_ncols=True)
    except ImportError:
        _iter2 = iter(all_signals)  # type: ignore[assignment]

    for abs_path, signals in _iter2:
        rel = os.path.relpath(abs_path, pdf_root)
        domain = _infer_domain(rel)
        domain_ctx = domain_contexts.get(domain)
        decision = evaluate_pdf_with_context(signals, domain_ctx)

        if decision.decision != "exclude":
            kept += 1
            continue

        for reason in decision.reason_codes:
            rule_counts[reason] = int(rule_counts.get(reason, 0)) + 1

        source_hash, _ = compute_pdf_sha256_and_size(abs_path)
        primary_reason = decision.reason_codes[0] if decision.reason_codes else "unspecified"
        copied_path = _copied_quarantine_path(abs_path, pdf_root, quarantine_root, primary_reason)

        if not config.dry_run:
            did_copy = _copy_if_needed(abs_path, copied_path)
            if did_copy:
                copied += 1

        exclusion_rows.append(
            {
                "source_pdf_path": abs_path,
                "source_pdf_sha256": source_hash,
                "copied_quarantine_path": copied_path,
                "decision": "exclude",
                "reason_codes": decision.reason_codes,
                "rule_confidence": decision.rule_confidence,
                "signals": decision.signals,
                "created_at": _utc_now_iso(),
                "qc_version": QC_VERSION,
            }
        )

    _write_jsonl_atomic(manifest_path, exclusion_rows)

    report_payload = {
        "created_at": _utc_now_iso(),
        "qc_version": QC_VERSION,
        "ruleset_hash": _compute_ruleset_hash(),
        "pdf_root": pdf_root,
        "quarantine_root": quarantine_root,
        "manifest_path": manifest_path,
        "dry_run": config.dry_run,
        "scanned": len(seen_paths),
        "excluded": len(exclusion_rows),
        "kept": kept,
        "copied": copied,
        "rule_counts": rule_counts,
    }
    _write_json_atomic(report_path, report_payload)

    return QCRunResult(
        report_path=report_path,
        manifest_path=manifest_path,
        scanned=len(seen_paths),
        excluded=len(exclusion_rows),
        kept=kept,
        copied=copied,
        dry_run=config.dry_run,
        rule_counts=rule_counts,
    )
