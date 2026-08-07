"""Prototype issue-compilation PDF splitter.

The splitter is intentionally conservative: it only writes child PDFs when a table
of contents exposes monotonic page references and the physical/printed page offset
can be inferred from early article pages. Uncertain PDFs are skipped with reasons.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import csv
import re
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class ArticleBoundary:
    """One inferred child-article span using one-based inclusive page numbers."""

    start_page: int
    end_page: int
    method: str
    confidence: float
    title_guess: str


@dataclass(frozen=True)
class BoundaryInference:
    boundaries: list[ArticleBoundary]
    method: str
    confidence: float
    skip_reason: str = ""

    @property
    def ok(self) -> bool:
        return bool(self.boundaries) and not self.skip_reason


@dataclass(frozen=True)
class UniquePdf:
    path: Path
    sha256: str


@dataclass(frozen=True)
class DuplicatePdf:
    path: Path
    sha256: str
    duplicate_of: Path


@dataclass(frozen=True)
class DedupeResult:
    unique: list[UniquePdf]
    duplicates: list[DuplicatePdf]


@dataclass(frozen=True)
class SplitConfig:
    pdf_root: str | Path
    output_root: str | Path = "artifacts/pdfs_split"
    runs_dir: str | Path = "artifacts/runs"
    domain_filter: str = ""
    limit: int = 0
    candidate_file: str | Path = ""
    candidate_issue_only: bool = False
    candidate_min_priority: float = 0.0


_TOC_RE = re.compile(r"\b(?:TABLE\s+OF\s+CONTENTS|CONTENTS)\b", re.I)
_PAGE_REF_RE = re.compile(r"\b[Pp]age\s+(\d{1,4})\b")
_PRINTED_NUMBER_RE = re.compile(r"^\s*(\d{1,4})\s*$")
_SPACE_RE = re.compile(r"\s+")
_AUTHOR_LINE_RE = re.compile(
    r"^(?:[A-Z][A-Za-z'`\-.]+|[A-Z]\.?)"
    r"(?:\s+(?:[A-Z][A-Za-z'`\-.]+|[A-Z]\.?)){1,6}\*?$"
)
_SECTION_LINE_RE = re.compile(
    r"^(?:ARTICLES?|CASE\s+NOTES?|NOTES?|COMMENTS?|BOOK\s+REVIEWS?|"
    r"GUIDELINES\s+FOR\s+CONTRIBUTORS|THE\s+STRONACHS.?\s+PRIZE)$",
    re.I,
)
_JOURNAL_HEADER_RE = re.compile(
    r"(?:LAW\s+REVIEW|LAW\s+JOURNAL|JOURNAL\s+OF|REVIEW:)\b.*(?:VOLUME|VOL\.?\s*\d)",
    re.I,
)
_FOOTNOTEISH_RE = re.compile(r"^\d{1,3}\s+")
_HEADINGISH_RE = re.compile(r"^[A-Z][A-Z0-9 ,;:'\"()\\-]{14,}$")


@dataclass(frozen=True)
class CandidateEntry:
    path: Path
    domain: str
    provided_sha256: str = ""


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def deduplicate_pdf_paths(paths: Iterable[str | Path]) -> DedupeResult:
    """Deduplicate PDF paths by SHA256, preserving first-seen order."""

    seen: dict[str, Path] = {}
    unique: list[UniquePdf] = []
    duplicates: list[DuplicatePdf] = []

    for raw_path in paths:
        path = Path(raw_path)
        digest = sha256_file(path)
        first = seen.get(digest)
        if first is not None:
            duplicates.append(DuplicatePdf(path=path, sha256=digest, duplicate_of=first))
            continue
        seen[digest] = path
        unique.append(UniquePdf(path=path, sha256=digest))

    return DedupeResult(unique=unique, duplicates=duplicates)


def deduplicate_candidates(candidates: Iterable[CandidateEntry]) -> DedupeResult:
    """Deduplicate candidate entries by provided SHA256 (if present) or file SHA256."""

    seen: dict[str, Path] = {}
    unique: list[UniquePdf] = []
    duplicates: list[DuplicatePdf] = []
    for item in candidates:
        digest = item.provided_sha256.strip().lower() or sha256_file(item.path)
        first = seen.get(digest)
        if first is not None:
            duplicates.append(DuplicatePdf(path=item.path, sha256=digest, duplicate_of=first))
            continue
        seen[digest] = item.path
        unique.append(UniquePdf(path=item.path, sha256=digest))
    return DedupeResult(unique=unique, duplicates=duplicates)


def _as_float(raw: Any, default: float = 0.0) -> float:
    try:
        if raw is None:
            return default
        return float(str(raw).strip())
    except (TypeError, ValueError):
        return default


def _as_int(raw: Any, default: int = 0) -> int:
    try:
        if raw is None:
            return default
        return int(float(str(raw).strip()))
    except (TypeError, ValueError):
        return default


def _is_issue_like_candidate(row: dict[str, str], pdf_path: Path) -> bool:
    name = pdf_path.name.lower()
    heuristics = (row.get("heuristics") or "").strip().lower()
    pages = _as_int(row.get("pages"), default=0)
    priority = _as_float(row.get("priority"), default=0.0)

    if any(
        token in name
        for token in (
            "table-of-contents",
            "table_of_contents",
            "toc",
            "front-matter",
            "front_matter",
            "contents-only",
        )
    ):
        return False

    if any(
        marker in heuristics
        for marker in (
            "filename:strong_issue_token",
            "filename:vol_issue_pattern",
            "filename:token",
            "pages:>120",
        )
    ):
        return True

    if pages >= 120 or priority >= 6.0:
        return True

    return bool(
        re.search(
            r"(?:full[-_ ]issue|complete[-_ ]issue|vol(?:ume)?[-_ ]?\d+|issue[-_ ]?\d+|book)",
            name,
            flags=re.I,
        )
    )


def load_candidates_from_tsv(
    candidate_file: str | Path,
    domain_filter: str = "",
    *,
    issue_only: bool = False,
    min_priority: float = 0.0,
) -> list[CandidateEntry]:
    path = Path(candidate_file)
    if not path.exists():
        return []
    needle = domain_filter.strip().lower()
    out: list[CandidateEntry] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            raw_pdf = (row.get("pdf_path") or "").strip()
            if not raw_pdf:
                continue
            pdf_path = Path(raw_pdf)
            if not pdf_path.exists():
                continue
            domain = (row.get("domain") or "").strip() or pdf_path.parent.name
            if needle and needle not in domain.lower() and needle not in str(pdf_path).lower():
                continue
            if min_priority > 0 and _as_float(row.get("priority"), default=0.0) < float(min_priority):
                continue
            if issue_only and not _is_issue_like_candidate(row, pdf_path):
                continue
            provided_sha = (row.get("sha256") or "").strip()
            out.append(CandidateEntry(path=pdf_path, domain=domain, provided_sha256=provided_sha))
    return out


def iter_pdf_candidates(pdf_root: str | Path, domain_filter: str = "") -> list[Path]:
    root = Path(pdf_root)
    if root.is_file():
        candidates = [root] if root.suffix.lower() == ".pdf" else []
    else:
        needle = domain_filter.strip()
        search_roots = [root]
        if needle:
            direct_domain_dir = root / needle
            if direct_domain_dir.is_dir():
                search_roots = [direct_domain_dir]
            else:
                lowered = needle.lower()
                matching_children = [
                    child
                    for child in root.iterdir()
                    if child.is_dir() and lowered in child.name.lower()
                ]
                if matching_children:
                    search_roots = sorted(matching_children)

        candidates = sorted(
            path for search_root in search_roots for path in search_root.rglob("*.pdf") if path.is_file()
        )

    needle_lower = domain_filter.strip().lower()
    if not needle_lower:
        return candidates
    return [path for path in candidates if needle_lower in str(path).lower()]


def infer_domain(pdf_path: str | Path, pdf_root: str | Path) -> str:
    path = Path(pdf_path)
    root = Path(pdf_root)
    try:
        rel = path.relative_to(root)
    except ValueError:
        return path.parent.name or "unknown"
    if len(rel.parts) > 1:
        return rel.parts[0]
    if root.name:
        return root.name
    return "unknown"


def infer_article_boundaries(page_texts: list[str]) -> BoundaryInference:
    """Infer article spans from TOC page references and early article-page clues.

    Returns one-based inclusive spans. The caller should skip when ``ok`` is false.
    """

    total_pages = len(page_texts)
    if total_pages < 3:
        return BoundaryInference([], "toc_page_refs", 0.0, "too_few_pages")

    toc_indices = _find_toc_indices(page_texts)
    if not toc_indices:
        fallback = _infer_boundaries_from_headings(page_texts)
        if fallback.ok:
            return fallback
        return BoundaryInference([], "toc_page_refs", 0.0, "toc_not_found")

    page_refs = _extract_toc_page_refs(page_texts, toc_indices)
    if len(page_refs) < 2:
        fallback = _infer_boundaries_from_headings(page_texts)
        if fallback.ok:
            return fallback
        return BoundaryInference([], "toc_page_refs", 0.0, "too_few_toc_page_refs")

    offset, offset_method = _infer_page_offset(page_texts, toc_indices, page_refs[0])
    if offset is None:
        return BoundaryInference([], "toc_page_refs", 0.0, "page_offset_not_inferred")

    starts: list[int] = []
    for page_ref in page_refs:
        physical_page = page_ref + offset
        if 1 <= physical_page <= total_pages and (not starts or physical_page > starts[-1]):
            starts.append(physical_page)

    if len(starts) < 2:
        return BoundaryInference([], "toc_page_refs", 0.0, "too_few_valid_boundaries")

    method = f"toc_page_refs+{offset_method}"
    confidence = 0.86 if offset_method == "printed_page_number" else 0.76
    boundaries: list[ArticleBoundary] = []
    for idx, start_page in enumerate(starts):
        end_page = starts[idx + 1] - 1 if idx + 1 < len(starts) else total_pages
        if end_page < start_page:
            continue
        title_guess = guess_title_from_article_page(page_texts[start_page - 1])
        boundaries.append(
            ArticleBoundary(
                start_page=start_page,
                end_page=end_page,
                method=method,
                confidence=confidence,
                title_guess=title_guess,
            )
        )

    if len(boundaries) < 2:
        return BoundaryInference([], method, 0.0, "too_few_article_spans")
    return BoundaryInference(boundaries, method, confidence)


def guess_title_from_article_page(page_text: str) -> str:
    lines = _clean_lines(page_text)
    if not lines:
        return ""

    abstract_idx = _first_line_index(lines, {"abstract"})
    if abstract_idx is not None:
        window = lines[max(0, abstract_idx - 10) : abstract_idx]
    else:
        window = lines[:12]

    filtered: list[str] = []
    for line in window:
        cleaned = _normalize_line(line).strip(" ,;:")
        if not cleaned:
            continue
        if _PRINTED_NUMBER_RE.match(cleaned):
            continue
        if _JOURNAL_HEADER_RE.search(cleaned):
            continue
        if _SECTION_LINE_RE.match(cleaned):
            continue
        if _FOOTNOTEISH_RE.match(cleaned):
            continue
        if re.search(r"\b(?:Oxford University Press|Law Commission|Act \d{4}|accessed)\b", cleaned):
            continue
        filtered.append(cleaned)

    if filtered and _looks_like_author_line(filtered[-1]):
        filtered = filtered[:-1]
    if not filtered:
        return ""

    title = " ".join(filtered[-5:])
    title = _SPACE_RE.sub(" ", title).strip()
    return title[:240]


def split_pdf(
    parent_pdf: str | Path,
    parent_sha256: str,
    domain: str,
    output_root: str | Path,
) -> tuple[list[dict[str, Any]], str]:
    """Split one parent PDF and return manifest rows plus a skip reason if skipped."""

    from pypdf import PdfReader, PdfWriter

    parent_path = Path(parent_pdf)
    try:
        reader = PdfReader(str(parent_path))
        page_texts = [(page.extract_text() or "") for page in reader.pages]
    except Exception as exc:  # pragma: no cover - depends on parser failure details
        return ([_skip_row(parent_path, parent_sha256, domain, f"pdf_read_failed:{exc}")], f"pdf_read_failed:{exc}")

    inference = infer_article_boundaries(page_texts)
    if not inference.ok:
        return ([_skip_row(parent_path, parent_sha256, domain, inference.skip_reason)], inference.skip_reason)

    parent_dir = Path(output_root) / _safe_path_part(domain) / _safe_path_part(parent_path.stem)
    parent_dir.mkdir(parents=True, exist_ok=True)
    for stale_child in parent_dir.glob("article_*.pdf"):
        stale_child.unlink()

    rows: list[dict[str, Any]] = []
    for idx, boundary in enumerate(inference.boundaries, start=1):
        child_path = parent_dir / f"article_{idx:03d}_p{boundary.start_page}-{boundary.end_page}.pdf"
        writer = PdfWriter()
        for page_idx in range(boundary.start_page - 1, boundary.end_page):
            writer.add_page(reader.pages[page_idx])
        with child_path.open("wb") as handle:
            writer.write(handle)

        rows.append(
            {
                "parent_pdf_path": str(parent_path),
                "parent_sha256": parent_sha256,
                "child_pdf_path": str(child_path),
                "start_page": boundary.start_page,
                "end_page": boundary.end_page,
                "method": boundary.method,
                "confidence": boundary.confidence,
                "title_guess": boundary.title_guess,
                "domain": domain,
            }
        )

    return rows, ""


def run_issue_split(config: SplitConfig) -> dict[str, Any]:
    pdf_root = Path(config.pdf_root)
    output_root = Path(config.output_root)
    runs_dir = Path(config.runs_dir)
    runs_dir.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)

    candidate_file = str(config.candidate_file).strip() if config.candidate_file else ""
    domain_by_path: dict[Path, str] = {}
    if candidate_file:
        loaded = load_candidates_from_tsv(
            candidate_file,
            config.domain_filter,
            issue_only=bool(config.candidate_issue_only),
            min_priority=float(config.candidate_min_priority or 0.0),
        )
        for item in loaded:
            domain_by_path[item.path] = item.domain
        dedupe = deduplicate_candidates(loaded)
        candidates = [item.path for item in loaded]
    else:
        candidates = iter_pdf_candidates(pdf_root, config.domain_filter)
        dedupe = deduplicate_pdf_paths(candidates)
    unique = dedupe.unique[: config.limit] if config.limit and config.limit > 0 else dedupe.unique

    manifest_path = runs_dir / f"issue_split_manifest_{utc_stamp()}.jsonl"
    stats = {
        "pdf_root": str(pdf_root),
        "candidate_file": candidate_file,
        "candidate_issue_only": bool(config.candidate_issue_only),
        "candidate_min_priority": float(config.candidate_min_priority or 0.0),
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "candidates": len(candidates),
        "unique_candidates": len(dedupe.unique),
        "duplicates_skipped": len(dedupe.duplicates),
        "processed": 0,
        "parents_split": 0,
        "parents_skipped": 0,
        "children_written": 0,
    }

    with manifest_path.open("w", encoding="utf-8") as manifest:
        for item in unique:
            domain = domain_by_path.get(item.path) or infer_domain(item.path, pdf_root)
            rows, skip_reason = split_pdf(item.path, item.sha256, domain, output_root)
            stats["processed"] += 1
            if skip_reason:
                stats["parents_skipped"] += 1
            else:
                stats["parents_split"] += 1
                stats["children_written"] += len(rows)
            for row in rows:
                manifest.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")

    return stats


def _find_toc_indices(page_texts: list[str]) -> list[int]:
    for idx, text in enumerate(page_texts[: min(len(page_texts), 25)]):
        lines = _clean_lines(text or "")
        if any(_TOC_RE.fullmatch(line) for line in lines):
            indices = [idx]
            next_idx = idx + 1
            if next_idx < len(page_texts):
                next_text = page_texts[next_idx] or ""
                if len(_PAGE_REF_RE.findall(next_text)) >= 2 and "abstract" not in next_text.lower():
                    indices.append(next_idx)
            return indices
    return []


def _extract_toc_page_refs(page_texts: list[str], toc_indices: list[int]) -> list[int]:
    start = min(toc_indices)
    end = min(len(page_texts), max(toc_indices) + 2)
    text = "\n".join(page_texts[start:end])
    refs: list[int] = []
    seen: set[int] = set()
    for match in _PAGE_REF_RE.finditer(text):
        ref = int(match.group(1))
        if ref <= 0 or ref in seen:
            continue
        if refs and ref <= refs[-1]:
            continue
        seen.add(ref)
        refs.append(ref)
    return refs


def _infer_page_offset(
    page_texts: list[str], toc_indices: list[int], first_page_ref: int
) -> tuple[int | None, str]:
    search_start = max(toc_indices) + 1
    search_end = min(len(page_texts), search_start + 20)

    for idx in range(search_start, search_end):
        if _has_printed_page_number(page_texts[idx], first_page_ref):
            return (idx + 1 - first_page_ref, "printed_page_number")

    for idx in range(search_start, search_end):
        text = page_texts[idx] or ""
        lowered = text.lower()
        if "abstract" in lowered or "keywords" in lowered or "1. introduction" in lowered:
            return (idx + 1 - first_page_ref, "first_article_text")

    return (None, "")


def _has_printed_page_number(page_text: str, expected: int) -> bool:
    expected_text = str(expected)
    for line in _clean_lines(page_text)[:12]:
        match = _PRINTED_NUMBER_RE.match(line)
        if match and match.group(1) == expected_text:
            return True
    return False


def _clean_lines(text: str) -> list[str]:
    return [_normalize_line(line) for line in (text or "").replace("\x00", "").splitlines() if line.strip()]


def _normalize_line(line: str) -> str:
    return _SPACE_RE.sub(" ", line).strip()


def _first_line_index(lines: list[str], needles: set[str]) -> int | None:
    for idx, line in enumerate(lines):
        if line.strip().lower().rstrip(":") in needles:
            return idx
    return None


def _looks_like_author_line(line: str) -> bool:
    if line.isupper() and "*" not in line:
        return False
    return bool(_AUTHOR_LINE_RE.match(line))


def _safe_path_part(raw: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", raw.strip())
    cleaned = cleaned.strip("._-")
    return cleaned or "unknown"


def _infer_boundaries_from_headings(page_texts: list[str]) -> BoundaryInference:
    """Fallback when TOC parsing fails.

    Heuristic: detect likely article-start pages by heading-like first lines and
    optional ABSTRACT cue, then split by start-page deltas.
    """

    starts: list[int] = []
    total_pages = len(page_texts)
    for idx, text in enumerate(page_texts, start=1):
        lines = _clean_lines(text)
        if not lines:
            continue
        top = lines[:16]
        first = top[0] if top else ""
        has_abs = any(line.strip().lower().startswith("abstract") for line in top[:10])
        headingish = bool(_HEADINGISH_RE.match(first)) and not _JOURNAL_HEADER_RE.search(first)
        # Page likely begins a new article if heading-like and abstract appears soon after.
        if headingish and has_abs:
            starts.append(idx)

    # prune near-duplicates
    pruned: list[int] = []
    for s in starts:
        if not pruned or s - pruned[-1] >= 4:
            pruned.append(s)
    if len(pruned) < 2:
        return BoundaryInference([], "heading_fallback", 0.0, "heading_fallback_insufficient")

    boundaries: list[ArticleBoundary] = []
    for i, start in enumerate(pruned):
        end = pruned[i + 1] - 1 if i + 1 < len(pruned) else total_pages
        if end < start:
            continue
        boundaries.append(
            ArticleBoundary(
                start_page=start,
                end_page=end,
                method="heading_fallback+abstract",
                confidence=0.58,
                title_guess=guess_title_from_article_page(page_texts[start - 1]),
            )
        )
    if len(boundaries) < 2:
        return BoundaryInference([], "heading_fallback", 0.0, "heading_fallback_insufficient")
    return BoundaryInference(boundaries, "heading_fallback+abstract", 0.58)


def _skip_row(parent_path: Path, parent_sha256: str, domain: str, reason: str) -> dict[str, Any]:
    return {
        "parent_pdf_path": str(parent_path),
        "parent_sha256": parent_sha256,
        "child_pdf_path": "",
        "start_page": None,
        "end_page": None,
        "method": "skipped",
        "confidence": 0.0,
        "title_guess": "",
        "domain": domain,
        "skip_reason": reason,
    }


# ---------------------------------------------------------------------------
# US law-review boundary inference (added 2026-08-06)
#
# `infer_article_boundaries` above was built against Aberdeen Student Law
# Review house style: it needs a line reading exactly "TABLE OF CONTENTS" and
# page references spelled "Page 139". Measured over 80 randomly sampled
# >=50pp issue PDFs from corpus/scraped, it produced boundaries for 3 of them
# (4%), and each of those 3 came from the low-confidence heading fallback --
# one 674-page volume was split into two articles.
#
# US law reviews do not write TOCs that way. What they do have is a running
# head that changes at every article boundary. The forms vary by journal but
# all carry a per-article token:
#
#   University of Hawai'i Law Review / Vol. 32:359   <- article start page
#   2022] Don't Be Afraid of Trial                   <- article title
#   50 Klein                                         <- author surname
#   KAMINSKI_FINALPROOF_07-20-22 (DO NOT DELETE)     <- production slug
#   2mohan (Do Not Delete)3/30/2014 8:26 AM          <- production slug
#
# So boundaries are found by detecting where that signature changes, and are
# cross-checked against a TOC parser that reads the trailing-page-number form
# US journals actually use ("Title / Author .... 139").
# ---------------------------------------------------------------------------

# Verso and recto carry DIFFERENT running heads, so a naive page-to-page
# comparison reports a change on every single page. Signatures are always
# compared within a parity stream.
_HEAD_DIGIT_RE = re.compile(r"\d+")
_HEAD_NOISE_RE = re.compile(r"[^A-Z ]+")
_VOL_START_RE = re.compile(r"\bVol(?:ume)?\.?\s*\d{1,3}\s*[:.;]\s*(\d{1,4})\b", re.I)
_TOC_ENTRY_RE = re.compile(r"^(?P<label>.*?\S)\s*[.…\s]{2,}\s*(?P<page>\d{1,4})$")
_TRAILING_PAGE_RE = re.compile(r"^(?P<label>.*?\S)\s+(?P<page>\d{1,4})$")
_CAPITALISED_TOKEN_RE = re.compile(r"\b[A-Z][A-Za-z'`.-]*")


def head_signature(page_text: str) -> str:
    """Normalise a page's running head into a comparable signature.

    Digits go first: folios change on every page and would otherwise make each
    page look like a new article. What survives is the alphabetic core --
    journal name, article title, author, or production slug.
    """
    lines = _clean_lines(page_text)
    if not lines:
        return ""
    head = lines[0].upper()
    head = _HEAD_DIGIT_RE.sub(" ", head)
    head = _HEAD_NOISE_RE.sub(" ", head)
    return _SPACE_RE.sub(" ", head).strip()


def _signature_similarity(left: str, right: str) -> float:
    """Token-set Jaccard. Tolerates the character noise scanned text carries."""
    left_tokens = {token for token in left.split() if len(token) > 2}
    right_tokens = {token for token in right.split() if len(token) > 2}
    if not left_tokens and not right_tokens:
        return 1.0
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    return intersection / union if union else 0.0


def article_start_pages_from_heads(page_texts: list[str]) -> list[int]:
    """Boundaries from the `Vol. 33:139` form, which names the article's start.

    This is the most reliable variant available: the header does not merely
    change at a boundary, it states which page the current article began on,
    so the boundary needs no inference at all.
    """
    observed: list[tuple[int, int]] = []
    for index, text in enumerate(page_texts, start=1):
        lines = _clean_lines(text)
        if not lines:
            continue
        window = " ".join(lines[:2]) + " " + " ".join(lines[-2:])
        match = _VOL_START_RE.search(window)
        if match:
            observed.append((index, int(match.group(1))))
    if len({start for _, start in observed}) < 2:
        return []

    # First physical page on which each printed start-page value appears.
    first_physical: dict[int, int] = {}
    for physical, start in observed:
        first_physical.setdefault(start, physical)

    # The header naming article N appears on its SECOND page at the earliest:
    # an article's opening page carries a drop title, not a running head.
    starts = sorted(max(1, physical - 1) for physical in first_physical.values())
    return _prune_starts(starts)


def change_points_from_heads(page_texts: list[str], threshold: float = 0.4) -> list[int]:
    """Boundaries from any running head that changes between articles.

    Compares each page against the previous page of the SAME parity, then
    keeps only changes corroborated by the other parity nearby -- a single
    stream flips on section breaks and full-page tables too.
    """
    signatures = [head_signature(text) for text in page_texts]
    raw: list[int] = []
    for index in range(2, len(signatures)):
        current, previous = signatures[index], signatures[index - 2]
        if not current or not previous:
            continue
        if _signature_similarity(current, previous) < threshold:
            raw.append(index + 1)
    if not raw:
        return []

    corroborated = [
        page
        for page in raw
        if any(other != page and abs(other - page) <= 2 for other in raw)
    ]
    # Report the earlier page of each corroborated verso/recto pair.
    return _prune_starts(sorted({min(page, page - 1) for page in corroborated}))


def _prune_starts(starts: Iterable[int], min_gap: int = 4) -> list[int]:
    """Drop boundaries closer together than a real article can be."""
    pruned: list[int] = []
    for start in sorted(set(starts)):
        if start < 1:
            continue
        if not pruned or start - pruned[-1] >= min_gap:
            pruned.append(start)
    return pruned


def parse_toc_printed_starts(page_texts: list[str], scan_pages: int = 12) -> list[int]:
    """Read printed start pages from a US-style contents listing.

    Entries look like `Remedies for the Wrongly Deported ... 139` or
    `Rachel E. Rosenbloom      139` -- a label then a trailing page number,
    with or without dot leaders. The sequence must be increasing; that is what
    separates a contents listing from a page of prose ending in a numeral.
    """
    best: list[int] = []
    for text in page_texts[:scan_pages]:
        found: list[int] = []
        for line in _clean_lines(text):
            leader_match = _TOC_ENTRY_RE.match(line)
            match = leader_match or _TRAILING_PAGE_RE.match(line)
            if not match:
                continue
            label = match.group("label").strip()
            if len(label) < 4 or not re.search(r"[A-Za-z]{3}", label):
                continue
            # Without dot leaders a trailing number is weak evidence: a line of
            # prose ending "...amended in 2019" parses identically. Contents
            # entries are titles and author names, so require the label to
            # carry at least two capitalised words.
            if leader_match is None and len(_CAPITALISED_TOKEN_RE.findall(label)) < 2:
                continue
            page = int(match.group("page"))
            if page <= 0 or (found and page <= found[-1]):
                continue
            found.append(page)
        if len(found) > len(best):
            best = found
    return best if len(best) >= 2 else []


def infer_law_review_boundaries(
    page_texts: list[str],
    domain: str = "",
    head_rules: dict[str, Any] | None = None,
) -> BoundaryInference:
    """Boundary inference for US law-review issue compilations.

    Runs the header and contents signals independently and requires them to
    agree before emitting a high-confidence split. Precision is the priority:
    a wrong boundary silently truncates one article and prepends its tail to
    the next, and both then enter the citation graph as real documents.
    """
    total_pages = len(page_texts)
    if total_pages < 20:
        return BoundaryInference([], "law_review", 0.0, "too_few_pages")

    rule = ((head_rules or {}).get("domains") or {}).get(domain.lower()) or {}
    if rule.get("kind") == "single_article_domain":
        # Validated as per-article PDFs misclassified upstream, not compilations.
        return BoundaryInference([], "domain_rule", 0.0, "single_article_domain")

    explicit = article_start_pages_from_heads(page_texts)
    changed = change_points_from_heads(page_texts)
    toc_printed = parse_toc_printed_starts(page_texts)
    by_rule = (
        boundaries_from_domain_rule(page_texts, rule)
        if rule.get("kind") == "pattern"
        else []
    )

    if explicit and len(explicit) >= 2:
        starts, method, confidence = explicit, "running_head_vol_start", 0.92
    elif by_rule and len(by_rule) >= 2:
        starts, method, confidence = by_rule, "domain_head_rule", 0.85
    elif changed and len(changed) >= 2:
        # Change detection alone is NOT safe to split on. Measured against
        # hand-read output: it fired on 29 of 80 sampled issues and put the
        # boundaries mid-article, at a near-constant 4-page period -- a full
        # page of footnote continuation or a landscape table displaces the
        # running head, which reads as a change. One Law & Inequality volume
        # came back cut every four pages inside a single article.
        #
        # A wrong boundary is worse than no boundary: it truncates one article
        # and prepends its tail to the next, and both then enter the citation
        # graph as genuine documents. So this signal is reported for triage and
        # never emitted. Lifting these journals needs a per-domain pattern, not
        # a better global threshold.
        return BoundaryInference(
            [], "running_head_change", 0.0, "change_signal_only_unverified"
        )
    else:
        return BoundaryInference([], "law_review", 0.0, "no_running_head_signal")

    # A contents listing that counts the same number of articles corroborates
    # the header signal; the counts rarely match exactly because tributes and
    # book reviews are listed but not always separately headed.
    if toc_printed:
        ratio = len(toc_printed) / max(len(starts), 1)
        if 0.6 <= ratio <= 1.7:
            confidence = min(0.97, confidence + 0.1)
            method = f"{method}+toc_agreement"

    if len(starts) > 80:
        return BoundaryInference([], method, 0.0, "implausible_article_count")

    # Every boundary must land on a page that actually opens an article. A
    # per-domain pattern validated against one issue can latch onto body text
    # in another issue of the same journal and cut it every eight pages; the
    # signal that produced the boundary cannot detect that, but the target page
    # can. When most boundaries fail, the signal is not tracking articles here.
    starts, opening_share = validate_boundary_starts(page_texts, starts)
    if opening_share < 0.6:
        return BoundaryInference(
            [], method, 0.0, f"boundaries_not_article_openings:{opening_share:.2f}"
        )
    confidence = min(confidence, 0.6 + 0.35 * opening_share)

    if len(starts) < 2:
        return BoundaryInference([], method, 0.0, "too_few_article_spans")

    boundaries: list[ArticleBoundary] = []
    for index, start_page in enumerate(starts):
        end_page = starts[index + 1] - 1 if index + 1 < len(starts) else total_pages
        if end_page < start_page:
            continue
        boundaries.append(
            ArticleBoundary(
                start_page=start_page,
                end_page=end_page,
                method=method,
                confidence=confidence,
                title_guess=guess_title_from_article_page(page_texts[start_page - 1]),
            )
        )
    if len(boundaries) < 2:
        return BoundaryInference([], method, 0.0, "too_few_article_spans")
    return BoundaryInference(boundaries, method, confidence)


# ---------------------------------------------------------------------------
# Per-domain running-head rules
#
# Most journals' heads carry a per-article token that no global pattern can
# find: an article title, an author surname, or a typesetting slug. Those are
# journal-level conventions, stable across every issue a journal published, so
# they are described once per domain in `issue_head_rules.json` rather than
# inferred per issue.
# ---------------------------------------------------------------------------

HEAD_RULES_PATH = Path(__file__).with_name("issue_head_rules.json")

# Minimum pages an article can occupy. Heads degrade in ways that flip the key
# for a page or two -- a `The`/`the` case flip, a front-matter region
# alternating between two export timestamps -- and each flip would otherwise
# emit a 1-2 page phantom article.
_MIN_ARTICLE_PAGES = 4


def load_head_rules(path: str | Path | None = None) -> dict[str, Any]:
    rule_path = Path(path or HEAD_RULES_PATH)
    if not rule_path.exists():
        return {"version": "0", "domains": {}}
    try:
        data = json.loads(rule_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"version": "0", "domains": {}}
    if not isinstance(data, dict) or not isinstance(data.get("domains"), dict):
        return {"version": "0", "domains": {}}
    return data


def _candidate_head_lines(page_text: str, which: str) -> list[str]:
    lines = _clean_lines(page_text)
    if not lines:
        return []
    if which == "last":
        return [lines[-1]]
    if which == "both":
        return [lines[0], lines[-1]] if len(lines) > 1 else [lines[0]]
    return [lines[0]]


def _normalise_key(raw: str) -> str:
    """Case- and punctuation-insensitive, so a `The`/`the` flip is not a boundary."""
    return _SPACE_RE.sub(" ", re.sub(r"[^A-Za-z0-9 ]+", " ", raw)).strip().upper()


def article_keys_for_pages(
    page_texts: list[str], rule: dict[str, Any]
) -> list[str | None]:
    """Per-page article key, with unmatched pages inheriting the previous one.

    Inheritance is the whole point. A full-page footnote run or a landscape
    table displaces the running head, and treating that as a new article was
    the bug that cut volumes every four pages.
    """
    patterns = []
    for raw in rule.get("article_key_patterns") or []:
        try:
            patterns.append(re.compile(raw))
        except re.error:
            continue
    which = str(rule.get("head_lines") or "first")

    keys: list[str | None] = []
    current: str | None = None
    for text in page_texts:
        matched: str | None = None
        for line in _candidate_head_lines(text, which):
            for pattern in patterns:
                found = pattern.search(line)
                if found:
                    try:
                        candidate = found.group("key")
                    except IndexError:  # pattern authored without a `key` group
                        candidate = ""
                    candidate = _normalise_key(candidate or "")
                    if candidate:
                        matched = candidate
                        break
            if matched:
                break
        if matched:
            current = matched
        keys.append(current)
    return keys


def boundaries_from_domain_rule(
    page_texts: list[str], rule: dict[str, Any]
) -> list[int]:
    """Start pages where the per-article key changes.

    Each boundary is backed off by one page. Where the head carries the article
    title, the article's OWN first page shows the display title instead of a
    running head, so the key does not change until page two. Without the
    back-off every child would lose its title page -- which is the page the
    downstream metadata pass reads title and author from.
    """
    keys = article_keys_for_pages(page_texts, rule)
    starts: list[int] = []
    previous: str | None = None
    for index, key in enumerate(keys, start=1):
        if key is None:
            continue
        if previous is not None and key != previous:
            starts.append(max(1, index - 1))
        previous = key
    if starts and starts[0] > 1:
        starts.insert(0, 1)
    return _prune_starts(starts, min_gap=_MIN_ARTICLE_PAGES)


# A running head, which sits above the body on a continuation page. Dropped
# before judging whether a page opens an article.
_RUNNING_HEAD_RE = re.compile(
    r"\[?\s*Vol(?:ume)?\.?\s*\d|"
    r"(?:LAW\s+REVIEW|LAW\s+JOURNAL|JOURNAL\s+OF|L\.\s*REV\.|"
    r"do\s+not\s+delete)",
    re.IGNORECASE,
)
# The same shapes after every space is removed. Extractors letter-space small
# caps -- `550 J OURNAL OF LAW, ECONOMICS & POLICY [V OL. 8:3` -- and the
# spaced form matches none of the patterns above, so the head survived into the
# body and its own shouted text read as a display title.
_RUNNING_HEAD_DESPACED_RE = re.compile(
    r"LAWREVIEW|LAWJOURNAL|JOURNALOF|L\.REV\.|DONOTDELETE|VOL(?:UME)?\.\d",
    re.IGNORECASE,
)
# The Word production slug some journals leave in the head: it always ends in a
# clock time. `4RATHREV1.DOCX 5/26/2011 5:18 PM` shouts like a title, and left
# in place it kept the real head below it from ever being examined.
_PRODUCTION_SLUG_RE = re.compile(r"\d{1,2}:\d{2}(:\d{2})?\s*[AP]\.?M\.?$", re.IGNORECASE)
# `2013 / STABILIZING DEMOCRACY`, `2012] THE FULL FEDERAL REGULATORY PURPOSE`.
# A year followed by a separator is a running head in every US law review seen
# here, and no display title opens that way.
_YEAR_HEAD_RE = re.compile(r"^\[?\s*\d{4}\s*[/\]|]")
_BARE_FOLIO_RE = re.compile(r"^\W*\d{1,4}\W*$")
# The other running-head shape: the article's shouted short title followed by
# its folio, sometimes letter-spaced by the extractor ("CENTERED 1 0 3",
# "BY THE NUMBERS 2 9"). Without this it reads as a display title and every
# continuation page looks like an article opening.
_TITLE_FOLIO_HEAD_RE = re.compile(r"^[A-Z][A-Z\s’'&,:.-]{2,60}?[\s\d]{1,14}$")

# A line of body prose resumed from the previous page: it starts lower case and
# runs the full measure. The width test is what separates it from a lower-case
# author line ("e. christi cunningham †"), which does open an article.
_RESUMED_PROSE_MIN_CHARS = 40


def _is_resumed_prose(line: str) -> bool:
    return bool(line) and line[:1].islower() and len(line) >= _RESUMED_PROSE_MIN_CHARS


def _upper_share(line: str) -> float:
    letters = [character for character in line if character.isalpha()]
    if not letters:
        return 0.0
    return sum(1 for character in letters if character.isupper()) / len(letters)


def _is_shouted(line: str) -> bool:
    """A short, mostly-capitalised line: either a display title or a head."""
    return len(line) <= 90 and _upper_share(line) >= 0.7


def _is_running_head_line(line: str) -> bool:
    if _BARE_FOLIO_RE.match(line) or _RUNNING_HEAD_RE.search(line):
        return True
    if _TITLE_FOLIO_HEAD_RE.match(line) or _PRODUCTION_SLUG_RE.search(line):
        return True
    if _YEAR_HEAD_RE.match(line):
        return True
    return bool(_RUNNING_HEAD_DESPACED_RE.search(re.sub(r"\s+", "", line)))


def _looks_like_sentence_prose(line: str) -> bool:
    """A full measure of running text, whatever case its first word is in.

    Resumed prose does not always start lower case -- a new sentence, or a
    proper noun carried over, begins with a capital. What still separates it
    from a display title is that a title capitalises most of its words and
    prose capitalises almost none.
    """
    if len(line) < 55:
        return False
    words = [word for word in re.findall(r"[A-Za-z][A-Za-z’'\-]*", line) if len(word) > 2]
    if len(words) < 6:
        return False
    capitalised = sum(1 for word in words if word[:1].isupper())
    return capitalised / len(words) <= 0.34


def _has_wordlike_content(line: str) -> bool:
    """Reject OCR debris such as `I-'- ' 'S`, which shouts but says nothing."""
    dense = re.sub(r"\s+", "", line)
    letters = [character for character in dense if character.isalpha()]
    return len(letters) >= 6 and len(letters) / max(len(dense), 1) >= 0.5


def looks_like_article_opening(page_text: str) -> bool:
    """True when a page starts an article rather than continuing one.

    This is the check that separates a real boundary from a pattern that has
    latched onto body text. A continuation page, once its running head is
    dropped, resumes mid-sentence in lower case:

        10  Harvard Journal of Law & Public Policy  [Vol. 40
        relocating and taking on a new allegiance...

    while an opening page leads with a display title. Without this gate, a
    per-domain pattern validated on one issue silently cuts a different issue
    of the same journal every eight pages.
    """
    lines = _clean_lines(page_text)
    body: list[str] = []
    for line in lines:
        if not body and _is_running_head_line(line):
            continue  # still in the running head
        body.append(line)
    if not body:
        return False

    # A shouted line sitting directly on top of resumed prose is a running
    # head, whatever it looks like on its own. This is the check the two
    # confirmed failures needed: `SACRIFICING MOTHERHOOD` over "more
    # particularly, whether the child can have two mothers" is the short-title
    # head of a continuation page, and so is `2021 / IMPLEMENTING PASH AND ITS
    # PROGENY WITHIN DLNR 421`. Neither is caught by the head patterns above --
    # one carries no folio, the other opens with the year -- and both are
    # capitalised enough to pass as display titles. An article's real display
    # title is never followed by a full measure of lower-case prose; what
    # follows it is a subtitle, an author, a date, or an epigraph.
    # The head can wrap onto a second line (`2019 / APPLYING INDIGENOUS
    # ECOLOGICAL KNOWLEDGE FOR` / `THE PROTECTION OF ENVIRONMENTAL COMMONS
    # 301`), so walk the whole run of shouted lines before looking at what sits
    # under it.
    shouted_run = 0
    while shouted_run < min(4, len(body)) and _is_shouted(body[shouted_run]):
        shouted_run += 1
    if shouted_run and shouted_run < len(body) and _is_resumed_prose(body[shouted_run]):
        return False

    # Judge the FIRST body line only. Scanning further down finds a title-like
    # line on almost any page -- a case name, a shouted heading mid-argument --
    # which let continuation pages through. An article's display title is at
    # the top of its own page or it is not there at all.
    first = body[0]
    # A section divider (`NOTES`, `ARTICLES`, `BOOK REVIEWS`) sits between
    # articles, never inside one, so it is a safe place to cut even though it
    # is too short to be a title.
    if _SECTION_LINE_RE.match(first):
        return True
    if len(first) < 8 or len(first) > 140:
        return False
    if not _has_wordlike_content(first):
        return False
    # With the head gone, a first line that is running text is the body of an
    # article already under way.
    if _looks_like_sentence_prose(first):
        return False
    upper_share = _upper_share(first)
    title_case = first[:1].isupper() and not first.endswith((",", "-", ";"))
    return upper_share >= 0.6 or title_case


def validate_boundary_starts(
    page_texts: list[str], starts: list[int], min_share: float = 0.6
) -> tuple[list[int], float]:
    """Drop starts that do not open an article; report the share that passed.

    A boundary that fails is merged into the article before it by simply being
    removed. If most boundaries fail the signal is not tracking articles at all
    and the caller should discard the whole document.
    """
    if not starts:
        return [], 0.0
    kept = [
        start
        for start in starts
        if start == 1 or looks_like_article_opening(page_texts[start - 1])
    ]
    return kept, len(kept) / len(starts)
