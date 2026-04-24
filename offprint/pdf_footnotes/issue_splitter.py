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
