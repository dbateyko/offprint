from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from importlib import metadata as importlib_metadata
from typing import Any

CITATION_TAXONOMY = {
    "case",
    "statute",
    "regulation",
    "municipal_code",
    "legislative_history",
    "secondary_source",
    "treaty_or_international",
    "other",
}

AUTHOR_MARKERS = {"*", "†", "‡", "§", "¶"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _package_version(name: str) -> str:
    try:
        return importlib_metadata.version(name)
    except Exception:
        return "unavailable"


def dependency_versions() -> dict[str, str]:
    return {
        "pdfplumber": _package_version("pdfplumber"),
        "pdfminer.six": _package_version("pdfminer.six"),
        "pymupdf": _package_version("PyMuPDF"),
        "eyecite": _package_version("eyecite"),
        "spacy": _package_version("spacy"),
        "en_core_web_sm": _package_version("en-core-web-sm"),
        "olmocr": _package_version("olmocr"),
        "opendataloader-pdf": _package_version("opendataloader-pdf"),
    }


@dataclass
class NoteChunk:
    page: int
    text: str
    source: str = "text"


@dataclass
class CitationMention:
    text: str
    citation_type: str
    source: str = "regex"

    def to_dict(self) -> dict[str, str]:
        citation_type = self.citation_type
        if citation_type not in CITATION_TAXONOMY:
            citation_type = "other"
        return {
            "text": self.text,
            "citation_type": citation_type,
            "source": self.source,
        }


@dataclass
class NoteRecord:
    ordinal: int
    label: str
    note_type: str
    text: str
    page_start: int
    page_end: int
    segments: list[NoteChunk] = field(default_factory=list)
    context_sentence: str = ""
    context_page: int = 0
    citation_mentions: list[CitationMention] = field(default_factory=list)
    features: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    quality_flags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["segments"] = [asdict(segment) for segment in self.segments]
        payload["citation_mentions"] = [citation.to_dict() for citation in self.citation_mentions]
        payload["confidence"] = round(float(self.confidence), 3)
        return payload


@dataclass
class AuthorNote:
    """Represents author attribution footnotes marked with *, †, ‡, etc."""

    marker: str
    text: str
    page: int

    def to_dict(self) -> dict[str, Any]:
        return {"marker": self.marker, "text": self.text, "page": self.page}


@dataclass
class OrdinalityReport:
    """Tracks footnote sequence integrity with gap tolerance."""

    status: str  # "valid" | "valid_with_gaps" | "invalid"
    expected_range: tuple[int, int]  # (min, max) of expected sequence
    actual_sequence: list[int]
    gaps: list[int]
    gap_tolerance: int
    tolerance_exceeded: bool
    gap_ratio: float = 0.0
    gap_ratio_threshold: float = 0.35

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "expected_range": list(self.expected_range),
            "actual_sequence": self.actual_sequence,
            "gaps": self.gaps,
            "gap_tolerance": self.gap_tolerance,
            "tolerance_exceeded": self.tolerance_exceeded,
            "gap_ratio": round(self.gap_ratio, 4),
            "gap_ratio_threshold": self.gap_ratio_threshold,
        }


@dataclass
class SidecarDocument:
    source_pdf_path: str
    pdf_sha256: str | None
    extractor_version: str
    created_at: str
    dependency_versions: dict[str, str]
    document_confidence: float
    warnings: list[str]
    features_preset: str
    notes: list[NoteRecord] = field(default_factory=list)
    author_notes: list[AuthorNote] = field(default_factory=list)
    ordinality: OrdinalityReport | None = None
    document_metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        # Convert notes list to dict keyed by label for cleaner output
        notes_dict: dict[str, Any] = {}
        label_counts: dict[str, int] = {}
        for note in self.notes:
            note_data = note.to_dict()
            label = str(note_data.pop("label"))
            # Remove ordinal from output - redundant with dict key
            note_data.pop("ordinal", None)
            count = label_counts.get(label, 0) + 1
            label_counts[label] = count
            key = label if count == 1 else f"{label}__dup{count}"
            notes_dict[key] = note_data

        return {
            "source_pdf_path": self.source_pdf_path,
            "pdf_sha256": self.pdf_sha256,
            "extractor_version": self.extractor_version,
            "created_at": self.created_at,
            "dependency_versions": dict(self.dependency_versions),
            "document_confidence": round(float(self.document_confidence), 3),
            "warnings": list(self.warnings),
            "features_preset": self.features_preset,
            "notes": notes_dict,
            "author_notes": [an.to_dict() for an in self.author_notes],
            "ordinality": self.ordinality.to_dict() if self.ordinality else None,
            "document_metadata": dict(self.document_metadata or {}),
        }
