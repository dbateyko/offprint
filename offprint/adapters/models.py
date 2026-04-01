from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ArticleMetadata:
    """Standardized metadata schema based on Dublin Core / schema.org."""

    title: Optional[str] = None
    authors: List[str] = field(default_factory=list)
    date: Optional[str] = None  # Prefer ISO format: YYYY-MM-DD or YYYY-MM or YYYY
    volume: Optional[str] = None
    issue: Optional[str] = None
    pages: Optional[str] = None
    citation: Optional[str] = None
    doi: Optional[str] = None
    abstract: Optional[str] = None
    url: Optional[str] = None
    source_url: Optional[str] = None  # Page where metadata was extracted
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dict, omitting None/empty values and merging extras."""
        data = {k: v for k, v in asdict(self).items() if v is not None and v != [] and k != "extra"}
        if self.extra:
            data.update(self.extra)
        return data

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "ArticleMetadata":
        if not payload:
            return cls()
        allowed_keys = {
            "title",
            "authors",
            "date",
            "volume",
            "issue",
            "pages",
            "citation",
            "doi",
            "abstract",
            "url",
            "source_url",
        }
        filtered = {k: v for k, v in payload.items() if k in allowed_keys}
        if isinstance(filtered.get("authors"), str):
            filtered["authors"] = [filtered["authors"]]
        extra = {k: v for k, v in payload.items() if k not in allowed_keys}
        metadata = cls(**filtered)
        if extra:
            metadata.extra = extra
        return metadata


def validate_metadata(metadata: ArticleMetadata) -> List[str]:
    """Return list of validation warnings."""
    warnings: List[str] = []
    if not metadata.title:
        warnings.append("Missing title")
    if not metadata.authors:
        warnings.append("Missing authors")
    if not metadata.date:
        warnings.append("Missing date")
    return warnings


def metadata_completeness(metadata: ArticleMetadata) -> Dict[str, Any]:
    """Compute a simple completeness score for manifests."""
    has_title = bool(metadata.title)
    has_authors = bool(metadata.authors)
    has_date = bool(metadata.date)
    has_volume = bool(metadata.volume)
    has_issue = bool(metadata.issue)
    flags = [has_title, has_authors, has_date, has_volume, has_issue]
    score = sum(1 for f in flags if f) / len(flags)
    return {
        "has_title": has_title,
        "has_authors": has_authors,
        "has_date": has_date,
        "has_volume": has_volume,
        "has_issue": has_issue,
        "completeness_score": round(score, 2),
    }
