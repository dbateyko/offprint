from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, is_dataclass, replace
from typing import Any, Callable

from .schema import NoteRecord

_METADATA_KEYS = (
    "volume",
    "reporter",
    "page",
    "title",
    "section",
    "pin_cite",
    "year",
    "month",
    "day",
    "court",
    "plaintiff",
    "defendant",
    "antecedent_guess",
    "resolved_case_name_short",
    "resolved_case_name",
    "publisher",
    "parenthetical",
)


def _load_get_citations() -> Callable[[str], list[Any]] | None:
    try:
        from eyecite import get_citations  # type: ignore
    except Exception:
        return None
    return get_citations


def _call_or_value(value: Any) -> Any:
    if callable(value):
        try:
            return value()
        except Exception:
            return None
    return value


def _citation_text(citation: Any) -> str:
    matched_text = _call_or_value(getattr(citation, "matched_text", None))
    if isinstance(matched_text, str) and matched_text:
        return " ".join(matched_text.split())
    return " ".join(str(citation).split())


def _citation_span(citation: Any) -> list[int] | None:
    span = _call_or_value(getattr(citation, "span", None))
    if not isinstance(span, (tuple, list)) or len(span) != 2:
        return None
    try:
        return [int(span[0]), int(span[1])]
    except (TypeError, ValueError):
        return None


def _metadata_dict(metadata: Any) -> dict[str, Any]:
    if metadata is None:
        return {}
    if is_dataclass(metadata):
        try:
            values = asdict(metadata)
        except Exception:
            values = {}
    elif isinstance(metadata, dict):
        values = dict(metadata)
    else:
        values = {
            key: getattr(metadata, key)
            for key in _METADATA_KEYS
            if hasattr(metadata, key)
        }
    return {key: value for key, value in values.items() if value is not None}


def _citation_to_dict(citation: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": type(citation).__name__,
        "text": _citation_text(citation),
    }

    span = _citation_span(citation)
    if span is not None:
        payload["span"] = span

    groups = getattr(citation, "groups", None)
    if isinstance(groups, dict):
        for key, value in groups.items():
            if value is not None:
                payload[key] = value

    for key, value in _metadata_dict(getattr(citation, "metadata", None)).items():
        payload.setdefault(key, value)

    return payload


def extract_citations(text: str) -> list[dict[str, Any]]:
    """Extract legal citations from text using eyecite when available.

    This module is deliberately optional: missing or failing eyecite produces an
    empty list rather than blocking footnote extraction.
    """
    if not text:
        return []

    get_citations = _load_get_citations()
    if get_citations is None:
        return []

    try:
        citations = get_citations(text)
    except Exception:
        return []

    enriched: list[dict[str, Any]] = []
    for citation in citations:
        try:
            enriched.append(_citation_to_dict(citation))
        except Exception:
            continue
    return enriched


def enrich_note(note: NoteRecord) -> NoteRecord:
    """Return a copy of a note with eyecite citation dicts in features."""
    features = deepcopy(note.features)
    features["citations"] = extract_citations(note.text)
    return replace(note, features=features)
