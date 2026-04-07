from __future__ import annotations

import re
from typing import Sequence
from urllib.parse import urlparse

from .schema import CitationMention, NoteRecord

CASE_RE = re.compile(r"\b[A-Z][\w'’\.-]+(?:\s+[A-Z][\w'’\.-]+)*\s+v\.\s+[A-Z][\w'’\.-]+", re.I)
STATUTE_RE = re.compile(r"\b\d+\s+U\.?S\.?C\.?\s*§+\s*[\w\.-]+", re.I)
REGULATION_RE = re.compile(r"\b\d+\s+C\.?F\.?R\.?\s*§+\s*[\w\.-]+", re.I)
MUNICIPAL_RE = re.compile(
    r"\b(?:municipal\s+code|mun\.\s+code|city\s+code|county\s+code|code\s+of\s+ordinances)\s*§+\s*[\w\.-]+",
    re.I,
)
LEG_HISTORY_RE = re.compile(
    r"\b(?:H\.R\.|S\.)\s*(?:Rep\.|Doc\.|Conf\.\s*Rep\.)\s*No\.?\s*[\w\-]+",
    re.I,
)
TREATY_RE = re.compile(r"\b(?:treaty|convention|protocol)\b", re.I)
SECONDARY_RE = re.compile(
    r"\b\d+\s+[A-Z][A-Za-z\.\s&,'-]{1,}\s+L\.\s*Rev\.\s+\d+\b",
    re.I,
)
URL_RE = re.compile(r"https?://[^\s)\]>\"']+", re.I)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")


def classify_citation_type(value: str) -> str:
    text = value or ""
    if CASE_RE.search(text):
        return "case"
    if STATUTE_RE.search(text):
        return "statute"
    if REGULATION_RE.search(text):
        return "regulation"
    if MUNICIPAL_RE.search(text):
        return "municipal_code"
    if LEG_HISTORY_RE.search(text):
        return "legislative_history"
    if TREATY_RE.search(text):
        return "treaty_or_international"
    if SECONDARY_RE.search(text):
        return "secondary_source"
    return "other"


def _normalize_url(url: str) -> str:
    trimmed = (url or "").strip().rstrip(".,;:)")
    if not trimmed:
        return ""
    parsed = urlparse(trimmed)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return trimmed


def extract_urls(text: str) -> list[str]:
    seen = set()
    urls: list[str] = []
    for match in URL_RE.findall(text or ""):
        normalized = _normalize_url(match)
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def extract_citation_mentions(text: str) -> list[CitationMention]:
    """Regex-only citation extraction. Eyecite is reserved for post-processing."""
    mentions: list[CitationMention] = []
    seen = set()

    regexes: Sequence[tuple[str, re.Pattern[str]]] = (
        ("case", CASE_RE),
        ("statute", STATUTE_RE),
        ("regulation", REGULATION_RE),
        ("municipal_code", MUNICIPAL_RE),
        ("legislative_history", LEG_HISTORY_RE),
        ("treaty_or_international", TREATY_RE),
        ("secondary_source", SECONDARY_RE),
    )

    for citation_type, pattern in regexes:
        for match in pattern.finditer(text or ""):
            value = " ".join(match.group(0).split())
            key = (value.lower(), citation_type)
            if key in seen:
                continue
            seen.add(key)
            mentions.append(
                CitationMention(text=value, citation_type=citation_type, source="regex")
            )

    return mentions


def extract_emails(text: str) -> list[str]:
    seen = set()
    items: list[str] = []
    for match in EMAIL_RE.findall(text or ""):
        value = match.strip()
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        items.append(value)
    return items


def extract_years(text: str) -> list[str]:
    return sorted(set(YEAR_RE.findall(text or "")))


def _extract_entities(text: str) -> list[dict[str, str]]:
    try:
        import spacy  # type: ignore

        model = spacy.load("en_core_web_sm")
    except Exception:
        return []

    try:
        doc = model(text or "")
    except Exception:
        return []

    entities: list[dict[str, str]] = []
    for ent in doc.ents:
        entities.append({"text": ent.text, "label": ent.label_})
    return entities


def enrich_note_features(note: NoteRecord, preset: str) -> None:
    if preset in {"legal", "all"}:
        urls = extract_urls(note.text)
        if urls:
            note.features["urls"] = urls
        years = extract_years(note.text)
        if years:
            note.features["years"] = years

    if preset == "all":
        emails = extract_emails(note.text)
        if emails:
            note.features["emails"] = emails
        entities = _extract_entities(note.text)
        if entities:
            note.features["entities"] = entities
