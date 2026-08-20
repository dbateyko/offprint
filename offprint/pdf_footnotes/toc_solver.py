"""TOC-driven article-boundary solver for law-review issue compilations.

The running-head splitter in :mod:`issue_splitter` derives a boundary from a
*lagging* signal: the head arrives 0-2 pages after the article actually starts,
by an amount that varies per journal and sometimes per article. This module
inverts that. The contents listing is treated as the specification of what the
issue contains, the printed folio stream as the locator that maps a printed page
number onto a physical page, and the running head as corroboration only.

Three parts:

1. :func:`parse_toc_entries` reads structured entries -- printed start page,
   title, author, section type -- rather than a bare list of numbers.
2. :func:`estimate_folio_offset` fits one global affine map
   ``printed = physical + offset`` by consensus over every folio candidate in
   the document. A number is selected because it participates in a stream that
   increments with the physical page, not because it is a number in a margin.
3. :func:`solve` scores every (entry, physical page) pair and chooses a
   *monotonic* assignment of entries to pages by dynamic programming. Boundaries
   are decided jointly, so one well-evidenced article constrains its neighbours.

Emission is three-way (``auto`` / ``review`` / ``abstain``) because the cost is
asymmetric: a mid-article cut yields two corrupt documents that enter the
citation graph as real, while a missed boundary only leaves a compilation
unsplit, which is the status quo. Every decision carries an evidence ledger.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Sequence
import math
import re

__all__ = [
    "Line",
    "PageEvidence",
    "TocEntry",
    "Assignment",
    "SolveResult",
    "extract_pages",
    "parse_toc_entries",
    "build_page_evidence",
    "estimate_folio_offset",
    "solve",
]


# ---------------------------------------------------------------------------
# Text model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Line:
    """One extracted text line, with geometry when the extractor supplies it."""

    text: str
    y0: float = 0.0
    y1: float = 0.0
    x0: float = 0.0
    x1: float = 0.0
    size: float = 0.0

    @property
    def has_geometry(self) -> bool:
        return self.y1 > self.y0 or self.x1 > self.x0


@dataclass
class Page:
    """A physical page: one-based index, its lines, and the media box height."""

    index: int
    lines: list[Line]
    height: float = 0.0
    width: float = 0.0

    @property
    def text(self) -> str:
        return "\n".join(line.text for line in self.lines)


_SPACE_RE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    return _SPACE_RE.sub(" ", (text or "").replace("\x00", "")).strip()


def extract_pages(pdf_path: str, max_pages: int = 0) -> list[Page]:
    """Read per-page lines with geometry.

    PyMuPDF is preferred because folio detection wants to know that a number sits
    in the top or bottom margin, which a flat text dump cannot say. pypdf is the
    fallback and produces geometry-free lines; every downstream check degrades to
    a positional rule (first/last lines) rather than failing.
    """
    try:
        return _extract_pages_pymupdf(pdf_path, max_pages)
    except ImportError:
        return _extract_pages_pypdf(pdf_path, max_pages)


def _extract_pages_pymupdf(pdf_path: str, max_pages: int = 0) -> list[Page]:
    import fitz  # type: ignore

    pages: list[Page] = []
    with fitz.open(pdf_path) as document:
        for index, page in enumerate(document, start=1):
            if max_pages and index > max_pages:
                break
            rect = page.rect
            lines: list[Line] = []
            data = page.get_text("dict")
            for block in data.get("blocks", []):
                for raw_line in block.get("lines", []):
                    spans = raw_line.get("spans", [])
                    text = _normalize("".join(span.get("text", "") for span in spans))
                    if not text:
                        continue
                    bbox = raw_line.get("bbox", (0.0, 0.0, 0.0, 0.0))
                    lines.append(
                        Line(
                            text=text,
                            x0=float(bbox[0]),
                            y0=float(bbox[1]),
                            x1=float(bbox[2]),
                            y1=float(bbox[3]),
                            size=max((float(span.get("size", 0.0)) for span in spans), default=0.0),
                        )
                    )
            lines.sort(key=lambda line: (round(line.y0, 1), line.x0))
            pages.append(
                Page(index=index, lines=lines, height=float(rect.height), width=float(rect.width))
            )
    return pages


def _extract_pages_pypdf(pdf_path: str, max_pages: int = 0) -> list[Page]:
    import logging
    import warnings

    from pypdf import PdfReader  # type: ignore

    warnings.filterwarnings("ignore")
    logging.getLogger("pypdf").setLevel(logging.CRITICAL)
    reader = PdfReader(pdf_path, strict=False)
    pages: list[Page] = []
    for index, page in enumerate(reader.pages, start=1):
        if max_pages and index > max_pages:
            break
        try:
            raw = page.extract_text() or ""
        except Exception:  # a damaged page should not abort the document
            raw = ""
        lines = [Line(text=_normalize(line)) for line in raw.splitlines() if line.strip()]
        pages.append(Page(index=index, lines=lines))
    return pages


def pages_from_texts(page_texts: Sequence[str]) -> list[Page]:
    """Build geometry-free pages from plain text, for tests and sidecar input."""
    return [
        Page(
            index=index,
            lines=[Line(text=_normalize(line)) for line in (text or "").splitlines() if line.strip()],
        )
        for index, text in enumerate(page_texts, start=1)
    ]


# ---------------------------------------------------------------------------
# Contents listing
# ---------------------------------------------------------------------------


_CONTENTS_HEAD_RE = re.compile(r"^(?:TABLE\s+OF\s+)?CONTENTS$", re.I)
_SECTION_RE = re.compile(
    r"^(ARTICLES?|ESSAYS?|NOTES?|CASE\s*NOTES?|COMMENTS?|COMMENTARY|BOOK\s+REVIEWS?|"
    r"REVIEWS?|TRIBUTES?|IN\s+MEMORIAM|FOREWORDS?|SYMPOSIUM|RECENT\s+DEVELOPMENTS?|"
    r"STUDENT\s+(?:NOTES?|WORKS?|SCHOLARSHIP)|SPEECHES?|REMARKS|LECTURES?|"
    r"DEVELOPMENTS?|ADDRESSES?)\s*:?$",
    re.I,
)
# `Remedies for the Wrongly Deported .......... 139` and the leaderless variant.
_LEADER_ENTRY_RE = re.compile(r"^(?P<label>.*?\S)\s*[.…·\-_\s]{3,}\s*(?P<page>\d{1,4})$")
# The dot leaders themselves, wherever they fall in the line. Most US law
# reviews print `Title . . . . . . Author  339`, so the leaders separate title
# from author rather than title from page number.
_LEADER_RUN_RE = re.compile(r"(?:\s*[.·…_\-]\s*){3,}|\s{4,}")
_SECTION_TAIL_RE = re.compile(
    r"\b(ARTICLES?|ESSAYS?|NOTES?|COMMENTS?|COMMENTARY|REVIEWS?|TRIBUTES?|MEMORIAM|"
    r"FOREWORDS?|SYMPOSIUM|DEVELOPMENTS?|REMARKS|SPEECHES?|LECTURES?|ADDRESSES?|"
    r"SCHOLARSHIP|CONTRIBUTIONS?)\s*:?$"
)
_TRAILING_ENTRY_RE = re.compile(r"^(?P<label>.*?\S)\s{1,}(?P<page>\d{1,4})$")
_CAP_TOKEN_RE = re.compile(r"\b[A-Z][A-Za-z'’`.\-]*")
# A person name as printed in a contents listing: `Rachel E. Rosenbloom`,
# `E. CHRISTI CUNNINGHAM`, `By Kenneth B. Nunn`.
_PERSON_RE = re.compile(
    r"^(?:By\s+|BY\s+)?"
    r"(?:[A-Z][A-Za-z'’`\-]+|[A-Z]\.)"
    r"(?:\s+(?:[A-Z][A-Za-z'’`\-]+|[A-Z]\.|van|von|de|del|della|di|la|le|Mc|Mac|St\.))"
    r"{1,5}"
    r"[,\s]*(?:Jr\.?|Sr\.?|I{1,3}|IV|Ph\.?D\.?|J\.?D\.?|M\.?D\.?|Esq\.?)?\s*[\*†‡]?$"
)
_FRONT_MATTER_TITLE_RE = re.compile(
    r"^(?:TABLE\s+OF\s+CONTENTS|CONTENTS|MASTHEAD|EDITORIAL\s+BOARD|BOARD\s+OF\s+EDITORS|"
    r"OFFICERS|SUBSCRIPTIONS?|INDEX|ERRATA|COVER|FRONT\s+MATTER|BACK\s+MATTER|"
    r"ABOUT\s+THE|SUBMISSIONS?|COPYRIGHT)\b",
    re.I,
)

_STOPWORDS = {
    "a", "an", "and", "as", "at", "be", "but", "by", "for", "from", "in", "into",
    "is", "it", "its", "of", "on", "or", "the", "to", "with", "without", "under",
    "over", "after", "before", "not", "no", "are", "was", "were", "that", "this",
}


@dataclass
class TocEntry:
    """One contents entry: what the issue says it contains, and where."""

    printed_page: int
    title: str
    author: str = ""
    section: str = ""
    raw: str = ""
    toc_page_index: int = 0

    @property
    def is_front_matter(self) -> bool:
        return bool(_FRONT_MATTER_TITLE_RE.match(self.title.strip()))

    @property
    def title_tokens(self) -> set[str]:
        return _content_tokens(self.title)

    @property
    def surnames(self) -> list[str]:
        return _surnames(self.author)


def _content_tokens(text: str) -> set[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z'’\-]+", (text or "").lower())
    return {token for token in tokens if len(token) > 2 and token not in _STOPWORDS}


def _surnames(author: str) -> list[str]:
    names: list[str] = []
    for chunk in re.split(r"\s*(?:,|&|\band\b)\s*", author or ""):
        tokens = [
            token
            for token in re.findall(r"[A-Za-z][A-Za-z'’\-]+", chunk)
            if len(token) >= 3 and token.lower() not in {"jr", "sr", "and", "by", "the"}
        ]
        if tokens:
            names.append(tokens[-1])
    return names


# A numbered or lettered section heading: `I.`, `II.`, `A`, `B.`, `1.`. An
# in-article contents page is made of these; a real contents listing is made of
# titles and person names. This is the single most useful discriminator between
# the two, and it needs no per-domain knowledge.
# The punctuation is required: without it this also matches `A VIEW FROM THE
# FRONT LINES`, and dropping that anchor cost the first entry of the American
# University listing.
_ENUMERATOR_RE = re.compile(r"^(?:[IVXLC]{1,6}|[A-H]|\d{1,2})[.)]\s+\S")
# The masthead block sitting above the listing -- `CONNECTICUT INSURANCE LAW
# JOURNAL VOLUME 31 2024-2025 NUMBER 2` -- ends in a number and reads as an
# entry with a printed start page of 2. Left in, it consumed the first real
# entry on three of the four compilations in the held-out set.
_MASTHEAD_TAIL_RE = re.compile(r"(?i)\b(?:volume|vol\.?|number|no\.?|issue|part|page|pp?\.)\s*$")
# A section enumerator ANYWHERE in the string, not just at the start. Row
# reconstruction joins a wrapped entry into one string, so an in-article
# contents page yields `Recently B. Hypothesis Two: Exclusion of...` and
# `Organization 2. Why the Bumper Sticker Gets It Right`. Roman numerals and
# digits only: `[A-H]\.` would also match the middle initial in
# `Remarks of the Honorable Eric H. Holder`.
_EMBEDDED_ENUM_RE = re.compile(r"(?:^|\s)(?:[IVX]{1,6}|\d{1,2})\.\s+[A-Z]")


def _row_lines(page: Page, overlap_share: float = 0.5) -> list[Line]:
    """Re-join lines that share a horizontal band, ordered left to right.

    Contents listings are typeset in columns -- title left, author centre, folio
    right -- and every extractor linearises them in some order of its own. The
    UConn listing comes out as `Katherine` / `85` / `INSURANCE ERA: RISK,` /
    `Hempstead`, which no line-wise parser can read. Grouping by vertical
    overlap and sorting by x reconstructs the printed row.

    Only used for the contents listing. Applying it to body text would merge the
    columns of a two-column journal into nonsense.
    """
    if not any(line.has_geometry for line in page.lines):
        return list(page.lines)

    rows: list[list[Line]] = []
    for line in sorted(page.lines, key=lambda item: (item.y0, item.x0)):
        placed = False
        for row in rows:
            reference = row[-1]
            height = min(line.y1 - line.y0, reference.y1 - reference.y0) or 1.0
            overlap = min(line.y1, reference.y1) - max(line.y0, reference.y0)
            if overlap >= overlap_share * height:
                row.append(line)
                placed = True
                break
        if not placed:
            rows.append([line])

    merged: list[Line] = []
    for row in rows:
        row.sort(key=lambda item: item.x0)
        merged.append(
            Line(
                text=_normalize(" ".join(item.text for item in row)),
                x0=row[0].x0,
                y0=min(item.y0 for item in row),
                x1=row[-1].x1,
                y1=max(item.y1 for item in row),
                size=max(item.size for item in row),
            )
        )
    return merged


def _listing_score(page: Page) -> int:
    """How much this page looks like a contents listing of *works*."""
    score = 0
    for line in _row_lines(page):
        if not _entry_shape(line.text):
            continue
        match = _LEADER_ENTRY_RE.match(line.text) or _TRAILING_ENTRY_RE.match(line.text)
        label = match.group("label").strip() if match else line.text
        if _ENUMERATOR_RE.match(label) or _EMBEDDED_ENUM_RE.search(label):
            score -= 1  # a section heading: this is an in-article contents page
            continue
        score += 1
    return score


def find_toc_pages(pages: Sequence[Page], scan_pages: int = 25) -> list[int]:
    """Physical indices of the contents listing.

    A `CONTENTS` heading is the anchor; the listing may run onto the next pages,
    which are recognised by continuing to carry entry-shaped lines.
    """
    # A contents listing sits in the front matter. Anything deeper is a data
    # table or an in-article list: `tilj.org/tilj-59n3-text-cavallaro.pdf` puts
    # an ICC-convictions table on page 18 of 30, and read as a listing it splits
    # a single article in two.
    limit = min(len(pages), scan_pages, max(15, int(0.15 * len(pages))))
    scores: list[tuple[int, int]] = []
    for page in pages[:limit]:
        score = _listing_score(page)
        # A `CONTENTS` heading corroborates but does not decide: the Chapman
        # issue heads its real listing `ARTICLES` and prints `TABLE OF CONTENTS`
        # only inside its first article, where an anchor-driven search finds the
        # wrong page and splits one article into its own sections.
        if any(_CONTENTS_HEAD_RE.match(line.text) for line in page.lines[:12]):
            score += 2
        scores.append((score, page.index))

    best_score = max((score for score, _ in scores), default=0)
    if best_score < 3:
        return []
    # Earliest page that is within one point of the best: a listing that runs
    # over two pages should start at the first of them.
    start = min(index for score, index in scores if score >= best_score - 1)

    indices = [start]
    cursor = start + 1
    while cursor <= len(pages) and cursor <= start + 4:
        if _listing_score(pages[cursor - 1]) >= 3:
            indices.append(cursor)
            cursor += 1
            continue
        break
    return indices


def _section_name(text: str) -> str:
    """`ARTICLES`, and also `SPECIAL GUEST REMARKS` / `STUDENT NOTES`.

    A section divider is a short shouted line whose last word names a piece
    type. Matching only the bare keywords let `SPECIAL GUEST REMARKS` parse as a
    person's name and attach itself to the entry above it as an author.
    """
    stripped = text.strip()
    match = _SECTION_RE.match(stripped)
    if match:
        return _normalize(match.group(1)).upper()
    if (
        len(stripped) <= 45
        and stripped.upper() == stripped
        and not re.search(r"\d", stripped)
        and _SECTION_TAIL_RE.search(stripped)
    ):
        return _normalize(stripped).upper()
    return ""


def _split_label(label: str) -> tuple[str, str]:
    """Split `Title . . . . Author` on its dot leaders."""
    segments = [segment.strip(" .·…_-") for segment in _LEADER_RUN_RE.split(label)]
    segments = [segment for segment in segments if segment]
    if len(segments) < 2:
        return label.strip(" .·…_-"), ""
    tail = segments[-1]
    if _PERSON_RE.match(tail) and len(tail) <= 70:
        return " ".join(segments[:-1]), tail
    return " ".join(segments), ""


def _entry_shape(text: str) -> bool:
    match = _LEADER_ENTRY_RE.match(text)
    if match:
        return len(match.group("label")) >= 4
    match = _TRAILING_ENTRY_RE.match(text)
    if not match:
        return False
    label = match.group("label")
    return len(label) >= 8 and len(_CAP_TOKEN_RE.findall(label)) >= 2


def parse_toc_entries(pages: Sequence[Page], toc_indices: Sequence[int] | None = None) -> list[TocEntry]:
    """Read structured entries from the contents listing.

    An entry wraps over several rows, and journals disagree about where the page
    number goes. Chapman prints it on the entry's LAST row::

        Still Problematic, Even Post-Settlement: Florida's "Don't
        Say Gay" Law and the Federal Constitution
        Catherine Jean Archibald ........................................ 1

    American University prints it on the entry's FIRST row::

        A VIEW FROM THE FRONT LINES: ................................ 259
        WHY PROTECTING IMMIGRANT WORKERS IS
        ...
        Andreas N. Akaras & Sebastian G. Amar

    So the rows carrying a page number are treated as anchors, both readings are
    built, and the one that recovers more authors wins. Guessing a single
    convention silently swallows the first entry of every listing that uses the
    other one.
    """
    indices = list(toc_indices) if toc_indices is not None else find_toc_pages(pages)
    if not indices:
        return []

    rows: list[tuple[int, str]] = []
    for physical in indices:
        for line in _row_lines(pages[physical - 1]):
            text = line.text.strip()
            if text and not _CONTENTS_HEAD_RE.match(text):
                rows.append((physical, text))

    anchors = [position for position, (_, text) in enumerate(rows) if _anchor_page(text)]
    if not anchors:
        return []

    above = _entries_for_mode(rows, anchors, "above")
    below = _entries_for_mode(rows, anchors, "below")
    # Entry count decides first. A reading that recovers three works with no
    # author beats one that recovers a single work with an author: a missing
    # author costs one signal, a missing entry costs a whole article.
    scored = max(
        (above, below),
        key=lambda built: (len(built), sum(1 for entry in built if entry.author)),
    )
    scored = _attach_following_authors(scored, pages, indices)
    if _listing_is_in_article(scored):
        return []
    return _clean_entries([entry for entry in scored if not entry.title[:1].islower()])


def _listing_is_in_article(entries: Sequence[TocEntry]) -> bool:
    """True when the parsed listing is one article's own contents page.

    This is the failure that matters most: an article's internal contents page
    lists its own sections against its own page numbers, and taking it as the
    issue listing turns one article into a dozen corrupt children. It was the
    cause of every `insufficient_evidence` abstention sampled on 2026-08-07 --
    the solver reached the right answer, but via the downstream continuation
    guard rather than by declining to read the page.

    Three whole-listing signals separate the two cleanly. Measured over five
    known-good listings and five known in-article ones, the good listings score
    0.00 / 0.00 / >=0.88 and the bad ones 0.00-0.67 / 0.10-0.69 / <=0.20:

    * **fragment titles** -- a real contents entry never starts mid-phrase in
      lower case; `of the Child B. The Convention on the Rights of the` does.
    * **embedded enumerators** -- `2. Why the Bumper Sticker Gets It Right`.
    * **no authors at all** -- an issue listing names people; a section list
      does not. Used only as a third check on longer listings, because a
      symposium programme can legitimately omit authors.
    """
    if len(entries) < 2:
        return False
    total = len(entries)
    # A fragment or an enumerator is NOT damning on its own: a real listing wraps
    # its titles too, and row reconstruction can split a wrapped title across
    # anchors, leaving `for the Average Worker Lisa A. Nagele-Piazza`. Judging on
    # shape alone rejected five real compilations out of six demotions when this
    # was first measured. What separates them is that a real listing still names
    # a person on those rows and a section list never does.
    suspect = sum(
        1
        for entry in entries
        if not entry.author
        and (entry.title[:1].islower() or _EMBEDDED_ENUM_RE.search(entry.title))
    )
    return suspect / total >= 0.25


def _anchor_page(text: str) -> int:
    """The printed page number this row carries, or 0 if it is not an anchor."""
    leader = _LEADER_ENTRY_RE.match(text)
    match = leader or _TRAILING_ENTRY_RE.match(text)
    if not match:
        return 0
    label = match.group("label").strip()
    if _MASTHEAD_TAIL_RE.search(label) or _ENUMERATOR_RE.match(label):
        return 0
    if leader is None and len(_CAP_TOKEN_RE.findall(label)) < 2:
        return 0
    # `V o l u m e 6 8 - I s s u e 3 - S p r i n g 2 0 2 5` parses as an entry
    # starting on printed page 5. Extractors letter-space small caps, and a
    # letter-spaced line is display type from a cover or masthead, never a
    # contents entry.
    tokens = [token for token in _LEADER_RUN_RE.sub(" ", label).split() if token]
    spaced = sum(1 for token in tokens if len(token) == 1 and token.isalpha())
    if len(tokens) >= 6 and spaced / len(tokens) > 0.4:
        return 0
    value = int(match.group("page"))
    return value if 0 < value < 10000 else 0


def _entries_for_mode(
    rows: Sequence[tuple[int, str]], anchors: Sequence[int], mode: str
) -> list[TocEntry]:
    sections = _section_at_row(rows)
    entries: list[TocEntry] = []
    for order, anchor in enumerate(anchors):
        physical, text = rows[anchor]
        page_number = _anchor_page(text)
        match = _LEADER_ENTRY_RE.match(text) or _TRAILING_ENTRY_RE.match(text)
        label = match.group("label").strip() if match else text

        if mode == "above":
            low = anchors[order - 1] + 1 if order else 0
            window = [item for _, item in rows[low:anchor]]
            parts = [*window, label]
        else:
            high = anchors[order + 1] if order + 1 < len(anchors) else len(rows)
            window = [item for _, item in rows[anchor + 1 : high]]
            parts = [label, *window]

        kept: list[str] = []
        for part in parts:
            if _section_name(part):
                if mode == "below":
                    break  # the divider introduces the NEXT entry
                kept = []  # ...and ends the entry above it
                continue
            kept.append(part)
        if not kept:
            kept = [label]

        entry = _entry_from_parts(kept, "", page_number, sections[anchor], physical)
        if entry is not None:
            entries.append(entry)
    return entries


def _section_at_row(rows: Sequence[tuple[int, str]]) -> list[str]:
    """The section divider (`ARTICLES`, `NOTE`) in force at each row."""
    sections: list[str] = []
    current = ""
    for _, text in rows:
        name = _section_name(text)
        if name:
            current = name
        sections.append(current)
    return sections


def _anchor_page(text: str) -> int:
    """The printed page number this row carries, or 0 if it is not an anchor."""
    leader = _LEADER_ENTRY_RE.match(text)
    match = leader or _TRAILING_ENTRY_RE.match(text)
    if not match:
        return 0
    label = match.group("label").strip()
    if _MASTHEAD_TAIL_RE.search(label) or _ENUMERATOR_RE.match(label):
        return 0
    if leader is None and len(_CAP_TOKEN_RE.findall(label)) < 2:
        return 0
    # `V o l u m e 6 8 - I s s u e 3 - S p r i n g 2 0 2 5` parses as an entry
    # starting on printed page 5. Extractors letter-space small caps, and a
    # letter-spaced line is display type from a cover or masthead, never a
    # contents entry.
    tokens = [token for token in _LEADER_RUN_RE.sub(" ", label).split() if token]
    spaced = sum(1 for token in tokens if len(token) == 1 and token.isalpha())
    if len(tokens) >= 6 and spaced / len(tokens) > 0.4:
        return 0
    value = int(match.group("page"))
    return value if 0 < value < 10000 else 0


def _entries_for_mode(
    rows: Sequence[tuple[int, str]], anchors: Sequence[int], mode: str
) -> list[TocEntry]:
    sections = _section_at_row(rows)
    entries: list[TocEntry] = []
    for order, anchor in enumerate(anchors):
        physical, text = rows[anchor]
        page_number = _anchor_page(text)
        match = _LEADER_ENTRY_RE.match(text) or _TRAILING_ENTRY_RE.match(text)
        label = match.group("label").strip() if match else text

        if mode == "above":
            low = anchors[order - 1] + 1 if order else 0
            window = [item for _, item in rows[low:anchor]]
            parts = [*window, label]
        else:
            high = anchors[order + 1] if order + 1 < len(anchors) else len(rows)
            window = [item for _, item in rows[anchor + 1 : high]]
            parts = [label, *window]

        kept: list[str] = []
        for part in parts:
            if _section_name(part):
                if mode == "below":
                    break  # the divider introduces the NEXT entry
                kept = []  # ...and ends the entry above it
                continue
            kept.append(part)
        if not kept:
            kept = [label]

        entry = _entry_from_parts(kept, "", page_number, sections[anchor], physical)
        if entry is not None:
            entries.append(entry)
    return entries


def _propagate_sections(
    entries: list[TocEntry],
    rows: Sequence[tuple[int, str]],
    anchors: Sequence[int],
    mode: str,
) -> list[TocEntry]:
    """Carry `ARTICLES` / `NOTE` forward to the entries that follow it."""
    section_at: dict[int, str] = {}
    current = ""
    for position, (_, text) in enumerate(rows):
        name = _section_name(text)
        if name:
            current = name
        section_at[position] = current
    out: list[TocEntry] = []
    cursor = 0
    for entry in entries:
        while cursor < len(anchors) and _anchor_page(rows[anchors[cursor]][1]) != entry.printed_page:
            cursor += 1
        position = anchors[cursor] if cursor < len(anchors) else 0
        out.append(
            TocEntry(
                printed_page=entry.printed_page,
                title=entry.title,
                author=entry.author,
                section=entry.section or section_at.get(position, ""),
                raw=entry.raw,
                toc_page_index=entry.toc_page_index,
            )
        )
    return out


def _entry_from_parts(
    parts: list[str], pending_author: str, page_number: int, section: str, toc_page: int
) -> TocEntry | None:
    if not parts:
        return None
    raw = " | ".join(parts)
    author = pending_author
    title_parts: list[str] = []
    for position, part in enumerate(parts):
        head, tail = _split_label(part)
        if head:
            title_parts.append(head)
        if tail and not author:
            author = tail
        elif tail:
            author = f"{author} & {tail}" if tail not in author else author
    if not title_parts:
        title_parts = list(parts)
    # The author usually sits on its own line, either the last line of the entry
    # (with the page number beside it) or the line above the title's last line.
    if not author:
        for position in range(len(title_parts) - 1, -1, -1):
            candidate = title_parts[position]
            if _PERSON_RE.match(candidate) and len(title_parts) > 1:
                author = candidate
                title_parts.pop(position)
                break
    title = _normalize(" ".join(title_parts))
    if not author and re.search(r"(?i)\sby\s", title):
        head, tail = re.split(r"(?i)\sby\s", title, maxsplit=1)
        if _PERSON_RE.match(tail.strip()) and len(head.strip()) >= 3:
            title, author = head.strip(), tail.strip()
    title = title.strip(" .…-·")
    if len(title) < 3 or not re.search(r"[A-Za-z]{3}", title):
        return None
    # `II. THE BACKGROUND AND CURRENT STATUS ... 5` is a section of one article,
    # not a work in the issue. Splitting on these turns a single article into a
    # dozen corrupt children, which is the worst outcome the splitter can have.
    if _ENUMERATOR_RE.match(title):
        return None
    author = re.sub(r"^(?:By|BY)\s+", "", author).strip(" .*†‡")
    return TocEntry(
        printed_page=page_number,
        title=title,
        author=author,
        section=section,
        raw=raw,
        toc_page_index=toc_page,
    )


def _attach_following_authors(
    entries: list[TocEntry], pages: Sequence[Page], indices: Sequence[int]
) -> list[TocEntry]:
    """Pick up authors printed on the line *after* the page number."""
    if not entries:
        return entries
    lines = [line.text.strip() for index in indices for line in pages[index - 1].lines]
    for position, entry in enumerate(entries):
        if entry.author:
            continue
        for cursor, text in enumerate(lines):
            if not text.endswith(str(entry.printed_page)):
                continue
            following = lines[cursor + 1] if cursor + 1 < len(lines) else ""
            if following and _PERSON_RE.match(following) and not _SECTION_RE.match(following):
                entries[position] = TocEntry(
                    printed_page=entry.printed_page,
                    title=entry.title,
                    author=re.sub(r"^(?:By|BY)\s+", "", following).strip(" .*†‡"),
                    section=entry.section,
                    raw=entry.raw,
                    toc_page_index=entry.toc_page_index,
                )
            break
    return entries


def _clean_entries(entries: list[TocEntry]) -> list[TocEntry]:
    """Drop front matter, dedupe, and require a strictly increasing sequence.

    A contents listing is monotonic by construction. A parsed sequence that is
    not monotonic has picked up a stray number, so the longest increasing
    subsequence is taken and the rest discarded -- discarding an entry costs a
    boundary, keeping a wrong one costs a corrupt document.
    """
    kept = [entry for entry in entries if not entry.is_front_matter and entry.printed_page > 0]
    if len(kept) < 2:
        return kept

    # Longest strictly increasing subsequence by printed page (O(n^2) is fine
    # here: contents listings run to a few dozen entries).
    best = [1] * len(kept)
    parent = [-1] * len(kept)
    for i in range(len(kept)):
        for j in range(i):
            if kept[j].printed_page < kept[i].printed_page and best[j] + 1 > best[i]:
                best[i], parent[i] = best[j] + 1, j
    tail = max(range(len(kept)), key=lambda i: best[i])
    chain: list[TocEntry] = []
    while tail != -1:
        chain.append(kept[tail])
        tail = parent[tail]
    chain.reverse()
    return chain


# ---------------------------------------------------------------------------
# Page evidence
# ---------------------------------------------------------------------------


_NUMBER_RE = re.compile(r"(?<!\d)(\d{1,4})(?!\d)")
_ROMAN_RE = re.compile(r"^[ivxlcdm]{1,8}$", re.I)
# `[Vol. 18:909` in a running head carries the ARTICLE's first printed page,
# not the page you are looking at. Read as a folio it matches the contents
# entry on every continuation page of that article, and since a law-review
# opening page often prints no folio at all, the boundary lands one page
# late -- the exact off-by-one this whole design exists to prevent. Found by
# the blind adjudicator disagreeing with the solver on btlj.org.
_VOL_PAGE_RE = re.compile(r"(?i)\bvol(?:ume)?\.?\s*\d+\s*[:.]\s*\d+")
_BYLINE_RE = re.compile(
    r"^(?:BY\s+)?[A-Z][A-Za-zÀ-ÿ.'’\-]+"
    r"(?:\s+[A-Z][A-Za-zÀ-ÿ.'’\-]+){1,4}\s*[\*†‡§]?\s*$"
)
_CUE_RE = re.compile(r"^(?:I\.\s*|1\.\s*)?(?:INTRODUCTION|ABSTRACT|PROLOGUE|FOREWORD)\b", re.I)
_RUNNING_HEAD_RE = re.compile(
    r"\[?\s*Vol(?:ume)?\.?\s*\d|(?:LAW\s+REVIEW|LAW\s+JOURNAL|JOURNAL\s+OF|L\.\s*REV\.|do\s+not\s+delete)",
    re.I,
)
_PRODUCTION_SLUG_RE = re.compile(r"\d{1,2}:\d{2}(:\d{2})?\s*[AP]\.?M\.?$", re.I)
_YEAR_HEAD_RE = re.compile(r"^\[?\s*\d{4}\s*[/\]|]")
_BARE_FOLIO_RE = re.compile(r"^\W*\d{1,4}\W*$")
_HEAD_NOISE_RE = re.compile(r"[^A-Z ]+")


def _head_norm(text: str) -> str:
    return _SPACE_RE.sub(" ", _HEAD_NOISE_RE.sub(" ", text.upper())).strip()


def _upper_share(text: str) -> float:
    letters = [character for character in text if character.isalpha()]
    if not letters:
        return 0.0
    return sum(1 for character in letters if character.isupper()) / len(letters)


def _is_resumed_prose(text: str) -> bool:
    return bool(text) and text[:1].islower() and len(text) >= 40


def _looks_like_sentence_prose(text: str) -> bool:
    if len(text) < 55:
        return False
    words = [word for word in re.findall(r"[A-Za-z][A-Za-z'’\-]*", text) if len(word) > 2]
    if len(words) < 6:
        return False
    return sum(1 for word in words if word[:1].isupper()) / len(words) <= 0.34


def _has_wordlike_content(text: str) -> bool:
    dense = _SPACE_RE.sub("", text)
    letters = [character for character in dense if character.isalpha()]
    return len(letters) >= 6 and len(letters) / max(len(dense), 1) >= 0.5


def _is_head_line(text: str) -> bool:
    if _BARE_FOLIO_RE.match(text) or _RUNNING_HEAD_RE.search(text):
        return True
    if _PRODUCTION_SLUG_RE.search(text) or _YEAR_HEAD_RE.match(text):
        return True
    dense = _SPACE_RE.sub("", text).upper()
    return bool(re.search(r"LAWREVIEW|LAWJOURNAL|JOURNALOF|L\.REV\.|DONOTDELETE|VOL(?:UME)?\.\d", dense))


@dataclass
class PageEvidence:
    """Everything the scorer knows about one physical page."""

    index: int
    folios: set[int] = field(default_factory=set)
    top_folios: set[int] = field(default_factory=set)
    head_key: str = ""
    display_lines: list[str] = field(default_factory=list)
    top_text: str = ""
    title_tokens: set[str] = field(default_factory=set)
    has_byline: bool = False
    has_cue: bool = False
    opens_like_article: bool = False
    continues_prose: bool = False
    is_roster: bool = False
    is_inline_toc: bool = False
    is_blank: bool = False
    max_size_rank: float = 0.0


def _folio_candidates(page: Page) -> tuple[set[int], set[int]]:
    """Numbers printed in the top and bottom margins.

    With geometry the margins are the outer 12% of the page; without it, the
    first and last two lines stand in. Roman numerals are ignored: front matter
    folios do not participate in the arabic stream the solver fits.
    """
    top: set[int] = set()
    bottom: set[int] = set()
    if not page.lines:
        return top, bottom

    has_geometry = page.height > 0 and any(line.has_geometry for line in page.lines)
    if has_geometry:
        # Law-review type blocks are inset well inside the media box: a folio
        # sitting at 85% of page height is in the margin, not in the body. The
        # bands are deliberately generous because the offset fit downstream
        # tolerates noise (it wants a stream that increments) but cannot recover
        # a folio that was never collected.
        top_cut = page.height * 0.18
        bottom_cut = page.height * 0.82
        top_lines = [line for line in page.lines if line.y1 <= top_cut]
        bottom_lines = [line for line in page.lines if line.y0 >= bottom_cut]
    else:
        top_lines = page.lines[:2]
        bottom_lines = page.lines[-2:]
    if not top_lines:
        top_lines = page.lines[:1]
    if not bottom_lines:
        bottom_lines = page.lines[-1:]

    for bucket, lines in ((top, top_lines), (bottom, bottom_lines)):
        for line in lines:
            if _ROMAN_RE.match(line.text.strip()):
                continue
            text = _VOL_PAGE_RE.sub(" ", line.text)
            for match in _NUMBER_RE.finditer(text):
                value = int(match.group(1))
                if 0 < value < 10000:
                    bucket.add(value)
    return top, bottom


def _body_lines(page: Page, repeated_heads: set[str]) -> list[str]:
    """Page lines with the running head stripped off the top."""
    body: list[str] = []
    for line in page.lines:
        text = line.text
        if not body and (_is_head_line(text) or _head_norm(text) in repeated_heads):
            continue
        body.append(text)
    return body


def repeated_head_keys(pages: Sequence[Page], min_count: int = 3) -> set[str]:
    """Head lines identified by repetition rather than by per-domain regex.

    A running head repeats across the document; a display title does not. This
    is journal-independent, which is the whole point -- it replaces a regex per
    domain with one count over the document in hand.
    """
    counter: Counter[str] = Counter()
    for page in pages:
        for line in page.lines[:2]:
            key = _head_norm(line.text)
            if len(key) >= 6:
                counter[key] += 1
    return {key for key, count in counter.items() if count >= min_count}


def build_page_evidence(pages: Sequence[Page]) -> list[PageEvidence]:
    repeated = repeated_head_keys(pages)
    sizes = [line.size for page in pages for line in page.lines if line.size > 0]
    median_size = sorted(sizes)[len(sizes) // 2] if sizes else 0.0

    evidence: list[PageEvidence] = []
    for page in pages:
        top_folios, bottom_folios = _folio_candidates(page)
        body = _body_lines(page, repeated)
        head_key = ""
        for line in page.lines[:2]:
            key = _head_norm(line.text)
            if key in repeated and len(key) >= 6:
                head_key = key
                break

        display = body[:9]
        first = body[0] if body else ""
        shouted_run = 0
        while shouted_run < min(4, len(body)) and len(body[shouted_run]) <= 90 and _upper_share(
            body[shouted_run]
        ) >= 0.7:
            shouted_run += 1
        continues = bool(shouted_run and shouted_run < len(body) and _is_resumed_prose(body[shouted_run]))
        continues = continues or _is_resumed_prose(first) or _looks_like_sentence_prose(first)

        title_like = (
            bool(first)
            and 8 <= len(first) <= 140
            and _has_wordlike_content(first)
            and not _looks_like_sentence_prose(first)
            and (_upper_share(first) >= 0.6 or (first[:1].isupper() and not first.endswith((",", "-", ";"))))
        )
        byline = any(
            _BYLINE_RE.match(line) and len(line) < 60 and len(line.split()) >= 2 for line in display[:9]
        )
        cue = any(_CUE_RE.match(line) for line in display[:9])
        # An `Introduction` cue that replaces the title rather than sitting below
        # one is an in-piece contents page, not an opening (two of the residual
        # false positives in the 2026-08-07 gold set).
        cue_below_title = cue and title_like

        entry_shaped = sum(1 for line in page.lines if _entry_shape(line.text))
        person_lines = sum(1 for line in page.lines if _PERSON_RE.match(line.text))
        non_empty = max(len([line for line in page.lines if line.text]), 1)

        largest = max((line.size for line in page.lines[:6]), default=0.0)

        evidence.append(
            PageEvidence(
                index=page.index,
                folios=top_folios | bottom_folios,
                top_folios=top_folios,
                head_key=head_key,
                display_lines=display,
                top_text=" ".join(display[:6]),
                title_tokens=_content_tokens(" ".join(display[:4])),
                has_byline=byline,
                has_cue=cue_below_title,
                opens_like_article=title_like and (byline or cue_below_title),
                continues_prose=continues,
                is_roster=person_lines >= 6 and person_lines / non_empty >= 0.5,
                # An article's opening page often prints the article's own
                # contents below the byline. That is an opening, not an
                # in-piece contents page, and penalising it pushed the
                # Chapman boundary one page late.
                is_inline_toc=entry_shaped >= 4 and not (title_like and byline),
                is_blank=len(page.lines) == 0,
                max_size_rank=(largest / median_size) if median_size else 0.0,
            )
        )
    return evidence


# ---------------------------------------------------------------------------
# Folio stream
# ---------------------------------------------------------------------------


@dataclass
class FolioFit:
    offset: int | None
    support: float
    runner_up_support: float
    longest_run: int
    n_pages: int
    method: str = ""
    candidates: list[tuple[int, int, int]] = field(default_factory=list)

    @property
    def margin(self) -> float:
        return self.support - self.runner_up_support


def estimate_folio_offset(
    evidence: Sequence[PageEvidence], start_index: int = 1, min_support: float = 0.25
) -> FolioFit:
    """Fit one global ``printed = physical + offset`` by consensus.

    Every folio candidate on every page votes for the offset it implies. The
    winner is the offset with the longest *consecutive run* of agreeing pages,
    not merely the most votes: a header carrying a volume number votes for a
    constant offset too, but it does not increment with the page, so its run is
    short wherever the true stream is present.
    """
    body = [item for item in evidence if item.index >= start_index]
    if not body:
        return FolioFit(None, 0.0, 0.0, 0, 0, "no_pages")

    votes: Counter[int] = Counter()
    for item in body:
        for folio in item.folios:
            votes[folio - item.index] += 1
    if not votes:
        return FolioFit(None, 0.0, 0.0, 0, len(body), "no_folio_candidates")

    scored: list[tuple[int, int, int]] = []
    for offset, count in votes.items():
        agreeing = [item.index for item in body if (item.index + offset) in item.folios]
        if not agreeing:
            continue
        longest, run = 1, 1
        for previous, current in zip(agreeing, agreeing[1:]):
            run = run + 1 if current == previous + 1 else 1
            longest = max(longest, run)
        scored.append((longest, len(agreeing), offset))
    if not scored:
        return FolioFit(None, 0.0, 0.0, 0, len(body), "no_folio_candidates")

    scored.sort(reverse=True)
    candidates = [(offset, count, run) for run, count, offset in scored[:6]]
    longest, support_count, offset = scored[0]
    runner_up = 0
    for _, other_count, other_offset in scored[1:]:
        if abs(other_offset - offset) > 1:
            runner_up = other_count
            break

    support = support_count / len(body)
    fit = FolioFit(
        offset=offset,
        support=support,
        runner_up_support=runner_up / len(body),
        longest_run=longest,
        n_pages=len(body),
        method="consensus_run",
        candidates=candidates,
    )
    if support < min_support or longest < 4:
        return FolioFit(
            None, support, fit.runner_up_support, longest, len(body), "weak_folio_stream", candidates
        )
    return fit


# ---------------------------------------------------------------------------
# Scoring and the monotonic assignment
# ---------------------------------------------------------------------------


@dataclass
class Signals:
    folio: bool = False
    offset_implied: bool = False
    title_similarity: float = 0.0
    author: bool = False
    opening: bool = False
    head_transition: bool = False
    continuation: bool = False
    roster_or_toc: bool = False

    @property
    def strong(self) -> list[str]:
        names = []
        if self.folio:
            names.append("folio")
        if self.title_similarity >= 0.55:
            names.append("title")
        if self.author:
            names.append("author")
        return names


TITLE_STRONG = 0.55
TITLE_WEAK = 0.30

WEIGHTS = {
    "folio": 3.0,
    "offset_implied": 1.5,
    "title_strong": 3.0,
    "title_weak": 1.0,
    "author": 2.5,
    "opening": 1.5,
    "opening_weak": 0.5,
    "head_transition": 0.75,
    "continuation": -3.0,
    "roster_or_toc": -3.0,
    "blank": -1.0,
}


def _similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    overlap = len(left & right)
    if not overlap:
        return 0.0
    return 2 * overlap / (len(left) + len(right))


def _title_similarity(entry: TocEntry, page: PageEvidence) -> float:
    entry_tokens = entry.title_tokens
    if not entry_tokens:
        return 0.0
    best = _similarity(entry_tokens, page.title_tokens)
    # Recall guard: a display title split across lines, letter-spaced by the
    # extractor, or set with a subtitle scores badly on the F1 above. Coverage of
    # the entry's tokens by the page's top region catches those.
    covered = len(entry_tokens & _content_tokens(page.top_text)) / len(entry_tokens)
    return max(best, covered * 0.9)


def _author_match(entry: TocEntry, page: PageEvidence) -> bool:
    surnames = entry.surnames
    if not surnames:
        return False
    haystack = " ".join(page.display_lines[:9]).lower()
    return any(surname.lower() in haystack for surname in surnames)


def score_pair(
    entry: TocEntry,
    page: PageEvidence,
    evidence: Sequence[PageEvidence],
    offset: int | None,
) -> tuple[float, Signals]:
    signals = Signals()
    total = 0.0

    if entry.printed_page in page.folios:
        signals.folio = True
        total += WEIGHTS["folio"]
    if offset is not None and page.index == entry.printed_page - offset:
        signals.offset_implied = True
        total += WEIGHTS["offset_implied"]

    similarity = _title_similarity(entry, page)
    signals.title_similarity = similarity
    if similarity >= TITLE_STRONG:
        total += WEIGHTS["title_strong"] * similarity
    elif similarity >= TITLE_WEAK:
        total += WEIGHTS["title_weak"] * similarity

    if _author_match(entry, page):
        signals.author = True
        total += WEIGHTS["author"]

    if page.opens_like_article:
        signals.opening = True
        total += WEIGHTS["opening"]
    elif page.has_byline or page.has_cue:
        total += WEIGHTS["opening_weak"]

    # Corroboration only: a new stable head appearing within two pages is
    # consistent with a boundary here, and can never place one.
    position = page.index - 1
    previous_key = evidence[position - 1].head_key if position - 1 >= 0 else ""
    for lookahead in range(0, 3):
        target = position + lookahead
        if target >= len(evidence):
            break
        key = evidence[target].head_key
        if key and key != previous_key:
            signals.head_transition = True
            total += WEIGHTS["head_transition"]
            break

    if page.continues_prose:
        signals.continuation = True
        total += WEIGHTS["continuation"]
    if page.is_roster or page.is_inline_toc:
        signals.roster_or_toc = True
        total += WEIGHTS["roster_or_toc"]
    if page.is_blank:
        total += WEIGHTS["blank"]

    return total, signals


@dataclass
class Assignment:
    entry: TocEntry
    page: int
    score: float
    signals: Signals
    margin: float = 0.0
    runner_up_page: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "printed_page": self.entry.printed_page,
            "title": self.entry.title,
            "author": self.entry.author,
            "section": self.entry.section,
            "physical_page": self.page,
            "score": round(self.score, 3),
            "margin": round(self.margin, 3) if math.isfinite(self.margin) else None,
            "runner_up_page": self.runner_up_page or None,
            "signals": {
                "folio": self.signals.folio,
                "offset_implied": self.signals.offset_implied,
                "title_similarity": round(self.signals.title_similarity, 3),
                "author": self.signals.author,
                "opening": self.signals.opening,
                "head_transition": self.signals.head_transition,
                "continuation": self.signals.continuation,
                "roster_or_toc": self.signals.roster_or_toc,
                "strong": self.signals.strong,
            },
        }


@dataclass
class SolveResult:
    status: str  # auto | review | abstain
    reason: str
    assignments: list[Assignment] = field(default_factory=list)
    folio: FolioFit | None = None
    n_toc_entries: int = 0
    n_pages: int = 0
    total_score: float = 0.0

    @property
    def start_pages(self) -> list[int]:
        return [assignment.page for assignment in self.assignments]

    def ledger(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "n_pages": self.n_pages,
            "n_toc_entries": self.n_toc_entries,
            "total_score": round(self.total_score, 3),
            "folio": (
                {
                    "offset": self.folio.offset,
                    "support": round(self.folio.support, 3),
                    "runner_up_support": round(self.folio.runner_up_support, 3),
                    "longest_run": self.folio.longest_run,
                    "method": self.folio.method,
                }
                if self.folio
                else None
            ),
            "assignments": [assignment.to_dict() for assignment in self.assignments],
        }


MIN_GAP = 2
NEG_INF = float("-inf")


def _assign(
    entries: Sequence[TocEntry],
    evidence: Sequence[PageEvidence],
    offset: int | None,
    window: int,
    forbidden: tuple[int, int] | None = None,
) -> tuple[float, list[int]] | None:
    """Monotonic maximum-score assignment of contents entries to physical pages.

    ``dp[i][p]`` is the best score for placing entries ``0..i`` with entry ``i``
    on page ``p``; a prefix maximum over the previous row makes it linear in
    pages. This is what makes the boundaries a joint decision rather than a
    sequence of independent guesses.
    """
    n_pages = len(evidence)
    if not entries or not n_pages:
        return None

    allowed: list[list[int]] = []
    for index, entry in enumerate(entries):
        if offset is None:
            candidates = list(range(1, n_pages + 1))
        else:
            implied = entry.printed_page - offset
            low = max(1, implied - window)
            high = min(n_pages, implied + window)
            candidates = list(range(low, high + 1))
            # Never lose a page that carries hard evidence just because the
            # global offset is off; a folio or title hit re-admits it.
            for page in evidence:
                if page.index in candidates:
                    continue
                if entry.printed_page in page.folios or _title_similarity(entry, page) >= TITLE_STRONG:
                    candidates.append(page.index)
            candidates.sort()
        if forbidden is not None and forbidden[0] == index:
            candidates = [page for page in candidates if page != forbidden[1]]
        if not candidates:
            return None
        allowed.append(candidates)

    scores: list[dict[int, tuple[float, Signals]]] = []
    for index, entry in enumerate(entries):
        row: dict[int, tuple[float, Signals]] = {}
        for page_index in allowed[index]:
            row[page_index] = score_pair(entry, evidence[page_index - 1], evidence, offset)
        scores.append(row)

    # dp over rows with a running prefix maximum of the previous row.
    previous_best: list[float] = [NEG_INF] * (n_pages + 2)
    previous_arg: list[int] = [-1] * (n_pages + 2)
    back: list[dict[int, int]] = []

    for index in range(len(entries)):
        current = {page: scores[index][page][0] for page in allowed[index]}
        row_back: dict[int, int] = {}
        if index == 0:
            row_values = dict(current)
        else:
            row_values = {}
            for page in allowed[index]:
                cutoff = page - MIN_GAP
                if cutoff < 1 or previous_best[cutoff] == NEG_INF:
                    continue
                row_values[page] = current[page] + previous_best[cutoff]
                row_back[page] = previous_arg[cutoff]
            if not row_values:
                return None
        back.append(row_back)

        running, running_arg = NEG_INF, -1
        new_best = [NEG_INF] * (n_pages + 2)
        new_arg = [-1] * (n_pages + 2)
        for page in range(1, n_pages + 1):
            value = row_values.get(page, NEG_INF)
            if value > running:
                running, running_arg = value, page
            new_best[page], new_arg[page] = running, running_arg
        previous_best, previous_arg = new_best, new_arg

    total = previous_best[n_pages]
    if total == NEG_INF:
        return None
    path = [previous_arg[n_pages]]
    for index in range(len(entries) - 1, 0, -1):
        path.append(back[index][path[-1]])
    path.reverse()
    return total, path


def _offset_agreeing_with_toc(
    folio: FolioFit, entries: Sequence[TocEntry], evidence: Sequence[PageEvidence]
) -> FolioFit:
    """Let the contents listing arbitrate between competing folio streams.

    Digital Commons stamps its own sequential page number on every page of a
    scanned issue. That stamp is a *perfect* incrementing stream, so it wins the
    consensus fit outright (`nsuworks.nova.edu/Vol._38_2C_Number_3.pdf`: offset
    -1 at support 1.00, against the journal's real folios 387-523 at 0.857) and
    every entry then looks like it belongs to no page in the document.

    The listing is the specification, so it breaks the tie: among the candidate
    offsets, prefer the one under which the most entries land on a page that
    actually prints their number.
    """
    if not folio.candidates or not entries:
        return folio
    # Only a stream with real support may displace the incumbent. Without this
    # floor the arbitration will happily invent an offset that makes two stray
    # numbers from a data table look like contents entries, which is precisely
    # the failure the folio-range veto exists to catch.
    viable = [
        (offset, count, run)
        for offset, count, run in folio.candidates
        if count >= max(8, 0.2 * folio.n_pages) and run >= 4
    ]
    if not viable:
        return folio
    best_offset, best_hits = folio.offset, -1
    for offset, _count, _run in viable:
        hits = 0
        for entry in entries:
            index = entry.printed_page - offset
            if 1 <= index <= len(evidence) and entry.printed_page in evidence[index - 1].folios:
                hits += 1
        if hits > best_hits:
            best_offset, best_hits = offset, hits
    if best_hits <= 0 or best_offset == folio.offset:
        return folio
    return FolioFit(
        offset=best_offset,
        support=folio.support,
        runner_up_support=folio.runner_up_support,
        longest_run=folio.longest_run,
        n_pages=folio.n_pages,
        method="toc_selected_stream",
        candidates=folio.candidates,
    )


def solve(
    pages: Sequence[Page],
    *,
    window: int = 8,
    min_margin: float = 2.0,
    min_articles: int = 2,
) -> SolveResult:
    """Assign contents entries to physical pages and decide what to emit."""
    n_pages = len(pages)
    if n_pages < 20:
        return SolveResult("abstain", "too_few_pages", n_pages=n_pages)

    toc_indices = find_toc_pages(pages)
    entries = parse_toc_entries(pages, toc_indices)
    if len(entries) < min_articles:
        return SolveResult("abstain", "no_usable_toc", n_pages=n_pages, n_toc_entries=len(entries))

    evidence = build_page_evidence(pages)
    body_start = (max(toc_indices) + 1) if toc_indices else 1
    folio = estimate_folio_offset(evidence, start_index=body_start)

    folio = _offset_agreeing_with_toc(folio, entries, evidence)

    # The listing's numbers must live in the same number space as the printed
    # folios. `tilj.org/tilj-59n3-text-cavallaro.pdf` is a single article running
    # printed pp. 153-182 whose ICC-convictions table parses as a listing with
    # start pages 3 and 5; nothing about the table's shape gives it away, but
    # those numbers are nowhere in the document's folio stream. This is the
    # check that makes TOC+folio mutually *checkable* rather than merely
    # combined.
    if folio.offset is not None:
        agreeing = [
            item.index + folio.offset
            for item in evidence
            if (item.index + folio.offset) in item.folios
        ]
        if agreeing:
            low, high = min(agreeing) - 5, max(agreeing) + 5
            inside = sum(1 for entry in entries if low <= entry.printed_page <= high)
            if inside / len(entries) < 0.5:
                return SolveResult(
                    "abstain",
                    f"toc_outside_folio_range:{low}-{high}",
                    folio=folio,
                    n_pages=n_pages,
                    n_toc_entries=len(entries),
                )

    solved = _assign(entries, evidence, folio.offset, window)
    if solved is None:
        return SolveResult(
            "abstain", "no_feasible_assignment", folio=folio, n_pages=n_pages, n_toc_entries=len(entries)
        )
    total, path = solved

    assignments: list[Assignment] = []
    for index, (entry, page_index) in enumerate(zip(entries, path)):
        score, signals = score_pair(entry, evidence[page_index - 1], evidence, folio.offset)
        assignments.append(Assignment(entry=entry, page=page_index, score=score, signals=signals))

    # Per-entry margin: how much worse the best *global* assignment becomes when
    # this entry is forbidden its chosen page. A boundary the document does not
    # actually determine has a margin near zero, whatever its own score is.
    for index, assignment in enumerate(assignments):
        alternative = _assign(entries, evidence, folio.offset, window, forbidden=(index, assignment.page))
        if alternative is None:
            assignment.margin = float("inf")
        else:
            assignment.margin = total - alternative[0]
            # Where this entry goes when its chosen page is taken away. This is
            # the boundary an adjudicator actually has to choose between, so it
            # is recorded rather than recomputed downstream.
            assignment.runner_up_page = alternative[1][index]

    status, reason = _emission_policy(assignments, folio, min_margin)
    return SolveResult(
        status=status,
        reason=reason,
        assignments=assignments,
        folio=folio,
        n_toc_entries=len(entries),
        n_pages=n_pages,
        total_score=total,
    )


def _emission_policy(
    assignments: Sequence[Assignment], folio: FolioFit, min_margin: float
) -> tuple[str, str]:
    """Three-way decision, requiring per-entry evidence rather than aggregates.

    "The contents listing counts about as many entries as the head rule found"
    is not evidence about any individual boundary and does not appear here.
    Agreement is required entry by entry:

      * folio agreement + (title or author) agreement, or
      * title + author agreement + an opening-page appearance.

    A running-head transition is never sufficient on its own.
    """
    substantive = [
        assignment
        for assignment in assignments
        if not (assignment.page <= 1 and assignment is assignments[0])
    ]
    if len(substantive) < 1:
        return "abstain", "no_substantive_boundaries"

    failures: list[str] = []
    weak: list[str] = []
    for assignment in substantive:
        signals = assignment.signals
        strong = set(signals.strong)
        two_ways = ("folio" in strong and (strong & {"title", "author"})) or (
            {"title", "author"} <= strong and signals.opening
        )
        if not two_ways:
            (weak if strong else failures).append(f"p{assignment.page}")
        if signals.continuation:
            failures.append(f"p{assignment.page}:continuation")
        if assignment.margin < min_margin:
            weak.append(f"p{assignment.page}:margin{assignment.margin:.1f}")

    if failures:
        return "abstain", "insufficient_evidence:" + ",".join(sorted(set(failures))[:6])
    if weak:
        return "review", "single_strong_signal:" + ",".join(sorted(set(weak))[:6])
    if folio.offset is None:
        return "review", "no_folio_offset"
    return "auto", "two_strong_signals_per_boundary"


def solve_pdf(pdf_path: str, **kwargs: Any) -> SolveResult:
    return solve(extract_pages(pdf_path), **kwargs)
