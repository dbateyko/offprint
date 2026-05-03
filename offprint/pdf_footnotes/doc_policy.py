from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .text_extract import suppress_pypdf_noise

DOC_POLICIES = {"article_only", "include_issue_compilations", "all"}
DOC_TYPES = {"article", "issue_compilation", "frontmatter", "other", "needs_ocr"}

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
_NON_ARTICLE_FILENAME_RE = re.compile(
    r"(?:"
    r"coversheet|bibliography|announce(?:ment)?|notice|staff(?:[-_ ]?(?:member|app(?:lication)?|writer))?|"
    r"program|picture|non[-_ ]?discrimination|"
    r"brochure|bulletin|blueprint|view(?:book|piece)|ebriefing|"
    r"one[-_ ]?pager|\d+[-_ ]?pager|weekly[-_ ]?report|testimony|application|"
    r"call[-_ ]?for[-_ ]?papers|rules[-_ ]of[-_ ]procedure|"
    r"eprs[-_ ]stu|icct[-_ ]report|summary[-_ ]?charts?|"
    r"tax[-_ ]?(?:checklist|estimate)|opening[-_ ]remarks|"
    r"case[-_ ]brief|land[-_ ]?acknowledgment|masthead|editorial[-_ ]?board|"
    r"jd[-_ ]?brochure|employer[-_ ]certification|lrap|"
    r"(?:^|[-_])transcript(?:[-_]|$)|"
    r"info[-_ ]?sheet|book[-_ ]?review|agenda|sponsors?|"
    r"(?:^|[-_])online[-_ ]?(?:supplement|symposium|essay|appendix|edition)|"
    r"moot[-_ ]issue|"
    r"financial[-_ ]statements?|"
    r"instrument[-_ ]of[-_ ]accession|treaty[-_ ]of[-_ ]|convention[-_ ]on[-_ ]|"
    r"books[-_ ]received|"
    r"(?:^|[-_])legal[-_ ]help(?:[-_]|\.|$)|"
    r"std[-_ ]?509|aba[-_ ]509|"
    r"enforcement[-_ ]report[-_ ]fy|annual[-_ ]report[-_ ]fy|"
    r"licensing[-_ ]letter|open[-_ ]letter|"
    r"(?:^|[-_])docket[-_ ]|"
    r"(?:^|[-_])brief\.pdf$|(?:^|[-_])brief[-_]\d|"
    # Residual triage 2026-05-02: handouts, contest source lists, and
    # publisher-specific non-article slugs caught in the article bucket
    # (commons.stmarytx Gold & Blue alumni mag, Victoria Legal Aid resource
    # handouts on yjil.org, derecho.uprrp.edu Spanish citation tables, JCS
    # contest source-material lists, ELR Pro extracted issue subsections,
    # ACUTA telecommunications-journal issue compilations on unl).
    r"vla[-_ ]?resource|tabla[-_ ]?de[-_ ]?citaci|"
    r"list[-_ ]?of[-_ ]?all[-_ ]?source[-_ ]?material|remix[-_ ]?art[-_ ]?contest|"
    r"gold[-_ ]?(?:and|&|n)[-_ ]?blue|acutajournal|"
    r"^elpar[-_ ]?\d+[-_ ]?copyright"
    r")",
    re.IGNORECASE,
)
_ROUNDTABLE_FILENAME_RE = re.compile(r"round[-_ ]?table|roundtable", re.IGNORECASE)

# Seasonal alumni-magazine filename pattern (e.g. "2011-fall.pdf",
# "2018_winter.pdf"). Cornell Law Forum is the canonical case: 82-pp issues
# get routed to the article bucket today and produce zero usable footnotes.
# Combined with a >=30 page count or a magazine title-text marker downstream,
# this becomes a confident `other` classification.
_SEASONAL_MAGAZINE_FILENAME_RE = re.compile(
    r"^\d{4}[-_ ]?(spring|fall|winter|summer|autumn)(?:[-_ ]|\.pdf$|$)",
    re.IGNORECASE,
)

# Bepress / repository coversheets render their first page as the recommended-
# citation block prefixed by the publication's section title — "Book Reviews",
# "Contents", "Front Matter", "Masthead". When the entire document is short
# (≤10 pp) and the first page opens with one of these, it's a coversheet, not
# a real article. The bepress download-citation block also creates 3-7 spurious
# numeric "footnote markers" (Volume/Issue/Article ids and OSCOLA/AGLC format
# snippets) that fool the article_like signal.
_BEPRESS_COVERSHEET_TITLE_RE = re.compile(
    r"^\s*(?:book\s*reviews?|contents|front[-\s]?matter|masthead|"
    r"editorial\s*board|table\s+of\s+contents)\s*$",
    re.IGNORECASE | re.MULTILINE,
)

# Court-filing markers: NYLS DigitalCommons hosts a court-filings repository
# whose PDFs land in our article bucket because they pass the bepress
# article-shape signals. The filing-type heading is a unique signature.
_COURT_FILING_TITLE_RE = re.compile(
    r"\b(?:petition\s+for\s+(?:rehearing|certiorari|writ)|"
    r"(?:rehearing\s+)?en\s+banc|"
    r"brief\s+(?:for|of|in\s+(?:support|opposition))|"
    r"motion\s+to\s+(?:dismiss|compel|strike)|"
    r"reply\s+brief)\b",
    re.IGNORECASE,
)

# Panel-transcript marker: a row of speaker tags ("MR. SMITH:", "PROFESSOR
# JONES:") repeated 3+ times signals a symposium/colloquium transcript rather
# than a footnoted article. (Real articles quote speakers but never structure
# the body around speaker turns.)
_PANEL_SPEAKER_RE = re.compile(
    r"(?:^|\n)\s*(?:MR|MS|MRS|DR|PROFESSOR|JUDGE|JUSTICE|SENATOR|REP)\.?\s+"
    r"[A-Z][A-Z]+:\s",
)

# Path-based skip patterns. Matches the absolute or relative PDF path; used
# to exclude legacy / manually-imported corpora that violate the
# `corpus/scraped/<domain>/<file>.pdf` flat-by-domain convention. Routed to
# `other` rather than deleted so downstream consumers can still find the
# files if they need them.
#
# - `www.stetson.edu/Volume NN/...` : 608 PDFs manually imported pre-offprint
#   (predates the scraper; see `corpus/scraped/www.stetson.edu/Volume 30/Codex.code-workspace`
#   and the `paused_waf` registry status). Real Stetson Law Review articles,
#   but their nested-by-volume layout breaks the domain-extraction logic and
#   skews per-publisher quality metrics. Documented 2026-05-03.
_LEGACY_PATH_SKIP_RE = re.compile(
    r"/www\.stetson\.edu/[Vv]olume[ _-]?\d+/",
)


@dataclass(frozen=True)
class DocSignals:
    page_count: int
    first_page_text: str
    citation_pattern_count: int
    footnote_marker_count: int
    metadata_article_fields: bool
    garbled_text: bool
    # Average chars per page sampled across the document body. None means the
    # caller did not probe (legacy path); below the OCR threshold means the
    # PDF lacks a usable text layer (scanned image-only pages) and should be
    # routed to `needs_ocr` rather than counted as an article-bucket failure.
    text_density_per_page: float | None = None


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


def probe_text_density(pdf_path: str, page_count: int) -> float | None:
    """Return average chars/page across up to 5 sampled pages (1, 25%, 50%,
    75%, last). Returns None if the PDF can't be opened.

    Used to detect scanned-image PDFs whose text layer is empty or near-empty
    on most pages. The footnote pipeline's solver and segmenter cannot
    recover footnotes from such docs regardless of heuristic improvements;
    they belong in the `needs_ocr` routing class rather than the article
    failure bucket.
    """
    if page_count <= 0:
        return None
    try:
        suppress_pypdf_noise()
        from pypdf import PdfReader  # type: ignore
    except Exception:
        return None
    try:
        reader = PdfReader(pdf_path)
        n = len(reader.pages)
        if n == 0:
            return None
        # Sample up to 5 pages spread across the doc (deduplicated).
        idxs = sorted({0, n // 4, n // 2, (3 * n) // 4, n - 1} & set(range(n)))
        total_chars = 0
        for i in idxs:
            try:
                txt = reader.pages[i].extract_text() or ""
            except Exception:
                txt = ""
            total_chars += len(txt.strip())
        return float(total_chars) / max(len(idxs), 1)
    except Exception:
        return None


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


def collect_signals(
    first_page_text: str,
    page_count: int,
    metadata: dict[str, Any] | None,
    *,
    text_density_per_page: float | None = None,
) -> DocSignals:
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
        text_density_per_page=text_density_per_page,
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

    # Legacy-import path skip (see _LEGACY_PATH_SKIP_RE). Fires before all
    # other rules — these PDFs violate the flat-by-domain corpus convention
    # and shouldn't be classified as articles.
    if _LEGACY_PATH_SKIP_RE.search(pdf_path):
        return DocDecision(
            doc_type="other",
            include=should_include_doc("other", doc_policy),
            reason_codes=["legacy_import_path"],
            confidence=0.99,
            platform_family=platform_family,
            domain=domain,
            ocr_candidate=False,
        )

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
        elif _NON_ARTICLE_FILENAME_RE.search(filename_l):
            doc_type = "other"
            reason_codes.append("non_article_filename")
            confidence = 0.96
            strong_frontmatter = True
        elif _SEASONAL_MAGAZINE_FILENAME_RE.search(filename_l) and (
            signals.page_count >= 30
            or re.search(r"\b(forum|magazine|alumni|bulletin|review[-\s]online)\b", text_l)
        ):
            # Cornell Law Forum-style alumni magazines: 30+ pp seasonal issues
            # whose footnote markers are actually photo credits / page refs.
            doc_type = "other"
            reason_codes.append("seasonal_alumni_magazine")
            confidence = 0.95
            strong_frontmatter = True
        elif (
            signals.page_count <= 10
            and _BEPRESS_COVERSHEET_TITLE_RE.search(signals.first_page_text)
        ):
            # Bepress coversheets (Book Reviews, Contents, Front Matter, etc.)
            # passing as articles because the recommended-citation block
            # produces ≥2 numeric "footnote markers".
            doc_type = "frontmatter"
            reason_codes.append("bepress_coversheet_title")
            confidence = 0.96
            strong_frontmatter = True
        elif _COURT_FILING_TITLE_RE.search(signals.first_page_text):
            # Court filings (Petition for Rehearing, Brief for / Brief of, En
            # Banc, motions) — appear in NYLS DigitalCommons court-filings
            # repository and others. Not articles.
            doc_type = "other"
            reason_codes.append("court_filing_title")
            confidence = 0.96
            strong_frontmatter = True
        elif len(_PANEL_SPEAKER_RE.findall(signals.first_page_text)) >= 3:
            # Panel/symposium transcripts: 3+ "MR. SMITH:" / "PROFESSOR JONES:"
            # speaker tags on the first page indicate a colloquium transcript
            # rather than a footnoted article.
            doc_type = "other"
            reason_codes.append("panel_transcript")
            confidence = 0.92
            strong_frontmatter = True
        elif _ROUNDTABLE_FILENAME_RE.search(filename_l):
            doc_type = "issue_compilation" if signals.page_count >= 20 else "other"
            reason_codes.append("roundtable_filename")
            confidence = 0.92
            strong_frontmatter = True
        elif signals.page_count > 200:
            # Docs over 200 pages are almost always issue compilations, books,
            # or government/institutional reports. Single law-review articles
            # essentially never reach this length in the corpus.
            doc_type = "issue_compilation"
            reason_codes.append("long_doc_page_count")
            confidence = 0.90
            strong_frontmatter = True
        elif (
            signals.page_count > 120
            and signals.footnote_marker_count < 3
            and not signals.metadata_article_fields
        ):
            # 120+ pp with essentially no footnote markers on the first page AND
            # no scraped article metadata is consistent with a report or
            # institutional document. Real articles have footnote markers on
            # their opening pages.
            doc_type = "issue_compilation"
            reason_codes.append("long_doc_no_notes_no_metadata")
            confidence = 0.80
            strong_frontmatter = True
        elif signals.page_count <= 3 and signals.footnote_marker_count < 3:
            doc_type = "frontmatter"
            reason_codes.append("short_doc_without_footnotes")
            confidence = 0.9
            strong_frontmatter = True
        elif platform_family == "digital_commons" and signals.page_count <= 5 and _DC_WRAPPER_RE.search(
            text_l
        ):
            doc_type = "frontmatter"
            reason_codes.append("dc_repository_wrapper")
            confidence = 0.98
            strong_frontmatter = True
        elif re.search(
            r"\b(editorial\s*board|masthead|inside[-\s]?cover|dedication|"
            r"foreword|preface|prolog(?:ue)?|errat[ao]|in\s+memoriam|memorial|"
            r"letter\s+from|front[-\s]?matter|back[-\s]?cover|front[-\s]?cover|"
            r"agenda|sponsors?|symposium|transcript|ceremony|keynote|speech|keynote\s+address)\b",
            filename_l + " " + text_l,
        ):
            doc_type = "frontmatter"
            reason_codes.append("frontmatter_marker")
            confidence = 0.96
            strong_frontmatter = True
        elif re.search(
            r"(?:^|[-_])(?:fm|bm)[-_]|symposium[-_](?:agenda|program|color|schedule)|"
            r"\btribute\b|\bforum\b(?!.*article)",
            filename_l,
        ) and signals.footnote_marker_count < 5:
            doc_type = "frontmatter"
            reason_codes.append("frontmatter_filename_weak_signal")
            confidence = 0.9
            strong_frontmatter = True
        elif "table of contents" in text_l or re.search(r"\b(toc|contents)\b", filename_l):
            # Only treat as frontmatter/compilation if it doesn't look like an article.
            # Real articles often start with a TOC.
            if not signals.metadata_article_fields and signals.footnote_marker_count < 3:
                doc_type = "issue_compilation" if signals.page_count > 10 else "frontmatter"
                reason_codes.append("toc_marker")
                confidence = 0.95
                strong_frontmatter = True
        elif "yearbook" in text_l or "yearbook" in filename_l:
            doc_type = "other"
            reason_codes.append("yearbook_marker")
            confidence = 0.98
            strong_frontmatter = True

    # OCR gate: if we probed text density and the doc averages < 200 chars per
    # page across its sampled pages, the text layer is unusable for footnote
    # extraction (scanned-image PDF, severely corrupted CMap, or non-Latin
    # script with broken encoding). Route to `needs_ocr` so it leaves the
    # article denominator and joins the OCR backlog rather than being marked
    # as `empty`/`invalid`. Only fires when no stronger classification has
    # already triggered (strong_frontmatter would have set doc_type to
    # frontmatter / other / issue_compilation already).
    #
    # Threshold: 200 chars/page averaged over 5 sampled pages. A genuine law-
    # review article is consistently 1500-3000 cpp; book-review squibs and
    # short covers have already been caught by earlier rules. Values below
    # 200 are essentially "nothing extractable" — any successful run on such
    # a doc is a bookkeeping accident, not a real recovery.
    if (
        not strong_frontmatter
        and doc_type == "article"
        and signals.text_density_per_page is not None
        and signals.text_density_per_page < 200.0
        and signals.page_count >= 4
    ):
        doc_type = "needs_ocr"
        reason_codes.append("low_text_density")
        confidence = 0.9

    # Short docs (covers, tributes, agendas) often carry scraped metadata but
    # lack the body signals of a real article. Require corroborating content
    # signals beyond metadata alone before labeling as article.
    short_doc = signals.page_count > 0 and signals.page_count < 6
    strong_body_signals = (
        signals.citation_pattern_count >= 2
        or signals.footnote_marker_count >= 5
        or ("abstract" in text_l and "introduction" in text_l)
    )
    article_like = strong_body_signals or (
        signals.metadata_article_fields and not short_doc and signals.footnote_marker_count >= 2
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
