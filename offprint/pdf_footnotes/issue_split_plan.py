"""Boundary source selection for issue splitting: TOC solver first, heads second.

This is the wiring layer between :mod:`toc_solver` (which decides *where* the
boundaries are and explains why) and the split runners (which write child PDFs).
It owns no boundary logic of its own; it only

1. asks :func:`toc_solver.solve` for an assignment and accepts it if its tier is
   at or above the requested one (``auto`` by default -- see below),
2. falls back to the running-head splitter in :mod:`issue_splitter` when the
   solver declines, and
3. renders the decision as a set of spans plus a provenance record complete
   enough to audit and reverse any child document.

**Why ``auto`` is the default.** The cost is asymmetric. A mid-article cut
yields two corrupt documents that enter the citation graph as real; a missed
boundary leaves a compilation unsplit, which is the status quo. The ``review``
tier is measured clean on the (small) gold sets but 12.6% of its weak boundaries
still disagree with an adjudicator that is 98.6% accurate on easy cases, so it
is opt-in per run, never a default.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

from . import toc_solver as T
from .issue_splitter import infer_law_review_boundaries, load_head_rules

__all__ = [
    "ChildSpan",
    "BoundaryPlan",
    "PROVENANCE_SCHEMA",
    "TIER_RANK",
    "plan_from_pages",
    "plan_pdf",
    "child_provenance",
]

PROVENANCE_SCHEMA = "offprint.issue_split.provenance.v1"

# `abstain` is deliberately absent: it is not a tier anything may be emitted at.
TIER_RANK = {"auto": 0, "review": 1}

MIN_CHILDREN = 2


@dataclass(frozen=True)
class ChildSpan:
    """One child document: an inclusive one-based page range of the parent."""

    index: int
    start_page: int
    end_page: int
    title: str = ""
    author: str = ""
    section: str = ""
    printed_page: int | None = None
    is_front_matter: bool = False
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def n_pages(self) -> int:
        return self.end_page - self.start_page + 1


@dataclass(frozen=True)
class BoundaryPlan:
    """What to write for one parent, and the evidence that decided it."""

    ok: bool
    source: str  # toc_solver | running_head | ""
    tier: str  # auto | review | "" (fallback carries no tier)
    reason: str  # emission reason, or the skip reason when not ok
    spans: list[ChildSpan] = field(default_factory=list)
    n_pages: int = 0
    ledger: dict[str, Any] = field(default_factory=dict)
    solver_status: str = ""
    solver_reason: str = ""

    @property
    def front_matter_pages(self) -> int:
        """Parent pages preceding the first child; dropped, not lost."""
        return (self.spans[0].start_page - 1) if self.spans else 0


def _spans_from_assignments(
    assignments: Sequence[T.Assignment], n_pages: int
) -> list[ChildSpan]:
    starts = [assignment.page for assignment in assignments]
    spans: list[ChildSpan] = []
    for index, assignment in enumerate(assignments):
        start = starts[index]
        end = (starts[index + 1] - 1) if index + 1 < len(starts) else n_pages
        if end < start:
            continue
        detail = assignment.to_dict()
        spans.append(
            ChildSpan(
                index=len(spans) + 1,
                start_page=start,
                end_page=end,
                title=assignment.entry.title,
                author=assignment.entry.author,
                section=assignment.entry.section,
                printed_page=assignment.entry.printed_page,
                is_front_matter=assignment.entry.is_front_matter,
                evidence={
                    "score": detail["score"],
                    "margin": detail["margin"],
                    "runner_up_page": detail["runner_up_page"],
                    "signals": detail["signals"],
                },
            )
        )
    return spans


def plan_from_pages(
    pages: Sequence[T.Page],
    domain: str = "",
    *,
    tier: str = "auto",
    fallback: str = "none",
    head_rules: dict[str, Any] | None = None,
    solve_kwargs: dict[str, Any] | None = None,
) -> BoundaryPlan:
    """Decide boundaries for one already-extracted document.

    ``tier`` is the *lowest* solver tier that may be emitted unattended.
    ``fallback`` is ``running_head`` or ``none``.

    The fallback runs **only when the solver abstains** -- when it has no
    opinion at all. A `review` document is one the solver has an opinion about
    and is unsure of, and letting the head rule overrule that is strictly worse
    than leaving it unsplit: measured on the 2026-08-09 trial set, both
    fallback firings were on `review` documents and one
    (`btlj.org/0000-36-1-full-issue-2.pdf`, 324 pp) produced two children whose
    first pages are mid-sentence continuation prose.

    ``fallback`` therefore defaults to ``none`` even though the running-head
    splitter is the status-quo production path. Enable it per run, knowingly.
    """
    if tier not in TIER_RANK:
        raise ValueError(f"tier must be one of {sorted(TIER_RANK)}, got {tier!r}")

    n_pages = len(pages)
    result = T.solve(pages, **(solve_kwargs or {}))
    ledger = result.ledger()

    if result.status in TIER_RANK and TIER_RANK[result.status] <= TIER_RANK[tier]:
        spans = _spans_from_assignments(result.assignments, n_pages)
        if len(spans) >= MIN_CHILDREN:
            return BoundaryPlan(
                ok=True,
                source="toc_solver",
                tier=result.status,
                reason=result.reason,
                spans=spans,
                n_pages=n_pages,
                ledger=ledger,
                solver_status=result.status,
                solver_reason=result.reason,
            )
        declined = f"too_few_children:{len(spans)}"
    elif result.status in TIER_RANK:
        # Emitted, but below the tier this run permits.
        declined = f"tier_below_threshold:{result.status}"
    else:
        declined = f"{result.status}:{result.reason}"

    if fallback != "running_head" or result.status in TIER_RANK:
        return BoundaryPlan(
            ok=False,
            source="",
            tier="",
            reason=declined,
            n_pages=n_pages,
            ledger=ledger,
            solver_status=result.status,
            solver_reason=result.reason,
        )

    page_texts = [page.text for page in pages]
    inference = infer_law_review_boundaries(
        page_texts, domain, head_rules if head_rules is not None else load_head_rules()
    )
    if not inference.ok:
        return BoundaryPlan(
            ok=False,
            source="",
            tier="",
            reason=f"{declined}|head_fallback:{inference.skip_reason}",
            n_pages=n_pages,
            ledger=ledger,
            solver_status=result.status,
            solver_reason=result.reason,
        )

    spans = [
        ChildSpan(
            index=index,
            start_page=boundary.start_page,
            end_page=boundary.end_page,
            title=boundary.title_guess,
            evidence={"method": boundary.method, "confidence": boundary.confidence},
        )
        for index, boundary in enumerate(inference.boundaries, start=1)
    ]
    if len(spans) < MIN_CHILDREN:
        return BoundaryPlan(
            ok=False,
            source="",
            tier="",
            reason=f"{declined}|head_fallback:too_few_children",
            n_pages=n_pages,
            ledger=ledger,
            solver_status=result.status,
            solver_reason=result.reason,
        )

    return BoundaryPlan(
        ok=True,
        source="running_head",
        tier="",
        reason=inference.method,
        spans=spans,
        n_pages=n_pages,
        ledger={
            "solver": ledger,
            "head_rule": {
                "method": inference.method,
                "confidence": inference.confidence,
                "domain": domain,
            },
        },
        solver_status=result.status,
        solver_reason=result.reason,
    )


def plan_pdf(pdf_path: str, domain: str = "", **kwargs: Any) -> BoundaryPlan:
    """Extract a PDF and plan its split. Call from a *process*, never a thread.

    :func:`toc_solver.extract_pages` uses PyMuPDF, which is not thread-safe; a
    ThreadPoolExecutor over it segfaults.
    """
    return plan_from_pages(T.extract_pages(pdf_path), domain, **kwargs)


def child_provenance(
    plan: BoundaryPlan,
    span: ChildSpan,
    *,
    parent: dict[str, Any],
    child: dict[str, Any],
    created_utc: str,
    run_id: str = "",
    tool: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The record that makes one child auditable and reversible.

    ``span`` plus ``parent.sha256`` is sufficient to reconstruct the child
    byte-for-byte from the parent, or to delete every child of a parent that a
    later review rejects.
    """
    return {
        "schema": PROVENANCE_SCHEMA,
        "run_id": run_id,
        "created_utc": created_utc,
        "derived": True,
        "parent": parent,
        "child": child,
        "span": {
            "article_index": span.index,
            "n_children": len(plan.spans),
            "start_page": span.start_page,
            "end_page": span.end_page,
            "n_pages": span.n_pages,
            "parent_pages": plan.n_pages,
            "front_matter_pages_dropped": plan.front_matter_pages,
        },
        "boundary": {
            "source": plan.source,
            "tier": plan.tier,
            "reason": plan.reason,
            "solver_status": plan.solver_status,
            "solver_reason": plan.solver_reason,
        },
        "toc_entry": {
            "printed_page": span.printed_page,
            "title": span.title,
            "author": span.author,
            "section": span.section,
            "is_front_matter": span.is_front_matter,
        },
        "evidence": span.evidence,
        "document_evidence": {
            "folio": plan.ledger.get("folio"),
            "n_toc_entries": plan.ledger.get("n_toc_entries"),
            "total_score": plan.ledger.get("total_score"),
        },
        "reverse": {
            "note": "delete child; parent is unmodified at parent.relpath",
            "reconstruct": (
                f"pages {span.start_page}-{span.end_page} of parent "
                f"sha256:{parent.get('sha256', '')}"
            ),
        },
        "tool": tool or {},
    }
