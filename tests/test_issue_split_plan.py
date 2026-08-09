"""Tests for the split wiring layer: tier gating, fallback, provenance.

These test the *wiring*, not the solver. The solver's own behaviour is pinned by
tests/test_toc_solver.py.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from offprint.pdf_footnotes import issue_split_plan as P
from offprint.pdf_footnotes import toc_solver as T


@dataclass
class _FakeResult:
    status: str
    reason: str
    assignments: list
    n_pages: int = 100

    def ledger(self) -> dict:
        return {"status": self.status, "reason": self.reason, "folio": None}


def _assignment(page: int, title: str) -> T.Assignment:
    entry = T.TocEntry(printed_page=page, title=title, author="Doe")
    return T.Assignment(entry=entry, page=page, score=9.0, signals=T.Signals(), margin=5.0)


def _patch_solver(monkeypatch, status: str, pages: list[int]) -> None:
    result = _FakeResult(
        status=status,
        reason="stubbed",
        assignments=[_assignment(page, f"Piece {page}") for page in pages],
    )
    monkeypatch.setattr(T, "solve", lambda pages_, **kwargs: result)


PAGES = T.pages_from_texts(["body"] * 100)


def test_auto_tier_emits(monkeypatch):
    _patch_solver(monkeypatch, "auto", [1, 20, 60])
    plan = P.plan_from_pages(PAGES, "example.org", tier="auto")
    assert plan.ok and plan.source == "toc_solver" and plan.tier == "auto"
    assert [(s.start_page, s.end_page) for s in plan.spans] == [(1, 19), (20, 59), (60, 100)]


def test_review_is_not_emitted_at_the_default_tier(monkeypatch):
    """The default must refuse `review`. This is the asymmetric-cost guard."""
    _patch_solver(monkeypatch, "review", [1, 20, 60])
    plan = P.plan_from_pages(PAGES, "example.org", tier="auto", fallback="none")
    assert not plan.ok
    assert plan.reason == "tier_below_threshold:review"


def test_review_is_emitted_when_deliberately_enabled(monkeypatch):
    _patch_solver(monkeypatch, "review", [1, 20, 60])
    plan = P.plan_from_pages(PAGES, "example.org", tier="review")
    assert plan.ok and plan.tier == "review"


def test_abstain_with_no_fallback_yields_no_boundaries(monkeypatch):
    _patch_solver(monkeypatch, "abstain", [])
    plan = P.plan_from_pages(PAGES, "example.org", fallback="none")
    assert not plan.ok and plan.spans == []
    assert plan.solver_status == "abstain"


def test_running_head_fallback_runs_only_when_the_solver_declines(monkeypatch):
    _patch_solver(monkeypatch, "abstain", [])
    calls: list[str] = []

    def _fake_infer(page_texts, domain, rules):
        calls.append(domain)
        from offprint.pdf_footnotes.issue_splitter import ArticleBoundary, BoundaryInference

        return BoundaryInference(
            [
                ArticleBoundary(1, 49, "domain_head_rule", 0.85, "First"),
                ArticleBoundary(50, 100, "domain_head_rule", 0.85, "Second"),
            ],
            "domain_head_rule",
            0.85,
        )

    monkeypatch.setattr(P, "infer_law_review_boundaries", _fake_infer)
    plan = P.plan_from_pages(PAGES, "example.org", fallback="running_head")
    assert calls == ["example.org"]
    assert plan.ok and plan.source == "running_head" and plan.tier == ""

    # ...and not when it does not decline.
    calls.clear()
    _patch_solver(monkeypatch, "auto", [1, 20, 60])
    assert P.plan_from_pages(PAGES, "example.org").source == "toc_solver"
    assert calls == []


def test_fallback_never_overrules_a_review_verdict(monkeypatch):
    """`review` means the solver has an opinion it is unsure of, not no opinion.

    Letting the head rule overrule it is strictly worse than leaving the
    compilation unsplit: on the 2026-08-09 trial both fallback firings were on
    `review` documents and one cut a 324-page BTLJ issue mid-article.
    """
    _patch_solver(monkeypatch, "review", [1, 20, 60])
    monkeypatch.setattr(
        P,
        "infer_law_review_boundaries",
        lambda *a, **k: pytest.fail("fallback ran on a review document"),
    )
    plan = P.plan_from_pages(PAGES, "example.org", tier="auto", fallback="running_head")
    assert not plan.ok and plan.reason == "tier_below_threshold:review"


def test_a_single_boundary_is_not_a_split(monkeypatch):
    _patch_solver(monkeypatch, "auto", [1])
    plan = P.plan_from_pages(PAGES, "example.org", fallback="none")
    assert not plan.ok and plan.reason.startswith("too_few_children")


def test_unknown_tier_is_rejected():
    with pytest.raises(ValueError):
        P.plan_from_pages(PAGES, "example.org", tier="abstain")


def test_provenance_carries_everything_needed_to_reverse_a_split(monkeypatch):
    _patch_solver(monkeypatch, "auto", [3, 40])
    plan = P.plan_from_pages(PAGES, "example.org")
    record = P.child_provenance(
        plan,
        plan.spans[1],
        parent={"relpath": "example.org/issue.pdf", "sha256": "abc", "n_pages": 100},
        child={"relpath": "example.org/issue/issue__a02_p40-100.pdf", "sha256": "def"},
        created_utc="20260808T000000Z",
        run_id="run1",
    )
    assert record["schema"] == P.PROVENANCE_SCHEMA
    assert record["parent"]["sha256"] == "abc"
    assert record["span"] == {
        "article_index": 2,
        "n_children": 2,
        "start_page": 40,
        "end_page": 100,
        "n_pages": 61,
        "parent_pages": 100,
        "front_matter_pages_dropped": 2,
    }
    assert record["boundary"]["source"] == "toc_solver"
    assert record["boundary"]["tier"] == "auto"
    assert "signals" in record["evidence"]
    assert "abc" in record["reverse"]["reconstruct"]
