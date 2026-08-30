"""The completeness gate, which decides whether a run collected the journal.

Every undercollection bug found on 2026-08-29 exited 0 with a cheerful summary,
so this gate is the thing standing between a truncated run and a silent success.
It had no tests, and the per-journal aggregation below was broken in a way that
would have failed every complete Penn run.
"""
import argparse
import importlib.util
import json
import sys
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "run_pipeline", Path(__file__).resolve().parents[1] / "scripts/pipeline/run_pipeline.py")
rp = importlib.util.module_from_spec(SPEC)
_argv, sys.argv = sys.argv, ["run_pipeline.py"]
SPEC.loader.exec_module(rp)
sys.argv = _argv


def _args(seeds_dir, ratio=0.75):
    return argparse.Namespace(sitemaps_dir=str(seeds_dir), completeness_min_ratio=ratio,
                              fail_on_incomplete=True, mode="full")


def _seed(tmp_path, urls, expected=None, name="j.json"):
    nav = {"expected_pdfs": expected} if expected else {}
    (tmp_path / name).write_text(json.dumps(
        {"start_urls": urls, "metadata": {"journal_name": "J", "navigation": nav}}))
    return tmp_path


def _summary(per_url):
    return {"summary": {"seeds": {u: {"ok_total": n, "runtime": {}, "completeness": {}}
                                  for u, n in per_url.items()}}}


def test_complete_run_passes(tmp_path):
    _seed(tmp_path, ["u1"], expected=10)
    assert rp._report_completeness(_summary({"u1": 10}), _args(tmp_path)) is False


def test_short_run_is_flagged(tmp_path):
    _seed(tmp_path, ["u1"], expected=100)
    assert rp._report_completeness(_summary({"u1": 20}), _args(tmp_path)) is True


def test_expected_count_is_per_journal_not_per_url(tmp_path):
    """The Penn shape: one start_url per article, all sharing the journal total.

    Judging each URL against 753 would score every one of them 1/753 and fail a
    complete collection.
    """
    urls = [f"u{i}" for i in range(753)]
    _seed(tmp_path, urls, expected=753)
    assert rp._report_completeness(_summary({u: 1 for u in urls}), _args(tmp_path)) is False


def test_same_shape_still_fails_when_genuinely_short(tmp_path):
    urls = [f"u{i}" for i in range(753)]
    _seed(tmp_path, urls, expected=753)
    got = {u: (1 if i < 200 else 0) for i, u in enumerate(urls)}
    assert rp._report_completeness(_summary(got), _args(tmp_path)) is True


def test_seed_without_a_declared_count_cannot_fail(tmp_path):
    """Unverifiable is reported, not treated as either pass or failure."""
    _seed(tmp_path, ["u1"])                      # no expected_pdfs
    assert rp._report_completeness(_summary({"u1": 1}), _args(tmp_path)) is False


def test_threshold_is_honoured(tmp_path):
    _seed(tmp_path, ["u1"], expected=100)
    assert rp._report_completeness(_summary({"u1": 80}), _args(tmp_path, ratio=0.75)) is False
    assert rp._report_completeness(_summary({"u1": 80}), _args(tmp_path, ratio=0.90)) is True


def test_empty_summary_is_not_a_failure(tmp_path):
    assert rp._report_completeness({"summary": {"seeds": {}}}, _args(tmp_path)) is False
