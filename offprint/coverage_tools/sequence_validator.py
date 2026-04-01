from __future__ import annotations

import re
import statistics
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    match = re.search(r"(\d+)", text)
    if not match:
        return None
    return int(match.group(1))


def _metadata(record: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = record.get("metadata")
    if isinstance(metadata, Mapping):
        return metadata
    return {}


def _volume_issue(record: Mapping[str, Any]) -> tuple[Optional[int], Optional[int]]:
    metadata = _metadata(record)
    volume = _to_int(metadata.get("volume") or record.get("volume"))
    issue = _to_int(metadata.get("issue") or record.get("issue"))
    return volume, issue


def detect_volume_gaps(records: Iterable[Mapping[str, Any]]) -> List[str]:
    volumes = sorted(
        {volume for volume, _ in (_volume_issue(r) for r in records) if volume is not None}
    )
    if len(volumes) < 2:
        return []
    expected = set(range(min(volumes), max(volumes) + 1))
    missing = sorted(expected - set(volumes))
    return [f"Missing volume {value}" for value in missing]


def detect_issue_gaps(records: Iterable[Mapping[str, Any]]) -> List[str]:
    by_volume: Dict[int, set[int]] = defaultdict(set)
    for record in records:
        volume, issue = _volume_issue(record)
        if volume is None or issue is None:
            continue
        by_volume[volume].add(issue)

    missing_messages: List[str] = []
    for volume in sorted(by_volume):
        issues = sorted(by_volume[volume])
        if len(issues) < 2:
            continue
        expected = set(range(min(issues), max(issues) + 1))
        missing = sorted(expected - set(issues))
        for issue in missing:
            missing_messages.append(f"Missing issue {issue} in volume {volume}")
    return missing_messages


def detect_issue_count_outliers(
    records: Iterable[Mapping[str, Any]], threshold: float = 0.5
) -> List[str]:
    counts: Counter[tuple[int, int]] = Counter()
    for record in records:
        volume, issue = _volume_issue(record)
        if volume is None or issue is None:
            continue
        counts[(volume, issue)] += 1

    if len(counts) < 2:
        return []

    values = list(counts.values())
    median_count = statistics.median(values)
    if median_count <= 0:
        return []

    cutoff = median_count * threshold
    outliers: List[str] = []
    for (volume, issue), count in sorted(counts.items()):
        if count <= cutoff:
            outliers.append(
                f"Low article count in volume {volume} issue {issue}: {count} (< {cutoff:.1f})"
            )
    return outliers


def compute_pdf_ratio(records: Iterable[Mapping[str, Any]]) -> float:
    records_list = list(records)
    if not records_list:
        return 0.0

    with_record_type = [r for r in records_list if _metadata(r).get("record_type")]
    if with_record_type:
        articles = [r for r in with_record_type if _metadata(r).get("record_type") == "article"]
    else:
        articles = records_list

    if not articles:
        return 0.0
    with_pdf = [r for r in articles if bool(r.get("pdf_url"))]
    return len(with_pdf) / len(articles)


def journal_confidence(metrics: Mapping[str, Any]) -> str:
    volume_gaps = list(metrics.get("volume_gaps") or [])
    issue_gaps = list(metrics.get("issue_gaps") or [])
    issue_outliers = list(metrics.get("issue_outliers") or [])
    pdf_ratio = float(metrics.get("pdf_ratio") or 0.0)

    if not volume_gaps and not issue_gaps and not issue_outliers and pdf_ratio >= 0.9:
        return "HIGH"
    if not volume_gaps and pdf_ratio >= 0.7:
        return "MEDIUM"
    return "LOW"
