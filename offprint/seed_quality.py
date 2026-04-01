from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional
from urllib.parse import urlparse


class SeedStatus(str, Enum):
    VALID = "VALID"
    SKIPPED_INVALID = "SKIPPED_INVALID"
    SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"


INVALID_HOSTS = {
    "accounts.google.com",
    "login.microsoftonline.com",
    "idp.login",
    "love-fruit.com",
}

UNSUPPORTED_PUBLISHER_HOSTS = {
    "www.cambridge.org",
    "link.springer.com",
    "www.apa.org",
    "www.wshein.com",
}

INVALID_PATH_HINTS = (
    "signin",
    "sign-in",
    "login",
    "auth",
    "oauth",
    "servicelogin",
)

JOURNAL_HINTS = (
    "journal",
    "lawreview",
    "law-review",
    "review",
    "issue",
    "archive",
    "all_issues",
    "articles",
    "law",
    "digitalcommons",
    "scholarship",
    "repository",
    "scholarlycommons",
    "scholar",
)

RETIRED_SEED_SIGNATURES = (
    ("ir.stthomas.edu", "/ustlj/"),
    ("researchonline.stthomas.edu", "/esploro/"),
    ("www.alibisocialbar.com", "/faqs/"),
    ("firstamendmentlawreview.org", "/"),
    ("djcl.org", "/issue/archive"),
    ("wfjlp.law.wfu.edu", "/"),
    ("www.lawinstitute.org", "/lawinsociety/"),
)


@dataclass(frozen=True)
class SeedAssessment:
    status: SeedStatus
    reason: str


def assess_seed(seed_url: str) -> SeedAssessment:
    parsed = urlparse(seed_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()

    if parsed.scheme not in {"http", "https"} or not host:
        return SeedAssessment(SeedStatus.SKIPPED_INVALID, "non_http_seed")

    if host in INVALID_HOSTS:
        return SeedAssessment(SeedStatus.SKIPPED_INVALID, "auth_provider_host")

    if host in UNSUPPORTED_PUBLISHER_HOSTS:
        return SeedAssessment(SeedStatus.SKIPPED_INVALID, "unsupported_publisher_host")

    for sig_host, sig_path in RETIRED_SEED_SIGNATURES:
        if host == sig_host and path.startswith(sig_path):
            return SeedAssessment(SeedStatus.SKIPPED_INVALID, "retired_repository_seed")

    haystack = f"{host} {path} {query}"
    if any(token in haystack for token in INVALID_PATH_HINTS):
        if "issue" not in haystack and "archive" not in haystack:
            return SeedAssessment(SeedStatus.SKIPPED_INVALID, "auth_or_login_path")

    if any(token in haystack for token in JOURNAL_HINTS):
        return SeedAssessment(SeedStatus.VALID, "journal_signal")

    # Conservative default: if we cannot infer quality, keep it.
    return SeedAssessment(SeedStatus.VALID, "unknown_but_allowed")


def assess_with_dedup(seed_url: str, key: str, seen: set[str]) -> SeedAssessment:
    assessment = assess_seed(seed_url)
    if assessment.status != SeedStatus.VALID:
        return assessment

    normalized_key = (key or "").strip().lower()
    if not normalized_key:
        normalized_key = seed_url.strip().lower()

    if normalized_key in seen:
        return SeedAssessment(SeedStatus.SKIPPED_DUPLICATE, "duplicate_target")

    seen.add(normalized_key)
    return assessment


def is_retryable_failure(reason: str, details: list[str]) -> bool:
    lowered_reason = (reason or "").lower()
    if lowered_reason in {"waf_blocked_headless", "all candidate downloads failed"}:
        for d in details or []:
            lowered = d.lower()
            if any(
                token in lowered
                for token in (
                    "waf_challenge",
                    "timeout",
                    "network",
                    "http_error (status=5",
                    "empty_response",
                )
            ):
                return True
    if "timeout" in lowered_reason or "network" in lowered_reason:
        return True
    return False


def best_domain(seed_url: str) -> Optional[str]:
    host = (urlparse(seed_url).netloc or "").strip().lower()
    return host or None
