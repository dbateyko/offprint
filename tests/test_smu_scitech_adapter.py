from __future__ import annotations

from offprint.adapters.base import DiscoveryResult
from offprint.adapters.smu_scitech import SMUScienceTechnologyLawReviewAdapter


def result(title: str) -> DiscoveryResult:
    return DiscoveryResult(
        page_url="https://scholar.smu.edu/scitech/vol21/iss1/1",
        pdf_url="https://scholar.smu.edu/context/scitech/article/1272/viewcontent/example.pdf",
        metadata={"title": title},
    )


def test_smu_scitech_filters_front_matter_but_keeps_articles() -> None:
    assert not SMUScienceTechnologyLawReviewAdapter._is_article(result("Front Matter"))
    assert not SMUScienceTechnologyLawReviewAdapter._is_article(result("Masthead"))
    assert SMUScienceTechnologyLawReviewAdapter._is_article(
        result("Regulating Artificial Intelligence in Health Care")
    )


def test_smu_scitech_forces_publication_scoped_issue_traversal() -> None:
    adapter = SMUScienceTechnologyLawReviewAdapter()

    adapter.configure_dc(
        enum_mode="oai_only",
        max_oai_records=40,
        min_domain_delay_ms=2_000,
        max_domain_delay_ms=4_000,
    )

    assert adapter.dc_enum_mode == "all_issues_only"
    assert adapter.dc_min_domain_delay_ms == 6_000
    assert adapter.dc_max_domain_delay_ms == 8_000


def test_smu_scitech_defaults_to_safe_pacing_for_manifest_recovery() -> None:
    adapter = SMUScienceTechnologyLawReviewAdapter()

    assert adapter.dc_min_domain_delay_ms == 6_000
    assert adapter.dc_max_domain_delay_ms == 8_000


def test_smu_scitech_preserves_more_cautious_operator_pacing() -> None:
    adapter = SMUScienceTechnologyLawReviewAdapter()

    adapter.configure_dc(min_domain_delay_ms=9_000, max_domain_delay_ms=12_000)

    assert adapter.dc_min_domain_delay_ms == 9_000
    assert adapter.dc_max_domain_delay_ms == 12_000


def test_smu_scitech_derives_volume_and_issue_from_article_url(monkeypatch) -> None:
    article = result("Regulating Artificial Intelligence in Health Care")
    article.page_url = "https://scholar.smu.edu/scitech/vol29/iss1/2"
    monkeypatch.setattr(
        "offprint.adapters.digital_commons_issue_article_hop."
        "DigitalCommonsIssueArticleHopAdapter.discover_pdfs",
        lambda self, seed_url, max_depth=0: iter([article]),
    )

    discovered = next(
        SMUScienceTechnologyLawReviewAdapter().discover_pdfs("https://scholar.smu.edu/scitech")
    )

    assert discovered.metadata["volume"] == "29"
    assert discovered.metadata["issue"] == "1"
