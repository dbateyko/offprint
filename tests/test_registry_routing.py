from __future__ import annotations

import pytest

from offprint.adapters import UnmappedAdapterError, pick_adapter_for
from offprint.adapters.registry import _find_sitemap_for_url


@pytest.mark.parametrize(
    "url, expected_cls",
    [
        # Every *.scholasticahq.com subdomain is a Scholastica journal — the
        # suffix heuristic must route unregistered ones (not just explicit hosts).
        ("https://appalachian.scholasticahq.com/issues", "ScholasticaBaseAdapter"),
        ("https://clb.scholasticahq.com/", "ScholasticaBaseAdapter"),
        # bepress/Digital Commons repos whose host prefix is too generic for the
        # substring heuristic, registered explicitly.
        ("https://docs.rwu.edu/cpc_justice/sitemap.xml", "DigitalCommonsIssueArticleHopAdapter"),
        ("https://scholars.unh.edu/unh_lr/", "DigitalCommonsIssueArticleHopAdapter"),
        # Penn's repository is Quartex despite the generic repository.* prefix,
        # which otherwise looks like Digital Commons to the suffix heuristic.
        (
            "https://repository.law.upenn.edu/journal-of-business-law/jbl",
            "QuartexAdapter",
        ),
    ],
)
def test_blocked_hosts_now_route_without_generic(url: str, expected_cls: str) -> None:
    # allow_generic=False mirrors the orchestrator's production gate: an unmapped
    # host raises UnmappedAdapterError (-> todo_adapter_blocked) instead of
    # silently falling back to the generic crawler.
    adapter = pick_adapter_for(url, allow_generic=False)
    assert adapter.__class__.__name__ == expected_cls


def test_scholastica_marketing_host_is_not_a_journal() -> None:
    # The bare apex (Scholastica's marketing site) is not a journal; the suffix
    # rule only matches subdomains, so it must still block under the prod gate.
    with pytest.raises(UnmappedAdapterError):
        pick_adapter_for("https://scholasticahq.com/", allow_generic=False)


def test_selector_lookup_matches_top_level_url_and_ignores_www(tmp_path, monkeypatch) -> None:
    sitemaps = tmp_path / "offprint/sitemaps"
    sitemaps.mkdir(parents=True)
    (sitemaps / "journal.json").write_text(
        '{"id":"journal","url":"https://example.edu/"}', encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)

    sitemap = _find_sitemap_for_url("https://www.example.edu/")

    assert sitemap is not None
    assert sitemap["id"] == "journal"
