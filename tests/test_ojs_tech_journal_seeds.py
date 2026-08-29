"""Seed + routing regression tests for the OJS technology-law journals
onboarded on 2026-08-24.

Both live on multi-journal OJS hosts (Institute of Advanced Legal Studies and
Masaryk University). The recurring, expensive mistake in this corpus is
``host != journal``: a seed pointed at the host root would harvest every other
journal the university publishes and attribute it to the wrong title. These
tests pin the publication-path scoping into CI.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from offprint.adapters import pick_adapter_for

SITEMAPS_DIR = Path(__file__).resolve().parents[1] / "offprint" / "sitemaps"

# seed filename -> (journal_name, required path segment, canonical archive root)
OJS_TECH_SEEDS = {
    "journals-sas-ac-uk-deeslr.json": (
        "Digital Evidence and Electronic Signature Law Review",
        "/deeslr/",
        "https://journals.sas.ac.uk/deeslr/issue/archive",
    ),
    "journals-muni-cz-mujlt.json": (
        "Masaryk University Journal of Law and Technology",
        "/mujlt/",
        "https://journals.muni.cz/mujlt/issue/archive",
    ),
}


def _load(seed_file: str) -> dict:
    return json.loads((SITEMAPS_DIR / seed_file).read_text(encoding="utf-8"))


@pytest.mark.parametrize("seed_file", sorted(OJS_TECH_SEEDS))
def test_seed_exists_and_is_valid_json(seed_file: str) -> None:
    seed = _load(seed_file)
    assert seed["id"] == seed_file[: -len(".json")]
    assert seed["start_urls"], "seed must carry at least one start URL"


@pytest.mark.parametrize("seed_file", sorted(OJS_TECH_SEEDS))
def test_seed_identifies_the_journal_and_platform(seed_file: str) -> None:
    journal_name, _segment, _archive = OJS_TECH_SEEDS[seed_file]
    meta = _load(seed_file)["metadata"]
    assert meta["journal_name"] == journal_name
    assert meta["platform"] == "ojs"
    assert meta["status"] == "active"
    assert meta["status_evidence_ref"], "an active seed must cite its smoke evidence"


@pytest.mark.parametrize("seed_file", sorted(OJS_TECH_SEEDS))
def test_seed_is_publication_scoped_not_host_rooted(seed_file: str) -> None:
    """host != journal: every URL in the seed must carry the publication slug."""
    _journal_name, segment, archive = OJS_TECH_SEEDS[seed_file]
    seed = _load(seed_file)

    assert seed["start_urls"] == [archive]
    for url in seed["start_urls"]:
        assert segment in url + "/", f"{url} is not scoped to {segment}"

    meta = seed["metadata"]
    assert segment in meta["url"] + "/"
    assert segment in meta["navigation"]["archive_root"]
    # The scope guard has to be written down, because the adapter itself only
    # constrains discovery to the origin, not to the publication path.
    assert "host != journal" in meta["navigation"]["scope_guard"]


@pytest.mark.parametrize("seed_file", sorted(OJS_TECH_SEEDS))
def test_seed_start_url_routes_to_the_ojs_adapter(seed_file: str) -> None:
    # allow_generic=False mirrors the orchestrator's production gate: an
    # unregistered host raises instead of silently falling back to the crawler.
    for url in _load(seed_file)["start_urls"]:
        adapter = pick_adapter_for(url, allow_generic=False)
        assert adapter.__class__.__name__ == "OJSAdapter"


@pytest.mark.parametrize(
    "url",
    [
        "https://journals.sas.ac.uk/deeslr/article/download/5776/5406",
        "https://journals.muni.cz/mujlt/article/download/11476/10858",
    ],
)
def test_galley_download_urls_route_to_the_ojs_adapter(url: str) -> None:
    assert pick_adapter_for(url, allow_generic=False).__class__.__name__ == "OJSAdapter"
