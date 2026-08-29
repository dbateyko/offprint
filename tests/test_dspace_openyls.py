"""Regression tests for the DSpace/openYLS path (YJHPLE onboarding, 2026-08-28)."""

from offprint.adapters.generic import GenericAdapter
from offprint.adapters.registry import pick_adapter_for
from offprint.adapters.dspace import DSpaceAdapter


def _md(item):
    adapter = GenericAdapter.__new__(GenericAdapter)
    return GenericAdapter._extract_dspace_metadata(adapter, item, "https://example.org/items/x")


def test_openyls_routes_to_dspace_adapter():
    adapter = pick_adapter_for(
        "https://openyls.law.yale.edu/collections/03845a38-2c3f-4021-82cb-27d9c650eb7a"
    )
    assert isinstance(adapter, DSpaceAdapter)


def test_collection_uuid_is_used_as_dspace_scope():
    adapter = GenericAdapter.__new__(GenericAdapter)
    scope = GenericAdapter._extract_dspace_scope_id(
        adapter, "https://openyls.law.yale.edu/collections/03845a38-2c3f-4021-82cb-27d9c650eb7a"
    )
    assert scope == "03845a38-2c3f-4021-82cb-27d9c650eb7a"


def test_legacy_bepress_identifier_yields_volume_and_issue():
    meta = _md(
        {
            "metadata": {
                "dc.title": [{"value": "Suffrage for People with Intellectual Disabilities"}],
                "dc.contributor.author": [{"value": "Kopel, Charles"}],
                "dc.identifier": [{"value": "yjhple/vol17/iss1/4"}],
            }
        }
    )
    assert meta["volume"] == "17"
    assert meta["issue"] == "1"
    assert meta["authors"] == ["Kopel, Charles"]


def test_non_bepress_dspace_item_gets_no_fabricated_volume():
    meta = _md(
        {
            "metadata": {
                "dc.title": [{"value": "Some Ohio State Item"}],
                "dc.identifier": [{"value": "http://hdl.handle.net/1811/12345"}],
            }
        }
    )
    assert "volume" not in meta
    assert "issue" not in meta
