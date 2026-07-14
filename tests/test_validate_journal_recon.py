from __future__ import annotations

from pathlib import Path

from scripts.research.validate_journal_recon import load_json, main, validate_document

REPO_ROOT = Path(__file__).resolve().parents[1]
RECON_DIR = REPO_ROOT / "data" / "reference" / "journal_recon"
SCHEMA_PATH = RECON_DIR / "schema.json"
EXAMPLE_PATH = RECON_DIR / "example-law-review.json"


def example_and_schema():
    return load_json(EXAMPLE_PATH), load_json(SCHEMA_PATH)


def test_checked_in_example_is_valid() -> None:
    example, schema = example_and_schema()
    assert validate_document(example, schema) == []


def test_schema_rejects_missing_required_field_and_unknown_property() -> None:
    example, schema = example_and_schema()
    del example["journal"]["canonical_url"]
    example["journal"]["untracked_guess"] = "value"

    errors = validate_document(example, schema)

    assert "$.journal: missing required property 'canonical_url'" in errors
    assert "$.journal.untracked_guess: additional property is not allowed" in errors


def test_semantics_reject_dangling_evidence_and_route_nodes() -> None:
    example, schema = example_and_schema()
    example["site_map"]["entry_points"][0]["evidence_ids"] = ["not-observed"]
    example["site_map"]["route_graph"]["edges"][0]["to"] = "missing-node"

    errors = validate_document(example, schema)

    assert any("unknown evidence id 'not-observed'" in error for error in errors)
    assert any("unknown node id 'missing-node'" in error for error in errors)


def test_ready_requires_sampling_pdf_checks_and_no_blocking_barrier() -> None:
    example, schema = example_and_schema()
    example["reconnaissance"]["sample_policy"]["article_pages_checked"] = 2
    example["file_delivery"]["validation"]["magic_bytes_checked"] = False
    example["access"]["barriers"].append(
        {
            "type": "waf",
            "severity": "blocking",
            "evidence_ids": ["archive"],
            "notes": "A challenge blocks enumeration.",
        }
    )

    errors = validate_document(example, schema)

    assert any("article_pages_checked" in error and "at least 3" in error for error in errors)
    assert any("magic_bytes_checked" in error and "must be true" in error for error in errors)
    assert any("blocking barrier" in error for error in errors)


def test_observed_metadata_availability_requires_a_source() -> None:
    example, schema = example_and_schema()
    example["metadata_fields"]["title"]["sources"] = []

    errors = validate_document(example, schema)

    assert any("metadata_fields.title.sources" in error for error in errors)


def test_cli_validates_default_directory(capsys) -> None:
    assert main([]) == 0
    assert f"PASS {EXAMPLE_PATH}" in capsys.readouterr().out


def test_statuses_must_match() -> None:
    example, schema = example_and_schema()
    example["verdict"]["status"] = "needs_headless"

    errors = validate_document(example, schema)

    assert "$.verdict.status: must match $.reconnaissance.status" in errors


def test_verdict_requires_adapter_recommendation() -> None:
    example, schema = example_and_schema()
    del example["verdict"]["recommended_adapter"]

    errors = validate_document(example, schema)

    assert "$.verdict: missing required property 'recommended_adapter'" in errors


def test_file_requirements_are_tri_state_not_boolean() -> None:
    example, schema = example_and_schema()
    example["file_delivery"]["patterns"][0]["requirements"]["cookies"] = False

    errors = validate_document(example, schema)

    assert any("requirements.cookies" in error and "not one of" in error for error in errors)


def test_invalid_field_types_report_errors_without_crashing() -> None:
    example, schema = example_and_schema()
    example["reconnaissance"]["sample_policy"]["article_pages_checked"] = "three"
    example["metadata_fields"]["title"]["availability"] = ["always"]

    errors = validate_document(example, schema)

    assert any("article_pages_checked: expected integer" in error for error in errors)
    assert any("metadata_fields.title.availability" in error for error in errors)
