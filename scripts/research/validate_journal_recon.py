#!/usr/bin/env python3
"""Validate journal reconnaissance dossiers without third-party dependencies."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import unquote, urlsplit

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DOSSIER_DIR = REPO_ROOT / "data" / "reference" / "journal_recon"
DEFAULT_SCHEMA = DEFAULT_DOSSIER_DIR / "schema.json"


class SchemaError(ValueError):
    """Raised when the checked-in schema uses an unsupported or invalid reference."""


def _json_type_matches(value: Any, expected: str) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "null":
        return value is None
    raise SchemaError(f"unsupported schema type: {expected!r}")


def _resolve_ref(root_schema: Mapping[str, Any], ref: str) -> Mapping[str, Any]:
    if not ref.startswith("#/"):
        raise SchemaError(f"only local JSON Pointer references are supported: {ref!r}")
    current: Any = root_schema
    for raw_part in ref[2:].split("/"):
        part = unquote(raw_part).replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or part not in current:
            raise SchemaError(f"schema reference does not resolve: {ref!r}")
        current = current[part]
    if not isinstance(current, dict):
        raise SchemaError(f"schema reference is not an object: {ref!r}")
    return current


def _format_ok(value: str, format_name: str) -> bool:
    if format_name == "uri":
        parsed = urlsplit(value)
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    if format_name == "date-time":
        try:
            parsed_date = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        return parsed_date.tzinfo is not None
    raise SchemaError(f"unsupported string format: {format_name!r}")


def _validate_schema_node(
    value: Any,
    schema: Mapping[str, Any],
    root_schema: Mapping[str, Any],
    path: str,
) -> List[str]:
    if "$ref" in schema:
        return _validate_schema_node(
            value, _resolve_ref(root_schema, schema["$ref"]), root_schema, path
        )

    errors: List[str] = []
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: expected constant {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: value {value!r} is not one of {schema['enum']!r}")

    expected_types = schema.get("type")
    if isinstance(expected_types, str):
        expected_types = [expected_types]
    if expected_types and not any(_json_type_matches(value, item) for item in expected_types):
        errors.append(f"{path}: expected {' or '.join(expected_types)}, got {type(value).__name__}")
        return errors

    if isinstance(value, dict):
        required = schema.get("required", [])
        for name in required:
            if name not in value:
                errors.append(f"{path}: missing required property {name!r}")
        properties = schema.get("properties", {})
        for name, child in value.items():
            child_path = f"{path}.{name}"
            if name in properties:
                errors.extend(
                    _validate_schema_node(child, properties[name], root_schema, child_path)
                )
            elif schema.get("additionalProperties") is False:
                errors.append(f"{child_path}: additional property is not allowed")

    if isinstance(value, list):
        if len(value) < schema.get("minItems", 0):
            errors.append(f"{path}: expected at least {schema['minItems']} item(s)")
        if schema.get("uniqueItems"):
            seen = set()
            for index, item in enumerate(value):
                marker = json.dumps(item, sort_keys=True, separators=(",", ":"))
                if marker in seen:
                    errors.append(f"{path}[{index}]: duplicate array item")
                seen.add(marker)
        if "items" in schema:
            for index, item in enumerate(value):
                errors.extend(
                    _validate_schema_node(item, schema["items"], root_schema, f"{path}[{index}]")
                )

    if isinstance(value, str):
        if len(value) < schema.get("minLength", 0):
            errors.append(f"{path}: string is shorter than {schema['minLength']} character(s)")
        if "pattern" in schema and re.search(schema["pattern"], value) is None:
            errors.append(f"{path}: value does not match pattern {schema['pattern']!r}")
        if "format" in schema and not _format_ok(value, schema["format"]):
            errors.append(f"{path}: value is not a valid {schema['format']}")

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{path}: value is less than minimum {schema['minimum']}")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{path}: value is greater than maximum {schema['maximum']}")
    return errors


def _walk_evidence_references(value: Any, path: str = "$") -> Iterable[Tuple[str, str]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if key == "evidence_ids" and isinstance(child, list):
                for index, evidence_id in enumerate(child):
                    if isinstance(evidence_id, str):
                        yield f"{child_path}[{index}]", evidence_id
            else:
                yield from _walk_evidence_references(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _walk_evidence_references(child, f"{path}[{index}]")


def _semantic_errors(document: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    reconnaissance = document.get("reconnaissance", {})
    evidence = reconnaissance.get("evidence", []) if isinstance(reconnaissance, dict) else []
    evidence_ids: List[str] = [
        item.get("id")
        for item in evidence
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    ]
    duplicates = sorted({item for item in evidence_ids if evidence_ids.count(item) > 1})
    for evidence_id in duplicates:
        errors.append(f"$.reconnaissance.evidence: duplicate evidence id {evidence_id!r}")
    known_evidence = set(evidence_ids)
    for path, evidence_id in _walk_evidence_references(document):
        if evidence_id not in known_evidence:
            errors.append(f"{path}: unknown evidence id {evidence_id!r}")

    site_map = document.get("site_map", {})
    route_graph = site_map.get("route_graph", {}) if isinstance(site_map, dict) else {}
    nodes = route_graph.get("nodes", []) if isinstance(route_graph, dict) else []
    node_ids: List[str] = [
        item.get("id")
        for item in nodes
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    ]
    duplicate_nodes = sorted({item for item in node_ids if node_ids.count(item) > 1})
    for node_id in duplicate_nodes:
        errors.append(f"$.site_map.route_graph.nodes: duplicate node id {node_id!r}")
    known_nodes = set(node_ids)
    edges = route_graph.get("edges", []) if isinstance(route_graph, dict) else []
    for index, edge in enumerate(edges):
        if not isinstance(edge, dict):
            continue
        for endpoint in ("from", "to"):
            node_id = edge.get(endpoint)
            if isinstance(node_id, str) and node_id not in known_nodes:
                errors.append(
                    f"$.site_map.route_graph.edges[{index}].{endpoint}: unknown node id {node_id!r}"
                )

    metadata_fields = document.get("metadata_fields", {})
    if isinstance(metadata_fields, dict):
        for name, field in metadata_fields.items():
            if not isinstance(field, dict):
                continue
            availability = field.get("availability")
            if (
                isinstance(availability, str)
                and availability in {"always", "sometimes"}
                and not field.get("sources")
            ):
                errors.append(
                    f"$.metadata_fields.{name}.sources: required when availability is "
                    f"{availability!r}"
                )

    verdict = document.get("verdict", {})
    recon_status = reconnaissance.get("status") if isinstance(reconnaissance, dict) else None
    verdict_status = verdict.get("status") if isinstance(verdict, dict) else None
    if recon_status != verdict_status:
        errors.append("$.verdict.status: must match $.reconnaissance.status")

    if verdict_status == "ready":
        sample_policy = reconnaissance.get("sample_policy", {})
        if isinstance(sample_policy, dict):
            for field in ("oldest_issue_checked", "middle_issue_checked", "newest_issue_checked"):
                if sample_policy.get(field) is not True:
                    errors.append(f"$.reconnaissance.sample_policy.{field}: must be true for ready")
            article_pages_checked = sample_policy.get("article_pages_checked")
            if (
                not isinstance(article_pages_checked, int)
                or isinstance(article_pages_checked, bool)
                or article_pages_checked < 3
            ):
                errors.append(
                    "$.reconnaissance.sample_policy.article_pages_checked: "
                    "must be at least 3 for ready"
                )
        if isinstance(verdict, dict) and verdict.get("blockers"):
            errors.append("$.verdict.blockers: must be empty for ready")
        access = document.get("access", {})
        barriers = access.get("barriers", []) if isinstance(access, dict) else []
        if any(isinstance(item, dict) and item.get("severity") == "blocking" for item in barriers):
            errors.append("$.access.barriers: ready dossier cannot contain a blocking barrier")
        file_delivery = document.get("file_delivery", {})
        validation = file_delivery.get("validation", {}) if isinstance(file_delivery, dict) else {}
        for field in ("magic_bytes_checked", "content_type_checked", "article_vs_issue_checked"):
            if not isinstance(validation, dict) or validation.get(field) is not True:
                errors.append(f"$.file_delivery.validation.{field}: must be true for ready")
    return errors


def validate_document(document: Any, schema: Mapping[str, Any]) -> List[str]:
    """Return deterministic validation errors for one decoded JSON document."""
    errors = _validate_schema_node(document, schema, schema, "$")
    if isinstance(document, dict):
        errors.extend(_semantic_errors(document))
    return errors


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def dossier_paths(inputs: Sequence[str]) -> List[Path]:
    candidates = [Path(item) for item in inputs] if inputs else [DEFAULT_DOSSIER_DIR]
    paths: List[Path] = []
    for candidate in candidates:
        if candidate.is_dir():
            paths.extend(
                path for path in sorted(candidate.glob("*.json")) if path.name != "schema.json"
            )
        else:
            paths.append(candidate)
    return paths


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="Dossier JSON files or directories")
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA, help="JSON Schema path")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        schema = load_json(args.schema)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"schema error: {args.schema}: {exc}", file=sys.stderr)
        return 2

    paths = dossier_paths(args.paths)
    if not paths:
        print("input error: no dossier JSON files found", file=sys.stderr)
        return 2

    failed = 0
    for path in paths:
        try:
            document = load_json(path)
            errors = validate_document(document, schema)
        except (OSError, json.JSONDecodeError, SchemaError) as exc:
            print(f"ERROR {path}: {exc}", file=sys.stderr)
            return 2
        if errors:
            failed += 1
            print(f"FAIL {path}")
            for error in errors:
                print(f"  - {error}")
        else:
            print(f"PASS {path}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
