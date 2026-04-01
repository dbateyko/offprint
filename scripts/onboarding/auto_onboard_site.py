#!/usr/bin/env python3
"""Auto-onboard a law-review site given a URL or an existing fingerprint JSON.

From a URL (fingerprints the site first, then onboards):
    python scripts/auto_onboard_site.py https://example.edu/law-review/

From a pre-computed fingerprint file:
    python scripts/auto_onboard_site.py --fingerprint artifacts/fingerprints/example.edu.json

Dry-run (show what would be written without touching the filesystem):
    python scripts/auto_onboard_site.py https://example.edu/law-review/ --dry-run

With smoke-test after onboarding:
    python scripts/auto_onboard_site.py https://example.edu/law-review/ --smoke-test
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_SITEMAPS_DIR  # noqa: E402
from offprint.site_fingerprinter import fingerprint_site, fingerprint_to_dict  # noqa: E402

# ---------------------------------------------------------------------------
# Platform → adapter mapping
# ---------------------------------------------------------------------------

PLATFORM_TO_ADAPTER: dict[str, str] = {
    "digital_commons": "DigitalCommonsBaseAdapter",
    "digitalcommons": "DigitalCommonsBaseAdapter",
    "ojs": "OJSAdapter",
    "wordpress": "WordPressAcademicBaseAdapter",
    "scholastica": "ScholasticaBaseAdapter",
    "drupal": "DrupalAdapter",
    "quartex": "QuartexAdapter",
    "janeway": "JanewayAdapter",
    "dspace": "DSpaceAdapter",
    "pubpub": "PubPubAdapter",
}

ADAPTER_IMPORT_HINTS: dict[str, str] = {
    "DigitalCommonsBaseAdapter": "from offprint.adapters.digital_commons_base import DigitalCommonsBaseAdapter",
    "OJSAdapter": "from offprint.adapters.ojs import OJSAdapter",
    "WordPressAcademicBaseAdapter": "from offprint.adapters.wordpress_academic_base import WordPressAcademicBaseAdapter",
    "ScholasticaBaseAdapter": "from offprint.adapters.scholastica_base import ScholasticaBaseAdapter",
    "DrupalAdapter": "from offprint.adapters.drupal import DrupalAdapter",
    "QuartexAdapter": "from offprint.adapters.quartex import QuartexAdapter",
    "JanewayAdapter": "from offprint.adapters.janeway import JanewayAdapter",
    "DSpaceAdapter": "from offprint.adapters.dspace import DSpaceAdapter",
    "PubPubAdapter": "from offprint.adapters.pubpub import PubPubAdapter",
    "GenericAdapter": "from offprint.adapters.generic import GenericAdapter",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _domain_from_url(url: str) -> str:
    """Return the netloc component of *url*, lowercased."""
    parsed = urlparse(url)
    return (parsed.netloc or url).lower().strip("/")


def _domain_to_slug(domain: str) -> str:
    """Convert a domain like ``example.edu`` to a filename slug ``example-edu``.

    Non-alphanumeric characters are replaced with hyphens; leading/trailing
    hyphens are stripped.
    """
    slug = re.sub(r"[^0-9A-Za-z]+", "-", domain).strip("-")
    return slug.lower()


def _platform_from_fingerprint(fp: dict[str, Any]) -> str:
    """Extract the platform string from a fingerprint dict, normalised to lower-case."""
    return str(fp.get("platform") or "unknown").lower().strip()


def _adapter_for_platform(platform: str, fingerprint: dict[str, Any]) -> str | None:
    """Return the adapter class name for *platform*, or ``None`` if unmapped.

    First consults the ``adapter_recommendation`` field of the fingerprint
    (set by the fingerprinter when it has high confidence), then falls back to
    the static ``PLATFORM_TO_ADAPTER`` table.

    Args:
        platform: Normalised platform string (e.g. ``"wordpress"``).
        fingerprint: Full fingerprint result dict.

    Returns:
        Adapter class name string, or ``None`` when the platform is unknown
        or requires a custom adapter.
    """
    recommended = str(fingerprint.get("adapter_recommendation") or "").strip()
    if recommended and recommended not in {"", "GenericAdapter", "unknown"}:
        return recommended
    return PLATFORM_TO_ADAPTER.get(platform)


def _needs_custom_adapter(platform: str, fingerprint: dict[str, Any]) -> bool:
    """Return True when the fingerprint signals that a custom adapter is required."""
    if bool(fingerprint.get("needs_custom_adapter")):
        return True
    return platform in {"unknown", ""} and not _adapter_for_platform(platform, fingerprint)


def _build_seed_dict(
    url: str, domain: str, platform: str, journal_name: str | None
) -> dict[str, Any]:
    """Build a new-format seed dict for *url*.

    Args:
        url: Canonical seed URL.
        domain: Normalised domain (netloc).
        platform: Platform string to embed in metadata.
        journal_name: Optional journal name; falls back to domain if absent.

    Returns:
        A seed dict ready for JSON serialisation.
    """
    slug = _domain_to_slug(domain)
    name = journal_name or domain
    return {
        "id": slug,
        "start_urls": [url],
        "source": "auto_onboard",
        "metadata": {
            "journal_name": name,
            "platform": platform,
            "url": url,
            "created_date": date.today().isoformat(),
            "status": "active",
        },
    }


def _find_last_register_line(registry_text: str) -> int:
    """Return the 0-based index of the last line containing ``register(``.

    Args:
        registry_text: Full text of the registry Python file.

    Returns:
        Line index, or ``-1`` if no ``register(`` call is found.
    """
    lines = registry_text.splitlines()
    last_idx = -1
    for i, line in enumerate(lines):
        if "register(" in line:
            last_idx = i
    return last_idx


def _append_register_call(
    registry_path: Path, domain: str, adapter_name: str, dry_run: bool
) -> None:
    """Append ``register("{domain}", {adapter_name})`` after the last existing register call.

    Inserts the new line immediately after the last ``register(`` line so it
    stays grouped with existing registrations rather than appearing at the end
    of the file.

    Args:
        registry_path: Path to ``registry.py``.
        domain: Domain string to register (e.g. ``"example.edu"``).
        adapter_name: Adapter class name (e.g. ``"WordPressAcademicBaseAdapter"``).
        dry_run: When True, print what would be written but do not modify the file.
    """
    text = registry_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    last_idx = _find_last_register_line(text)

    new_line = f'register("{domain}", {adapter_name})\n'

    if dry_run:
        print(f"  [dry-run] would append to {registry_path}: {new_line.rstrip()}", flush=True)
        return

    if last_idx >= 0 and last_idx + 1 < len(lines):
        lines.insert(last_idx + 1, new_line)
    else:
        lines.append(new_line)

    registry_path.write_text("".join(lines), encoding="utf-8")
    print(f"  appended to {registry_path}: {new_line.rstrip()}", flush=True)


def _print_stub_adapter(domain: str, adapter_name: str, fingerprint: dict[str, Any]) -> None:
    """Print a stub custom-adapter file to stdout.

    Args:
        domain: Domain the adapter will handle.
        adapter_name: Suggested class name for the new adapter.
        fingerprint: Fingerprint dict; may contain ``detected_selectors``.
    """
    selectors: dict[str, Any] = fingerprint.get("detected_selectors") or {}
    selector_comment_lines = (
        "\n".join(f"    #   {k}: {v!r}" for k, v in selectors.items()) or "    #   (none detected)"
    )

    stub = f"""\
#!/usr/bin/env python3
\"\"\"Custom adapter for {domain}.

Auto-generated stub — fill in the missing implementation details.
\"\"\"
from __future__ import annotations

from typing import Iterator

from offprint.adapters.base import Adapter, DiscoveryResult


class {adapter_name}(Adapter):
    \"\"\"Adapter for {domain}.

    Detected selectors from fingerprinter:
{selector_comment_lines}
    \"\"\"

    DOMAIN = "{domain}"

    def discover_pdfs(self, seed_url: str, max_depth: int = 3) -> Iterator[DiscoveryResult]:
        \"\"\"Discover PDFs from *seed_url*.\"\"\"
        # TODO: implement discovery logic
        raise NotImplementedError("discover_pdfs not implemented for {domain}")

    def download_pdf(self, pdf_url: str, out_dir: str) -> str:
        \"\"\"Download the PDF at *pdf_url* into *out_dir* and return the local path.\"\"\"
        return self._download_with_generic(pdf_url, out_dir)
"""
    print("\n" + "=" * 72, flush=True)
    print(f"STUB ADAPTER for {domain} → {adapter_name}", flush=True)
    print("=" * 72, flush=True)
    print(stub, flush=True)


def _run_smoke_test(url: str, adapter_name: str) -> None:
    """Run a quick smoke test against *url* using the named adapter class.

    Imports the adapter by class name from the adapters package, calls
    ``discover_pdfs``, and reports the first PDF found or the failure reason.

    Args:
        url: Seed URL to test.
        adapter_name: Fully qualified or simple class name of the adapter.
    """
    print(f"\n[smoke] running quick discovery on {url} via {adapter_name} …", flush=True)
    try:
        import importlib

        adapters_pkg = importlib.import_module("offprint.adapters")
        adapter_cls = getattr(adapters_pkg, adapter_name, None)
        if adapter_cls is None:
            # Try picking via registry
            from offprint.adapters import pick_adapter_for

            adapter_inst = pick_adapter_for(url)
        else:
            adapter_inst = adapter_cls()

        found_pdf: str | None = None
        count = 0
        for result in adapter_inst.discover_pdfs(url, max_depth=1):
            count += 1
            if result.pdf_url and found_pdf is None:
                found_pdf = result.pdf_url
            if count >= 5:
                break

        if found_pdf:
            print(
                f"  [smoke] PASS — first PDF: {found_pdf} ({count} result(s) checked)", flush=True
            )
        else:
            print(f"  [smoke] WARN — no PDF URLs found in first {count} result(s)", flush=True)
    except Exception as exc:
        print(f"  [smoke] FAIL — {exc}", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Entry point for the auto_onboard_site CLI."""
    parser = argparse.ArgumentParser(
        description="Auto-onboard a law-review site: generate seed JSON and registry entry.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("url", nargs="?", help="Seed URL to fingerprint and onboard.")
    parser.add_argument(
        "--fingerprint", metavar="PATH", help="Path to an existing fingerprint JSON file."
    )
    parser.add_argument(
        "--sitemaps-dir", default=DEFAULT_SITEMAPS_DIR, metavar="DIR", help="Directory for seed JSON files."
    )
    parser.add_argument(
        "--registry-file",
        default="offprint/adapters/registry.py",
        metavar="FILE",
        help="Path to registry.py.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done without writing files."
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="After onboarding, run a quick discover_pdfs smoke test.",
    )
    args = parser.parse_args()

    if args.url and args.fingerprint:
        parser.error("Provide either a URL or --fingerprint, not both.")
    if not args.url and not args.fingerprint:
        parser.error("Provide a positional URL or --fingerprint PATH.")

    # ------------------------------------------------------------------
    # Step 1: obtain fingerprint
    # ------------------------------------------------------------------
    if args.fingerprint:
        fp_path = Path(args.fingerprint)
        if not fp_path.is_file():
            print(f"[error] fingerprint file not found: {fp_path}", flush=True)
            return 1
        fingerprint: dict[str, Any] = json.loads(fp_path.read_text(encoding="utf-8"))
        seed_url: str = str(fingerprint.get("seed_url") or fingerprint.get("url") or "")
        if not seed_url and args.url:
            seed_url = args.url
        if not seed_url:
            print(
                "[error] could not determine seed URL from fingerprint; pass as positional arg.",
                flush=True,
            )
            return 1
    else:
        seed_url = args.url
        print(f"[onboard] fingerprinting {seed_url} …", flush=True)
        try:
            fingerprint = fingerprint_to_dict(fingerprint_site(seed_url))
        except Exception as exc:
            print(f"[error] fingerprinting failed: {exc}", flush=True)
            return 1

    domain = _domain_from_url(seed_url)
    platform = _platform_from_fingerprint(fingerprint)
    adapter_name = _adapter_for_platform(platform, fingerprint)

    print(f"[onboard] domain   : {domain}", flush=True)
    print(f"[onboard] platform : {platform}", flush=True)
    print(f"[onboard] adapter  : {adapter_name or '(needs custom)'}", flush=True)

    # ------------------------------------------------------------------
    # Step 2: custom-adapter path
    # ------------------------------------------------------------------
    if _needs_custom_adapter(platform, fingerprint) or adapter_name is None:
        print(
            f"\n[onboard] This site requires a custom adapter. "
            f"Suggested class name: {_domain_to_slug(domain).replace('-', '_').title().replace('_', '')}Adapter",
            flush=True,
        )
        suggested_class = (
            "".join(part.capitalize() for part in _domain_to_slug(domain).split("-")) + "Adapter"
        )
        _print_stub_adapter(domain, suggested_class, fingerprint)
        print(
            f"\n  Next steps:\n"
            f"    1. Save the stub above to offprint/adapters/{_domain_to_slug(domain).replace('-', '_')}.py\n"
            f"    2. Implement discover_pdfs()\n"
            f'    3. Add register("{domain}", {suggested_class}) to offprint/adapters/registry.py\n'
            f"    4. Create offprint/sitemaps/{_domain_to_slug(domain)}.json",
            flush=True,
        )
        return 0

    # ------------------------------------------------------------------
    # Step 3: generate seed JSON
    # ------------------------------------------------------------------
    journal_name: str | None = str(fingerprint.get("journal_name") or "").strip() or None
    seed_dict = _build_seed_dict(seed_url, domain, platform, journal_name)
    slug = _domain_to_slug(domain)
    sitemaps_dir = Path(args.sitemaps_dir)
    seed_path = sitemaps_dir / f"{slug}.json"

    if seed_path.exists():
        print(f"[onboard] seed file already exists, skipping: {seed_path}", flush=True)
    elif args.dry_run:
        print(f"  [dry-run] would write seed JSON to {seed_path}:", flush=True)
        print(json.dumps(seed_dict, indent=2), flush=True)
    else:
        sitemaps_dir.mkdir(parents=True, exist_ok=True)
        seed_path.write_text(json.dumps(seed_dict, indent=2) + "\n", encoding="utf-8")
        print(f"  wrote seed JSON: {seed_path}", flush=True)

    # ------------------------------------------------------------------
    # Step 4: register in registry.py
    # ------------------------------------------------------------------
    registry_path = Path(args.registry_file)
    if not registry_path.is_file():
        print(f"[warn] registry file not found, skipping registration: {registry_path}", flush=True)
    else:
        existing_text = registry_path.read_text(encoding="utf-8")
        if f'"{domain}"' in existing_text or f"'{domain}'" in existing_text:
            print(f"[onboard] domain already registered in {registry_path}, skipping.", flush=True)
        else:
            _append_register_call(registry_path, domain, adapter_name, dry_run=args.dry_run)

    import_hint = ADAPTER_IMPORT_HINTS.get(adapter_name, "")
    if import_hint:
        print(f"  import hint: {import_hint}", flush=True)

    print(f"\n[onboard] done — {domain} onboarded as {adapter_name}", flush=True)

    # ------------------------------------------------------------------
    # Step 5: optional smoke test
    # ------------------------------------------------------------------
    if args.smoke_test and not args.dry_run:
        _run_smoke_test(seed_url, adapter_name)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
