"""Regression tests for GeorgetownLawWPAdapter (www.law.georgetown.edu multisite).

Two failure modes are pinned here:

1. **Wrong WordPress root.** ``law.georgetown.edu`` is a WP multisite. The REST
   API and sitemap that matter live under the journal's subsite path, not at the
   origin. Rooting them at the origin enumerates the law school (faculty bios,
   news, program PDFs) and finds essentially no journal articles -- the
   ``host != journal`` mistake.

2. **Cross-tenant PDF leak.** ``WordPressAcademicBaseAdapter`` exempts any
   ``/wp-content/uploads/`` path from seed-path scoping, which is safe on a
   single-tenant install but on this host would let another journal's PDF (or an
   externally linked one from a reference list) be attributed to the seeded
   journal.
"""

from __future__ import annotations

from offprint.adapters.georgetown_law_wp import GeorgetownLawWPAdapter
from offprint.adapters.registry import pick_adapter_for

GLJ_SEED = "https://www.law.georgetown.edu/georgetown-law-journal/in-print/"
GLJ_UPLOADS = "https://www.law.georgetown.edu/georgetown-law-journal/wp-content/uploads"


def _adapter(seed_prefix: str = "/georgetown-law-journal/in-print/") -> GeorgetownLawWPAdapter:
    adapter = GeorgetownLawWPAdapter()
    adapter.base_url = "https://www.law.georgetown.edu"
    adapter.domain = "www.law.georgetown.edu"
    adapter._seed_path_prefix = seed_prefix
    return adapter


def test_registry_routes_host_to_georgetown_adapter() -> None:
    assert isinstance(pick_adapter_for(GLJ_SEED), GeorgetownLawWPAdapter)


def test_subsite_base_is_the_journal_not_the_origin() -> None:
    adapter = _adapter()
    assert (
        adapter._subsite_base(GLJ_SEED)
        == "https://www.law.georgetown.edu/georgetown-law-journal"
    )
    # A deep article URL still resolves to the same subsite root.
    deep = (
        "https://www.law.georgetown.edu/georgetown-law-journal/in-print/volume-110/"
        "volume-110-issue-2-december-2021/surveillance-and-the-tyrant-test/"
    )
    assert (
        adapter._subsite_base(deep)
        == "https://www.law.georgetown.edu/georgetown-law-journal"
    )
    # Origin root has no subsite -- nothing to pin, so the base is left alone.
    assert adapter._subsite_base("https://www.law.georgetown.edu/") is None


def test_pin_base_to_subsite_rewrites_rest_and_sitemap_root() -> None:
    adapter = _adapter()
    adapter._pin_base_to_subsite(GLJ_SEED)
    assert adapter.base_url == "https://www.law.georgetown.edu/georgetown-law-journal"


def test_accepts_own_subsite_pdf() -> None:
    adapter = _adapter()
    assert adapter._is_preferred_pdf_url(
        f"{GLJ_UPLOADS}/sites/26/2022/02/Ferguson_Surveillance-and-the-Tyrant-Test.pdf"
    )


def test_rejects_sibling_journal_pdf_on_same_host() -> None:
    """The uploads exemption must not cross multisite tenants."""
    adapter = _adapter()
    assert not adapter._is_preferred_pdf_url(
        "https://www.law.georgetown.edu/american-criminal-law-review/"
        "wp-content/uploads/sites/23/2024/01/Some-ACLR-Article.pdf"
    )


def test_rejects_law_school_wide_pdf() -> None:
    """Law-school PDFs (briefs, reports) sit outside any journal subsite."""
    adapter = _adapter()
    assert not adapter._is_preferred_pdf_url(
        "https://www.law.georgetown.edu/wp-content/uploads/2019/03/1996laws-final-report.pdf"
    )


def test_rejects_third_party_reference_list_pdf() -> None:
    """A cited PDF on an unrelated host is never this journal's article.

    Same class of bug as the 2026-08-24 OJS scope leak, which attributed a
    Polish court's PDF to a law journal.
    """
    adapter = _adapter()
    for url in (
        "https://www.katowice.sa.gov.pl/container/some-judgment.pdf",
        "https://lirias.kuleuven.be/retrieve/123456/paper.pdf",
    ):
        assert not adapter._is_preferred_pdf_url(url)


def test_seed_scope_excludes_glj_online_subpublication() -> None:
    """GLJ Online is a separate sub-publication and needs its own seed."""
    adapter = _adapter()
    assert adapter._url_matches_seed_scope(
        "https://www.law.georgetown.edu/georgetown-law-journal/in-print/volume-113/"
        "volume-113-issue-3-february-2025/the-reality-of-the-good-faith-exception/"
    )
    assert not adapter._url_matches_seed_scope(
        "https://www.law.georgetown.edu/georgetown-law-journal/submit/glj-online/"
        "glj-online-vol-115/25002-2/"
    )
    assert not adapter._url_matches_seed_scope(
        "https://www.law.georgetown.edu/american-criminal-law-review/in-print/"
    )


def test_download_session_overrides_blocked_user_agent() -> None:
    """The host's Varnish 403s the default Windows-Chrome UA on downloads."""
    from offprint.adapters.georgetown_law_wp import _UserAgentOverrideSession

    class RecordingSession:
        def __init__(self) -> None:
            self.seen: list[dict] = []

        def get(self, url, **kwargs):
            self.seen.append(dict(kwargs.get("headers") or {}))
            return "resp"

        head = get

    inner = RecordingSession()
    wrapped = _UserAgentOverrideSession(inner, "TestUA/1.0")
    wrapped.get(
        "https://www.law.georgetown.edu/georgetown-law-journal/wp-content/uploads/x.pdf",
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.law.georgetown.edu/",
        },
    )
    wrapped.head("https://www.law.georgetown.edu/x.pdf")
    assert [h["User-Agent"] for h in inner.seen] == ["TestUA/1.0", "TestUA/1.0"]
    assert inner.seen[0]["Referer"] == "https://www.law.georgetown.edu/"
    assert wrapped.seen is inner.seen


def test_adapter_download_uses_override_session() -> None:
    from offprint.adapters.georgetown_law_wp import _UserAgentOverrideSession
    import offprint.adapters.generic as generic_mod

    adapter = _adapter()
    captured = {}

    class FakeGeneric:
        last_download_meta = {"ok": True}

        def __init__(self, session=None):
            captured["session"] = session

        def download_pdf(self, pdf_url, out_dir, referer="", **kwargs):
            return "/tmp/x.pdf"

    real = generic_mod.GenericAdapter
    generic_mod.GenericAdapter = FakeGeneric
    try:
        assert adapter.download_pdf(f"{GLJ_UPLOADS}/sites/26/x.pdf", "/tmp") == "/tmp/x.pdf"
    finally:
        generic_mod.GenericAdapter = real

    assert isinstance(captured["session"], _UserAgentOverrideSession)
    assert "Windows NT" not in adapter.DOWNLOAD_USER_AGENT
