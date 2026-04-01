from __future__ import annotations

from typing import Dict, Optional, Type
from urllib.parse import urlparse

import requests

from .aipla_quarterly_journal import AIPLAQuarterlyJournalAdapter
from .ajcl_archive import AJCLArchiveAdapter
from .base import Adapter
from .berkeley_btlj import BerkeleyBTLJAdapter
from .blogger import BloggerAdapter
from .cambridge_core import CambridgeCoreAdapter
from .digital_commons_issue_article_hop import DigitalCommonsIssueArticleHopAdapter
from .drupal import DrupalAdapter
from .dspace import DSpaceAdapter
from .escholarship import EScholarshipAdapter
from .generic import GenericAdapter
from .georgetown_jnslp import GeorgetownJNSLPAdapter
from .greenbag import GreenBagAdapter
from .selector_driven import SelectorDrivenAdapter
from .harvard_jolt import HarvardJOLTAdapter
from .illinois_jltp import IllinoisJLTPAdapter
from .jurimetrics import JurimetricsAdapter
from .janeway import JanewayAdapter
from .nc_jolt import NorthCarolinaJOLTAdapter
from .nebraska_law_review import NebraskaLawReviewAdapter
from .ojs import OJSAdapter
from .plone import PloneAdapter
from .pubpub import PubPubAdapter
from .quartex import QuartexAdapter
from .penn_law_review import PennLawReviewAdapter
from .roman_legal_tradition import RomanLegalTraditionAdapter
from .rutgers_law_review import RutgersLawReviewAdapter
from .rutgers_law_journals import RutgersLawJournalsAdapter
from .sc_jle import SCJLEAdapter

# Scholastica Adapters
from .scholastica_base import ScholasticaBaseAdapter
from .scholastica_journals import (
    AlbanyGovernmentLawReviewAdapter,
    AlbanyLawJournalScienceTechAdapter,
    AlbanyLawReviewAdapter,
    AppalachianJournalLawAdapter,
)
from .springer import SpringerAdapter
from .squarespace import SquarespaceAdapter
from .uh_hjil import UHHJILAdapter
from .umassd_ojs import UMassDOJSAdapter
from .und_law_review import UNDLawReviewAdapter
from .virginia_jolt import VirginiaJOLTAdapter
from .weebly import WeeblyAdapter
from .wix import WixAdapter

# WordPress Academic Adapters
from .wordpress_academic_base import WordPressAcademicBaseAdapter
from .wustl_journals import WUSTLJournalsAdapter
from .yale_law_journal import YaleLawJournalAdapter
from .tlcp import TLCPAdapter

ADAPTERS: Dict[str, Type[Adapter] | Adapter] = {}


class UnmappedAdapterError(RuntimeError):
    def __init__(self, *, host: str, url: str):
        self.host = host
        self.url = url
        message = f"No adapter mapping for host='{host}' url='{url}'"
        super().__init__(message)


def register(domain: str, adapter_cls: Type[Adapter] | Adapter) -> None:
    """Register an adapter for a domain (e.g., 'example.org')."""
    normalized = domain.lower()
    ADAPTERS[normalized] = adapter_cls
    # Many seeds use bare domains while registry entries are "www.*".
    # Auto-register a bare-domain alias to avoid accidental GenericAdapter routing.
    if normalized.startswith("www."):
        ADAPTERS.setdefault(normalized[4:], adapter_cls)


def register_many(domains: list[str], adapter_cls: Type[Adapter] | Adapter) -> None:
    """Declarative helper for bulk host registration."""
    for domain in domains:
        register(domain, adapter_cls)


def _find_sitemap_for_url(url: str) -> Optional[Dict]:
    import os, json, glob
    for sdir in ["offprint/sitemaps", "offprint/sitemaps/from_nav_maps", "../adapter-autoresearch-pack/sitemaps", "../adapter-autoresearch-pack/sitemaps/from_nav_maps"]:
        if not os.path.exists(sdir): continue
        for spath in glob.glob(os.path.join(sdir, "*.json")):
            try:
                with open(spath, encoding="utf-8") as f: data = json.load(f)
                if any(u in url for u in (data.get("start_urls") or [])) or data.get("metadata", {}).get("url") == url:
                    return data
            except: continue
    return None


def pick_adapter_for(
    url: str,
    session: Optional[requests.Session] = None,
    allow_generic: bool = True,
) -> Adapter:
    import os, json, glob
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    if host.endswith("americanbar.org") and "/groups/science_technology/resources/jurimetrics" in path:
        return JurimetricsAdapter(session=session)
    exact = ADAPTERS.get(host)
    if exact is not None:
        if isinstance(exact, type):
            if "SelectorDrivenAdapter" in exact.__name__:
                found_sitemap = _find_sitemap_for_url(url)
                if found_sitemap: return exact(sitemap=found_sitemap, session=session)
                else: raise TypeError(f"SelectorDrivenAdapter requires a sitemap for {url}. None found in offprint/sitemaps/.")
            return exact(session=session)
        if session and hasattr(exact, "session"): exact.session = session
        return exact
    for domain, cls_or_inst in ADAPTERS.items():
        if host.endswith("." + domain):
            if isinstance(cls_or_inst, type):
                if "SelectorDrivenAdapter" in cls_or_inst.__name__:
                    found_sitemap = _find_sitemap_for_url(url)
                    if found_sitemap: return cls_or_inst(sitemap=found_sitemap, session=session)
                    else: raise TypeError(f"SelectorDrivenAdapter requires a sitemap for {url}. None found in offprint/sitemaps/.")
                return cls_or_inst(session=session)
            if session and hasattr(cls_or_inst, "session"): cls_or_inst.session = session
            return cls_or_inst
    if host == "escholarship.org" or host.endswith(".escholarship.org"): return EScholarshipAdapter(session=session)
    if any(sub in host for sub in ["digitalcommons.", "scholarlycommons.", "scholarship.", "scholarworks.", "engagedscholarship.", "repository.", "uknowledge.", "via.library.", "ir.lawnet.", "commons.", "academicworks.", "archives."]):
        return DigitalCommonsIssueArticleHopAdapter(session=session)
    if allow_generic:
        from .generic import GenericAdapter
        return GenericAdapter(session=session)
    raise UnmappedAdapterError(host=host, url=url)
register("albertalawreview.com", OJSAdapter)
register("repository.arizona.edu", SelectorDrivenAdapter)
register("ahrlj.up.ac.za", SelectorDrivenAdapter)
register("arizonajournal.org", SelectorDrivenAdapter)
register("abdn.ac.uk", SelectorDrivenAdapter)
register("www.abdn.ac.uk", SelectorDrivenAdapter)
register("ctlj.colorado.edu", SelectorDrivenAdapter)
register("georgetownlawtechreview.org", SelectorDrivenAdapter)
register("www.stanfordlawreview.org", SelectorDrivenAdapter)
register("www.uclalawreview.org", SelectorDrivenAdapter)
register("www.pennlawreview.com", SelectorDrivenAdapter)
register("jolt.law.harvard.edu", SelectorDrivenAdapter)
register("www.vjolt.org", SelectorDrivenAdapter)
register("law.adelaide.edu.au", SelectorDrivenAdapter)
register("ilj.law.indiana.edu", SelectorDrivenAdapter)
register("texaslawreview.org", WordPressAcademicBaseAdapter)
register("www.bu.edu", WordPressAcademicBaseAdapter)
register("www.law.georgetown.edu", WordPressAcademicBaseAdapter)
register("publications.lawschool.cornell.edu", WordPressAcademicBaseAdapter)
register("virginialawreview.org", WordPressAcademicBaseAdapter)
register("www.californialawreview.org", SquarespaceAdapter)
register("dlj.law.duke.edu", WordPressAcademicBaseAdapter)
register("ir.lawnet.fordham.edu", DigitalCommonsIssueArticleHopAdapter)
register("scholar.smu.edu", DigitalCommonsIssueArticleHopAdapter)
register("openscholarship.wustl.edu", DigitalCommonsIssueArticleHopAdapter)
register("scholarship.law.nd.edu", DigitalCommonsIssueArticleHopAdapter)
register("scholarship.law.vanderbilt.edu", DigitalCommonsIssueArticleHopAdapter)
register("www.law.uchicago.edu", DrupalAdapter)
register("www.law.uw.edu", WordPressAcademicBaseAdapter)
register("suffolk.edu", WordPressAcademicBaseAdapter)
register("archives.law.nccu.edu", DigitalCommonsIssueArticleHopAdapter)

# Recent onboarded hosts that were still missing explicit routing.
register_many(
    [
        "aulawreview.org",
        "business-law-review.law.miami.edu",
        "cardozoaelj.com",
        "cardozolawreview.com",
        "georgialawreview.org",
        "illinoislawreview.org",
        "jhr.law.northwestern.edu",
        "northcarolinalawreview.org",
        "texastechlawreview.org",
        "wakeforestlawreview.com",
        "www.georgialawreview.org",
        "www.wakeforestlawreview.com",
        "www.templelawreview.org",
        "waynelawreview.org",
    ],
    WordPressAcademicBaseAdapter,
)
register("ojs.lib.umassd.edu", UMassDOJSAdapter)
register("www.aipla.org", AIPLAQuarterlyJournalAdapter)

register_many(
    [
        "jlep.net",
        "www.californialawreview.org",
        "www.kentuckylawjournal.org",
        "www.regentuniversitylawreview.com",
        "www.vjil.org",
        "www.vlbr.org",
    ],
    SquarespaceAdapter,
)
register_many(
    [
        "www.cardozociclr.com",
        "www.rutgersracelawreview.org",
        "www.velj.org",
    ],
    WixAdapter,
)
register("www.texasbusinesslaw.org", PloneAdapter)
register("www.romanlegaltradition.org", RomanLegalTraditionAdapter)
register("romanlegaltradition.org", RomanLegalTraditionAdapter)
register("lawecommons.luc.edu", DigitalCommonsIssueArticleHopAdapter)
register("www.cumberlandlawreview.com", WixAdapter)
register("cumberlandlawreview.com", WixAdapter)
register("www.texenrls.org", WordPressAcademicBaseAdapter)
register("texenrls.org", WordPressAcademicBaseAdapter)
register("jle.aals.org", DigitalCommonsIssueArticleHopAdapter)
register("ibanet.org", GenericAdapter)
register_many(
    [
        "jnslp.com",
        "www.jnslp.com",
    ],
    GeorgetownJNSLPAdapter,
)
register_many(
    [
        "albanylawscitech.org",
        "www.albanylawscitech.org",
    ],
    AlbanyLawJournalScienceTechAdapter,
)
register_many(
    [
        "tsinghuachinalawreview.law.tsinghua.edu.cn",
        "www.tsinghuachinalawreview.law.tsinghua.edu.cn",
    ],
    GenericAdapter,
)

register_many(
    [
        "www.alwd.org",
        "www.journaloflaw.us",
        "www.luc.edu",
        "www.lsd-journal.net",
        "mckinneylaw.iu.edu",
        "www.samford.edu",
        "www.slu.edu",
        "www.telj.org",
    ],
    SelectorDrivenAdapter,
)
register("journals.indianapolis.iu.edu", OJSAdapter)
register("kb.osu.edu", DSpaceAdapter)
