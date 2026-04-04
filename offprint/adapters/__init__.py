from .base import Adapter
from .berkeley_btlj import BerkeleyBTLJAdapter
from .blogger import BloggerAdapter
from .colorado_law import ColoradoCTLJAdapter, ColoradoJTHTLAdapter
from .cambridge_core import CambridgeCoreAdapter
from .columbia_stlr import ColumbiaSTLRAdapter
from .digitalcommons import DigitalCommonsAdapter
from .digital_commons_issue_article_hop import DigitalCommonsIssueArticleHopAdapter
from .dspace import DSpaceAdapter
from .escholarship import EScholarshipAdapter
from .example_site import ExampleSiteAdapter
from .generic import GenericAdapter
from .georgetown_gltr import GeorgetownGLTRAdapter
from .georgetown_jnslp import GeorgetownJNSLPAdapter
from .greenbag import GreenBagAdapter
from .harvard_jolt import HarvardJOLTAdapter
from .jolt_base import JOLTBaseAdapter
from .illinois_jltp import IllinoisJLTPAdapter
from .jurimetrics import JurimetricsAdapter
from .nc_jolt import NorthCarolinaJOLTAdapter
from .nebraska_law_review import NebraskaLawReviewAdapter
from .ojs import OJSAdapter
from .plone import PloneAdapter
from .pubpub import PubPubAdapter
from .registry import UnmappedAdapterError, pick_adapter_for, register
from .selector_driven import SelectorDrivenAdapter
from .richmond_jolt import RichmondJOLTAdapter
from .rutgers_law_journals import RutgersLawJournalsAdapter
from .rutgers_law_review import RutgersLawReviewAdapter
from .weebly import WeeblyAdapter
from .stanford_stlr import StanfordSTLRAdapter
from .virginia_jolt import VirginiaJOLTAdapter
from .wustl_journals import WUSTLJournalsAdapter
from .wix import WixAdapter
from .yale_jolt import YaleJOLTAdapter
from .springer import SpringerAdapter
from .squarespace import SquarespaceAdapter
from .ajcl_archive import AJCLArchiveAdapter
from .aipla_quarterly_journal import AIPLAQuarterlyJournalAdapter
from .sc_jle import SCJLEAdapter
from .uh_hjil import UHHJILAdapter
from .umassd_ojs import UMassDOJSAdapter
from .und_law_review import UNDLawReviewAdapter
from .roman_legal_tradition import RomanLegalTraditionAdapter
from .tlcp import TLCPAdapter
from .yale_law_journal import YaleLawJournalAdapter
from .issue_archive_enumerator import (
    IssueArchiveEnumeratorAdapter,
    register_enumerator_config,
)

# Register site-specific adapters here
register("example.org", ExampleSiteAdapter)
# Prefer the Columbia-specific adapter; other OJS sites fall back to OJSAdapter
register("journals.library.columbia.edu", ColumbiaSTLRAdapter)
# Register Stanford Technology Law Review
register("law.stanford.edu", StanfordSTLRAdapter)
# Register Georgetown Law Technology Review
register("georgetownlawtechreview.org", GeorgetownGLTRAdapter)

# Register Yale Law Journal
register("www.yalelawjournal.org", YaleLawJournalAdapter)
register("yalelawjournal.org", YaleLawJournalAdapter)

# Register DigitalCommons/Scholarship repositories
register("digitalcommons.law.umaryland.edu", DigitalCommonsIssueArticleHopAdapter)  # Maryland JBTL
register("digitalcommons.law.uw.edu", DigitalCommonsIssueArticleHopAdapter)  # Washington WJLTA
register("scholarship.law.duke.edu", DigitalCommonsIssueArticleHopAdapter)  # Duke LTR
register("scholarlycommons.law.northwestern.edu", DigitalCommonsIssueArticleHopAdapter)  # Northwestern JTIP
register("digitalcommons.law.scu.edu", DigitalCommonsIssueArticleHopAdapter)  # Santa Clara HTLJ
register("repository.uclawsf.edu", DigitalCommonsIssueArticleHopAdapter)  # UC Hastings STLJ
register("scholarship.law.ufl.edu", DigitalCommonsIssueArticleHopAdapter)  # Florida JTLP
register("scholarship.law.vanderbilt.edu", DigitalCommonsIssueArticleHopAdapter)  # Vanderbilt JETLAW
register("scholarship.law.edu", DigitalCommonsIssueArticleHopAdapter)  # Catholic University JLT
register("red.library.usd.edu", DigitalCommonsIssueArticleHopAdapter)  # South Dakota Law Review

# Register custom platform adapters
register("jolt.law.harvard.edu", HarvardJOLTAdapter)  # Harvard JOLT
register("journals.library.wustl.edu", WUSTLJournalsAdapter)  # WUSTL Journals host
register("btlj.org", BerkeleyBTLJAdapter)  # Berkeley Technology Law Journal
register("illinoisjltp.com", IllinoisJLTPAdapter)  # Illinois JLTP
register("ncjolt.org", NorthCarolinaJOLTAdapter)  # North Carolina JOLT
register("yjolt.org", YaleJOLTAdapter)  # Yale JOLT
register("www.vjolt.org", VirginiaJOLTAdapter)  # Virginia JOLT
register("jolt.richmond.edu", RichmondJOLTAdapter)  # Richmond JOLT
register("www.jthtl.org", ColoradoJTHTLAdapter)  # Colorado JTHTL
register("ctlj.colorado.edu", ColoradoCTLJAdapter)  # Colorado CTLJ
register("journals.law.harvard.edu", IssueArchiveEnumeratorAdapter)  # Harvard Law journals (JLPP, etc.)

__all__ = [
    "Adapter",
    "GenericAdapter",
    "ExampleSiteAdapter",
    "BloggerAdapter",
    "StanfordSTLRAdapter",
    "GeorgetownGLTRAdapter",
    "GeorgetownJNSLPAdapter",
    "GreenBagAdapter",
    "DigitalCommonsAdapter",
    "DSpaceAdapter",
    "EScholarshipAdapter",
    "CambridgeCoreAdapter",
    "PloneAdapter",
    "JOLTBaseAdapter",
    "HarvardJOLTAdapter",
    "BerkeleyBTLJAdapter",
    "IllinoisJLTPAdapter",
    "JurimetricsAdapter",
    "NorthCarolinaJOLTAdapter",
    "NebraskaLawReviewAdapter",
    "YaleJOLTAdapter",
    "VirginiaJOLTAdapter",
    "RichmondJOLTAdapter",
    "RutgersLawJournalsAdapter",
    "RutgersLawReviewAdapter",
    "ColoradoJTHTLAdapter",
    "ColoradoCTLJAdapter",
    "WUSTLJournalsAdapter",
    "PubPubAdapter",
    "WeeblyAdapter",
    "SquarespaceAdapter",
    "AJCLArchiveAdapter",
    "AIPLAQuarterlyJournalAdapter",
    "SCJLEAdapter",
    "TLCPAdapter",
    "UHHJILAdapter",
    "UMassDOJSAdapter",
    "UNDLawReviewAdapter",
    "RomanLegalTraditionAdapter",
    "YaleLawJournalAdapter",
    "WixAdapter",
    "SpringerAdapter",
    "IssueArchiveEnumeratorAdapter",
    "register_enumerator_config",
    "register",
    "pick_adapter_for",
    "UnmappedAdapterError",
    "OJSAdapter",
    "SelectorDrivenAdapter",
]
