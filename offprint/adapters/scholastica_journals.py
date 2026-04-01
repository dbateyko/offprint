from __future__ import annotations

from .scholastica_base import ScholasticaBaseAdapter


class AlbanyGovernmentLawReviewAdapter(ScholasticaBaseAdapter):
    """Albany Government Law Review adapter."""

    def __init__(self, **kwargs):
        super().__init__(journal_slug="albany-government-law-review", **kwargs)


class AlbanyLawJournalScienceTechAdapter(ScholasticaBaseAdapter):
    """Albany Law Journal of Science & Technology adapter."""

    def __init__(self, **kwargs):
        super().__init__(journal_slug="albanylawjournal", **kwargs)


class AlbanyLawReviewAdapter(ScholasticaBaseAdapter):
    """Albany Law Review adapter."""

    def __init__(self, **kwargs):
        super().__init__(journal_slug="albany-law-review", **kwargs)


class AppalachianJournalLawAdapter(ScholasticaBaseAdapter):
    """Appalachian Journal of Law adapter."""

    def __init__(self, **kwargs):
        super().__init__(journal_slug="appalachian-journal-of-law", **kwargs)


class BostonCollegeLawReviewAdapter(ScholasticaBaseAdapter):
    """Boston College Law Review adapter."""

    def __init__(self, **kwargs):
        super().__init__(journal_slug="boston-college-law-review", **kwargs)
