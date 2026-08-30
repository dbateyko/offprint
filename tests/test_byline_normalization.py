"""A byline must survive the shape each adapter happens to write it in.

Adapters disagree: some write `authors` as a list, some as a bare string for a
single author, and at least one writes singular `author`. The singular spelling
used to be dropped silently -- `ArticleMetadata.from_dict` whitelists only
`authors`, so it landed in `extra` and never reached the typed field.
"""

from __future__ import annotations

from offprint.adapters.models import ArticleMetadata
from offprint.pipeline.normalization import _normalize_metadata


def test_singular_author_reaches_the_typed_field() -> None:
    """The georgetown_gltr regression."""
    assert ArticleMetadata.from_dict({"author": "Jane Roe"}).authors == ["Jane Roe"]


def test_singular_author_does_not_linger_in_extra() -> None:
    assert ArticleMetadata.from_dict({"author": "Jane Roe"}).extra == {}


def test_a_single_author_string_becomes_a_list() -> None:
    """`digital_commons_base` emits a bare str when there is exactly one author."""
    assert ArticleMetadata.from_dict({"authors": "Jane Roe"}).authors == ["Jane Roe"]


def test_a_real_author_list_is_left_alone() -> None:
    assert ArticleMetadata.from_dict({"authors": ["A", "B"]}).authors == ["A", "B"]


def test_plural_wins_over_singular() -> None:
    payload = {"authors": ["Jane Roe"], "author": "Someone Else"}
    assert ArticleMetadata.from_dict(payload).authors == ["Jane Roe"]


def test_normalization_folds_singular_author() -> None:
    assert _normalize_metadata({"author": "Jane Roe"})["authors"] == ["Jane Roe"]


def test_normalization_coerces_a_lone_string() -> None:
    assert _normalize_metadata({"authors": "Jane Roe"})["authors"] == ["Jane Roe"]


def test_normalization_still_defaults_authors_to_none() -> None:
    """An adapter that never looked must stay distinguishable from one that found nothing."""
    assert _normalize_metadata({})["authors"] is None
