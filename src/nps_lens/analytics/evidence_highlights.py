"""Literal evidence spans derived from features that contributed to an accepted link."""

from __future__ import annotations

import re
from collections.abc import Collection

from sklearn.feature_extraction.text import strip_accents_unicode

from nps_lens.analytics.text_mining import STOPWORDS_ES

_WORD = re.compile(r"[^\W_]+", re.UNICODE)
_STOPWORDS = {strip_accents_unicode(word) for word in STOPWORDS_ES}


def contributing_terms(
    text: str, word_features: set[str], char_features: set[str]
) -> tuple[str, ...]:
    """Map nonzero shared word/character features back to visible incident words."""
    terms = set()
    for match in _WORD.finditer(text):
        word = strip_accents_unicode(match.group().lower())
        if len(word) < 3 or word in _STOPWORDS:
            continue
        morphological_char_match = any(
            len(feature.strip()) >= 4
            and (
                word == feature.strip()
                or (word.startswith(feature.strip()) and len(word) - len(feature.strip()) <= 2)
                or (feature.strip().startswith(word) and len(feature.strip()) - len(word) <= 2)
            )
            for feature in char_features
        )
        if word in word_features or morphological_char_match:
            terms.add(word)
    return tuple(sorted(terms))


def evidence_segments(text: str, terms: Collection[str]) -> list[dict[str, object]]:
    """Preserve original text and encode emphasis without HTML or JS offset assumptions."""
    if not terms:
        return []
    segments: list[dict[str, object]] = []
    cursor = 0
    for match in _WORD.finditer(text):
        if strip_accents_unicode(match.group().lower()) not in terms:
            continue
        if match.start() > cursor:
            segments.append({"text": text[cursor : match.start()], "bold": False})
        segments.append({"text": match.group(), "bold": True})
        cursor = match.end()
    if not segments:
        return []
    if cursor < len(text):
        segments.append({"text": text[cursor:], "bold": False})
    return segments
