"""Literal evidence spans derived from features that contributed to an accepted link."""

from __future__ import annotations

import re
from collections.abc import Collection

from sklearn.feature_extraction.text import strip_accents_unicode

_WORD = re.compile(r"[^\W_]+", re.UNICODE)


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
