"""Grounding checks for LLM exchanges, without pretending to infer text meaning."""

from __future__ import annotations

import re

from pydantic import StringConstraints
from typing_extensions import Annotated

Reason = Annotated[str, StringConstraints(strip_whitespace=True, min_length=12, max_length=2000)]


_REDACTION = re.compile(r"\[[^\]\n]{1,80}\]")
_WORD = re.compile(r"\w+", flags=re.UNICODE)
_MAX_REDACTION_GAP_WORDS = 8


def _compact_whitespace(value: str) -> str:
    return " ".join(value.split())


def _source_preserving_excerpt(quote: str, text: str) -> bool:
    """Accept extractive evidence that only omits/redacts short source spans.

    ChatGPT is instructed not to echo personal data. A safe response can therefore
    remove a name/account fragment from an otherwise literal quote. We accept that
    narrow transformation, but never inserted or reordered words.
    """

    cleaned = _REDACTION.sub(" ", quote)
    quote_words = _WORD.findall(cleaned)
    source_words = _WORD.findall(text)
    if not quote_words:
        return False
    cursor = 0
    first = True
    for word in quote_words:
        try:
            index = source_words.index(word, cursor)
        except ValueError:
            return False
        if not first and index - cursor > _MAX_REDACTION_GAP_WORDS:
            return False
        cursor = index + 1
        first = False
    return True


def validate_quote(quote: str, text: str) -> None:
    if not quote.strip():
        raise ValueError("La evidencia debe ser una cita literal no vacía del texto original.")
    if quote in text:
        return
    if _compact_whitespace(quote) in _compact_whitespace(text):
        return
    if _source_preserving_excerpt(quote, text):
        return
    raise ValueError(
        "La evidencia debe ser una cita literal o una redacción extractiva del texto original."
    )
