"""Shared detection of comment content and placeholder labels."""

from __future__ import annotations

import string
import unicodedata
from collections.abc import Callable

import pandas as pd

COMMENT_COLUMNS = ("comment_txt", "Comment", "Comentario", "Comentarios", "comentario")
_CONTENT_PLACEHOLDERS = frozenset(
    {
        "",
        "nan",
        "nat",
        "none",
        "null",
        "na",
        "n/a",
        "nd",
        "n/d",
        "sin comentario",
        "sin comentarios",
        "sincomentario",
        "sincomentarios",
        "sin contenido",
        "sin informacion",
        "sin datos",
        "sin respuesta",
        "sin tema",
        "sin clasificar",
        "sin clasificacion",
        "no clasificado",
        "no clasificada",
        "no hay comentarios",
        "no informado",
        "no disponible",
        "no aplica",
        "comentario vacio",
        "vacio",
        "no comment",
        "no comments",
        "no content",
        "empty",
    }
)
_NONSPECIFIC_CONTENT = _CONTENT_PLACEHOLDERS | {
    "generico",
    "generica",
    "genericos",
    "genericas",
    "general",
    "otros",
    "otras",
    "tema generico",
    "temas genericos",
    "comentario generico",
    "comentarios genericos",
    "contenido generico",
    "otros temas",
    "sin detalle",
    "sin detalles",
    "sin especificar",
    "no especificado",
    "no especificada",
    "sin clasificacion tematica",
    "informacion insuficiente",
    "tema no cubierto",
}
_LABEL_TRIM = string.whitespace + string.punctuation + "¡¿—–…"
_MAX_MARKER_LENGTH = 2 * max(map(len, _NONSPECIFIC_CONTENT))


def normalize_content(value: object) -> str:
    text = "" if value is None or value is pd.NA else str(value)
    text = "".join(
        c for c in unicodedata.normalize("NFKD", text.casefold()) if not unicodedata.combining(c)
    )
    return " ".join(text.split()).strip(_LABEL_TRIM)


def _matches_marker(value: object, markers: frozenset[str]) -> bool:
    text = (
        "" if value is None or value is pd.NA else " ".join(str(value).split()).strip(_LABEL_TRIM)
    )
    return len(text) <= _MAX_MARKER_LENGTH and normalize_content(text) in markers


def is_content_placeholder(value: object) -> bool:
    return _matches_marker(value, _CONTENT_PLACEHOLDERS)


def is_nonspecific_content(value: object) -> bool:
    return _matches_marker(value, _NONSPECIFIC_CONTENT)


def _comment_mask(
    frame: pd.DataFrame, excluded: Callable[[object], bool]
) -> pd.Series[bool] | None:
    for column in COMMENT_COLUMNS:
        if column in frame.columns:
            text = frame[column].astype(object)
            placeholders = {value: excluded(value) for value in text.unique()}
            return ~text.map(placeholders).astype(bool)
    return None


def nonempty_comment_mask(frame: pd.DataFrame) -> pd.Series[bool] | None:
    return _comment_mask(frame, is_content_placeholder)


def useful_comment_mask(frame: pd.DataFrame) -> pd.Series[bool] | None:
    return _comment_mask(frame, is_nonspecific_content)
