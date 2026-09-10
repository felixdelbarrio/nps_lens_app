from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

STOPWORDS_ES = {
    "de",
    "la",
    "que",
    "el",
    "en",
    "y",
    "a",
    "los",
    "del",
    "se",
    "las",
    "por",
    "un",
    "para",
    "con",
    "no",
    "una",
    "su",
    "al",
    "lo",
    "como",
    "más",
    "pero",
    "sus",
    "le",
    "ya",
    "o",
    "este",
    "sí",
    "porque",
    "esta",
    "entre",
    "cuando",
    "muy",
    "sin",
    "sobre",
    "también",
    "me",
    "hasta",
    "hay",
    "donde",
    "quien",
    "desde",
    "todo",
    "nos",
    "durante",
    "todos",
    "uno",
    "les",
    "ni",
    "contra",
    "otros",
    "ese",
    "eso",
    "ante",
    "ellos",
    "e",
    "esto",
    "mí",
    "antes",
    "algunos",
    "qué",
    "unos",
    "yo",
    "otro",
    "otras",
    "otra",
    "él",
    "tanto",
    "esa",
    "estos",
    "mucho",
    "quienes",
    "nada",
    "muchos",
    "cual",
    "poco",
    "ella",
    "estar",
    "estas",
    "algunas",
    "algo",
    "nosotros",
    "mi",
}


TONE_LEXICON: dict[str, list[str]] = {
    "frustracion": [
        "no puedo",
        "no deja",
        "error",
        "falla",
        "fallo",
        "bloquea",
        "se queda",
        "lento",
        "intermitente",
        "timeout",
    ],
    "urgencia": ["urge", "urgente", "ya", "hoy", "ahora"],
    "confusion": ["no entiendo", "como", "donde", "por que", "qué"],
    "aprecio": ["excelente", "muy bien", "genial", "gracias", "práctica", "facil", "rápida"],
    "sugerencia": ["podrian", "deberian", "seria bueno", "me gustaria", "falta"],
}


@dataclass(frozen=True)
class TopicCluster:
    cluster_id: int
    n: int
    top_terms: list[str]
    examples: list[str]


def preprocess_text(value: object) -> str:
    if value is None or bool(pd.isna(value)):
        return ""
    s2 = str(value).lower()
    s2 = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in s2)
    s2 = " ".join(s2.split())
    return s2


def summarize_taxonomy(frame: pd.DataFrame, limit: int = 10) -> list[TopicCluster]:
    """Summarize the resolved lens without training or clustering on visual filters."""
    if frame.empty:
        return []
    labels = frame.get("Subpalanca", pd.Series("", index=frame.index)).astype("string").fillna("")
    labels = labels.mask(labels.eq(""), frame.get("Palanca", pd.Series("", index=frame.index)))
    comments = (
        frame.get("Comment", frame.get("comment_txt", pd.Series("", index=frame.index)))
        .fillna("")
        .astype(str)
    )
    return [
        TopicCluster(
            i,
            int(count),
            [str(label)],
            comments.loc[labels.eq(label) & comments.str.strip().ne("")].head(5).tolist(),
        )
        for i, (label, count) in enumerate(labels[labels.ne("")].value_counts().head(limit).items())
    ]


def classify_tone(text: Optional[str]) -> list[str]:
    if not text:
        return []
    t = preprocess_text(text)
    labels: list[str] = []
    for label, pats in TONE_LEXICON.items():
        for p in pats:
            if p in t:
                labels.append(label)
                break
    return sorted(set(labels))
