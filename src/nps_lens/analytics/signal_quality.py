"""Reserve labels are quality signals, never actionable drivers. Keep KPI populations intact."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

import pandas as pd

INSUFFICIENT_WARNING_RATE = 0.20
SUSPICIOUS_REVIEW_RATE = 0.10


def _normalize(value: object) -> str:
    return "".join(
        c
        for c in unicodedata.normalize(
            "NFKD", str("" if value is None or value is pd.NA else value).casefold()
        )
        if not unicodedata.combining(c)
    )


def is_reserve_category(value: object) -> bool:
    parts = re.split(r"\s*(?:>|/|::|·)\s*", _normalize(value))
    return any(
        p.strip() in {"sin clasificacion tematica", "informacion insuficiente", "tema no cubierto"}
        for p in parts
    )


def actionable_rows(frame: pd.DataFrame) -> pd.DataFrame:
    mask = pd.Series(True, index=frame.index)
    for column in (
        "Palanca",
        "Subpalanca",
        "palanca",
        "subpalanca",
        "value",
        "nps_topic",
        "title",
        "incident_topic",
    ):
        if column in frame:
            mask &= ~frame[column].astype(object).map(is_reserve_category).astype(bool)
    return frame.loc[mask].copy()


def signal_quality(frame: pd.DataFrame) -> dict[str, Any]:
    sub = frame.get("Subpalanca", pd.Series("", index=frame.index)).map(_normalize)
    insufficient = int(sub.eq("informacion insuficiente").sum())
    uncovered = int(sub.eq("tema no cubierto").sum())
    warnings = []
    if len(frame) and insufficient / len(frame) > INSUFFICIENT_WARNING_RATE:
        warnings.append(
            "La taxonomía o la ingesta no están capturando suficiente señal accionable."
        )
    return {
        "insufficient_comments": insufficient,
        "uncovered_topics": uncovered,
        "total_comments": len(frame),
        "warnings": warnings,
        "message": f"Calidad de señal: {insufficient} comentarios no accionables / {uncovered} temas no cubiertos.",
    }


def audit_classifications(comments: dict[str, str], assignments: dict[str, Any]) -> dict[str, Any]:
    """Flag interpretations for human/LLM review; never assign a category by keywords."""
    suspicious = []
    for key, assignment in assignments.items():
        sub = assignment.get("primary_classification", {}).get("sublever", "")
        if _normalize(sub) != "informacion insuficiente":
            continue
        if re.search(
            r"\b(funciona\w*|entr[ao]\w*|cierr\w*|token|transferencia\w*|pago\w*|lent[ao]\w*|error\w*|pesim[ao]|excelente)\b",
            _normalize(comments.get(key, "")),
        ):
            suspicious.append(key)
    rate = len(suspicious) / len(assignments) if assignments else 0.0
    return {
        "suspicious_ids": suspicious,
        "suspicious_count": len(suspicious),
        "review_required": rate > SUSPICIOUS_REVIEW_RATE,
        "suspicious_rate": rate,
    }
