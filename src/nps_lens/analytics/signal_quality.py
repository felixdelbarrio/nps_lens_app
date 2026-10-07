"""Reserve labels are quality signals, never actionable drivers. Keep KPI populations intact."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from nps_lens.domain.comment_text import (
    is_nonspecific_content,
    nonempty_comment_mask,
    normalize_content,
    useful_comment_mask,
)

INSUFFICIENT_WARNING_RATE = 0.20


def is_reserve_category(value: object) -> bool:
    parts = re.split(r"\s*(?:>|/|::|·)\s*", normalize_content(value))
    return any(is_nonspecific_content(part) for part in parts)


def actionable_rows(frame: pd.DataFrame) -> pd.DataFrame:
    comment_mask = useful_comment_mask(frame)
    mask = comment_mask if comment_mask is not None else pd.Series(True, index=frame.index)
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
            labels = frame[column].astype(object)
            reserves = {value: is_reserve_category(value) for value in labels.unique()}
            mask &= ~labels.map(reserves).astype(bool)
    return frame.loc[mask].copy()


def signal_quality(frame: pd.DataFrame) -> dict[str, Any]:
    comment_mask = nonempty_comment_mask(frame)
    frame = frame.loc[comment_mask] if comment_mask is not None else frame.iloc[:0]
    sub = frame.get("Subpalanca", pd.Series("", index=frame.index)).map(normalize_content)
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
        "message": (
            f"De {len(frame)} comentarios: {insufficient} no accionables / "
            f"{uncovered} con tema no cubierto."
        ),
    }


def audit_classifications(
    comments: dict[str, str], assignments: dict[str, Any], catalog: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Report reserve review signals, not semantic verdicts or quality gates."""
    insufficient = []
    uncovered = []
    suspicious = []
    uncovered_suspicious = []
    labels = {
        normalize_content(pair["sublever"])
        for pair in (catalog or {}).values()
        if not is_reserve_category(pair["lever"])
    }
    for key, assignment in assignments.items():
        sub = normalize_content(assignment.get("primary_classification", {}).get("sublever", ""))
        text = normalize_content(comments.get(key, ""))
        if sub == "informacion insuficiente":
            insufficient.append(key)
            if re.search(
                r"\b(funciona\w*|entr[ao]\w*|cierr\w*|token|transferencia\w*|pago\w*|lent[ao]\w*|error\w*|pesim[ao]|excelente)\b",
                text,
            ):
                suspicious.append(key)
        elif sub == "tema no cubierto":
            uncovered.append(key)
            if any(re.search(r"\b" + re.escape(label) + r"\b", text) for label in labels):
                uncovered_suspicious.append(key)
    return {
        "insufficient_count": len(insufficient),
        "suspicious_ids": suspicious,
        "suspicious_count": len(suspicious),
        "suspicious_rate": len(suspicious) / len(insufficient) if insufficient else 0.0,
        "uncovered_count": len(uncovered),
        "uncovered_suspicious_ids": uncovered_suspicious,
        "uncovered_suspicious_count": len(uncovered_suspicious),
        "uncovered_suspicious_rate": (
            len(uncovered_suspicious) / len(uncovered) if uncovered else 0.0
        ),
        "review_required": bool(suspicious or uncovered_suspicious),
    }
