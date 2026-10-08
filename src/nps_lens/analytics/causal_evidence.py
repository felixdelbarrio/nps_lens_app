"""One conservative evidence model and impact ranking for every output channel.

Semantic judgments must be supplied by the classifier/reviewer, never inferred from
confidence, a shared category, or keywords. Dates and recurrence are checked here.
"""

from __future__ import annotations

import math
from typing import Any, Mapping

import pandas as pd

from nps_lens.analytics.linking_policy import LINK_MAX_DAYS_APART, temporal_mask
from nps_lens.analytics.signal_quality import is_reserve_category
from nps_lens.core.nps_math import valid_nps_scores

EVIDENCE_COPY = {
    "SIN_EVIDENCIA": ("Sin evidencia", "No hay vínculo defendible."),
    "INDICIO_SEMANTICO": (
        "Indicio semántico",
        "Evidencia semántica compatible; requiere validación operativa.",
    ),
    "ASOCIACION_OPERATIVA_CONSISTENTE": (
        "Asociación operativa consistente",
        "Asociación operativa consistente: misma tarea y síntoma, dentro de ventana temporal.",
    ),
    "EVIDENCIA_OPERATIVA_RECURRENTE": (
        "Evidencia operativa recurrente",
        "Evidencia operativa recurrente: misma tarea, mismo síntoma y recurrencia.",
    ),
    "CAUSALIDAD_NO_ACREDITADA": (
        "Asociación no acreditada",
        "No hay evidencia suficiente para sostener una vinculación.",
    ),
}


def link_confidence_label(engine: str) -> str:
    return "CONFIANZA SEMÁNTICA" if engine == "llm" else "SIMILITUD TEXTUAL"


def number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def linked_comment_metrics(links: pd.DataFrame) -> dict[str, Any]:
    """Count each response once; frequency-weight scores across the entire scenario."""
    unique = links[["nps_id", "nps_score"]].drop_duplicates("nps_id")
    scores = valid_nps_scores(unique["nps_score"])
    counts = scores.dropna().astype(int).value_counts(sort=False).sort_index()
    scored = int(counts.sum())
    distribution = [
        {
            "score": int(score),
            "count": int(count),
            "label": f"Score {score} : {count} {'comentario' if count == 1 else 'comentarios'}",
        }
        for score, count in counts.items()
    ]
    missing = len(unique) - scored
    if missing:
        distribution.append(
            {
                "score": None,
                "count": missing,
                "label": f"Sin score : {missing} {'comentario' if missing == 1 else 'comentarios'}",
            }
        )
    return {
        "linked_comments": len(unique),
        "score_distribution": distribution,
        "avg_score": (
            sum(int(score) * int(count) for score, count in counts.items()) / scored
            if scored
            else None
        ),
        "detractor_rate": int(counts.loc[counts.index <= 6].sum()) / scored if scored else 0.0,
    }


def engine_quality(row: Mapping[str, Any]) -> float:
    """Select the engine's own score, without treating confidence as cosine similarity."""
    column = (
        "avg_semantic_confidence" if row.get("causal_engine") == "llm" else "avg_text_similarity"
    )
    return number(row.get(column))


def scenario_impact_score(row: Mapping[str, Any]) -> float:
    if any(
        is_reserve_category(row.get(k, "")) for k in ("palanca", "subpalanca", "nps_topic", "title")
    ):
        return -1000.0
    comments = max(0, number(row.get("linked_comments")))
    incidents = max(0, number(row.get("linked_incidents")))
    confidence = min(1, max(0, engine_quality(row)))
    score = min(10, max(0, number(row.get("avg_nps", row.get("avg_score")), 10)))
    detractors = min(1, max(0, number(row.get("detractor_rate"))))
    # Bounded contributions keep a large, vague cluster from dominating strong evidence.
    impact = (
        15 * min(1, math.log1p(comments) / math.log(21))
        + 10 * min(1, math.log1p(incidents) / math.log(11))
        + 30 * (10 - score) / 10 * detractors
        + 20 * confidence
        + 15 * min(1, max(0, incidents - 1) / 3)
        + 10 * min(1, max(0, number(row.get("freshness"))))
    )
    impact -= 15 if comments < 3 else 0
    impact -= 15 if row.get("causal_engine") == "llm" and confidence < 0.6 else 0
    impact -= 15 if score > 6 else 0
    impact -= 15 * (1 - min(1, max(0, number(row.get("quote_coverage")))))
    impact -= 10 * (1 - min(1, max(0, number(row.get("narrative_coverage")))))
    return round(impact, 4)


class CausalEvidenceEvaluator:
    def __init__(self, max_days_apart: int = LINK_MAX_DAYS_APART) -> None:
        self.max_days_apart = max_days_apart

    def evaluate(self, link: Mapping[str, Any]) -> dict[str, Any]:
        warnings = []
        confidence = (
            number(link.get("semantic_confidence"))
            if link.get("causal_engine") == "llm"
            else number(link.get("text_similarity"))
        )
        quotes = all(
            isinstance(link.get(key), str) and bool(link[key].strip())
            for key in ("comment_quote", "incident_quote")
        )
        same_task = link.get("same_task") is True
        same_symptom = link.get("same_symptom") is True
        comment_date = pd.to_datetime(
            link.get("comment_date", link.get("nps_date")), errors="coerce", utc=True
        )
        start = pd.to_datetime(
            link.get("incident_start_date", link.get("incident_date")), errors="coerce", utc=True
        )
        close = pd.to_datetime(link.get("incident_close_date"), errors="coerce", utc=True)
        temporal = bool(temporal_mask(comment_date, start, self.max_days_apart))
        if pd.notna(close) and pd.notna(start) and close < start:
            temporal = False
            warnings.append("Fechas de apertura/cierre incompatibles.")
        if not temporal:
            warnings.append(
                "Temporalidad no acreditada: falta fecha o excede la ventana de asociación."
            )
        if not quotes:
            warnings.append("Faltan citas literales en ambas fuentes.")
        if not same_task or not same_symptom:
            warnings.append("Misma tarea y síntoma pendientes de validación semántica.")
        reserve = any(
            is_reserve_category(link.get(k, ""))
            for k in ("palanca", "subpalanca", "nps_topic", "incident_topic")
        )
        if not link.get("nps_id") or not link.get("incident_id") or reserve or confidence <= 0:
            level = "SIN_EVIDENCIA"
        elif link.get("causal_engine") == "llm" and confidence < 0.3:
            level = "CAUSALIDAD_NO_ACREDITADA"
        elif not (
            quotes
            and same_task
            and same_symptom
            and temporal
            and link.get("causal_engine") == "llm"
            and confidence >= 0.6
        ):
            level = "INDICIO_SEMANTICO"
        else:
            level = "ASOCIACION_OPERATIVA_CONSISTENTE"
            if (
                number(link.get("linked_comments")) >= 3
                and number(link.get("linked_incidents")) >= 2
                and number(link.get("avg_score"), 10) <= 6
                and number(link.get("detractor_rate")) >= 0.7
                and confidence >= 0.8
            ):
                level = "EVIDENCIA_OPERATIVA_RECURRENTE"
        label, reason = EVIDENCE_COPY[level]
        return {
            "evidence_level": level,
            "evidence_label": label,
            "evidence_reason": reason,
            "confidence_label": (
                "Alta" if confidence >= 0.8 else "Media" if confidence >= 0.6 else "Baja"
            ),
            "actionability": (
                "Alta"
                if level == "EVIDENCIA_OPERATIVA_RECURRENTE"
                else "Media" if level == "ASOCIACION_OPERATIVA_CONSISTENTE" else "Baja"
            ),
            "warnings": warnings,
            "temporal_relation": "valid" if temporal else "unverified",
        }

    def evaluate_scenario(self, links: pd.DataFrame) -> dict[str, Any]:
        pairs = links.drop_duplicates(["nps_id", "incident_id"])
        score_metrics = linked_comment_metrics(pairs)
        metrics = {**score_metrics, "linked_incidents": pairs.incident_id.nunique()}
        evaluated = [self.evaluate({**row, **metrics}) for row in pairs.to_dict("records")]
        # A heterogeneous scenario cannot inherit its strongest pair's assertion.
        strength = {
            "SIN_EVIDENCIA": 0,
            "CAUSALIDAD_NO_ACREDITADA": 1,
            "INDICIO_SEMANTICO": 2,
            "ASOCIACION_OPERATIVA_CONSISTENTE": 3,
            "EVIDENCIA_OPERATIVA_RECURRENTE": 4,
        }
        evidence = (
            min(evaluated, key=lambda r: strength[r["evidence_level"]])
            if evaluated
            else self.evaluate({})
        )
        evidence["warnings"] = list(dict.fromkeys(w for e in evaluated for w in e["warnings"]))
        evidence.update(score_metrics)
        evidence["quote_coverage"] = sum(
            all(
                isinstance(r.get(key), str) and bool(r[key].strip())
                for key in ("comment_quote", "incident_quote")
            )
            for r in pairs.to_dict("records")
        ) / max(1, len(pairs))
        evidence["narrative_coverage"] = (
            float(
                pairs.get("incident_summary", pd.Series("", index=pairs.index))
                .fillna("")
                .str.strip()
                .ne("")
                .mean()
            )
            if len(pairs)
            else 0.0
        )
        dates = pd.to_datetime(pairs.get("nps_date"), errors="coerce", utc=True)
        starts = pd.to_datetime(pairs.get("incident_date"), errors="coerce", utc=True)
        evidence["freshness"] = (
            max(
                0.0,
                1
                - number((dates - starts).dt.days.abs().mean(), self.max_days_apart)
                / max(1, self.max_days_apart),
            )
            if dates is not None and starts is not None
            else 0.0
        )
        return evidence
