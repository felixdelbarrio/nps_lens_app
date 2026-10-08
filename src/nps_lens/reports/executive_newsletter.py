from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from nps_lens.analytics.signal_quality import signal_quality
from nps_lens.design.brand import BRAND
from nps_lens.domain.comment_text import is_nonspecific_content
from nps_lens.domain.privacy import redact_operational_snippet
from nps_lens.reports.coherence import validate_metric_payload
from nps_lens.reports.narrative import (
    experience_topics,
    ordered_scenarios,
    topic_name,
    unique_comments,
)
from nps_lens.services.analytics.kpis_service import format_metric, format_percentage, format_volume


def _dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def _number(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


def _normalized(value: object) -> str:
    return " ".join(str(value or "").casefold().split())


def _period_label(period_start: date, period_end: date) -> str:
    months = (
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "octubre",
        "noviembre",
        "diciembre",
    )
    if period_start.year == period_end.year and period_start.month == period_end.month:
        return (
            f"{period_start.day}–{period_end.day} {months[period_end.month - 1]} {period_end.year}"
        )
    return f"{period_start:%d/%m/%Y}–{period_end:%d/%m/%Y}"


def _focus_rows(topics: pd.DataFrame) -> list[dict[str, object]]:
    return [
        {
            "label": row["value"],
            "score": row["score"],
            "nps": format_metric(row.get("nps")),
            "detractors": format_percentage(row.get("detractor_rate")),
            "opinions": format_volume(row.get("n")),
        }
        for row in topics.to_dict("records")
    ]


def _connections(linking: dict[str, object], topics: pd.DataFrame) -> list[dict[str, object]]:
    cards = _list(_dict(linking.get("scenarios")).get("cards"))
    ordered = pd.DataFrame(cards)
    if "narrative_rank" not in ordered:
        ordered = ordered_scenarios(ordered, topics)
    connections = []
    for card in ordered.to_dict("records"):
        topic = str(card.get("title") or card.get("nps_topic") or "")
        if is_nonspecific_content(topic):
            continue
        comments = unique_comments(card.get("comment_records"))
        connections.append(
            {
                "topic": topic,
                "anchor_topic": topic_name(card),
                "semantic_links": int(_number(card.get("linked_pairs")) or 0),
                "evidence_reason": card.get(
                    "evidence_reason",
                    "Evidencia semántica compatible; requiere validación operativa.",
                ),
                "comments": [
                    " ".join(redact_operational_snippet(str(item.get("comment") or "")).split())
                    for item in comments
                ],
            }
        )
    return connections


def build_executive_newsletter(
    *,
    current_df: pd.DataFrame,
    period_kpis: dict[str, object],
    linking: dict[str, object],
    topic_channel: str,
    period_start: date,
    period_end: date,
) -> dict[str, object]:
    """Build the single, publication-time editorial model used by every newsletter renderer."""
    validate_metric_payload(period_kpis)
    period = _dict(period_kpis.get("period"))
    display = _dict(period.get("display"))
    deltas = _dict(period.get("deltas"))
    score_specs = (
        ("Comentarios", "comments"),
        ("NPS clásico mensual", "classic_nps"),
        ("Score medio", "nps_average"),
        ("Promotores", "promoter_rate"),
        ("Detractores", "detractor_rate"),
    )
    scorecard = []
    for label, key in score_specs:
        delta = _dict(deltas.get(key))
        scorecard.append(
            {
                "label": label,
                "value": str(display.get(key, "n/d")),
                "delta": str(delta.get("display") or ""),
            }
        )

    topics = experience_topics(current_df, channel=topic_channel)
    focus = _focus_rows(topics)
    primary = focus[0] if focus else None
    connections = _connections(linking, topics)

    quotes: list[str] = []
    visited: set[str] = set()
    for connection in connections:
        topic = _normalized(connection["anchor_topic"])
        if topic in visited:
            continue
        candidates = [
            str(q) for q in _list(connection.get("comments")) if not is_nonspecific_content(str(q))
        ]
        if not candidates:
            continue
        visited.add(topic)
        quote = next((q for q in candidates if q not in quotes), None)
        if quote:
            quotes.append(quote)
        if len(visited) == 2:
            break
    signals = [
        {
            "label": row["label"],
            "reason": f"Score medio {format_metric(row['score'])}; NPS {row['nps']}; {row['detractors']} detractores; {row['opinions']} opiniones.",
        }
        for row in focus
    ]

    if primary:
        headline = f"El menor score medio de experiencia está en {str(primary['label']).casefold()}"
        related = next(
            (
                item
                for item in connections
                if _normalized(str(item["anchor_topic"]).split(" > ")[0])
                == _normalized(primary["label"])
            ),
            None,
        )
        reason = (
            f"Caso relacionado: {related['topic']}. {related['evidence_reason']}"
            if related
            else "Los casos enlazados se presentan por tópico; requieren validación operativa."
        )
        lead = (
            f"La lectura del periodo sitúa {primary['label']} en el centro de la señal: "
            f"score medio {format_metric(primary['score'])}, NPS {primary['nps']}, {primary['detractors']} detractores y "
            f"{primary['opinions']} opiniones. {reason}"
        )
    else:
        headline = "No hay evidencia suficiente para destacar un foco de fricción"
        lead = (
            "El NPS incluye todas las respuestas del periodo. No hay temas específicos "
            "con comentarios útiles para elaborar este insight."
        )

    return {
        "brand": BRAND["name"],
        "initiative": BRAND["initiative"],
        "initiative_name": BRAND["initiative_name"],
        "product": "NPS Lens",
        "promise": "La voz del cliente conectada con la operación",
        "period": _period_label(period_start, period_end),
        "headline": headline,
        "lead": lead,
        "signal_quality": signal_quality(current_df),
        "scorecard": scorecard,
        "quotes": quotes,
        "signals": signals,
    }
