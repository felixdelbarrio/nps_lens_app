from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from nps_lens.analytics.causal_evidence import scenario_impact_score
from nps_lens.analytics.channel_topic_scope import restrict_to_topics, topics_observed_in_channel
from nps_lens.analytics.drivers import grouped_driver_stats
from nps_lens.analytics.signal_quality import actionable_rows, signal_quality
from nps_lens.design.brand import BRAND
from nps_lens.domain.comment_text import is_nonspecific_content
from nps_lens.domain.privacy import redact_operational_snippet
from nps_lens.reports.coherence import validate_metric_payload
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


def _focus_rows(current_df: pd.DataFrame, *, topic_channel: str) -> list[dict[str, object]]:
    if current_df is None or current_df.empty or "Palanca" not in current_df.columns:
        return []
    topic_keys = topics_observed_in_channel(current_df, "Palanca", topic_channel)
    source = actionable_rows(restrict_to_topics(current_df, "Palanca", topic_keys))
    grouped = grouped_driver_stats(source, "Palanca")
    if grouped.empty:
        return []
    grouped["detractor_volume"] = grouped["det_count"].fillna(0)
    ranked = (
        grouped.sort_values(
            ["detractor_volume", "detractor_rate", "n"], ascending=[False, False, False]
        )["Palanca"]
        .astype(str)
        .tolist()
    )
    order = ranked[:4]
    lookup = grouped.set_index("Palanca", drop=False)
    rows: list[dict[str, object]] = []
    for label in order:
        row = lookup.loc[label]
        rows.append(
            {
                "label": label,
                "nps": format_metric(row.get("nps")),
                "detractors": format_percentage(row.get("detractor_rate")),
                "opinions": format_volume(row.get("n")),
            }
        )
    return rows


def _connections(linking: dict[str, object]) -> list[dict[str, object]]:
    scenario_block = _dict(linking.get("scenarios"))
    cards = [card for card in _list(scenario_block.get("cards")) if isinstance(card, dict)]
    if cards:
        scoped = actionable_rows(pd.DataFrame(cards))
        cards = scoped.astype(object).where(pd.notna(scoped), None).to_dict("records")
    selected = sorted(
        (card for card in cards if scenario_impact_score(card) > -1000),
        key=lambda card: (
            _number(card.get("linked_incidents")) or 0,
            _number(card.get("linked_comments")) or 0,
            scenario_impact_score(card),
        ),
        reverse=True,
    )[:4]
    connections: list[dict[str, object]] = []
    for card in selected:
        topic = str(card.get("title") or card.get("nps_topic") or "")
        if is_nonspecific_content(topic):
            continue
        comments = [
            record for record in _list(card.get("comment_records")) if isinstance(record, dict)
        ]
        connections.append(
            {
                "topic": topic,
                "anchor_topic": str(card.get("anchor_topic") or card.get("nps_topic") or ""),
                "semantic_links": int(_number(card.get("linked_pairs")) or 0),
                "evidence_reason": card.get(
                    "evidence_reason",
                    "Evidencia semántica compatible; requiere validación operativa.",
                ),
                "comments": [
                    redact_operational_snippet(str(item.get("comment") or "")).strip()
                    for item in comments
                    if redact_operational_snippet(str(item.get("comment") or "")).strip()
                ][:2],
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

    focus = _focus_rows(current_df, topic_channel=topic_channel)
    primary = focus[0] if focus else None
    connections = _connections(linking)

    quotes: list[str] = []
    for connection in connections:
        for raw_quote in _list(connection.get("comments")):
            quote = str(raw_quote)
            if not is_nonspecific_content(quote) and quote not in quotes:
                quotes.append(quote)
            if len(quotes) == 4:
                break
        if len(quotes) == 4:
            break
    signals = [
        {
            "label": row["label"],
            "reason": f"NPS {row['nps']}; {row['detractors']} detractores; {row['opinions']} opiniones.",
        }
        for row in focus
    ]
    known = {_normalized(item["label"]) for item in signals}
    for connection in connections:
        label = str(connection["topic"])
        if _normalized(label) not in known and len(signals) < 5:
            signals.append(
                {
                    "label": label,
                    "reason": f"{connection['semantic_links']} vínculos. {connection['evidence_reason']}",
                }
            )

    if primary:
        headline = f"El principal foco de fricción está en {str(primary['label']).casefold()}"
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
            f"NPS {primary['nps']}, {primary['detractors']} detractores y "
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
        "signals": signals[:5],
    }
