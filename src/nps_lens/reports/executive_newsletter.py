from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from nps_lens.analytics.channel_topic_scope import restrict_to_topics, topics_observed_in_channel
from nps_lens.analytics.drivers import grouped_driver_stats
from nps_lens.services.analytics.kpis_service import format_metric, format_percentage, format_volume

_WATCH_LABELS = (
    "Funcionamiento continuo",
    "Pagos/transferencias",
    "Agregar funcionalidad",
    "Uso",
)
_CONNECTION_TERMS = (
    "problemas con transferencias",
    "dispersión de nómina",
    "pagos masivos",
    "no funciona bien",
    "estado de cuenta",
    "login",
    "interface",
    "actualización",
)


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


def _first_matching_label(labels: list[str], target: str) -> str:
    key = _normalized(target)
    return next((label for label in labels if _normalized(label) == key), "")


def _period_label(period_start: date, period_end: date) -> str:
    months = (
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
    )
    if period_start.year == period_end.year and period_start.month == period_end.month:
        return f"{period_start.day}–{period_end.day} {months[period_end.month - 1]} {period_end.year}"
    return f"{period_start:%d/%m/%Y}–{period_end:%d/%m/%Y}"


def _focus_rows(current_df: pd.DataFrame, *, topic_channel: str, period_kpis: dict[str, object]) -> list[dict[str, object]]:
    if current_df is None or current_df.empty or "Palanca" not in current_df.columns:
        return []
    topic_keys = topics_observed_in_channel(current_df, "Palanca", topic_channel)
    source = restrict_to_topics(current_df, "Palanca", topic_keys)
    grouped = grouped_driver_stats(source, "Palanca")
    if grouped.empty:
        return []
    grouped["score"] = pd.to_numeric(source["NPS"], errors="coerce").groupby(source["Palanca"]).mean().reindex(grouped["Palanca"]).to_numpy()
    base_nps = _number(_dict(_dict(period_kpis).get("historical")).get("kpis", {}).get("classic_nps"))
    grouped["delta"] = grouped["nps"] - base_nps if base_nps is not None else np.nan
    grouped["detractor_volume"] = grouped["det_count"].fillna(0)
    labels = grouped["Palanca"].astype(str).tolist()
    preferred = [_first_matching_label(labels, label) for label in _WATCH_LABELS]
    preferred = [label for label in preferred if label]
    ranked = grouped.sort_values(["detractor_volume", "detractor_rate", "n"], ascending=[False, False, False])["Palanca"].astype(str).tolist()
    order = list(dict.fromkeys(preferred + ranked))[:4]
    lookup = grouped.set_index("Palanca", drop=False)
    rows: list[dict[str, object]] = []
    for label in order:
        row = lookup.loc[label]
        delta = _number(row.get("delta"))
        rows.append({
            "label": label,
            "nps": format_metric(row.get("nps")),
            "delta": f"{format_metric(delta, signed=True)} pts" if delta is not None else "sin base",
            "detractors": format_percentage(row.get("detractor_rate")),
            "score": format_metric(row.get("score")),
            "opinions": format_volume(row.get("n")),
            "nps_value": _number(row.get("nps")),
            "delta_value": delta,
            "detractor_rate": _number(row.get("detractor_rate")),
            "detractor_volume": int(row.get("det_count") or 0),
        })
    return rows


def _scenario_priority(card: dict[str, object]) -> tuple[int, float]:
    title = _normalized(card.get("title"))
    term_rank = next((index for index, term in enumerate(_CONNECTION_TERMS) if term in title), len(_CONNECTION_TERMS))
    return term_rank, -float(_number(card.get("linked_pairs")) or 0)


def _connections(linking: dict[str, object]) -> list[dict[str, object]]:
    scenario_block = _dict(linking.get("scenarios"))
    cards = [card for card in _list(scenario_block.get("cards")) if isinstance(card, dict)]
    selected = sorted(cards, key=_scenario_priority)[:4]
    connections: list[dict[str, object]] = []
    for card in selected:
        comments = [record for record in _list(card.get("comment_records")) if isinstance(record, dict)]
        incidents = [record for record in _list(card.get("incident_records")) if isinstance(record, dict)]
        connections.append({
            "topic": str(card.get("title") or card.get("nps_topic") or "Tópico observado"),
            "average_score": format_metric(card.get("avg_nps")),
            "semantic_links": int(_number(card.get("linked_pairs")) or 0),
            "comments": [str(item.get("comment") or "").strip() for item in comments if str(item.get("comment") or "").strip()][:2],
            "incidents": [{"id": str(item.get("incident_id") or "").strip(), "summary": str(item.get("summary") or "").strip()} for item in incidents if str(item.get("incident_id") or "").strip()][:3],
            "caveat": "Relación semántica observada; no implica causalidad.",
        })
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
    period = _dict(period_kpis.get("period"))
    display = _dict(period.get("display"))
    base_display = _dict(period.get("base_display"))
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
        scorecard.append({
            "label": label,
            "value": str(display.get(key, "n/d")),
            "base": str(base_display.get(key, "")),
            "delta": str(delta.get("display") or ""),
        })

    focus = _focus_rows(current_df, topic_channel=topic_channel, period_kpis=period_kpis)
    primary = focus[0] if focus else {"label": "la experiencia digital", "nps": "n/d", "detractors": "n/d", "opinions": "0", "delta": "sin base"}
    functioning = next((row for row in focus if _normalized(row.get("label")) == "funcionamiento continuo"), None)
    if functioning is not None:
        primary = functioning
    connections = _connections(linking)

    insights = [{
        "title": f"{primary['label']} concentra la principal señal de fricción",
        "evidence": f"NPS {primary['nps']} · {primary['detractors']} detractores · {primary['opinions']} opiniones",
        "meaning": "Es el foco que combina peor experiencia observada y volumen detractor dentro de las palancas visibles en Web.",
    }]
    candidates = [row for row in focus if row is not primary]
    if candidates:
        best = max(candidates, key=lambda row: float(row.get("nps_value") if row.get("nps_value") is not None else -1e9))
        insights.append({"title": f"{best['label']} ofrece la mejor señal relativa", "evidence": f"NPS {best['nps']} · score {best['score']}", "meaning": "Sirve como contraste frente al principal foco de dolor del periodo."})
        deteriorations = [row for row in focus if row.get("delta_value") is not None]
        if deteriorations:
            worst = min(deteriorations, key=lambda row: float(row["delta_value"]))
            insights.append({
                "title": f"{worst['label']} registra el mayor deterioro visible",
                "evidence": f"{worst['delta']} frente a {period.get('base_label') or 'la base anterior'}",
                "meaning": "La comparación corresponde al NPS mensual del periodo, no al NPS acumulado histórico.",
            })
    if connections:
        top = max(connections, key=lambda item: int(item["semantic_links"]))
        insights.append({
            "title": "La señal del cliente tiene reflejo operativo",
            "evidence": f"{top['semantic_links']} vínculos semánticos en {top['topic']}",
            "meaning": "Los comentarios y las incidencias comparten señales semánticas; la relación no demuestra causalidad.",
        })

    quotes: list[str] = []
    for connection in connections:
        for quote in connection["comments"]:
            if quote not in quotes:
                quotes.append(quote)
            if len(quotes) == 4:
                break
        if len(quotes) == 4:
            break
    signals = [{"label": row["label"], "reason": f"NPS {row['nps']}; {row['detractors']} detractores; {row['opinions']} opiniones."} for row in focus]
    known = {_normalized(item["label"]) for item in signals}
    for connection in connections:
        label = str(connection["topic"])
        if _normalized(label) not in known and len(signals) < 5:
            signals.append({"label": label, "reason": f"{connection['semantic_links']} vínculos semánticos con incidencias relacionadas."})

    return {
        "brand": "BBVA BANCA DE EMPRESAS E INSTITUCIONES",
        "product": "NPS Lens",
        "promise": "La voz del cliente conectada con la operación",
        "period": _period_label(period_start, period_end),
        "headline": f"El principal foco de fricción está en {str(primary['label']).casefold()}",
        "lead": (
            f"La lectura del periodo sitúa {primary['label']} en el centro de la señal: "
            f"NPS {primary['nps']}, {primary['detractors']} detractores y "
            f"{primary['opinions']} opiniones. Las relaciones con Helix se presentan como "
            "evidencia semántica, no como causalidad demostrada."
        ),
        "scorecard": scorecard,
        "insights": insights[:4],
        "focus": {"title": str(primary["label"]), "rows": focus},
        "connections": connections,
        "quotes": quotes,
        "signals": signals[:5],
    }
