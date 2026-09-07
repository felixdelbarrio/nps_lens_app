from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from nps_lens.analytics.incident_attribution import (
    summarize_attribution_chains,
)
from nps_lens.analytics.incident_rationale import IncidentRationaleSummary
from nps_lens.domain.labels import SCORE_AVERAGE_0_10_LABEL, SCORE_DELTA_LABEL
from nps_lens.services.analytics.kpis_service import (
    format_metric,
    format_percentage,
    format_volume,
)


@dataclass(frozen=True)
class ExecSummary:
    n: int
    nps_avg: float
    detractor_rate: float
    neutral_rate: float
    promoter_rate: float
    top_detractor_driver: str
    top_promoter_driver: str


@dataclass(frozen=True)
class PeriodComparison:
    label_current: str
    label_baseline: str
    nps_current: float
    nps_baseline: float
    delta_nps: float
    detr_current: float
    detr_baseline: float
    delta_detr_pp: float
    n_current: int
    n_baseline: int


def _date_range_label(d0: Optional[pd.Timestamp], d1: Optional[pd.Timestamp]) -> str:
    if d0 is None or d1 is None or pd.isna(d0) or pd.isna(d1):
        return "(sin fechas)"
    return f"{d0.date().isoformat()} → {d1.date().isoformat()}"


def _month_label_es(ts: Optional[pd.Timestamp]) -> str:
    if ts is None or pd.isna(ts):
        return ""
    months = {
        1: "enero",
        2: "febrero",
        3: "marzo",
        4: "abril",
        5: "mayo",
        6: "junio",
        7: "julio",
        8: "agosto",
        9: "septiembre",
        10: "octubre",
        11: "noviembre",
        12: "diciembre",
    }
    return f"{months.get(int(ts.month), 'mes').title()} {int(ts.year)}"


def _period_label(
    d0: Optional[pd.Timestamp],
    d1: Optional[pd.Timestamp],
    *,
    current_anchor: Optional[pd.Timestamp],
    kind: str,
) -> str:
    base = _date_range_label(d0, d1)
    if d0 is None or d1 is None or pd.isna(d0) or pd.isna(d1):
        return base

    if kind == "current":
        month_txt = _month_label_es(d1)
        return f"Mes actual ({month_txt} · {base})" if month_txt else base

    if current_anchor is None or pd.isna(current_anchor):
        return base

    current_month_start = current_anchor.normalize().replace(day=1)
    if d1.normalize() < current_month_start:
        month_txt = _month_label_es(current_anchor)
        return f"Base histórica anterior a {month_txt} ({base})" if month_txt else base
    return base


def compare_periods(df_current: pd.DataFrame, df_baseline: pd.DataFrame) -> PeriodComparison:
    """Business-friendly period comparison for NPS and detractor rate."""
    cur = df_current.dropna(subset=["NPS"]) if "NPS" in df_current.columns else df_current
    base = df_baseline.dropna(subset=["NPS"]) if "NPS" in df_baseline.columns else df_baseline

    n_cur = int(len(cur))
    n_base = int(len(base))
    nps_cur = float(np.nanmean(cur["NPS"])) if n_cur else float("nan")
    nps_base = float(np.nanmean(base["NPS"])) if n_base else float("nan")
    detr_cur = _safe_rate(cur["NPS"] <= 6) if n_cur else 0.0
    detr_base = _safe_rate(base["NPS"] <= 6) if n_base else 0.0

    current_window = df_current.attrs.get("period_window")
    baseline_window = df_baseline.attrs.get("period_window")

    d0c = (
        pd.Timestamp(current_window.start)
        if current_window is not None
        else (pd.to_datetime(cur.get("Fecha"), errors="coerce").min() if n_cur else None)
    )
    d1c = (
        pd.Timestamp(current_window.end)
        if current_window is not None
        else (pd.to_datetime(cur.get("Fecha"), errors="coerce").max() if n_cur else None)
    )
    d0b = (
        pd.Timestamp(baseline_window.start)
        if baseline_window is not None
        else (pd.to_datetime(base.get("Fecha"), errors="coerce").min() if n_base else None)
    )
    d1b = (
        pd.Timestamp(baseline_window.end)
        if baseline_window is not None
        else (pd.to_datetime(base.get("Fecha"), errors="coerce").max() if n_base else None)
    )

    delta_nps = (
        float(nps_cur - nps_base) if pd.notna(nps_cur) and pd.notna(nps_base) else float("nan")
    )
    return PeriodComparison(
        label_current=_period_label(d0c, d1c, current_anchor=d1c, kind="current"),
        label_baseline=_period_label(d0b, d1b, current_anchor=d1c, kind="baseline"),
        nps_current=nps_cur,
        nps_baseline=nps_base,
        delta_nps=delta_nps,
        detr_current=detr_cur,
        detr_baseline=detr_base,
        delta_detr_pp=float((detr_cur - detr_base) * 100.0),
        n_current=n_cur,
        n_baseline=n_base,
    )


def _safe_rate(mask: pd.Series) -> float:
    if mask is None or mask.empty:
        return 0.0
    return float(mask.mean())


def _card_value(card: object, key: str, default: object = "") -> object:
    if isinstance(card, dict):
        return card.get(key, default)
    return getattr(card, key, default)


def _evidence_list(card: object, key: str, limit: int) -> list[str]:
    raw = _card_value(card, key, [])
    if isinstance(raw, list):
        values = raw
    elif raw is None:
        values = []
    else:
        values = [raw]
    return [str(v).strip() for v in values[: int(limit)] if str(v).strip()]


def _evidence_total(card: object, total_key: str, sample_key: str, limit: int) -> int:
    raw_total = _card_value(card, total_key, None)
    try:
        return int(float(raw_total))
    except Exception:
        return len(_evidence_list(card, sample_key, limit))


def executive_summary(df: pd.DataFrame) -> ExecSummary:
    tmp = df.copy()
    tmp = tmp.dropna(subset=["NPS"]) if "NPS" in tmp.columns else tmp
    n = int(len(tmp))
    nps_avg = float(np.nanmean(tmp["NPS"])) if n else float("nan")

    # Detractor / promoter definitions on 0..10 score
    detr = _safe_rate(tmp["NPS"] <= 6) if n else 0.0
    neutral = _safe_rate((tmp["NPS"] >= 7) & (tmp["NPS"] <= 8)) if n else 0.0
    prom = _safe_rate(tmp["NPS"] >= 9) if n else 0.0

    # Simple driver: average NPS by Palanca
    top_det = "(sin datos)"
    top_pro = "(sin datos)"
    if n and "Palanca" in tmp.columns:
        g = tmp.groupby(tmp["Palanca"].astype(str), dropna=False)["NPS"].mean().sort_values()
        if not g.empty:
            top_det = str(g.index[0])
            top_pro = str(g.index[-1])

    return ExecSummary(
        n=n,
        nps_avg=nps_avg,
        detractor_rate=detr,
        neutral_rate=neutral,
        promoter_rate=prom,
        top_detractor_driver=top_det,
        top_promoter_driver=top_pro,
    )


def explain_nps_gaps(gaps_df: pd.DataFrame, max_items: int = 5) -> list[str]:
    """Describe the observed difference from the overall NPS and its sample size."""
    if gaps_df.empty:
        return ["No se observan brechas negativas con el mínimo de respuestas seleccionado."]
    return [
        f"**{row['dimension']}={row['value']}**: NPS {format_metric(row['nps'])}, "
        f"diferencia frente al global **{format_metric(row['gap_vs_overall'], signed=True)} puntos**, "
        f"{format_volume(row['n'])} respuestas ({format_percentage(row['sample_share'])} de la muestra), "
        f"{format_volume(row['detractors'])} detractores."
        for _, row in gaps_df.head(max_items).iterrows()
    ]


def explain_topics(topics_df: pd.DataFrame, max_items: int = 5) -> list[str]:
    if topics_df.empty:
        return ["No hay suficiente texto para extraer temas."]

    out: list[str] = []
    d = topics_df.sort_values("n", ascending=False).head(max_items)
    for _, r in d.iterrows():
        cid = int(r.get("cluster_id", -1))
        n = int(r.get("n", 0))
        terms = list(r.get("top_terms", []))[:5]
        out.append(f"Tema **#{cid}** (n={n}): suele mencionar *{', '.join(terms)}*.")
    return out


def build_executive_story(
    summary: ExecSummary,
    comparison: Optional[PeriodComparison] = None,
    top_nps_gaps: Optional[list[str]] = None,
    top_topics: Optional[list[str]] = None,
) -> str:
    """Generate a copy/paste-ready executive story in Spanish.

    Intentionally non-technical: explains *what is happening* and *what to do next*.
    """
    lines: list[str] = []
    lines.append("# Informe de negocio — NPS Lens")
    lines.append("")

    lines.append("## 1) Qué está pasando")
    nps_val = "—" if summary.n == 0 else format_metric(summary.nps_avg)
    lines.append(
        f"- **Muestras**: {format_volume(summary.n)} · **{SCORE_AVERAGE_0_10_LABEL}**: {nps_val} · "
        f"**Detractores**: {format_percentage(summary.detractor_rate)} · "
        f"**Neutros**: {format_percentage(summary.neutral_rate)} · "
        f"**Promotores**: {format_percentage(summary.promoter_rate)}"
    )
    lines.append(
        (
            f"- **Zona de fricción**: {summary.top_detractor_driver} · "
            f"**Zona fuerte**: {summary.top_promoter_driver}"
        )
    )

    if comparison is not None and comparison.n_current and comparison.n_baseline:
        lines.append("")
        lines.append("## 2) Cambio vs base de comparación")
        lines.append(
            f"- Periodo actual: **{comparison.label_current}** (n={format_volume(comparison.n_current)})"
        )
        lines.append(
            f"- Periodo base: **{comparison.label_baseline}** (n={format_volume(comparison.n_baseline)})"
        )
        d_nps = (
            "—"
            if comparison.delta_nps != comparison.delta_nps
            else format_metric(comparison.delta_nps, signed=True)
        )
        lines.append(
            f"- Variación: **{SCORE_DELTA_LABEL} {d_nps}** · **Δ detractores {format_metric(comparison.delta_detr_pp, signed=True)} pp**"
        )

    if top_nps_gaps:
        lines.append("")
        lines.append("## 3) Brechas NPS observadas")
        for b in top_nps_gaps[:5]:
            lines.append(f"- {b}")

    if top_topics:
        lines.append("")
        lines.append("## 4) Qué están diciendo (temas de texto)")
        for b in top_topics[:6]:
            lines.append(f"- {b}")

    lines.append("")
    return "\n".join(lines) + "\n"


def _fmt_lag(lag_weeks: float) -> str:
    if lag_weeks != lag_weeks:
        return "n/d"
    return f"{format_metric(lag_weeks)}w"


def _fmt_pct(value: float) -> str:
    if value != value:
        return "n/d"
    return format_percentage(value)


def _fmt_delta(value: float) -> str:
    if value != value:
        return "n/d"
    return format_metric(value, signed=True)


def _observed_evidence_lines(
    summary: IncidentRationaleSummary,
    rationale_df: pd.DataFrame,
    attribution_df: Optional[pd.DataFrame],
    attribution_summary: Optional[dict[str, int]],
    focus_name: str,
    top_k: int,
) -> list[str]:
    scope = attribution_summary or summarize_attribution_chains(attribution_df)
    lines = [
        f"Se analizaron **{summary.topics_analyzed} tópicos**, **{format_volume(summary.responses)} respuestas** y **{format_volume(summary.incidents)} incidencias**.",
        f"Los tres tópicos con más incidencias concentran **{format_percentage(summary.top3_incident_share)}** del total relacionado.",
    ]
    if np.isfinite(summary.median_lag_weeks):
        lines.append(f"El mejor lag observado tiene una mediana de **{format_metric(summary.median_lag_weeks)} semanas**.")
    if int(scope.get("linked_pairs_total", 0)):
        lines.append(
            f"Se encontraron **{format_volume(scope['linked_pairs_total'])} vínculos semánticos** entre "
            f"**{format_volume(scope['linked_incidents_total'])} incidencias** y "
            f"**{format_volume(scope['linked_comments_total'])} comentarios**."
        )
    if rationale_df is not None and not rationale_df.empty:
        for _, row in rationale_df.head(top_k).iterrows():
            lines.append(
                f"**{row.get('nps_topic', '')}**: {format_volume(row.get('responses', 0))} respuestas, "
                f"{format_volume(row.get('incidents', 0))} incidencias, "
                f"{format_metric(row.get('incident_rate_per_100_responses'))} por 100 respuestas; "
                f"la tasa de {focus_name} difiere "
                f"{format_metric(row.get('focus_rate_difference_pp'), signed=True)} pp entre periodos de incidencia alta y baja, "
                f"y la nota media difiere {format_metric(row.get('score_mean_difference'), signed=True)} puntos."
            )
    return lines


def build_incident_ppt_story(
    summary: IncidentRationaleSummary,
    rationale_df: pd.DataFrame,
    *,
    attribution_df: Optional[pd.DataFrame] = None,
    attribution_summary: Optional[dict[str, int]] = None,
    focus_name: str = "detractores",
    top_k: int = 5,
) -> str:
    """Narrate observed volumes, comparisons and semantic links without causal claims."""
    lines = ["# Evidencia observada — Incidencias y NPS", "", "## Observación"]
    lines.extend(
        f"- {line}"
        for line in _observed_evidence_lines(
            summary, rationale_df, attribution_df, attribution_summary, focus_name, top_k
        )
    )
    lines.extend(["", "## Evidencias vinculadas"])
    cards = attribution_df.head(top_k).to_dict(orient="records") if attribution_df is not None and not attribution_df.empty else []
    if not cards:
        lines.append("- No hay vínculos semánticos con el umbral y la ventana seleccionados.")
    for card in cards:
        title = str(_card_value(card, "nps_topic", ""))
        lines.append(
            f"- **{title}**: {format_volume(_card_value(card, 'linked_pairs', 0))} vínculos, "
            f"similitud media {format_metric(_card_value(card, 'avg_similarity', np.nan))}, "
            f"nota media {format_metric(_card_value(card, 'avg_nps', np.nan))}."
        )
        for incident in _evidence_list(card, "incident_examples", 2):
            lines.append(f"  Helix: {incident}")
        for comment in _evidence_list(card, "comment_examples", 2):
            lines.append(f"  VoC: {comment}")
    lines.append("- Las asociaciones temporales y semánticas no demuestran causalidad.")
    return "\n".join(lines) + "\n"


def build_ppt_8slide_script(
    summary: IncidentRationaleSummary,
    rationale_df: pd.DataFrame,
    *,
    attribution_df: Optional[pd.DataFrame] = None,
    attribution_summary: Optional[dict[str, int]] = None,
    touchpoint_source: str = "",
    service_origin: str,
    service_origin_n1: str,
    focus_name: str,
    period_label: str,
    top_k: int = 5,
) -> str:
    """Create an eight-section evidence script; selection uses direct counts."""
    del touchpoint_source
    observed = _observed_evidence_lines(
        summary, rationale_df, attribution_df, attribution_summary, focus_name, top_k
    )
    sections = [
        ("Alcance", [f"{service_origin} · {service_origin_n1} · {period_label}", *observed[:2]]),
        ("Evolución", [f"Serie de incidencias y tasa de {focus_name} por semana."]),
        ("Vínculos semánticos", observed[2:3] or ["Sin vínculos en la ventana seleccionada."]),
        ("Comparación de periodos", observed[3:] or ["Sin grupos alto/bajo comparables."]),
        ("Asociación temporal", ["Correlación, mejor lag y changepoints observados por tópico."]),
        ("Evidencia Helix", ["Incidencias relacionadas, fechas y organización responsable observada."]),
        ("Evidencia VoC", ["Comentarios vinculados con nota y fecha de respuesta."]),
        ("Límites", ["La similitud, la correlación y el lag describen asociación; no prueban causalidad."]),
    ]
    lines = ["# Guion de evidencia — NPS e incidencias"]
    for number, (title, bullets) in enumerate(sections, start=1):
        lines.extend(["", f"## Slide {number} — {title}", *[f"- {bullet}" for bullet in bullets]])
    return "\n".join(lines) + "\n"

