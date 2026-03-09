from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from nps_lens.analytics.incident_attribution import (
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
)
from nps_lens.analytics.incident_rationale import IncidentRationaleSummary


@dataclass(frozen=True)
class ExecSummary:
    n: int
    nps_avg: float
    detractor_rate: float
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
        promoter_rate=prom,
        top_detractor_driver=top_det,
        top_promoter_driver=top_pro,
    )


def explain_opportunities(opps_df: pd.DataFrame, max_items: int = 5) -> list[str]:
    """Human-friendly bullets for opportunities table."""
    if opps_df.empty:
        return ["No se detectaron oportunidades con el umbral actual."]

    out: list[str] = []
    for _, r in opps_df.head(max_items).iterrows():
        dim = str(r.get("dimension", ""))
        val = str(r.get("value", ""))
        uplift = float(r.get("potential_uplift", 0.0))
        conf = float(r.get("confidence", 0.0))
        n = int(r.get("n", 0))
        out.append(
            (
                (
                    f"Si mejoramos **{dim}={val}**, el modelo estima un "
                    f"**potencial de +{uplift:.1f} puntos** "
                    f"(confianza ~{conf:.2f}, n={n})."
                )
            )
        )
    return out


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
    top_opportunities: Optional[list[str]] = None,
    top_topics: Optional[list[str]] = None,
) -> str:
    """Generate a copy/paste-ready executive story in Spanish.

    Intentionally non-technical: explains *what is happening* and *what to do next*.
    """
    lines: list[str] = []
    lines.append("# Informe de negocio — NPS Lens")
    lines.append("")

    lines.append("## 1) Qué está pasando")
    nps_val = "—" if summary.n == 0 else f"{summary.nps_avg:.2f}"
    lines.append(
        f"- **Muestras**: {summary.n:,} · **NPS medio (0-10)**: {nps_val} · "
        f"**Detractores**: {summary.detractor_rate*100:.1f}% · "
        f"**Promotores**: {summary.promoter_rate*100:.1f}%"
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
            f"- Periodo actual: **{comparison.label_current}** (n={comparison.n_current:,})"
        )
        lines.append(
            f"- Periodo base: **{comparison.label_baseline}** (n={comparison.n_baseline:,})"
        )
        d_nps = (
            "—" if comparison.delta_nps != comparison.delta_nps else f"{comparison.delta_nps:+.2f}"
        )
        lines.append(
            f"- Variación: **Δ NPS {d_nps}** · **Δ detractores {comparison.delta_detr_pp:+.1f} pp**"
        )

    if top_opportunities:
        lines.append("")
        lines.append("## 3) Dónde atacar primero (oportunidades)")
        for b in top_opportunities[:5]:
            lines.append(f"- {b}")

    if top_topics:
        lines.append("")
        lines.append("## 4) Qué están diciendo (temas de texto)")
        for b in top_topics[:6]:
            lines.append(f"- {b}")

    lines.append("")
    lines.append("## 5) Próximos pasos recomendados")
    lines.append("- Validar si hay releases / incidencias / campañas en la ventana del cambio.")
    lines.append("- Abrir 1-2 hipótesis por oportunidad priorizada y definir cómo se medirán.")
    lines.append("- Generar un **Deep-Dive Pack** y guardar aprendizaje en la **Knowledge Cache**.")

    return "\n".join(lines) + "\n"


def _fmt_lag(lag_weeks: float) -> str:
    if lag_weeks != lag_weeks:
        return "n/d"
    return f"{lag_weeks:.1f}w"


def _fmt_pct(value: float) -> str:
    if value != value:
        return "n/d"
    return f"{value*100:.0f}%"


def _fmt_delta(value: float) -> str:
    if value != value:
        return "n/d"
    return f"{value:+.1f}"


def build_incident_ppt_story(
    summary: IncidentRationaleSummary,
    rationale_df: pd.DataFrame,
    *,
    attribution_df: Optional[pd.DataFrame] = None,
    focus_name: str = "detractores",
    top_k: int = 5,
) -> str:
    """Narrative ready for PowerPoint committee sessions."""
    cards = (
        attribution_df.head(int(top_k)).to_dict(orient="records")
        if attribution_df is not None and not attribution_df.empty
        else []
    )
    lines: list[str] = []
    lines.append("# Racional de negocio — Incidencias vs NPS térmico")
    lines.append("")
    lines.append("## 1) Observación")
    lines.append(
        f"- Se analizaron **{summary.topics_analyzed} tópicos** con evidencia multi-fuente (NPS + Helix)."
    )
    lines.append(
        f"- El modelo estima **{summary.total_nps_impact:.2f} pts de impacto total en NPS** asociados a fricción operativa."
    )
    lines.append(
        f"- Potencial de recuperación estimado: **{summary.nps_points_recoverable:.2f} pts NPS**."
    )
    lines.append(
        f"- La concentración de incidencias en top-3 tópicos alcanza **{summary.top3_incident_share*100:.1f}%**."
    )
    if summary.median_lag_weeks == summary.median_lag_weeks:
        lines.append(
            f"- Tiempo de reacción estimado (mediana de lag): **{summary.median_lag_weeks:.1f} semanas**."
        )
    lines.append(
        f"- En el pico de afectación, la probabilidad del foco analizado sube a **{summary.peak_focus_probability*100:.0f}%**."
    )
    lines.append(
        f"- El delta NPS esperado en los journeys afectados es de **{summary.expected_nps_delta:+.1f} puntos**."
    )

    lines.append("")
    lines.append("## 2) Cadena de impacto")
    if not cards:
        lines.append(
            "- No hay señal suficiente para construir una cadena causal robusta con el umbral actual."
        )
    else:
        for card in cards:
            title = str(_card_value(card, "nps_topic", _card_value(card, "title", "")))
            touchpoint = str(_card_value(card, "touchpoint", ""))
            incident_examples = _evidence_list(card, "incident_examples", 5)
            comment_examples = _evidence_list(card, "comment_examples", 2)
            incident_total = _evidence_total(card, "linked_incidents", "incident_examples", 5)
            comment_total = _evidence_total(card, "linked_comments", "comment_examples", 2)
            probability = float(
                _card_value(
                    card, "detractor_probability", _card_value(card, "focus_probability", np.nan)
                )
            )
            delta_nps = float(_card_value(card, "nps_delta_expected", np.nan))
            impact = float(_card_value(card, "total_nps_impact", 0.0))
            statement = str(_card_value(card, "chain_story", _card_value(card, "statement", "")))
            lines.append(
                f"- **{title}**: ({len(incident_examples)}) incidencias Helix mostradas sobre **{touchpoint}** -> ({len(comment_examples)}) comentarios VoC -> riesgo de {focus_name}."
            )
            lines.append(
                "  Impacto esperado: "
                f"probabilidad {focus_name} **{_fmt_pct(probability)}** · "
                f"Δ NPS **{_fmt_delta(delta_nps)}** · "
                f"impacto total **{impact:.2f} pts** · "
                f"evidencia validada **{incident_total} incidencias / {comment_total} comentarios**."
            )
            for incident in incident_examples:
                lines.append(f"  Helix: {incident}")
            for comment in comment_examples:
                lines.append(f"  VoC: {comment}")
            lines.append(f"  Conclusión: {statement}")

    lines.append("")
    lines.append("## 3) Evidencia estadística")
    if cards:
        for card in cards:
            title = (
                str(card.get("nps_topic", card.get("title", "")))
                if isinstance(card, dict)
                else str(card.title)
            )
            causal_score = (
                float(card.get("causal_score", 0.0))
                if isinstance(card, dict)
                else float(card.causal_score)
            )
            confidence = (
                float(card.get("confidence", 0.0))
                if isinstance(card, dict)
                else float(card.confidence)
            )
            concentration = (
                float(card.get("linked_pairs", 0.0))
                if isinstance(card, dict)
                else float(card.concentration_share)
            )
            lines.append(
                (
                    f"- {title}: causal score **{causal_score:.2f}**, confianza **{confidence:.2f}**, "
                    f"evidencia validada **{concentration:.0f} links**."
                )
            )
    lines.append(
        "- La lectura correcta no es incidencia ↔ comentario, sino incidencia -> touchpoint -> experiencia negativa -> comentario -> NPS."
    )

    lines.append("")
    lines.append("## 4) Plan operativo 30-60-90")
    lines.append(
        "- 30 días: activar quick wins en touchpoints críticos y cerrar brechas de instrumentación."
    )
    lines.append("- 60 días: desplegar fixes estructurales en tópicos P1 con mayor NPS en riesgo.")
    lines.append(
        "- 90 días: consolidar aprendizaje (confirmado/rechazado), medir recuperación y recalibrar prioridades."
    )

    lines.append("")
    lines.append("## 5) KPI de seguimiento semanal")
    lines.append(f"- % {focus_name}")
    lines.append("- Incidencias por tópico priorizado")
    lines.append("- Delta NPS esperado e impacto total atribuido")
    lines.append("- Cumplimiento de ETA por owner/lane")
    return "\n".join(lines) + "\n"


def build_ppt_8slide_script(
    summary: IncidentRationaleSummary,
    rationale_df: pd.DataFrame,
    *,
    attribution_df: Optional[pd.DataFrame] = None,
    touchpoint_source: str = "",
    service_origin: str,
    service_origin_n1: str,
    focus_name: str,
    period_label: str,
    top_k: int = 5,
) -> str:
    """Generate a business-first 8-slide script for periodic committee sessions."""
    top = rationale_df.head(int(top_k)).copy() if rationale_df is not None else pd.DataFrame()
    cards = (
        attribution_df.head(3).to_dict(orient="records")
        if attribution_df is not None and not attribution_df.empty
        else []
    )
    topics = top.get("nps_topic", pd.Series(dtype=str)).astype(str).tolist()
    top_topics = ", ".join(topics[:3]) if topics else "Sin tópicos priorizados"

    lines: list[str] = []
    lines.append("# Guion de negocio — 8 slides (NPS térmico vs incidencias)")
    lines.append("")
    lines.append("## Slide 1 — Mensaje principal")
    lines.append(
        f"- Contexto: **{service_origin} · {service_origin_n1}** | Periodo: **{period_label}**."
    )
    lines.append(
        f"- Se estiman **{summary.total_nps_impact:.2f} pts de impacto total en NPS** asociados a incidencias."
    )
    lines.append(
        f"- Potencial recuperable estimado: **{summary.nps_points_recoverable:.2f} pts NPS**."
    )
    lines.append(f"- Concentración top-3 incidencias: **{summary.top3_incident_share*100:.1f}%**.")
    lines.append("- Decisión sugerida: activar plan semanal en tópicos P1.")
    lines.append("")

    lines.append("## Slide 2 — Qué está pasando en la señal")
    lines.append(
        f"- Evolución semanal de **% {focus_name} vs incidencias** (usar gráfico de timeline causal)."
    )
    lines.append("- Señalar semanas con ruptura y eventos operativos/release.")
    lines.append("- Mensaje clave: cuándo la incidencia precede el deterioro NPS.")
    lines.append("")

    mode = str(touchpoint_source or "").strip()
    if mode == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS:
        lines.append("## Slide 3 — Journeys que explican la detracción")
        lines.append(
            "- Objetivo: identificar rutas de degradación de experiencia que conectan incidencias con la voz del cliente para priorizar causas raíz accionables."
        )
    elif mode == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
        lines.append("## Slide 3 — Journeys rotos detectados")
        lines.append(
            "- Narrativa obligatoria: incidencia -> embeddings / keywords / clustering semántico -> touchpoint roto -> comentario -> NPS."
        )
    else:
        lines.append("## Slide 3 — Impact Chain")
        lines.append(
            "- Narrativa obligatoria: incidencia -> touchpoint -> experiencia negativa -> comentario -> NPS."
        )
    if not cards:
        lines.append("- No hay evidencia suficiente para construir la cadena con rigor.")
    else:
        for card in cards:
            title = str(_card_value(card, "nps_topic", _card_value(card, "title", "")))
            touchpoint = str(_card_value(card, "touchpoint", ""))
            probability = float(
                _card_value(
                    card, "detractor_probability", _card_value(card, "focus_probability", np.nan)
                )
            )
            delta_nps = float(_card_value(card, "nps_delta_expected", np.nan))
            impact = float(_card_value(card, "total_nps_impact", 0.0))
            incident_examples = _evidence_list(card, "incident_examples", 5)
            comment_examples = _evidence_list(card, "comment_examples", 2)
            incident_total = _evidence_total(card, "linked_incidents", "incident_examples", 5)
            comment_total = _evidence_total(card, "linked_comments", "comment_examples", 2)
            if mode == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS:
                expected_evidence = str(_card_value(card, "journey_expected_evidence", "")).strip()
                impact_label = str(_card_value(card, "journey_impact_label", "")).strip()
                lines.append(
                    f"- {title}: {expected_evidence or 'journey causal defendible'} | "
                    f"impacto esperado {impact_label or 'alto'} | "
                    f"probabilidad {focus_name} {_fmt_pct(probability)} | "
                    f"Δ NPS {_fmt_delta(delta_nps)} | "
                    f"impacto {impact:.2f} pts | "
                    f"evidencia validada {incident_total}/{comment_total}."
                )
            elif mode == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
                expected_evidence = str(_card_value(card, "journey_expected_evidence", "")).strip()
                lines.append(
                    f"- {title}: {expected_evidence or 'cluster semántico defendible'} | "
                    f"touchpoint {touchpoint or 'detectado automáticamente'} | "
                    f"probabilidad {focus_name} {_fmt_pct(probability)} | "
                    f"Δ NPS {_fmt_delta(delta_nps)} | "
                    f"impacto {impact:.2f} pts | "
                    f"evidencia validada {incident_total}/{comment_total}."
                )
            else:
                lines.append(
                    f"- {title}: ({len(incident_examples)}) incidencias mostradas sobre {touchpoint} | "
                    f"({len(comment_examples)}) VoC | "
                    f"probabilidad {focus_name} {_fmt_pct(probability)} | "
                    f"Δ NPS {_fmt_delta(delta_nps)} | "
                    f"impacto {impact:.2f} pts | "
                    f"evidencia validada {incident_total}/{comment_total}."
                )
            for incident in incident_examples:
                lines.append(f"- Helix: {incident}")
            for comment in comment_examples:
                lines.append(f"- VoC: {comment}")
    lines.append("")

    lines.append("## Slide 4 — Dónde duele (causas priorizadas)")
    lines.append(f"- Tópicos con mayor criticidad: **{top_topics}**.")
    if top.empty:
        lines.append("- No hay evidencia suficiente para priorización robusta.")
    else:
        for _, r in top.iterrows():
            lines.append(
                (
                    f"- {str(r.get('nps_topic',''))}: prioridad={float(r.get('priority',0.0)):.2f}, "
                    f"confianza={float(r.get('confidence',0.0)):.2f}, "
                    f"causal score={float(r.get('causal_score',0.0)):.2f}."
                )
            )
    lines.append("")

    lines.append("## Slide 5 — Cuánto impacta al NPS")
    lines.append("- Mostrar barra comparativa **NPS en riesgo vs NPS recuperable** por tópico.")
    if not top.empty:
        risk_top = float(pd.to_numeric(top["nps_points_at_risk"], errors="coerce").fillna(0).sum())
        rec_top = float(
            pd.to_numeric(top["nps_points_recoverable"], errors="coerce").fillna(0).sum()
        )
        lines.append(
            f"- Top temas analizados: riesgo={risk_top:.2f} pts | recuperable={rec_top:.2f} pts."
        )
    lines.append(
        f"- Delta NPS esperado agregado: **{summary.expected_nps_delta:+.1f} pts** | impacto total atribuido **{summary.total_nps_impact:.2f} pts**."
    )
    lines.append("- Mensaje clave: impacto económico esperado de corregir tópicos P1.")
    lines.append("")

    lines.append("## Slide 6 — Qué atacamos primero")
    lines.append("- Usar matriz de prioridad (confianza x NPS en riesgo x volumen incidencias).")
    if top.empty:
        lines.append("- Definir backlog inicial de hipótesis con instrumentación mínima.")
    else:
        for _, r in top.head(3).iterrows():
            lines.append(
                (
                    f"- P1 {str(r.get('nps_topic',''))}: lane={str(r.get('action_lane',''))}, "
                    f"owner={str(r.get('owner_role',''))}, ETA={int(r.get('eta_weeks',0) or 0)} semanas."
                )
            )
    lines.append("")

    lines.append("## Slide 7 — Plan 30-60-90")
    lines.append("- 30 días: quick wins operativos + corrección de fricción evidente.")
    lines.append("- 60 días: fixes estructurales y reducción de recurrencia de incidencias.")
    lines.append("- 90 días: escalado de prácticas efectivas + recalibración de prioridades.")
    lines.append("")

    lines.append("## Slide 8 — Gobierno y métricas")
    lines.append(f"- KPI leading: incidencias por tópico P1, SLA de resolución, % {focus_name}.")
    lines.append(
        "- KPI lagging: NPS térmico, NPS en riesgo (pts), NPS recuperable realizado (pts)."
    )
    lines.append("- Cadencia: comité semanal con owners de producto, tecnología y operaciones.")
    lines.append("")
    return "\n".join(lines) + "\n"
