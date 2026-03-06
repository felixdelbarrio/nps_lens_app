from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

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

    d0c = pd.to_datetime(cur.get("Fecha"), errors="coerce").min() if n_cur else None
    d1c = pd.to_datetime(cur.get("Fecha"), errors="coerce").max() if n_cur else None
    d0b = pd.to_datetime(base.get("Fecha"), errors="coerce").min() if n_base else None
    d1b = pd.to_datetime(base.get("Fecha"), errors="coerce").max() if n_base else None

    delta_nps = (
        float(nps_cur - nps_base) if pd.notna(nps_cur) and pd.notna(nps_base) else float("nan")
    )
    return PeriodComparison(
        label_current=_date_range_label(d0c, d1c),
        label_baseline=_date_range_label(d0b, d1b),
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
        lines.append("## 2) Cambio vs periodo anterior")
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


def build_incident_ppt_story(
    summary: IncidentRationaleSummary,
    rationale_df: pd.DataFrame,
    *,
    focus_name: str = "detractores",
    top_k: int = 5,
) -> str:
    """Narrative ready for PowerPoint committee sessions."""
    lines: list[str] = []
    lines.append("# Racional de negocio — Incidencias vs NPS térmico")
    lines.append("")
    lines.append("## 1) Qué está pasando")
    lines.append(
        f"- Se analizaron **{summary.topics_analyzed} tópicos** con evidencia multi-fuente (NPS + Helix)."
    )
    lines.append(
        f"- El modelo estima **{summary.nps_points_at_risk:.2f} pts NPS en riesgo** por incidencias."
    )
    lines.append(
        f"- Potencial de recuperación estimado: **{summary.nps_points_recoverable:.2f} pts NPS**."
    )
    lines.append(
        f"- Concentración de incidencias en top-3 tópicos: **{summary.top3_incident_share*100:.1f}%**."
    )
    if summary.median_lag_weeks == summary.median_lag_weeks:
        lines.append(
            f"- Tiempo de reacción estimado (mediana de lag): **{summary.median_lag_weeks:.1f} semanas**."
        )

    lines.append("")
    lines.append("## 2) Dónde atacar primero")
    top = rationale_df.head(int(top_k)) if rationale_df is not None else pd.DataFrame()
    if top.empty:
        lines.append("- No hay señal suficiente para priorizar con el umbral actual.")
    else:
        for _, r in top.iterrows():
            topic = str(r.get("nps_topic", ""))
            risk = float(r.get("nps_points_at_risk", 0.0))
            rec = float(r.get("nps_points_recoverable", 0.0))
            prio = float(r.get("priority", 0.0))
            lane = str(r.get("action_lane", ""))
            owner = str(r.get("owner_role", ""))
            eta = int(r.get("eta_weeks", 0))
            lag = _fmt_lag(float(r.get("best_lag_weeks", np.nan)))
            lines.append(
                (
                    f"- **{topic}** | riesgo={risk:.2f} pts | recuperable={rec:.2f} pts | "
                    f"prioridad={prio:.2f} | lane={lane} | owner={owner} | ETA={eta}w | lag={lag}"
                )
            )

    lines.append("")
    lines.append("## 3) Plan operativo 30-60-90")
    lines.append("- 30 días: activar quick wins y cerrar brechas de instrumentación.")
    lines.append("- 60 días: desplegar fixes estructurales en tópicos P1 con mayor NPS en riesgo.")
    lines.append("- 90 días: consolidar aprendizaje (confirmado/rechazado) y recalibrar prioridades.")

    lines.append("")
    lines.append("## 4) KPI de seguimiento semanal")
    lines.append(f"- % {focus_name}")
    lines.append("- Incidencias por tópico priorizado")
    lines.append("- NPS en riesgo (pts) y NPS recuperable (pts)")
    lines.append("- Cumplimiento de ETA por owner/lane")
    return "\n".join(lines) + "\n"


def build_wow_prompt(
    *,
    objective: str,
    business_story_md: str,
    top_topics_df: pd.DataFrame,
    deep_dive_pack_json: str,
) -> str:
    """Prompt template for copy/paste workflows with ChatGPT (no API required)."""
    topic_lines: list[str] = []
    for _, r in top_topics_df.head(8).iterrows():
        topic_lines.append(
            (
                f"- {str(r.get('nps_topic',''))} | risk={float(r.get('nps_points_at_risk',0.0)):.2f} "
                f"| recoverable={float(r.get('nps_points_recoverable',0.0)):.2f} "
                f"| priority={float(r.get('priority',0.0)):.2f} "
                f"| lane={str(r.get('action_lane',''))}"
            )
        )
    topics_block = "\n".join(topic_lines) if topic_lines else "- Sin topicos priorizados."

    return (
        "Actua como Principal Consultant de banca empresas para comite de negocio.\n"
        "Objetivo de negocio:\n"
        f"{objective}\n\n"
        "Entregable obligatorio (en ESPANOL y en formato Markdown):\n"
        "1) Mensaje principal (max 8 lineas).\n"
        "2) Mapa de causa-efecto incidencia -> NPS (tabla con confidence y riesgos).\n"
        "3) Plan semanal de ejecucion (owner, ETA, KPI leading/lagging, criterio de exito).\n"
        "4) 3 experimentos de mejora continua (diseno, muestra, metrica, regla go/no-go).\n"
        "5) Guion de 6 slides para PowerPoint (titulo + bullets por slide).\n\n"
        "Reglas:\n"
        "- No inventes datos. Usa solo la evidencia entregada.\n"
        "- Si falta evidencia, dilo explicitamente y propone como medirla.\n"
        "- Prioriza impacto economico y velocidad de recuperacion del NPS.\n\n"
        "Narrativa base de negocio:\n"
        f"{business_story_md}\n\n"
        "Topicos priorizados:\n"
        f"{topics_block}\n\n"
        "Deep-Dive Pack JSON:\n"
        f"{deep_dive_pack_json}\n"
    )


def build_ppt_8slide_script(
    summary: IncidentRationaleSummary,
    rationale_df: pd.DataFrame,
    *,
    service_origin: str,
    service_origin_n1: str,
    focus_name: str,
    period_label: str,
    top_k: int = 5,
) -> str:
    """Generate a business-first 8-slide script for periodic committee sessions."""
    top = rationale_df.head(int(top_k)).copy() if rationale_df is not None else pd.DataFrame()
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
        f"- Se estiman **{summary.nps_points_at_risk:.2f} pts NPS en riesgo** asociados a incidencias."
    )
    lines.append(
        f"- Potencial recuperable estimado: **{summary.nps_points_recoverable:.2f} pts NPS**."
    )
    lines.append(
        f"- Concentración top-3 incidencias: **{summary.top3_incident_share*100:.1f}%**."
    )
    lines.append("- Decisión sugerida: activar plan semanal en tópicos P1.")
    lines.append("")

    lines.append("## Slide 2 — Qué está pasando en la señal")
    lines.append(
        f"- Evolución semanal de **% {focus_name} vs incidencias** (usar gráfico de timeline causal)."
    )
    lines.append("- Señalar semanas con ruptura y eventos operativos/release.")
    lines.append("- Mensaje clave: cuándo la incidencia precede el deterioro NPS.")
    lines.append("")

    lines.append("## Slide 3 — Dónde duele (causas priorizadas)")
    lines.append(f"- Tópicos con mayor criticidad: **{top_topics}**.")
    if top.empty:
        lines.append("- No hay evidencia suficiente para priorización robusta.")
    else:
        for _, r in top.iterrows():
            lines.append(
                (
                    f"- {str(r.get('nps_topic',''))}: prioridad={float(r.get('priority',0.0)):.2f}, "
                    f"confianza={float(r.get('confidence',0.0)):.2f}, "
                    f"Δ%{focus_name}={float(r.get('delta_focus_rate_pp',0.0)):.2f} pp."
                )
            )
    lines.append("")

    lines.append("## Slide 4 — Cuánto impacta al NPS")
    lines.append("- Mostrar barra comparativa **NPS en riesgo vs NPS recuperable** por tópico.")
    if not top.empty:
        risk_top = float(pd.to_numeric(top["nps_points_at_risk"], errors="coerce").fillna(0).sum())
        rec_top = float(
            pd.to_numeric(top["nps_points_recoverable"], errors="coerce").fillna(0).sum()
        )
        lines.append(f"- Top temas analizados: riesgo={risk_top:.2f} pts | recuperable={rec_top:.2f} pts.")
    lines.append("- Mensaje clave: impacto económico esperado de corregir tópicos P1.")
    lines.append("")

    lines.append("## Slide 5 — Qué atacamos primero")
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

    lines.append("## Slide 6 — Plan 30-60-90")
    lines.append("- 30 días: quick wins operativos + corrección de fricción evidente.")
    lines.append("- 60 días: fixes estructurales y reducción de recurrencia de incidencias.")
    lines.append("- 90 días: escalado de prácticas efectivas + recalibración de prioridades.")
    lines.append("")

    lines.append("## Slide 7 — Gobierno y métricas")
    lines.append(f"- KPI leading: incidencias por tópico P1, SLA de resolución, % {focus_name}.")
    lines.append("- KPI lagging: NPS térmico, NPS en riesgo (pts), NPS recuperable realizado (pts).")
    lines.append("- Cadencia: comité semanal con owners de producto, tecnología y operaciones.")
    lines.append("")

    lines.append("## Slide 8 — Decisiones requeridas al comité")
    lines.append("- Aprobación de backlog P1 y asignación explícita de owners.")
    lines.append("- Priorización de capacidad (tecnología/operaciones) para plan 30-60-90.")
    lines.append("- Acuerdo de criterios de éxito y fecha de revisión de negocio.")
    return "\n".join(lines) + "\n"
