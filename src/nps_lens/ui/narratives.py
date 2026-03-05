from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd


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

    delta_nps = float(nps_cur - nps_base) if pd.notna(nps_cur) and pd.notna(nps_base) else float("nan")
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
    lines.append("# Informe ejecutivo — NPS Lens")
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
        d_nps = "—" if comparison.delta_nps != comparison.delta_nps else f"{comparison.delta_nps:+.2f}"
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
