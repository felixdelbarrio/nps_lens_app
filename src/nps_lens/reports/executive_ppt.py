from __future__ import annotations

import ast
import contextlib
import os
import re
import tempfile
import textwrap
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_AUTO_SHAPE_TYPE
from pptx.enum.text import MSO_AUTO_SIZE, MSO_VERTICAL_ANCHOR, PP_ALIGN
from pptx.util import Inches, Pt

from nps_lens.analytics.drivers import compute_nps_from_scores, driver_table
from nps_lens.analytics.hotspot_metrics import (
    HOTSPOT_EVIDENCE_COLUMNS,
    summarize_hotspot_counts,
)
from nps_lens.analytics.hotspot_metrics import (
    build_hotspot_daily_breakdown as build_hotspot_daily_breakdown_metrics,
)
from nps_lens.analytics.incident_attribution import (
    EXECUTIVE_JOURNEY_CATALOG,
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    summarize_attribution_chains,
)
from nps_lens.analytics.nps_helix_link import build_nps_topic
from nps_lens.analytics.opportunities import rank_opportunities
from nps_lens.analytics.text_mining import extract_topics
from nps_lens.design.tokens import (
    DesignTokens,
    bbva_typography_tokens,
    executive_report_palette,
    nps_score_color,
    palette,
    plotly_continuous_scale,
    plotly_risk_scale,
)
from nps_lens.reports.ppt_template import (
    CorporatePresentationTheme,
    build_presentation,
    resolve_layout,
)
from nps_lens.ui.charts import (
    chart_cohort_heatmap,
    chart_daily_kpis,
    chart_daily_mix_business,
    chart_daily_volume,
    chart_driver_bar,
    chart_driver_delta,
    chart_opportunities_bar,
    chart_topic_bars,
)
from nps_lens.ui.business import driver_delta_table
from nps_lens.ui.narratives import explain_opportunities
from nps_lens.ui.theme import get_theme

BBVA_COLORS = executive_report_palette(DesignTokens.default(), mode="light")
BBVA_TYPOGRAPHY = bbva_typography_tokens()
PPT_THEME = CorporatePresentationTheme(
    display_font=BBVA_TYPOGRAPHY.display,
    heading_font=BBVA_TYPOGRAPHY.heading,
    body_font=BBVA_TYPOGRAPHY.body,
    medium_font=BBVA_TYPOGRAPHY.medium,
)

BBVA_FONT_DISPLAY = PPT_THEME.display_font
BBVA_FONT_HEAD = PPT_THEME.heading_font
BBVA_FONT_BODY = PPT_THEME.body_font
BBVA_FONT_MEDIUM = PPT_THEME.medium_font


def _ppt_nps_marker_colors(values: pd.Series | list[object]) -> list[str]:
    series = values if isinstance(values, pd.Series) else pd.Series(list(values))
    tokens = DesignTokens.default()
    return [nps_score_color(tokens, "light", value) for value in series.tolist()]


@dataclass(frozen=True)
class BusinessPptResult:
    file_name: str
    content: bytes
    slide_count: int


@dataclass(frozen=True)
class ZoomIncident:
    incident_id: str
    incident_date: Optional[pd.Timestamp]
    nps_topic: str
    incident_summary: str
    detractor_comment: str
    similarity: float
    hot_term: str
    mention_incidents: int = 0
    mention_comments: int = 0
    hotspot_incidents: int = 0
    hotspot_comments: int = 0
    hotspot_links: int = 0
    sample_incidents: str = ""
    sample_comments: str = ""


def _rgb(hex_code: str) -> RGBColor:
    code = str(hex_code or "").strip().lstrip("#")
    if len(code) != 6:
        code = BBVA_COLORS["ink"]
    return RGBColor(int(code[0:2], 16), int(code[2:4], 16), int(code[4:6], 16))


def _safe_float(v: object, default: float = 0.0) -> float:
    try:
        f = float(v)
    except Exception:
        return float(default)
    if not np.isfinite(f):
        return float(default)
    return float(f)


def _safe_int(v: object, default: int = 0) -> int:
    try:
        i = int(float(v))
    except Exception:
        return int(default)
    return int(i)


def _fmt_pct_or_nd(v: object) -> str:
    f = _safe_float(v, default=float("nan"))
    return "n/d" if not np.isfinite(f) else f"{f*100:.0f}%"


def _fmt_signed_or_nd(v: object, decimals: int = 1) -> str:
    f = _safe_float(v, default=float("nan"))
    return "n/d" if not np.isfinite(f) else f"{f:+.{int(decimals)}f}"


def _fmt_num_or_nd(v: object, decimals: int = 2) -> str:
    f = _safe_float(v, default=float("nan"))
    return "n/d" if not np.isfinite(f) else f"{f:.{int(decimals)}f}"


def _clip(txt: object, max_len: int) -> str:
    s = " ".join(str(txt or "").split())
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _coerce_datetime_scalar(value: object) -> pd.Timestamp:
    try:
        return pd.Timestamp(pd.to_datetime(value, format="mixed", dayfirst=True, errors="coerce"))
    except TypeError:
        return pd.Timestamp(pd.to_datetime(value, dayfirst=True, errors="coerce"))


def _coerce_datetime_series(values: object) -> pd.Series:
    series = values if isinstance(values, pd.Series) else pd.Series(values)
    try:
        return pd.to_datetime(series, format="mixed", dayfirst=True, errors="coerce")
    except TypeError:
        return pd.to_datetime(series, dayfirst=True, errors="coerce")


def _wrap_label(
    txt: object,
    *,
    width: int = 24,
    max_lines: int = 2,
    joiner: str = "<br>",
) -> str:
    clean = " ".join(str(txt or "").split())
    if not clean:
        return ""
    lines = textwrap.wrap(clean, width=max(int(width), 8)) or [clean]
    if len(lines) > max(int(max_lines), 1):
        lines = lines[:max_lines]
        lines[-1] = _clip(lines[-1], max(int(width) - 1, 8))
    return joiner.join(lines)


def _configure_text_frame(tf: object) -> None:
    with contextlib.suppress(Exception):
        tf.word_wrap = True
    with contextlib.suppress(Exception):
        tf.auto_size = MSO_AUTO_SIZE.TEXT_TO_FIT_SHAPE


def _focus_risk_label(focus_name: str) -> str:
    focus = str(focus_name or "").strip().lower()
    if focus in {"detractores", "detractor", "detraccion", "detracción"}:
        return "detracción"
    if focus in {"promotores", "promotor"}:
        return "promoción"
    return str(focus_name or "impacto").strip()


def _focus_probability_label(focus_name: str) -> str:
    focus_label = _focus_risk_label(focus_name)
    return f"Prob. de {focus_label}"


def _action_lane_label(value: object) -> str:
    lane = str(value or "").strip()
    normalized = unicodedata.normalize("NFKD", lane).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    mapping = {
        "quick win operativo": "Corrección operativa inmediata",
        "fix estructural": "Corrección estructural",
        "instrumentacion + validacion": "Revisión de medición y validación",
        "instrumentacion+validacion": "Revisión de medición y validación",
        "voc + analitica": "Análisis y validación adicional",
        "canal + operaciones": "Canal y operaciones",
        "producto + tecnologia": "Producto y tecnología",
    }
    return mapping.get(normalized, lane or "n/d")


def _format_opportunity_scope(dimension: object, value: object) -> str:
    dim = str(dimension or "").strip().lower()
    clean_value = _clip(value, 42)
    if dim == "palanca":
        return f"{clean_value} (palanca)"
    if dim == "subpalanca":
        return f"{clean_value} (subpalanca)"
    if dim == "nps_topic":
        return clean_value
    return clean_value


def _clean_evidence_excerpt(text: object, *, max_len: int = 128) -> str:
    clean = " ".join(str(text or "").split())
    if not clean:
        return ""
    for marker in ["Síntoma:", "Sintoma:", "Descripcion:", "Descripción:"]:
        if marker in clean:
            clean = clean.split(marker, 1)[1].strip()
            break
    clean = re.sub(r"^(ACOTAMIENTO IRD|Acotamiento IRD)\s*", "", clean)
    return _clip(clean, max_len)


def _is_cover_metric_line(text: str) -> bool:
    low = str(text or "").strip().lower()
    return any(
        token in low
        for token in [
            "muestras",
            "comentarios analizados",
            "nps medio",
            "nps clásico",
            "nps clasico",
            "detractores",
            "promotores",
        ]
    )


def _slug(value: object, *, max_len: int = 42) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "na"
    norm = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    norm = re.sub(r"[^a-z0-9]+", "-", norm)
    norm = re.sub(r"-{2,}", "-", norm).strip("-")
    if not norm:
        return "na"
    return norm[: int(max_len)].strip("-") or "na"


def _safe_date(value: object) -> str:
    try:
        ts = _coerce_datetime_scalar(value)
        return str(ts.date())
    except Exception:
        return str(value or "")


def _safe_dt(value: object) -> Optional[pd.Timestamp]:
    try:
        ts = _coerce_datetime_scalar(value)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts)


def _month_label_es(d: date) -> str:
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
    return f"{months.get(int(d.month), 'mes')} {int(d.year)}"


def _comment_column(df: pd.DataFrame) -> str:
    for candidate in ["Comment", "Comentario", "comment", "comentario"]:
        if candidate in df.columns:
            return candidate
    return ""


def _nps_band(value: object) -> str:
    score = _safe_float(value, default=float("nan"))
    if not np.isfinite(score):
        return "Sin dato"
    if score <= 6.0:
        return "Detractor"
    if score >= 9.0:
        return "Promotor"
    return "Pasivo"


def _coerce_nps_records(nps_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    cols = [
        "date",
        "NPS",
        "comment_txt",
        "nps_topic",
        "Palanca",
        "Subpalanca",
        "band",
    ]
    if nps_df is None or nps_df.empty:
        return pd.DataFrame(columns=cols)

    out = nps_df.copy()
    out["date"] = _coerce_datetime_series(out.get("Fecha")).dt.normalize()
    out["NPS"] = pd.to_numeric(out.get("NPS"), errors="coerce")
    comment_col = _comment_column(out)
    out["comment_txt"] = (
        out.get(comment_col, pd.Series([""] * len(out), index=out.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    if "nps_topic" in out.columns:
        out["nps_topic"] = out["nps_topic"].astype(str).fillna("").str.strip()
    else:
        out["nps_topic"] = build_nps_topic(out).astype(str).fillna("").str.strip()
    out["Palanca"] = (
        out.get("Palanca", pd.Series([""] * len(out), index=out.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    out["Subpalanca"] = (
        out.get("Subpalanca", pd.Series([""] * len(out), index=out.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    out["band"] = out["NPS"].map(_nps_band)
    out = out.dropna(subset=["date"]).copy()
    return out[cols].copy()


def _split_period_frames(
    nps_df: pd.DataFrame,
    *,
    period_start: date,
    period_end: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if nps_df.empty:
        return nps_df.copy(), nps_df.copy()
    start_ts = pd.Timestamp(period_start)
    end_ts = pd.Timestamp(period_end)
    current = nps_df[(nps_df["date"] >= start_ts) & (nps_df["date"] <= end_ts)].copy()
    baseline = nps_df[nps_df["date"] < start_ts].copy()
    if baseline.empty:
        baseline = nps_df[(nps_df["date"] < start_ts) | (nps_df["date"] > end_ts)].copy()
    return current, baseline


def _split_source_period_frames(
    nps_df: Optional[pd.DataFrame],
    *,
    period_start: date,
    period_end: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if nps_df is None or nps_df.empty or "Fecha" not in nps_df.columns:
        return pd.DataFrame(), pd.DataFrame()

    out = nps_df.copy()
    out["Fecha"] = _coerce_datetime_series(out["Fecha"])
    out = out.dropna(subset=["Fecha"]).copy()
    if out.empty:
        return pd.DataFrame(), pd.DataFrame()

    start_ts = pd.Timestamp(period_start)
    end_ts = pd.Timestamp(period_end)
    current = out[(out["Fecha"] >= start_ts) & (out["Fecha"] <= end_ts)].copy()
    baseline = out[out["Fecha"] < start_ts].copy()
    if baseline.empty:
        baseline = out[(out["Fecha"] < start_ts) | (out["Fecha"] > end_ts)].copy()
    return current, baseline


def _period_overview(current_nps_df: pd.DataFrame) -> dict[str, object]:
    scores = pd.to_numeric(current_nps_df.get("NPS"), errors="coerce").dropna()
    comment_series = current_nps_df.get("comment_txt", pd.Series(dtype=str)).astype(str).str.strip()
    comment_count = (
        int(comment_series.ne("").sum()) if not comment_series.empty else int(len(scores))
    )
    detr = float((scores <= 6).mean()) if not scores.empty else 0.0
    prom = float((scores >= 9).mean()) if not scores.empty else 0.0
    pas = float(((scores >= 7) & (scores <= 8)).mean()) if not scores.empty else 0.0
    nps_mean = float(scores.mean()) if not scores.empty else float("nan")
    classic_nps = compute_nps_from_scores(scores) if not scores.empty else float("nan")
    daily_mix = _daily_group_mix(current_nps_df)
    start_classic = (
        float(pd.to_numeric(daily_mix["nps_classic"], errors="coerce").iloc[0])
        if not daily_mix.empty
        else float("nan")
    )
    end_classic = (
        float(pd.to_numeric(daily_mix["nps_classic"], errors="coerce").iloc[-1])
        if not daily_mix.empty
        else float("nan")
    )
    start_detr = (
        float(pd.to_numeric(daily_mix["detractor_rate"], errors="coerce").iloc[0])
        if not daily_mix.empty
        else float("nan")
    )
    end_detr = (
        float(pd.to_numeric(daily_mix["detractor_rate"], errors="coerce").iloc[-1])
        if not daily_mix.empty
        else float("nan")
    )
    driver_col = "Subpalanca" if current_nps_df.get("Subpalanca") is not None else "Palanca"
    if driver_col not in current_nps_df.columns:
        driver_col = "Palanca"
    pain_point = ""
    strength_point = ""
    if driver_col in current_nps_df.columns and "NPS" in current_nps_df.columns:
        driver_view = current_nps_df[[driver_col, "NPS"]].copy().dropna(subset=["NPS"])
        if not driver_view.empty:
            driver_view[driver_col] = driver_view[driver_col].astype(str).str.strip()
            driver_view = driver_view[driver_view[driver_col] != ""]
            if not driver_view.empty:
                ranking = (
                    driver_view.groupby(driver_col, dropna=False)
                    .agg(nps_mean=("NPS", "mean"), n=("NPS", "size"))
                    .sort_values(["nps_mean", "n"], ascending=[True, False])
                )
                if not ranking.empty:
                    pain_point = str(ranking.index[0])
                    strength_point = str(ranking.index[-1])
    return {
        "comments": comment_count,
        "nps_mean": nps_mean,
        "detractor_rate": detr,
        "promoter_rate": prom,
        "passive_rate": pas,
        "classic_nps": classic_nps,
        "start_classic": start_classic,
        "end_classic": end_classic,
        "classic_delta": (
            end_classic - start_classic
            if np.isfinite(start_classic) and np.isfinite(end_classic)
            else float("nan")
        ),
        "start_detr": start_detr,
        "end_detr": end_detr,
        "detractor_delta_pp": (
            ((end_detr - start_detr) * 100.0)
            if np.isfinite(start_detr) and np.isfinite(end_detr)
            else float("nan")
        ),
        "pain_point": pain_point,
        "strength_point": strength_point,
    }


def _daily_group_mix(nps_df: pd.DataFrame) -> pd.DataFrame:
    if nps_df is None or nps_df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "responses",
                "detractor_rate",
                "passive_rate",
                "promoter_rate",
                "nps_classic",
                "nps_mean",
            ]
        )

    work = nps_df.copy()
    work["date"] = _coerce_datetime_series(work["date"])
    work = work.dropna(subset=["date"]).copy()
    if work.empty:
        return pd.DataFrame()

    grouped = (
        work.groupby("date")
        .agg(
            responses=("NPS", "count"),
            detractors=("band", lambda s: int((s == "Detractor").sum())),
            passives=("band", lambda s: int((s == "Pasivo").sum())),
            promoters=("band", lambda s: int((s == "Promotor").sum())),
            nps_mean=("NPS", "mean"),
        )
        .reset_index()
        .sort_values("date")
    )
    grouped["detractor_rate"] = grouped["detractors"] / grouped["responses"].replace({0: np.nan})
    grouped["passive_rate"] = grouped["passives"] / grouped["responses"].replace({0: np.nan})
    grouped["promoter_rate"] = grouped["promoters"] / grouped["responses"].replace({0: np.nan})
    grouped["nps_classic"] = (grouped["promoter_rate"] - grouped["detractor_rate"]) * 100.0
    return grouped


def _merge_daily_incidents(base_df: pd.DataFrame, overall_daily: pd.DataFrame) -> pd.DataFrame:
    base = base_df.copy()
    if base.empty:
        return base
    out = base.copy()
    out["date"] = _coerce_datetime_series(out["date"]).dt.normalize()
    if overall_daily is None or overall_daily.empty or "date" not in overall_daily.columns:
        out["incidents"] = 0.0
        return out
    inc = overall_daily.copy()
    inc["date"] = _coerce_datetime_series(inc["date"]).dt.normalize()
    inc["incidents"] = pd.to_numeric(inc.get("incidents"), errors="coerce").fillna(0.0)
    inc = inc.groupby("date", as_index=False).agg(incidents=("incidents", "sum"))
    out = out.merge(inc, on="date", how="left")
    out["incidents"] = pd.to_numeric(out["incidents"], errors="coerce").fillna(0.0)
    return out


def _topic_summary(by_topic_daily: Optional[pd.DataFrame]) -> pd.DataFrame:
    cols = ["nps_topic", "comments", "focus_comments", "incidents", "nps_mean", "share"]
    if by_topic_daily is None or by_topic_daily.empty or "nps_topic" not in by_topic_daily.columns:
        return pd.DataFrame(columns=cols)

    d = by_topic_daily.copy()
    d["responses"] = pd.to_numeric(d.get("responses"), errors="coerce").fillna(0.0)
    d["focus_count"] = pd.to_numeric(d.get("focus_count"), errors="coerce").fillna(0.0)
    d["incidents"] = pd.to_numeric(d.get("incidents"), errors="coerce").fillna(0.0)
    d["nps_mean"] = pd.to_numeric(d.get("nps_mean"), errors="coerce")
    d["topic_nps_weight"] = d["nps_mean"].fillna(0.0) * d["responses"]
    out = (
        d.groupby("nps_topic", as_index=False)
        .agg(
            comments=("responses", "sum"),
            focus_comments=("focus_count", "sum"),
            incidents=("incidents", "sum"),
            topic_nps_weight=("topic_nps_weight", "sum"),
        )
        .sort_values(["comments", "focus_comments", "incidents"], ascending=False)
    )
    out["nps_mean"] = out["topic_nps_weight"] / out["comments"].replace({0.0: np.nan})
    out["share"] = (
        out["comments"] / out["comments"].sum() if float(out["comments"].sum()) > 0 else 0.0
    )
    out["nps_topic"] = out["nps_topic"].astype(str).str.strip()
    out = out[out["nps_topic"] != ""].copy()
    return out[cols]


def _text_topics_table(current_nps_df: pd.DataFrame, *, top_k: int = 10) -> pd.DataFrame:
    cols = ["cluster_id", "n", "top_terms", "examples", "label", "top_terms_txt", "example_txt"]
    if (
        current_nps_df is None
        or current_nps_df.empty
        or "comment_txt" not in current_nps_df.columns
    ):
        return pd.DataFrame(columns=cols)

    comments = current_nps_df["comment_txt"].astype(str).str.strip()
    comments = comments[comments.ne("")]
    if comments.empty:
        return pd.DataFrame(columns=cols)

    topics = extract_topics(comments, n_clusters=max(int(top_k), 10))
    if not topics:
        return pd.DataFrame(columns=cols)

    d = (
        pd.DataFrame([topic.__dict__ for topic in topics])
        .sort_values("n", ascending=False)
        .head(int(top_k))
        .copy()
    )
    d["label"] = d.apply(
        lambda row: f"#{int(row['cluster_id'])}: {', '.join(list(row['top_terms'])[:3])}",
        axis=1,
    )
    d["top_terms_txt"] = d["top_terms"].apply(
        lambda values: ", ".join([str(v).strip() for v in list(values)[:6] if str(v).strip()])
    )
    d["example_txt"] = d["examples"].apply(
        lambda values: " | ".join([_clip(v, 42) for v in list(values)[:2] if str(v).strip()])
    )
    return d[cols].reset_index(drop=True)


def _driver_change_table(
    current_nps_df: pd.DataFrame,
    baseline_nps_df: pd.DataFrame,
    *,
    dimension: str,
) -> pd.DataFrame:
    cols = [
        "value",
        "n_current",
        "nps_current",
        "nps_baseline",
        "delta_nps",
        "detr_current",
        "detr_baseline",
        "delta_detr_pp",
    ]
    if (
        current_nps_df is None
        or current_nps_df.empty
        or baseline_nps_df is None
        or baseline_nps_df.empty
        or dimension not in current_nps_df.columns
        or dimension not in baseline_nps_df.columns
    ):
        return pd.DataFrame(columns=cols)

    cur = pd.DataFrame([s.__dict__ for s in driver_table(current_nps_df, dimension=dimension)])
    base = pd.DataFrame([s.__dict__ for s in driver_table(baseline_nps_df, dimension=dimension)])
    if cur.empty or base.empty:
        return pd.DataFrame(columns=cols)

    merged = cur.rename(
        columns={
            "n": "n_current",
            "nps": "nps_current",
            "detractor_rate": "detr_current",
        }
    ).merge(
        base[["value", "nps", "detractor_rate"]],
        on="value",
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(columns=cols)
    merged = merged.rename(
        columns={
            "nps": "nps_baseline",
            "detractor_rate": "detr_baseline",
        }
    )
    merged["delta_nps"] = pd.to_numeric(merged["nps_current"], errors="coerce") - pd.to_numeric(
        merged["nps_baseline"], errors="coerce"
    )
    merged["delta_detr_pp"] = (
        pd.to_numeric(merged["detr_current"], errors="coerce")
        - pd.to_numeric(merged["detr_baseline"], errors="coerce")
    ) * 100.0
    return merged[cols].sort_values(["delta_nps", "n_current"], ascending=[True, False])


def _group_matrix(current_nps_df: pd.DataFrame, *, dimension: str, top_k: int = 8) -> pd.DataFrame:
    cols = [dimension, "band", "share", "count"]
    if current_nps_df is None or current_nps_df.empty or dimension not in current_nps_df.columns:
        return pd.DataFrame(columns=cols)
    d = current_nps_df.copy()
    d[dimension] = d[dimension].astype(str).fillna("").str.strip()
    d = d[d[dimension] != ""].copy()
    if d.empty:
        return pd.DataFrame(columns=cols)

    top_dims = (
        d.groupby(dimension, as_index=False)
        .agg(count=("NPS", "count"), detractors=("band", lambda s: int((s == "Detractor").sum())))
        .sort_values(["detractors", "count"], ascending=False)
        .head(int(top_k))
    )
    scoped = d[d[dimension].isin(top_dims[dimension].tolist())].copy()
    matrix = (
        scoped.groupby([dimension, "band"], as_index=False)
        .agg(count=("NPS", "count"))
        .merge(
            top_dims[[dimension, "count"]].rename(columns={"count": "dimension_total"}),
            on=dimension,
        )
    )
    matrix["share"] = matrix["count"] / matrix["dimension_total"].replace({0: np.nan})
    return matrix[cols]


def _gap_vs_overall_table(current_nps_df: pd.DataFrame, *, top_k: int = 10) -> pd.DataFrame:
    if current_nps_df is None or current_nps_df.empty:
        return pd.DataFrame(columns=["value", "n", "nps", "gap_vs_overall"])
    work = current_nps_df.copy()
    if "nps_topic" not in work.columns or work["nps_topic"].astype(str).str.strip().eq("").all():
        work["nps_topic"] = build_nps_topic(work).astype(str).fillna("").str.strip()
    stats = pd.DataFrame([s.__dict__ for s in driver_table(work, dimension="nps_topic")])
    if stats.empty:
        return pd.DataFrame(columns=["value", "n", "nps", "gap_vs_overall"])
    stats = stats[stats["n"] > 0].copy()
    stats = stats.sort_values(["gap_vs_overall", "n"], ascending=[True, False]).head(int(top_k))
    return stats[["value", "n", "nps", "gap_vs_overall"]]


def _dimension_gap_table(
    current_nps_df: pd.DataFrame,
    *,
    dimension: str,
    top_k: int = 10,
) -> pd.DataFrame:
    cols = ["value", "n", "nps", "gap_vs_overall"]
    if current_nps_df is None or current_nps_df.empty or dimension not in current_nps_df.columns:
        return pd.DataFrame(columns=cols)
    stats = pd.DataFrame([s.__dict__ for s in driver_table(current_nps_df, dimension=dimension)])
    if stats.empty:
        return pd.DataFrame(columns=cols)
    stats = stats[stats["n"] > 0].copy()
    stats = stats.sort_values(["gap_vs_overall", "n"], ascending=[True, False]).head(int(top_k))
    return stats[cols]


def _opportunities_table(current_nps_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["dimension", "value", "n", "current_nps", "potential_uplift", "confidence", "why"]
    if current_nps_df is None or current_nps_df.empty:
        return pd.DataFrame(columns=cols)
    work = current_nps_df.copy()
    if "nps_topic" not in work.columns or work["nps_topic"].astype(str).str.strip().eq("").all():
        work["nps_topic"] = build_nps_topic(work).astype(str).fillna("").str.strip()
    min_n = max(20, int(len(work) * 0.04))
    rows = rank_opportunities(
        work,
        dimensions=[dim for dim in ["Palanca", "Subpalanca", "nps_topic"] if dim in work.columns],
        min_n=min_n,
    )
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([row.__dict__ for row in rows])[cols]


def _matching_topic_for_chain(
    chain_row: pd.Series,
    by_topic_daily: Optional[pd.DataFrame],
) -> str:
    if by_topic_daily is None or by_topic_daily.empty or "nps_topic" not in by_topic_daily.columns:
        return ""
    values = set(by_topic_daily["nps_topic"].astype(str).str.strip().tolist())
    candidates = [
        str(chain_row.get("nps_topic", "") or "").strip(),
        " > ".join(
            [
                str(chain_row.get("palanca", "") or "").strip(),
                str(chain_row.get("subpalanca", "") or "").strip(),
            ]
        ).strip(" >"),
        str(chain_row.get("palanca", "") or "").strip(),
    ]
    for candidate in candidates:
        if candidate and candidate in values:
            return candidate
    return ""


def _chain_comment_heatmap_fig(chain_row: pd.Series) -> Optional[go.Figure]:
    comment_records = chain_row.get("comment_records")
    if not isinstance(comment_records, list) or not comment_records:
        return None
    rows = pd.DataFrame(comment_records)
    if rows.empty or "date" not in rows.columns or "group" not in rows.columns:
        return None
    rows["date"] = _coerce_datetime_series(rows["date"])
    rows = rows.dropna(subset=["date"]).copy()
    if rows.empty:
        return None
    rows["group"] = (
        rows["group"]
        .astype(str)
        .replace(
            {
                "Promoter": "Promotor",
                "Passive": "Pasivo",
                "Detractor": "Detractor",
                "PROMOTER": "Promotor",
                "PASSIVE": "Pasivo",
                "DETRACTOR": "Detractor",
            }
        )
    )
    rows["count"] = 1
    heat = (
        rows.groupby(["group", "date"], as_index=False)
        .agg(count=("count", "sum"))
        .pivot(index="group", columns="date", values="count")
        .fillna(0.0)
    )
    if heat.empty:
        return None
    heat = heat.reindex(["Detractor", "Pasivo", "Promotor"]).dropna(how="all")
    fig = go.Figure(
        data=go.Heatmap(
            z=heat.to_numpy(),
            x=list(heat.columns),
            y=list(heat.index),
            colorscale=plotly_continuous_scale(DesignTokens.default(), "light"),
            showscale=False,
            text=heat.to_numpy(dtype=int),
            texttemplate="%{text}",
            hovertemplate="%{y}<br>%{x|%d %b}: %{z:.0f} comentarios<extra></extra>",
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=16, t=20, b=34),
        xaxis_title="Día",
        yaxis_title="Grupo",
    )
    return fig


def _chain_portfolio_fig(
    chain_df: Optional[pd.DataFrame],
    *,
    highlight_topic: str,
) -> Optional[go.Figure]:
    if chain_df is None or chain_df.empty:
        return None
    d = chain_df.copy()
    d["priority"] = pd.to_numeric(d.get("priority"), errors="coerce").fillna(0.0)
    d["confidence"] = pd.to_numeric(d.get("confidence"), errors="coerce").fillna(0.0)
    d["impact"] = pd.to_numeric(d.get("total_nps_impact"), errors="coerce").fillna(0.0)
    d["links"] = pd.to_numeric(d.get("linked_pairs"), errors="coerce").fillna(0.0)
    if d.empty:
        return None
    d["is_highlight"] = (
        d.get("nps_topic", "").astype(str).str.strip() == str(highlight_topic).strip()
    )
    colors = [
        "#" + (BBVA_COLORS["red"] if is_h else BBVA_COLORS["sky"])
        for is_h in d["is_highlight"].tolist()
    ]
    fig = go.Figure(
        go.Scatter(
            x=d["confidence"],
            y=d["impact"],
            mode="markers+text",
            text=d["nps_topic"].astype(str).map(lambda value: _wrap_label(value, width=14)),
            textposition="top center",
            marker=dict(size=12 + d["links"] * 3.0, color=colors, opacity=0.88),
            customdata=d[["links"]].to_numpy(),
            hovertemplate=(
                "%{text}<br>Solidez=%{x:.2f}<br>Impacto=%{y:.2f} pts"
                "<br>Vínculos=%{customdata[0]:.0f}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=16, t=20, b=30),
        xaxis=dict(title="Solidez de la evidencia", range=[0, 1]),
        yaxis=dict(title="Impacto total (pts NPS)", rangemode="tozero"),
        showlegend=False,
    )
    return fig


def _chain_temporal_fig(
    chain_row: pd.Series,
    *,
    by_topic_daily: Optional[pd.DataFrame],
    lag_days_by_topic: Optional[pd.DataFrame],
    lag_weeks_by_topic: Optional[pd.DataFrame],
    changepoints_by_topic: Optional[pd.DataFrame],
) -> Optional[go.Figure]:
    if by_topic_daily is None or by_topic_daily.empty:
        return None
    topic_key = _matching_topic_for_chain(chain_row, by_topic_daily)
    if not topic_key:
        return None
    data = by_topic_daily[by_topic_daily["nps_topic"].astype(str).str.strip() == topic_key].copy()
    if data.empty:
        return None
    data["date"] = _coerce_datetime_series(data["date"])
    data = data.dropna(subset=["date"]).sort_values("date")
    data["focus_rate"] = pd.to_numeric(data.get("focus_rate"), errors="coerce").fillna(0.0)
    data["incidents"] = pd.to_numeric(data.get("incidents"), errors="coerce").fillna(0.0)
    data["nps_mean"] = pd.to_numeric(data.get("nps_mean"), errors="coerce")
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=data["date"],
            y=data["incidents"],
            name="Incidencias",
            marker=dict(color="#" + BBVA_COLORS["yellow"]),
            opacity=0.56,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=data["date"],
            y=data["focus_rate"] * 100.0,
            name="% foco",
            mode="lines",
            yaxis="y2",
            line=dict(color="#" + BBVA_COLORS["red"], width=2.4),
        )
    )
    if data["nps_mean"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=data["date"],
                y=data["nps_mean"],
                name="NPS medio",
                yaxis="y3",
                mode="lines+markers",
                line=dict(color="#" + BBVA_COLORS["blue"], width=2.4),
                marker=dict(size=6, color=_ppt_nps_marker_colors(data["nps_mean"])),
            )
        )
    cp_map = _changepoints_map(changepoints_by_topic)
    for cp in cp_map.get(topic_key, []):
        fig.add_vline(x=cp, line_dash="dot", line_color="#" + BBVA_COLORS["sky"], line_width=1.2)
    lag_days = _lag_days_for_topic(
        topic_key,
        lag_days_by_topic=lag_days_by_topic,
        lag_weeks_by_topic=lag_weeks_by_topic,
        rationale_df=pd.DataFrame(),
        ranking_df=None,
    )
    if lag_days > 0 and not data.empty:
        anchor = data["date"].min() + pd.Timedelta(days=lag_days)
        if anchor <= data["date"].max():
            fig.add_vline(
                x=anchor,
                line_dash="dash",
                line_color="#" + BBVA_COLORS["orange"],
                line_width=1.2,
            )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=84, t=20, b=28),
        legend=dict(orientation="h", x=0.0, y=1.08),
        xaxis_title="Día",
        yaxis=dict(title="Incidencias", rangemode="tozero"),
        yaxis2=dict(title="% foco", overlaying="y", side="right", range=[0, 100], showgrid=False),
        yaxis3=dict(
            title="NPS medio",
            overlaying="y",
            side="right",
            anchor="free",
            position=0.92,
            showgrid=False,
            range=[0, 10],
        ),
    )
    return fig


def _nps_evolution_fig(daily_mix: pd.DataFrame, overall_daily: pd.DataFrame) -> Optional[go.Figure]:
    if daily_mix is None or daily_mix.empty:
        return None
    d = _merge_daily_incidents(daily_mix, overall_daily)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["nps_classic"],
            name="NPS clásico",
            mode="lines+markers",
            line=dict(color="#" + BBVA_COLORS["blue"], width=3.0),
            marker=dict(size=7, color="#" + BBVA_COLORS["sky"]),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["detractor_rate"] * 100.0,
            name="% detractores",
            yaxis="y2",
            mode="lines",
            line=dict(color="#" + BBVA_COLORS["red"], width=2.4),
        )
    )
    fig.add_trace(
        go.Bar(
            x=d["date"],
            y=d["incidents"],
            name="Incidencias",
            yaxis="y3",
            marker=dict(color="#" + BBVA_COLORS["yellow"]),
            opacity=0.70,
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=84, t=20, b=24),
        legend=dict(orientation="h", x=0.0, y=1.08),
        xaxis_title="Día",
        yaxis=dict(title="NPS clásico", rangemode="tozero"),
        yaxis2=dict(
            title="% detractores", overlaying="y", side="right", range=[0, 100], showgrid=False
        ),
        yaxis3=dict(
            title="Incidencias",
            overlaying="y",
            side="right",
            anchor="free",
            position=0.92,
            showgrid=False,
            rangemode="tozero",
        ),
    )
    return fig


def _top_topics_fig(topics_df: pd.DataFrame, *, top_k: int = 10) -> Optional[go.Figure]:
    if topics_df is None or topics_df.empty:
        return None
    d = topics_df.head(int(top_k)).copy().iloc[::-1]
    d["topic_label"] = d["nps_topic"].astype(str).map(lambda value: _wrap_label(value, width=26))
    fig = go.Figure(
        go.Bar(
            x=d["comments"],
            y=d["topic_label"],
            orientation="h",
            marker=dict(color=_ppt_nps_marker_colors(d["nps_mean"])),
            text=[f"{int(v)} · {s*100:.0f}%" for v, s in zip(d["comments"], d["share"])],
            textposition="outside",
            cliponaxis=False,
            customdata=np.column_stack([d["nps_mean"].fillna(np.nan), d["nps_topic"].astype(str)]),
            hovertemplate="%{customdata[1]}<br>Comentarios=%{x:.0f}<br>NPS medio=%{customdata[0]:.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=34, r=28, t=20, b=24),
        xaxis_title="Comentarios",
        yaxis_title="",
        showlegend=False,
    )
    return fig


def _topic_heatmap_fig(
    by_topic_daily: Optional[pd.DataFrame], *, top_k: int = 10
) -> Optional[go.Figure]:
    top = _topic_summary(by_topic_daily).head(int(top_k))
    if top.empty or by_topic_daily is None or by_topic_daily.empty:
        return None
    d = by_topic_daily.copy()
    d["date"] = _coerce_datetime_series(d["date"])
    d["responses"] = pd.to_numeric(d.get("responses"), errors="coerce").fillna(0.0)
    d = d[d["nps_topic"].astype(str).isin(top["nps_topic"].astype(str).tolist())].copy()
    if d.empty:
        return None
    pivot = (
        d.groupby(["nps_topic", "date"], as_index=False)
        .agg(responses=("responses", "sum"))
        .pivot(index="nps_topic", columns="date", values="responses")
        .fillna(0.0)
    )
    pivot = pivot.reindex(top["nps_topic"].tolist()).fillna(0.0)
    pivot.index = [_wrap_label(value, width=24) for value in pivot.index]
    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.to_numpy(),
            x=list(pivot.columns),
            y=list(pivot.index),
            colorscale=plotly_continuous_scale(DesignTokens.default(), "light"),
            showscale=False,
            hovertemplate="%{y}<br>%{x|%d %b}: %{z:.0f} comentarios<extra></extra>",
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=16, t=20, b=30),
        xaxis_title="Día",
        yaxis_title="Top temas",
    )
    return fig


def _daily_group_mix_fig(daily_mix: pd.DataFrame) -> Optional[go.Figure]:
    if daily_mix is None or daily_mix.empty:
        return None
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=daily_mix["date"],
            y=daily_mix["promoter_rate"] * 100.0,
            name="Promotores",
            marker=dict(color="#" + BBVA_COLORS["green"]),
        )
    )
    fig.add_trace(
        go.Bar(
            x=daily_mix["date"],
            y=daily_mix["passive_rate"] * 100.0,
            name="Pasivos",
            marker=dict(color="#" + BBVA_COLORS["yellow"]),
        )
    )
    fig.add_trace(
        go.Bar(
            x=daily_mix["date"],
            y=daily_mix["detractor_rate"] * 100.0,
            name="Detractores",
            marker=dict(color="#" + BBVA_COLORS["red"]),
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=16, t=20, b=24),
        barmode="stack",
        legend=dict(orientation="h", x=0.0, y=1.08),
        xaxis_title="Día",
        yaxis=dict(title="% sobre comentarios del día", range=[0, 100]),
    )
    return fig


def _delta_bars_fig(change_df: pd.DataFrame, *, metric: str, x_title: str) -> Optional[go.Figure]:
    if change_df is None or change_df.empty or metric not in change_df.columns:
        return None
    d = change_df.copy()
    d[metric] = pd.to_numeric(d[metric], errors="coerce")
    d = d.dropna(subset=[metric]).copy()
    if d.empty:
        return None
    d["abs_metric"] = d[metric].abs()
    d = d.sort_values(["abs_metric", "n_current"], ascending=[False, False]).head(8).iloc[::-1]
    d["color"] = np.where(d[metric] < 0, "#" + BBVA_COLORS["red"], "#" + BBVA_COLORS["green"])
    d["axis_label"] = d["value"].astype(str).map(lambda value: _wrap_label(value, width=20))
    fig = go.Figure(
        go.Bar(
            x=d[metric],
            y=d["axis_label"],
            orientation="h",
            marker=dict(color=d["color"].tolist()),
            text=[f"{v:+.1f}" for v in d[metric].tolist()],
            textposition="outside",
            cliponaxis=False,
            showlegend=False,
        )
    )
    fig.add_vline(x=0, line_color="#" + BBVA_COLORS["line"], line_width=1)
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=24, t=20, b=24),
        xaxis_title=x_title,
        yaxis_title="",
    )
    return fig


def _group_heatmap_fig(matrix_df: pd.DataFrame, *, dimension: str) -> Optional[go.Figure]:
    if matrix_df is None or matrix_df.empty or dimension not in matrix_df.columns:
        return None
    pivot = (
        matrix_df.pivot(index=dimension, columns="band", values="share")
        .fillna(0.0)
        .reindex(columns=["Detractor", "Pasivo", "Promotor"], fill_value=0.0)
    )
    if pivot.empty:
        return None
    pivot.index = [_wrap_label(value, width=18) for value in pivot.index]
    fig = go.Figure(
        data=go.Heatmap(
            z=(pivot.to_numpy() * 100.0),
            x=list(pivot.columns),
            y=list(pivot.index),
            colorscale=plotly_risk_scale(DesignTokens.default(), "light"),
            showscale=False,
            text=np.vectorize(lambda v: f"{v:.0f}%")(pivot.to_numpy() * 100.0),
            texttemplate="%{text}",
            hovertemplate="%{y}<br>%{x}: %{z:.1f}%<extra></extra>",
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=16, t=20, b=24),
        xaxis_title="Grupo NPS",
        yaxis_title="",
    )
    return fig


def _gap_vs_overall_fig(gap_df: pd.DataFrame) -> Optional[go.Figure]:
    if gap_df is None or gap_df.empty:
        return None
    d = gap_df.copy().iloc[::-1]
    d["axis_label"] = d["value"].astype(str).map(lambda value: _wrap_label(value, width=22))
    fig = go.Figure(
        go.Bar(
            x=d["gap_vs_overall"],
            y=d["axis_label"],
            orientation="h",
            marker=dict(color="#" + BBVA_COLORS["red"]),
            text=[f"{v:+.1f}" for v in d["gap_vs_overall"].tolist()],
            textposition="outside",
            cliponaxis=False,
        )
    )
    fig.add_vline(x=0, line_color="#" + BBVA_COLORS["line"], line_width=1)
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=32, r=28, t=20, b=24),
        xaxis_title="Diferencia frente al NPS medio",
        yaxis_title="",
        showlegend=False,
    )
    return fig


def _opportunity_bubble_fig(opps_df: pd.DataFrame) -> Optional[go.Figure]:
    if opps_df is None or opps_df.empty:
        return None
    d = opps_df.head(10).copy()
    d["size"] = np.clip(np.sqrt(pd.to_numeric(d["n"], errors="coerce").fillna(0.0)) * 2.4, 12, 40)
    d["plot_label"] = d.apply(
        lambda row: _wrap_label(
            _format_opportunity_scope(row.get("dimension"), row.get("value")), width=16
        ),
        axis=1,
    )
    colors = {
        "Palanca": "#" + BBVA_COLORS["blue"],
        "Subpalanca": "#" + BBVA_COLORS["orange"],
        "nps_topic": "#" + BBVA_COLORS["red"],
    }
    fig = go.Figure(
        go.Scatter(
            x=d["confidence"],
            y=d["potential_uplift"],
            mode="markers+text",
            text=d["plot_label"],
            textposition="top center",
            marker=dict(
                size=d["size"],
                color=[
                    colors.get(str(dim), "#" + BBVA_COLORS["sky"])
                    for dim in d["dimension"].tolist()
                ],
                opacity=0.82,
            ),
            hovertemplate=(
                "%{customdata[2]}<br>Dimensión=%{customdata[0]}<br>Potencial=%{y:.1f} pts"
                "<br>Confianza=%{x:.2f}<br>n=%{customdata[1]:.0f}<extra></extra>"
            ),
            customdata=d[["dimension", "n", "value"]].to_numpy(),
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=24, t=20, b=24),
        xaxis=dict(title="Solidez de la evidencia", range=[0, 1]),
        yaxis=dict(title="Impacto potencial (pts NPS)", rangemode="tozero"),
        showlegend=False,
    )
    return fig


def _causal_daily_timeline_fig(
    daily_mix: pd.DataFrame, overall_daily: pd.DataFrame
) -> Optional[go.Figure]:
    if daily_mix is None or daily_mix.empty:
        return None
    d = _merge_daily_incidents(daily_mix, overall_daily)
    if d.empty:
        return None
    tokens = DesignTokens.default()
    pal = palette(tokens, "light")
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=d["date"],
            y=d["incidents"],
            name="# incidencias",
            yaxis="y2",
            opacity=0.75,
            marker=dict(color=pal["color.primary.accent.value-01.default"]),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["detractor_rate"] * 100.0,
            mode="lines+markers",
            name="% detractores",
            line=dict(color=pal["color.primary.bg.alert"], width=2),
            marker=dict(color=pal["color.primary.bg.alert"], size=6),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["passive_rate"] * 100.0,
            mode="lines+markers",
            name="% pasivos",
            line=dict(color=pal["color.primary.bg.warning"], width=2),
            marker=dict(color=pal["color.primary.bg.warning"], size=6),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["promoter_rate"] * 100.0,
            mode="lines+markers",
            name="% promotores",
            line=dict(color=pal["color.primary.bg.success"], width=2),
            marker=dict(color=pal["color.primary.bg.success"], size=6),
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=48, t=20, b=24),
        legend=dict(orientation="h", x=0.0, y=1.08),
        xaxis_title="Día",
        yaxis=dict(title="Tasa por grupo", tickformat=".0%"),
        yaxis2=dict(
            title="Incidencias",
            overlaying="y",
            side="right",
            rangemode="tozero",
            showgrid=False,
        ),
    )
    return fig


def _journeys_overview_fig(chain_df: Optional[pd.DataFrame]) -> Optional[go.Figure]:
    if chain_df is None or chain_df.empty:
        return None
    d = chain_df.copy()
    d["impact"] = pd.to_numeric(d.get("nps_points_at_risk"), errors="coerce").fillna(0.0)
    d.loc[d["impact"] <= 0.0, "impact"] = pd.to_numeric(
        d.get("total_nps_impact"), errors="coerce"
    ).fillna(0.0)
    d["linked_pairs"] = pd.to_numeric(d.get("linked_pairs"), errors="coerce").fillna(0.0)
    d = d.sort_values(["impact", "linked_pairs"], ascending=False).head(3).iloc[::-1]
    if d.empty:
        return None
    d["axis_label"] = d["nps_topic"].astype(str).map(lambda value: _wrap_label(value, width=20))
    fig = go.Figure(
        go.Bar(
            x=d["impact"],
            y=d["axis_label"],
            orientation="h",
            marker=dict(
                color=[
                    "#" + BBVA_COLORS["orange"],
                    "#" + BBVA_COLORS["blue"],
                    "#" + BBVA_COLORS["red"],
                ][-len(d) :]
            ),
            text=[f"{v:.2f} pts" for v in d["impact"].tolist()],
            textposition="outside",
            cliponaxis=False,
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=32, r=28, t=20, b=24),
        xaxis_title="NPS en riesgo",
        yaxis_title="",
        showlegend=False,
    )
    return fig


def _patch_kaleido_executable_for_space_paths() -> None:
    """Patch kaleido executable lookup when project path contains spaces."""
    try:
        from kaleido.scopes import base as kaleido_base
    except Exception:
        return

    cls = kaleido_base.BaseScope
    if getattr(cls, "_nps_lens_kaleido_patched", False):
        return

    try:
        default_exec = str(cls.executable_path())
    except Exception:
        return

    if " " not in default_exec or os.name == "nt":
        cls._nps_lens_kaleido_patched = True
        return

    exec_path = Path(default_exec)
    exec_dir = exec_path.parent
    real_bin = exec_dir / "bin" / "kaleido"
    if not real_bin.exists():
        cls._nps_lens_kaleido_patched = True
        return

    shim_dir = Path(tempfile.gettempdir()) / "nps_lens_kaleido"
    shim_dir.mkdir(parents=True, exist_ok=True)
    shim_path = shim_dir / "kaleido-shim"
    shim_path.write_text(
        f'#!/bin/sh\ncd "{exec_dir}" || exit 1\nexec "./bin/kaleido" "$@"\n',
        encoding="utf-8",
    )
    with contextlib.suppress(Exception):
        shim_path.chmod(0o755)

    cls.executable_path = classmethod(lambda scope_cls: str(shim_path))  # type: ignore[assignment]
    cls._nps_lens_kaleido_patched = True


def _apply_ppt_figure_theme(fig: go.Figure) -> go.Figure:
    ink = "#" + BBVA_COLORS["ink"]
    grid = "#" + BBVA_COLORS["line"]
    white = "#" + BBVA_COLORS["white"]

    for trace in fig.data:
        name = str(getattr(trace, "name", "") or "").strip().lower()
        trace_type = str(getattr(trace, "type", "") or "").strip().lower()
        is_incidents = "incid" in name or "helix" in name
        is_detractor = any(token in name for token in ["detrac", "crit", "alto", "foco"])
        is_passive = any(token in name for token in ["pasiv", "moderad"])
        is_promoter = "promot" in name
        is_nps = "nps" in name

        if trace_type == "bar":
            color = None
            if is_incidents:
                color = "#" + BBVA_COLORS["sky"]
            elif is_promoter:
                color = "#" + BBVA_COLORS["green"]
            elif is_passive:
                color = "#" + BBVA_COLORS["yellow"]
            elif is_detractor or is_nps:
                color = "#" + BBVA_COLORS["red"]
            if color:
                with contextlib.suppress(Exception):
                    trace.marker.color = color
        elif trace_type == "scatter":
            if is_nps:
                with contextlib.suppress(Exception):
                    trace.line.color = "#" + BBVA_COLORS["blue"]
                if "markers" in str(getattr(trace, "mode", "") or ""):
                    with contextlib.suppress(Exception):
                        if not isinstance(
                            getattr(trace.marker, "color", None), (list, tuple, np.ndarray)
                        ):
                            trace.marker.color = "#" + BBVA_COLORS["sky"]
                    with contextlib.suppress(Exception):
                        trace.marker.size = max(8, int(getattr(trace.marker, "size", 8) or 8))
            elif is_incidents:
                with contextlib.suppress(Exception):
                    trace.line.color = "#" + BBVA_COLORS["sky"]
                with contextlib.suppress(Exception):
                    trace.marker.color = "#" + BBVA_COLORS["sky"]
            elif is_promoter:
                with contextlib.suppress(Exception):
                    trace.line.color = "#" + BBVA_COLORS["green"]
            elif is_passive:
                with contextlib.suppress(Exception):
                    trace.line.color = "#" + BBVA_COLORS["yellow"]
            elif is_detractor:
                with contextlib.suppress(Exception):
                    trace.line.color = "#" + BBVA_COLORS["red"]

    current_margin = fig.layout.margin.to_plotly_json() if fig.layout.margin else {}
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor=white,
        plot_bgcolor=white,
        font=dict(family=BBVA_FONT_BODY, size=17, color=ink),
        legend=dict(
            orientation="h",
            x=0.0,
            xanchor="left",
            y=-0.16,
            yanchor="top",
            font=dict(size=16, color=ink),
            title_font=dict(size=16, color=ink),
            bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(
            l=int(current_margin.get("l", 24)),
            r=int(current_margin.get("r", 24)),
            t=int(current_margin.get("t", 20)),
            b=max(int(current_margin.get("b", 24)), 72),
        ),
        hoverlabel=dict(font=dict(family=BBVA_FONT_BODY, size=13, color=ink)),
    )
    fig.for_each_xaxis(
        lambda axis: axis.update(
            tickfont=dict(size=16, color=ink),
            title_font=dict(size=17, color=ink),
            automargin=True,
            gridcolor=grid,
            linecolor=grid,
        )
    )
    fig.for_each_yaxis(
        lambda axis: axis.update(
            tickfont=dict(size=16, color=ink),
            title_font=dict(size=17, color=ink),
            automargin=True,
            gridcolor=grid,
            linecolor=grid,
        )
    )
    return fig


def _kaleido_png(fig: go.Figure, *, width: int = 1600, height: int = 900) -> Optional[bytes]:
    try:
        _patch_kaleido_executable_for_space_paths()
        themed = _apply_ppt_figure_theme(fig)
        return pio.to_image(themed, format="png", width=width, height=height, scale=2)
    except Exception:
        return None


def _layout_fallback_index(prs: Presentation, kind: str) -> int:
    count = len(prs.slide_layouts)
    if count <= 0:
        return 0
    if kind == "cover":
        return min(0, count - 1)
    if kind == "section":
        return min(2, count - 1)
    return min(6, count - 1)


def _new_slide(prs: Presentation, *, kind: str = "content") -> object:
    preferred = (
        [PPT_THEME.cover_layout]
        if kind == "cover"
        else ([PPT_THEME.section_layout] if kind == "section" else [PPT_THEME.content_layout])
    )
    layout = resolve_layout(
        prs,
        preferred,
        fallback_index=_layout_fallback_index(prs, kind),
    )
    slide = prs.slides.add_slide(layout)
    for shape in list(slide.shapes):
        if not getattr(shape, "is_placeholder", False):
            continue
        try:
            sp = shape._element
            sp.getparent().remove(sp)
        except Exception:
            continue
    return slide


def _add_bg(slide: object, color: str) -> None:
    shape = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.RECTANGLE, Inches(0), Inches(0), Inches(13.333), Inches(7.5)
    )
    shape.fill.solid()
    shape.fill.fore_color.rgb = _rgb(color)
    shape.line.fill.background()


def _add_header(
    slide: object, *, title: str, subtitle: str, dark: bool = False, right_note: str = ""
) -> None:
    title_color = BBVA_COLORS["white"] if dark else BBVA_COLORS["ink"]
    sub_color = "A9B7D2" if dark else BBVA_COLORS["muted"]

    box = slide.shapes.add_textbox(Inches(0.65), Inches(0.28), Inches(9.8), Inches(0.85))
    tf = box.text_frame
    _configure_text_frame(tf)
    tf.clear()
    p = tf.paragraphs[0]
    r = p.add_run()
    r.text = title
    r.font.name = BBVA_FONT_DISPLAY
    r.font.size = Pt(28)
    r.font.bold = True
    r.font.color.rgb = _rgb(title_color)

    sb = slide.shapes.add_textbox(Inches(0.65), Inches(0.95), Inches(11.5), Inches(0.42))
    stf = sb.text_frame
    _configure_text_frame(stf)
    stf.clear()
    sp = stf.paragraphs[0]
    sr = sp.add_run()
    sr.text = subtitle
    sr.font.name = BBVA_FONT_BODY
    sr.font.size = Pt(12.5)
    sr.font.color.rgb = _rgb(sub_color)

    if right_note.strip():
        rb = slide.shapes.add_textbox(Inches(10.4), Inches(0.35), Inches(2.2), Inches(0.30))
        rtf = rb.text_frame
        _configure_text_frame(rtf)
        rtf.clear()
        rp = rtf.paragraphs[0]
        rp.alignment = PP_ALIGN.RIGHT
        rr = rp.add_run()
        rr.text = right_note
        rr.font.name = BBVA_FONT_BODY
        rr.font.size = Pt(10)
        rr.font.color.rgb = _rgb(sub_color)

    line = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.RECTANGLE, Inches(0.65), Inches(1.28), Inches(12.0), Inches(0.04)
    )
    line.fill.solid()
    line.fill.fore_color.rgb = _rgb(BBVA_COLORS["sky"] if dark else BBVA_COLORS["line"])
    line.line.fill.background()


def _panel(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    height: float,
    title: str = "",
    subtitle: str = "",
    fill: str = "",
    border: str = "",
    title_size: float = 15,
) -> object:
    box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(left),
        Inches(top),
        Inches(width),
        Inches(height),
    )
    box.fill.solid()
    box.fill.fore_color.rgb = _rgb(fill or BBVA_COLORS["white"])
    box.line.color.rgb = _rgb(border or BBVA_COLORS["line"])
    if not title and not subtitle:
        return box

    tf = box.text_frame
    _configure_text_frame(tf)
    tf.clear()
    if title:
        p0 = tf.paragraphs[0]
        r0 = p0.add_run()
        r0.text = title
        r0.font.name = BBVA_FONT_HEAD
        r0.font.size = Pt(title_size)
        r0.font.bold = True
        r0.font.color.rgb = _rgb(BBVA_COLORS["ink"])
    if subtitle:
        p1 = tf.add_paragraph()
        p1.space_before = Pt(4)
        r1 = p1.add_run()
        r1.text = subtitle
        r1.font.name = BBVA_FONT_BODY
        r1.font.size = Pt(10.5)
        r1.font.color.rgb = _rgb(BBVA_COLORS["muted"])
    return box


def _figure_in_panel(
    slide: object,
    *,
    figure: Optional[go.Figure],
    left: float,
    top: float,
    width: float,
    height: float,
    empty_note: str,
) -> None:
    img = _kaleido_png(figure) if figure is not None else None
    if img is not None:
        slide.shapes.add_picture(
            BytesIO(img),
            Inches(left),
            Inches(top),
            width=Inches(width),
            height=Inches(height),
        )
        return

    box = slide.shapes.add_textbox(Inches(left), Inches(top), Inches(width), Inches(height))
    tf = box.text_frame
    _configure_text_frame(tf)
    tf.clear()
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    r = p.add_run()
    r.text = empty_note
    r.font.name = BBVA_FONT_BODY
    r.font.size = Pt(12)
    r.font.color.rgb = _rgb(BBVA_COLORS["muted"])


def _add_stat_card(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    height: float,
    label: str,
    value: str,
    accent: str,
    hint: str = "",
) -> None:
    box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(left),
        Inches(top),
        Inches(width),
        Inches(height),
    )
    box.fill.solid()
    box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    box.line.color.rgb = _rgb(BBVA_COLORS["line"])

    accent_bar = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(left),
        Inches(top),
        Inches(0.12),
        Inches(height),
    )
    accent_bar.fill.solid()
    accent_bar.fill.fore_color.rgb = _rgb(accent)
    accent_bar.line.fill.background()

    tf = box.text_frame
    tf.vertical_anchor = MSO_VERTICAL_ANCHOR.MIDDLE
    tf.clear()
    p0 = tf.paragraphs[0]
    p0.alignment = PP_ALIGN.CENTER
    r0 = p0.add_run()
    r0.text = label.upper()
    r0.font.name = BBVA_FONT_MEDIUM
    r0.font.size = Pt(10)
    r0.font.bold = True
    r0.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    p1 = tf.add_paragraph()
    p1.space_before = Pt(10)
    p1.alignment = PP_ALIGN.CENTER
    r1 = p1.add_run()
    r1.text = value
    r1.font.name = BBVA_FONT_DISPLAY
    value_len = len(str(value or ""))
    r1.font.size = Pt(24 if value_len <= 14 else (18 if value_len <= 28 else 12))
    r1.font.bold = True
    r1.font.color.rgb = _rgb(BBVA_COLORS["ink"])

    if hint:
        p2 = tf.add_paragraph()
        p2.space_before = Pt(6)
        p2.alignment = PP_ALIGN.CENTER
        r2 = p2.add_run()
        r2.text = hint
        r2.font.name = BBVA_FONT_BODY
        r2.font.size = Pt(9.5)
        r2.font.color.rgb = _rgb(BBVA_COLORS["muted"])


def _add_bullet_lines(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    height: float,
    title: str,
    lines: list[str],
    accent: str = "",
    body_font_size_pt: float = 11.0,
) -> None:
    if not str(title or "").strip():
        box = slide.shapes.add_shape(
            MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
            Inches(left),
            Inches(top),
            Inches(width),
            Inches(height),
        )
        box.fill.solid()
        box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
        box.line.color.rgb = _rgb(accent or BBVA_COLORS["line"])
        tf = box.text_frame
        _configure_text_frame(tf)
        tf.clear()
        for idx, line in enumerate(lines[:6]):
            p = tf.paragraphs[0] if idx == 0 else tf.add_paragraph()
            p.space_before = Pt(4 if idx else 0)
            p.level = 0
            r = p.add_run()
            r.text = _clip(line, 145 if width <= 4.0 else 170)
            r.font.name = BBVA_FONT_BODY
            r.font.size = Pt(body_font_size_pt)
            r.font.color.rgb = _rgb(BBVA_COLORS["muted"])
        return

    panel = _panel(
        slide,
        left=left,
        top=top,
        width=width,
        height=height,
        title=title,
        fill=BBVA_COLORS["white"],
        border=accent or BBVA_COLORS["line"],
    )
    tf = panel.text_frame
    _configure_text_frame(tf)
    for line in lines[:6]:
        p = tf.add_paragraph()
        p.space_before = Pt(6)
        p.level = 0
        r = p.add_run()
        r.text = f"• {_clip(line, 145 if width <= 4.0 else 170)}"
        r.font.name = BBVA_FONT_BODY
        r.font.size = Pt(body_font_size_pt)
        r.font.color.rgb = _rgb(BBVA_COLORS["muted"])


def _add_compact_table(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    title: str,
    headers: list[str],
    rows: list[list[str]],
    row_height: float = 0.34,
    col_width_ratios: Optional[list[float]] = None,
    clip_lengths: Optional[list[int]] = None,
    font_size_pt: float = 9.6,
) -> None:
    height = 0.62 + row_height * max(len(rows), 1) + 0.12
    panel = _panel(
        slide,
        left=left,
        top=top,
        width=width,
        height=height,
        title=title,
        fill=BBVA_COLORS["white"],
    )
    base_top = top + 0.46
    if col_width_ratios and len(col_width_ratios) == len(headers):
        ratio_sum = sum(col_width_ratios) or 1.0
        column_widths = [((width - 0.16) * ratio / ratio_sum) for ratio in col_width_ratios]
    else:
        column_widths = [(width - 0.16) / max(len(headers), 1)] * len(headers)
    x_positions: list[float] = []
    cursor = left
    for col_width in column_widths:
        x_positions.append(cursor)
        cursor += col_width

    for idx, header in enumerate(headers):
        tb = slide.shapes.add_textbox(
            Inches(x_positions[idx] + 0.05),
            Inches(base_top),
            Inches(column_widths[idx] - 0.08),
            Inches(0.20),
        )
        tf = tb.text_frame
        _configure_text_frame(tf)
        tf.clear()
        p = tf.paragraphs[0]
        r = p.add_run()
        r.text = header
        r.font.name = BBVA_FONT_MEDIUM
        r.font.size = Pt(9.5)
        r.font.bold = True
        r.font.color.rgb = _rgb(BBVA_COLORS["blue"])

    for row_idx, row in enumerate(rows[:10], start=1):
        current_top = base_top + 0.16 + row_height * row_idx
        for col_idx, value in enumerate(row[: len(headers)]):
            tb = slide.shapes.add_textbox(
                Inches(x_positions[col_idx] + 0.05),
                Inches(current_top),
                Inches(column_widths[col_idx] - 0.08),
                Inches(0.24),
            )
            tf = tb.text_frame
            _configure_text_frame(tf)
            tf.clear()
            p = tf.paragraphs[0]
            r = p.add_run()
            clip_len = (
                clip_lengths[col_idx]
                if clip_lengths is not None and col_idx < len(clip_lengths)
                else 48
            )
            r.text = _clip(value, clip_len)
            r.font.name = BBVA_FONT_BODY
            r.font.size = Pt(font_size_pt)
            r.font.color.rgb = _rgb(BBVA_COLORS["muted"] if col_idx else BBVA_COLORS["ink"])

    del panel


def _add_chart_slide(
    prs: Presentation,
    *,
    title: str,
    subtitle: str,
    figure: Optional[go.Figure],
    rationale_title: str,
    rationale_lines: Iterable[str],
) -> None:  # pragma: no cover - legacy slide kept for backwards compatibility
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(slide, title=title, subtitle=subtitle)

    chart_box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE, Inches(0.66), Inches(1.45), Inches(8.2), Inches(5.4)
    )
    chart_box.fill.solid()
    chart_box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    chart_box.line.color.rgb = _rgb(BBVA_COLORS["line"])

    img = _kaleido_png(figure) if figure is not None else None
    if img is not None:
        slide.shapes.add_picture(
            BytesIO(img), Inches(0.82), Inches(1.60), width=Inches(7.84), height=Inches(5.08)
        )
    else:
        tf = chart_box.text_frame
        tf.clear()
        p = tf.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run()
        r.text = "No hay información suficiente para este gráfico en el periodo seleccionado."
        r.font.name = BBVA_FONT_BODY
        r.font.size = Pt(13)
        r.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    side = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE, Inches(8.95), Inches(1.45), Inches(3.75), Inches(5.4)
    )
    side.fill.solid()
    side.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    side.line.color.rgb = _rgb(BBVA_COLORS["line"])

    stf = side.text_frame
    stf.clear()

    hp = stf.paragraphs[0]
    hr = hp.add_run()
    hr.text = rationale_title
    hr.font.name = BBVA_FONT_HEAD
    hr.font.bold = True
    hr.font.size = Pt(18)
    hr.font.color.rgb = _rgb(BBVA_COLORS["ink"])

    for line in list(rationale_lines)[:8]:
        p = stf.add_paragraph()
        p.level = 0
        p.space_after = Pt(7)
        p.text = f"• {_clip(line, 175)}"
        p.font.name = BBVA_FONT_BODY
        p.font.size = Pt(12)
        p.font.color.rgb = _rgb(BBVA_COLORS["muted"])


def _add_period_summary_slide(
    prs: Presentation,
    *,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    period_start: date,
    period_end: date,
) -> None:  # pragma: no cover - legacy slide kept for backwards compatibility
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])

    month_txt = _month_label_es(period_end)
    p_label = f"{_safe_date(period_start)} -> {_safe_date(period_end)}"
    cards = [
        ("SERVICE ORIGEN", _clip(service_origin or "N/D", 38), "Ámbito analizado"),
        ("NIVEL N1", _clip(service_origin_n1 or "N/D", 38), "Segmentación principal"),
        ("NIVEL N2", _clip(service_origin_n2 or "N/D", 38), "Segmentación secundaria"),
        ("MES EN CURSO", _clip(month_txt.title(), 38), f"Ventana: {p_label}"),
    ]

    left0 = 0.65
    top = 0.85
    card_w = 3.18
    gap = 0.35
    card_h = 3.05

    for i, (label, value, caption) in enumerate(cards):
        left = Inches(left0 + i * (card_w + gap))
        card = slide.shapes.add_shape(
            MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
            left,
            Inches(top),
            Inches(card_w),
            Inches(card_h),
        )
        card.fill.solid()
        card.fill.fore_color.rgb = _rgb(BBVA_COLORS["bg_light"])
        card.line.color.rgb = _rgb(BBVA_COLORS["line"])

        tf = card.text_frame
        tf.clear()

        p1 = tf.paragraphs[0]
        r1 = p1.add_run()
        r1.text = label
        r1.font.name = BBVA_FONT_MEDIUM
        r1.font.bold = True
        r1.font.size = Pt(13)
        r1.font.color.rgb = _rgb(BBVA_COLORS["ink"])

        p2 = tf.add_paragraph()
        p2.space_before = Pt(18)
        r2 = p2.add_run()
        r2.text = value
        r2.font.name = BBVA_FONT_HEAD
        r2.font.bold = True
        r2.font.size = Pt(30)
        r2.font.color.rgb = _rgb(BBVA_COLORS["ink"])

        p3 = tf.add_paragraph()
        p3.space_before = Pt(8)
        r3 = p3.add_run()
        r3.text = caption
        r3.font.name = BBVA_FONT_BODY
        r3.font.size = Pt(12)
        r3.font.color.rgb = _rgb(BBVA_COLORS["muted"])


def _plain_md(text: object) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    s = re.sub(r"`([^`]*)`", r"\1", s)
    s = re.sub(r"\*\*([^*]+)\*\*", r"\1", s)
    s = re.sub(r"^#+\s*", "", s)
    return " ".join(s.split())


def _parse_story_sections(story_md: str) -> list[tuple[str, list[str]]]:
    sections: list[tuple[str, list[str]]] = []
    title = ""
    bullets: list[str] = []
    for raw in str(story_md or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("## "):
            if title:
                sections.append((title, bullets))
            title = _plain_md(line[3:])
            bullets = []
            continue
        if line.startswith("# "):
            continue
        if line.startswith("- "):
            bullets.append(_plain_md(line[2:]))
            continue
        if title:
            bullets.append(_plain_md(line))
    if title:
        sections.append((title, bullets))
    return sections


def _cover_summary_lines(overview: dict[str, object], story_md: str) -> list[str]:
    lines: list[str] = []
    pain_point = str(overview.get("pain_point", "") or "").strip()
    strength_point = str(overview.get("strength_point", "") or "").strip()
    classic_delta = _safe_float(overview.get("classic_delta"), default=float("nan"))
    detractor_delta_pp = _safe_float(overview.get("detractor_delta_pp"), default=float("nan"))

    if pain_point:
        lines.append(f"La mayor fricción del periodo se concentra en {pain_point}.")
    if strength_point and strength_point != pain_point:
        lines.append(f"La mejor señal de experiencia se observa en {strength_point}.")
    if np.isfinite(classic_delta) and abs(classic_delta) >= 0.1:
        direction = "sube" if classic_delta > 0 else "cae"
        lines.append(
            f"Del inicio al cierre del periodo, el NPS clásico {direction} {abs(classic_delta):.1f} puntos."
        )
    if np.isfinite(detractor_delta_pp) and abs(detractor_delta_pp) >= 0.1:
        direction = "sube" if detractor_delta_pp > 0 else "baja"
        lines.append(
            f"El peso detractor {direction} {abs(detractor_delta_pp):.1f} puntos porcentuales en la ventana analizada."
        )

    for _, bullets in _parse_story_sections(story_md):
        for bullet in bullets:
            clean = str(bullet or "").strip()
            if not clean or _is_cover_metric_line(clean):
                continue
            if clean.lower().startswith(
                (
                    "zona de fricción",
                    "zona de friccion",
                    "zona fuerte",
                    "periodo actual",
                    "periodo base",
                )
            ):
                continue
            clean = (
                clean.replace("VoC", "comentarios de cliente")
                .replace("quick wins", "acciones rápidas")
                .replace("owners", "equipos responsables")
                .replace("owner", "equipo responsable")
                .replace("Si mejoramos Palanca=", "La oportunidad más clara está en ")
                .replace("Si mejoramos Subpalanca=", "La oportunidad más clara está en ")
                .replace("el modelo estima un potencial de", "con un impacto potencial de")
            )
            if clean not in lines:
                lines.append(clean)
            if len(lines) >= 4:
                return lines[:4]
    return lines[:4]


def _add_story_card(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    height: float,
    title: str,
    bullets: list[str],
    fill: str = "",
) -> None:
    box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(left),
        Inches(top),
        Inches(width),
        Inches(height),
    )
    box.fill.solid()
    box.fill.fore_color.rgb = _rgb(fill or BBVA_COLORS["white"])
    box.line.color.rgb = _rgb(BBVA_COLORS["line"])

    tf = box.text_frame
    tf.clear()

    p0 = tf.paragraphs[0]
    r0 = p0.add_run()
    r0.text = title
    r0.font.name = BBVA_FONT_HEAD
    r0.font.bold = True
    r0.font.size = Pt(18)
    r0.font.color.rgb = _rgb(BBVA_COLORS["ink"])

    max_bullets = 3 if height <= 1.7 else 4
    for bullet in bullets[:max_bullets]:
        p = tf.add_paragraph()
        p.space_before = Pt(8)
        p.space_after = Pt(0)
        p.level = 0
        r = p.add_run()
        r.text = f"• {_clip(bullet, 150 if width > 5.5 else 120)}"
        r.font.name = BBVA_FONT_BODY
        r.font.size = Pt(12)
        r.font.color.rgb = _rgb(BBVA_COLORS["muted"])


def _add_business_story_slide(
    prs: Presentation,
    *,
    story_md: str,
    period_label: str,
) -> None:  # pragma: no cover - legacy slide kept for backwards compatibility
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="Informe de negocio",
        subtitle="Lectura ejecutiva del mes seleccionado frente al histórico anterior, lista para comité.",
        right_note="Slide 2",
    )

    banner = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(0.70),
        Inches(1.52),
        Inches(11.95),
        Inches(0.55),
    )
    banner.fill.solid()
    banner.fill.fore_color.rgb = _rgb(BBVA_COLORS["ink"])
    banner.line.fill.background()
    btf = banner.text_frame
    btf.clear()
    bp = btf.paragraphs[0]
    br = bp.add_run()
    br.text = f"Periodo analizado: {period_label}"
    br.font.name = BBVA_FONT_MEDIUM
    br.font.bold = True
    br.font.size = Pt(13)
    br.font.color.rgb = _rgb(BBVA_COLORS["white"])

    sections = _parse_story_sections(story_md)
    section_map = {title: bullets for title, bullets in sections}
    layout = [
        ("1) Qué está pasando", 0.70, 2.22, 5.8, 1.75, BBVA_COLORS["white"]),
        ("2) Cambio vs base de comparación", 6.63, 2.22, 5.8, 1.75, BBVA_COLORS["white"]),
        ("3) Dónde atacar primero (oportunidades)", 0.70, 4.10, 5.8, 1.95, BBVA_COLORS["white"]),
        ("4) Qué están diciendo (temas de texto)", 6.63, 4.10, 5.8, 1.95, BBVA_COLORS["white"]),
    ]
    for title, left, top, width, height, fill in layout:
        _add_story_card(
            slide,
            left=left,
            top=top,
            width=width,
            height=height,
            title=title,
            bullets=section_map.get(title, ["Sin contenido disponible para esta sección."]),
            fill=fill,
        )

    _add_story_card(
        slide,
        left=0.70,
        top=6.20,
        width=11.73,
        height=0.78,
        title="5) Próximos pasos recomendados",
        bullets=section_map.get(
            "5) Próximos pasos recomendados",
            ["Validar releases, definir hipótesis y aterrizar el plan de acción del mes."],
        ),
        fill=BBVA_COLORS["bg_light"],
    )


def _add_impact_chain_slide(
    prs: Presentation,
    *,
    cards: list[object],
    focus_name: str,
    period_label: str,
    presentation_mode: str = "",
    executive_journey_catalog: Optional[list[dict[str, object]]] = None,
) -> None:  # pragma: no cover - legacy slide kept for backwards compatibility
    if str(presentation_mode or "").strip() == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS:
        _add_executive_journey_summary_slide(
            prs,
            cards=cards,
            focus_name=focus_name,
            period_label=period_label,
            executive_journey_catalog=executive_journey_catalog,
        )
        return

    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    is_broken_journey_mode = (
        str(presentation_mode or "").strip() == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS
    )
    _add_header(
        slide,
        title="Marco causal",
        subtitle=(
            "Cómo se atribuye el impacto: incidencia -> embeddings / clustering -> journey roto -> VoC -> NPS "
            f"· periodo {period_label}"
            if is_broken_journey_mode
            else f"Cómo se atribuye el impacto: incidencia -> touchpoint -> VoC -> NPS · periodo {period_label}"
        ),
    )
    steps = (
        [
            ("1. Incidencia", "Helix aporta el INC y la descripción ampliada del fallo real."),
            (
                "2. Embeddings + keywords",
                "La app agrupa señales semánticas similares sin depender de una tabla manual de journeys.",
            ),
            (
                "3. Journey roto",
                "Cada cluster se convierte en un touchpoint roto defendible con su palanca dominante.",
            ),
            (
                "4. Comentario VoC",
                "Se muestran verbatims reales enlazados con el cluster, no frases genéricas ni heurísticas aisladas.",
            ),
            (
                "5. NPS",
                f"El efecto final se expresa en riesgo de {focus_name}, delta NPS e impacto total.",
            ),
        ]
        if is_broken_journey_mode
        else [
            ("1. Incidencia", "Helix aporta el INC y la descripción ampliada del fallo real."),
            (
                "2. Touchpoint",
                "Se identifica el momento del journey afectado, no solo el sistema técnico.",
            ),
            (
                "3. Palanca / subpalanca",
                "La fricción se traduce al lenguaje NPS con el mismo topic usado en la app.",
            ),
            (
                "4. Comentario VoC",
                "Se muestran verbatims reales enlazados con el caso Helix, no frases genéricas.",
            ),
            (
                "5. NPS",
                f"El efecto final se expresa en riesgo de {focus_name}, delta NPS e impacto total.",
            ),
        ]
    )
    left = 0.80
    top = 1.70
    width = 11.7
    gap = 0.15
    box_h = 0.78
    for idx, (title, body) in enumerate(steps):
        box = slide.shapes.add_shape(
            MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
            Inches(left),
            Inches(top + idx * (box_h + gap)),
            Inches(width),
            Inches(box_h),
        )
        box.fill.solid()
        box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
        box.line.color.rgb = _rgb(BBVA_COLORS["line"])
        tf = box.text_frame
        tf.clear()
        p1 = tf.paragraphs[0]
        r1 = p1.add_run()
        r1.text = title
        r1.font.name = BBVA_FONT_MEDIUM
        r1.font.bold = True
        r1.font.size = Pt(14)
        r1.font.color.rgb = _rgb(BBVA_COLORS["blue"])
        p2 = tf.add_paragraph()
        r2 = p2.add_run()
        r2.text = body
        r2.font.name = BBVA_FONT_BODY
        r2.font.size = Pt(11.5)
        r2.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    note = slide.shapes.add_textbox(Inches(0.90), Inches(6.45), Inches(11.4), Inches(0.55))
    ntf = note.text_frame
    ntf.clear()
    np = ntf.paragraphs[0]
    nr = np.add_run()
    nr.text = (
        "Solo se presentan temas con link explícito entre Helix y VoC. "
        "Se excluyen etiquetas genéricas sin comentario defendible, como 'Sin comentarios'."
    )
    nr.font.name = BBVA_FONT_BODY
    nr.font.size = Pt(11.5)
    nr.font.color.rgb = _rgb(BBVA_COLORS["muted"])


def _add_executive_journey_summary_slide(
    prs: Presentation,
    *,
    cards: list[object],
    focus_name: str,
    period_label: str,
    executive_journey_catalog: Optional[list[dict[str, object]]] = None,
) -> None:  # pragma: no cover - legacy slide kept for backwards compatibility
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    focus_label = (
        "detracción" if str(focus_name).strip().lower() == "detractores" else str(focus_name)
    )
    _add_header(
        slide,
        title=f"NPS Lens — Journeys que explican la {focus_label}",
        subtitle=f"Resumen ejecutivo 1 página · periodo {period_label}",
    )

    objective = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(0.72),
        Inches(1.42),
        Inches(12.0),
        Inches(1.08),
    )
    objective.fill.solid()
    objective.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    objective.line.color.rgb = _rgb(BBVA_COLORS["line"])
    otf = objective.text_frame
    otf.clear()
    op = otf.paragraphs[0]
    or1 = op.add_run()
    or1.text = "Objetivo"
    or1.font.name = BBVA_FONT_HEAD
    or1.font.bold = True
    or1.font.size = Pt(16)
    or1.font.color.rgb = _rgb(BBVA_COLORS["ink"])
    op2 = otf.add_paragraph()
    or2 = op2.add_run()
    or2.text = (
        "Identificar rutas de degradación de experiencia que conectan señales operativas "
        "(incidencias) con la voz del cliente (NPS) para priorizar causas raíz accionables."
    )
    or2.font.name = BBVA_FONT_BODY
    or2.font.size = Pt(11.5)
    or2.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    table_box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(0.72),
        Inches(2.72),
        Inches(12.0),
        Inches(2.42),
    )
    table_box.fill.solid()
    table_box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    table_box.line.color.rgb = _rgb(BBVA_COLORS["line"])

    catalog_rows = list(executive_journey_catalog or EXECUTIVE_JOURNEY_CATALOG)
    row_y = 2.92
    cols = [0.92, 3.15, 6.45, 10.15]
    headers = ["Journey", "Qué ocurre", "Evidencia esperada", "Impacto en NPS"]
    for idx, header in enumerate(headers):
        tb = slide.shapes.add_textbox(Inches(cols[idx]), Inches(row_y), Inches(2.2), Inches(0.26))
        ttf = tb.text_frame
        ttf.clear()
        p = ttf.paragraphs[0]
        r = p.add_run()
        r.text = header
        r.font.name = BBVA_FONT_MEDIUM
        r.font.bold = True
        r.font.size = Pt(11.5)
        r.font.color.rgb = _rgb(BBVA_COLORS["blue"])

    for row_idx, journey in enumerate(catalog_rows[:3], start=1):
        card = next(
            (
                item
                for item in cards
                if str(item.get("nps_topic", "") if isinstance(item, dict) else "")
                == str(journey["title"])
            ),
            None,
        )
        current_y = row_y + 0.32 + (row_idx - 1) * 0.58
        values = [
            f"{row_idx}. {journey['title']}",
            str(journey["what_occurs"]),
            str(journey["expected_evidence"]),
            str(journey["impact_label"]),
        ]
        if isinstance(card, dict):
            impact_override = str(card.get("journey_impact_label", "")).strip()
            if impact_override:
                values[3] = impact_override
        widths = [2.0, 3.05, 3.45, 1.55]
        for idx, (value, width) in enumerate(zip(values, widths)):
            tb = slide.shapes.add_textbox(
                Inches(cols[idx]),
                Inches(current_y),
                Inches(width),
                Inches(0.42),
            )
            ttf = tb.text_frame
            ttf.clear()
            p = ttf.paragraphs[0]
            r = p.add_run()
            r.text = _clip(value, 84)
            r.font.name = BBVA_FONT_BODY
            r.font.size = Pt(10.8)
            r.font.color.rgb = _rgb(BBVA_COLORS["ink"] if idx == 0 else BBVA_COLORS["muted"])

    left_box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(0.72),
        Inches(5.42),
        Inches(5.85),
        Inches(1.25),
    )
    left_box.fill.solid()
    left_box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    left_box.line.color.rgb = _rgb(BBVA_COLORS["line"])
    ltf = left_box.text_frame
    ltf.clear()
    lp = ltf.paragraphs[0]
    lr = lp.add_run()
    lr.text = "Valor diferencial de NPS Lens"
    lr.font.name = BBVA_FONT_HEAD
    lr.font.bold = True
    lr.font.size = Pt(16)
    lr.font.color.rgb = _rgb(BBVA_COLORS["ink"])
    for line in [
        "Temas mencionados por clientes -> Journeys de caída de experiencia",
        "Comentarios aislados -> Conexión con incidencias operativas",
        "Insights descriptivos -> Hipótesis causales accionables",
    ]:
        p = ltf.add_paragraph()
        p.text = f"• {_clip(line, 85)}"
        p.font.name = BBVA_FONT_BODY
        p.font.size = Pt(10.8)
        p.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    right_box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(6.85),
        Inches(5.42),
        Inches(5.87),
        Inches(1.25),
    )
    right_box.fill.solid()
    right_box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    right_box.line.color.rgb = _rgb(BBVA_COLORS["line"])
    rtf = right_box.text_frame
    rtf.clear()
    rp = rtf.paragraphs[0]
    rr = rp.add_run()
    rr.text = "Resultado esperado"
    rr.font.name = BBVA_FONT_HEAD
    rr.font.bold = True
    rr.font.size = Pt(16)
    rr.font.color.rgb = _rgb(BBVA_COLORS["ink"])
    for line in [
        "Dónde se rompe la experiencia",
        "Qué incidencias lo provocan",
        f"Cuántos {focus_name} genera",
        "Qué acciones priorizar",
    ]:
        p = rtf.add_paragraph()
        p.text = f"• {line}"
        p.font.name = BBVA_FONT_BODY
        p.font.size = Pt(10.8)
        p.font.color.rgb = _rgb(BBVA_COLORS["muted"])


def _chain_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if value is None:
        return []
    txt = str(value).strip()
    return [txt] if txt else []


def _chain_header(label: str, shown: int, total: int) -> str:
    if shown < total:
        return f"{label} ({shown} de {total})"
    return f"{label} ({shown})"


def _chain_incident_records(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        incident_id = str(entry.get("incident_id", "") or "").strip()
        summary = str(entry.get("summary", "") or "").strip()
        url = str(entry.get("url", "") or "").strip()
        if incident_id or summary:
            out.append(
                {
                    "incident_id": incident_id,
                    "summary": summary,
                    "url": url,
                }
            )
    return out


def _chain_priority_summary(  # pragma: no cover - legacy helper for compatibility mode
    chain_row: pd.Series, *, focus_name: str
) -> list[str]:
    owner = str(chain_row.get("owner_role", "") or "").strip()
    lane = str(chain_row.get("action_lane", "") or "").strip()
    eta_weeks = _safe_float(chain_row.get("eta_weeks", np.nan), default=np.nan)
    responses = _safe_float(chain_row.get("responses", np.nan), default=np.nan)
    incidents = _safe_float(chain_row.get("incidents", np.nan), default=np.nan)
    incident_rate = _safe_float(
        chain_row.get("incident_rate_per_100_responses", np.nan),
        default=np.nan,
    )
    delta_focus = _safe_float(chain_row.get("delta_focus_rate_pp", np.nan), default=np.nan)
    risk = _safe_float(chain_row.get("nps_points_at_risk", np.nan), default=np.nan)
    recoverable = _safe_float(chain_row.get("nps_points_recoverable", np.nan), default=np.nan)
    priority = _safe_float(chain_row.get("priority", np.nan), default=np.nan)
    confidence = _safe_float(chain_row.get("confidence", np.nan), default=np.nan)
    parts_top = [
        f"Prioridad {_fmt_num_or_nd(priority)}",
        f"Confianza {_fmt_num_or_nd(confidence)}",
        f"NPS en riesgo {_fmt_num_or_nd(risk)} pts",
        f"NPS recuperable {_fmt_num_or_nd(recoverable)} pts",
    ]
    parts_bottom = [
        f"Delta % {focus_name} {_fmt_signed_or_nd(delta_focus)} pp",
        f"Incidencias/100 resp. {_fmt_num_or_nd(incident_rate)}",
        f"Incidencias {_fmt_num_or_nd(incidents, decimals=0)}",
        f"Respuestas {_fmt_num_or_nd(responses, decimals=0)}",
    ]
    if lane:
        parts_bottom.append(f"Lane {lane}")
    if owner:
        parts_bottom.append(f"Owner {owner}")
    if np.isfinite(eta_weeks):
        parts_bottom.append(f"ETA {eta_weeks:.1f} semanas")
    return [
        " · ".join(parts_top),
        " · ".join(parts_bottom),
    ]


def _add_chain_evidence_slide(
    prs: Presentation,
    *,
    chain_row: pd.Series,
    idx: int,
    focus_name: str,
    period_label: str,
) -> None:  # pragma: no cover - legacy slide kept for backwards compatibility
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])

    topic = _clip(chain_row.get("nps_topic", "Tema sin etiqueta"), 72)
    touchpoint = _clip(chain_row.get("touchpoint", "Touchpoint sin etiqueta"), 42)
    palanca = _clip(chain_row.get("palanca", "n/d"), 30)
    subpalanca = _clip(chain_row.get("subpalanca", "n/d"), 36)
    presentation_mode = str(chain_row.get("presentation_mode", "") or "").strip()
    linked_incidents = int(_safe_int(chain_row.get("linked_incidents", 0), default=0))
    linked_comments = int(_safe_int(chain_row.get("linked_comments", 0), default=0))
    helix_records = _chain_incident_records(chain_row.get("incident_records"))[:5]
    helix_lines = (
        [
            str(rec.get("summary", "")).strip()
            for rec in helix_records
            if str(rec.get("summary", "")).strip()
        ]
        if helix_records
        else _chain_list(chain_row.get("incident_examples"))[:5]
    )
    voc_lines = _chain_list(chain_row.get("comment_examples"))[:2]
    shown_incidents = len(helix_lines)
    shown_comments = len(voc_lines)
    header_title = (
        f"Journey {idx}: {topic}"
        if presentation_mode == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS
        else (
            f"Journey roto {idx}: {topic}"
            if presentation_mode == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS
            else f"Tema prioritario {idx}: {touchpoint}"
        )
    )
    _add_header(
        slide,
        title=header_title,
        subtitle=f"{topic} · periodo {period_label}",
    )

    flow_y = 1.55
    step_w = 2.32
    step_gap = 0.18
    step_titles = [
        f"({shown_incidents}) Incidencias",
        touchpoint,
        f"{palanca} / {subpalanca}",
        f"({shown_comments}) Comentarios VoC",
        "NPS",
    ]
    step_colors = ["sky", "blue", "green", "orange", "red"]
    for s_idx, (label, color_key) in enumerate(zip(step_titles, step_colors)):
        x = 0.70 + s_idx * (step_w + step_gap)
        box = slide.shapes.add_shape(
            MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
            Inches(x),
            Inches(flow_y),
            Inches(step_w),
            Inches(0.72),
        )
        box.fill.solid()
        box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
        box.line.color.rgb = _rgb(BBVA_COLORS[color_key])
        tf = box.text_frame
        tf.clear()
        p = tf.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run()
        r.text = label
        r.font.name = BBVA_FONT_MEDIUM
        r.font.size = Pt(12)
        r.font.bold = True
        r.font.color.rgb = _rgb(BBVA_COLORS["ink"])
        if s_idx < len(step_titles) - 1:
            arr = slide.shapes.add_textbox(
                Inches(x + step_w),
                Inches(flow_y + 0.18),
                Inches(step_gap),
                Inches(0.30),
            )
            atf = arr.text_frame
            atf.clear()
            ap = atf.paragraphs[0]
            ap.alignment = PP_ALIGN.CENTER
            ar = ap.add_run()
            ar.text = "→"
            ar.font.name = BBVA_FONT_HEAD
            ar.font.size = Pt(16)
            ar.font.bold = True
            ar.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    metric_box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(0.70),
        Inches(2.45),
        Inches(12.0),
        Inches(0.88),
    )
    metric_box.fill.solid()
    metric_box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    metric_box.line.color.rgb = _rgb(BBVA_COLORS["line"])
    mtf = metric_box.text_frame
    mtf.clear()
    metrics = [
        f"Prob. {focus_name}: {_fmt_pct_or_nd(chain_row.get('detractor_probability', np.nan))}",
        f"Delta NPS: {_fmt_signed_or_nd(chain_row.get('nps_delta_expected', np.nan))}",
        f"Impacto total: {_fmt_num_or_nd(chain_row.get('total_nps_impact', 0.0))} pts",
        f"Links validados: {int(_safe_int(chain_row.get('linked_pairs', 0), default=0))}",
        f"Confianza: {_fmt_num_or_nd(chain_row.get('confidence', 0.0))}",
    ]
    for m_idx, metric in enumerate(metrics):
        p = mtf.paragraphs[0] if m_idx == 0 else mtf.add_paragraph()
        p.level = 0
        r = p.add_run()
        r.text = metric
        r.font.name = BBVA_FONT_BODY
        r.font.size = Pt(11)
        r.font.color.rgb = _rgb(BBVA_COLORS["ink"])

    helix_box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(0.70),
        Inches(3.55),
        Inches(5.85),
        Inches(2.45),
    )
    helix_box.fill.solid()
    helix_box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    helix_box.line.color.rgb = _rgb(BBVA_COLORS["line"])
    htf = helix_box.text_frame
    htf.clear()
    hp = htf.paragraphs[0]
    hr = hp.add_run()
    hr.text = _chain_header("Evidencia Helix", shown_incidents, linked_incidents)
    hr.font.name = BBVA_FONT_HEAD
    hr.font.size = Pt(18)
    hr.font.bold = True
    hr.font.color.rgb = _rgb(BBVA_COLORS["ink"])
    if not helix_lines:
        helix_lines = ["No hay suficiente evidencia Helix validada para elevar otro caso."]
    for idx_line, line in enumerate(helix_lines):
        p = htf.add_paragraph()
        p.font.name = BBVA_FONT_BODY
        p.font.size = Pt(11.5)
        p.font.color.rgb = _rgb(BBVA_COLORS["muted"])
        if idx_line < len(helix_records):
            rec = helix_records[idx_line]
            rec_id = str(rec.get("incident_id", "")).strip()
            rec_summary = _clip(str(rec.get("summary", "")).strip(), 150)
            rec_url = str(rec.get("url", "")).strip()
            bullet = p.add_run()
            bullet.text = "• "
            bullet.font.name = BBVA_FONT_BODY
            bullet.font.size = Pt(11.5)
            bullet.font.color.rgb = _rgb(BBVA_COLORS["muted"])
            if rec_id:
                id_run = p.add_run()
                id_run.text = rec_id
                id_run.font.name = BBVA_FONT_MEDIUM
                id_run.font.size = Pt(11.5)
                id_run.font.bold = True
                id_run.font.color.rgb = _rgb(BBVA_COLORS["blue"])
                if rec_url.startswith(("http://", "https://", "file://")):
                    id_run.hyperlink.address = rec_url
            if rec_summary:
                sep_run = p.add_run()
                sep_run.text = " · " if rec_id else ""
                sep_run.font.name = BBVA_FONT_BODY
                sep_run.font.size = Pt(11.5)
                sep_run.font.color.rgb = _rgb(BBVA_COLORS["muted"])
                text_run = p.add_run()
                text_run.text = rec_summary
                text_run.font.name = BBVA_FONT_BODY
                text_run.font.size = Pt(11.5)
                text_run.font.color.rgb = _rgb(BBVA_COLORS["muted"])
        else:
            p.text = f"• {_clip(line, 170)}"

    voc_box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(6.82),
        Inches(3.55),
        Inches(5.88),
        Inches(2.45),
    )
    voc_box.fill.solid()
    voc_box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    voc_box.line.color.rgb = _rgb(BBVA_COLORS["line"])
    vtf = voc_box.text_frame
    vtf.clear()
    vp = vtf.paragraphs[0]
    vr = vp.add_run()
    vr.text = _chain_header("Evidencia Voz del Cliente", shown_comments, linked_comments)
    vr.font.name = BBVA_FONT_HEAD
    vr.font.size = Pt(18)
    vr.font.bold = True
    vr.font.color.rgb = _rgb(BBVA_COLORS["ink"])
    if not voc_lines:
        voc_lines = [
            "No hay suficiente evidencia VoC enlazada para construir otro relato defendible."
        ]
    for line in voc_lines:
        p = vtf.add_paragraph()
        p.text = f"• {_clip(line, 170)}"
        p.font.name = BBVA_FONT_BODY
        p.font.size = Pt(11.5)
        p.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    footer_box = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(0.70),
        Inches(6.18),
        Inches(12.0),
        Inches(0.84),
    )
    footer_box.fill.solid()
    footer_box.fill.fore_color.rgb = _rgb(BBVA_COLORS["white"])
    footer_box.line.color.rgb = _rgb(BBVA_COLORS["line"])
    ftf = footer_box.text_frame
    ftf.clear()
    fp = ftf.paragraphs[0]
    fr = fp.add_run()
    fr.text = "Priorización del tema"
    fr.font.name = BBVA_FONT_HEAD
    fr.font.size = Pt(13)
    fr.font.bold = True
    fr.font.color.rgb = _rgb(BBVA_COLORS["ink"])
    for line in _chain_priority_summary(chain_row, focus_name=focus_name):
        p = ftf.add_paragraph()
        p.text = _clip(line, 175)
        p.font.name = BBVA_FONT_BODY
        p.font.size = Pt(10.6)
        p.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    concl = slide.shapes.add_textbox(Inches(0.78), Inches(7.04), Inches(11.8), Inches(0.22))
    ctf = concl.text_frame
    ctf.clear()
    cp = ctf.paragraphs[0]
    cr = cp.add_run()
    cr.text = _clip(chain_row.get("chain_story", ""), 170)
    cr.font.name = BBVA_FONT_BODY
    cr.font.size = Pt(9.8)
    cr.font.color.rgb = _rgb(BBVA_COLORS["muted"])


def _pick_first_col(df: pd.DataFrame, candidates: list[str]) -> str:
    lower_map = {str(c).strip().lower(): str(c) for c in df.columns}
    for c in candidates:
        hit = lower_map.get(str(c).strip().lower())
        if hit:
            return hit
    return ""


def _prepare_daily_signals(
    overall: pd.DataFrame,
    *,
    period_start: Optional[date],
    period_end: Optional[date],
) -> tuple[pd.DataFrame, bool]:
    """Normalize timeline to daily grain with NPS mean, detractor share and incidents."""
    if overall is None or overall.empty:
        return pd.DataFrame(columns=["date", "nps_mean", "detractor_rate", "incidents"]), False

    d = overall.copy()
    time_col = "date" if "date" in d.columns else ("week" if "week" in d.columns else "")
    if not time_col:
        return pd.DataFrame(columns=["date", "nps_mean", "detractor_rate", "incidents"]), False

    d[time_col] = _coerce_datetime_series(d[time_col])
    d = d.dropna(subset=[time_col]).copy()
    if d.empty:
        return pd.DataFrame(columns=["date", "nps_mean", "detractor_rate", "incidents"]), False

    if time_col == "week":
        d["date"] = d[time_col].dt.normalize()
    else:
        d["date"] = d[time_col].dt.normalize()

    inc_col = _pick_first_col(d, ["incidents", "incidencias", "incident_count"])
    focus_col = _pick_first_col(d, ["focus_rate", "detractor_rate", "rate_detractors"])
    nps_col = _pick_first_col(
        d,
        [
            "nps_mean",
            "nps_avg",
            "nps_media",
            "nps",
            "nps_score",
            "score_mean",
            "nps_current",
        ],
    )
    responses_col = _pick_first_col(d, ["responses", "respuestas", "n"])

    d["incidents"] = pd.to_numeric(d.get(inc_col, 0.0), errors="coerce").fillna(0.0).clip(lower=0.0)
    d["detractor_rate"] = (
        pd.to_numeric(d.get(focus_col, np.nan), errors="coerce").fillna(0.0).clip(0.0, 1.0)
    )

    nps_estimated = False
    if nps_col:
        d["nps_mean"] = pd.to_numeric(d[nps_col], errors="coerce")
    else:
        # Fallback when daily mean NPS is not available in aggregates.
        d["nps_mean"] = (1.0 - d["detractor_rate"]) * 10.0
        nps_estimated = True

    if responses_col:
        d["responses"] = (
            pd.to_numeric(d[responses_col], errors="coerce").fillna(0.0).clip(lower=0.0)
        )
    else:
        d["responses"] = 1.0

    if period_start is not None:
        d = d[d["date"] >= pd.Timestamp(period_start)]
    if period_end is not None:
        d = d[d["date"] <= pd.Timestamp(period_end)]
    if d.empty:
        return (
            pd.DataFrame(columns=["date", "nps_mean", "detractor_rate", "incidents"]),
            nps_estimated,
        )

    d["_w"] = np.maximum(pd.to_numeric(d["responses"], errors="coerce").fillna(0.0), 1.0)
    d["_nps_w"] = pd.to_numeric(d["nps_mean"], errors="coerce").fillna(0.0) * d["_w"]
    d["_det_w"] = pd.to_numeric(d["detractor_rate"], errors="coerce").fillna(0.0) * d["_w"]
    agg = (
        d.groupby("date", as_index=False)
        .agg(
            incidents=("incidents", "sum"),
            responses=("responses", "sum"),
            _w=("_w", "sum"),
            _nps_w=("_nps_w", "sum"),
            _det_w=("_det_w", "sum"),
        )
        .sort_values("date")
    )
    agg["nps_mean"] = agg["_nps_w"] / agg["_w"].replace({0.0: np.nan})
    agg["detractor_rate"] = agg["_det_w"] / agg["_w"].replace({0.0: np.nan})
    agg = agg.drop(columns=["_w", "_nps_w", "_det_w"])

    start_d = pd.Timestamp(period_start) if period_start is not None else agg["date"].min()
    end_d = pd.Timestamp(period_end) if period_end is not None else agg["date"].max()
    if pd.isna(start_d) or pd.isna(end_d) or start_d > end_d:
        return (
            pd.DataFrame(columns=["date", "nps_mean", "detractor_rate", "incidents"]),
            nps_estimated,
        )

    idx = pd.date_range(start=start_d.normalize(), end=end_d.normalize(), freq="D")
    out = agg.set_index("date").reindex(idx).rename_axis("date").reset_index()
    out["incidents"] = pd.to_numeric(out["incidents"], errors="coerce").fillna(0.0).clip(lower=0.0)

    for c in ["nps_mean", "detractor_rate"]:
        vals = pd.to_numeric(out[c], errors="coerce")
        if int(vals.notna().sum()) >= 2:
            vals = vals.interpolate(limit_direction="both")
        elif int(vals.notna().sum()) == 1:
            vals = vals.fillna(float(vals.dropna().iloc[0]))
        else:
            vals = vals.fillna(0.0)
        out[c] = vals

    out["nps_mean"] = out["nps_mean"].clip(0.0, 10.0)
    out["detractor_rate"] = out["detractor_rate"].clip(0.0, 1.0)
    return out[["date", "nps_mean", "detractor_rate", "incidents"]].copy(), nps_estimated


def _history_fig(  # pragma: no cover - legacy chart kept for backwards compatibility
    daily: pd.DataFrame, *, focus_name: str
) -> Optional[go.Figure]:
    if daily is None or daily.empty:
        return None
    d = daily.sort_values("date").copy()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["nps_mean"],
            name="NPS medio",
            mode="lines+markers",
            line=dict(color="#" + BBVA_COLORS["blue"], width=3.2),
            marker=dict(
                size=8,
                color=_ppt_nps_marker_colors(d["nps_mean"]),
                line=dict(color="#" + BBVA_COLORS["bg_light"], width=1),
            ),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["detractor_rate"] * 100.0,
            name=f"Opiniones {focus_name}",
            mode="lines",
            yaxis="y2",
            line=dict(color="#" + BBVA_COLORS["red"], width=2.6),
        )
    )
    fig.add_trace(
        go.Bar(
            x=d["date"],
            y=d["incidents"],
            name="Incidencias",
            yaxis="y3",
            marker=dict(color="#" + BBVA_COLORS["yellow"]),
            opacity=0.78,
        )
    )

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=84, t=22, b=24),
        legend=dict(orientation="h", x=0.01, y=1.08),
        xaxis=dict(title="Día"),
        yaxis=dict(title="NPS medio (0-10)", range=[0, 10]),
        yaxis2=dict(
            title=f"% {focus_name}",
            overlaying="y",
            side="right",
            range=[0, 100],
            showgrid=False,
        ),
        yaxis3=dict(
            title="Incidencias",
            overlaying="y",
            side="right",
            anchor="free",
            position=0.92,
            showgrid=False,
            rangemode="tozero",
        ),
    )
    return fig


def _hotspot_daily_breakdown(
    daily_signals: pd.DataFrame,
    incident_evidence_df: Optional[pd.DataFrame],
    incident_timeline_df: Optional[pd.DataFrame] = None,
) -> tuple[pd.DataFrame, dict[int, str]]:
    return build_hotspot_daily_breakdown_metrics(
        daily_signals,
        incident_evidence_df,
        incident_timeline_df,
        max_hotspots=3,
    )


def _hotspot_stack_fig(
    daily_signals: pd.DataFrame,
    incident_evidence_df: Optional[pd.DataFrame],
    incident_timeline_df: Optional[pd.DataFrame] = None,
) -> Optional[go.Figure]:  # pragma: no cover - legacy chart kept for backwards compatibility
    d, term_by_rank = _hotspot_daily_breakdown(
        daily_signals, incident_evidence_df, incident_timeline_df
    )
    if d.empty:
        return None

    n_lbl = "No hotspot"
    h1_lbl = (
        f"Hotspot 1: {term_by_rank.get(1, '')}".strip(": ")
        if term_by_rank.get(1, "")
        else "Hotspot 1"
    )
    h2_lbl = (
        f"Hotspot 2: {term_by_rank.get(2, '')}".strip(": ")
        if term_by_rank.get(2, "")
        else "Hotspot 2"
    )
    h3_lbl = (
        f"Hotspot 3: {term_by_rank.get(3, '')}".strip(": ")
        if term_by_rank.get(3, "")
        else "Hotspot 3"
    )

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=d["date"],
            y=d["no_hotspot"],
            name=n_lbl,
            marker=dict(color="#" + BBVA_COLORS["blue"]),
        )
    )
    fig.add_trace(
        go.Bar(
            x=d["date"],
            y=d["hotspot_3"],
            name=h3_lbl,
            marker=dict(color="#" + BBVA_COLORS["yellow"]),
        )
    )
    fig.add_trace(
        go.Bar(
            x=d["date"],
            y=d["hotspot_2"],
            name=h2_lbl,
            marker=dict(color="#" + BBVA_COLORS["orange"]),
        )
    )
    fig.add_trace(
        go.Bar(
            x=d["date"],
            y=d["hotspot_1"],
            name=h1_lbl,
            marker=dict(color="#" + BBVA_COLORS["red"]),
        )
    )

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=24, t=22, b=96),
        barmode="stack",
        xaxis=dict(title="Día"),
        yaxis=dict(title="Incidencias registradas", rangemode="tozero"),
        legend=dict(
            orientation="h",
            x=0.0,
            y=-0.24,
            xanchor="left",
            yanchor="top",
            font=dict(size=12),
        ),
    )
    return fig


def _top_hotspots_fig(
    incident_evidence_df: Optional[pd.DataFrame],
    incident_timeline_df: Optional[pd.DataFrame],
    *,
    top_k: int = 3,
) -> Optional[go.Figure]:  # pragma: no cover - legacy chart kept for backwards compatibility
    hs = summarize_hotspot_counts(
        incident_evidence_df,
        incident_timeline_df,
        max_hotspots=max(1, int(top_k)),
    )
    if hs is None or hs.empty:
        return None

    d = hs.copy()
    d["hot_term"] = d.get("hot_term", "").astype(str).str.strip()
    d = d[d["hot_term"] != ""].copy()
    if d.empty:
        return None

    # Keep strict coherence with centralized hotspot summary:
    # rank and totals come directly from summarize_hotspot_counts.
    d["hot_rank"] = pd.to_numeric(d.get("hot_rank"), errors="coerce").fillna(999).astype(int)
    d["mention_comments"] = pd.to_numeric(d.get("mention_comments"), errors="coerce").fillna(0.0)
    d["hotspot_comments"] = pd.to_numeric(d.get("hotspot_comments"), errors="coerce").fillna(0.0)
    d["chart_nps_comments"] = pd.to_numeric(d.get("chart_nps_comments"), errors="coerce").fillna(
        0.0
    )
    d["impact"] = d["chart_nps_comments"].clip(lower=0.0)
    d.loc[d["impact"] <= 0.0, "impact"] = d["hotspot_comments"].clip(lower=0.0)
    d.loc[d["impact"] <= 0.0, "impact"] = d["mention_comments"].clip(lower=0.0)
    d = d.sort_values(["hot_rank"]).head(int(top_k)).copy()
    if d.empty:
        return None

    d["label"] = d["hot_term"].astype(str).str.upper().str.slice(0, 44)
    rank_color = {
        1: "#" + BBVA_COLORS["red"],
        2: "#" + BBVA_COLORS["orange"],
        3: "#" + BBVA_COLORS["yellow"],
    }
    d["bar_color"] = d["hot_rank"].map(rank_color).fillna("#" + BBVA_COLORS["red"])
    d["inbar"] = [
        f"#{int(r)}  {lbl}  ·  {int(v)}"
        for r, lbl, v in zip(d["hot_rank"], d["label"], d["impact"])
    ]
    d = d.iloc[::-1].copy()

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=d["impact"],
            y=d["label"],
            orientation="h",
            marker=dict(color=d["bar_color"].tolist()),
            text=d["inbar"].tolist(),
            textposition="inside",
            insidetextanchor="start",
            textfont=dict(size=19, color="#" + BBVA_COLORS["white"]),
            customdata=d[
                [
                    "hot_rank",
                    "mention_incidents",
                    "mention_comments",
                    "hotspot_links",
                    "chart_nps_comments",
                ]
            ].to_numpy(),
            hovertemplate=(
                "Hotspot #%{customdata[0]:.0f}=%{y}<br>"
                "Comentarios del gráfico=%{customdata[4]:.0f}<br>"
                "Comentarios negativos=%{customdata[2]:.0f}<br>"
                "Incidencias Helix=%{customdata[1]:.0f}<br>"
                "Links validados=%{customdata[3]:.0f}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=24, t=22, b=24),
        uniformtext=dict(minsize=16, mode="show"),
        xaxis=dict(
            title="",
            showticklabels=False,
            showgrid=False,
            zeroline=False,
            visible=False,
        ),
        yaxis=dict(title=""),
        showlegend=False,
    )
    return fig


def _hotspot_matches_by_day(
    incident_timeline_df: Optional[pd.DataFrame],
    incident_evidence_df: Optional[pd.DataFrame],
    *,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
) -> pd.DataFrame:
    cols = ["date", "matched_incidents"]
    if incident_timeline_df is None or incident_timeline_df.empty:
        return pd.DataFrame(columns=cols)

    t = incident_timeline_df.copy()
    req = {"date", "helix_records", "nps_comments"}
    if not req.issubset(set(t.columns)):
        return pd.DataFrame(columns=cols)

    t["date"] = _coerce_datetime_series(t["date"])
    t = t.dropna(subset=["date"])
    t = t[(t["date"] >= month_start) & (t["date"] <= month_end)].copy()
    if t.empty:
        return pd.DataFrame(columns=cols)

    t["helix_records"] = (
        pd.to_numeric(t["helix_records"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    t["nps_comments"] = (
        pd.to_numeric(t["nps_comments"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )

    # Prefer hotspot-level rows (hot_term pre-aggregated in the app payload).
    top_terms: list[str] = []
    if (
        incident_evidence_df is not None
        and not incident_evidence_df.empty
        and {"hot_term", "hot_rank"}.issubset(set(incident_evidence_df.columns))
    ):
        e = incident_evidence_df.copy()
        e["hot_term"] = e["hot_term"].astype(str).str.strip()
        e["hot_rank"] = pd.to_numeric(e["hot_rank"], errors="coerce")
        e = e.dropna(subset=["hot_rank"])
        e = e[e["hot_term"] != ""]
        if not e.empty:
            e = e.sort_values(["hot_rank"])
            top_terms = e["hot_term"].drop_duplicates().head(3).tolist()

    if "hot_term" in t.columns:
        t["hot_term"] = t["hot_term"].astype(str).str.strip()
        t = (
            t[t["hot_term"].isin(top_terms)].copy()
            if top_terms
            else t[t["hot_term"].str.strip() != ""].copy()
        )

    if t.empty:
        return pd.DataFrame(columns=cols)

    # Coincidence means there is operational signal and detractor signal on the day.
    t = t[(t["helix_records"] > 0) & (t["nps_comments"] > 0)].copy()
    if t.empty:
        return pd.DataFrame(columns=cols)

    out = (
        t.groupby("date", as_index=False)
        .agg(matched_incidents=("helix_records", "sum"))
        .sort_values("date")
    )
    out["matched_incidents"] = (
        pd.to_numeric(out["matched_incidents"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    return out


def _month_overlap_fig(
    month_daily: pd.DataFrame,
    *,
    focus_name: str,
    matched_daily: pd.DataFrame,
    matched_label: str = "Incidencias con match detractor",
) -> Optional[go.Figure]:  # pragma: no cover - legacy chart kept for backwards compatibility
    if month_daily is None or month_daily.empty:
        return None

    d = month_daily.sort_values("date").copy()
    m = (
        matched_daily.copy()
        if matched_daily is not None
        else pd.DataFrame(columns=["date", "matched_incidents"])
    )
    m["date"] = _coerce_datetime_series(m.get("date"))
    m = m.dropna(subset=["date"])
    m = m.groupby("date", as_index=False).agg(matched_incidents=("matched_incidents", "sum"))
    d = d.merge(m, on="date", how="left")
    d["matched_incidents"] = pd.to_numeric(d["matched_incidents"], errors="coerce").fillna(0.0)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["nps_mean"],
            name="NPS medio",
            mode="lines+markers",
            line=dict(color="#" + BBVA_COLORS["blue"], width=3.2),
            marker=dict(
                size=8,
                color=_ppt_nps_marker_colors(d["nps_mean"]),
                line=dict(color="#" + BBVA_COLORS["bg_light"], width=1),
            ),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["detractor_rate"] * 100.0,
            name=f"Opiniones {focus_name}",
            mode="lines",
            yaxis="y2",
            line=dict(color="#" + BBVA_COLORS["red"], width=2.6),
        )
    )
    fig.add_trace(
        go.Bar(
            x=d["date"],
            y=d["incidents"],
            name="Incidencias totales",
            yaxis="y3",
            marker=dict(color="#" + BBVA_COLORS["yellow"]),
            opacity=0.42,
        )
    )

    highlight_text = [str(int(v)) if float(v) > 0 else "" for v in d["matched_incidents"].tolist()]
    fig.add_trace(
        go.Bar(
            x=d["date"],
            y=d["matched_incidents"],
            name=matched_label,
            yaxis="y3",
            marker=dict(color="#" + BBVA_COLORS["orange"]),
            opacity=0.88,
            text=highlight_text,
            textposition="outside",
        )
    )

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=24, r=84, t=22, b=24),
        legend=dict(orientation="h", x=0.01, y=1.10),
        barmode="overlay",
        xaxis=dict(title="Día"),
        yaxis=dict(title="NPS medio (0-10)", range=[0, 10]),
        yaxis2=dict(
            title=f"% {focus_name}",
            overlaying="y",
            side="right",
            range=[0, 100],
            showgrid=False,
        ),
        yaxis3=dict(
            title="Incidencias",
            overlaying="y",
            side="right",
            anchor="free",
            position=0.92,
            showgrid=False,
            rangemode="tozero",
        ),
    )
    return fig


def _prepare_incident_evidence(  # pragma: no cover - legacy helper kept for backwards compatibility
    incident_evidence_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    cols = list(HOTSPOT_EVIDENCE_COLUMNS)
    if incident_evidence_df is None or incident_evidence_df.empty:
        return pd.DataFrame(columns=cols)

    d = incident_evidence_df.copy()
    id_col = _pick_first_col(
        d, ["incident_id", "incident number", "incident_number", "id de la incidencia"]
    )
    topic_col = _pick_first_col(d, ["nps_topic", "topic", "topico", "tópico"]) or ""
    date_col = _pick_first_col(d, ["incident_date", "fecha", "date", "opened_at"])
    summary_candidates = [
        "incident_summary",
        "Detailed Description",
        "Detailed Decription",
        "bbva_detaileddescription",
        "description",
        "descripcion",
        "descripción",
        "short description",
        "brief description",
        "bbva_shortdescription",
        "summary",
    ]
    comment_col = _pick_first_col(
        d,
        [
            "detractor_comment",
            "nps_comment",
            "comment",
            "comentario",
            "comentario detractor",
        ],
    )
    sim_col = _pick_first_col(d, ["similarity", "sim", "score"])
    hot_term_col = _pick_first_col(d, ["hot_term", "termino_caliente", "term"])
    hot_rank_col = _pick_first_col(d, ["hot_rank", "hotterm_rank", "rank"])
    mention_inc_col = _pick_first_col(
        d,
        ["mention_incidents", "hotspot_incidents_mentions", "mentions_incidents"],
    )
    mention_com_col = _pick_first_col(
        d,
        ["mention_comments", "hotspot_comments_mentions", "mentions_comments"],
    )
    hot_inc_col = _pick_first_col(d, ["hotspot_incidents", "cluster_incidents", "incidents_count"])
    hot_com_col = _pick_first_col(d, ["hotspot_comments", "cluster_comments", "comments_count"])
    hot_lnk_col = _pick_first_col(d, ["hotspot_links", "cluster_links", "links_count"])
    lower_map = {str(c).strip().lower(): str(c) for c in d.columns}
    summary_cols: list[str] = []
    for cand in summary_candidates:
        hit = lower_map.get(str(cand).strip().lower())
        if not hit:
            continue
        if hit not in summary_cols:
            summary_cols.append(hit)

    if summary_cols:
        summary_stack = pd.concat(
            [
                d[col]
                .astype(str)
                .fillna("")
                .str.strip()
                .replace({"nan": "", "NaN": "", "None": "", "NaT": ""})
                for col in summary_cols
            ],
            axis=1,
        )
        arr = summary_stack.to_numpy(dtype=object)
        non_empty = arr != ""
        first_idx = non_empty.argmax(axis=1)
        has_any = non_empty.any(axis=1)
        vals = np.full(len(summary_stack), "", dtype=object)
        rows = np.arange(len(summary_stack))
        vals[has_any] = arr[rows[has_any], first_idx[has_any]]
        incident_summary = pd.Series(vals, index=d.index)
    else:
        incident_summary = pd.Series([""] * len(d), index=d.index)

    out = pd.DataFrame(
        {
            "incident_id": d[id_col].astype(str) if id_col else "",
            "incident_date": _coerce_datetime_series(d[date_col]) if date_col else pd.NaT,
            "nps_topic": d[topic_col].astype(str) if topic_col else "",
            "incident_summary": incident_summary,
            "detractor_comment": d[comment_col].astype(str) if comment_col else "",
            "similarity": (
                pd.to_numeric(d[sim_col], errors="coerce").fillna(0.0) if sim_col else 0.0
            ),
            "hot_term": d[hot_term_col].astype(str) if hot_term_col else "",
            "hot_rank": pd.to_numeric(d[hot_rank_col], errors="coerce") if hot_rank_col else np.nan,
            "mention_incidents": (
                pd.to_numeric(d[mention_inc_col], errors="coerce").fillna(0.0)
                if mention_inc_col
                else 0.0
            ),
            "mention_comments": (
                pd.to_numeric(d[mention_com_col], errors="coerce").fillna(0.0)
                if mention_com_col
                else 0.0
            ),
            "hotspot_incidents": (
                pd.to_numeric(d[hot_inc_col], errors="coerce").fillna(0.0) if hot_inc_col else 0.0
            ),
            "hotspot_comments": (
                pd.to_numeric(d[hot_com_col], errors="coerce").fillna(0.0) if hot_com_col else 0.0
            ),
            "hotspot_links": (
                pd.to_numeric(d[hot_lnk_col], errors="coerce").fillna(0.0) if hot_lnk_col else 0.0
            ),
        }
    )
    out["incident_id"] = out["incident_id"].astype(str).str.strip()
    out["nps_topic"] = out["nps_topic"].astype(str).str.strip()
    out["incident_summary"] = out["incident_summary"].astype(str).str.strip()
    out["detractor_comment"] = out["detractor_comment"].astype(str).str.strip()
    out["hot_term"] = out["hot_term"].astype(str).str.strip()
    out["mention_incidents"] = (
        pd.to_numeric(out["mention_incidents"], errors="coerce").fillna(0.0).astype(int)
    )
    out["mention_comments"] = (
        pd.to_numeric(out["mention_comments"], errors="coerce").fillna(0.0).astype(int)
    )
    out["hotspot_incidents"] = (
        pd.to_numeric(out["hotspot_incidents"], errors="coerce").fillna(0.0).astype(int)
    )
    out["hotspot_comments"] = (
        pd.to_numeric(out["hotspot_comments"], errors="coerce").fillna(0.0).astype(int)
    )
    out["hotspot_links"] = (
        pd.to_numeric(out["hotspot_links"], errors="coerce").fillna(0.0).astype(int)
    )
    out = out[(out["incident_id"] != "") | (out["nps_topic"] != "")].copy()
    out = out.sort_values(
        ["hot_rank", "similarity"], ascending=[True, False], na_position="last"
    ).reset_index(drop=True)
    return out[cols]


def _top_topics_for_zoom(
    rationale_df: pd.DataFrame,
    ranking_df: Optional[pd.DataFrame],
    *,
    max_topics: int = 3,
) -> list[str]:  # pragma: no cover - legacy helper kept for backwards compatibility
    topics: list[str] = []
    if rationale_df is not None and not rationale_df.empty and "nps_topic" in rationale_df.columns:
        src = rationale_df.copy()
        sort_cols = []
        if "priority" in src.columns:
            sort_cols.append("priority")
        if "nps_points_at_risk" in src.columns:
            sort_cols.append("nps_points_at_risk")
        if sort_cols:
            src = src.sort_values(sort_cols, ascending=False)
        topics.extend(src["nps_topic"].astype(str).tolist())

    if ranking_df is not None and not ranking_df.empty and "nps_topic" in ranking_df.columns:
        src = ranking_df.copy()
        metric = "confidence_learned" if "confidence_learned" in src.columns else "confidence"
        if metric in src.columns:
            src[metric] = pd.to_numeric(src[metric], errors="coerce").fillna(0.0)
            src = src.sort_values([metric], ascending=False)
        topics.extend(src["nps_topic"].astype(str).tolist())

    out: list[str] = []
    seen: set[str] = set()
    for t in topics:
        key = str(t).strip()
        if not key or key in seen:
            continue
        out.append(key)
        seen.add(key)
        if len(out) >= int(max_topics):
            break
    return out


def _select_zoom_incidents(
    top_topics: list[str],
    incident_evidence_df: pd.DataFrame,
    *,
    max_items: int = 3,
) -> list[ZoomIncident]:  # pragma: no cover - legacy helper kept for backwards compatibility
    selected: list[ZoomIncident] = []

    if incident_evidence_df is not None and not incident_evidence_df.empty:
        ev = incident_evidence_df.copy()
        ev["hot_rank"] = pd.to_numeric(ev.get("hot_rank"), errors="coerce")
        ev["similarity"] = pd.to_numeric(ev.get("similarity"), errors="coerce").fillna(0.0)
        ev["hot_term"] = ev.get("hot_term", "").astype(str).str.strip()
        ev["incident_id"] = ev.get("incident_id", "").astype(str).str.strip()
        ev["detractor_comment"] = ev.get("detractor_comment", "").astype(str)
        ev["_has_comment"] = ev["detractor_comment"].str.strip().ne("")

        hotspots: list[tuple[float, str, pd.DataFrame]] = []
        ranked = ev.dropna(subset=["hot_rank"])
        if not ranked.empty:
            for (rk, term), g in ranked.groupby(["hot_rank", "hot_term"], dropna=False):
                hotspots.append((float(rk), str(term or "").strip(), g.copy()))
            hotspots.sort(
                key=lambda x: (
                    x[0],
                    -float(pd.to_numeric(x[2]["similarity"], errors="coerce").fillna(0.0).mean()),
                )
            )
        else:
            for topic in top_topics:
                g = ev[ev["nps_topic"].astype(str) == str(topic)].copy()
                if g.empty:
                    continue
                hotspots.append((999.0, "", g))
            if not hotspots:
                hotspots = [(999.0, "", ev.copy())]

        for _rk, term, g in hotspots:
            if g.empty:
                continue
            g = g.sort_values(["_has_comment", "similarity"], ascending=[False, False]).copy()
            rep = g.iloc[0]

            inc_id = str(rep.get("incident_id", "")).strip()
            topic = str(rep.get("nps_topic", "")).strip() or "Tópico sin etiqueta"
            hot_term = _clip(term or rep.get("hot_term", ""), 48)

            mention_inc = _safe_int(g.get("mention_incidents", pd.Series([0])).max(), default=0)
            mention_com = _safe_int(g.get("mention_comments", pd.Series([0])).max(), default=0)
            hotspot_inc = _safe_int(g.get("hotspot_incidents", pd.Series([0])).max(), default=0)
            hotspot_com = _safe_int(g.get("hotspot_comments", pd.Series([0])).max(), default=0)
            hotspot_lnk = _safe_int(g.get("hotspot_links", pd.Series([0])).max(), default=0)
            if mention_inc <= 0:
                mention_inc = int(hotspot_inc)
            if mention_com <= 0:
                mention_com = int(hotspot_com)

            sample_incidents = (
                g["incident_id"]
                .astype(str)
                .str.strip()
                .replace("", np.nan)
                .dropna()
                .drop_duplicates()
                .head(3)
                .tolist()
            )
            sample_comments = (
                g["detractor_comment"]
                .astype(str)
                .str.strip()
                .replace("", np.nan)
                .dropna()
                .drop_duplicates()
                .head(2)
                .tolist()
            )

            selected.append(
                ZoomIncident(
                    incident_id=inc_id or f"N/D-{len(selected) + 1}",
                    incident_date=_safe_dt(rep.get("incident_date")),
                    nps_topic=topic,
                    incident_summary=_clip(rep.get("incident_summary", ""), 190),
                    detractor_comment=_clip(rep.get("detractor_comment", ""), 220),
                    similarity=_safe_float(rep.get("similarity", 0.0), default=0.0),
                    hot_term=hot_term,
                    mention_incidents=mention_inc,
                    mention_comments=mention_com,
                    hotspot_incidents=hotspot_inc,
                    hotspot_comments=hotspot_com,
                    hotspot_links=hotspot_lnk,
                    sample_incidents=", ".join(sample_incidents),
                    sample_comments=" | ".join([_clip(c, 95) for c in sample_comments]),
                )
            )
            if len(selected) >= int(max_items):
                return selected

    while len(selected) < int(max_items):
        idx = len(selected) + 1
        topic = top_topics[idx - 1] if idx - 1 < len(top_topics) else "Tópico sin evidencia"
        selected.append(
            ZoomIncident(
                incident_id=f"N/D-{idx}",
                incident_date=None,
                nps_topic=topic,
                incident_summary="No se encontró descripción de incidencia para este tópico en el periodo.",
                detractor_comment="No se encontró comentario detractor validado con la política estricta activa.",
                similarity=0.0,
                hot_term="",
                mention_incidents=0,
                mention_comments=0,
                hotspot_incidents=0,
                hotspot_comments=0,
                hotspot_links=0,
                sample_incidents="",
                sample_comments="",
            )
        )
    return selected[: int(max_items)]


def _hotspot_summary_row(
    incident: ZoomIncident,
    hotspot_summary: pd.DataFrame,
) -> Optional[pd.Series]:  # pragma: no cover - legacy helper kept for backwards compatibility
    if hotspot_summary is None or hotspot_summary.empty:
        return None

    hot_term_key = str(incident.hot_term or "").strip()
    if hot_term_key and "hot_term" in hotspot_summary.columns:
        hit = hotspot_summary[
            hotspot_summary["hot_term"].astype(str).str.strip() == hot_term_key
        ].head(1)
        if not hit.empty:
            return hit.iloc[0]

    if "hot_rank" in hotspot_summary.columns:
        ranked = pd.to_numeric(hotspot_summary["hot_rank"], errors="coerce")
        if np.isfinite(ranked).any():
            order = hotspot_summary.assign(_rank=ranked).sort_values(["_rank"], na_position="last")
            return order.head(1).iloc[0]
    return None


def _parse_changepoints(value: object) -> list[pd.Timestamp]:
    if value is None:
        return []
    raw = value
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            with contextlib.suppress(Exception):
                raw = ast.literal_eval(s)
        else:
            raw = [s]

    if not isinstance(raw, (list, tuple, np.ndarray, pd.Series)):
        raw = [raw]

    out: list[pd.Timestamp] = []
    for v in raw:
        ts = _safe_dt(v)
        if ts is not None:
            out.append(ts.normalize())
    return out


def _changepoints_map(
    changepoints_by_topic: Optional[pd.DataFrame],
) -> dict[str, list[pd.Timestamp]]:
    if changepoints_by_topic is None or changepoints_by_topic.empty:
        return {}
    if "nps_topic" not in changepoints_by_topic.columns:
        return {}

    out: dict[str, list[pd.Timestamp]] = {}
    for _, r in changepoints_by_topic.iterrows():
        topic = str(r.get("nps_topic", "")).strip()
        if not topic:
            continue
        cp_col = r.get("changepoints", [])
        parsed = _parse_changepoints(cp_col)
        out[topic] = parsed
    return out


def _lag_days_for_topic(
    topic: str,
    *,
    lag_days_by_topic: Optional[pd.DataFrame],
    lag_weeks_by_topic: Optional[pd.DataFrame],
    rationale_df: pd.DataFrame,
    ranking_df: Optional[pd.DataFrame],
) -> int:
    topic = str(topic or "").strip()
    if not topic:
        return 0

    def _hit(df: Optional[pd.DataFrame], col: str) -> Optional[int]:
        if df is None or df.empty:
            return None
        if "nps_topic" not in df.columns or col not in df.columns:
            return None
        h = df[df["nps_topic"].astype(str) == topic].head(1)
        if h.empty:
            return None
        return max(0, _safe_int(h.iloc[0][col], default=0))

    direct = _hit(lag_days_by_topic, "best_lag_days")
    if direct is not None:
        return direct

    for frame in [ranking_df, lag_weeks_by_topic, rationale_df]:
        lag_w = _hit(frame, "best_lag_weeks")
        if lag_w is not None:
            return max(0, int(round(float(lag_w) * 7.0)))

    return 0


def _incident_related_timeline(
    *,
    incident_id: str,
    hot_term: str = "",
    incident_timeline_df: Optional[pd.DataFrame],
) -> pd.DataFrame:  # pragma: no cover - legacy helper kept for backwards compatibility
    cols = [
        "date",
        "helix_records",
        "nps_comments",
        "nps_comments_moderate",
        "nps_comments_high",
        "nps_comments_critical",
        "incident_ids",
    ]
    if incident_timeline_df is None or incident_timeline_df.empty:
        return pd.DataFrame(columns=cols)
    req = {"incident_id", "date", "helix_records", "nps_comments"}
    if not req.issubset(set(incident_timeline_df.columns)):
        return pd.DataFrame(columns=cols)

    d = incident_timeline_df.copy()
    hot_term_key = str(hot_term or "").strip()
    if hot_term_key and "hot_term" in d.columns:
        d["hot_term"] = d["hot_term"].astype(str).str.strip()
        d = d[d["hot_term"] == hot_term_key].copy()
    else:
        d["incident_id"] = d["incident_id"].astype(str).str.strip()
        d = d[d["incident_id"] == str(incident_id).strip()].copy()
    if d.empty:
        return pd.DataFrame(columns=cols)

    d["date"] = _coerce_datetime_series(d["date"])
    d = d.dropna(subset=["date"])
    if d.empty:
        return pd.DataFrame(columns=cols)

    d["helix_records"] = (
        pd.to_numeric(d["helix_records"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    d["nps_comments"] = (
        pd.to_numeric(d["nps_comments"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    sev_mod = (
        d["nps_comments_moderate"]
        if "nps_comments_moderate" in d.columns
        else pd.Series([0.0] * len(d), index=d.index)
    )
    sev_high = (
        d["nps_comments_high"]
        if "nps_comments_high" in d.columns
        else pd.Series([0.0] * len(d), index=d.index)
    )
    sev_critical = (
        d["nps_comments_critical"]
        if "nps_comments_critical" in d.columns
        else pd.Series([0.0] * len(d), index=d.index)
    )
    d["nps_comments_moderate"] = pd.to_numeric(sev_mod, errors="coerce").fillna(0.0).clip(lower=0.0)
    d["nps_comments_high"] = pd.to_numeric(sev_high, errors="coerce").fillna(0.0).clip(lower=0.0)
    d["nps_comments_critical"] = (
        pd.to_numeric(sev_critical, errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    if "incident_ids" not in d.columns:
        d["incident_ids"] = d.get("incident_id", "").astype(str)
    d["incident_ids"] = d["incident_ids"].astype(str).fillna("")

    def _merge_ids(values: pd.Series) -> str:
        raw: list[str] = []
        for v in values.astype(str).tolist():
            for part in str(v).split("|"):
                p = part.strip()
                if p:
                    raw.append(p)
        seen: set[str] = set()
        out: list[str] = []
        for item in raw:
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
        return " | ".join(out)

    d = (
        d.groupby("date", as_index=False)
        .agg(
            helix_records=("helix_records", "sum"),
            nps_comments=("nps_comments", "sum"),
            nps_comments_moderate=("nps_comments_moderate", "sum"),
            nps_comments_high=("nps_comments_high", "sum"),
            nps_comments_critical=("nps_comments_critical", "sum"),
            incident_ids=("incident_ids", _merge_ids),
        )
        .sort_values("date")
    )
    d = d[(d["helix_records"] > 0) | (d["nps_comments"] > 0)].copy()
    return d[cols].copy()


def _zoom_incident_fig(
    *,
    topic_daily: pd.DataFrame,
    related_timeline: pd.DataFrame,
    incident: ZoomIncident,
    lag_days: int,
    changepoints: list[pd.Timestamp],
    focus_name: str,
    period_start: Optional[date] = None,
    period_end: Optional[date] = None,
) -> Optional[go.Figure]:  # pragma: no cover - legacy chart kept for backwards compatibility
    del incident, lag_days, focus_name

    rel = related_timeline.copy() if related_timeline is not None else pd.DataFrame()
    if not rel.empty:
        rel["date"] = _coerce_datetime_series(rel["date"])
        rel = rel.dropna(subset=["date"]).sort_values("date")
        rel["helix_records"] = (
            pd.to_numeric(rel["helix_records"], errors="coerce").fillna(0.0).clip(lower=0.0)
        )
        rel["nps_comments"] = (
            pd.to_numeric(rel["nps_comments"], errors="coerce").fillna(0.0).clip(lower=0.0)
        )
        rel["nps_comments_moderate"] = (
            pd.to_numeric(rel.get("nps_comments_moderate"), errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0)
        )
        rel["nps_comments_high"] = (
            pd.to_numeric(rel.get("nps_comments_high"), errors="coerce").fillna(0.0).clip(lower=0.0)
        )
        rel["nps_comments_critical"] = (
            pd.to_numeric(rel.get("nps_comments_critical"), errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0)
        )
        rel["incident_ids"] = rel.get("incident_ids", "").astype(str).fillna("")

        def _merge_ids(values: pd.Series) -> str:
            raw: list[str] = []
            for v in values.astype(str).tolist():
                for part in str(v).split("|"):
                    p = part.strip()
                    if p:
                        raw.append(p)
            seen: set[str] = set()
            out: list[str] = []
            for item in raw:
                if item in seen:
                    continue
                seen.add(item)
                out.append(item)
            return " | ".join(out)

        rel = (
            rel.groupby("date", as_index=False)
            .agg(
                helix_records=("helix_records", "sum"),
                nps_comments=("nps_comments", "sum"),
                nps_comments_moderate=("nps_comments_moderate", "sum"),
                nps_comments_high=("nps_comments_high", "sum"),
                nps_comments_critical=("nps_comments_critical", "sum"),
                incident_ids=("incident_ids", _merge_ids),
            )
            .sort_values("date")
        )

    if rel.empty:
        return None

    start_ts: Optional[pd.Timestamp] = None
    end_ts: Optional[pd.Timestamp] = None
    if period_start is not None and period_end is not None:
        start_ts = _safe_dt(period_start)
        end_ts = _safe_dt(period_end)
    if start_ts is None or end_ts is None or end_ts < start_ts:
        start_ts = pd.Timestamp(rel["date"].min()).normalize()
        end_ts = pd.Timestamp(rel["date"].max()).normalize()

    scope = pd.DataFrame({"date": pd.date_range(start=start_ts, end=end_ts, freq="D")})
    z = scope.merge(rel, on="date", how="left")
    z["helix_records"] = pd.to_numeric(z.get("helix_records"), errors="coerce").fillna(0.0)
    z["nps_comments"] = pd.to_numeric(z.get("nps_comments"), errors="coerce").fillna(0.0)
    z["nps_comments_moderate"] = pd.to_numeric(
        z.get("nps_comments_moderate"), errors="coerce"
    ).fillna(0.0)
    z["nps_comments_high"] = pd.to_numeric(z.get("nps_comments_high"), errors="coerce").fillna(0.0)
    z["nps_comments_critical"] = pd.to_numeric(
        z.get("nps_comments_critical"), errors="coerce"
    ).fillna(0.0)
    z["incident_ids"] = z.get("incident_ids", "").astype(str).fillna("")
    nps_series = pd.DataFrame(columns=["date", "nps_mean"])
    if (
        topic_daily is not None
        and not topic_daily.empty
        and {"date", "nps_mean"}.issubset(set(topic_daily.columns))
    ):
        nps_series = topic_daily[["date", "nps_mean"]].copy()
        nps_series["date"] = _coerce_datetime_series(nps_series["date"])
        nps_series["nps_mean"] = pd.to_numeric(nps_series["nps_mean"], errors="coerce")
        nps_series = nps_series.dropna(subset=["date", "nps_mean"])
        nps_series = (
            nps_series.groupby("date", as_index=False)
            .agg(nps_mean=("nps_mean", "mean"))
            .sort_values("date")
        )

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=z["date"],
            y=z["nps_comments_moderate"],
            name="Comentarios moderados (NPS 5-6)",
            marker=dict(color="#FCA5A5"),
            opacity=0.88,
        )
    )
    fig.add_trace(
        go.Bar(
            x=z["date"],
            y=z["nps_comments_high"],
            name="Comentarios altos (NPS 3-4)",
            marker=dict(color="#EF4444"),
            opacity=0.88,
        )
    )
    fig.add_trace(
        go.Bar(
            x=z["date"],
            y=z["nps_comments_critical"],
            name="Comentarios críticos (NPS 0-2)",
            marker=dict(color="#B91C1C"),
            opacity=0.92,
        )
    )
    totals = z["nps_comments_moderate"] + z["nps_comments_high"] + z["nps_comments_critical"]
    total_labels = [str(int(v)) if float(v) > 0 else "" for v in totals.tolist()]
    fig.add_trace(
        go.Scatter(
            x=z["date"],
            y=totals,
            mode="text",
            text=total_labels,
            textposition="top center",
            showlegend=False,
            hoverinfo="skip",
            textfont=dict(size=10, color="#" + BBVA_COLORS["muted"]),
        )
    )

    points = z[z["helix_records"] > 0].copy()

    def _point_label(v: object) -> str:
        items = [p.strip() for p in str(v or "").split("|") if p.strip()]
        if not items:
            return ""
        inc_items = [it for it in items if str(it).upper().startswith("INC")]
        return str((inc_items[0] if inc_items else items[0])).strip()

    point_labels_raw = points["incident_ids"].map(_point_label).tolist()
    point_labels: list[str] = []
    last_labeled_date: Optional[pd.Timestamp] = None
    for dt, lbl in zip(points["date"].tolist(), point_labels_raw):
        label = str(lbl or "").strip()
        if not label:
            point_labels.append("")
            continue
        dt_ts = pd.Timestamp(dt).normalize()
        if last_labeled_date is not None and int((dt_ts - last_labeled_date).days) < 2:
            point_labels.append("")
            continue
        point_labels.append(label)
        last_labeled_date = dt_ts

    fig.add_trace(
        go.Scatter(
            x=points["date"],
            y=points["helix_records"],
            name="Incidencias Helix del hotspot",
            yaxis="y2",
            mode="markers+text",
            marker=dict(size=8),
            text=point_labels,
            textposition="top right",
            textfont=dict(size=9, color="#" + BBVA_COLORS["blue"]),
        )
    )

    if not nps_series.empty:
        fig.add_trace(
            go.Scatter(
                x=nps_series["date"],
                y=nps_series["nps_mean"],
                name="NPS medio",
                yaxis="y3",
                mode="lines+markers",
                line=dict(color="#" + BBVA_COLORS["blue"], width=2.6),
                marker=dict(
                    size=6,
                    color=_ppt_nps_marker_colors(nps_series["nps_mean"]),
                    line=dict(color="#" + BBVA_COLORS["bg_light"], width=1),
                ),
            )
        )

    cp_in_window = []
    min_d = z["date"].min()
    max_d = z["date"].max()
    for cp in changepoints:
        if cp < min_d or cp > max_d:
            continue
        cp_in_window.append(cp)
        fig.add_vline(
            x=cp,
            line_width=1.5,
            line_dash="dot",
            line_color="#" + BBVA_COLORS["sky"],
        )

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=20, r=20, t=20, b=24),
        legend=dict(orientation="h", x=0.01, y=1.08),
        barmode="stack",
        xaxis_title="Día",
        yaxis=dict(title="Comentarios negativos (volumen diario)", rangemode="tozero"),
        yaxis2=dict(
            title="Incidencias Helix del hotspot (puntos)",
            overlaying="y",
            side="right",
            rangemode="tozero",
            showgrid=False,
        ),
        yaxis3=dict(
            title="NPS medio (0-10)",
            overlaying="y",
            side="right",
            anchor="free",
            position=0.92,
            range=[0, 10],
            showgrid=False,
        ),
    )

    if cp_in_window:
        fig.add_annotation(
            xref="paper",
            yref="paper",
            x=0.99,
            y=0.98,
            text=f"Change points visibles: {len(cp_in_window)}",
            showarrow=False,
            font=dict(size=10, color="#" + BBVA_COLORS["muted"]),
            align="right",
        )

    return fig


def _topic_metrics(topic: str, rationale_df: pd.DataFrame) -> dict[str, float]:
    if rationale_df is None or rationale_df.empty or "nps_topic" not in rationale_df.columns:
        return {}
    hit = rationale_df[rationale_df["nps_topic"].astype(str) == str(topic)].head(1)
    if hit.empty:
        return {}
    r = hit.iloc[0]
    return {
        "risk": _safe_float(r.get("nps_points_at_risk", 0.0), default=0.0),
        "recoverable": _safe_float(r.get("nps_points_recoverable", 0.0), default=0.0),
        "priority": _safe_float(r.get("priority", 0.0), default=0.0),
        "confidence": _safe_float(r.get("confidence", 0.0), default=0.0),
        "focus_probability": _safe_float(
            r.get("focus_probability_with_incident", np.nan), default=np.nan
        ),
        "nps_delta_expected": _safe_float(r.get("nps_delta_expected", np.nan), default=np.nan),
        "total_nps_impact": _safe_float(r.get("total_nps_impact", 0.0), default=0.0),
        "causal_score": _safe_float(r.get("causal_score", 0.0), default=0.0),
        "lag_weeks": _safe_float(r.get("best_lag_weeks", np.nan), default=np.nan),
    }


def _touchpoint_method_label(source: str) -> str:
    mode = str(source or "").strip()
    if mode == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS:
        return "Catálogo ejecutivo de journeys"
    if mode == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
        return "Cruce semántico incidencias-comentarios"
    return "Relación incidencia -> comentario -> NPS"


def _set_placeholder_text(
    slide: object, idx: int, text: str, *, font_name: str, size_pt: float
) -> None:
    try:
        placeholder = slide.placeholders[idx]
    except Exception:
        return
    tf = placeholder.text_frame
    tf.clear()
    p = tf.paragraphs[0]
    r = p.add_run()
    r.text = text
    r.font.name = font_name
    r.font.size = Pt(size_pt)
    r.font.bold = True
    r.font.color.rgb = _rgb(BBVA_COLORS["ink"])


def _add_cover_slide(
    prs: Presentation,
    *,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    period_start: date,
    period_end: date,
    overview: dict[str, object],
    story_md: str,
) -> None:
    slide = _new_slide(prs, kind="cover")
    title = "NPS Lens"
    subtitle = f"{service_origin} · {service_origin_n1}".strip(" ·")
    if service_origin_n2:
        subtitle = f"{subtitle} · {service_origin_n2}".strip(" ·")
    period_label = f"{_safe_date(period_start)} -> {_safe_date(period_end)}"

    eyebrow = slide.shapes.add_textbox(Inches(0.70), Inches(0.36), Inches(2.4), Inches(0.26))
    etf = eyebrow.text_frame
    _configure_text_frame(etf)
    etf.clear()
    ep = etf.paragraphs[0]
    er = ep.add_run()
    er.text = "Lectura ejecutiva"
    er.font.name = BBVA_FONT_MEDIUM
    er.font.size = Pt(10.5)
    er.font.bold = True
    er.font.color.rgb = _rgb(BBVA_COLORS["blue"])

    hero = slide.shapes.add_textbox(Inches(0.70), Inches(0.58), Inches(5.8), Inches(0.55))
    htf = hero.text_frame
    _configure_text_frame(htf)
    htf.clear()
    hp = htf.paragraphs[0]
    hr = hp.add_run()
    hr.text = title
    hr.font.name = BBVA_FONT_DISPLAY
    hr.font.size = Pt(30)
    hr.font.bold = True
    hr.font.color.rgb = _rgb(BBVA_COLORS["ink"])

    sub = slide.shapes.add_textbox(Inches(0.70), Inches(0.98), Inches(6.2), Inches(0.34))
    stf = sub.text_frame
    _configure_text_frame(stf)
    stf.clear()
    sp = stf.paragraphs[0]
    sr = sp.add_run()
    sr.text = subtitle
    sr.font.name = BBVA_FONT_BODY
    sr.font.size = Pt(14)
    sr.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    ribbon = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(0.62),
        Inches(1.36),
        Inches(12.0),
        Inches(0.52),
    )
    ribbon.fill.solid()
    ribbon.fill.fore_color.rgb = _rgb(BBVA_COLORS["blue"])
    ribbon.line.fill.background()
    rtf = ribbon.text_frame
    rtf.clear()
    rp = rtf.paragraphs[0]
    rr = rp.add_run()
    rr.text = f"Periodo analizado · {period_label}"
    rr.font.name = BBVA_FONT_MEDIUM
    rr.font.size = Pt(12)
    rr.font.bold = True
    rr.font.color.rgb = _rgb(BBVA_COLORS["white"])

    summary_lines = _cover_summary_lines(overview, story_md) or [
        "La lectura combina señal de experiencia, comentarios y evolución del periodo.",
        "Las tarjetas de la derecha recogen el tamaño de muestra y el equilibrio entre detractores y promotores.",
        "El resto del deck explica qué temas pesan más y dónde conviene priorizar acciones.",
    ]
    _add_bullet_lines(
        slide,
        left=0.70,
        top=2.02,
        width=6.75,
        height=3.36,
        title="Mensaje clave del periodo",
        lines=summary_lines,
        accent=BBVA_COLORS["sky"],
    )

    _add_stat_card(
        slide,
        left=7.68,
        top=2.02,
        width=2.3,
        height=1.58,
        label="Comentarios",
        value=f"{int(_safe_int(overview.get('comments', 0))):,}".replace(",", "."),
        accent=BBVA_COLORS["blue"],
        hint="Base útil del periodo",
    )
    _add_stat_card(
        slide,
        left=10.12,
        top=2.02,
        width=2.3,
        height=1.58,
        label="NPS medio",
        value=_fmt_num_or_nd(overview.get("nps_mean", np.nan)),
        accent=BBVA_COLORS["green"],
        hint="Escala 0-10",
    )
    _add_stat_card(
        slide,
        left=7.68,
        top=3.74,
        width=2.3,
        height=1.58,
        label="% detractores",
        value=_fmt_pct_or_nd(overview.get("detractor_rate", np.nan)),
        accent=BBVA_COLORS["red"],
        hint="Valoraciones <= 6",
    )
    _add_stat_card(
        slide,
        left=10.12,
        top=3.74,
        width=2.3,
        height=1.58,
        label="% promotores",
        value=_fmt_pct_or_nd(overview.get("promoter_rate", np.nan)),
        accent=BBVA_COLORS["green"],
        hint="Valoraciones >= 9",
    )


def _add_overview_slide(
    prs: Presentation,
    *,
    service_origin: str,
    service_origin_n1: str,
    period_label: str,
    period_end: date,
    overview: dict[str, object],
    selected_nps_df: Optional[pd.DataFrame],
    period_days: int,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="1. Evolución del NPS del periodo",
        subtitle=f"{service_origin} · {service_origin_n1} · {period_label}",
    )
    _panel(
        slide,
        left=0.66,
        top=1.48,
        width=9.00,
        height=5.42,
        title="NPS clásico y peso detractor",
    )
    _figure_in_panel(
        slide,
        figure=chart_daily_kpis(
            selected_nps_df.copy() if selected_nps_df is not None else pd.DataFrame(),
            get_theme("light"),
            days=max(int(period_days), 1),
        ),
        left=0.82,
        top=1.84,
        width=8.68,
        height=4.84,
        empty_note="No hay suficiente señal diaria para construir la evolución del periodo.",
    )

    month_label = _month_label_es(period_end).title()
    trend_lines = [
        f"El periodo arranca con NPS clásico {_fmt_num_or_nd(overview.get('start_classic', np.nan))} y termina en {_fmt_num_or_nd(overview.get('end_classic', np.nan))}.",
        f"El peso detractor pasa de {_fmt_pct_or_nd(overview.get('start_detr', np.nan))} a {_fmt_pct_or_nd(overview.get('end_detr', np.nan))}.",
        "NPS clásico = promotores menos detractores; se usa para seguir la señal neta del periodo.",
    ]
    _add_bullet_lines(
        slide,
        left=9.92,
        top=1.48,
        width=2.76,
        height=5.42,
        title=f"As-Is {month_label}",
        lines=trend_lines,
        accent=BBVA_COLORS["orange"],
        body_font_size_pt=13.0,
    )


def _add_deep_dive_slide(
    prs: Presentation,
    *,
    period_label: str,
    text_topics_df: pd.DataFrame,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="2. Qué han dicho los clientes",
        subtitle=f"Temas más repetidos en los comentarios del periodo · {period_label}",
    )
    _panel(slide, left=0.66, top=1.48, width=8.2, height=5.42, title="Top temas del periodo")
    _figure_in_panel(
        slide,
        figure=chart_topic_bars(text_topics_df, get_theme("light"), top_k=10),
        left=0.82,
        top=1.82,
        width=7.18,
        height=2.72,
        empty_note="No hay suficiente volumen textual para construir el top 10.",
    )

    table_rows = [
        [
            str(row.cluster_id),
            f"{int(row.n):,}".replace(",", "."),
            str(row.top_terms_txt),
            str(row.example_txt),
        ]
        for row in text_topics_df.head(4).itertuples()
    ]
    _add_compact_table(
        slide,
        left=0.82,
        top=4.64,
        width=7.18,
        title="Clusters",
        headers=["cluster_id", "n", "top_terms", "examples"],
        rows=table_rows or [["-", "-", "Sin datos", "Sin ejemplos"]],
        row_height=0.31,
        col_width_ratios=[0.8, 0.8, 2.5, 2.3],
        clip_lengths=[8, 8, 44, 40],
        font_size_pt=9.2,
    )

    bullet_lines = [
        f"{row.label}: {int(row.n):,} comentarios.".replace(",", ".")
        for _, row in text_topics_df.head(4).iterrows()
    ] or ["No se han detectado temas con masa crítica suficiente."]
    _add_bullet_lines(
        slide,
        left=9.00,
        top=1.48,
        width=3.68,
        height=5.42,
        title="Qué destaca",
        lines=bullet_lines,
        accent=BBVA_COLORS["sky"],
        body_font_size_pt=13.0,
    )


def _add_topic_timing_slide(
    prs: Presentation,
    *,
    period_label: str,
    period_days: int,
    selected_nps_df: Optional[pd.DataFrame],
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    source_df = selected_nps_df.copy() if selected_nps_df is not None else pd.DataFrame()
    _add_header(
        slide,
        title="2. Cuándo y cómo lo dicen",
        subtitle=f"Volumen diario de respuestas y reparto de promotores, pasivos y detractores · {period_label}",
    )
    _panel(
        slide,
        left=0.66,
        top=1.48,
        width=12.02,
        height=2.38,
        title="Cuándo lo dicen",
    )
    _figure_in_panel(
        slide,
        figure=chart_daily_volume(source_df, get_theme("light"), days=max(int(period_days), 1)),
        left=0.86,
        top=1.80,
        width=11.62,
        height=1.86,
        empty_note="No hay señal suficiente para mostrar el volumen diario del periodo.",
    )
    _panel(slide, left=0.66, top=4.02, width=12.02, height=2.88, title="Cómo lo dicen")
    _figure_in_panel(
        slide,
        figure=chart_daily_mix_business(
            source_df, get_theme("light"), days=max(int(period_days), 1)
        ),
        left=0.86,
        top=4.34,
        width=11.62,
        height=2.26,
        empty_note="No hay señal suficiente para la distribución diaria por grupo.",
    )


def _add_change_vs_past_slide(
    prs: Presentation,
    *,
    period_label: str,
    current_label: str,
    baseline_label: str,
    current_source_df: pd.DataFrame,
    baseline_source_df: pd.DataFrame,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="3. Qué ha cambiado respecto al pasado",
        subtitle=f"Periodo actual frente a la base histórica anterior · actual {current_label} · base {baseline_label}",
    )
    _panel(slide, left=0.66, top=1.48, width=12.02, height=2.38, title="Palanca")
    _figure_in_panel(
        slide,
        figure=chart_driver_delta(
            driver_delta_table(
                current_source_df,
                baseline_source_df,
                dimension="Palanca",
                min_n=50,
            ),
            get_theme("light"),
        ),
        left=0.86,
        top=1.80,
        width=11.62,
        height=1.86,
        empty_note="No hay base histórica suficiente para comparar por palanca.",
    )
    _panel(slide, left=0.66, top=4.02, width=12.02, height=2.88, title="Subpalanca")
    _figure_in_panel(
        slide,
        figure=chart_driver_delta(
            driver_delta_table(
                current_source_df,
                baseline_source_df,
                dimension="Subpalanca",
                min_n=50,
            ),
            get_theme("light"),
        ),
        left=0.86,
        top=4.34,
        width=11.62,
        height=2.26,
        empty_note="No hay base histórica suficiente para comparar por subpalanca.",
    )


def _add_pain_by_group_slide(
    prs: Presentation,
    *,
    period_label: str,
    selected_nps_df: Optional[pd.DataFrame],
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    source_df = selected_nps_df.copy() if selected_nps_df is not None else pd.DataFrame()
    _add_header(
        slide,
        title="4. Dónde duele según el tipo de cliente",
        subtitle=f"NPS por canal y eje de experiencia dentro del periodo analizado · {period_label}",
    )
    _panel(slide, left=0.66, top=1.48, width=6.0, height=5.42, title="Palanca x Canal")
    _figure_in_panel(
        slide,
        figure=chart_cohort_heatmap(
            source_df,
            get_theme("light"),
            row_dim="Palanca",
            col_dim="Canal",
            min_n=30,
        ),
        left=0.82,
        top=1.86,
        width=5.68,
        height=4.92,
        empty_note="No hay señal suficiente para mostrar la matriz Palanca x Canal.",
    )
    _panel(slide, left=6.90, top=1.48, width=5.78, height=5.42, title="Subpalanca x Canal")
    _figure_in_panel(
        slide,
        figure=chart_cohort_heatmap(
            source_df,
            get_theme("light"),
            row_dim="Subpalanca",
            col_dim="Canal",
            min_n=30,
        ),
        left=7.06,
        top=1.86,
        width=5.46,
        height=4.92,
        empty_note="No hay señal suficiente para mostrar la matriz Subpalanca x Canal.",
    )


def _add_gap_slide(
    prs: Presentation,
    *,
    period_label: str,
    palanca_gap_df: pd.DataFrame,
    subpalanca_gap_df: pd.DataFrame,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="5. Casos más alejados del promedio",
        subtitle=f"Top de casos con peor diferencia frente al NPS medio general · {period_label}",
    )
    _panel(slide, left=0.66, top=1.48, width=8.15, height=2.38, title="Palanca")
    _figure_in_panel(
        slide,
        figure=chart_driver_bar(palanca_gap_df, get_theme("light"), top_k=10),
        left=0.82,
        top=1.80,
        width=7.82,
        height=1.86,
        empty_note="No hay suficiente señal para el ranking de brechas por palanca.",
    )

    palanca_lines = [
        f"{idx + 1}. {_clip(row.value, 30)} · n={int(row.n)} · NPS {_fmt_num_or_nd(row.nps)} · gap {float(row.gap_vs_overall):+.1f}"
        for idx, row in enumerate(palanca_gap_df.head(5).itertuples())
    ]
    _add_bullet_lines(
        slide,
        left=8.98,
        top=1.48,
        width=3.70,
        height=2.38,
        title="",
        lines=palanca_lines,
        body_font_size_pt=11.0,
    )

    _panel(slide, left=0.66, top=4.02, width=8.15, height=2.88, title="Subpalanca")
    _figure_in_panel(
        slide,
        figure=chart_driver_bar(subpalanca_gap_df, get_theme("light"), top_k=10),
        left=0.82,
        top=4.34,
        width=7.82,
        height=2.26,
        empty_note="No hay suficiente señal para el ranking de brechas por subpalanca.",
    )
    subpalanca_lines = [
        f"{idx + 1}. {_clip(row.value, 30)} · n={int(row.n)} · NPS {_fmt_num_or_nd(row.nps)} · gap {float(row.gap_vs_overall):+.1f}"
        for idx, row in enumerate(subpalanca_gap_df.head(5).itertuples())
    ]
    _add_bullet_lines(
        slide,
        left=8.98,
        top=4.02,
        width=3.70,
        height=2.88,
        title="",
        lines=subpalanca_lines,
        body_font_size_pt=11.0,
    )


def _add_opportunity_slide(
    prs: Presentation,
    *,
    period_label: str,
    opportunities_df: pd.DataFrame,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="6. Oportunidades a priorizar",
        subtitle=f"Ranking de oportunidades por impacto potencial y solidez de evidencia · {period_label}",
    )
    opp_chart_df = opportunities_df.copy()
    if not opp_chart_df.empty and "dimension" in opp_chart_df.columns:
        palanca_df = opp_chart_df[opp_chart_df["dimension"].astype(str) == "Palanca"].copy()
        if not palanca_df.empty:
            opp_chart_df = palanca_df
    if not opp_chart_df.empty and "label" not in opp_chart_df.columns:
        opp_chart_df["label"] = opp_chart_df.apply(
            lambda row: f"{row.get('dimension')}={row.get('value')}",
            axis=1,
        )
    _panel(
        slide,
        left=0.66,
        top=1.48,
        width=12.02,
        height=4.30,
        title="Ranking por impacto estimado x confianza",
    )
    _figure_in_panel(
        slide,
        figure=chart_opportunities_bar(opp_chart_df, get_theme("light"), top_k=10),
        left=0.86,
        top=1.86,
        width=11.62,
        height=3.62,
        empty_note="No se identificaron oportunidades robustas con el umbral actual.",
    )
    lines = explain_opportunities(opp_chart_df, max_items=5)
    _add_bullet_lines(
        slide,
        left=0.66,
        top=5.94,
        width=12.02,
        height=0.96,
        title="",
        lines=lines,
        accent=BBVA_COLORS["line"],
        body_font_size_pt=12.5,
    )


def _add_causal_timeline_slide(
    prs: Presentation,
    *,
    period_label: str,
    daily_mix: pd.DataFrame,
    overall_daily: pd.DataFrame,
    nps_points_at_risk: float,
    nps_points_recoverable: float,
    top3_incident_share: float,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    incidents_total = int(
        pd.to_numeric(overall_daily.get("incidents", 0.0), errors="coerce").fillna(0.0).sum()
    )
    detractor_avg = float(
        pd.to_numeric(
            overall_daily.get(
                "focus_rate",
                overall_daily.get("detractor_rate", daily_mix.get("detractor_rate", 0.0)),
            ),
            errors="coerce",
        )
        .fillna(0.0)
        .mean()
    )
    _add_header(
        slide,
        title="7. Cuando la operación afecta a la experiencia",
        subtitle=period_label,
    )
    _panel(
        slide,
        left=0.66,
        top=1.48,
        width=8.15,
        height=5.42,
        title="Evolución diaria de incidencias y grupos NPS",
    )
    _figure_in_panel(
        slide,
        figure=_causal_daily_timeline_fig(daily_mix, overall_daily),
        left=0.82,
        top=1.86,
        width=7.83,
        height=4.92,
        empty_note="No hay cobertura diaria suficiente para el timeline causal.",
    )
    _panel(
        slide,
        left=9.02,
        top=1.48,
        width=3.66,
        height=5.42,
        title="Cómo leerlo",
        border=BBVA_COLORS["red"],
    )
    text_box = slide.shapes.add_textbox(Inches(9.22), Inches(2.02), Inches(3.26), Inches(1.05))
    text_tf = text_box.text_frame
    _configure_text_frame(text_tf)
    text_tf.clear()
    text_p = text_tf.paragraphs[0]
    text_p.alignment = PP_ALIGN.LEFT
    text_r = text_p.add_run()
    text_r.text = (
        "Las incidencias degradan momentos críticos del journey, lo que genera experiencias "
        "negativas que se reflejan en los comentarios y finalmente en el NPS."
    )
    text_r.font.name = BBVA_FONT_BODY
    text_r.font.size = Pt(12.5)
    text_r.font.color.rgb = _rgb(BBVA_COLORS["muted"])
    _add_stat_card(
        slide,
        left=9.18,
        top=3.48,
        width=1.60,
        height=1.12,
        label="Incidencias",
        value=f"{incidents_total:,}",
        accent=BBVA_COLORS["blue"],
    )
    _add_stat_card(
        slide,
        left=10.92,
        top=3.48,
        width=1.60,
        height=1.12,
        label="% detractores",
        value=f"{detractor_avg*100.0:.2f}%",
        accent=BBVA_COLORS["red"],
    )
    _add_stat_card(
        slide,
        left=9.18,
        top=4.78,
        width=1.60,
        height=1.12,
        label="NPS en riesgo",
        value=f"{nps_points_at_risk:.2f} pts",
        accent=BBVA_COLORS["orange"],
    )
    _add_stat_card(
        slide,
        left=10.92,
        top=4.78,
        width=1.60,
        height=1.12,
        label="NPS recuperable",
        value=f"{nps_points_recoverable:.2f} pts",
        accent=BBVA_COLORS["green"],
    )


def _add_journeys_summary_slide(
    prs: Presentation,
    *,
    period_label: str,
    touchpoint_source: str,
    chain_df: pd.DataFrame,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="8. Experiencias afectadas del periodo",
        subtitle=f"Resumen ejecutivo de casos donde incidencias y comentarios apuntan a la misma fricción · {period_label}",
    )
    scope = summarize_attribution_chains(chain_df)
    _add_stat_card(
        slide,
        left=0.66,
        top=1.48,
        width=2.8,
        height=1.10,
        label="Cómo se enlaza",
        value=_touchpoint_method_label(touchpoint_source),
        accent=BBVA_COLORS["blue"],
        hint="Incidencia -> comentario -> NPS",
    )
    _add_stat_card(
        slide,
        left=3.62,
        top=1.48,
        width=2.0,
        height=1.10,
        label="INC vinculadas",
        value=str(int(scope.get("linked_incidents_total", 0))),
        accent=BBVA_COLORS["orange"],
    )
    _add_stat_card(
        slide,
        left=5.78,
        top=1.48,
        width=2.0,
        height=1.10,
        label="Comentarios enlazados",
        value=str(int(scope.get("linked_comments_total", 0))),
        accent=BBVA_COLORS["green"],
    )
    _add_stat_card(
        slide,
        left=7.94,
        top=1.48,
        width=2.0,
        height=1.10,
        label="Vínculos validados",
        value=str(int(scope.get("linked_pairs_total", 0))),
        accent=BBVA_COLORS["red"],
    )
    _panel(slide, left=0.66, top=2.88, width=7.25, height=4.02, title="Casos con mayor riesgo")
    _figure_in_panel(
        slide,
        figure=_journeys_overview_fig(chain_df),
        left=0.82,
        top=3.24,
        width=6.93,
        height=3.36,
        empty_note="No hay chains defendibles para resumir el periodo.",
    )
    rows = []
    if chain_df is not None and not chain_df.empty:
        for row in chain_df.head(5).itertuples():
            rows.append(
                [
                    str(row.nps_topic),
                    _fmt_num_or_nd(getattr(row, "priority", np.nan)),
                    _fmt_num_or_nd(getattr(row, "confidence", np.nan)),
                    _fmt_num_or_nd(getattr(row, "nps_points_at_risk", np.nan)),
                    _clip(getattr(row, "owner_role", "") or "n/d", 20),
                ]
            )
    _add_compact_table(
        slide,
        left=8.10,
        top=2.88,
        width=4.58,
        title="Resumen por caso",
        headers=["Caso", "Prio.", "Conf.", "Riesgo", "Equipo"],
        rows=rows or [["Sin evidencia", "-", "-", "-", "-"]],
        row_height=0.32,
        col_width_ratios=[2.4, 0.6, 0.6, 0.7, 1.1],
        clip_lengths=[34, 6, 6, 8, 18],
        font_size_pt=8.9,
    )


def _add_chain_scenario_slide(
    prs: Presentation,
    *,
    chain_row: pd.Series,
    idx: int,
    focus_name: str,
    period_label: str,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    title = _clip(chain_row.get("nps_topic", f"Cadena {idx}"), 70)
    linked_incidents = int(_safe_int(chain_row.get("linked_incidents", 0), default=0))
    linked_comments = int(_safe_int(chain_row.get("linked_comments", 0), default=0))
    touchpoint = _clip(chain_row.get("touchpoint", "Touchpoint"), 36)
    focus_label = _focus_risk_label(focus_name)
    _add_header(
        slide,
        title=f"9.{idx} Caso causal",
        subtitle=f"{title} · {touchpoint} · {period_label}",
    )
    chain_statement = (
        f"{linked_incidents} incidencias de Helix y {linked_comments} comentarios de cliente "
        f"convergen en '{title}' y explican riesgo de {focus_label}."
    )
    banner = _panel(
        slide,
        left=0.66,
        top=1.48,
        width=12.0,
        height=0.80,
        title="Cadena de análisis",
        subtitle=chain_statement,
        fill=BBVA_COLORS["white"],
        border=BBVA_COLORS["sky"],
        title_size=13,
    )
    del banner

    metrics = [
        (
            _focus_probability_label(focus_name),
            _fmt_pct_or_nd(chain_row.get("detractor_probability", np.nan)),
            BBVA_COLORS["red"],
        ),
        (
            "Cambio esperado en NPS",
            _fmt_signed_or_nd(chain_row.get("nps_delta_expected", np.nan)),
            BBVA_COLORS["orange"],
        ),
        (
            "Impacto total",
            f"{_fmt_num_or_nd(chain_row.get('total_nps_impact', np.nan))} pts",
            BBVA_COLORS["blue"],
        ),
        (
            "Solidez de la evidencia",
            _fmt_num_or_nd(chain_row.get("confidence", np.nan)),
            BBVA_COLORS["green"],
        ),
        (
            "Vínculos validados",
            str(int(_safe_int(chain_row.get("linked_pairs", 0), default=0))),
            BBVA_COLORS["sky"],
        ),
        (
            "Prioridad sugerida",
            _fmt_num_or_nd(chain_row.get("priority", np.nan)),
            BBVA_COLORS["red"],
        ),
        (
            "NPS en riesgo",
            f"{_fmt_num_or_nd(chain_row.get('nps_points_at_risk', np.nan))} pts",
            BBVA_COLORS["red"],
        ),
        (
            "NPS recuperable",
            f"{_fmt_num_or_nd(chain_row.get('nps_points_recoverable', np.nan))} pts",
            BBVA_COLORS["green"],
        ),
        (
            "Equipo responsable",
            _clip(chain_row.get("owner_role", "n/d"), 24),
            BBVA_COLORS["blue"],
        ),
    ]
    for pos, (label, value, accent) in enumerate(metrics):
        row = pos // 3
        col = pos % 3
        _add_stat_card(
            slide,
            left=0.66 + col * 4.05,
            top=2.52 + row * 1.30,
            width=3.78,
            height=1.08,
            label=label,
            value=value,
            accent=accent,
        )

    _add_bullet_lines(
        slide,
        left=0.66,
        top=5.28,
        width=5.75,
        height=1.62,
        title="Incidencias relacionadas",
        lines=[
            _clean_evidence_excerpt(line, max_len=112)
            for line in _chain_list(chain_row.get("incident_examples"))[:3]
        ]
        or ["No se han encontrado evidencias Helix adicionales para este escenario."],
        accent=BBVA_COLORS["orange"],
    )
    _add_bullet_lines(
        slide,
        left=6.63,
        top=5.28,
        width=6.03,
        height=1.62,
        title="Comentarios de cliente",
        lines=[
            _clean_evidence_excerpt(line, max_len=116)
            for line in _chain_list(chain_row.get("comment_examples"))[:2]
        ]
        or ["No se han encontrado verbatims adicionales para este escenario."],
        accent=BBVA_COLORS["red"],
    )


def _add_chain_detail_slide(
    prs: Presentation,
    *,
    chain_row: pd.Series,
    idx: int,
    period_label: str,
    chain_df: pd.DataFrame,
    by_topic_daily: Optional[pd.DataFrame],
    lag_days_by_topic: Optional[pd.DataFrame],
    lag_weeks_by_topic: Optional[pd.DataFrame],
    changepoints_by_topic: Optional[pd.DataFrame],
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    title = _clip(chain_row.get("nps_topic", f"Cadena {idx}"), 72)
    _add_header(
        slide,
        title=f"10.{idx} Detalle del caso",
        subtitle=f"{title} · posición del caso, comentarios y secuencia temporal · {period_label}",
    )
    _panel(slide, left=0.66, top=1.48, width=4.05, height=2.35, title="Posición del caso")
    _figure_in_panel(
        slide,
        figure=_chain_portfolio_fig(chain_df, highlight_topic=str(chain_row.get("nps_topic", ""))),
        left=0.82,
        top=1.84,
        width=3.73,
        height=1.83,
        empty_note="No hay cartera suficiente para posicionar el caso.",
    )
    quant_lines = [
        f"Palanca: {_clip(chain_row.get('palanca', 'n/d'), 28)}",
        f"Subpalanca: {_clip(chain_row.get('subpalanca', 'n/d'), 28)}",
        f"Retraso estimado: {_lag_days_for_topic(_matching_topic_for_chain(chain_row, by_topic_daily), lag_days_by_topic=lag_days_by_topic, lag_weeks_by_topic=lag_weeks_by_topic, rationale_df=pd.DataFrame(), ranking_df=None)} días",
        f"Puntos de cambio: {len(_changepoints_map(changepoints_by_topic).get(_matching_topic_for_chain(chain_row, by_topic_daily), []))}",
        f"Tipo de acción: {_clip(_action_lane_label(chain_row.get('action_lane', 'n/d')), 34)}",
        f"Equipo responsable: {_clip(chain_row.get('owner_role', 'n/d'), 30)}",
    ]
    _add_bullet_lines(
        slide,
        left=4.92,
        top=1.48,
        width=3.28,
        height=2.35,
        title="Resumen cuantitativo",
        lines=quant_lines,
        accent=BBVA_COLORS["blue"],
    )
    _panel(slide, left=8.40, top=1.48, width=4.26, height=2.35, title="Mapa de comentarios")
    _figure_in_panel(
        slide,
        figure=_chain_comment_heatmap_fig(chain_row),
        left=8.56,
        top=1.84,
        width=3.94,
        height=1.83,
        empty_note="No hay suficiente detalle temporal en los comentarios vinculados.",
    )
    _panel(slide, left=0.66, top=4.06, width=8.22, height=2.84, title="Secuencia temporal")
    _figure_in_panel(
        slide,
        figure=_chain_temporal_fig(
            chain_row,
            by_topic_daily=by_topic_daily,
            lag_days_by_topic=lag_days_by_topic,
            lag_weeks_by_topic=lag_weeks_by_topic,
            changepoints_by_topic=changepoints_by_topic,
        ),
        left=0.82,
        top=4.42,
        width=7.90,
        height=2.28,
        empty_note="No hay cobertura suficiente para la vista temporal de la cadena.",
    )
    _add_bullet_lines(
        slide,
        left=9.10,
        top=4.06,
        width=3.56,
        height=2.84,
        title="Evidencias clave",
        lines=(
            [
                f"Helix: {_clean_evidence_excerpt(line, max_len=98)}"
                for line in _chain_list(chain_row.get("incident_examples"))[:2]
            ]
            + [
                f"Cliente: {_clean_evidence_excerpt(line, max_len=98)}"
                for line in _chain_list(chain_row.get("comment_examples"))[:2]
            ]
        )
        or ["Sin evidencias adicionales en el detalle."],
        accent=BBVA_COLORS["orange"],
    )


def generate_business_review_ppt(
    *,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    period_start: date,
    period_end: date,
    focus_name: str,
    overall_weekly: pd.DataFrame,
    rationale_df: pd.DataFrame,
    nps_points_at_risk: float,
    nps_points_recoverable: float,
    top3_incident_share: float,
    median_lag_weeks: float,
    story_md: str,
    script_8slides_md: str,
    attribution_df: Optional[pd.DataFrame] = None,
    ranking_df: Optional[pd.DataFrame] = None,
    by_topic_daily: Optional[pd.DataFrame] = None,
    lag_days_by_topic: Optional[pd.DataFrame] = None,
    by_topic_weekly: Optional[pd.DataFrame] = None,
    lag_weeks_by_topic: Optional[pd.DataFrame] = None,
    template_name: str = "Plantilla corporativa fija v1",
    corporate_fixed: bool = True,
    logo_path: Optional[Path] = None,
    selected_nps_df: Optional[pd.DataFrame] = None,
    comparison_nps_df: Optional[pd.DataFrame] = None,
    template_path: Optional[Path] = None,
    incident_evidence_df: Optional[pd.DataFrame] = None,
    changepoints_by_topic: Optional[pd.DataFrame] = None,
    incident_timeline_df: Optional[pd.DataFrame] = None,
    hotspot_focus_note: str = "",
    touchpoint_source: str = "",
    executive_journey_catalog: Optional[list[dict[str, object]]] = None,
) -> BusinessPptResult:
    """Build a business deck aligned to the selected period and BBVA corporate template."""
    del (
        script_8slides_md,
        template_name,
        corporate_fixed,
        logo_path,
        incident_evidence_df,
        incident_timeline_df,
        hotspot_focus_note,
        median_lag_weeks,
        rationale_df,
        ranking_df,
        by_topic_weekly,
        executive_journey_catalog,
    )

    prs = build_presentation(template_path=template_path, workspace_root=Path.cwd())
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)

    period_label = f"{_safe_date(period_start)} -> {_safe_date(period_end)}"

    selected_raw = _coerce_nps_records(selected_nps_df)
    compare_raw = _coerce_nps_records(comparison_nps_df)
    current_period, baseline_period = _split_period_frames(
        compare_raw if not compare_raw.empty else selected_raw,
        period_start=period_start,
        period_end=period_end,
    )
    current_source_period, baseline_source_period = _split_source_period_frames(
        comparison_nps_df if comparison_nps_df is not None else selected_nps_df,
        period_start=period_start,
        period_end=period_end,
    )
    if current_source_period.empty and selected_nps_df is not None:
        current_source_period, _ = _split_source_period_frames(
            selected_nps_df,
            period_start=period_start,
            period_end=period_end,
        )
    if selected_raw.empty:
        selected_raw = current_period.copy()
    if selected_raw.empty:
        selected_raw = compare_raw[
            (compare_raw["date"] >= pd.Timestamp(period_start))
            & (compare_raw["date"] <= pd.Timestamp(period_end))
        ].copy()

    daily_signals, _ = _prepare_daily_signals(
        overall_weekly,
        period_start=period_start,
        period_end=period_end,
    )
    daily_mix = _daily_group_mix(selected_raw)
    if daily_mix.empty and not daily_signals.empty:
        daily_mix = daily_signals[["date", "nps_mean", "detractor_rate"]].copy()
        daily_mix["responses"] = 0.0
        daily_mix["passive_rate"] = (1.0 - daily_mix["detractor_rate"]).clip(lower=0.0)
        daily_mix["promoter_rate"] = 0.0
        daily_mix["nps_classic"] = (1.0 - daily_mix["detractor_rate"] * 2.0) * 100.0

    overview = _period_overview(selected_raw)
    text_topics_df = _text_topics_table(selected_raw, top_k=10)
    palanca_change = _driver_change_table(current_period, baseline_period, dimension="Palanca")
    subpalanca_change = _driver_change_table(
        current_period, baseline_period, dimension="Subpalanca"
    )
    palanca_matrix = _group_matrix(selected_raw, dimension="Palanca")
    subpalanca_matrix = _group_matrix(selected_raw, dimension="Subpalanca")
    gap_df = _gap_vs_overall_table(selected_raw, top_k=10)
    palanca_gap_df = _dimension_gap_table(selected_raw, dimension="Palanca", top_k=10)
    subpalanca_gap_df = _dimension_gap_table(selected_raw, dimension="Subpalanca", top_k=10)
    opportunities_df = _opportunities_table(selected_raw)
    chains = attribution_df.copy() if attribution_df is not None else pd.DataFrame()

    _add_cover_slide(
        prs,
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        service_origin_n2=service_origin_n2,
        period_start=period_start,
        period_end=period_end,
        overview=overview,
        story_md=story_md,
    )
    _add_overview_slide(
        prs,
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        period_label=period_label,
        period_end=period_end,
        overview=overview,
        selected_nps_df=selected_nps_df,
        period_days=(pd.Timestamp(period_end) - pd.Timestamp(period_start)).days + 1,
    )
    _add_deep_dive_slide(prs, period_label=period_label, text_topics_df=text_topics_df)
    _add_topic_timing_slide(
        prs,
        period_label=period_label,
        period_days=(pd.Timestamp(period_end) - pd.Timestamp(period_start)).days + 1,
        selected_nps_df=selected_nps_df,
    )
    if not baseline_period.empty:
        current_label = f"{_safe_date(period_start)} -> {_safe_date(period_end)}"
        baseline_label = (
            f"{_safe_date(baseline_period['date'].min())} -> {_safe_date(baseline_period['date'].max())}"
            if not baseline_period.empty
            else "sin base"
        )
        _add_change_vs_past_slide(
            prs,
            period_label=period_label,
            current_label=current_label,
            baseline_label=baseline_label,
            current_source_df=current_source_period,
            baseline_source_df=baseline_source_period,
        )
    _add_pain_by_group_slide(
        prs,
        period_label=period_label,
        selected_nps_df=selected_nps_df,
    )
    _add_gap_slide(
        prs,
        period_label=period_label,
        palanca_gap_df=palanca_gap_df,
        subpalanca_gap_df=subpalanca_gap_df,
    )
    _add_opportunity_slide(prs, period_label=period_label, opportunities_df=opportunities_df)
    _add_causal_timeline_slide(
        prs,
        period_label=period_label,
        daily_mix=daily_mix,
        overall_daily=daily_signals,
        nps_points_at_risk=nps_points_at_risk,
        nps_points_recoverable=nps_points_recoverable,
        top3_incident_share=top3_incident_share,
    )
    _add_journeys_summary_slide(
        prs,
        period_label=period_label,
        touchpoint_source=touchpoint_source,
        chain_df=chains,
    )
    if chains is not None and not chains.empty:
        for idx, (_, chain_row) in enumerate(chains.head(3).iterrows(), start=1):
            _add_chain_scenario_slide(
                prs,
                chain_row=chain_row,
                idx=idx,
                focus_name=focus_name,
                period_label=period_label,
            )
            _add_chain_detail_slide(
                prs,
                chain_row=chain_row,
                idx=idx,
                period_label=period_label,
                chain_df=chains,
                by_topic_daily=by_topic_daily,
                lag_days_by_topic=lag_days_by_topic,
                lag_weeks_by_topic=lag_weeks_by_topic,
                changepoints_by_topic=changepoints_by_topic,
            )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    file_name = f"nps-incidencias-{_slug(service_origin)}-{_slug(service_origin_n1)}-{stamp}.pptx"

    buff = BytesIO()
    prs.save(buff)
    return BusinessPptResult(
        file_name=file_name, content=buff.getvalue(), slide_count=len(prs.slides)
    )
