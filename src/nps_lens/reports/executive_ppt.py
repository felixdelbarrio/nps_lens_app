from __future__ import annotations

import contextlib
import hashlib
import math
import os
import re
import tempfile
import textwrap
import unicodedata
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from threading import RLock
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

from nps_lens.analytics.drivers import driver_table
from nps_lens.analytics.incident_attribution import (
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
)
from nps_lens.analytics.nps_helix_link import build_nps_topic
from nps_lens.analytics.opportunities import rank_opportunities
from nps_lens.analytics.text_mining import extract_topics
from nps_lens.core.nps_math import daily_metrics as shared_daily_metrics
from nps_lens.design.tokens import (
    DesignTokens,
    bbva_typography_tokens,
    executive_report_palette,
)
from nps_lens.domain.causal_methods import get_causal_method_spec
from nps_lens.reports.content_selectors import (
    parse_markdown_strong,
    select_causal_scenarios,
    select_negative_delta_rows,
    select_nonzero_kpis,
    select_opportunities,
    select_text_clusters,
)
from nps_lens.reports.editorial_tokens import (
    CHANGE_SLIDE_LAYOUT,
    EDITORIAL_COPY,
    EDITORIAL_LIMITS,
    EXECUTIVE_TABLE_STYLE,
    JOURNEY_SUMMARY_LAYOUT,
)
from nps_lens.reports.ppt_template import (
    CorporatePresentationTheme,
    build_presentation,
    resolve_layout,
)
from nps_lens.reports.presentation_context import (
    CausalEvidenceRecord,
    CausalScenarioViewModel,
    CausalViewModel,
    DimensionViewModel,
    PresentationContext,
)
from nps_lens.services.analytics.kpis_service import (
    build_period_boundary_kpis,
    build_period_kpis,
    compute_score_kpis,
    format_delta,
    format_metric,
    format_percentage,
    format_volume,
)
from nps_lens.ui.charts import (
    _compact_axis_label,
    chart_causal_entity_bar,
    chart_cohort_heatmap,
    chart_daily_nps_committee_stack,
    chart_driver_delta,
    chart_opportunities_bar,
    chart_topic_bars,
)
from nps_lens.ui.historic_changes import get_changes_vs_historic
from nps_lens.ui.narratives import explain_opportunities
from nps_lens.ui.population import POP_ALL
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
REPORT_ASSETS = Path(__file__).resolve().parents[3] / "assets" / "ppt" / "bbva"
_FIGURE_PNG_CACHE: OrderedDict[str, bytes] = OrderedDict()
_FIGURE_PNG_LOCK = RLock()
_FIGURE_PNG_CACHE_LIMIT = 24


@dataclass(frozen=True)
class BusinessPptResult:
    file_name: str
    content: bytes
    slide_count: int
    saved_path: str = ""


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


def _fmt_locale_number(
    value: object,
    *,
    decimals: int = 0,
    signed: bool = False,
    default: str = "n/d",
) -> str:
    f = _safe_float(value, default=float("nan"))
    if not np.isfinite(f):
        return default
    precision = max(int(decimals), 0)
    rendered = f"{f:+,.{precision}f}" if signed else f"{f:,.{precision}f}"
    return rendered.replace(",", "_").replace(".", ",").replace("_", ".")


def _fmt_count_or_nd(v: object) -> str:
    return format_volume(v, default="n/d")


def _fmt_count_with_label(v: object, *, singular: str, plural: str) -> str:
    count = _safe_int(v, default=-1)
    label = singular if count == 1 else plural
    return f"**{_fmt_count_or_nd(v)}** {label}"


def _fmt_pct_or_nd(v: object, decimals: int = 2) -> str:
    if decimals == 2:
        return format_percentage(v)
    f = _safe_float(v, default=float("nan"))
    return "n/d" if not np.isfinite(f) else f"{_fmt_locale_number(f * 100.0, decimals=decimals)}%"


def _fmt_signed_or_nd(v: object, decimals: int = 2) -> str:
    if decimals == 2:
        return format_metric(v, signed=True)
    return _fmt_locale_number(v, decimals=decimals, signed=True)


def _fmt_num_or_nd(v: object, decimals: int = 2) -> str:
    if decimals == 2:
        return format_metric(v)
    return _fmt_locale_number(v, decimals=decimals)


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
    with contextlib.suppress(Exception):
        tf.vertical_anchor = MSO_VERTICAL_ANCHOR.TOP


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
            "score medio",
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


_CATEGORY_CANONICALS = {
    "funcionamiento continuo": "Funcionamiento continuo",
    "agregar funcionalidad": "Agregar funcionalidad",
    "fallas en el login": "Fallas en el login",
}


def _category_key(value: object) -> str:
    clean = " ".join(str(value or "").split()).strip().casefold()
    if clean in {"", "nan", "none", "null", "<na>"}:
        return ""
    clean = unicodedata.normalize("NFKD", clean).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", clean).strip()


def _normalize_category_value(value: object) -> str:
    clean = " ".join(str(value or "").split()).strip()
    key = _category_key(clean)
    if not key:
        return ""
    return _CATEGORY_CANONICALS.get(key, clean)


def _normalize_presentation_categories(
    df: pd.DataFrame,
    *,
    columns: Iterable[str] = ("Palanca", "Subpalanca"),
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", []))
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = out[column].map(_normalize_category_value)
    return out


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
    out["Palanca"] = out.get("Palanca", pd.Series([""] * len(out), index=out.index)).map(
        _normalize_category_value
    )
    out["Subpalanca"] = out.get("Subpalanca", pd.Series([""] * len(out), index=out.index)).map(
        _normalize_category_value
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
    out = _normalize_presentation_categories(out)

    start_ts = pd.Timestamp(period_start)
    end_ts = pd.Timestamp(period_end)
    current = out[(out["Fecha"] >= start_ts) & (out["Fecha"] <= end_ts)].copy()
    baseline = out[out["Fecha"] < start_ts].copy()
    if baseline.empty:
        baseline = out[(out["Fecha"] < start_ts) | (out["Fecha"] > end_ts)].copy()
    return current, baseline


def _period_overview(
    current_nps_df: pd.DataFrame,
    *,
    period_kpis: Optional[dict[str, object]] = None,
) -> dict[str, object]:
    period_block = (
        period_kpis.get("period", {})
        if isinstance(period_kpis, dict) and isinstance(period_kpis.get("period"), dict)
        else {}
    )
    aggregate_payload = (
        period_block.get("kpis", {}) if isinstance(period_block.get("kpis"), dict) else {}
    )
    aggregate_kpis = (
        compute_score_kpis(pd.DataFrame())
        if aggregate_payload
        else compute_score_kpis(current_nps_df)
    )
    temporal_kpis = (
        period_kpis.get("temporal", {})
        if isinstance(period_kpis, dict) and isinstance(period_kpis.get("temporal"), dict)
        else build_period_boundary_kpis(current_nps_df)
    )
    temporal_base = temporal_kpis.get("base_kpis", {})
    temporal_actual = temporal_kpis.get("kpis", {})
    temporal_deltas = temporal_kpis.get("deltas", {})
    start_classic = (
        _safe_float(temporal_base.get("classic_nps"), default=float("nan"))
        if isinstance(temporal_base, dict)
        else float("nan")
    )
    end_classic = (
        _safe_float(temporal_actual.get("classic_nps"), default=float("nan"))
        if isinstance(temporal_actual, dict)
        else float("nan")
    )
    start_detr = (
        _safe_float(temporal_base.get("detractor_rate"), default=float("nan"))
        if isinstance(temporal_base, dict)
        else float("nan")
    )
    end_detr = (
        _safe_float(temporal_actual.get("detractor_rate"), default=float("nan"))
        if isinstance(temporal_actual, dict)
        else float("nan")
    )
    classic_delta_payload = (
        temporal_deltas.get("classic_nps", {}) if isinstance(temporal_deltas, dict) else {}
    )
    detractor_delta_payload = (
        temporal_deltas.get("detractor_rate", {}) if isinstance(temporal_deltas, dict) else {}
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
        "comments": _safe_int(
            aggregate_payload.get("comments", aggregate_kpis.comments), default=0
        ),
        "nps_mean": _safe_float(
            aggregate_payload.get("nps_average", aggregate_kpis.nps_average),
            default=float("nan"),
        ),
        "detractor_rate": _safe_float(
            aggregate_payload.get("detractor_rate", aggregate_kpis.detractor_rate),
            default=float("nan"),
        ),
        "promoter_rate": _safe_float(
            aggregate_payload.get("promoter_rate", aggregate_kpis.promoter_rate),
            default=float("nan"),
        ),
        "passive_rate": _safe_float(
            aggregate_payload.get("neutral_rate", aggregate_kpis.neutral_rate),
            default=float("nan"),
        ),
        "classic_nps": _safe_float(
            aggregate_payload.get("classic_nps", aggregate_kpis.classic_nps),
            default=float("nan"),
        ),
        "start_classic": start_classic,
        "end_classic": end_classic,
        "classic_delta": _safe_float(
            (
                classic_delta_payload.get("value")
                if isinstance(classic_delta_payload, dict)
                else np.nan
            ),
            default=float("nan"),
        ),
        "start_detr": start_detr,
        "end_detr": end_detr,
        "detractor_delta_pp": (
            _safe_float(
                (
                    detractor_delta_payload.get("value")
                    if isinstance(detractor_delta_payload, dict)
                    else np.nan
                ),
                default=float("nan"),
            )
            * 100.0
        ),
        "pain_point": pain_point,
        "strength_point": strength_point,
    }


def _daily_metrics_for_ppt(nps_df: pd.DataFrame) -> pd.DataFrame:
    if nps_df is None or nps_df.empty:
        return pd.DataFrame(
            columns=[
                "day",
                "n",
                "det_pct",
                "pas_pct",
                "pro_pct",
                "classic_nps",
                "detractor_rate",
                "passive_rate",
                "promoter_rate",
                "nps_avg",
            ]
        )
    date_col = "date" if "date" in nps_df.columns else "Fecha"
    if date_col not in nps_df.columns or "NPS" not in nps_df.columns:
        return pd.DataFrame()
    return shared_daily_metrics(nps_df, days=None, date_col=date_col, score_col="NPS")


def _daily_group_mix_from_metrics(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics is None or metrics.empty:
        return pd.DataFrame()
    required = {
        "day",
        "n",
        "detractor_rate",
        "passive_rate",
        "promoter_rate",
        "classic_nps",
        "nps_avg",
    }
    if not required.issubset(set(metrics.columns)):
        return pd.DataFrame()
    out = metrics[
        [
            "day",
            "n",
            "detractor_rate",
            "passive_rate",
            "promoter_rate",
            "classic_nps",
            "nps_avg",
        ]
    ].copy()
    return out.rename(
        columns={
            "day": "date",
            "n": "responses",
            "classic_nps": "nps_classic",
            "nps_avg": "nps_mean",
        }
    )


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
        "n_baseline",
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
        base[["value", "n", "nps", "detractor_rate"]],
        on="value",
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(columns=cols)
    merged = merged.rename(
        columns={
            "nps": "nps_baseline",
            "n": "n_baseline",
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


def _opportunities_table(
    current_nps_df: pd.DataFrame,
    *,
    dimension: str = "Palanca",
    min_n: int = 200,
) -> pd.DataFrame:
    cols = ["dimension", "value", "n", "current_nps", "potential_uplift", "confidence", "why"]
    if current_nps_df is None or current_nps_df.empty:
        return pd.DataFrame(columns=cols)
    work = _normalize_presentation_categories(current_nps_df, columns=[dimension])
    if "nps_topic" not in work.columns or work["nps_topic"].astype(str).str.strip().eq("").all():
        work["nps_topic"] = build_nps_topic(work).astype(str).fillna("").str.strip()
    dims = [str(dimension)] if str(dimension or "").strip() in work.columns else []
    if not dims:
        return pd.DataFrame(columns=cols)
    work = work[work[dimension].astype(str).str.strip().ne("")].copy()
    if work.empty:
        return pd.DataFrame(columns=cols)
    rows = rank_opportunities(
        work,
        dimensions=dims,
        min_n=max(1, int(min_n)),
    )
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([row.__dict__ for row in rows])[cols]


def _build_overview_figure(
    history_nps_df: Optional[pd.DataFrame],
    *,
    period_start: date,
    period_end: date,
    metrics: Optional[pd.DataFrame] = None,
) -> Optional[go.Figure]:
    history = _coerce_nps_records(history_nps_df)
    if history.empty:
        return None
    full_metrics = metrics if metrics is not None else _daily_metrics_for_ppt(history)
    history_days = max(int((history["date"].max() - history["date"].min()).days) + 1, 1)
    fig = chart_daily_nps_committee_stack(
        history,
        get_theme("light"),
        days=history_days,
        metrics=full_metrics,
    )
    if fig is None:
        return None
    fig.update_layout(
        legend=dict(orientation="h", x=0.0, y=1.18, yanchor="bottom", title_text=""),
        margin=dict(l=72, r=86, t=84, b=76),
    )
    fig.add_vrect(
        x0=pd.Timestamp(period_start),
        x1=pd.Timestamp(period_end),
        fillcolor=f"#{BBVA_COLORS['sky']}",
        opacity=0.12,
        line_width=0,
        annotation_text="Periodo solicitado",
        annotation_position="top left",
    )
    fig.update_xaxes(
        side="bottom",
        ticklabelposition="outside bottom",
        automargin=True,
        tickfont=dict(size=20),
        title_font=dict(size=20),
    )
    fig.update_yaxes(tickfont=dict(size=18), title_font=dict(size=18))
    return fig


def _build_text_topic_figure(text_topics_df: pd.DataFrame) -> Optional[go.Figure]:
    topic_fig = chart_topic_bars(
        text_topics_df, get_theme("light"), top_k=EDITORIAL_LIMITS.max_text_chart_clusters
    )
    if topic_fig is None or text_topics_df.empty:
        return topic_fig
    topic_rows = (
        text_topics_df.sort_values("n", ascending=False)
        .head(EDITORIAL_LIMITS.max_text_chart_clusters)
        .copy()
    )
    topic_labels = []
    for row in topic_rows.itertuples():
        terms = [str(term).strip() for term in list(row.top_terms)[:2] if str(term).strip()]
        topic_labels.append(_clip(f"#{int(row.cluster_id)} · {', '.join(terms)}", 24))
    with contextlib.suppress(Exception):
        topic_fig.data[0].y = topic_labels
    counts = pd.to_numeric(text_topics_df.get("n"), errors="coerce").dropna()
    xmax = float(counts.max()) if not counts.empty else 0.0
    topic_fig.update_xaxes(
        title_text="Comentarios",
        nticks=5,
        dtick=2000 if xmax >= 6000 else 1000 if xmax >= 2000 else None,
        tickformat="~s",
        tickfont=dict(size=20),
        title_font=dict(size=20),
    )
    y_font = 26 if len(topic_rows) <= 5 else 22 if len(topic_rows) <= 8 else 19
    topic_fig.update_yaxes(tickfont=dict(size=y_font), automargin=True)
    topic_fig.update_layout(margin=dict(l=270, r=28, t=18, b=54), bargap=0.26)
    return topic_fig


def _build_driver_delta_figure(
    delta_df: pd.DataFrame, *, panel_height_in: float, max_rows: int = 12
) -> Optional[go.Figure]:
    """Render the historic-change chart from the shared Insights dataset only.

    The PPT layer receives the single-source dataset used by Insights and does
    not recompute deltas, counts, ranking or top-N. The only work here is visual
    normalization for the committee 16:9 deck. Labels are never rewritten after
    ``chart_driver_delta`` creates the figure; mutating Plotly categorical y
    values after construction can create duplicate categories in PowerPoint
    exports.
    """

    if delta_df.empty:
        return None

    row_count = max(1, min(int(max_rows), len(delta_df)))
    fig = chart_driver_delta(delta_df, get_theme("light"), top_k=row_count)
    if fig is None:
        return None

    rendered_labels = [str(value) for value in list(getattr(fig.data[0], "y", []))]
    label_count = len(rendered_labels) or row_count
    max_len = max((len(label.replace("<br>", " ")) for label in rendered_labels), default=0)
    left_margin = 250 if max_len >= 26 else 225 if max_len >= 18 else 205
    y_font_size = 10 if label_count >= 11 else 11 if label_count >= 9 else 12

    fig.update_traces(
        text=None,
        textposition="none",
        cliponaxis=True,
    )
    fig.update_yaxes(
        tickfont=dict(size=y_font_size, family=BBVA_FONT_MEDIUM),
        automargin=False,
        title_text="",
    )
    fig.update_xaxes(
        title_text="Delta NPS Clásico (actual - base)",
        tickfont=dict(size=10, family=BBVA_FONT_BODY),
        title_font=dict(size=11, family=BBVA_FONT_MEDIUM),
        nticks=5,
        zeroline=True,
        zerolinecolor="#" + BBVA_COLORS["muted"],
        zerolinewidth=1,
    )
    fig.update_layout(
        margin=dict(
            l=left_margin,
            r=22,
            t=6,
            b=38 if panel_height_in <= 3.0 else 46,
        ),
        bargap=0.24 if label_count >= 11 else 0.20,
        uniformtext=dict(mode="show", minsize=8),
    )
    return fig


def _build_web_heatmap_figure(source_df: pd.DataFrame, *, row_dim: str) -> Optional[go.Figure]:
    required = {row_dim, "Canal", "NPS"}
    if source_df is None or source_df.empty or not required.issubset(set(source_df.columns)):
        return None
    chart_df = _normalize_presentation_categories(source_df, columns=[row_dim])
    chart_df = chart_df.dropna(subset=[row_dim, "Canal", "NPS"]).copy()
    if chart_df.empty:
        return None
    chart_df[row_dim] = chart_df[row_dim].astype(str).str.strip()
    chart_df["Canal"] = chart_df["Canal"].astype(str).str.strip()
    chart_df = chart_df[
        chart_df[row_dim].ne("") & chart_df["Canal"].str.casefold().eq("web")
    ].copy()
    if chart_df.empty:
        return None
    fig = chart_cohort_heatmap(
        chart_df, get_theme("light"), row_dim=row_dim, col_dim="Canal", min_n=1
    )
    if fig is None:
        return None
    row_stats = (
        chart_df.groupby(row_dim, as_index=False)
        .agg(n=("NPS", "size"), nps=("NPS", "mean"))
        .sort_values(["nps", "n"], ascending=[True, False])
        .head(EDITORIAL_LIMITS.max_web_rows)
    )
    labels = [
        _compact_axis_label(value, width=24, max_lines=2, max_chars=44).replace("<br>", " ")
        for value in row_stats[row_dim].astype(str).tolist()
    ]
    with contextlib.suppress(Exception):
        fig.data[0].y = labels
    fig.update_yaxes(
        tickfont=dict(size=30 if len(labels) <= 5 else 26, family=BBVA_FONT_MEDIUM),
        automargin=True,
        title_text="",
    )
    fig.update_xaxes(
        side="bottom", tickangle=0, tickfont=dict(size=22), automargin=True, title_text=""
    )
    fig.update_layout(margin=dict(l=390, r=116, t=18, b=54))
    fig.update_coloraxes(
        showscale=True,
        colorbar=dict(
            title="Score",
            tickmode="array",
            tickvals=[0, 2, 6, 8, 10],
            len=0.74,
            y=0.5,
            thickness=14,
        ),
    )
    return fig


def _build_web_dimension_table(source_df: pd.DataFrame, *, dimension: str) -> pd.DataFrame:
    cols = ["value", "n", "nps", "detractor_rate"]
    required = {dimension, "Canal", "NPS"}
    if source_df is None or source_df.empty or not required.issubset(set(source_df.columns)):
        return pd.DataFrame(columns=cols)
    work = _normalize_presentation_categories(source_df, columns=[dimension])
    work = work.dropna(subset=[dimension, "Canal", "NPS"]).copy()
    work[dimension] = work[dimension].astype(str).str.strip()
    work["Canal"] = work["Canal"].astype(str).str.strip()
    work["NPS"] = pd.to_numeric(work["NPS"], errors="coerce")
    work = (
        work[work[dimension].ne("") & work["Canal"].str.casefold().eq("web")]
        .dropna(subset=["NPS"])
        .copy()
    )
    if work.empty:
        return pd.DataFrame(columns=cols)
    out = (
        work.groupby(dimension, as_index=False)
        .agg(
            n=("NPS", "size"),
            nps=("NPS", "mean"),
            detractor_rate=(
                "NPS",
                lambda s: float((pd.to_numeric(s, errors="coerce") <= 6).mean()),
            ),
        )
        .rename(columns={dimension: "value"})
    )
    return (
        out.sort_values(["nps", "n"], ascending=[True, False])
        .head(EDITORIAL_LIMITS.max_web_rows)[cols]
        .copy()
    )


def _build_opportunity_figure(opp_df: pd.DataFrame) -> Optional[go.Figure]:
    fig = chart_opportunities_bar(opp_df, get_theme("light"), top_k=max(len(opp_df), 1))
    if fig is None or opp_df.empty:
        return fig
    plot_df = select_opportunities(opp_df, max_rows=EDITORIAL_LIMITS.max_opportunities)
    label_count = len(plot_df)
    label_lengths = (
        plot_df["label"].astype(str).str.replace("<br>", " ", regex=False).str.len()
        if "label" in plot_df.columns
        else pd.Series(dtype=float)
    )
    max_len = int(label_lengths.max() or 0)
    y_font_size = 30 if label_count <= 5 else 27 if label_count <= 7 else 24
    left_margin = 340 if max_len >= 28 else 300 if max_len >= 22 else 255
    uplift = pd.to_numeric(plot_df.get("potential_uplift"), errors="coerce")
    text_values = [
        _fmt_signed_or_nd(value, decimals=1) if np.isfinite(value) else ""
        for value in uplift.tolist()
    ]
    with contextlib.suppress(Exception):
        fig.data[0].text = text_values
        fig.data[0].textposition = "outside"
        fig.data[0].cliponaxis = False
        fig.data[0].textfont.size = 20
    fig.update_yaxes(
        title_text="", tickfont=dict(size=y_font_size, family=BBVA_FONT_MEDIUM), automargin=True
    )
    fig.update_xaxes(
        title_text="Impacto estimado", tickfont=dict(size=19), title_font=dict(size=20), nticks=5
    )
    fig.update_layout(margin=dict(l=left_margin, r=72, t=18, b=54), bargap=0.34)
    return fig


def _prepare_opportunity_chart_df(opportunities_df: pd.DataFrame) -> pd.DataFrame:
    opp_chart_df = select_opportunities(
        opportunities_df, max_rows=EDITORIAL_LIMITS.max_opportunities
    )
    if opp_chart_df.empty:
        return opp_chart_df

    def _opp_label(row: pd.Series) -> str:
        value = str(row.get("value", "")).strip()
        base = value
        return _compact_axis_label(
            base, width=22 if len(base) >= 22 else 18, max_lines=2, max_chars=38
        )

    opp_chart_df["label"] = opp_chart_df.apply(_opp_label, axis=1)
    return opp_chart_df


def _first_existing_series(df: pd.DataFrame, *columns: str) -> pd.Series:
    for column in columns:
        if column in df.columns:
            return df[column]
    return pd.Series(dtype=str)


def _build_journey_table(
    *,
    touchpoint_source: str,
    entity_summary_df: pd.DataFrame,
    broken_journeys_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    cols = [
        "journey",
        "touchpoint",
        "palanca",
        "subpalanca",
        "anchor_topic",
        "nps_topic",
        "links",
        "comments",
        "nps",
        "confidence",
    ]
    if (
        str(touchpoint_source or "").strip() == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS
        and broken_journeys_df is not None
        and not broken_journeys_df.empty
    ):
        source = broken_journeys_df.copy()
        source["priority_sort"] = pd.to_numeric(
            source.get("linked_pairs"),
            errors="coerce",
        ).fillna(0.0)
        source["confidence_sort"] = pd.to_numeric(
            source.get("semantic_cohesion"),
            errors="coerce",
        ).fillna(0.0)
        source["nps_sort"] = pd.to_numeric(
            source.get("avg_nps"),
            errors="coerce",
        ).fillna(10.0)
        out = (
            source.sort_values(
                ["priority_sort", "confidence_sort", "nps_sort"],
                ascending=[False, False, True],
            )
            .head(EDITORIAL_LIMITS.max_journey_rows)
            .copy()
        )
        return pd.DataFrame(
            {
                "journey": _first_existing_series(out, "journey_label").astype(str),
                "touchpoint": _first_existing_series(out, "touchpoint").astype(str),
                "palanca": _first_existing_series(out, "palanca").astype(str),
                "subpalanca": _first_existing_series(out, "subpalanca").astype(str),
                "anchor_topic": _first_existing_series(
                    out,
                    "anchor_topic",
                    "nps_topic",
                    "topic",
                ).astype(str),
                "nps_topic": _first_existing_series(out, "nps_topic", "topic").astype(str),
                "links": pd.to_numeric(
                    _first_existing_series(out, "linked_pairs"),
                    errors="coerce",
                )
                .fillna(0)
                .astype(int),
                "comments": pd.to_numeric(
                    _first_existing_series(out, "linked_comments"),
                    errors="coerce",
                )
                .fillna(0)
                .astype(int),
                "nps": pd.to_numeric(_first_existing_series(out, "avg_nps"), errors="coerce"),
                "confidence": pd.to_numeric(
                    _first_existing_series(out, "semantic_cohesion"),
                    errors="coerce",
                ).fillna(0.0),
            }
        )
    if entity_summary_df is None or entity_summary_df.empty:
        return pd.DataFrame(columns=cols)
    source = select_causal_scenarios(
        entity_summary_df,
        max_rows=EDITORIAL_LIMITS.max_journey_rows,
    )
    return pd.DataFrame(
        {
            "journey": _first_existing_series(
                source,
                "journey",
                "entity_label",
                "nps_topic",
            ).astype(str),
            "touchpoint": _first_existing_series(source, "touchpoint").astype(str),
            "palanca": _first_existing_series(source, "palanca").astype(str),
            "subpalanca": _first_existing_series(source, "subpalanca").astype(str),
            "anchor_topic": _first_existing_series(
                source,
                "anchor_topic",
                "source_nps_topic",
                "nps_topic",
            ).astype(str),
            "nps_topic": _first_existing_series(
                source,
                "nps_topic",
                "source_nps_topic",
            ).astype(str),
            "links": pd.to_numeric(
                _first_existing_series(source, "linked_pairs"),
                errors="coerce",
            )
            .fillna(0)
            .astype(int),
            "comments": pd.to_numeric(
                _first_existing_series(source, "linked_comments"),
                errors="coerce",
            )
            .fillna(0)
            .astype(int),
            "nps": pd.to_numeric(_first_existing_series(source, "avg_nps"), errors="coerce"),
            "confidence": pd.to_numeric(
                _first_existing_series(source, "confidence"),
                errors="coerce",
            ).fillna(0.0),
        }
    )


def _build_journey_summary_figure(
    summary_df: pd.DataFrame, *, touchpoint_source: str
) -> Optional[go.Figure]:
    method_spec = get_causal_method_spec(touchpoint_source)
    plot_df = summary_df.copy() if summary_df is not None else pd.DataFrame()
    if plot_df.empty:
        return None
    plot_df["entity_label"] = plot_df.get("nps_topic", "").astype(str).str.strip()
    fig = chart_causal_entity_bar(
        plot_df,
        get_theme("light"),
        entity_label=method_spec.entity_singular,
        top_k=min(EDITORIAL_LIMITS.max_journey_rows, len(plot_df)) if not plot_df.empty else 10,
    )
    if fig is None or plot_df.empty:
        return fig
    y_values = [str(value).replace("<br>", " ") for value in list(getattr(fig.data[0], "y", []))]
    if not y_values:
        return fig
    max_len = max(len(value) for value in y_values)
    label_count = len(y_values)
    wrap_width = 44 if max_len <= 46 else 40 if max_len <= 58 else 36
    max_chars = 92 if max_len <= 72 else 84
    y_font_size = 27 if label_count <= 6 else 24 if label_count <= 8 else 22
    left_margin = 430 if max_len >= 72 else 390 if max_len >= 58 else 350 if max_len >= 44 else 320
    pretty_labels = [
        _compact_axis_label(value, width=wrap_width, max_lines=2, max_chars=max_chars)
        for value in y_values
    ]
    with contextlib.suppress(Exception):
        fig.data[0].y = pretty_labels
    fig.update_yaxes(
        title_text="",
        tickmode="array",
        tickvals=pretty_labels,
        ticktext=pretty_labels,
        tickfont=dict(size=y_font_size, family=BBVA_FONT_MEDIUM, color="#" + BBVA_COLORS["ink"]),
        automargin=True,
    )
    fig.update_xaxes(
        title_text="Links validados Helix↔VoC",
        tickfont=dict(size=17),
        title_font=dict(size=18),
        nticks=6,
        automargin=True,
    )
    fig.update_layout(margin=dict(l=left_margin, r=102, t=14, b=38), bargap=0.22)
    fig.update_coloraxes(
        colorbar=dict(
            title=dict(text="NPS en riesgo", side="right", font=dict(size=15)),
            tickmode="array",
            tickvals=[0, 1, 2, 3, 4],
            tickfont=dict(size=14),
            len=0.82,
            y=0.5,
            thickness=16,
        )
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


def _apply_ppt_figure_theme(
    fig: go.Figure,
    *,
    panel_width_in: float | None = None,
    panel_height_in: float | None = None,
) -> go.Figure:
    ink = "#" + BBVA_COLORS["ink"]
    grid = "#" + BBVA_COLORS["line"]
    white = "#" + BBVA_COLORS["white"]
    compact_panel = bool(
        (panel_width_in is not None and panel_width_in <= 6.0)
        or (panel_height_in is not None and panel_height_in <= 2.5)
    )
    trace_types = {
        str(getattr(trace, "type", "") or "").strip().lower()
        for trace in fig.data
        if trace is not None
    }
    has_heatmap = "heatmap" in trace_types
    has_legend = (
        sum(
            1
            for trace in fig.data
            if bool(getattr(trace, "showlegend", True))
            and str(getattr(trace, "name", "") or "").strip()
        )
        > 1
    )
    has_scatter_text = any(
        str(getattr(trace, "type", "") or "").strip().lower() == "scatter"
        and "text" in str(getattr(trace, "mode", "") or "").lower()
        for trace in fig.data
    )
    has_colorbar = any(
        bool(getattr(trace, "showscale", False))
        or bool(getattr(getattr(trace, "colorbar", None), "title", None))
        for trace in fig.data
    )

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
    base_font_size = 18 if has_heatmap or compact_panel else 17
    tick_font_size = 16 if has_heatmap or compact_panel else 15
    axis_title_font_size = 18 if has_heatmap or compact_panel else 17
    legend_font_size = 15 if compact_panel else 14

    def _font_size(value: object, fallback: int) -> int:
        try:
            size = int(float(value))
        except Exception:
            return int(fallback)
        return max(size, int(fallback))

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor=white,
        plot_bgcolor=white,
        font=dict(family=BBVA_FONT_BODY, size=base_font_size, color=ink),
        legend=dict(
            orientation="h",
            x=0.0,
            xanchor="left",
            y=1.12 if has_legend else 1.04,
            yanchor="bottom",
            font=dict(size=legend_font_size, color=ink),
            title_font=dict(size=legend_font_size, color=ink),
            bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(
            l=max(
                int(current_margin.get("l", 24)),
                78 if has_scatter_text or compact_panel else 58,
            ),
            r=max(int(current_margin.get("r", 24)), 92 if has_colorbar else 44),
            t=max(int(current_margin.get("t", 20)), 40 if has_heatmap or has_legend else 28),
            b=max(
                int(current_margin.get("b", 24)),
                84 if has_heatmap or compact_panel else 58,
            ),
        ),
        hoverlabel=dict(font=dict(family=BBVA_FONT_BODY, size=13, color=ink)),
    )
    fig.for_each_xaxis(
        lambda axis: axis.update(
            tickfont=dict(
                size=_font_size(
                    getattr(getattr(axis, "tickfont", None), "size", None), tick_font_size
                ),
                color=ink,
            ),
            title_font=dict(
                size=_font_size(
                    getattr(getattr(getattr(axis, "title", None), "font", None), "size", None),
                    axis_title_font_size,
                ),
                color=ink,
            ),
            automargin=True,
            gridcolor=grid,
            linecolor=grid,
        )
    )
    fig.for_each_yaxis(
        lambda axis: axis.update(
            tickfont=dict(
                size=_font_size(
                    getattr(getattr(axis, "tickfont", None), "size", None), tick_font_size
                ),
                color=ink,
            ),
            title_font=dict(
                size=_font_size(
                    getattr(getattr(getattr(axis, "title", None), "font", None), "size", None),
                    axis_title_font_size,
                ),
                color=ink,
            ),
            automargin=True,
            gridcolor=grid,
            linecolor=grid,
        )
    )
    for trace in fig.data:
        if "text" not in str(getattr(trace, "mode", "") or "").lower():
            continue
        with contextlib.suppress(Exception):
            trace.textfont.size = max(
                int(getattr(getattr(trace, "textfont", None), "size", 0) or 0),
                16,
            )
    if has_heatmap:
        for trace in fig.data:
            if str(getattr(trace, "type", "") or "").strip().lower() != "heatmap":
                continue
            with contextlib.suppress(Exception):
                trace.xgap = max(int(getattr(trace, "xgap", 0) or 0), 2)
            with contextlib.suppress(Exception):
                trace.ygap = max(int(getattr(trace, "ygap", 0) or 0), 2)
            if has_colorbar:
                with contextlib.suppress(Exception):
                    trace.colorbar.thickness = 14
                with contextlib.suppress(Exception):
                    trace.colorbar.len = 0.74
                with contextlib.suppress(Exception):
                    trace.colorbar.y = 0.48
                with contextlib.suppress(Exception):
                    trace.colorbar.title.side = "right"
                with contextlib.suppress(Exception):
                    trace.colorbar.tickfont.size = 15
                with contextlib.suppress(Exception):
                    trace.colorbar.title.font.size = 16
    for axis_name in fig.layout:
        if not str(axis_name).startswith(("xaxis", "yaxis")):
            continue
        axis = getattr(fig.layout, axis_name, None)
        if axis is None:
            continue
        with contextlib.suppress(Exception):
            if compact_panel and str(axis_name).startswith("xaxis") and axis.nticks is None:
                axis.nticks = 5
        with contextlib.suppress(Exception):
            if compact_panel and str(axis_name).startswith("yaxis") and axis.nticks is None:
                axis.nticks = 8
    return fig


def _pillow_color(
    value: object, default: str = "#42526E"
) -> tuple[int, int, int]:  # pragma: no cover
    try:
        from PIL import ImageColor
    except Exception:
        return (66, 82, 110)

    if isinstance(value, (list, tuple)) and len(value) >= 3:
        with contextlib.suppress(Exception):
            return tuple(int(float(channel)) for channel in value[:3])

    raw = str(value or "").strip()
    if not raw:
        raw = default
    if raw.lower().startswith("rgba("):
        parts = [part.strip() for part in raw[5:-1].split(",")]
        if len(parts) >= 3:
            with contextlib.suppress(Exception):
                return tuple(int(float(channel)) for channel in parts[:3])
    try:
        return ImageColor.getrgb(raw)
    except Exception:
        try:
            return ImageColor.getrgb(default)
        except Exception:
            return (66, 82, 110)


def _pillow_font(size: int, *, bold: bool = False):  # pragma: no cover
    try:
        from PIL import ImageFont
    except Exception:
        return None

    fonts_dir = Path(__file__).resolve().parents[3] / "assets" / "ppt" / "bbva" / "fonts"
    candidates = [
        fonts_dir / ("BentonSansBBVA-Bold.ttf" if bold else "BentonSansBBVA-Book.ttf"),
        fonts_dir / "BentonSansBBVA-Medium.ttf",
    ]
    for candidate in candidates:
        if candidate.exists():
            with contextlib.suppress(Exception):
                return ImageFont.truetype(str(candidate), size=max(int(size), 8))
    with contextlib.suppress(Exception):
        return ImageFont.load_default()
    return None


def _pillow_text_size(draw: object, text: str, font: object) -> tuple[int, int]:  # pragma: no cover
    with contextlib.suppress(Exception):
        bbox = draw.multiline_textbbox((0, 0), str(text or ""), font=font, spacing=4)
        return max(int(bbox[2] - bbox[0]), 0), max(int(bbox[3] - bbox[1]), 0)
    return (0, 0)


def _plotly_tick_label(value: object) -> str:  # pragma: no cover
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).strftime("%d/%m")
    text = str(value or "").strip()
    if not text:
        return ""
    with contextlib.suppress(Exception):
        ts = pd.Timestamp(text)
        if pd.notna(ts):
            return ts.strftime("%d/%m")
    return _clip(text.replace("<br>", " "), 24)


def _plotly_title_text(value: object, default: str = "") -> str:  # pragma: no cover
    if value is None:
        return default
    text = getattr(value, "text", None)
    if text not in (None, ""):
        return str(text)
    raw = str(value or "").strip()
    return raw if raw and raw != "None" else default


def _nice_ticks(
    min_value: float, max_value: float, *, target: int = 5
) -> list[float]:  # pragma: no cover
    if not np.isfinite(min_value) or not np.isfinite(max_value):
        return [0.0, 1.0]
    if math.isclose(min_value, max_value):
        anchor = 0.0 if math.isclose(max_value, 0.0) else max_value
        return [anchor, anchor + 1.0]
    span = max_value - min_value
    raw_step = span / max(int(target), 2)
    magnitude = 10 ** math.floor(math.log10(abs(raw_step))) if raw_step else 1.0
    normalized = raw_step / magnitude if magnitude else raw_step
    if normalized <= 1:
        step = 1 * magnitude
    elif normalized <= 2:
        step = 2 * magnitude
    elif normalized <= 5:
        step = 5 * magnitude
    else:
        step = 10 * magnitude
    start = math.floor(min_value / step) * step
    end = math.ceil(max_value / step) * step
    ticks: list[float] = []
    cursor = start
    while cursor <= end + step * 0.5:
        ticks.append(float(cursor))
        cursor += step
        if len(ticks) > 10:
            break
    return ticks or [min_value, max_value]


def _format_tick_value(value: float) -> str:  # pragma: no cover
    if not np.isfinite(value):
        return ""
    if abs(value) >= 1000:
        return _fmt_count_or_nd(value)
    if math.isclose(value, round(value), abs_tol=1e-9):
        return f"{int(round(value))}"
    return _fmt_num_or_nd(value, decimals=1)


def _plotly_colorscale_color(
    colorscale: object, ratio: float
) -> tuple[int, int, int]:  # pragma: no cover
    with contextlib.suppress(Exception):
        from plotly.colors import sample_colorscale

        sampled = sample_colorscale(colorscale, [min(max(float(ratio), 0.0), 1.0)])[0]
        return _pillow_color(sampled)
    default_scale = [
        [0.0, "#" + BBVA_COLORS["red"]],
        [0.5, "#" + BBVA_COLORS["yellow"]],
        [1.0, "#" + BBVA_COLORS["green"]],
    ]
    with contextlib.suppress(Exception):
        from plotly.colors import sample_colorscale

        sampled = sample_colorscale(default_scale, [min(max(float(ratio), 0.0), 1.0)])[0]
        return _pillow_color(sampled)
    return _pillow_color("#" + BBVA_COLORS["sky"])


def _plotly_bar_colors(
    trace: object, count: int, layout: object
) -> list[tuple[int, int, int]]:  # pragma: no cover
    marker = getattr(trace, "marker", None)
    color = getattr(marker, "color", None)
    if isinstance(color, (list, tuple, np.ndarray, pd.Series)):
        values = list(color)
        if values and all(
            isinstance(item, (int, float, np.integer, np.floating)) for item in values
        ):
            numeric = pd.to_numeric(pd.Series(values), errors="coerce")
            vmin = float(numeric.min()) if not numeric.dropna().empty else 0.0
            vmax = float(numeric.max()) if not numeric.dropna().empty else 1.0
            span = max(vmax - vmin, 1e-9)
            colorscale = getattr(marker, "colorscale", None) or getattr(
                getattr(layout, "coloraxis", None), "colorscale", None
            )
            return [
                (
                    _plotly_colorscale_color(colorscale, (float(value) - vmin) / span)
                    if np.isfinite(float(value))
                    else _pillow_color("#" + BBVA_COLORS["line"])
                )
                for value in numeric.fillna(vmin).tolist()
            ]
        return [_pillow_color(item) for item in values[:count]] + [
            _pillow_color("#" + BBVA_COLORS["sky"])
        ] * max(count - len(values), 0)
    return [_pillow_color(color or "#" + BBVA_COLORS["sky"])] * count


def _pillow_render_heatmap(
    fig: go.Figure, width: int, height: int
) -> Optional[bytes]:  # pragma: no cover
    try:
        from PIL import Image, ImageDraw
    except Exception:
        return None

    traces = [
        trace
        for trace in fig.data
        if str(getattr(trace, "type", "") or "").strip().lower() == "heatmap"
    ]
    if not traces:
        return None
    trace = traces[0]
    z = np.array(getattr(trace, "z", []), dtype=float)
    if z.size == 0:
        return None
    x_values = [str(value).replace("<br>", " ") for value in list(getattr(trace, "x", []))]
    y_values = [str(value).replace("<br>", " ") for value in list(getattr(trace, "y", []))]
    rows = max(len(y_values), z.shape[0])
    cols = max(len(x_values), z.shape[1] if z.ndim > 1 else 0)
    if rows <= 0 or cols <= 0:
        return None

    image = Image.new("RGB", (width, height), _pillow_color("#FFFFFF"))
    draw = ImageDraw.Draw(image)
    title_font = _pillow_font(max(height // 36, 18), bold=True)
    axis_font = _pillow_font(max(height // 44, 14))
    tick_font = _pillow_font(max(height // 46, 12))

    plot_left = int(width * 0.30)
    plot_right = int(width * 0.88)
    plot_top = int(height * 0.10)
    plot_bottom = int(height * 0.86)
    cell_gap = max(int(min(width, height) * 0.003), 2)
    cell_w = max((plot_right - plot_left - cell_gap * (cols - 1)) // max(cols, 1), 1)
    cell_h = max((plot_bottom - plot_top - cell_gap * (rows - 1)) // max(rows, 1), 1)

    valid = z[np.isfinite(z)]
    zmin = float(valid.min()) if valid.size else 0.0
    zmax = float(valid.max()) if valid.size else 1.0
    span = max(zmax - zmin, 1e-9)
    colorscale = getattr(trace, "colorscale", None) or getattr(
        getattr(fig.layout, "coloraxis", None), "colorscale", None
    )

    for row in range(rows):
        for col in range(cols):
            value = float(z[row, col]) if row < z.shape[0] and col < z.shape[1] else float("nan")
            ratio = 0.0 if not np.isfinite(value) else (value - zmin) / span
            fill = _plotly_colorscale_color(colorscale, ratio)
            x0 = plot_left + col * (cell_w + cell_gap)
            y0 = plot_top + row * (cell_h + cell_gap)
            draw.rounded_rectangle(
                [(x0, y0), (x0 + cell_w, y0 + cell_h)],
                radius=max(min(cell_w, cell_h) // 8, 2),
                fill=fill,
                outline=_pillow_color("#FFFFFF"),
                width=1,
            )

    for row, label in enumerate(y_values[:rows]):
        wrapped = _wrap_label(label, width=18, max_lines=2, joiner="\n")
        tw, th = _pillow_text_size(draw, wrapped, tick_font)
        y0 = plot_top + row * (cell_h + cell_gap) + max((cell_h - th) // 2, 0)
        draw.multiline_text(
            (plot_left - tw - 16, y0),
            wrapped,
            fill=_pillow_color("#" + BBVA_COLORS["ink"]),
            font=tick_font,
            spacing=3,
            align="right",
        )

    max_x_ticks = min(cols, 8)
    step = max(int(math.ceil(cols / max(max_x_ticks, 1))), 1)
    for col, label in enumerate(x_values[:cols]):
        if col % step != 0 and col != cols - 1:
            continue
        short = _wrap_label(_plotly_tick_label(label), width=10, max_lines=2, joiner="\n")
        tw, _ = _pillow_text_size(draw, short, tick_font)
        x0 = plot_left + col * (cell_w + cell_gap) + max((cell_w - tw) // 2, 0)
        draw.multiline_text(
            (x0, plot_bottom + 12),
            short,
            fill=_pillow_color("#" + BBVA_COLORS["ink"]),
            font=tick_font,
            spacing=2,
            align="center",
        )

    colorbar_left = int(width * 0.92)
    colorbar_top = plot_top
    colorbar_bottom = plot_bottom
    for idx in range(colorbar_top, colorbar_bottom):
        ratio = 1.0 - ((idx - colorbar_top) / max(colorbar_bottom - colorbar_top, 1))
        draw.line(
            [(colorbar_left, idx), (colorbar_left + 16, idx)],
            fill=_plotly_colorscale_color(colorscale, ratio),
            width=1,
        )
    draw.text(
        (colorbar_left - 4, colorbar_top - 24),
        _plotly_title_text(getattr(getattr(trace, "colorbar", None), "title", None), "NPS"),
        fill=_pillow_color("#" + BBVA_COLORS["ink"]),
        font=title_font,
    )
    for tick in _nice_ticks(zmin, zmax, target=4):
        ratio = (tick - zmin) / span if span else 0.0
        y_tick = colorbar_bottom - int((colorbar_bottom - colorbar_top) * ratio)
        draw.line(
            [(colorbar_left + 18, y_tick), (colorbar_left + 24, y_tick)],
            fill=_pillow_color("#" + BBVA_COLORS["ink"]),
            width=1,
        )
        draw.text(
            (colorbar_left + 28, y_tick - 8),
            _format_tick_value(tick),
            fill=_pillow_color("#" + BBVA_COLORS["muted"]),
            font=axis_font,
        )

    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def _pillow_render_xy(
    fig: go.Figure, width: int, height: int
) -> Optional[bytes]:  # pragma: no cover
    try:
        from PIL import Image, ImageDraw
    except Exception:
        return None

    if not fig.data:
        return None

    trace_types = {
        str(getattr(trace, "type", "") or "").strip().lower()
        for trace in fig.data
        if trace is not None
    }
    horizontal = bool(
        trace_types == {"bar"}
        and all(str(getattr(trace, "orientation", "") or "").lower() == "h" for trace in fig.data)
    )

    image = Image.new("RGB", (width, height), _pillow_color("#FFFFFF"))
    draw = ImageDraw.Draw(image)
    axis_font = _pillow_font(max(height // 44, 14))
    tick_font = _pillow_font(max(height // 48, 12))
    legend_font = _pillow_font(max(height // 50, 11))

    legend_items = [
        trace
        for trace in fig.data
        if bool(getattr(trace, "showlegend", True))
        and str(getattr(trace, "name", "") or "").strip()
    ]
    legend_y = 14
    legend_x = 18
    for trace in legend_items[:6]:
        trace_type = str(getattr(trace, "type", "") or "").strip().lower()
        if trace_type == "scatter":
            color = getattr(getattr(trace, "line", None), "color", None) or getattr(
                getattr(trace, "marker", None), "color", None
            )
        else:
            color = getattr(getattr(trace, "marker", None), "color", None)
            if isinstance(color, (list, tuple, np.ndarray, pd.Series)):
                color = list(color)[0] if list(color) else "#" + BBVA_COLORS["sky"]
        draw.rounded_rectangle(
            [(legend_x, legend_y + 4), (legend_x + 16, legend_y + 16)],
            radius=4,
            fill=_pillow_color(color or "#" + BBVA_COLORS["sky"]),
        )
        label = _clip(getattr(trace, "name", "") or "", 22)
        draw.text(
            (legend_x + 22, legend_y),
            label,
            fill=_pillow_color("#" + BBVA_COLORS["ink"]),
            font=legend_font,
        )
        label_w, _ = _pillow_text_size(draw, label, legend_font)
        legend_x += label_w + 52

    plot_left = int(width * (0.34 if horizontal else 0.10))
    plot_right = int(width * 0.92)
    plot_top = int(height * 0.16)
    plot_bottom = int(height * 0.84)
    right_axis_present = any(str(getattr(trace, "yaxis", "y")) == "y2" for trace in fig.data)
    if right_axis_present:
        plot_right = int(width * 0.86)

    draw.rectangle(
        [(plot_left, plot_top), (plot_right, plot_bottom)],
        outline=_pillow_color("#" + BBVA_COLORS["line"]),
        width=1,
    )

    if horizontal:
        trace = next(
            (item for item in fig.data if str(getattr(item, "type", "")).lower() == "bar"), None
        )
        if trace is None:
            return None
        categories = [str(value).replace("<br>", " ") for value in list(getattr(trace, "y", []))]
        values = pd.to_numeric(pd.Series(list(getattr(trace, "x", []))), errors="coerce").fillna(
            0.0
        )
        if values.empty:
            return None
        min_value = min(float(values.min()), 0.0)
        max_value = max(float(values.max()), 0.0)
        span = max(max_value - min_value, 1e-9)
        zero_x = plot_left + int(((0.0 - min_value) / span) * (plot_right - plot_left))
        bar_colors = _plotly_bar_colors(trace, len(values), fig.layout)
        ticks = _nice_ticks(min_value, max_value, target=5)

        for tick in ticks:
            x = plot_left + int(((tick - min_value) / span) * (plot_right - plot_left))
            draw.line(
                [(x, plot_top), (x, plot_bottom)],
                fill=_pillow_color("#" + BBVA_COLORS["line"]),
                width=1,
            )
            label = _format_tick_value(tick)
            tw, _ = _pillow_text_size(draw, label, axis_font)
            draw.text(
                (x - tw // 2, plot_bottom + 12),
                label,
                fill=_pillow_color("#" + BBVA_COLORS["muted"]),
                font=axis_font,
            )

        row_h = max((plot_bottom - plot_top) // max(len(categories), 1), 1)
        for idx, (category, value) in enumerate(zip(categories, values.tolist())):
            center_y = plot_top + idx * row_h + row_h // 2
            label = _wrap_label(category, width=18, max_lines=2, joiner="\n")
            tw, th = _pillow_text_size(draw, label, tick_font)
            draw.multiline_text(
                (plot_left - tw - 16, center_y - th // 2),
                label,
                fill=_pillow_color("#" + BBVA_COLORS["ink"]),
                font=tick_font,
                spacing=2,
                align="right",
            )
            end_x = plot_left + int(((float(value) - min_value) / span) * (plot_right - plot_left))
            x0, x1 = sorted((zero_x, end_x))
            draw.rounded_rectangle(
                [(x0, center_y - max(row_h // 4, 6)), (x1, center_y + max(row_h // 4, 6))],
                radius=8,
                fill=(
                    bar_colors[idx]
                    if idx < len(bar_colors)
                    else _pillow_color("#" + BBVA_COLORS["sky"])
                ),
            )
            value_label = _format_tick_value(float(value))
            draw.text(
                (x1 + 8, center_y - 8),
                value_label,
                fill=_pillow_color("#" + BBVA_COLORS["ink"]),
                font=axis_font,
            )
        draw.line(
            [(zero_x, plot_top), (zero_x, plot_bottom)],
            fill=_pillow_color("#" + BBVA_COLORS["muted"]),
            width=2,
        )
    else:
        category_values: list[object] = []
        for trace in fig.data:
            if str(getattr(trace, "type", "") or "").strip().lower() not in {"bar", "scatter"}:
                continue
            for value in list(getattr(trace, "x", [])):
                if value not in category_values:
                    category_values.append(value)
        if not category_values:
            return None
        n = len(category_values)
        x_positions = {
            value: plot_left + int(idx * (plot_right - plot_left) / max(n - 1, 1))
            for idx, value in enumerate(category_values)
        }

        left_values: list[float] = []
        right_values: list[float] = []
        stacked_primary: dict[object, float] = {value: 0.0 for value in category_values}
        for trace in fig.data:
            trace_type = str(getattr(trace, "type", "") or "").strip().lower()
            series = pd.to_numeric(
                pd.Series(list(getattr(trace, "y", []))), errors="coerce"
            ).fillna(0.0)
            axis_key = "right" if str(getattr(trace, "yaxis", "y")) == "y2" else "left"
            if axis_key == "right":
                right_values.extend(series.tolist())
            else:
                if (
                    trace_type == "bar"
                    and str(getattr(fig.layout, "barmode", "") or "").lower() == "stack"
                ):
                    x_trace = list(getattr(trace, "x", []))
                    for x_value, y_value in zip(x_trace, series.tolist()):
                        stacked_primary[x_value] = stacked_primary.get(x_value, 0.0) + float(
                            y_value
                        )
                else:
                    left_values.extend(series.tolist())
        if stacked_primary:
            left_values.extend(stacked_primary.values())
        left_min = min(0.0, float(min(left_values)) if left_values else 0.0)
        left_max = max(float(max(left_values)) if left_values else 1.0, 0.0)
        right_min = min(0.0, float(min(right_values)) if right_values else 0.0)
        right_max = max(float(max(right_values)) if right_values else 1.0, 0.0)
        left_span = max(left_max - left_min, 1e-9)
        right_span = max(right_max - right_min, 1e-9)

        def left_y(value: float) -> int:
            return plot_bottom - int(((value - left_min) / left_span) * (plot_bottom - plot_top))

        def right_y(value: float) -> int:
            return plot_bottom - int(((value - right_min) / right_span) * (plot_bottom - plot_top))

        for tick in _nice_ticks(left_min, left_max, target=5):
            y = left_y(tick)
            draw.line(
                [(plot_left, y), (plot_right, y)],
                fill=_pillow_color("#" + BBVA_COLORS["line"]),
                width=1,
            )
            label = _format_tick_value(tick)
            tw, _ = _pillow_text_size(draw, label, axis_font)
            draw.text(
                (plot_left - tw - 10, y - 8),
                label,
                fill=_pillow_color("#" + BBVA_COLORS["muted"]),
                font=axis_font,
            )
        if right_axis_present:
            for tick in _nice_ticks(right_min, right_max, target=5):
                y = right_y(tick)
                label = _format_tick_value(tick)
                draw.text(
                    (plot_right + 10, y - 8),
                    label,
                    fill=_pillow_color("#" + BBVA_COLORS["muted"]),
                    font=axis_font,
                )

        if right_axis_present:
            draw.text(
                (plot_right + 8, plot_top - 24),
                _plotly_title_text(
                    getattr(getattr(fig.layout, "yaxis2", None), "title", None), "Incidencias"
                ),
                fill=_pillow_color("#" + BBVA_COLORS["ink"]),
                font=axis_font,
            )
        draw.text(
            (plot_left, plot_top - 24),
            _plotly_title_text(getattr(getattr(fig.layout, "yaxis", None), "title", None), ""),
            fill=_pillow_color("#" + BBVA_COLORS["ink"]),
            font=axis_font,
        )

        bar_width = max(int((plot_right - plot_left) / max(n, 1) * 0.62), 6)
        stacked_cache: dict[tuple[str, object], float] = {}
        for trace in fig.data:
            trace_type = str(getattr(trace, "type", "") or "").strip().lower()
            x_trace = list(getattr(trace, "x", []))
            y_trace = pd.to_numeric(
                pd.Series(list(getattr(trace, "y", []))), errors="coerce"
            ).fillna(0.0)
            axis_key = "right" if str(getattr(trace, "yaxis", "y")) == "y2" else "left"
            if trace_type == "bar":
                bar_colors = _plotly_bar_colors(trace, len(y_trace), fig.layout)
                stacked = (
                    str(getattr(fig.layout, "barmode", "") or "").lower() == "stack"
                    and axis_key == "left"
                )
                for idx, (x_value, y_value) in enumerate(zip(x_trace, y_trace.tolist())):
                    x = x_positions.get(x_value)
                    if x is None:
                        continue
                    if stacked:
                        baseline = stacked_cache.get((axis_key, x_value), 0.0)
                        y0 = left_y(baseline + float(y_value))
                        y1 = left_y(baseline)
                        stacked_cache[(axis_key, x_value)] = baseline + float(y_value)
                    else:
                        y0 = (
                            right_y(float(y_value))
                            if axis_key == "right"
                            else left_y(float(y_value))
                        )
                        y1 = right_y(0.0) if axis_key == "right" else left_y(0.0)
                    draw.rounded_rectangle(
                        [(x - bar_width // 2, min(y0, y1)), (x + bar_width // 2, max(y0, y1))],
                        radius=6,
                        fill=(
                            bar_colors[idx]
                            if idx < len(bar_colors)
                            else _pillow_color("#" + BBVA_COLORS["sky"])
                        ),
                    )
            elif trace_type == "scatter":
                points: list[tuple[int, int]] = []
                line_color = getattr(getattr(trace, "line", None), "color", None) or getattr(
                    getattr(trace, "marker", None), "color", None
                )
                color = _pillow_color(line_color or "#" + BBVA_COLORS["blue"])
                for x_value, y_value in zip(x_trace, y_trace.tolist()):
                    x = x_positions.get(x_value)
                    if x is None or not np.isfinite(float(y_value)):
                        continue
                    y = right_y(float(y_value)) if axis_key == "right" else left_y(float(y_value))
                    points.append((x, y))
                if len(points) >= 2:
                    draw.line(points, fill=color, width=max(width // 500, 3))
                if "markers" in str(getattr(trace, "mode", "") or "").lower():
                    radius = max(width // 260, 4)
                    for x, y in points:
                        draw.ellipse(
                            [(x - radius, y - radius), (x + radius, y + radius)],
                            fill=color,
                            outline=_pillow_color("#FFFFFF"),
                            width=1,
                        )

        max_x_ticks = min(n, 10)
        step = max(int(math.ceil(n / max(max_x_ticks, 1))), 1)
        for idx, value in enumerate(category_values):
            if idx % step != 0 and idx != n - 1:
                continue
            label = _wrap_label(_plotly_tick_label(value), width=10, max_lines=2, joiner="\n")
            tw, _ = _pillow_text_size(draw, label, tick_font)
            x = x_positions[value]
            draw.multiline_text(
                (x - tw // 2, plot_bottom + 10),
                label,
                fill=_pillow_color("#" + BBVA_COLORS["muted"]),
                font=tick_font,
                spacing=2,
                align="center",
            )

    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def _pillow_chart_png(
    fig: go.Figure, *, width: int, height: int
) -> Optional[bytes]:  # pragma: no cover
    try:
        themed = _apply_ppt_figure_theme(go.Figure(fig))
    except Exception:
        themed = fig
    if any(
        str(getattr(trace, "type", "") or "").strip().lower() == "heatmap" for trace in themed.data
    ):
        return _pillow_render_heatmap(themed, width, height)
    return _pillow_render_xy(themed, width, height)


def _kaleido_png(
    fig: go.Figure,
    *,
    width: int = 1600,
    height: int = 900,
    panel_width_in: float | None = None,
    panel_height_in: float | None = None,
) -> Optional[bytes]:
    try:
        _patch_kaleido_executable_for_space_paths()
        themed = _apply_ppt_figure_theme(
            go.Figure(fig),
            panel_width_in=panel_width_in,
            panel_height_in=panel_height_in,
        )
        fingerprint = hashlib.sha256(
            (
                themed.to_json()
                + f"|{width}|{height}|{panel_width_in}|{panel_height_in}"
            ).encode("utf-8")
        ).hexdigest()
        with _FIGURE_PNG_LOCK:
            cached = _FIGURE_PNG_CACHE.get(fingerprint)
            if cached is not None:
                _FIGURE_PNG_CACHE.move_to_end(fingerprint)
                return cached
        attempts = [
            (themed, max(int(width), 960), max(int(height), 540)),
            (
                themed,
                min(max(int(width), 960), 1800),
                min(max(int(height), 540), 1080),
            ),
            (
                _apply_ppt_figure_theme(
                    go.Figure(fig),
                    panel_width_in=panel_width_in,
                    panel_height_in=panel_height_in,
                ),
                1280,
                720,
            ),
        ]
        for candidate, attempt_width, attempt_height in attempts:
            with contextlib.suppress(Exception):
                rendered = pio.to_image(
                    candidate,
                    format="png",
                    width=attempt_width,
                    height=attempt_height,
                    scale=1,
                )
                with _FIGURE_PNG_LOCK:
                    _FIGURE_PNG_CACHE[fingerprint] = rendered
                    _FIGURE_PNG_CACHE.move_to_end(fingerprint)
                    while len(_FIGURE_PNG_CACHE) > _FIGURE_PNG_CACHE_LIMIT:
                        _FIGURE_PNG_CACHE.popitem(last=False)
                return rendered
    except Exception:
        pass
    return _pillow_chart_png(fig, width=max(int(width), 960), height=max(int(height), 540))


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

    long_title = len(str(title)) > 72
    box = slide.shapes.add_textbox(
        Inches(0.65),
        Inches(0.20 if long_title else 0.28),
        Inches(9.8 if right_note.strip() else 12.0),
        Inches(0.85),
    )
    tf = box.text_frame
    _configure_text_frame(tf)
    tf.clear()
    p = tf.paragraphs[0]
    r = p.add_run()
    r.text = title
    r.font.name = BBVA_FONT_DISPLAY
    r.font.size = Pt(22 if long_title else 28)
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


def _add_takeaway_band(slide: object, text: str, *, top: float = 6.72) -> None:
    band = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.ROUNDED_RECTANGLE,
        Inches(0.66),
        Inches(top),
        Inches(12.02),
        Inches(0.50),
    )
    band.fill.solid()
    band.fill.fore_color.rgb = _rgb(BBVA_COLORS["blue"])
    band.line.fill.background()
    tf = band.text_frame
    _configure_text_frame(tf)
    tf.clear()
    tf.vertical_anchor = MSO_VERTICAL_ANCHOR.MIDDLE
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    r = p.add_run()
    r.text = _clip(str(text).replace("**", ""), 180)
    r.font.name = BBVA_FONT_MEDIUM
    r.font.size = Pt(11.5)
    r.font.bold = True
    r.font.color.rgb = _rgb(BBVA_COLORS["white"])

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
    target_ppi: Optional[int] = None,
) -> None:
    img = None
    if figure is not None:
        if target_ppi is not None and int(target_ppi) > 0:
            width_px = max(int(width * int(target_ppi)), 640)
            height_px = max(int(height * int(target_ppi)), 360)
            scale = 1.0
        else:
            base_ppi = 180
            width_px = max(int(width * base_ppi), 1400)
            height_px = max(int(height * base_ppi), 480)
            scale = max(1800 / max(width_px, 1), 900 / max(height_px, 1), 1.0)
        img = _kaleido_png(
            figure,
            width=int(width_px * scale),
            height=int(height_px * scale),
            panel_width_in=width,
            panel_height_in=height,
        )
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
    _configure_text_frame(tf)
    tf.vertical_anchor = MSO_VERTICAL_ANCHOR.TOP
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


def _dict_payload(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _delta_accent(delta_payload: dict[str, object], *, default: str) -> str:
    favorable = delta_payload.get("favorable")
    if favorable is True:
        return BBVA_COLORS["green"]
    if favorable is False:
        return BBVA_COLORS["red"]
    return default


def _add_comparative_stat_card(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    height: float,
    label: str,
    base_value: str,
    actual_value: str,
    delta_text: str,
    accent: str,
    delta_accent: str,
    base_label: str = "Inicio periodo",
    actual_label: str = "Fin periodo",
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
        Inches(0.10),
        Inches(height),
    )
    accent_bar.fill.solid()
    accent_bar.fill.fore_color.rgb = _rgb(accent)
    accent_bar.line.fill.background()

    title = slide.shapes.add_textbox(
        Inches(left + 0.18), Inches(top + 0.08), Inches(width - 0.34), Inches(0.18)
    )
    tf = title.text_frame
    _configure_text_frame(tf)
    tf.clear()
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    r = p.add_run()
    r.text = label.upper()
    r.font.name = BBVA_FONT_MEDIUM
    r.font.size = Pt(8.6)
    r.font.bold = True
    r.font.color.rgb = _rgb(BBVA_COLORS["muted"])

    col_w = (width - 0.44) / 2.0
    for idx, (column_label, value) in enumerate(
        [(base_label, base_value), (actual_label, actual_value)]
    ):
        x = left + 0.22 + idx * col_w
        header = slide.shapes.add_textbox(
            Inches(x), Inches(top + 0.34), Inches(col_w), Inches(0.18)
        )
        htf = header.text_frame
        _configure_text_frame(htf)
        htf.clear()
        hp = htf.paragraphs[0]
        hp.alignment = PP_ALIGN.CENTER
        hr = hp.add_run()
        hr.text = column_label
        hr.font.name = BBVA_FONT_BODY
        hr.font.size = Pt(7.6)
        hr.font.color.rgb = _rgb(BBVA_COLORS["muted"])

        value_box = slide.shapes.add_textbox(
            Inches(x), Inches(top + 0.52), Inches(col_w), Inches(0.30)
        )
        vtf = value_box.text_frame
        _configure_text_frame(vtf)
        vtf.clear()
        vp = vtf.paragraphs[0]
        vp.alignment = PP_ALIGN.CENTER
        vr = vp.add_run()
        vr.text = value
        vr.font.name = BBVA_FONT_DISPLAY
        vr.font.size = Pt(16 if len(value) <= 8 else 13)
        vr.font.bold = True
        vr.font.color.rgb = _rgb(BBVA_COLORS["ink"])

    delta = slide.shapes.add_textbox(
        Inches(left + 0.18), Inches(top + height - 0.28), Inches(width - 0.34), Inches(0.22)
    )
    dtf = delta.text_frame
    _configure_text_frame(dtf)
    dtf.clear()
    dp = dtf.paragraphs[0]
    dp.alignment = PP_ALIGN.CENTER
    dr = dp.add_run()
    dr.text = delta_text
    dr.font.name = BBVA_FONT_MEDIUM
    dr.font.size = Pt(9.2)
    dr.font.bold = True
    dr.font.color.rgb = _rgb(delta_accent)


def _add_markdown_runs(
    paragraph: object,
    *,
    text: object,
    prefix: str = "",
    max_chars: int = 170,
    font_size_pt: float = 11.0,
    color: str = "",
) -> None:
    remaining = max(int(max_chars), 1)
    if prefix:
        run = paragraph.add_run()
        run.text = prefix
        run.font.name = BBVA_FONT_BODY
        run.font.size = Pt(font_size_pt)
        run.font.color.rgb = _rgb(color or BBVA_COLORS["muted"])
    visible_len = 0
    truncated = False
    for segment in parse_markdown_strong(text):
        if remaining <= 0:
            truncated = True
            break
        segment_text = segment.text
        if len(segment_text) > remaining:
            segment_text = segment_text[: max(remaining - 1, 0)].rstrip() + "…"
            truncated = True
        run = paragraph.add_run()
        run.text = segment_text
        run.font.name = BBVA_FONT_BODY if not segment.bold else BBVA_FONT_MEDIUM
        run.font.size = Pt(font_size_pt)
        run.font.bold = bool(segment.bold)
        run.font.color.rgb = _rgb(color or BBVA_COLORS["muted"])
        visible_len += len(segment_text)
        remaining -= len(segment_text)
        if truncated:
            break
    if visible_len == 0 and not prefix:
        run = paragraph.add_run()
        run.text = ""
        run.font.name = BBVA_FONT_BODY
        run.font.size = Pt(font_size_pt)


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
            p.alignment = PP_ALIGN.LEFT
            p.space_before = Pt(4 if idx else 0)
            p.level = 0
            _add_markdown_runs(
                p,
                text=line,
                prefix="• ",
                max_chars=145 if width <= 4.0 else 170,
                font_size_pt=body_font_size_pt,
                color=BBVA_COLORS["muted"],
            )
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
        p.alignment = PP_ALIGN.LEFT
        p.space_before = Pt(6)
        p.level = 0
        _add_markdown_runs(
            p,
            text=line,
            prefix="• ",
            max_chars=145 if width <= 4.0 else 170,
            font_size_pt=body_font_size_pt,
            color=BBVA_COLORS["muted"],
        )


def _add_markdown_panel(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    height: float,
    title: str,
    body: str,
    border: str,
    title_size: float = 13.0,
    body_size: float = 11.2,
    max_chars: int = 220,
) -> None:
    _panel(
        slide,
        left=left,
        top=top,
        width=width,
        height=height,
        title=title,
        fill=BBVA_COLORS["white"],
        border=border,
        title_size=title_size,
    )
    tb = slide.shapes.add_textbox(
        Inches(left + 0.18),
        Inches(top + 0.42),
        Inches(width - 0.36),
        Inches(max(height - 0.52, 0.20)),
    )
    tf = tb.text_frame
    _configure_text_frame(tf)
    tf.clear()
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.LEFT
    _add_markdown_runs(
        p,
        text=body,
        max_chars=max_chars,
        font_size_pt=body_size,
        color=BBVA_COLORS["muted"],
    )


def _add_incident_evidence_panel(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    height: float,
    records: list[CausalEvidenceRecord],
    fallback_lines: list[str],
) -> None:
    _panel(
        slide,
        left=left,
        top=top,
        width=width,
        height=height,
        title=EDITORIAL_COPY.incident_examples_title,
        fill=BBVA_COLORS["white"],
        border=BBVA_COLORS["line"],
        title_size=13,
    )
    items = records[: EDITORIAL_LIMITS.max_helix_evidence]
    if not items:
        items = [
            CausalEvidenceRecord(incident_id="", summary=line, url="")
            for line in fallback_lines[: EDITORIAL_LIMITS.max_helix_evidence]
        ]
    if not items:
        items = [
            CausalEvidenceRecord(
                incident_id="",
                summary="No se han encontrado evidencias Helix adicionales defendibles para este escenario.",
                url="",
            )
        ]

    cols = 2
    gap_x = 0.22
    gap_y = 0.18
    inner_left = left + 0.18
    inner_top = top + 0.62
    card_width = (width - 0.36 - gap_x) / cols
    card_height = (height - 0.84 - gap_y) / 2
    for idx, item in enumerate(items[:4]):
        row = idx // cols
        col = idx % cols
        card_left = inner_left + col * (card_width + gap_x)
        card_top = inner_top + row * (card_height + gap_y)
        _panel(
            slide,
            left=card_left,
            top=card_top,
            width=card_width,
            height=card_height,
            title="",
            fill="F7F8F8",
            border=BBVA_COLORS["line"],
        )
        head = slide.shapes.add_textbox(
            Inches(card_left + 0.14),
            Inches(card_top + 0.10),
            Inches(card_width - 0.28),
            Inches(0.24),
        )
        htf = head.text_frame
        _configure_text_frame(htf)
        htf.clear()
        hp = htf.paragraphs[0]
        prefix = hp.add_run()
        prefix.text = f"Evidencia {idx + 1}"
        prefix.font.name = BBVA_FONT_HEAD
        prefix.font.size = Pt(10.8)
        prefix.font.bold = True
        prefix.font.color.rgb = _rgb(BBVA_COLORS["ink"])
        incident_id = str(item.incident_id or "").strip()
        if incident_id:
            sep = hp.add_run()
            sep.text = " · "
            sep.font.name = BBVA_FONT_BODY
            sep.font.size = Pt(10.8)
            sep.font.color.rgb = _rgb(BBVA_COLORS["muted"])
            link = hp.add_run()
            link.text = incident_id
            link.font.name = BBVA_FONT_MEDIUM
            link.font.size = Pt(10.8)
            link.font.bold = True
            link.font.underline = bool(item.url)
            link.font.color.rgb = _rgb(BBVA_COLORS["blue"] if item.url else BBVA_COLORS["muted"])
            if item.url:
                link.hyperlink.address = item.url

        body = slide.shapes.add_textbox(
            Inches(card_left + 0.14),
            Inches(card_top + 0.40),
            Inches(card_width - 0.28),
            Inches(max(card_height - 0.50, 0.28)),
        )
        btf = body.text_frame
        _configure_text_frame(btf)
        btf.clear()
        bp = btf.paragraphs[0]
        _add_markdown_runs(
            bp,
            text=item.summary,
            max_chars=128,
            font_size_pt=9.2,
            color=BBVA_COLORS["muted"],
        )


@dataclass(frozen=True)
class WrappedTableLayout:
    rows: list[list[str]]
    row_heights: list[float]
    font_size_pt: float
    total_height: float


def _wrap_text_to_width(text: object, *, column_width_in: float, font_size_pt: float) -> str:
    """Deterministically wrap text for PPT table cells without ellipsis."""
    clean = " ".join(str(text or "").replace("…", " ").split())
    if not clean:
        return ""
    # Conservative average glyph width: PPT font size points -> inches.
    avg_char_width_in = max(font_size_pt, 1.0) / 72.0 * 0.50
    usable_width = max(float(column_width_in) - 0.10, 0.20)
    chars_per_line = max(int(usable_width / avg_char_width_in), 8)
    return "\n".join(
        textwrap.wrap(
            clean,
            width=chars_per_line,
            break_long_words=False,
            break_on_hyphens=False,
        )
        or [clean]
    )


def _build_wrapped_table_layout(
    rows: list[list[str]],
    *,
    column_widths: list[float],
    font_size_pt: float,
    min_row_height: float,
    max_total_rows_height: float | None = None,
) -> WrappedTableLayout:
    """Return wrapped cell text and deterministic row heights for premium tables."""
    size = float(font_size_pt)
    wrapped_rows: list[list[str]] = []
    row_heights: list[float] = []

    def render(candidate_size: float) -> tuple[list[list[str]], list[float], float]:
        rendered: list[list[str]] = []
        heights: list[float] = []
        line_height = max(candidate_size / 72.0 * 1.20, 0.12)
        for raw_row in rows:
            rendered_row: list[str] = []
            max_lines = 1
            for idx, value in enumerate(raw_row):
                width = column_widths[min(idx, len(column_widths) - 1)] if column_widths else 1.0
                wrapped = _wrap_text_to_width(
                    value,
                    column_width_in=width,
                    font_size_pt=candidate_size,
                )
                rendered_row.append(wrapped)
                max_lines = max(max_lines, wrapped.count("\n") + 1)
            rendered.append(rendered_row)
            heights.append(max(float(min_row_height), 0.10 + line_height * max_lines))
        return rendered, heights, sum(heights)

    wrapped_rows, row_heights, total = render(size)
    if max_total_rows_height is not None and total > max_total_rows_height:
        for candidate in (size - 0.5, size - 1.0, size - 1.5):
            if candidate < 8.0:
                break
            wrapped_rows, row_heights, total = render(candidate)
            size = candidate
            if total <= max_total_rows_height:
                break
        if total > max_total_rows_height and row_heights:
            scale = max_total_rows_height / total
            row_heights = [max(min_row_height, h * scale) for h in row_heights]
            total = sum(row_heights)
    return WrappedTableLayout(
        rows=wrapped_rows,
        row_heights=row_heights,
        font_size_pt=size,
        total_height=total,
    )


def _add_wrapped_journey_table(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    title: str,
    headers: list[str],
    rows: list[list[str]],
    col_width_ratios: list[float],
    font_size_pt: float = 9.2,
    header_font_size_pt: float = 8.7,
    max_rows: int = 6,
    panel_border: str = "",
    max_rows_height: float = 2.05,
    min_row_height: float = 0.55,
    header_height: float = 0.70,
    title_pad: float = 0.62,
) -> None:
    visible_rows = (
        rows[: max(int(max_rows), 1)]
        if rows
        else [["Sin evidencia suficiente"] + ["-"] * (len(headers) - 1)]
    )
    ratio_sum = sum(col_width_ratios) or 1.0
    column_widths = [((width - 0.26) * ratio / ratio_sum) for ratio in col_width_ratios]
    layout = _build_wrapped_table_layout(
        visible_rows,
        column_widths=column_widths,
        font_size_pt=font_size_pt,
        min_row_height=min_row_height,
        max_total_rows_height=max_rows_height,
    )
    resolved_title_pad = title_pad if str(title or "").strip() else 0.24
    height = resolved_title_pad + header_height + layout.total_height + 0.20
    _panel(
        slide,
        left=left,
        top=top,
        width=width,
        height=height,
        title=title,
        fill=BBVA_COLORS["white"],
        border=panel_border or BBVA_COLORS["line"],
    )
    base_top = top + (0.50 if str(title or "").strip() else 0.18)
    x_positions: list[float] = []
    cursor = left + 0.13
    for col_width in column_widths:
        x_positions.append(cursor)
        cursor += col_width

    for idx, header in enumerate(headers):
        tb = slide.shapes.add_textbox(
            Inches(x_positions[idx]),
            Inches(base_top),
            Inches(column_widths[idx] - 0.06),
            Inches(header_height - 0.10),
        )
        tf = tb.text_frame
        _configure_text_frame(tf)
        tf.clear()
        p = tf.paragraphs[0]
        r = p.add_run()
        r.text = _wrap_text_to_width(
            header,
            column_width_in=column_widths[idx],
            font_size_pt=header_font_size_pt,
        )
        r.font.name = BBVA_FONT_MEDIUM
        r.font.size = Pt(header_font_size_pt)
        r.font.bold = True
        r.font.color.rgb = _rgb(BBVA_COLORS["blue"])

    current_top = base_top + header_height
    for row_idx, row in enumerate(layout.rows):
        row_height = layout.row_heights[row_idx]
        for col_idx, value in enumerate(row[: len(headers)]):
            tb = slide.shapes.add_textbox(
                Inches(x_positions[col_idx]),
                Inches(current_top),
                Inches(column_widths[col_idx] - 0.06),
                Inches(row_height),
            )
            tf = tb.text_frame
            _configure_text_frame(tf)
            tf.word_wrap = True
            tf.auto_size = MSO_AUTO_SIZE.NONE
            tf.vertical_anchor = MSO_VERTICAL_ANCHOR.TOP
            tf.clear()
            p = tf.paragraphs[0]
            r = p.add_run()
            r.text = str(value).replace("…", "")
            r.font.name = BBVA_FONT_BODY
            r.font.size = Pt(layout.font_size_pt)
            r.font.color.rgb = _rgb(BBVA_COLORS["muted"] if col_idx else BBVA_COLORS["ink"])
        current_top += row_height


def _add_table_text_cell(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    height: float,
    text: str,
    font_size_pt: float,
    bold: bool = False,
    align: object = PP_ALIGN.LEFT,
    color: str = "ink",
) -> None:
    cell = slide.shapes.add_textbox(
        Inches(left),
        Inches(top),
        Inches(width),
        Inches(height),
    )
    tf = cell.text_frame
    _configure_text_frame(tf)
    tf.clear()
    p = tf.paragraphs[0]
    p.alignment = align
    r = p.add_run()
    r.text = text
    r.font.name = BBVA_FONT_MEDIUM if bold else BBVA_FONT_BODY
    r.font.size = Pt(font_size_pt)
    r.font.bold = bold
    r.font.color.rgb = _rgb(BBVA_COLORS[color])


def _add_grid_table(
    slide: object,
    *,
    left: float,
    top: float,
    width: float,
    headers: list[str],
    rows: list[list[str]],
    col_width_ratios: list[float],
    header_height: float,
    row_height: float,
    header_fill: str,
    border_color: str,
    header_font_size_pt: float = 10.0,
    cell_font_size_pt: float = 10.0,
    show_row_dividers: bool = True,
) -> None:
    ratio_sum = sum(col_width_ratios) or 1.0
    col_widths = [width * ratio / ratio_sum for ratio in col_width_ratios]
    col_lefts: list[float] = []
    cursor = left
    for col_width in col_widths:
        col_lefts.append(cursor)
        cursor += col_width

    header_bg = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.RECTANGLE,
        Inches(left),
        Inches(top),
        Inches(width),
        Inches(header_height),
    )
    header_bg.fill.solid()
    header_bg.fill.fore_color.rgb = _rgb(header_fill)
    header_bg.line.color.rgb = _rgb(border_color)

    for idx, header in enumerate(headers):
        _add_table_text_cell(
            slide,
            left=col_lefts[idx] + 0.10,
            top=top + 0.05,
            width=col_widths[idx] - 0.20,
            height=header_height - 0.06,
            text=header,
            font_size_pt=header_font_size_pt,
            bold=True,
            align=PP_ALIGN.LEFT if idx == 0 else PP_ALIGN.CENTER,
            color="blue",
        )

    line_height = 0.01
    for row_idx, row_values in enumerate(rows):
        current_top = top + header_height + row_height * row_idx
        if show_row_dividers:
            separator = slide.shapes.add_shape(
                MSO_AUTO_SHAPE_TYPE.RECTANGLE,
                Inches(left),
                Inches(current_top),
                Inches(width),
                Inches(line_height),
            )
            separator.fill.solid()
            separator.fill.fore_color.rgb = _rgb(border_color)
            separator.line.color.rgb = _rgb(border_color)
        for col_idx, value in enumerate(row_values[: len(headers)]):
            _add_table_text_cell(
                slide,
                left=col_lefts[col_idx] + 0.10,
                top=current_top + 0.05,
                width=col_widths[col_idx] - 0.20,
                height=row_height - 0.06,
                text=value,
                font_size_pt=cell_font_size_pt,
                align=PP_ALIGN.LEFT if col_idx == 0 else PP_ALIGN.CENTER,
                color="ink" if col_idx == 0 else "muted",
            )

    bottom_line_top = top + header_height + row_height * len(rows)
    bottom_line = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.RECTANGLE,
        Inches(left),
        Inches(bottom_line_top),
        Inches(width),
        Inches(line_height),
    )
    bottom_line.fill.solid()
    bottom_line.fill.fore_color.rgb = _rgb(border_color)
    bottom_line.line.color.rgb = _rgb(border_color)


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
    header_font_size_pt: float = 10.5,
    cell_height: float = 0.30,
    max_rows: int = 6,
    panel_border: str = "",
    numeric_columns: Optional[set[int]] = None,
) -> None:
    title_pad = 0.62 if str(title or "").strip() else 0.24
    visible_rows = max(min(len(rows), max(int(max_rows), 1)), 1)
    height = title_pad + 0.54 + row_height * visible_rows
    panel = _panel(
        slide,
        left=left,
        top=top,
        width=width,
        height=height,
        title=title,
        fill=BBVA_COLORS["white"],
        border=panel_border or BBVA_COLORS["line"],
    )
    base_top = top + (0.46 if str(title or "").strip() else 0.18)
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
        r.font.size = Pt(header_font_size_pt)
        r.font.bold = True
        r.font.color.rgb = _rgb(BBVA_COLORS["blue"])

    for row_idx, row in enumerate(rows[: max(int(max_rows), 1)], start=1):
        current_top = base_top + 0.16 + row_height * row_idx
        for col_idx, value in enumerate(row[: len(headers)]):
            tb = slide.shapes.add_textbox(
                Inches(x_positions[col_idx] + 0.05),
                Inches(current_top),
                Inches(column_widths[col_idx] - 0.08),
                Inches(cell_height),
            )
            tf = tb.text_frame
            _configure_text_frame(tf)
            tf.clear()
            p = tf.paragraphs[0]
            if numeric_columns and col_idx in numeric_columns:
                p.alignment = PP_ALIGN.CENTER
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
            "Del inicio al cierre del periodo, el NPS clásico "
            f"{direction} {_fmt_num_or_nd(abs(classic_delta), decimals=2)} puntos."
        )
    if np.isfinite(detractor_delta_pp) and abs(detractor_delta_pp) >= 0.1:
        direction = "sube" if detractor_delta_pp > 0 else "baja"
        lines.append(
            "El peso detractor "
            f"{direction} {_fmt_num_or_nd(abs(detractor_delta_pp), decimals=2)} puntos porcentuales "
            "en la ventana analizada."
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


def _chain_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if value is None:
        return []
    txt = str(value).strip()
    return [txt] if txt else []


def _chain_incident_records(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        incident_id = str(entry.get("incident_id", "") or "").strip()
        summary = str(entry.get("summary", "") or "").strip()
        url = str(
            entry.get("url", "")
            or entry.get("incident_id__href", "")
            or entry.get("incident_id__hyperlink", "")
            or entry.get("href", "")
            or ""
        ).strip()
        if incident_id or summary:
            out.append(
                {
                    "incident_id": incident_id,
                    "summary": summary,
                    "url": url,
                }
            )
    return out


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


def _build_dimension_view_model(
    *,
    dimension: str,
    slide_number: int,
    selected_raw: pd.DataFrame,
    current_source_period: pd.DataFrame,
    baseline_source_period: pd.DataFrame,
    current_label: str,
    baseline_label: str,
) -> DimensionViewModel:
    delta_df = get_changes_vs_historic(
        current_source_period,
        baseline_source_period,
        dimension=dimension,
        min_n=EDITORIAL_LIMITS.min_change_rows_n,
    )
    if delta_df.empty:
        delta_df = _driver_change_table(
            selected_raw,
            pd.DataFrame(columns=selected_raw.columns),
            dimension=dimension,
        )
    change_table = select_negative_delta_rows(
        delta_df,
        max_rows=CHANGE_SLIDE_LAYOUT.max_rows,
    )
    change_figure = _build_driver_delta_figure(
        delta_df,
        panel_height_in=CHANGE_SLIDE_LAYOUT.chart.height,
    )
    source_for_web = current_source_period if not current_source_period.empty else selected_raw
    web_table = _build_web_dimension_table(source_for_web, dimension=dimension)
    opportunities_min_n = max(20, min(200, int(max(len(selected_raw), 1) * 0.02)))
    opportunities = _opportunities_table(
        selected_raw, dimension=dimension, min_n=opportunities_min_n
    )
    opportunities = _prepare_opportunity_chart_df(opportunities)
    return DimensionViewModel(
        dimension=dimension,
        slide_number=slide_number,
        change_df=delta_df,
        change_table_df=change_table,
        change_figure=change_figure,
        web_heatmap_figure=_build_web_heatmap_figure(source_for_web, row_dim=dimension),
        web_table_df=web_table,
        opportunities_df=opportunities,
        opportunities_figure=_build_opportunity_figure(opportunities),
        opportunity_bullets=explain_opportunities(
            opportunities, max_items=EDITORIAL_LIMITS.max_opportunity_bullets
        ),
    )


def _build_causal_scenarios(
    chains: pd.DataFrame,
    *,
    focus_name: str,
) -> list[CausalScenarioViewModel]:
    scenarios: list[CausalScenarioViewModel] = []
    selected = select_causal_scenarios(chains, max_rows=EDITORIAL_LIMITS.max_causal_scenarios)
    for idx, (_, row) in enumerate(selected.iterrows(), start=1):
        raw_kpis = [
            (
                _focus_probability_label(focus_name),
                _fmt_pct_or_nd(row.get("detractor_probability", np.nan)),
                BBVA_COLORS["red"],
            ),
            ("Confianza", _fmt_num_or_nd(row.get("confidence", np.nan)), BBVA_COLORS["green"]),
            (
                "Vínculos validados",
                str(int(_safe_int(row.get("linked_pairs", 0), default=0))),
                BBVA_COLORS["sky"],
            ),
            (
                "Cambio esperado en NPS Clásico",
                _fmt_signed_or_nd(row.get("nps_delta_expected", np.nan)),
                BBVA_COLORS["orange"],
            ),
            (
                "Impacto total",
                f"{_fmt_num_or_nd(row.get('total_nps_impact', np.nan))} pts",
                BBVA_COLORS["blue"],
            ),
        ]
        incident_lines = [
            _clean_evidence_excerpt(line, max_len=130)
            for line in _chain_list(row.get("incident_examples"))[
                : EDITORIAL_LIMITS.max_helix_evidence
            ]
        ]
        comment_lines = [
            _clean_evidence_excerpt(line, max_len=135)
            for line in _chain_list(row.get("comment_examples"))[
                : EDITORIAL_LIMITS.max_voc_evidence
            ]
        ]
        helix_records = _chain_incident_records(row.get("incident_records"))
        helix_evidence_records = [
            CausalEvidenceRecord(
                incident_id=record.get("incident_id", ""),
                summary=_clean_evidence_excerpt(record.get("summary", ""), max_len=170),
                url=record.get("url", ""),
            )
            for record in helix_records[: EDITORIAL_LIMITS.max_helix_evidence]
        ]
        helix_lines = [
            _clean_evidence_excerpt(
                f"{record.get('incident_id', '')}: {record.get('summary', '')}", max_len=145
            )
            for record in helix_records[: EDITORIAL_LIMITS.max_helix_evidence]
        ]
        if not helix_lines:
            helix_lines = incident_lines[: EDITORIAL_LIMITS.max_helix_evidence]
        scenarios.append(
            CausalScenarioViewModel(
                index=idx,
                row=row,
                kpis=select_nonzero_kpis(
                    raw_kpis, max_items=EDITORIAL_LIMITS.max_visible_causal_kpis
                ),
                incident_lines=incident_lines,
                comment_lines=comment_lines,
                helix_evidence_lines=helix_lines,
                helix_evidence_records=helix_evidence_records,
            )
        )
    return scenarios


def _build_presentation_context(
    *,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    period_start: date,
    period_end: date,
    focus_name: str,
    overall_weekly: pd.DataFrame,
    story_md: str,
    attribution_df: Optional[pd.DataFrame],
    selected_nps_df: Optional[pd.DataFrame],
    comparison_nps_df: Optional[pd.DataFrame],
    touchpoint_source: str,
    entity_summary_df: Optional[pd.DataFrame],
    entity_summary_kpis: Optional[list[dict[str, str]]],
    broken_journeys_df: Optional[pd.DataFrame],
    period_kpis: Optional[dict[str, object]] = None,
) -> PresentationContext:
    period_label = f"{_safe_date(period_start)} -> {_safe_date(period_end)}"
    period_days = (pd.Timestamp(period_end) - pd.Timestamp(period_start)).days + 1
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
    selected_daily_metrics = _daily_metrics_for_ppt(selected_raw)
    daily_mix = _daily_group_mix_from_metrics(selected_daily_metrics)
    if daily_mix.empty and not daily_signals.empty:
        daily_mix = daily_signals[["date", "nps_mean", "detractor_rate"]].copy()
        daily_mix["responses"] = 0.0
        daily_mix["passive_rate"] = (1.0 - daily_mix["detractor_rate"]).clip(lower=0.0)
        daily_mix["promoter_rate"] = 0.0
        daily_mix["nps_classic"] = (1.0 - daily_mix["detractor_rate"] * 2.0) * 100.0

    kpi_history_df = (
        comparison_nps_df
        if comparison_nps_df is not None
        else selected_nps_df if selected_nps_df is not None else pd.DataFrame()
    )
    kpi_current_df = selected_nps_df if selected_nps_df is not None else current_period
    resolved_period_kpis = period_kpis or build_period_kpis(
        history_df=kpi_history_df,
        current_df=kpi_current_df,
        pop_year=POP_ALL,
        pop_month=POP_ALL,
        context_label=period_label,
        period_start=period_start,
        period_end=period_end,
    )
    overview = _period_overview(selected_raw, period_kpis=resolved_period_kpis)
    detractor_raw = (
        selected_raw[selected_raw["band"].astype(str).str.casefold().eq("detractor")].copy()
        if "band" in selected_raw.columns
        else selected_raw.copy()
    )
    text_topics = _text_topics_table(detractor_raw, top_k=EDITORIAL_LIMITS.max_text_chart_clusters)
    current_label = f"{_safe_date(period_start)} -> {_safe_date(period_end)}"
    baseline_label = (
        f"{_safe_date(baseline_period['date'].min())} -> {_safe_date(baseline_period['date'].max())}"
        if not baseline_period.empty
        else "sin base histórica"
    )
    dimensions = {
        "Palanca": _build_dimension_view_model(
            dimension="Palanca",
            slide_number=5,
            selected_raw=selected_raw,
            current_source_period=current_source_period,
            baseline_source_period=baseline_source_period,
            current_label=current_label,
            baseline_label=baseline_label,
        ),
        "Subpalanca": _build_dimension_view_model(
            dimension="Subpalanca",
            slide_number=6,
            selected_raw=selected_raw,
            current_source_period=current_source_period,
            baseline_source_period=baseline_source_period,
            current_label=current_label,
            baseline_label=baseline_label,
        ),
    }
    del current_label, baseline_label

    chains = attribution_df.copy() if attribution_df is not None else pd.DataFrame()
    causal_entity_summary = (
        entity_summary_df.copy() if entity_summary_df is not None else pd.DataFrame()
    )
    method_spec = get_causal_method_spec(touchpoint_source)
    causal = CausalViewModel(
        touchpoint_source=str(touchpoint_source or "").strip(),
        method_label=method_spec.label,
        method_title=method_spec.navigation_title,
        method_subtitle=method_spec.navigation_subtitle,
        entity_summary_df=causal_entity_summary,
        entity_summary_figure=_build_journey_summary_figure(
            causal_entity_summary, touchpoint_source=touchpoint_source
        ),
        entity_summary_kpis=list(entity_summary_kpis or []),
        journey_table_df=_build_journey_table(
            touchpoint_source=touchpoint_source,
            entity_summary_df=causal_entity_summary,
            broken_journeys_df=broken_journeys_df,
        ),
        scenarios=_build_causal_scenarios(chains, focus_name=focus_name),
    )
    return PresentationContext(
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        service_origin_n2=service_origin_n2,
        period_start=period_start,
        period_end=period_end,
        period_label=period_label,
        period_days=period_days,
        focus_name=focus_name,
        overview=overview,
        period_kpis=resolved_period_kpis,
        story_md=story_md,
        selected_raw=selected_raw,
        daily_mix=daily_mix,
        daily_signals=daily_signals,
        overview_figure=_build_overview_figure(
            comparison_nps_df if comparison_nps_df is not None else selected_nps_df,
            period_start=period_start,
            period_end=period_end,
        ),
        text_topics_df=text_topics,
        text_topic_figure=_build_text_topic_figure(text_topics),
        current_label=f"{_safe_date(period_start)} -> {_safe_date(period_end)}",
        baseline_label=(
            f"{_safe_date(baseline_period['date'].min())} -> {_safe_date(baseline_period['date'].max())}"
            if not baseline_period.empty
            else "sin base histórica"
        ),
        dimensions=dimensions,
        causal=causal,
    )


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
    del overview, story_md
    slide = _new_slide(prs, kind="cover")
    _add_bg(slide, BBVA_COLORS["bg_dark"])
    logo = REPORT_ASSETS / "bbva-white.png"
    hero_image = REPORT_ASSETS / "nps-bars.png"
    if logo.exists():
        slide.shapes.add_picture(str(logo), Inches(0.72), Inches(0.48), width=Inches(1.58))
    if hero_image.exists():
        slide.shapes.add_picture(
            str(hero_image), Inches(8.20), Inches(1.44), width=Inches(4.46), height=Inches(4.46)
        )

    title = "Análisis NPS\ntérmico y causalidad"
    subtitle = f"{service_origin} · {service_origin_n1}".strip(" ·")
    if service_origin_n2:
        subtitle = f"{subtitle} · {service_origin_n2}".strip(" ·")
    period_label = f"{_safe_date(period_start)} -> {_safe_date(period_end)}"
    subtitle = f"{subtitle} · {period_label}".strip(" ·")

    hero = slide.shapes.add_textbox(Inches(0.78), Inches(2.02), Inches(7.20), Inches(2.02))
    htf = hero.text_frame
    _configure_text_frame(htf)
    htf.clear()
    hp = htf.paragraphs[0]
    hr = hp.add_run()
    hr.text = title
    hr.font.name = BBVA_FONT_DISPLAY
    hr.font.size = Pt(39)
    hr.font.bold = True
    hr.font.color.rgb = _rgb(BBVA_COLORS["white"])

    sub = slide.shapes.add_textbox(Inches(0.82), Inches(4.38), Inches(7.10), Inches(0.78))
    stf = sub.text_frame
    _configure_text_frame(stf)
    stf.clear()
    sp = stf.paragraphs[0]
    sr = sp.add_run()
    sr.text = subtitle
    sr.font.name = BBVA_FONT_BODY
    sr.font.size = Pt(15)
    sr.font.color.rgb = _rgb("C7D3EA")

    rule = slide.shapes.add_shape(
        MSO_AUTO_SHAPE_TYPE.RECTANGLE, Inches(0.82), Inches(5.52), Inches(6.52), Inches(0.05)
    )
    rule.fill.solid()
    rule.fill.fore_color.rgb = _rgb(BBVA_COLORS["sky"])
    rule.line.fill.background()


def _add_nps_section_cover_slide(prs: Presentation, *, context: PresentationContext) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="La señal del periodo, comparada con su base histórica",
        subtitle=f"{context.service_origin} · {context.service_origin_n1} · {context.period_label}",
    )

    period_scope = _dict_payload(context.period_kpis.get("period"))
    period_display = _dict_payload(period_scope.get("display"))
    period_base_display = _dict_payload(period_scope.get("base_display"))
    period_deltas = _dict_payload(period_scope.get("deltas"))
    comment_total = str(
        period_display.get(
            "comments",
            format_volume(context.overview.get("comments", 0)),
        )
    )
    summary = (
        _cover_summary_lines(context.overview, context.story_md)
        or [
            "El bloque ordena la señal del periodo desde evolución, comentario y deterioros por dimensión.",
        ]
    )[:3]
    comment_line = f"Se analizaron **{comment_total}** comentarios útiles durante el período."
    if comment_line not in summary:
        summary.append(comment_line)
    _add_bullet_lines(
        slide,
        left=0.82,
        top=1.48,
        width=7.2,
        height=4.96,
        title=EDITORIAL_COPY.nps_highlights_title,
        lines=summary,
        accent=BBVA_COLORS["sky"],
        body_font_size_pt=12.8,
    )
    card_specs = [
        ("Comentarios", "comments", BBVA_COLORS["sky"]),
        ("Score medio", "nps_average", BBVA_COLORS["green"]),
        ("NPS Clásico", "classic_nps", BBVA_COLORS["blue"]),
        ("Detractores", "detractor_rate", BBVA_COLORS["orange"]),
        ("Promotores", "promoter_rate", BBVA_COLORS["green"]),
    ]
    for index, (label, kpi_key, accent) in enumerate(card_specs):
        delta_payload = _dict_payload(period_deltas.get(kpi_key))
        _add_comparative_stat_card(
            slide,
            left=8.55,
            top=1.48 + index * 1.02,
            width=3.72,
            height=0.88,
            label=label,
            base_value=str(period_base_display.get(kpi_key, "n/d")),
            actual_value=str(period_display.get(kpi_key, "n/d")),
            delta_text=str(
                delta_payload.get(
                    "display", format_delta(delta_payload.get("value"), kpi_key=kpi_key)
                )
            ),
            accent=accent,
            delta_accent=_delta_accent(delta_payload, default=BBVA_COLORS["muted"]),
            base_label=str(period_scope.get("base_label", "Histórico anterior")),
            actual_label=str(period_scope.get("actual_label", context.period_label)),
        )
    _add_takeaway_band(
        slide,
        summary[0] if summary else "La lectura ejecutiva se concentra en el periodo solicitado.",
    )


def _add_dimension_change_slide(
    prs: Presentation,
    *,
    context: PresentationContext,
    view_model: DimensionViewModel,
) -> None:
    dimension = view_model.dimension
    layout = CHANGE_SLIDE_LAYOUT
    table_style = EXECUTIVE_TABLE_STYLE
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title=f"El deterioro frente al histórico se concentra en {dimension}",
        subtitle=(
            f"Periodo actual frente a la base histórica anterior · actual {context.current_label} · "
            f"base {context.baseline_label}"
        ),
    )

    _panel(
        slide,
        left=layout.chart_panel.left,
        top=layout.chart_panel.top,
        width=layout.chart_panel.width,
        height=layout.chart_panel.height,
        title="",
        fill=BBVA_COLORS["white"],
        border=table_style.panel_border,
    )
    _figure_in_panel(
        slide,
        figure=view_model.change_figure,
        left=layout.chart.left,
        top=layout.chart.top,
        width=layout.chart.width,
        height=layout.chart.height,
        empty_note=f"No hay base histórica suficiente para comparar {dimension.lower()}.",
        target_ppi=190,
    )

    _panel(
        slide,
        left=layout.table_panel.left,
        top=layout.table_panel.top,
        width=layout.table_panel.width,
        height=layout.table_panel.height,
        title="",
        fill=BBVA_COLORS["white"],
        border=table_style.panel_border,
    )

    rows = []
    for row in view_model.change_table_df.head(layout.max_rows).itertuples():
        rows.append(
            [
                str(row.value),
                _fmt_signed_or_nd(row.delta_nps, decimals=2),
                _fmt_num_or_nd(row.nps_current, decimals=2),
                _fmt_num_or_nd(row.nps_baseline, decimals=2),
                _fmt_count_or_nd(row.n_current),
                _fmt_count_or_nd(row.n_baseline),
            ]
        )
    if not rows:
        rows = [["Sin deterioro real", "-", "-", "-", "-", "-"]]

    table_left = layout.table_panel.left + layout.table_inner_x
    table_top = layout.table_panel.top + layout.table_inner_top
    table_width = layout.table_panel.width - (layout.table_inner_x * 2)
    _add_grid_table(
        slide,
        left=table_left,
        top=table_top,
        width=table_width,
        headers=list(layout.headers),
        rows=rows[: layout.max_rows],
        col_width_ratios=list(layout.width_ratios),
        header_height=layout.header_height,
        row_height=layout.row_height,
        header_fill=table_style.header_fill,
        border_color=table_style.panel_border,
        header_font_size_pt=10.0,
        cell_font_size_pt=9.4,
        show_row_dividers=False,
    )
    worst = rows[0]
    _add_takeaway_band(
        slide,
        f"Mayor deterioro: {worst[0]} ({worst[1]} puntos frente a la base).",
        top=6.92,
    )


def _add_web_pain_dimension_slide(
    prs: Presentation,
    *,
    context: PresentationContext,
    view_model: DimensionViewModel,
) -> None:
    dimension = view_model.dimension
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title=f"El dolor Web se localiza en focos concretos de {dimension}",
        subtitle=f"Score del canal Web por {dimension.lower()} dentro del periodo analizado · {context.period_label}",
    )
    _panel(
        slide,
        left=0.66,
        top=1.48,
        width=8.02,
        height=5.04,
        title="",
    )
    _figure_in_panel(
        slide,
        figure=view_model.web_heatmap_figure,
        left=0.94,
        top=1.68,
        width=7.34,
        height=4.50,
        empty_note=f"No hay señal suficiente para mostrar {dimension.lower()} en el canal Web.",
        target_ppi=178,
    )
    rows = [
        [
            _clip(row.value, 36),
            _fmt_count_or_nd(row.n),
            _fmt_num_or_nd(row.nps, decimals=1),
            _fmt_pct_or_nd(row.detractor_rate),
        ]
        for row in view_model.web_table_df.head(EDITORIAL_LIMITS.max_web_rows).itertuples()
    ]
    _add_compact_table(
        slide,
        left=8.92,
        top=1.48,
        width=3.76,
        title="Focos Web",
        headers=[dimension, "n", "Score", "% det."],
        rows=rows or [["Sin datos", "-", "-", "-"]],
        row_height=0.44,
        col_width_ratios=[2.20, 0.88, 0.76, 0.82],
        clip_lengths=[34, 10, 8, 8],
        font_size_pt=9.7,
        header_font_size_pt=9.0,
        cell_height=0.30,
        max_rows=EDITORIAL_LIMITS.max_web_rows,
        numeric_columns={1, 2, 3},
    )
    leader = rows[0] if rows else ["Sin datos", "-", "-", "-"]
    _add_takeaway_band(
        slide,
        f"Foco prioritario Web: {leader[0]} · score {leader[2]} · detractores {leader[3]}.",
    )


def _add_opportunity_dimension_slide(
    prs: Presentation,
    *,
    context: PresentationContext,
    view_model: DimensionViewModel,
) -> None:
    dimension = view_model.dimension
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title=f"Las oportunidades combinan impacto potencial y evidencia suficiente · {dimension}",
        subtitle=f"Ranking por impacto potencial y solidez de evidencia · {context.period_label}",
    )
    _panel(
        slide,
        left=0.66,
        top=1.48,
        width=12.02,
        height=4.02,
        title="",
    )
    _figure_in_panel(
        slide,
        figure=view_model.opportunities_figure,
        left=0.86,
        top=1.66,
        width=11.62,
        height=3.58,
        empty_note=f"No se identificaron oportunidades robustas para {dimension.lower()} con el umbral actual.",
        target_ppi=176,
    )
    _add_bullet_lines(
        slide,
        left=0.66,
        top=5.66,
        width=12.02,
        height=0.82,
        title="",
        lines=view_model.opportunity_bullets,
        accent=BBVA_COLORS["line"],
        body_font_size_pt=12.0,
    )
    _add_takeaway_band(
        slide,
        view_model.opportunity_bullets[0]
        if view_model.opportunity_bullets
        else f"No hay oportunidades robustas para {dimension.lower()} con el umbral actual.",
    )


def _add_overview_slide(
    prs: Presentation,
    *,
    service_origin: str,
    service_origin_n1: str,
    period_label: str,
    period_start: date,
    period_end: date,
    overview: dict[str, object],
    selected_nps_df: Optional[pd.DataFrame],
    overview_figure: Optional[go.Figure] = None,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="El periodo se explica dentro de todo el histórico disponible",
        subtitle=f"NPS clásico · histórico completo con {period_label} resaltado · {service_origin} · {service_origin_n1}",
    )
    _panel(
        slide,
        left=4.02,
        top=1.48,
        width=8.66,
        height=5.04,
        title="Histórico completo · periodo solicitado resaltado",
    )
    _figure_in_panel(
        slide,
        figure=(
            overview_figure
            if overview_figure is not None
            else _build_overview_figure(
                selected_nps_df,
                period_start=period_start,
                period_end=period_end,
            )
        ),
        left=4.18,
        top=1.84,
        width=8.30,
        height=4.42,
        empty_note="No hay suficiente señal para construir el histórico completo.",
        target_ppi=170,
    )

    month_label = _month_label_es(period_end).title()
    trend_lines = [
        f"El periodo arranca con NPS clásico **{_fmt_num_or_nd(overview.get('start_classic', np.nan))}** y termina en **{_fmt_num_or_nd(overview.get('end_classic', np.nan))}**.",
        f"El peso detractor pasa de **{_fmt_pct_or_nd(overview.get('start_detr', np.nan))}** a **{_fmt_pct_or_nd(overview.get('end_detr', np.nan))}**.",
        "NPS Clásico = % Promotores - % Detractores. Rango: -100 a +100.",
        "Se utiliza para seguir la señal neta del período.",
    ]
    _add_bullet_lines(
        slide,
        left=0.66,
        top=1.48,
        width=3.10,
        height=5.04,
        title=f"Mensaje · {month_label}",
        lines=trend_lines,
        accent=BBVA_COLORS["orange"],
        body_font_size_pt=13.0,
    )
    _add_takeaway_band(
        slide,
        f"El mensaje y los KPIs corresponden exclusivamente a {period_label}; la curva aporta el contexto histórico completo.",
    )


def _add_deep_dive_slide(
    prs: Presentation,
    *,
    period_label: str,
    text_topics_df: pd.DataFrame,
    topic_figure: Optional[go.Figure] = None,
) -> None:
    topic_fig = (
        topic_figure if topic_figure is not None else _build_text_topic_figure(text_topics_df)
    )

    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="La voz detractora concentra los temas que más erosionan la experiencia",
        subtitle=f"Temas más repetidos en los comentarios detractores del periodo · {period_label}",
    )
    _panel(slide, left=0.66, top=1.48, width=12.02, height=5.04, title="Top temas detractores")
    _figure_in_panel(
        slide,
        figure=topic_fig,
        left=0.82,
        top=1.82,
        width=11.62,
        height=2.88,
        empty_note="No hay suficiente volumen textual para construir el top 10.",
        target_ppi=176,
    )

    summary_df = select_text_clusters(
        text_topics_df, max_clusters=EDITORIAL_LIMITS.max_text_table_clusters
    )
    table_rows = [
        [
            str(row.top_terms_txt),
            str(row.example_txt),
        ]
        for row in summary_df.itertuples()
    ]
    _add_compact_table(
        slide,
        left=0.82,
        top=4.82,
        width=11.62,
        title="",
        headers=["top_terms", "examples"],
        rows=table_rows or [["Sin datos", "Sin ejemplos"]],
        row_height=0.34,
        col_width_ratios=[4.20, 5.80],
        clip_lengths=[74, 88],
        font_size_pt=9.8,
        header_font_size_pt=9.7,
        cell_height=0.26,
        max_rows=EDITORIAL_LIMITS.max_text_table_clusters,
    )
    leader = summary_df.iloc[0].get("top_terms_txt", "") if not summary_df.empty else ""
    _add_takeaway_band(
        slide,
        f"Prioridad de escucha: {leader}." if leader else "Sin señal textual suficiente para priorizar temas.",
    )


def _add_journeys_summary_slide(
    prs: Presentation,
    *,
    period_label: str,
    touchpoint_source: str,
    entity_summary_df: pd.DataFrame,
    entity_summary_kpis: list[dict[str, str]],
    entity_summary_figure: Optional[go.Figure] = None,
    journey_table_df: Optional[pd.DataFrame] = None,
) -> None:
    method_spec = get_causal_method_spec(touchpoint_source)
    layout = JOURNEY_SUMMARY_LAYOUT

    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title=method_spec.navigation_title,
        subtitle=f"{method_spec.navigation_subtitle} · {period_label}",
    )
    for index, metric in enumerate(entity_summary_kpis[:3]):
        _add_stat_card(
            slide,
            left=0.66 + index * 3.96,
            top=1.48,
            width=3.78,
            height=1.10,
            label=str(metric.get("label", "")).strip(),
            value=str(metric.get("value", "")).strip(),
            accent=(
                BBVA_COLORS["red"]
                if index == 0
                else BBVA_COLORS["orange"] if index == 1 else BBVA_COLORS["green"]
            ),
        )
    _panel(
        slide,
        left=layout.chart_panel.left,
        top=layout.chart_panel.top,
        width=layout.chart_panel.width,
        height=layout.chart_panel.height,
        title="",
    )
    _figure_in_panel(
        slide,
        figure=(
            entity_summary_figure
            if entity_summary_figure is not None
            else _build_journey_summary_figure(
                entity_summary_df, touchpoint_source=touchpoint_source
            )
        ),
        left=layout.chart.left,
        top=layout.chart.top,
        width=layout.chart.width,
        height=layout.chart.height,
        empty_note=method_spec.table_empty_message,
        target_ppi=170,
    )
    table_df = journey_table_df if journey_table_df is not None else pd.DataFrame()
    rows = []
    for row in table_df.head(EDITORIAL_LIMITS.max_journey_rows).itertuples():
        rows.append(
            [
                str(getattr(row, "journey", "") or ""),
                str(getattr(row, "touchpoint", "") or ""),
                str(getattr(row, "palanca", "") or ""),
                str(getattr(row, "subpalanca", "") or ""),
                str(
                    getattr(row, "anchor_topic", "")
                    or getattr(row, "nps_topic", "")
                    or getattr(row, "topic", "")
                    or ""
                ),
            ]
        )
    _add_wrapped_journey_table(
        slide,
        left=layout.table_panel_left,
        top=layout.table_panel_top,
        width=layout.table_panel_width,
        title=method_spec.table_title,
        headers=list(layout.headers),
        rows=rows or [["Sin evidencia suficiente", "-", "-", "-", "-"]],
        col_width_ratios=list(layout.width_ratios),
        font_size_pt=8.9,
        header_font_size_pt=8.2,
        max_rows=layout.max_rows,
        panel_border=BBVA_COLORS["red"],
        max_rows_height=layout.table_max_rows_height,
        min_row_height=layout.wrapped_min_row_height,
        header_height=layout.wrapped_header_height,
        title_pad=layout.wrapped_title_pad,
    )



def _add_causal_analysis_slide(
    prs: Presentation,
    *,
    context: PresentationContext,
    scenario: CausalScenarioViewModel,
) -> None:
    row = scenario.row
    method_spec = get_causal_method_spec(
        str(row.get("presentation_mode", "") or context.causal.touchpoint_source).strip()
    )
    title = _clip(row.get("nps_topic", f"Escenario {scenario.index}"), 72)
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title=f"{title}: causalidad defendible",
        subtitle=(
            f"Análisis causal de {method_spec.entity_singular}: "
            f"Escenario #{scenario.index} · {context.period_label}"
        ),
    )
    chain_statement = (
        f"{_fmt_count_with_label(row.get('linked_incidents', 0), singular='incidencia Helix', plural='incidencias Helix')} y "
        f"{_fmt_count_with_label(row.get('linked_comments', 0), singular='comentario', plural='comentarios')} convergen en "
        f"{title} con lectura {method_spec.label}."
    )
    _add_markdown_panel(
        slide,
        left=0.66,
        top=1.48,
        width=12.02,
        height=0.76,
        title=EDITORIAL_COPY.scenario_summary_title,
        body=chain_statement,
        border=BBVA_COLORS["sky"],
        title_size=13,
        body_size=11.3,
        max_chars=210,
    )

    evidence = (
        scenario.helix_evidence_lines[: EDITORIAL_LIMITS.max_helix_evidence]
        or scenario.incident_lines[: EDITORIAL_LIMITS.max_helix_evidence]
        or ["No se han encontrado evidencias Helix adicionales defendibles para este escenario."]
    )
    _add_incident_evidence_panel(
        slide,
        left=4.12,
        top=2.42,
        width=8.56,
        height=2.76,
        records=scenario.helix_evidence_records,
        fallback_lines=evidence,
    )

    visible_kpis = scenario.kpis[:4]
    for pos, (label, value, accent) in enumerate(visible_kpis):
        _add_stat_card(
            slide,
            left=0.66,
            top=2.42 + pos * 1.02,
            width=3.18,
            height=0.86,
            label=label,
            value=value,
            accent=accent,
        )
    _add_bullet_lines(
        slide,
        left=4.12,
        top=5.38,
        width=8.56,
        height=1.28,
        title=EDITORIAL_COPY.linked_comments_examples_title,
        lines=scenario.comment_lines
        or ["No se han encontrado verbatims adicionales para este escenario."],
        accent=BBVA_COLORS["red"],
        body_font_size_pt=9.8,
    )
    _add_takeaway_band(
        slide,
        f"{method_spec.flow}: {title} concentra evidencia operativa y voz de cliente.",
        top=6.90,
    )


def _add_causal_fallback_slide(
    prs: Presentation,
    *,
    context: PresentationContext,
) -> None:
    slide = _new_slide(prs)
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="La evidencia disponible no permite afirmar causalidad",
        subtitle=(
            "No se identificaron suficientes evidencias para construir escenarios causales "
            f"robustos durante el período analizado · {context.period_label}"
        ),
    )

    selected = context.selected_raw if context.selected_raw is not None else pd.DataFrame()
    comments = (
        int(selected.get("comment_txt", pd.Series(dtype=str)).astype(str).str.strip().ne("").sum())
        if not selected.empty
        else 0
    )
    active_levers = (
        int(
            selected.get("Palanca", pd.Series(dtype=str))
            .astype(str)
            .str.strip()
            .replace("", np.nan)
            .nunique()
        )
        if not selected.empty
        else 0
    )
    entity_summary = context.causal.entity_summary_df
    linked_pairs = (
        float(pd.to_numeric(entity_summary.get("linked_pairs"), errors="coerce").fillna(0.0).sum())
        if entity_summary is not None
        and not entity_summary.empty
        and "linked_pairs" in entity_summary.columns
        else 0.0
    )
    incidents = (
        float(
            pd.to_numeric(entity_summary.get("linked_incidents"), errors="coerce").fillna(0.0).sum()
        )
        if entity_summary is not None
        and not entity_summary.empty
        and "linked_incidents" in entity_summary.columns
        else 0.0
    )
    confidence = (
        float(pd.to_numeric(entity_summary.get("confidence"), errors="coerce").dropna().mean())
        if entity_summary is not None
        and not entity_summary.empty
        and "confidence" in entity_summary.columns
        else float("nan")
    )
    coverage = linked_pairs / max(float(len(selected)), 1.0)
    metrics = [
        ("Volumen analizado", _fmt_count_or_nd(len(selected)), BBVA_COLORS["blue"]),
        ("Incidencias observadas", _fmt_count_or_nd(incidents), BBVA_COLORS["orange"]),
        ("Comentarios útiles", _fmt_count_or_nd(comments), BBVA_COLORS["sky"]),
        ("Palancas activas", _fmt_count_or_nd(active_levers), BBVA_COLORS["green"]),
        ("Cobertura causal", _fmt_pct_or_nd(coverage), BBVA_COLORS["red"]),
        ("Confianza promedio", _fmt_num_or_nd(confidence), BBVA_COLORS["green"]),
    ]
    for idx, (label, value, accent) in enumerate(metrics):
        row = idx // 3
        col = idx % 3
        _add_stat_card(
            slide,
            left=0.66 + col * 4.05,
            top=1.62 + row * 1.36,
            width=3.72,
            height=1.08,
            label=label,
            value=value,
            accent=accent,
        )
    _add_bullet_lines(
        slide,
        left=0.66,
        top=4.52,
        width=12.02,
        height=1.78,
        title="Lectura ejecutiva",
        lines=[
            "No se identificaron patrones causales estadísticamente defendibles para el período analizado.",
            "El deck mantiene el análisis descriptivo de NPS Clásico, brechas, detracción, temas y evolución.",
            "La ausencia de escenarios no modifica la lectura de NPS ni las brechas frente al NPS global.",
        ],
        accent=BBVA_COLORS["orange"],
        body_font_size_pt=13.0,
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
    entity_summary_df: Optional[pd.DataFrame] = None,
    entity_summary_kpis: Optional[list[dict[str, str]]] = None,
    executive_journey_catalog: Optional[list[dict[str, object]]] = None,
    broken_journeys_df: Optional[pd.DataFrame] = None,
    report_dimension_analysis: str = "palanca",
    period_kpis: Optional[dict[str, object]] = None,
    include_causal_section: bool = True,
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
        rationale_df,
        ranking_df,
        by_topic_daily,
        lag_days_by_topic,
        by_topic_weekly,
        lag_weeks_by_topic,
        changepoints_by_topic,
        executive_journey_catalog,
    )
    dimension_mode = str(report_dimension_analysis or "palanca").strip().lower()
    if dimension_mode not in {"palanca", "subpalanca"}:
        dimension_mode = "palanca"

    prs = build_presentation(template_path=template_path, workspace_root=Path.cwd())
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)

    context = _build_presentation_context(
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        service_origin_n2=service_origin_n2,
        period_start=period_start,
        period_end=period_end,
        focus_name=focus_name,
        overall_weekly=overall_weekly,
        story_md=story_md,
        attribution_df=attribution_df,
        selected_nps_df=selected_nps_df,
        comparison_nps_df=comparison_nps_df,
        touchpoint_source=touchpoint_source,
        entity_summary_df=entity_summary_df,
        entity_summary_kpis=entity_summary_kpis,
        broken_journeys_df=broken_journeys_df,
        period_kpis=period_kpis,
    )

    _add_cover_slide(
        prs,
        service_origin=context.service_origin,
        service_origin_n1=context.service_origin_n1,
        service_origin_n2=context.service_origin_n2,
        period_start=context.period_start,
        period_end=context.period_end,
        overview=context.overview,
        story_md=context.story_md,
    )
    _add_nps_section_cover_slide(prs, context=context)
    _add_overview_slide(
        prs,
        service_origin=context.service_origin,
        service_origin_n1=context.service_origin_n1,
        period_label=context.period_label,
        period_start=context.period_start,
        period_end=context.period_end,
        overview=context.overview,
        selected_nps_df=selected_nps_df,
        overview_figure=context.overview_figure,
    )
    _add_deep_dive_slide(
        prs,
        period_label=context.period_label,
        text_topics_df=context.text_topics_df,
        topic_figure=context.text_topic_figure,
    )
    visible_dimension = "Subpalanca" if dimension_mode == "subpalanca" else "Palanca"
    _add_dimension_change_slide(
        prs,
        context=context,
        view_model=context.dimensions[visible_dimension],
    )
    _add_web_pain_dimension_slide(
        prs,
        context=context,
        view_model=context.dimensions[visible_dimension],
    )
    _add_opportunity_dimension_slide(
        prs,
        context=context,
        view_model=context.dimensions[visible_dimension],
    )
    if include_causal_section:
        _add_journeys_summary_slide(
            prs,
            period_label=context.period_label,
            touchpoint_source=context.causal.touchpoint_source,
            entity_summary_df=context.causal.entity_summary_df,
            entity_summary_kpis=context.causal.entity_summary_kpis,
            entity_summary_figure=context.causal.entity_summary_figure,
            journey_table_df=context.causal.journey_table_df,
        )
        if not context.causal.scenarios:
            _add_causal_fallback_slide(
                prs,
                context=context,
            )
        for scenario in context.causal.scenarios:
            _add_causal_analysis_slide(
                prs,
                context=context,
                scenario=scenario,
            )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    file_name = f"nps-incidencias-{_slug(service_origin)}-{_slug(service_origin_n1)}-{stamp}.pptx"

    buff = BytesIO()
    prs.save(buff)
    return BusinessPptResult(
        file_name=file_name, content=buff.getvalue(), slide_count=len(prs.slides)
    )
