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
from copy import deepcopy
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
from pptx.oxml.xmlchemy import OxmlElement
from pptx.util import Inches, Pt

from nps_lens.analytics.channel_topic_scope import (
    restrict_to_topics,
    topics_observed_in_channel,
)
from nps_lens.analytics.drivers import driver_table
from nps_lens.analytics.incident_attribution import (
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
)
from nps_lens.analytics.nps_helix_link import build_nps_topic
from nps_lens.analytics.text_mining import summarize_taxonomy
from nps_lens.design.tokens import (
    DesignTokens,
    bbva_typography_tokens,
    executive_report_palette,
)
from nps_lens.domain.causal_methods import get_causal_method_spec
from nps_lens.platform.resources import resource_root
from nps_lens.reports.content_selectors import (
    select_causal_scenarios,
    select_negative_delta_rows,
    select_nonzero_kpis,
    select_text_clusters,
)
from nps_lens.reports.editorial_tokens import EDITORIAL_LIMITS
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
    chart_daily_nps_committee_stack,
    chart_driver_delta,
)
from nps_lens.ui.historic_changes import get_changes_vs_historic
from nps_lens.ui.population import POP_ALL
from nps_lens.ui.theme import get_theme

BBVA_COLORS = executive_report_palette(DesignTokens.default(), mode="light")
BBVA_TYPOGRAPHY = bbva_typography_tokens()
BBVA_FONT_DISPLAY = BBVA_TYPOGRAPHY.display
BBVA_FONT_HEAD = BBVA_TYPOGRAPHY.heading
BBVA_FONT_BODY = BBVA_TYPOGRAPHY.body
BBVA_FONT_MEDIUM = BBVA_TYPOGRAPHY.medium
REPORT_DESIGN_VERSION = "thermal-causality-v3"
REPORT_ASSETS = resource_root() / "assets" / "ppt" / "bbva"
REPORT_TEMPLATE = REPORT_ASSETS / "nps-thermal-causality-v3-template.pptx"
_FIGURE_PNG_CACHE: OrderedDict[str, bytes] = OrderedDict()
_FIGURE_PNG_LOCK = RLock()
_FIGURE_PNG_CACHE_LIMIT = 24


@dataclass(frozen=True)
class BusinessPptResult:
    file_name: str
    content: bytes
    slide_count: int
    compact_file_name: str
    compact_content: bytes
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


def _long_date_es(value: object) -> str:
    timestamp = _coerce_datetime_scalar(value)
    if pd.isna(timestamp):
        return _safe_date(value)
    month = _month_label_es(timestamp).split()[0].title()
    return f"{timestamp.day} de {month} de {timestamp.year}"


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
        "Canal",
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
    out["Canal"] = out.get("Canal", pd.Series([""] * len(out), index=out.index)).astype(str).str.strip()
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
    return current, baseline


def _period_overview(
    current_nps_df: pd.DataFrame,
    *,
    period_kpis: Optional[dict[str, object]] = None,
    topic_channel: str = "Web",
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
        period_block.get("temporal", {})
        if isinstance(period_block.get("temporal"), dict)
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
    driver_col = "Subpalanca" if "Subpalanca" in current_nps_df.columns else "Palanca"
    topic_keys = topics_observed_in_channel(current_nps_df, driver_col, topic_channel)
    driver_source = restrict_to_topics(current_nps_df, driver_col, topic_keys)
    if driver_col not in driver_source.columns:
        driver_col = "Palanca"
    pain_point = ""
    strength_point = ""
    if driver_col in driver_source.columns and "NPS" in driver_source.columns:
        driver_view = driver_source[[driver_col, "NPS"]].copy().dropna(subset=["NPS"])
        if not driver_view.empty:
            driver_view[driver_col] = driver_view[driver_col].astype(str).str.strip()
            driver_view = driver_view[driver_view[driver_col] != ""]
            if not driver_view.empty:
                detractor_counts = driver_view.loc[
                    driver_view["NPS"].le(6), driver_col
                ].value_counts()
                promoter_counts = driver_view.loc[
                    driver_view["NPS"].ge(9), driver_col
                ].value_counts()
                if not detractor_counts.empty:
                    pain_point = str(detractor_counts.index[0])
                if not promoter_counts.empty:
                    strength_point = str(promoter_counts.index[0])
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

    topics = summarize_taxonomy(
        current_nps_df.rename(
            columns={"palanca": "Palanca", "subpalanca": "Subpalanca", "comment_txt": "Comment"}
        ),
        limit=top_k,
    )
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


def _build_overview_figure(
    history_nps_df: Optional[pd.DataFrame],
    *,
    period_start: date,
    period_end: date,
    metrics: Optional[pd.DataFrame] = None,
) -> Optional[go.Figure]:
    history = _coerce_nps_records(history_nps_df)
    history = history.loc[history["date"].le(pd.Timestamp(period_end))].copy()
    if history.empty:
        return None
    if metrics is not None:
        full_metrics = metrics
    else:
        scored = history.dropna(subset=["date", "NPS"]).copy()
        if scored.empty:
            return None
        scored["day"] = scored["date"].dt.normalize()
        scored["det"] = scored["NPS"].le(6).astype(int)
        scored["pas"] = scored["NPS"].between(7, 8).astype(int)
        scored["pro"] = scored["NPS"].ge(9).astype(int)
        full_metrics = (
            scored.groupby("day", as_index=False)
            .agg(n=("NPS", "size"), det=("det", "sum"), pas=("pas", "sum"), pro=("pro", "sum"))
            .sort_values("day")
        )
        total = full_metrics["n"].replace({0: np.nan})
        full_metrics["det_pct"] = full_metrics["det"] / total * 100.0
        full_metrics["pas_pct"] = full_metrics["pas"] / total * 100.0
        full_metrics["pro_pct"] = full_metrics["pro"] / total * 100.0
        full_metrics["classic_nps"] = full_metrics["pro_pct"] - full_metrics["det_pct"]
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
        annotation_text="Periodo analizado",
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


def _build_topic_dimension_table(source_df: pd.DataFrame, *, dimension: str) -> pd.DataFrame:
    cols = ["value", "n", "nps", "detractor_rate"]
    required = {dimension, "NPS"}
    if source_df is None or source_df.empty or not required.issubset(set(source_df.columns)):
        return pd.DataFrame(columns=cols)
    work = _normalize_presentation_categories(source_df, columns=[dimension])
    work = work.dropna(subset=[dimension, "NPS"]).copy()
    work[dimension] = work[dimension].astype(str).str.strip()
    work["NPS"] = pd.to_numeric(work["NPS"], errors="coerce")
    work = work[work[dimension].ne("")].dropna(subset=["NPS"]).copy()
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
        .head(EDITORIAL_LIMITS.max_topic_rows)[cols]
        .copy()
    )


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
        "similarity",
    ]
    if (
        str(touchpoint_source or "").strip() == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS
        and broken_journeys_df is not None
        and not broken_journeys_df.empty
    ):
        source = broken_journeys_df.copy()
        source["links_sort"] = pd.to_numeric(
            source.get("linked_pairs"),
            errors="coerce",
        ).fillna(0.0)
        source["similarity_sort"] = pd.to_numeric(
            source.get("semantic_cohesion"),
            errors="coerce",
        ).fillna(0.0)
        source["nps_sort"] = pd.to_numeric(
            source.get("avg_nps"),
            errors="coerce",
        ).fillna(10.0)
        out = (
            source.sort_values(
                ["links_sort", "similarity_sort", "nps_sort"],
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
                "similarity": pd.to_numeric(
                    _first_existing_series(out, "avg_similarity"),
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
            "similarity": pd.to_numeric(
                _first_existing_series(source, "avg_similarity"),
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
        title_text="Vínculos semánticos Helix↔VoC",
        tickfont=dict(size=17),
        title_font=dict(size=18),
        nticks=6,
        automargin=True,
    )
    fig.update_layout(margin=dict(l=left_margin, r=102, t=14, b=38), bargap=0.22)
    fig.update_coloraxes(
        colorbar=dict(
            title=dict(text="Confianza", side="right", font=dict(size=15)),
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

    fonts_dir = REPORT_ASSETS / "fonts"
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
            (themed.to_json() + f"|{width}|{height}|{panel_width_in}|{panel_height_in}").encode(
                "utf-8"
            )
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


def _dict_payload(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


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


def _plain_md(text: object) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    s = re.sub(r"`([^`]*)`", r"\1", s)
    s = re.sub(r"\*\*([^*]+)\*\*", r"\1", s)
    s = re.sub(r"^#+\s*", "", s)
    return " ".join(s.split())


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


def _build_dimension_view_model(
    *,
    dimension: str,
    selected_raw: pd.DataFrame,
    current_source_period: pd.DataFrame,
    baseline_source_period: pd.DataFrame,
    topic_channel: str,
) -> DimensionViewModel:
    topic_source = current_source_period if not current_source_period.empty else selected_raw
    topic_keys = topics_observed_in_channel(topic_source, dimension, topic_channel)
    selected_raw = restrict_to_topics(selected_raw, dimension, topic_keys)
    current_source_period = restrict_to_topics(current_source_period, dimension, topic_keys)
    baseline_source_period = restrict_to_topics(baseline_source_period, dimension, topic_keys)
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
        max_rows=EDITORIAL_LIMITS.max_change_rows,
    )
    change_figure = _build_driver_delta_figure(
        delta_df,
        panel_height_in=2.58,
    )
    metric_source = current_source_period if not current_source_period.empty else selected_raw
    return DimensionViewModel(
        change_table_df=change_table,
        change_figure=change_figure,
        topic_table_df=_build_topic_dimension_table(metric_source, dimension=dimension),
    )


def _build_causal_scenarios(
    chains: pd.DataFrame,
    *,
    focus_name: str,
) -> list[CausalScenarioViewModel]:
    scenarios: list[CausalScenarioViewModel] = []
    selected = select_causal_scenarios(chains, max_rows=len(chains))
    for idx, (_, row) in enumerate(selected.iterrows(), start=1):
        raw_kpis = [
            (
                f"% {focus_name} en incidencia alta",
                _fmt_pct_or_nd(row.get("focus_rate_high_incidence", np.nan)),
                BBVA_COLORS["red"],
            ),
            (
                "Nota media del tópico",
                _fmt_num_or_nd(row.get("avg_nps", np.nan)),
                BBVA_COLORS["green"],
            ),
            (
                "Vínculos semánticos",
                str(int(_safe_int(row.get("linked_pairs", 0), default=0))),
                BBVA_COLORS["sky"],
            ),
            (
                "Confianza",
                _fmt_pct_or_nd(row.get("avg_similarity", np.nan), decimals=0),
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
                segments=(record.get("summary_segments") or []),
            )
            for record in helix_records
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
    topic_channel: str,
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
    selected_raw = selected_raw.loc[
        selected_raw["date"].between(pd.Timestamp(period_start), pd.Timestamp(period_end))
    ].copy()
    if selected_raw.empty:
        selected_raw = compare_raw.loc[
            compare_raw["date"].between(pd.Timestamp(period_start), pd.Timestamp(period_end))
        ].copy()

    kpi_history_df = (
        comparison_nps_df
        if comparison_nps_df is not None
        else selected_nps_df if selected_nps_df is not None else pd.DataFrame()
    )
    kpi_current_df = current_source_period if not current_source_period.empty else selected_raw
    resolved_period_kpis = period_kpis or build_period_kpis(
        history_df=kpi_history_df,
        current_df=kpi_current_df,
        pop_year=POP_ALL,
        pop_month=POP_ALL,
        context_label=period_label,
        period_start=period_start,
        period_end=period_end,
    )
    channel_key = str(topic_channel or "").strip().casefold()
    channel_values = selected_raw["Canal"].fillna("").astype(str).str.strip()
    channel_selected = (
        selected_raw
        if channel_key in {"", "all", "todos"} or not channel_values.ne("").any()
        else selected_raw.loc[channel_values.str.casefold().eq(channel_key)]
    )
    overview = _period_overview(
        selected_raw,
        period_kpis=resolved_period_kpis,
        topic_channel=topic_channel,
    )
    base_kpis = compute_score_kpis(baseline_source_period)
    cumulative_kpis = compute_score_kpis(
        pd.concat([baseline_source_period, current_source_period], ignore_index=True)
    )
    base_dates = (
        pd.to_datetime(baseline_source_period["Fecha"], errors="coerce").dropna()
        if "Fecha" in baseline_source_period.columns
        else pd.Series(dtype="datetime64[ns]")
    )
    overview.update(
        {
            "base_classic_nps": base_kpis.classic_nps,
            "cumulative_classic_nps": cumulative_kpis.classic_nps,
            "cumulative_classic_delta": (
                float(cumulative_kpis.classic_nps - base_kpis.classic_nps)
                if cumulative_kpis.classic_nps is not None and base_kpis.classic_nps is not None
                else float("nan")
            ),
            "base_start": base_dates.min().date().isoformat() if not base_dates.empty else "",
            "base_end": (
                (pd.Timestamp(period_start) - pd.Timedelta(days=1)).date().isoformat()
                if not base_dates.empty
                else ""
            ),
        }
    )
    detractor_raw = (
        channel_selected[
            channel_selected["band"].astype(str).str.casefold().eq("detractor")
        ].copy()
        if "band" in channel_selected.columns
        else channel_selected.copy()
    )
    text_topics = _text_topics_table(detractor_raw, top_k=EDITORIAL_LIMITS.max_text_chart_clusters)
    if not text_topics.empty:
        text_topics = text_topics.loc[
            ~text_topics["top_terms"].map(
                lambda values: any(
                    " ".join(str(value).casefold().split()) == "sin comentarios"
                    for value in list(values or [])
                )
            )
        ].reset_index(drop=True)
    dimensions = {
        "Palanca": _build_dimension_view_model(
            dimension="Palanca",
            selected_raw=selected_raw,
            current_source_period=current_source_period,
            baseline_source_period=baseline_source_period,
            topic_channel=topic_channel,
        ),
        "Subpalanca": _build_dimension_view_model(
            dimension="Subpalanca",
            selected_raw=selected_raw,
            current_source_period=current_source_period,
            baseline_source_period=baseline_source_period,
            topic_channel=topic_channel,
        ),
    }

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
        topic_channel=topic_channel,
        overview=overview,
        period_kpis=resolved_period_kpis,
        overview_figure=_build_overview_figure(
            comparison_nps_df
            if comparison_nps_df is not None and not comparison_nps_df.empty
            else selected_nps_df,
            period_start=period_start,
            period_end=period_end,
        ),
        text_topics_df=text_topics,
        current_label=f"{_safe_date(period_start)} -> {_safe_date(period_end)}",
        baseline_label=(
            f"{_safe_date(baseline_period['date'].min())} -> {_safe_date(baseline_period['date'].max())}"
            if not baseline_period.empty
            else "sin base histórica"
        ),
        dimensions=dimensions,
        causal=causal,
    )


def _set_template_text(
    shape: object,
    text: object,
    *,
    size: float | None = None,
    bold: bool | None = None,
    color: str | None = None,
    font: str | None = None,
    align: PP_ALIGN | None = None,
) -> None:
    tf = shape.text_frame
    tf.clear()
    tf.word_wrap = True
    paragraph = tf.paragraphs[0]
    if align is not None:
        paragraph.alignment = align
    run = paragraph.add_run()
    run.text = str(text or "")
    run.font.name = font or "Lato"
    if size is not None:
        run.font.size = Pt(size)
    if bold is not None:
        run.font.bold = bold
    if color:
        run.font.color.rgb = _rgb(color)


def _set_template_messages(shape: object, messages: list[str], *, size: float) -> None:
    tf = shape.text_frame
    tf.clear()
    tf.word_wrap = True
    for index, message in enumerate(messages):
        paragraph = tf.paragraphs[0] if index == 0 else tf.add_paragraph()
        paragraph.alignment = PP_ALIGN.LEFT
        paragraph.space_after = Pt(12)
        properties = paragraph._p.get_or_add_pPr()
        for child in list(properties):
            if child.tag.rsplit("}", 1)[-1] in {"buAutoNum", "buBlip", "buChar", "buNone"}:
                properties.remove(child)
        properties.insert(0, OxmlElement("a:buNone"))
        properties.set("marL", "0")
        properties.set("indent", "0")
        run = paragraph.add_run()
        run.text = message
        run.font.name = "Source Serif 4"
        run.font.size = Pt(size)
        run.font.color.rgb = _rgb(BBVA_COLORS["ink"])


def _set_template_table(table: object, rows: list[list[str]]) -> None:
    for row_index, table_row in enumerate(table.rows):
        table_row.height = Inches(0.42 if row_index == 0 else 0.40)
        values = rows[row_index] if row_index < len(rows) else []
        for column_index, cell in enumerate(table_row.cells):
            cell.text = str(values[column_index]) if column_index < len(values) else ""
            cell.text_frame.word_wrap = True
            cell.margin_left = Inches(0.08)
            cell.margin_right = Inches(0.06)
            cell.margin_top = Inches(0.04)
            cell.margin_bottom = Inches(0.03)
            for paragraph in cell.text_frame.paragraphs:
                for run in paragraph.runs:
                    run.font.name = "Lato"
                    run.font.size = Pt(8.4 if row_index == 0 else 8.2)
                    run.font.bold = row_index == 0
                    run.font.color.rgb = _rgb("FFFFFF" if row_index == 0 else BBVA_COLORS["ink"])


def _replace_template_table(
    slide: object,
    shape_index: int,
    rows: list[list[str]],
    *,
    column_widths: list[float],
) -> object:
    shape = slide.shapes[shape_index]
    left, top, height = shape.left, shape.top, shape.height
    width = sum((column.width for column in shape.table.columns), 0) if shape.has_table else shape.width
    shape._element.getparent().remove(shape._element)
    table = slide.shapes.add_table(len(rows), len(column_widths), left, top, width, height).table
    for column, width_in in zip(table.columns, column_widths):
        column.width = Inches(width_in)
    for row_index, row in enumerate(table.rows):
        for cell in row.cells:
            cell.fill.solid()
            cell.fill.fore_color.rgb = _rgb(
                BBVA_COLORS["blue"] if row_index == 0 else "FFFFFF"
            )
    _set_template_table(table, rows)
    return table


def _set_template_labeled_value(
    shape: object,
    label: str,
    value: str,
    *,
    size: float = 10.5,
    color: str = "666666",
) -> None:
    tf = shape.text_frame
    tf.clear()
    tf.word_wrap = True
    paragraph = tf.paragraphs[0]
    label_run = paragraph.add_run()
    label_run.text = f"{label}\n"
    label_run.font.name = "Lato"
    label_run.font.size = Pt(size)
    label_run.font.color.rgb = _rgb(color)
    value_run = paragraph.add_run()
    value_run.text = value
    value_run.font.name = "Lato"
    value_run.font.size = Pt(size)
    value_run.font.bold = True
    value_run.font.color.rgb = _rgb(color)


def _add_highlighted_runs(
    paragraph: object,
    text: str,
    segments: object,
    *,
    size: float,
    color: str,
) -> None:
    terms = {
        str(segment.get("text", "")).strip().casefold()
        for segment in (segments if isinstance(segments, list) else [])
        if isinstance(segment, dict) and segment.get("bold") and str(segment.get("text", "")).strip()
    }
    pattern = (
        re.compile("(" + "|".join(re.escape(term) for term in sorted(terms, key=len, reverse=True)) + ")", re.IGNORECASE)
        if terms
        else None
    )
    parts = pattern.split(text) if pattern else [text]
    for part in parts:
        if not part:
            continue
        run = paragraph.add_run()
        run.text = part
        run.font.name = "Lato"
        run.font.size = Pt(size)
        run.font.bold = part.casefold() in terms
        run.font.color.rgb = _rgb(color)


def _duplicate_slide(prs: Presentation, source_index: int) -> object:
    source = prs.slides[source_index]
    target = prs.slides.add_slide(source.slide_layout)
    for shape in list(target.shapes):
        shape._element.getparent().remove(shape._element)
    source_bg = getattr(source.element.cSld, "bg", None)
    target_bg = getattr(target.element.cSld, "bg", None)
    if target_bg is not None:
        target.element.cSld.remove(target_bg)
    if source_bg is not None:
        target.element.cSld.insert(0, deepcopy(source_bg))
    for shape in source.shapes:
        target.shapes._spTree.insert_element_before(deepcopy(shape.element), "p:extLst")
    return target


def _move_slide(prs: Presentation, source_index: int, target_index: int) -> None:
    slide_ids = prs.slides._sldIdLst
    slide_id = slide_ids[source_index]
    slide_ids.remove(slide_id)
    slide_ids.insert(target_index, slide_id)


def _replace_template_picture(
    slide: object,
    picture_index: int,
    figure: Optional[go.Figure],
    *,
    empty_note: str,
) -> None:
    picture = slide.shapes[picture_index]
    left, top, width, height = picture.left, picture.top, picture.width, picture.height
    picture._element.getparent().remove(picture._element)
    if figure is None:
        box = slide.shapes.add_textbox(left, top, width, height)
        _set_template_text(
            box,
            empty_note,
            size=13,
            color=BBVA_COLORS["muted"],
            align=PP_ALIGN.CENTER,
        )
        return
    png = _kaleido_png(
        figure,
        width=max(int(width / 914400 * 190), 1200),
        height=max(int(height / 914400 * 190), 700),
        panel_width_in=width / 914400,
        panel_height_in=height / 914400,
    )
    if png:
        slide.shapes.add_picture(BytesIO(png), left, top, width=width, height=height)


def _remove_slide(prs: Presentation, index: int) -> None:
    slide_id = prs.slides._sldIdLst[index]
    prs.part.drop_rel(slide_id.rId)
    prs.slides._sldIdLst.remove(slide_id)


def _template_period_rows(context: PresentationContext) -> list[list[str]]:
    period = _dict_payload(context.period_kpis.get("period"))
    current = _dict_payload(period.get("display"))
    base = _dict_payload(period.get("base_display"))
    deltas = _dict_payload(period.get("deltas"))
    base_label = str(period.get("base_label") or "Base histórica")
    actual_label = str(period.get("actual_label") or context.period_label)
    specs = (
        ("COMENTARIOS", "comments"),
        ("SCORE MEDIO", "nps_average"),
        ("PROMOTORES", "promoter_rate"),
        ("DETRACTORES", "detractor_rate"),
        ("NPS CLÁSICO MENSUAL", "classic_nps"),
    )
    rows = [["Indicador", _clip(base_label, 24), _clip(actual_label, 24), "Variación"]]
    for label, key in specs:
        delta = _dict_payload(deltas.get(key))
        rows.append(
            [
                label,
                str(base.get(key, "n/d")),
                str(current.get(key, "n/d")),
                str(delta.get("display") or format_delta(delta.get("value"), kpi_key=key)),
            ]
        )
    return rows


def _scenario_evidence(shape: object, scenario: CausalScenarioViewModel) -> None:
    records = scenario.helix_evidence_records
    fallback = scenario.helix_evidence_lines[: EDITORIAL_LIMITS.max_helix_evidence]
    tf = shape.text_frame
    tf.clear()
    tf.word_wrap = True
    entries = records or [CausalEvidenceRecord("", line, "") for line in fallback]
    if not entries:
        entries = [CausalEvidenceRecord("", "Sin evidencia Helix vinculada en el periodo.", "")]
    max_entries = EDITORIAL_LIMITS.max_helix_evidence
    has_overflow = len(entries) > max_entries
    detailed_entries = list(entries[: max_entries - 1] if has_overflow else entries[:max_entries])

    def prepare_paragraph(paragraph: object, *, size: float) -> None:
        paragraph_properties = paragraph._p.get_or_add_pPr()
        for child in list(paragraph_properties):
            if child.tag.rsplit("}", 1)[-1] in {"buAutoNum", "buBlip", "buChar", "buNone"}:
                paragraph_properties.remove(child)
        paragraph_properties.insert(0, OxmlElement("a:buNone"))
        paragraph_properties.set("marL", str(int(Pt(14))))
        paragraph_properties.set("indent", str(-int(Pt(10))))
        paragraph.level = 0
        paragraph.alignment = PP_ALIGN.LEFT
        paragraph.space_after = Pt(5)
        bullet_run = paragraph.add_run()
        bullet_run.text = "• "
        bullet_run.font.name = "Lato"
        bullet_run.font.size = Pt(size)
        bullet_run.font.color.rgb = _rgb(BBVA_COLORS["ink"])

    visible_count = len(detailed_entries) + (1 if has_overflow else 0)
    size = 11 if visible_count <= 2 else 9.5
    for index, record in enumerate(detailed_entries):
        paragraph = tf.paragraphs[0] if index == 0 else tf.add_paragraph()
        prepare_paragraph(paragraph, size=size)
        if record.incident_id:
            id_run = paragraph.add_run()
            id_run.text = record.incident_id
            id_run.font.name = "Lato"
            id_run.font.size = Pt(size)
            id_run.font.bold = True
            id_run.font.color.rgb = _rgb(BBVA_COLORS["ink"])
            if record.url:
                id_run.hyperlink.address = record.url
        if record.summary:
            separator = paragraph.add_run()
            separator.text = ": "
            separator.font.name = "Lato"
            separator.font.size = Pt(size)
            separator.font.color.rgb = _rgb(BBVA_COLORS["ink"])
            _add_highlighted_runs(
                paragraph,
                _clip(record.summary, 165),
                record.segments,
                size=size,
                color=BBVA_COLORS["ink"],
            )

    if has_overflow:
        remaining = [record for record in entries[len(detailed_entries) :] if record.incident_id]
        if remaining:
            paragraph = tf.add_paragraph()
            prepare_paragraph(paragraph, size=size)
            line_length = 0
            for record in remaining:
                incident_length = len(record.incident_id) + (2 if line_length else 0)
                if line_length and line_length + incident_length > 72:
                    paragraph = tf.add_paragraph()
                    prepare_paragraph(paragraph, size=size)
                    line_length = 0
                if line_length:
                    separator = paragraph.add_run()
                    separator.text = ", "
                    separator.font.name = "Lato"
                    separator.font.size = Pt(size)
                    separator.font.color.rgb = _rgb(BBVA_COLORS["ink"])
                id_run = paragraph.add_run()
                id_run.text = record.incident_id
                id_run.font.name = "Lato"
                id_run.font.size = Pt(size)
                id_run.font.bold = True
                id_run.font.color.rgb = _rgb(BBVA_COLORS["ink"])
                if record.url:
                    id_run.hyperlink.address = record.url
                line_length += incident_length


def _scenario_comment_groups(row: pd.Series, fallback: list[str]) -> list[tuple[str, str, list[dict[str, object]]]]:
    records = row.get("comment_records")
    source = records if isinstance(records, list) else []
    grouped: OrderedDict[str, list[dict[str, object]]] = OrderedDict()
    for record in source:
        if not isinstance(record, dict):
            continue
        score = str(record.get("nps", "n/d") or "n/d")
        grouped.setdefault(score, []).append(record)
    output: list[tuple[str, str, list[dict[str, object]]]] = []
    for score, score_records in grouped.items():
        comments = [str(record.get("comment", "")).strip() for record in score_records]
        comments = [comment for comment in comments if comment]
        segments = [
            segment
            for record in score_records
            for segment in (record.get("comment_segments") or [])
            if isinstance(segment, dict)
        ]
        if comments:
            output.append((f"Score {score}: ", "; ".join(comments), segments))
    if output:
        return output[:2]
    return [("", line.replace("NPS ", "Score "), []) for line in fallback[:2]]


def _set_scenario_comment(shape: object, label: str, text: str, segments: object) -> None:
    tf = shape.text_frame
    tf.clear()
    tf.word_wrap = True
    paragraph = tf.paragraphs[0]
    paragraph.alignment = PP_ALIGN.CENTER
    if label:
        run = paragraph.add_run()
        run.text = label
        run.font.name = "Lato"
        run.font.size = Pt(11)
        run.font.color.rgb = _rgb(BBVA_COLORS["ink"])
    _add_highlighted_runs(
        paragraph,
        _clip(text, 205),
        segments,
        size=11,
        color=BBVA_COLORS["ink"],
    )


def _fill_template_deck(
    prs: Presentation,
    *,
    context: PresentationContext,
    dimension_mode: str,
    include_causal_section: bool,
) -> None:
    if len(prs.slides) != 9:
        raise ValueError("La plantilla ejecutiva BBVA debe contener exactamente 9 diapositivas.")

    dimension = "Subpalanca" if dimension_mode == "subpalanca" else "Palanca"
    view = context.dimensions[dimension]
    topic_channel = context.topic_channel or "Todos"
    month = _month_label_es(context.period_end).title()
    base_month = _month_label_es(context.period_start - pd.Timedelta(days=1)).title()
    method = get_causal_method_spec(context.causal.touchpoint_source)

    cover = prs.slides[0]
    _set_template_text(
        cover.shapes[0],
        "NPS : Comentarios\ne incidencias",
        size=38,
        bold=True,
        color="FFFFFF",
        font="Source Serif 4",
    )
    scope = " · ".join(
        value
        for value in (context.service_origin, context.service_origin_n1, context.service_origin_n2)
        if value
    )
    _set_template_text(
        cover.shapes[1],
        f"{scope} · {context.period_label} · Método de agrupación: "
        f"{method.label if include_causal_section else 'No aplicado (sin evidencia Helix)'}",
        size=12,
        color="FFFFFF",
    )

    comparison = prs.slides[1]
    delta = _safe_float(context.overview.get("classic_delta"), default=float("nan"))
    _set_template_text(
        comparison.shapes[4],
        f"Evolución del NPS clásico mensual ({_safe_date(context.period_start)} a {_safe_date(context.period_end)})",
        size=25,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
    )
    _set_template_text(
        comparison.shapes[2],
        f"Comparativa de indicadores\n{base_month} frente a {month}",
        size=12,
        bold=True,
        color="FFFFFF",
        font="Source Serif 4",
    )
    _set_template_table(comparison.shapes[6].table, _template_period_rows(context))
    _set_template_text(
        comparison.shapes[8],
        f"Entre los tópicos observados en {topic_channel}, la mayor fricción por volumen detractor se concentra en {context.overview.get('pain_point') or 'sin señal suficiente'}.",
        size=13,
        color=BBVA_COLORS["ink"],
    )
    _set_template_text(
        comparison.shapes[12],
        f"Entre los tópicos observados en {topic_channel}, la mejor señal por volumen promotor se concentra en {context.overview.get('strength_point') or 'sin señal suficiente'}.",
        size=13,
        color=BBVA_COLORS["ink"],
    )
    comparison.shapes[8].top = Inches(1.58)
    comparison.shapes[8].height = Inches(1.16)
    comparison.shapes[12].top = Inches(3.48)
    comparison.shapes[12].height = Inches(1.24)
    _set_template_text(
        comparison.shapes[3],
        f"La experiencia de cliente {'mejora' if delta > 0 else 'empeora' if delta < 0 else 'se mantiene'}: "
        f"el NPS clásico mensual varía {_fmt_signed_or_nd(delta)} pts y el peso detractor "
        f"{_fmt_signed_or_nd(context.overview.get('detractor_delta_pp'))} pp.",
        size=11.5,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
        align=PP_ALIGN.CENTER,
    )

    history = prs.slides[2]
    _set_template_text(
        history.shapes[5],
        f"Evolución del NPS clásico acumulado histórico ({context.overview.get('base_start') or _safe_date(context.period_start)} a {_safe_date(context.period_end)})",
        size=22,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
    )
    _set_template_text(
        history.shapes[2],
        f"NPS clásico diario y distribución por grupo\nPeriodo {context.period_label} resaltado",
        size=12,
        bold=True,
        color="FFFFFF",
        font="Source Serif 4",
    )
    _set_template_messages(
        history.shapes[3],
        [
            f"El NPS clásico histórico terminó en {base_month} en {_fmt_num_or_nd(context.overview.get('base_classic_nps'))}.",
            f"A {_long_date_es(context.period_end)} alcanza {_fmt_num_or_nd(context.overview.get('cumulative_classic_nps'))}.",
            f"El peso detractor pasa de {_fmt_pct_or_nd(context.overview.get('start_detr'))} a {_fmt_pct_or_nd(context.overview.get('end_detr'))}.",
        ],
        size=11.5,
    )
    _set_template_text(
        history.shapes[4],
        f"El NPS clásico acumulado varía {_fmt_signed_or_nd(context.overview.get('cumulative_classic_delta'))} puntos frente a la base de {base_month}.",
        size=11.5,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
        align=PP_ALIGN.CENTER,
    )
    _replace_template_picture(
        history,
        6,
        context.overview_figure,
        empty_note="Sin histórico suficiente para construir la curva.",
    )
    history.shapes[-1].left = Inches(3.35)
    history.shapes[-1].top = Inches(1.82)
    history.shapes[-1].width = Inches(6.05)
    history.shapes[-1].height = Inches(2.92)

    topics = prs.slides[3]
    _set_template_text(
        topics.shapes[4],
        f"Principales comentarios de los detractores del mes ({_safe_date(context.period_start)} a {_safe_date(context.period_end)}) en el canal {topic_channel}",
        size=22,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
    )
    topic_rows = [["Comentarios", "Top terms", "Ejemplos"]]
    selected_topics = select_text_clusters(context.text_topics_df, max_clusters=5)
    for row in selected_topics.itertuples():
        examples = [str(value) for value in list(getattr(row, "examples", []))[:3]]
        topic_rows.append(
            [
                _fmt_count_or_nd(getattr(row, "n", 0)),
                _clip(getattr(row, "top_terms_txt", ""), 62),
                _clip(" · ".join(examples) if examples else "Sin comentarios", 210),
            ]
        )
    _replace_template_table(
        topics,
        6,
        topic_rows,
        column_widths=[1.0, 2.45, 5.11],
    )
    leader = topic_rows[1][1] if len(topic_rows) > 1 else "sin señal textual suficiente"
    _set_template_text(
        topics.shapes[3],
        f"La escucha detractora del canal {topic_channel} concentra su señal principal en {leader}.",
        size=11,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
        align=PP_ALIGN.CENTER,
    )

    change = prs.slides[4]
    change_rows = [["Valor", "Delta NPS Clásico", "NPS actual", "NPS base", "n actual", "n base"]]
    for row in view.change_table_df.head(4).itertuples():
        change_rows.append(
            [
                _clip(row.value, 23),
                _fmt_signed_or_nd(row.delta_nps),
                _fmt_num_or_nd(row.nps_current),
                _fmt_num_or_nd(row.nps_baseline),
                _fmt_count_or_nd(row.n_current),
                _fmt_count_or_nd(row.n_baseline),
            ]
        )
    worst = change_rows[1] if len(change_rows) > 1 else ["Sin deterioro", "n/d"]
    _set_template_text(
        change.shapes[0],
        f"{worst[0]} lidera el deterioro entre los tópicos observados en {topic_channel} en {month}, frente a la base de {base_month}",
        size=21,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
    )
    change.shapes[4].height = Inches(0.62)
    _set_template_text(
        change.shapes[4],
        f"Qué ha cambiado en {dimension}\nActual {context.current_label} · base {context.baseline_label}",
        size=11,
        bold=True,
        color="FFFFFF",
        font="Source Serif 4",
    )
    _set_template_text(
        change.shapes[6],
        f"{worst[0]} presenta el mayor Delta NPS Clásico ({worst[1]} puntos).",
        size=11,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
        align=PP_ALIGN.CENTER,
    )
    _replace_template_picture(
        change,
        8,
        view.change_figure,
        empty_note=f"Sin base suficiente para comparar {dimension.lower()}.",
    )
    _replace_template_table(
        change,
        7,
        change_rows,
        column_widths=[1.00, 0.64, 0.63, 0.70, 0.70, 0.70],
    )

    pain = prs.slides[5]
    pain_rows = list(view.topic_table_df.head(4).itertuples())
    leader_name = str(getattr(pain_rows[0], "value", "Sin señal")) if pain_rows else "Sin señal"
    _set_template_text(pain.shapes[1], "Volumen de opiniones", size=11, color=BBVA_COLORS["ink"])
    _set_template_text(
        pain.shapes[0],
        f"{leader_name} concentra el mayor dolor entre los tópicos observados en {topic_channel}",
        size=25,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
    )
    for column in range(4):
        row = pain_rows[column] if column < len(pain_rows) else None
        offset = 2 + column * 6 if column else 2
        header_index = (2, 8, 9, 10)[column]
        volume_index = (14, 17, 20, 23)[column]
        score_index = (13, 16, 19, 22)[column]
        detractor_index = (6, 15, 18, 21)[column]
        del offset
        _set_template_text(
            pain.shapes[header_index],
            _clip(getattr(row, "value", "Sin comentarios") if row else "Sin comentarios", 28),
            size=11,
            bold=True,
            color=BBVA_COLORS["ink"],
            align=PP_ALIGN.CENTER,
        )
        _set_template_labeled_value(
            pain.shapes[volume_index],
            "Opiniones totales:",
            _fmt_count_or_nd(getattr(row, "n", 0)) if row else "Sin comentarios",
        )
        _set_template_labeled_value(
            pain.shapes[score_index],
            "Score total:",
            _fmt_num_or_nd(getattr(row, "nps", np.nan), decimals=1) if row else "n/d",
        )
        _set_template_labeled_value(
            pain.shapes[detractor_index],
            "Detractores del periodo:",
            _fmt_pct_or_nd(getattr(row, "detractor_rate", np.nan)) if row else "n/d",
        )
    _set_template_text(
        pain.shapes[7],
        f"{leader_name} presenta la señal más crítica entre los tópicos observados en {topic_channel}.",
        size=11,
        bold=True,
        color=BBVA_COLORS["ink"],
        font="Source Serif 4",
        align=PP_ALIGN.CENTER,
    )

    scenarios = context.causal.scenarios if include_causal_section else []
    while len(prs.slides) < 6 + len(scenarios):
        # The first causal slide is the canonical scenario layout. Later
        # template slides may use alternate masters, so extending from the
        # first one keeps every generated scenario visually consistent.
        _duplicate_slide(prs, 6)
    keep_slides = 6 + len(scenarios)
    while len(prs.slides) > keep_slides:
        _remove_slide(prs, len(prs.slides) - 1)
    for offset, scenario in enumerate(scenarios):
        slide = prs.slides[6 + offset]
        row = scenario.row
        title = str(row.get("nps_topic") or f"Escenario de evidencia {offset + 1}").replace(
            " > ", " / "
        )
        title_shape = slide.shapes[1]
        full_title = title
        title_shape.width = prs.slide_width - title_shape.left - Inches(0.35)
        title_shape.height = Inches(0.78)
        title_size = 22 if len(full_title) < 49 else 18 if len(full_title) < 65 else 16
        _set_template_text(
            title_shape,
            full_title,
            size=title_size,
            bold=True,
            color=BBVA_COLORS["ink"],
            font="Source Serif 4",
        )
        _set_template_text(
            slide.shapes[0], str(7 + offset), size=8, color=BBVA_COLORS["ink"], align=PP_ALIGN.RIGHT
        )
        _set_template_text(
            slide.shapes[4],
            "NOTA MEDIA DEL TÓPICO",
            size=12,
            bold=False,
            color=BBVA_COLORS["blue"],
            align=PP_ALIGN.CENTER,
        )
        _set_template_text(
            slide.shapes[3],
            _fmt_num_or_nd(row.get("avg_nps")),
            size=30,
            bold=True,
            color=BBVA_COLORS["ink"],
            font="Source Serif 4",
            align=PP_ALIGN.CENTER,
        )
        _set_template_text(
            slide.shapes[7],
            "CONFIANZA",
            size=12,
            bold=False,
            color=BBVA_COLORS["blue"],
            align=PP_ALIGN.CENTER,
        )
        _set_template_text(
            slide.shapes[6],
            _fmt_pct_or_nd(row.get("avg_similarity"), decimals=0),
            size=30,
            bold=True,
            color=BBVA_COLORS["ink"],
            font="Source Serif 4",
            align=PP_ALIGN.CENTER,
        )
        slide.shapes[9].left = Inches(0.38)
        slide.shapes[9].top = Inches(4.90)
        slide.shapes[9].width = Inches(9.37)
        slide.shapes[9].height = Inches(0.48)
        slide.shapes[9].fill.solid()
        slide.shapes[9].fill.fore_color.rgb = _rgb(BBVA_COLORS["sky"])
        _set_template_text(
            slide.shapes[9],
            f"VÍNCULOS SEMÁNTICOS: {_safe_int(row.get('linked_pairs', 0))} · lectura {method.label}.",
            size=11,
            bold=True,
            color=BBVA_COLORS["ink"],
            font="Source Serif 4",
            align=PP_ALIGN.CENTER,
        )
        content_shapes = [
            shape for shape in list(slide.shapes)[10:] if getattr(shape, "has_text_frame", False)
        ]
        comments = _scenario_comment_groups(
            row,
            scenario.comment_lines or ["Sin comentario VoC vinculado en el periodo."],
        )
        evidence_shape = content_shapes[-1]
        quote_shapes = content_shapes[:-1]
        if len(quote_shapes) == 1 and len(comments) >= 2:
            first_quote = quote_shapes[0]
            first_quote.left = Inches(3.55)
            second_quote = slide.shapes.add_shape(
                MSO_AUTO_SHAPE_TYPE.RECTANGLE,
                Inches(3.55),
                Inches(1.63),
                Inches(5.83),
                Inches(0.67),
            )
            second_quote.fill.solid()
            second_quote.fill.fore_color.rgb = _rgb(BBVA_COLORS["sky"])
            second_quote.line.fill.background()
            second_quote.text_frame.margin_left = Inches(0.10)
            second_quote.text_frame.margin_right = Inches(0.10)
            second_quote.text_frame.margin_top = Inches(0.07)
            second_quote.text_frame.margin_bottom = Inches(0.05)
            quote_shapes.append(second_quote)
        content_top = 1.08
        for index, quote_shape in enumerate(quote_shapes):
            _, comment_text, _ = comments[index] if index < len(comments) else ("", "", [])
            estimated_lines = max(1, min(3, int(np.ceil(max(len(comment_text), 1) / 82))))
            quote_shape.left = Inches(3.55)
            quote_shape.top = Inches(content_top)
            quote_shape.width = Inches(5.83)
            quote_shape.height = Inches(0.34 + 0.19 * estimated_lines)
            quote_shape.fill.solid()
            quote_shape.fill.fore_color.rgb = _rgb(BBVA_COLORS["sky"])
            quote_shape.line.fill.background()
            content_top += 0.34 + 0.19 * estimated_lines + 0.08
        evidence_shape.left = Inches(3.55)
        evidence_shape.top = Inches(content_top + 0.03)
        evidence_shape.width = Inches(5.83)
        evidence_shape.height = Inches(max(1.15, 4.66 - content_top))
        for index, quote_shape in enumerate(quote_shapes):
            label, text, segments = comments[index] if index < len(comments) else ("", "", [])
            _set_scenario_comment(quote_shape, label, text, segments)
        _scenario_evidence(evidence_shape, scenario)

    _move_slide(prs, 2, 1)


def generate_business_review_ppt(
    *,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    period_start: date,
    period_end: date,
    focus_name: str,
    topic_channel: str = "Web",
    attribution_df: Optional[pd.DataFrame] = None,
    selected_nps_df: Optional[pd.DataFrame] = None,
    comparison_nps_df: Optional[pd.DataFrame] = None,
    touchpoint_source: str = "",
    entity_summary_df: Optional[pd.DataFrame] = None,
    entity_summary_kpis: Optional[list[dict[str, str]]] = None,
    broken_journeys_df: Optional[pd.DataFrame] = None,
    report_dimension_analysis: str = "palanca",
    period_kpis: Optional[dict[str, object]] = None,
    include_causal_section: bool = True,
) -> BusinessPptResult:
    """Build the single BBVA thermal-causality deck for the selected period."""
    dimension_mode = str(report_dimension_analysis or "palanca").strip().lower()
    if dimension_mode not in {"palanca", "subpalanca"}:
        dimension_mode = "palanca"

    if not REPORT_TEMPLATE.exists():
        raise FileNotFoundError(f"No se encuentra la plantilla ejecutiva: {REPORT_TEMPLATE}")
    prs = Presentation(str(REPORT_TEMPLATE))
    prs.core_properties.subject = "NPS Lens · comentarios e incidencias"
    prs.core_properties.keywords = f"BBVA,NPS,incidencias,{REPORT_DESIGN_VERSION}"
    prs.core_properties.comments = f"NPS Lens report design: {REPORT_DESIGN_VERSION}"

    context = _build_presentation_context(
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        service_origin_n2=service_origin_n2,
        period_start=period_start,
        period_end=period_end,
        focus_name=focus_name,
        topic_channel=topic_channel,
        attribution_df=attribution_df,
        selected_nps_df=selected_nps_df,
        comparison_nps_df=comparison_nps_df,
        touchpoint_source=touchpoint_source,
        entity_summary_df=entity_summary_df,
        entity_summary_kpis=entity_summary_kpis,
        broken_journeys_df=broken_journeys_df,
        period_kpis=period_kpis,
    )

    _fill_template_deck(
        prs,
        context=context,
        dimension_mode=dimension_mode,
        include_causal_section=include_causal_section,
    )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    file_name = (
        f"nps-comentarios-incidencias-{_slug(service_origin)}-"
        f"{_slug(service_origin_n1)}-{stamp}.pptx"
    )

    buff = BytesIO()
    prs.save(buff)
    content = buff.getvalue()
    compact_prs = Presentation(BytesIO(content))
    for slide_index in (2, 1):
        if len(compact_prs.slides) > slide_index:
            _remove_slide(compact_prs, slide_index)
    compact_buff = BytesIO()
    compact_prs.save(compact_buff)
    compact_file_name = file_name.replace(".pptx", "-sin-evolucion-nps.pptx")
    return BusinessPptResult(
        file_name=file_name,
        content=content,
        slide_count=len(prs.slides),
        compact_file_name=compact_file_name,
        compact_content=compact_buff.getvalue(),
    )
