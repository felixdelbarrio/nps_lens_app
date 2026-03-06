from __future__ import annotations

import ast
import contextlib
import os
import re
import tempfile
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
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

from nps_lens.analytics.hotspot_metrics import (
    HOTSPOT_EVIDENCE_COLUMNS,
    summarize_hotspot_counts,
)
from nps_lens.analytics.hotspot_metrics import (
    build_hotspot_daily_breakdown as build_hotspot_daily_breakdown_metrics,
)

BBVA_COLORS = {
    "bg_dark": "061B4E",
    "bg_light": "F4F7FB",
    "line": "D6DFEA",
    "ink": "0A1F44",
    "muted": "42526E",
    "white": "FFFFFF",
    "blue": "004481",
    "sky": "2DCCCD",
    "green": "16A34A",
    "amber": "D97706",
    "yellow": "FACC15",
    "orange": "FB923C",
    "red": "DC2626",
}

BBVA_FONT_HEAD = "BentonSansBBVA Bold"
BBVA_FONT_BODY = "BentonSansBBVA Book"
BBVA_FONT_MEDIUM = "BentonSansBBVA Medium"


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


def _clip(txt: object, max_len: int) -> str:
    s = " ".join(str(txt or "").split())
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


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
        return str(pd.to_datetime(value).date())
    except Exception:
        return str(value or "")


def _safe_dt(value: object) -> Optional[pd.Timestamp]:
    try:
        ts = pd.to_datetime(value, errors="coerce")
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
        f"#!/bin/sh\ncd \"{exec_dir}\" || exit 1\nexec \"./bin/kaleido\" \"$@\"\n",
        encoding="utf-8",
    )
    with contextlib.suppress(Exception):
        shim_path.chmod(0o755)

    cls.executable_path = classmethod(lambda scope_cls: str(shim_path))  # type: ignore[assignment]
    cls._nps_lens_kaleido_patched = True


def _kaleido_png(fig: go.Figure, *, width: int = 1600, height: int = 900) -> Optional[bytes]:
    try:
        _patch_kaleido_executable_for_space_paths()
        return pio.to_image(fig, format="png", width=width, height=height, scale=2)
    except Exception:
        return None


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
    tf.clear()
    p = tf.paragraphs[0]
    r = p.add_run()
    r.text = title
    r.font.name = BBVA_FONT_HEAD
    r.font.size = Pt(28)
    r.font.bold = True
    r.font.color.rgb = _rgb(title_color)

    sb = slide.shapes.add_textbox(Inches(0.65), Inches(0.95), Inches(11.5), Inches(0.42))
    stf = sb.text_frame
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


def _add_chart_slide(
    prs: Presentation,
    *,
    title: str,
    subtitle: str,
    figure: Optional[go.Figure],
    rationale_title: str,
    rationale_lines: Iterable[str],
) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
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
) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _add_bg(slide, BBVA_COLORS["bg_light"])
    _add_header(
        slide,
        title="Resumen del periodo",
        subtitle="Qué está pasando, dónde mirar primero y por qué (lenguaje de negocio).",
    )

    month_txt = _month_label_es(period_end)
    p_label = f"{_safe_date(period_start)} -> {_safe_date(period_end)}"
    cards = [
        ("SERVICE ORIGEN", _clip(service_origin or "N/D", 38), "Ámbito analizado"),
        ("NIVEL N1", _clip(service_origin_n1 or "N/D", 38), "Segmentación principal"),
        ("NIVEL N2", _clip(service_origin_n2 or "N/D", 38), "Segmentación secundaria"),
        ("MES EN CURSO", _clip(month_txt.title(), 38), f"Ventana: {p_label}"),
    ]

    left0 = 0.65
    top = 1.55
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

    d[time_col] = pd.to_datetime(d[time_col], errors="coerce")
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
        d["responses"] = pd.to_numeric(d[responses_col], errors="coerce").fillna(0.0).clip(lower=0.0)
    else:
        d["responses"] = 1.0

    if period_start is not None:
        d = d[d["date"] >= pd.Timestamp(period_start)]
    if period_end is not None:
        d = d[d["date"] <= pd.Timestamp(period_end)]
    if d.empty:
        return pd.DataFrame(columns=["date", "nps_mean", "detractor_rate", "incidents"]), nps_estimated

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
        return pd.DataFrame(columns=["date", "nps_mean", "detractor_rate", "incidents"]), nps_estimated

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


def _history_fig(daily: pd.DataFrame, *, focus_name: str) -> Optional[go.Figure]:
    if daily is None or daily.empty:
        return None
    d = daily.sort_values("date").copy()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=d["date"],
            y=d["nps_mean"],
            name="NPS medio",
            mode="lines",
            line=dict(color="#" + BBVA_COLORS["green"], width=3.2),
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
) -> Optional[go.Figure]:
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
) -> Optional[go.Figure]:
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
    d["chart_nps_comments"] = pd.to_numeric(d.get("chart_nps_comments"), errors="coerce").fillna(0.0)
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

    t["date"] = pd.to_datetime(t["date"], errors="coerce")
    t = t.dropna(subset=["date"])
    t = t[(t["date"] >= month_start) & (t["date"] <= month_end)].copy()
    if t.empty:
        return pd.DataFrame(columns=cols)

    t["helix_records"] = pd.to_numeric(t["helix_records"], errors="coerce").fillna(0.0).clip(lower=0.0)
    t["nps_comments"] = pd.to_numeric(t["nps_comments"], errors="coerce").fillna(0.0).clip(lower=0.0)

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
) -> Optional[go.Figure]:
    if month_daily is None or month_daily.empty:
        return None

    d = month_daily.sort_values("date").copy()
    m = matched_daily.copy() if matched_daily is not None else pd.DataFrame(columns=["date", "matched_incidents"])
    m["date"] = pd.to_datetime(m.get("date"), errors="coerce")
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
            mode="lines",
            line=dict(color="#" + BBVA_COLORS["green"], width=3.2),
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


def _prepare_incident_evidence(incident_evidence_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    cols = list(HOTSPOT_EVIDENCE_COLUMNS)
    if incident_evidence_df is None or incident_evidence_df.empty:
        return pd.DataFrame(columns=cols)

    d = incident_evidence_df.copy()
    id_col = _pick_first_col(d, ["incident_id", "incident number", "incident_number", "id de la incidencia"])
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
            "incident_date": pd.to_datetime(d[date_col], errors="coerce") if date_col else pd.NaT,
            "nps_topic": d[topic_col].astype(str) if topic_col else "",
            "incident_summary": incident_summary,
            "detractor_comment": d[comment_col].astype(str) if comment_col else "",
            "similarity": pd.to_numeric(d[sim_col], errors="coerce").fillna(0.0) if sim_col else 0.0,
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
    out = out.sort_values(["hot_rank", "similarity"], ascending=[True, False], na_position="last").reset_index(drop=True)
    return out[cols]


def _top_topics_for_zoom(
    rationale_df: pd.DataFrame,
    ranking_df: Optional[pd.DataFrame],
    *,
    max_topics: int = 3,
) -> list[str]:
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
) -> list[ZoomIncident]:
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
            hotspots.sort(key=lambda x: (x[0], -float(pd.to_numeric(x[2]["similarity"], errors="coerce").fillna(0.0).mean())))
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
) -> Optional[pd.Series]:
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
            order = hotspot_summary.assign(_rank=ranked).sort_values(
                ["_rank"], na_position="last"
            )
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


def _changepoints_map(changepoints_by_topic: Optional[pd.DataFrame]) -> dict[str, list[pd.Timestamp]]:
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
) -> pd.DataFrame:
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

    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.dropna(subset=["date"])
    if d.empty:
        return pd.DataFrame(columns=cols)

    d["helix_records"] = pd.to_numeric(d["helix_records"], errors="coerce").fillna(0.0).clip(lower=0.0)
    d["nps_comments"] = pd.to_numeric(d["nps_comments"], errors="coerce").fillna(0.0).clip(lower=0.0)
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
    d["nps_comments_moderate"] = (
        pd.to_numeric(sev_mod, errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    d["nps_comments_high"] = (
        pd.to_numeric(sev_high, errors="coerce").fillna(0.0).clip(lower=0.0)
    )
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
) -> Optional[go.Figure]:
    del incident, lag_days, focus_name

    rel = related_timeline.copy() if related_timeline is not None else pd.DataFrame()
    if not rel.empty:
        rel["date"] = pd.to_datetime(rel["date"], errors="coerce")
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
            pd.to_numeric(rel.get("nps_comments_high"), errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0)
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
    z["nps_comments_moderate"] = (
        pd.to_numeric(z.get("nps_comments_moderate"), errors="coerce").fillna(0.0)
    )
    z["nps_comments_high"] = (
        pd.to_numeric(z.get("nps_comments_high"), errors="coerce").fillna(0.0)
    )
    z["nps_comments_critical"] = (
        pd.to_numeric(z.get("nps_comments_critical"), errors="coerce").fillna(0.0)
    )
    z["incident_ids"] = z.get("incident_ids", "").astype(str).fillna("")
    nps_series = pd.DataFrame(columns=["date", "nps_mean"])
    if (
        topic_daily is not None
        and not topic_daily.empty
        and {"date", "nps_mean"}.issubset(set(topic_daily.columns))
    ):
        nps_series = topic_daily[["date", "nps_mean"]].copy()
        nps_series["date"] = pd.to_datetime(nps_series["date"], errors="coerce")
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
                mode="lines",
                line=dict(color="#" + BBVA_COLORS["green"], width=2.6),
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
        "lag_weeks": _safe_float(r.get("best_lag_weeks", np.nan), default=np.nan),
    }


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
    ranking_df: Optional[pd.DataFrame] = None,
    by_topic_daily: Optional[pd.DataFrame] = None,
    lag_days_by_topic: Optional[pd.DataFrame] = None,
    by_topic_weekly: Optional[pd.DataFrame] = None,
    lag_weeks_by_topic: Optional[pd.DataFrame] = None,
    template_name: str = "Plantilla corporativa fija v1",
    corporate_fixed: bool = True,
    logo_path: Optional[Path] = None,
    incident_evidence_df: Optional[pd.DataFrame] = None,
    changepoints_by_topic: Optional[pd.DataFrame] = None,
    incident_timeline_df: Optional[pd.DataFrame] = None,
    hotspot_focus_note: str = "",
) -> BusinessPptResult:
    """Build a business deck focused on daily NPS, matched incidents and top-3 zooms."""
    del by_topic_daily, by_topic_weekly, story_md, script_8slides_md, template_name, corporate_fixed, logo_path

    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)

    period_label = f"{_safe_date(period_start)} -> {_safe_date(period_end)}"

    _add_period_summary_slide(
        prs,
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        service_origin_n2=service_origin_n2,
        period_start=period_start,
        period_end=period_end,
    )

    daily_signals, nps_estimated = _prepare_daily_signals(
        overall_weekly,
        period_start=period_start,
        period_end=period_end,
    )

    tfig = _history_fig(daily_signals, focus_name=focus_name)
    _add_chart_slide(
        prs,
        title="Evolución histórica diaria de NPS e incidencias",
        subtitle=f"{service_origin} · {service_origin_n1} · periodo {period_label}",
        figure=tfig,
        rationale_title="Racional",
        rationale_lines=[
            "La línea verde refleja el NPS medio diario para ver tendencia real y no solo ruido puntual.",
            f"La línea roja sigue la evolución de opiniones de {focus_name} y permite detectar tensión en experiencia.",
            "Las columnas amarillas muestran incidencias registradas para relacionar operación y percepción cliente.",
            "Este gráfico es la base para explicar causalidad operativa en comité.",
            (
                "Nota: el NPS medio diario se estimó a partir del mix de foco por falta de serie NPS diaria directa."
                if nps_estimated
                else "El NPS medio diario se calcula sobre respuestas reales del periodo."
            ),
        ],
    )

    top_topics = _top_topics_for_zoom(rationale_df, ranking_df, max_topics=3)
    evidence = _prepare_incident_evidence(incident_evidence_df)
    hotspot_summary = summarize_hotspot_counts(
        evidence,
        incident_timeline_df,
        max_hotspots=3,
    )
    zoom_incidents = _select_zoom_incidents(top_topics, evidence, max_items=3)
    hotspot_terms = (
        hotspot_summary["hot_term"]
        .astype(str)
        .str.strip()
        .mask(lambda s: s == "")
        .dropna()
        .drop_duplicates()
        .head(3)
        .tolist()
        if not hotspot_summary.empty and "hot_term" in hotspot_summary.columns
        else []
    )

    top3_fig = _top_hotspots_fig(evidence, incident_timeline_df, top_k=3)
    _add_chart_slide(
        prs,
        title="Top 3 hotspots operativos",
        subtitle=f"Histórico total del periodo {period_label} · ranking coherente con conteo centralizado",
        figure=top3_fig,
        rationale_title="Racional",
        rationale_lines=[
            "Top 3 priorizado con la misma fuente de conteos centralizada usada en app, insights y zooms.",
            "Se usa la misma señaletica de la presentación: hotspot #1 rojo, #2 naranja, #3 amarillo.",
            "Cada barra muestra dentro del trazo el hotspot y su volumen para evitar dependencia de lectura del eje X.",
            "Cada barra representa un hotspot operativo Helix validado por señal detractora en NPS.",
            (
                f"Top detectado: {', '.join(hotspot_summary['hot_term'].astype(str).head(3).tolist())}."
                if hotspot_summary is not None and not hotspot_summary.empty
                else "No se detectó señal suficiente para construir el top de hotspots."
            ),
            (
                _clip(hotspot_focus_note, 170)
                if str(hotspot_focus_note or "").strip()
                else "El eje de foco se prioriza por mayor cobertura de ocurrencia sobre descripciones Helix."
            ),
        ],
    )

    hfig = _hotspot_stack_fig(daily_signals, evidence, incident_timeline_df)
    hotspot_txt = ", ".join([_clip(t, 18) for t in hotspot_terms]) if hotspot_terms else "n/d"
    _add_chart_slide(
        prs,
        title="Incidencias históricas diarias por hotspot",
        subtitle=(
            f"Histórico total del periodo {period_label} · focos operativos Helix priorizados: {hotspot_txt}"
        ),
        figure=hfig,
        rationale_title="Racional",
        rationale_lines=[
            "Cada columna representa las incidencias registradas por día sobre todo el histórico disponible.",
            "La columna está segmentada por foco: rojo (hotspot 1), naranja (hotspot 2), amarillo (hotspot 3) y azul (sin hotspot).",
            "La leyenda horizontal debajo del gráfico identifica claramente qué término de negocio corresponde a cada hotspot.",
            "Estos hotspots se derivan de términos operativos Helix validados con señal NPS detractora; no son un ranking puro de Palanca/Subpalanca.",
            (
                _clip(hotspot_focus_note, 170)
                if str(hotspot_focus_note or "").strip()
                else "El eje de foco se prioriza por mayor cobertura de ocurrencia sobre descripciones Helix."
            ),
            "Si una incidencia cae en varios hotspots, se asigna al foco de mayor prioridad (1>2>3) para evitar doble conteo diario.",
            "Esto permite ver en qué días domina cada foco caliente y dónde concentrar acciones de dirección.",
        ],
    )

    cp_map = _changepoints_map(changepoints_by_topic)

    for idx, incident in enumerate(zoom_incidents, start=1):
        lag_days = _lag_days_for_topic(
            incident.nps_topic,
            lag_days_by_topic=lag_days_by_topic,
            lag_weeks_by_topic=lag_weeks_by_topic,
            rationale_df=rationale_df,
            ranking_df=ranking_df,
        )
        related_timeline = _incident_related_timeline(
            incident_id=incident.incident_id,
            hot_term=incident.hot_term,
            incident_timeline_df=incident_timeline_df,
        )
        zoom_nps = (
            daily_signals[["date", "nps_mean"]].copy()
            if {"date", "nps_mean"}.issubset(set(daily_signals.columns))
            else pd.DataFrame(columns=["date", "nps_mean"])
        )
        cp_list = cp_map.get(incident.nps_topic, [])
        zfig = _zoom_incident_fig(
            topic_daily=zoom_nps,
            related_timeline=related_timeline,
            incident=incident,
            lag_days=lag_days,
            changepoints=cp_list,
            focus_name=focus_name,
            period_start=period_start,
            period_end=period_end,
        )

        summary_row = _hotspot_summary_row(incident, hotspot_summary)
        chart_helix_total = (
            int(summary_row.get("chart_helix_records", 0))
            if summary_row is not None
            else (
                int(
                    pd.to_numeric(related_timeline.get("helix_records"), errors="coerce")
                    .fillna(0.0)
                    .sum()
                )
                if not related_timeline.empty
                else 0
            )
        )
        chart_comments_total = (
            int(summary_row.get("chart_nps_comments", 0))
            if summary_row is not None
            else (
                int(
                    pd.to_numeric(related_timeline.get("nps_comments"), errors="coerce")
                    .fillna(0.0)
                    .sum()
                )
                if not related_timeline.empty
                else 0
            )
        )
        chart_days = (
            int(summary_row.get("days_with_evidence", 0))
            if summary_row is not None
            else int(len(related_timeline))
        )
        hotspot_links = (
            int(summary_row.get("hotspot_links", incident.hotspot_links))
            if summary_row is not None
            else int(incident.hotspot_links)
        )
        mention_incidents = (
            int(summary_row.get("mention_incidents", incident.mention_incidents))
            if summary_row is not None
            else int(incident.mention_incidents)
        )
        mention_comments = (
            int(summary_row.get("mention_comments", incident.mention_comments))
            if summary_row is not None
            else int(incident.mention_comments)
        )
        validated_incidents = (
            int(summary_row.get("hotspot_incidents", incident.hotspot_incidents))
            if summary_row is not None
            else int(incident.hotspot_incidents)
        )
        validated_comments = (
            int(summary_row.get("hotspot_comments", incident.hotspot_comments))
            if summary_row is not None
            else int(incident.hotspot_comments)
        )
        chart_scope_txt = (
            "Serie diaria validada del gráfico"
            if int(hotspot_links) > 0
            else "Serie diaria por mención cruzada (Helix+NPS)"
        )

        metrics = _topic_metrics(incident.nps_topic, rationale_df)
        lag_weeks_txt = (
            f"{metrics.get('lag_weeks', np.nan):.1f} sem"
            if np.isfinite(metrics.get("lag_weeks", np.nan))
            else "n/d"
        )
        incident_date_txt = (
            str(incident.incident_date.date()) if incident.incident_date is not None else "sin fecha"
        )
        hot_label = str(incident.hot_term or "").strip() or incident.incident_id

        _add_chart_slide(
            prs,
            title=f"Zoom de foco caliente {idx}: {_clip(hot_label, 56)}",
            subtitle=f"{_clip(incident.nps_topic, 88)} · fecha referencia {incident_date_txt}",
            figure=zfig,
            rationale_title="Ficha + lectura",
            rationale_lines=[
                (
                    f"Menciones del hotspot (amplio): "
                    f"{int(mention_incidents)} incidencias Helix y {int(mention_comments)} comentarios negativos."
                ),
                (
                    f"Vínculos validados (estricto): {int(validated_incidents)} incidencias, "
                    f"{int(validated_comments)} comentarios y {int(hotspot_links)} links semánticos."
                ),
                (
                    f"{chart_scope_txt}: {int(chart_helix_total)} incidencias y "
                    f"{int(chart_comments_total)} comentarios negativos."
                ),
                (
                    f"Incidencias ejemplo: {incident.sample_incidents}"
                    if str(incident.sample_incidents or "").strip()
                    else f"Incidencia representativa: {incident.incident_id}"
                ),
                (
                    f"Comentarios ejemplo: {_clip(incident.sample_comments, 120)}"
                    if str(incident.sample_comments or "").strip()
                    else f"Comentario ejemplo: {_clip(incident.detractor_comment, 120)}"
                ),
                (
                    f"Término Helix caliente: {incident.hot_term}"
                    if str(incident.hot_term or "").strip()
                    else "Término Helix caliente: n/d"
                ),
                f"Descripción: {_clip(incident.incident_summary, 120)}",
                (
                    f"Timeline del zoom: barras rojas apiladas por intensidad "
                    f"(moderado/alto/crítico), puntos azules (etiqueta INC cuando cabe) "
                    f"para días con incidencias del hotspot y línea verde de NPS medio diario "
                    f"({int(chart_days)} días con evidencia)."
                ),
                f"Lag estimado: {int(lag_days)} días ({lag_weeks_txt}) · change points detectados: {len(cp_list)}.",
                (
                    f"Impacto estimado en negocio: riesgo {metrics.get('risk', 0.0):.2f} pts NPS, "
                    f"recuperable {metrics.get('recoverable', 0.0):.2f} pts, prioridad {metrics.get('priority', 0.0):.2f}."
                ),
                (
                    f"Lenguaje de negocio: al corregir esta incidencia se reduce la fricción "
                    f"que explica picos de {focus_name} con efecto rezagado."
                ),
            ],
        )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    file_name = f"nps-incidencias-{_slug(service_origin)}-{_slug(service_origin_n1)}-{stamp}.pptx"

    buff = BytesIO()
    prs.save(buff)
    return BusinessPptResult(file_name=file_name, content=buff.getvalue(), slide_count=len(prs.slides))
