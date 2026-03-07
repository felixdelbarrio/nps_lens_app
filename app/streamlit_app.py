from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from dotenv import find_dotenv, load_dotenv

# Lazy import to avoid heavy imports + noisy DeprecationWarnings at app start
# (Plotly triggers a NumPy alias deprecation warning in some versions.)
from nps_lens.analytics.causal import best_effort_ate_logit
from nps_lens.analytics.incident_attribution import (
    TOUCHPOINT_SOURCE_DOMAIN,
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    TOUCHPOINT_SOURCE_HELIX_N2,
    build_incident_attribution_chains,
)
from nps_lens.analytics.hotspot_metrics import (
    align_hotspot_evidence_to_axis,
    build_hotspot_evidence,
    build_hotspot_timeline,
    select_best_business_axis_for_hotspots,
    summarize_hotspot_counts,
)
from nps_lens.analytics.incident_rationale import (
    build_incident_nps_rationale,
    summarize_incident_nps_rationale,
)
from nps_lens.analytics.linking_policy import (
    HOTSPOT_MIN_TERM_OCCURRENCES,
    LINK_MAX_DAYS_APART,
    LINK_MIN_SIMILARITY,
    LINK_TOP_K_PER_INCIDENT,
)
from nps_lens.analytics.nps_helix_link import (
    build_incident_display_text,
    can_use_daily_resample,
    causal_rank_by_topic,
    daily_aggregates,
    detect_detractor_changepoints_with_bootstrap,
    estimate_best_lag_by_topic,
    estimate_best_lag_days_by_topic,
    incidents_lead_changepoints_flag,
    link_incidents_to_nps_topics,
    weekly_aggregates,
)
from nps_lens.analytics.opportunities import rank_opportunities
from nps_lens.analytics.text_mining import extract_topics
from nps_lens.application.service import AppService
from nps_lens.config import Settings
from nps_lens.core.disk_cache import DiskCache
from nps_lens.core.knowledge_cache import load_entries as kc_load_entries
from nps_lens.core.knowledge_cache import score_adjustments as kc_score_adjustments
from nps_lens.core.perf import PerfTracker
from nps_lens.core.store import DatasetContext, DatasetStore, HelixIncidentStore
from nps_lens.design.tokens import (
    DesignTokens,
    cp_level_color,
    palette,
    plotly_risk_scale,
)
from nps_lens.ingest.helix_incidents import read_helix_incidents_excel
from nps_lens.ingest.nps_thermal import read_nps_thermal_excel
from nps_lens.llm.insight_response import validate_insight_response
from nps_lens.ui.business import default_windows, driver_delta_table, slice_by_window
from nps_lens.ui.charts import (
    chart_cohort_heatmap,
    chart_daily_kpis,
    chart_daily_mix_business,
    chart_daily_score_semaforo,
    chart_daily_volume,
    chart_driver_bar,
    chart_driver_delta,
    chart_incident_priority_matrix,
    chart_incident_risk_recovery,
    chart_nps_trend,
    chart_topic_bars,
)
from nps_lens.ui.components import card, executive_banner, impact_chain, kpi, pills, section
from nps_lens.ui.narratives import (
    build_executive_story,
    build_incident_ppt_story,
    build_ppt_8slide_script,
    build_wow_prompt,
    compare_periods,
    executive_summary,
    explain_opportunities,
    explain_topics,
)
from nps_lens.ui.plotly_theme import apply_plotly_theme
from nps_lens.ui.population import POP_ALL, month_format_es, population_date_window
from nps_lens.ui.theme import Theme, apply_theme, get_theme


# Lazy import to avoid heavy imports + noisy DeprecationWarnings at app start
# (Plotly triggers a NumPy alias deprecation warning in some versions.)
def _plotly():
    """Lazy import Plotly.

    Plotly versions that still reference `np.bool8` can emit a NumPy DeprecationWarning
    at import-time (NumPy >= 1.24). This is upstream noise, so we silence that specific
    warning *only* around the import.
    """
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r".*np\.bool8.*deprecated.*",
            category=DeprecationWarning,
        )
        import plotly.express as px  # type: ignore
        import plotly.graph_objects as go  # type: ignore
    return px, go


REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_RESULTS_DIR = REPO_ROOT / "data" / "cache" / "results"


def _resolve_logo_path() -> Optional[Path]:
    candidates = [REPO_ROOT / "assets" / "logo.png"]
    if getattr(sys, "frozen", False):
        candidates.insert(0, Path(sys._MEIPASS) / "assets" / "logo.png")
    for path in candidates:
        if path.exists():
            return path
    return None


_logo_path = _resolve_logo_path()
st.set_page_config(
    page_title="NPS Lens — Senda MX",
    page_icon=str(_logo_path) if _logo_path else "📈",
    layout="wide",
)

# Session-wide performance tracker (timings)
if "_perf" not in st.session_state:
    st.session_state["_perf"] = PerfTracker()

# Deterministic disk cache for compute artifacts
if "_disk_cache" not in st.session_state:
    st.session_state["_disk_cache"] = DiskCache(CACHE_RESULTS_DIR)

# Application service (use-cases). Centralizes compute + caching + timings.
if "_app_service" not in st.session_state:
    st.session_state["_app_service"] = AppService(
        disk_cache=st.session_state["_disk_cache"],  # type: ignore
        perf=st.session_state["_perf"],  # type: ignore
    )

LLM_SYSTEM_PROMPT = """Eres el analista oficial de Insights para BBVA Banca de Empresas. Tu trabajo es:

1. Leer un "LLM Deep-Dive Pack" (Markdown o JSON) generado por la plataforma de Voz del Cliente.
2. Detectar insights no obvios y causas raíz plausibles basadas únicamente en la evidencia provista (cuantitativa y/o cualitativa), sin inventar datos ni afirmar hechos no sustentados.
3. Devolver SOLO un JSON válido (sin texto adicional) con el esquema requerido.

REGLA CRÍTICA — SOLO JSON:
- Tu respuesta debe ser exclusivamente un objeto JSON (sin explicaciones, sin títulos, sin Markdown, sin bloques de código).
- Usa comillas dobles estándar " (no comillas tipográficas).
- Sin trailing commas.
- No incluyas comillas dobles dentro de strings. Si necesitas comillas en un texto, escápalas como ".
- No envuelvas el JSON en Markdown (sin ```json).
- No uses NaN/Infinity/None; usa null si aplica.
- No agregues campos fuera del esquema.
- Si algún campo no puede completarse con evidencia del pack, usa null o listas vacías según corresponda y explica la carencia en "assumptions" y/o "risks" (sin inventar).

PRIVACIDAD Y SEGURIDAD (OBLIGATORIO):
- No incluyas datos personales o sensibles (PII) en ningún campo.
- Si el pack contiene PII, no la reproduzcas: usa "[REDACTED]" o reformula.
- No incluyas credenciales, tokens, claves API, secretos ni información confidencial interna.
- No intentes reidentificar personas/empresas ni inferir atributos sensibles.

CRITERIOS PARA "INSIGHTS NO OBVIOS" (OBLIGATORIO):
- Convergencia quant + qual
- Ruptura por segmento/ruta
- Cambio significativo vs baseline (si el pack lo incluye)
- Efecto en cadena
- Contradicción aparente
- Asimetría (pocos casos con alto impacto)

REGLAS PARA CAUSAS RAÍZ (OBLIGATORIO):
- root_causes[].cause debe ser concreta y accionable (no genérica).
- root_causes[].why debe explicar el mecanismo y conectar evidencia -> hipótesis.
- Separa evidencia (evidence) de suposiciones (assumptions).
- No afirmes causalidad si solo hay correlación.
- Incluye 1-3 causas raíz (máximo 3).

PUNTUACIONES Y FORMATOS (OBLIGATORIO):
- confidence: 0.0-1.0 (0.0-0.3 insuficiente, 0.4-0.6 parcial, 0.7-0.85 sólida, 0.9-1.0 muy sólida)
- severity: 1-5 (1 menor, 5 crítico)
- eta: "YYYY-MM-DD" si hay fechas; si no, estimación corta ("2w", "1m") y decláralo en assumptions.
- insight_id: slug estable tipo "bbva-be-{period}-{route_signature}-001" (minúsculas, guiones).
- period: el periodo del pack; si no existe usa "unknown" y anótalo en assumptions.
- journey_route: si no aparece, usa "unknown".
- Usa [] para listas sin elementos confirmables. Usa "unknown" (no null) para tags sin evidencia.

EVIDENCIA (OBLIGATORIO):
- evidence.quant: SOLO métricas del pack (value siempre string, con unidad si aplica).
- evidence.qual: SOLO verbatims del pack, sin PII (máx 5 por causa). No inventes quotes.

PRUEBAS Y ACCIONES (OBLIGATORIO):
- tests_or_checks: 2-5 comprobaciones concretas.
- actions: 1-3 acciones por causa (owner por rol, no nombres).

TAGS (OBLIGATORIO, SIN INVENTAR):
- Completa tags solo si el pack lo contiene o se puede derivar textualmente.
- Si no hay evidencia, usa "unknown" para geo/channel/lever/sublever/period/route_signature.

Devuelve SOLO un JSON con el esquema indicado y sin ningún texto adicional."""

LLM_BUSINESS_QUESTIONS = [
    "Devuelve SOLO el JSON del esquema (sin texto adicional). Prioriza causas raiz con impacto demostrable en NPS y plan de accion.",
    "Construye una narrativa de comite: 3 hipotesis causales no obvias, checks concretos, quick wins y riesgos por evidencia insuficiente.",
    "Disena un playbook semanal: top 3 palancas, owner por rol, ETA y KPI leading/lagging para recuperar NPS termico.",
    "Genera guion de 8 slides: mensaje principal, señal temporal, causas, impacto, prioridades, plan 30-60-90, gobierno KPI y decisiones de comité.",
]


DEFAULT_OPP_DIMS = ("Canal", "Palanca", "Subpalanca", "UsuarioDecisión", "Segmento")


@st.cache_data(show_spinner=False)
def load_context_df(
    store_dir: Path,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    nps_group_choice: str,
    columns: tuple[str, ...],
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    month_filter: Optional[str] = None,
) -> pd.DataFrame:
    """Load dataset for a context with column projection.

    Uses the DatasetStore (JSONL source of truth + partitioned Parquet cache).
    The `columns` tuple is part of the cache key, so each view can request only
    the columns it needs (min CPU/RAM).
    """
    store = DatasetStore(store_dir)
    stored = store.get(
        DatasetContext(service_origin=service_origin, service_origin_n1=service_origin_n1)
    )
    if stored is None:
        return pd.DataFrame()

    table = store.load_table(
        stored,
        columns=list(columns),
        date_start=pd.to_datetime(date_start) if date_start else None,
        date_end=pd.to_datetime(date_end) if date_end else None,
    )
    # Convert to pandas only at the edge (Streamlit/Plotly). The Arrow Table is cached
    # as RecordBatches in the store for reuse across charts.
    df = table.to_pandas()

    # Cross-year month filter (used only when pop_year == "Todos" and pop_month != "Todos").
    if month_filter and "Fecha" in df.columns:
        try:
            dt = pd.to_datetime(df["Fecha"], errors="coerce")
            df = df.loc[dt.dt.month.astype(int) == int(month_filter)].copy()
        except Exception:
            pass

    # Optional filter by service_origin_n2 (comma-separated values).
    # IMPORTANT: strict token-set equality.
    # - Selecting "SN2X" must NOT include rows like "SN2X, SN2Y".
    # - Selecting "SN2X, SN2Y" matches rows with the same set (order-insensitive).
    # Prefer precomputed token-set key when available (faster than per-row tokenset())
    n2_key = DatasetContext._norm_n2(service_origin_n2)
    if n2_key:
        if "_service_origin_n2_key" not in df.columns:
            raise KeyError(
                "Dataset missing required derived column '_service_origin_n2_key'. "
                "Re-import the Excel to rebuild derived features."
            )
        df = df.loc[df["_service_origin_n2_key"].astype(str) == n2_key].copy()

    # Lightweight dtype optimization (safe even with partial columns)
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    if "NPS" in df.columns:
        df["NPS"] = pd.to_numeric(df["NPS"], errors="coerce")

    for c in ["Canal", "Palanca", "Subpalanca", "UsuarioDecisión", "Segmento", "NPS Group"]:
        if c in df.columns:
            df[c] = df[c].astype("category")

    # Apply global NPS group filter (sidebar): Todos / Detractores / Neutros / Promotores
    df = filter_nps_by_group(df, nps_group_choice)

    return df


VIEW_COLUMNS = {
    "resumen": (
        "Fecha",
        "NPS",
        "NPS Group",
        "Canal",
        "Palanca",
        "Subpalanca",
        "UsuarioDecisión",
        "Segmento",
        "Comment",
        "ID",
    ),
    "drivers": (
        "Fecha",
        "NPS",
        "NPS Group",
        "Canal",
        "Palanca",
        "Subpalanca",
        "UsuarioDecisión",
        "Segmento",
    ),
    "texto": (
        "Fecha",
        "NPS",
        "NPS Group",
        "Comment",
        "Canal",
        "Palanca",
        "Subpalanca",
        "UsuarioDecisión",
        "Segmento",
    ),
    "llm": (
        "Fecha",
        "NPS",
        "NPS Group",
        "Comment",
        "Canal",
        "Palanca",
        "Subpalanca",
        "UsuarioDecisión",
        "Segmento",
        "ID",
    ),
    "datos": (),  # empty tuple => load full dataset for inspection
}


def filter_nps_by_group(df: pd.DataFrame, group_mode: str) -> pd.DataFrame:
    """Filter NPS dataframe by selected NPS group.

    group_mode values: Todos | Detractores | Promotores | Neutros
    """
    gm = (group_mode or "Todos").strip().lower()
    if gm in ("todos", "all"):
        return df

    if df is None or df.empty:
        return df

    # Normalize group labels if present
    if "NPS Group" in df.columns:
        g = df["NPS Group"].astype(str).str.strip().str.lower()
        if gm.startswith("detr"):
            return df.loc[g.str.contains("detr", na=False)].copy()
        if gm.startswith("prom"):
            return df.loc[g.str.contains("prom", na=False)].copy()
        if gm.startswith("neu") or gm.startswith("pas"):
            return df.loc[g.str.contains("pas", na=False) | g.str.contains("neut", na=False)].copy()

    # Fallback to score if group label missing
    if "NPS" in df.columns:
        s = pd.to_numeric(df["NPS"], errors="coerce")
        if gm.startswith("detr"):
            return df.loc[s <= 6].copy()
        if gm.startswith("prom"):
            return df.loc[s >= 9].copy()
        if gm.startswith("neu") or gm.startswith("pas"):
            return df.loc[(s >= 7) & (s <= 8)].copy()

    return df


# Column sets per chart (granular manifest). Each chart requests only what it needs.
CHART_COLUMNS = {
    "trend_weekly": ("Fecha", "NPS"),
    "daily_mix": ("Fecha", "NPS"),
    "daily_volume": ("Fecha", "NPS"),
    "daily_kpis": ("Fecha", "NPS"),
    "daily_semaforo": ("Fecha", "NPS"),
    "daily_llm": (
        "Fecha",
        "NPS",
        "NPS Group",
        "Comment",
        "Palanca",
        "Subpalanca",
        "Canal",
        "UsuarioDecisión",
        "Segmento",
    ),
    "drivers_bar": ("NPS", "Palanca", "Subpalanca", "Canal", "UsuarioDecisión", "Segmento"),
    "drivers_delta": (
        "Fecha",
        "NPS",
        "Palanca",
        "Subpalanca",
        "Canal",
        "UsuarioDecisión",
        "Segmento",
    ),
    "cohort_heatmap": ("NPS", "Palanca", "Subpalanca", "Canal", "UsuarioDecisión", "Segmento"),
    "topics": ("Comment",),
}


def optimize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Reduce memory footprint and speed up groupbys (categoricals + downcast)."""
    out = df.copy()

    # Categoricals for low-cardinality strings (faster groupby, much less RAM)
    for c in out.select_dtypes(include=["object"]).columns:
        if c.lower() in {"comment", "comentario", "verbatim", "texto"}:
            continue
        nunique = out[c].nunique(dropna=False)
        if nunique > 0 and nunique / max(len(out), 1) < 0.35:
            out[c] = out[c].astype("category")

    for c in out.select_dtypes(include=["int64", "int32"]).columns:
        out[c] = pd.to_numeric(out[c], downcast="integer")
    for c in out.select_dtypes(include=["float64", "float32"]).columns:
        out[c] = pd.to_numeric(out[c], downcast="float")

    return out


def _annotate_chain_candidates(chain_df: pd.DataFrame) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()

    out = chain_df.copy().reset_index(drop=True)
    def _safe_int_label(value: object) -> int:
        try:
            return int(float(value))
        except Exception:
            return 0

    topic = out.get("nps_topic", pd.Series([""] * len(out), index=out.index)).astype(str).str.strip()
    touchpoint = (
        out.get("touchpoint", pd.Series([""] * len(out), index=out.index)).astype(str).str.strip()
    )
    out["chain_key"] = [
        hashlib.sha1(f"{tp}|{tpnt}".encode("utf-8")).hexdigest()[:12]
        for tp, tpnt in zip(topic.tolist(), touchpoint.tolist())
    ]
    out["selection_label"] = [
        (
            f"{touchpoint_val or 'Touchpoint sin etiquetar'} | {topic_val or 'Tema sin etiqueta'} | "
            f"{_safe_int_label(inc)} INC | {_safe_int_label(com)} VoC"
        )
        for topic_val, touchpoint_val, inc, com in zip(
            topic.tolist(),
            touchpoint.tolist(),
            out.get("linked_incidents", pd.Series([0] * len(out), index=out.index)).tolist(),
            out.get("linked_comments", pd.Series([0] * len(out), index=out.index)).tolist(),
        )
    ]
    return out


def _sync_chain_selection_state(
    chain_df: pd.DataFrame,
    *,
    key_prefix: str,
    default_limit: int = 3,
) -> list[str]:
    if chain_df is None or chain_df.empty:
        st.session_state[f"{key_prefix}_sig"] = "empty"
        st.session_state[f"{key_prefix}_selected"] = []
        st.session_state[f"{key_prefix}_view_idx"] = 0
        return []

    keys = chain_df["chain_key"].astype(str).tolist()
    sig = hashlib.sha1("|".join(keys).encode("utf-8")).hexdigest()
    sig_key = f"{key_prefix}_sig"
    selected_key = f"{key_prefix}_selected"
    view_idx_key = f"{key_prefix}_view_idx"
    if st.session_state.get(sig_key) != sig:
        st.session_state[sig_key] = sig
        st.session_state[selected_key] = keys[: min(int(default_limit), len(keys))]
        st.session_state[view_idx_key] = 0

    selected = [
        str(k)
        for k in st.session_state.get(selected_key, [])
        if str(k) in keys
    ][: int(default_limit)]
    if not selected:
        selected = keys[: min(int(default_limit), len(keys))]
        st.session_state[selected_key] = selected
    else:
        st.session_state[selected_key] = selected

    current_idx = int(st.session_state.get(view_idx_key, 0) or 0)
    if current_idx < 0 or current_idx >= len(keys):
        st.session_state[view_idx_key] = 0
    return selected


def _select_chain_rows(chain_df: pd.DataFrame, selected_keys: list[str]) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()
    if not selected_keys:
        return chain_df.head(0).copy()

    selected = chain_df[chain_df["chain_key"].astype(str).isin([str(k) for k in selected_keys])].copy()
    if selected.empty:
        return selected

    selected["__order"] = pd.Categorical(
        selected["chain_key"].astype(str),
        categories=[str(k) for k in selected_keys],
        ordered=True,
    )
    selected = selected.sort_values("__order").drop(columns="__order").reset_index(drop=True)
    return selected


def _cap_chain_evidence_rows(
    chain_df: pd.DataFrame,
    *,
    max_incident_examples: int = 5,
    max_comment_examples: int = 2,
) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()

    out = chain_df.copy()

    def _normalize_list(value: object) -> list[str]:
        if isinstance(value, list):
            values = value
        elif value in (None, ""):
            values = []
        else:
            values = [value]
        return [str(v).strip() for v in values if str(v).strip()]

    def _cap(values: list[str], limit: int) -> list[str]:
        try:
            max_items = int(limit)
        except Exception:
            return values
        if max_items <= 0:
            return values
        return values[:max_items]

    out["incident_examples"] = [
        _cap(_normalize_list(v), max_incident_examples)
        for v in out.get("incident_examples", pd.Series([[]] * len(out), index=out.index)).tolist()
    ]
    out["comment_examples"] = [
        _cap(_normalize_list(v), max_comment_examples)
        for v in out.get("comment_examples", pd.Series([[]] * len(out), index=out.index)).tolist()
    ]
    return out


def load_llm_insights_for_context(
    settings: Settings, service_origin: str, service_origin_n1: str
) -> list[dict[str, Any]]:
    """Load persisted LLM insights for the selected context."""
    from nps_lens.llm.knowledge_cache import KnowledgeCache

    kc = KnowledgeCache.for_context(
        settings.knowledge_dir, service_origin=service_origin, service_origin_n1=service_origin_n1
    )
    data = kc.load()
    entries = data.get("entries", [])
    # entries are dicts; keep as-is
    return list(entries)


def _df_fingerprint(
    df: pd.DataFrame, *, cols: Optional[list[str]] = None, sample_rows: int = 1500
) -> str:
    """Cheap-ish fingerprint for deterministic caching.

    We avoid hashing full DataFrames (slow). Instead we combine:
      - shape + columns + dtypes
      - Fecha min/max (if present)
      - hash of a small head() sample on selected columns
    """
    from hashlib import sha1

    if df is None:
        return "empty"
    use_cols = cols or list(df.columns)
    use_cols = [c for c in use_cols if c in df.columns]

    parts = [f"r={len(df)}", f"c={len(use_cols)}"]
    parts.append(",".join(use_cols))
    try:
        dtypes = [f"{c}:{str(df[c].dtype)}" for c in use_cols[:40]]
        parts.append("|".join(dtypes))
    except Exception:
        pass

    if "Fecha" in df.columns:
        try:
            s = pd.to_datetime(df["Fecha"], errors="coerce").dropna()
            if not s.empty:
                parts.append(f"fmin={s.min().date().isoformat()}")
                parts.append(f"fmax={s.max().date().isoformat()}")
        except Exception:
            pass

    try:
        sample = df[use_cols].head(int(sample_rows)).copy()
        # pandas util hash is stable for given values
        h = pd.util.hash_pandas_object(sample, index=True).values
        parts.append(sha1(h.tobytes()).hexdigest())
    except Exception:
        pass

    raw = "|".join(parts)
    return sha1(raw.encode("utf-8")).hexdigest()


def cached_driver_table(df: pd.DataFrame, dimension: str) -> pd.DataFrame:
    svc: AppService = st.session_state.get("_app_service")  # type: ignore
    stats_df = svc.driver_stats(df, dimension=dimension)
    if "gap_vs_overall" not in stats_df.columns:
        raise KeyError("gap_vs_overall missing from driver_table output")
    return stats_df


def cached_rank_opportunities(
    df: pd.DataFrame,
    min_n: int,
    *,
    dimensions: Optional[list[str]] = None,
):
    perf: PerfTracker = st.session_state.get("_perf")  # type: ignore
    cache: DiskCache = st.session_state.get("_disk_cache")  # type: ignore

    fp = _df_fingerprint(
        df, cols=["NPS", "Palanca", "Subpalanca", "Canal", "UsuarioDecisión", "Segmento"]
    )
    dims = [d for d in (dimensions or list(DEFAULT_OPP_DIMS)) if d in df.columns]
    key = cache.make_key(
        namespace="opps", dataset_sig=fp, params={"min_n": int(min_n), "dims": dims}
    )
    hit = cache.get(key)
    if hit is not None:
        return hit

    with perf.track("opportunities"):
        out = rank_opportunities(df, dimensions=dims, min_n=min_n)
    cache.set(key, out, meta={"namespace": "opps"})
    return out


def _copy_to_clipboard(payload: str, *, toast: str = "Copiado") -> None:
    """Copy text to clipboard via a tiny JS snippet (Streamlit has no native clipboard API)."""
    # Use JSON encoding to safely escape quotes/newlines.
    js = "<script>" f"navigator.clipboard.writeText({json.dumps(payload)});" "</script>"
    components.html(js, height=0)
    # `st.toast` exists on newer Streamlit; fallback to success if not.
    if hasattr(st, "toast"):
        st.toast(toast)
    else:
        st.success(toast)


def _clipboard_copy_widget(text: str, *, label: str = "Copiar prompt") -> None:
    """Render a browser-side copy button.

    Why this exists:
    - Calling `navigator.clipboard.writeText(...)` from Python-triggered reruns is often
      blocked because it is *not* considered a user gesture by the browser.
    - Rendering an actual HTML button and copying on its click is reliably treated as
      a user gesture, so clipboard copy works consistently.

    This widget is self-contained and does not require extra components.
    """

    payload_b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
    uid = hashlib.sha1(payload_b64.encode("ascii")).hexdigest()[:10]

    html = f"""
    <div style="display:flex; gap:12px; align-items:center;">
      <button
        id="nps_copy_{uid}"
        style="
          width: 100%;
          padding: 10px 14px;
          border-radius: 12px;
          border: 0;
          cursor: pointer;
          font-weight: 650;
          background: var(--nps-accent, #1f77ff);
          color: white;
        "
        title="Copiar al portapapeles"
      >{label}</button>
      <span id="nps_copy_msg_{uid}" style="font-size:12px; color: var(--nps-muted, #6b7280);"></span>
    </div>
    <script>
      (function() {{
        const btn = document.getElementById("nps_copy_{uid}");
        const msg = document.getElementById("nps_copy_msg_{uid}");
        // Base64 -> UTF-8 (avoid mojibake like "MÃ©xico" / "raÃ­z")
        const b64 = "{payload_b64}";
        const bin = atob(b64);
        const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
        const txt = new TextDecoder("utf-8").decode(bytes);
        async function doCopy() {{
          try {{
            await navigator.clipboard.writeText(txt);
            msg.textContent = "Copiado ✅";
            const old = btn.textContent;
            btn.textContent = "Copiado";
            setTimeout(() => {{ btn.textContent = old; msg.textContent = ""; }}, 1800);
          }} catch (e) {{
            msg.textContent = "No se pudo copiar. Selecciona el texto y usa Ctrl/Cmd+C.";
          }}
        }}
        btn.addEventListener("click", doCopy);
      }})();
    </script>
    """
    components.html(html, height=52)


def _repair_json_text(text: str) -> str:
    """Best-effort repair for common 'almost JSON' LLM outputs.

    Repairs:
    - smart quotes -> ASCII quotes
    - markdown fences ```json ... ```
    - trailing commas
    - Python literals (None/True/False) -> JSON (null/true/false)
    - NaN/Infinity -> null
    """
    if not text:
        return ""

    s = text.strip()

    # Remove markdown fences
    import re

    s = re.sub(r"```(?:json)?\s*", "", s, flags=re.IGNORECASE)
    s = s.replace("```", "")

    # Normalize punctuation / smart quotes
    rep = {
        "\u201c": '"',
        "\u201d": '"',
        "\u201e": '"',
        "\u00ab": '"',
        "\u00bb": '"',
        "\u2018": "'",
        "\u2019": "'",
        "\u201a": "'",
        "\u00a0": " ",
    }
    for k, v in rep.items():
        s = s.replace(k, v)

    def _escape_unescaped_quotes_in_strings(payload: str) -> str:
        """Escape inner quotes inside JSON strings.

        LLMs sometimes emit unescaped double quotes inside a quoted string, e.g.:
            "driver "Funcionamiento""  (good)
            "driver "Funcionamiento""    (bad)

        We treat a quote as a string terminator only if the next non-space
        character is one of: ':', ',', '}', ']', or end-of-input.
        Otherwise, we escape it as an inner quote.
        """
        out: list[str] = []
        in_str = False
        esc = False
        n = len(payload)
        for i, ch in enumerate(payload):
            if not in_str:
                out.append(ch)
                if ch == '"':
                    in_str = True
                continue

            # in string
            if esc:
                out.append(ch)
                esc = False
                continue
            if ch == "\\":
                out.append(ch)
                esc = True
                continue
            if ch == '"':
                # lookahead
                j = i + 1
                while j < n and payload[j].isspace():
                    j += 1
                nxt = payload[j] if j < n else ""
                if nxt in (":", ",", "}", "]", ""):
                    out.append(ch)
                    in_str = False
                else:
                    # Emit an escaped quote (\") so the resulting JSON stays valid.
                    out.append('\\"')
                continue
            out.append(ch)
        return "".join(out)

    # If the user pasted something that contains JSON but with extra text, extract first balanced {...}
    start = s.find("{")
    if start != -1:
        depth = 0
        end = -1
        for i in range(start, len(s)):
            ch = s[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        if end != -1:
            s = s[start:end].strip()

    # Replace python-ish literals with JSON literals (careful: only whole words)
    s = re.sub(r"\bNone\b", "null", s)
    s = re.sub(r"\bTrue\b", "true", s)
    s = re.sub(r"\bFalse\b", "false", s)
    s = re.sub(r"\bNaN\b", "null", s)
    s = re.sub(r"\bInfinity\b", "null", s)
    s = re.sub(r"\b-Infinity\b", "null", s)

    # Remove trailing commas before } or ]
    s = re.sub(r",\s*([}\]])", r"\1", s)

    # Escape inner quotes inside strings (common LLM mistake)
    s = _escape_unescaped_quotes_in_strings(s)

    return s.strip()


def _parse_json_with_repair(
    text: str,
) -> tuple[Optional[dict[str, Any]], Optional[str], Optional[str]]:
    """Parse JSON from pasted text. Repairs automatically when possible.

    Returns: (obj, repaired_json_text, error_message)
    """
    import json
    import re

    if not text or not text.strip():
        return None, None, None

    def _canonical(obj_any: Any) -> str:
        """Return canonical JSON text for a parsed object."""
        try:
            return json.dumps(_json_sanitize(obj_any), ensure_ascii=False, indent=2)
        except Exception:
            return json.dumps(obj_any, ensure_ascii=False, indent=2, default=str)

    # First try: strict JSON parse after minimal normalization
    candidate = _repair_json_text(text)
    try:
        obj = json.loads(candidate)
        if isinstance(obj, dict):
            return obj, _canonical(obj), None
        return None, candidate, "El JSON detectado no es un objeto (dict)."
    except json.JSONDecodeError as e:
        # Fallback: python-literal parsing (handles single quotes, trailing commas, etc.)
        try:
            import ast

            py_candidate = candidate
            # Convert JSON literals back to Python literals for literal_eval
            py_candidate = re.sub(r"\bnull\b", "None", py_candidate)
            py_candidate = re.sub(r"\btrue\b", "True", py_candidate)
            py_candidate = re.sub(r"\bfalse\b", "False", py_candidate)
            py_obj = ast.literal_eval(py_candidate)
            if isinstance(py_obj, dict):
                return py_obj, _canonical(py_obj), None
        except Exception:
            pass
        return None, candidate, f"JSON invalido (linea {e.lineno}, col {e.colno}): {e.msg}"
    except Exception as e:
        return None, candidate, f"JSON invalido: {e}"


def _try_parse_json(text: str) -> Optional[dict[str, Any]]:
    """Backward-compatible wrapper used in previews."""
    obj, _, _ = _parse_json_with_repair(text)
    return obj


def _json_sanitize(obj: Any) -> Any:
    """Make objects JSON-serializable (pandas/numpy friendly).

    The Deep-Dive pack contains pandas objects (e.g., Timestamps) which are not
    serializable by Python's standard json module.
    """

    # pandas / numpy missing values
    # Only evaluate scalar missingness. Calling pd.isna() on arrays/Series returns
    # array-like and NumPy deprecates truthiness of empty arrays.
    try:
        from pandas.api.types import is_scalar

        if obj is pd.NaT:
            return None
        if is_scalar(obj) and not isinstance(obj, (str, int, float, bool)):
            na = pd.isna(obj)
            if isinstance(na, (bool, np.bool_)) and bool(na):
                return None
    except Exception:
        pass

    # datetime-like
    if isinstance(obj, pd.Timestamp):
        try:
            if obj.tzinfo is not None:
                obj = obj.tz_convert(None)
            return obj.isoformat()
        except Exception:
            return str(obj)
    if isinstance(obj, np.datetime64):
        try:
            return pd.to_datetime(obj).to_pydatetime().isoformat()
        except Exception:
            return str(obj)

    # numpy scalars
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()

    # containers
    if isinstance(obj, dict):
        return {str(k): _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_sanitize(v) for v in obj]

    return obj


def _validate_insight_schema(obj: dict[str, Any]) -> tuple[bool, list[str]]:
    ok, errs, _norm = validate_insight_response(obj)
    if ok:
        return True, []
    return False, errs


def _render_llm_insights(theme: Theme) -> None:
    insights = st.session_state.get("llm_insights")
    # Avoid NumPy truthiness warnings (empty array truth value is deprecated).
    is_empty = False
    if insights is None:
        is_empty = True
    elif isinstance(insights, np.ndarray):
        is_empty = insights.size == 0
    else:
        try:
            is_empty = len(insights) == 0  # type: ignore[arg-type]
        except TypeError:
            is_empty = False

    if is_empty:
        st.info(
            "Aún no has añadido insights del LLM. Ve a la pestaña **✨ Insights LLM** "
            "para pegarlos aquí."
        )
        return

    st.markdown(
        "<div class='nps-card nps-card--flat'>"
        "<b>Insights integrados</b><br/>"
        "<span class='nps-muted'>Estos hallazgos forman parte del discurso del dashboard "
        "(Resumen/Drivers). "
        "Puedes eliminarlos o exportarlos como briefing.</span>"
        "</div>",
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def _daily_metrics(df: pd.DataFrame, *, days: int) -> pd.DataFrame:
    """Compute daily metrics used by charts and the LLM helper.

    Returns a dataframe with:
    day, n, det_pct, pas_pct, pro_pct, classic_nps
    """

    if "Fecha" not in df.columns or "NPS" not in df.columns:
        return pd.DataFrame()

    tmp = df.dropna(subset=["Fecha", "NPS"]).copy()
    if tmp.empty:
        return pd.DataFrame()

    tmp["day"] = tmp["Fecha"].dt.floor("D")
    end = tmp["day"].max()
    start = end - pd.Timedelta(days=int(days) - 1)
    tmp = tmp.loc[tmp["day"] >= start].copy()
    if tmp.empty:
        return pd.DataFrame()

    scores = pd.to_numeric(tmp["NPS"], errors="coerce")
    tmp["score"] = scores.clip(lower=0, upper=10)
    tmp = tmp.dropna(subset=["score"]).copy()
    if tmp.empty:
        return pd.DataFrame()

    tmp["is_det"] = tmp["score"] <= 6
    tmp["is_pas"] = (tmp["score"] >= 7) & (tmp["score"] <= 8)
    tmp["is_pro"] = tmp["score"] >= 9

    agg = (
        tmp.groupby("day", as_index=False)
        .agg(
            n=("score", "size"),
            det=("is_det", "mean"),
            pas=("is_pas", "mean"),
            pro=("is_pro", "mean"),
        )
        .sort_values("day")
    )
    agg["det_pct"] = agg["det"] * 100.0
    agg["pas_pct"] = agg["pas"] * 100.0
    agg["pro_pct"] = agg["pro"] * 100.0
    agg["classic_nps"] = (agg["pro"] - agg["det"]) * 100.0
    return agg[["day", "n", "det_pct", "pas_pct", "pro_pct", "classic_nps"]].copy()


def render_sidebar(  # noqa: PLR0915
    settings: Settings,
) -> tuple[
    Optional[Path],
    int,
    str,
    str,
    str,
    str,
    str,
    str,
    Path,
    str,
    Optional[str],
    bool,
    str,
]:
    """Single-source sidebar: Context -> dataset -> controls.

    Context dimensions:
      - service_origin (antes: geografía)
      - service_origin_n1 (antes: canal)
      - service_origin_n2 (opcional; filtro, puede venir vacío o separado por comas)
    """
    store = DatasetStore(settings.data_dir / "store")

    # IMPORTANT CONTRACT:
    # Context values MUST come from .env (Settings.from_env). We do not infer
    # options from stored datasets to avoid silent drift. Advanced users can
    # add/remove values by editing the .env.

    service_origin_options = list(settings.service_origin_values) or [
        settings.default_service_origin
    ]
    # Keep current/default selectable even if .env was edited incorrectly.
    for v in [settings.default_service_origin]:
        if v and v not in service_origin_options:
            service_origin_options.append(v)

    # Default context: first stored dataset, else Settings defaults.
    if "_ctx" not in st.session_state:
        ctx0 = store.default_context() or DatasetContext(
            settings.default_service_origin,
            settings.default_service_origin_n1,
        )
        st.session_state["_ctx"] = {
            "service_origin": ctx0.service_origin,
            "service_origin_n1": ctx0.service_origin_n1,
            "service_origin_n2": "",
        }

    ctx_state = st.session_state["_ctx"]
    cur_so = str(ctx_state.get("service_origin", settings.default_service_origin))
    cur_n1 = str(ctx_state.get("service_origin_n1", settings.default_service_origin_n1))
    cur_n2 = str(ctx_state.get("service_origin_n2", ""))

    defaults = st.session_state.get(
        "_controls",
        {
            "theme_mode": "light",
            "min_n": 200,
        },
    )

    with st.sidebar:
        st.header("Contexto")
        if cur_so not in service_origin_options:
            service_origin_options = [cur_so, *service_origin_options]
        service_origin = st.selectbox(
            "Service origin",
            service_origin_options,
            index=service_origin_options.index(cur_so) if cur_so in service_origin_options else 0,
        )

        # service_origin_n1 options are sourced from .env mapping.
        n1_opts = list(settings.service_origin_n1_map.get(service_origin, []))
        if not n1_opts:
            # Safe fallback to keep the UI usable even if the mapping is incomplete.
            n1_opts = [settings.default_service_origin_n1]
        if cur_n1 not in n1_opts:
            n1_opts = [cur_n1, *n1_opts]
        service_origin_n1 = st.selectbox(
            "Service origin N1",
            n1_opts,
            index=n1_opts.index(cur_n1) if cur_n1 in n1_opts else 0,
        )

        # service_origin_n2 tokens are sourced from .env (optional). If present,
        # we use a multiselect to avoid typos and ensure stable filtering.
        n2_allowed = list(settings.service_origin_n2_values)
        if n2_allowed:
            cur_n2_list = [v.strip() for v in (cur_n2 or "").split(",") if v.strip()]
            selected = st.multiselect(
                "Service origin N2 (opcional)",
                options=n2_allowed,
                default=[v for v in cur_n2_list if v in n2_allowed],
                help="Opcional. Selecciona uno o varios valores (el dataset puede venir vacío o con múltiples valores separados por comas).",
            )
            service_origin_n2 = ", ".join(selected)
        else:
            service_origin_n2 = st.text_input(
                "Service origin N2 (opcional)",
                value=cur_n2,
                help="Opcional. Puede venir vacío o con valores separados por comas (ej: SN2A, SN2B).",
            )

        # Persist context selection (includes n2 filter)
        st.session_state["_ctx"] = {
            "service_origin": service_origin,
            "service_origin_n1": service_origin_n1,
            "service_origin_n2": service_origin_n2,
        }

        st.subheader("Población NPS")

        # Global time population: Año/Mes (transversal a TODO el dashboard).
        # Defaults: last available year/month from the persisted dataset for the selected context.
        pop_year_default = POP_ALL
        pop_month_default = POP_ALL

        ctx_base = DatasetContext(
            service_origin=service_origin, service_origin_n1=service_origin_n1
        )
        meta_pop = store.read_meta(ctx_base)
        dataset_id = str(meta_pop.get("dataset_id") or "")
        years_available, months_by_year = store.available_year_month(ctx_base.key())

        # Determine default from meta.date_range.max (authoritative)
        try:
            dr = meta_pop.get("date_range") or {}
            max_s = dr.get("max")
            ts = pd.to_datetime(max_s, errors="coerce") if max_s else None
            if ts is not None and not pd.isna(ts):
                pop_year_default = str(int(ts.year))
                pop_month_default = str(int(ts.month)).zfill(2)
        except Exception:
            pass

        # If dataset changed (new upload), reset population defaults deterministically.
        if dataset_id and st.session_state.get("_pop_dataset_id") != dataset_id:
            st.session_state["_pop_dataset_id"] = dataset_id
            st.session_state["_pop_year"] = pop_year_default
            st.session_state["_pop_month"] = pop_month_default

        year_options = [POP_ALL] + years_available
        cur_pop_year = str(st.session_state.get("_pop_year", pop_year_default))
        if cur_pop_year not in year_options:
            cur_pop_year = pop_year_default if pop_year_default in year_options else POP_ALL

        pop_year = st.selectbox(
            "Año",
            options=year_options,
            index=year_options.index(cur_pop_year) if cur_pop_year in year_options else 0,
            help="Filtro global temporal que aplica a todo el dashboard (datos, topics, drivers, journey, alertas, NPS↔Helix, LLM packs).",
        )
        st.session_state["_pop_year"] = pop_year

        # Month options depend on year selection.
        if pop_year != POP_ALL and pop_year in months_by_year:
            month_options = [POP_ALL] + months_by_year.get(pop_year, [])
        else:
            # Union of months that actually exist in the dataset (avoid showing empty months).
            months_union: set[str] = set()
            for _yy, mlist in months_by_year.items():
                months_union.update(mlist)
            month_options = [POP_ALL] + sorted(months_union)

        cur_pop_month = str(st.session_state.get("_pop_month", pop_month_default))
        # If user changed year, ensure month is still valid.
        if cur_pop_month not in month_options:
            cur_pop_month = (
                pop_month_default
                if pop_year == pop_year_default and pop_month_default in month_options
                else POP_ALL
            )

        pop_month = st.selectbox(
            "Mes",
            options=month_options,
            index=month_options.index(cur_pop_month) if cur_pop_month in month_options else 0,
            format_func=month_format_es,
            help="Filtro global temporal. Si seleccionas Año=Todos y Mes=Marzo, se analiza Marzo en todos los años disponibles.",
        )
        st.session_state["_pop_month"] = pop_month

        # First load of this Streamlit session: force default group to "Todos"
        # even if a stale value exists from a previous browser/state snapshot.
        if "_nps_group_init_done" not in st.session_state:
            st.session_state["_nps_group_choice"] = POP_ALL
            st.session_state["_nps_group_init_done"] = True
        elif st.session_state.get("_nps_group_choice") not in NPS_GROUP_OPTIONS:
            st.session_state["_nps_group_choice"] = POP_ALL

        nps_group_choice = st.selectbox(
            "Grupo",
            options=NPS_GROUP_OPTIONS,
            index=(
                NPS_GROUP_OPTIONS.index(st.session_state.get("_nps_group_choice", POP_ALL))
                if st.session_state.get("_nps_group_choice") in NPS_GROUP_OPTIONS
                else 0
            ),
            help="Selecciona la población sobre la que se calculan TODOS los análisis e insights (Drivers, Texto, Journey, Alertas, Insights LLM, NPS↔Helix, Datos).",
        )
        st.session_state["_nps_group_choice"] = nps_group_choice

        st.divider()
        st.header("NPS")
        st.caption(
            "Sube el Excel de NPS térmico para el contexto seleccionado (service_origin + service_origin_n1)."
        )

        up = st.file_uploader("Subir Excel NPS térmico (.xlsx)", type=["xlsx", "xlsm", "xls"])
        sheet_name = st.text_input("Hoja (opcional)", value="") or None

        ctx = DatasetContext(service_origin=service_origin, service_origin_n1=service_origin_n1)
        stored = store.get(ctx)
        if stored is not None:
            meta = json.loads(stored.meta_path.read_text(encoding="utf-8"))
            st.success(
                f"Dataset activo: {meta.get('rows', '?'):,} filas · actualizado {meta.get('updated_at_utc', '?')}"
            )
        else:
            st.info("No hay dataset persistido para este contexto. Sube el Excel para empezar.")

        if st.button("Importar / actualizar NPS", type="primary", use_container_width=True):
            if up is None:
                st.warning("Primero sube un Excel.")
            else:
                uploads_dir = settings.data_dir / "uploads"
                uploads_dir.mkdir(parents=True, exist_ok=True)
                upload_path = uploads_dir / up.name

                # Write only if changed to avoid rerun storms
                buf = up.getbuffer()
                if (not upload_path.exists()) or (upload_path.stat().st_size != len(buf)):
                    upload_path.write_bytes(buf)

                res = read_nps_thermal_excel(
                    str(upload_path),
                    service_origin=service_origin,
                    service_origin_n1=service_origin_n1,
                    sheet_name=sheet_name,
                )
                store.save_df(ctx, res.df, source=f"excel:{upload_path.name}")
                st.session_state["_last_import_issues"] = [asdict(i) for i in res.issues]
                st.rerun()

        issues = st.session_state.get("_last_import_issues") or []
        if issues:
            with st.expander("Avisos / errores del último import", expanded=False):
                st.json(issues)

        # --------------------
        # Helix (Incidencias)
        # --------------------
        st.divider()
        st.header("Helix")
        st.caption(
            "Sube el Excel de incidencias reportadas al servicio. Se ingestan SOLO las filas que pertenecen al contexto seleccionado."
        )

        helix_up = st.file_uploader(
            "Subir Excel Helix (incidencias) (.xlsx)",
            type=["xlsx", "xlsm", "xls"],
            key="helix_uploader",
        )
        helix_sheet = st.text_input("Hoja Helix (opcional)", value="", key="helix_sheet") or None

        helix_store = HelixIncidentStore(settings.data_dir / "helix")
        hctx = DatasetContext(service_origin=service_origin, service_origin_n1=service_origin_n1)
        hstored = helix_store.get(hctx)
        if hstored is not None:
            hmeta = json.loads(hstored.meta_path.read_text(encoding="utf-8"))
            st.success(
                f"Helix activo: {hmeta.get('rows', '?'):,} filas · actualizado {hmeta.get('updated_at_utc', '?')}"
            )
        else:
            st.info(
                "No hay incidencias Helix persistidas para este contexto. Sube el Excel para ingestar."
            )

        if st.button("Importar / actualizar Helix", type="secondary", use_container_width=True):
            if helix_up is None:
                st.warning("Primero sube un Excel de Helix.")
            else:
                uploads_dir = settings.data_dir / "uploads"
                uploads_dir.mkdir(parents=True, exist_ok=True)
                upload_path = uploads_dir / helix_up.name

                buf = helix_up.getbuffer()
                if (not upload_path.exists()) or (upload_path.stat().st_size != len(buf)):
                    upload_path.write_bytes(buf)

                res2 = read_helix_incidents_excel(
                    str(upload_path),
                    service_origin=service_origin,
                    service_origin_n1=service_origin_n1,
                    service_origin_n2=service_origin_n2,
                    sheet_name=helix_sheet,
                )

                st.session_state["_last_helix_import_issues"] = [asdict(i) for i in res2.issues]

                if res2.df is None or res2.df.empty:
                    st.warning(
                        "Ingesta NO realizada: el fichero no contiene registros para el contexto seleccionado."
                    )
                else:
                    helix_store.save_df(hctx, res2.df, source=f"excel:{upload_path.name}")
                    st.rerun()

        helix_issues = st.session_state.get("_last_helix_import_issues") or []
        if helix_issues:
            with st.expander("Avisos / errores del último import Helix", expanded=False):
                st.json(helix_issues)

        st.divider()
        st.header("Experiencia")
        theme_mode = st.selectbox(
            "Modo visual",
            ["light", "dark"],
            index=0 if defaults["theme_mode"] == "light" else 1,
        )
        touchpoint_mode_labels = {
            TOUCHPOINT_SOURCE_DOMAIN: "Touchpoint como ahora",
            TOUCHPOINT_SOURCE_HELIX_N2: "Touchpoint como N2 asignado en Helix",
            TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: "Journeys ejecutivos de detracción",
        }
        current_touchpoint_mode = str(
            st.session_state.get("_touchpoint_source", TOUCHPOINT_SOURCE_DOMAIN)
        )
        if current_touchpoint_mode not in touchpoint_mode_labels:
            current_touchpoint_mode = TOUCHPOINT_SOURCE_DOMAIN
        touchpoint_source = st.radio(
            "Modo de lectura causal",
            options=list(touchpoint_mode_labels.keys()),
            index=list(touchpoint_mode_labels.keys()).index(current_touchpoint_mode),
            format_func=lambda key: touchpoint_mode_labels.get(str(key), str(key)),
            help="Elige si el racional se construye con el touchpoint actual, con el N2 final asignado en Helix o con una lectura ejecutiva de journeys.",
        )
        st.session_state["_touchpoint_source"] = touchpoint_source

        st.divider()
        st.header("Controles")
        with st.form("apply_controls", clear_on_submit=False):
            min_n = st.slider(
                "Mínimo N para oportunidades",
                50,
                1500,
                int(defaults["min_n"]),
                step=50,
            )
            applied = st.form_submit_button("Aplicar")

        if applied:
            st.session_state["_controls"] = {
                "theme_mode": theme_mode,
                "min_n": int(min_n),
            }

        st.divider()
        st.header("⚡ Performance")
        perf: PerfTracker = st.session_state.get("_perf")  # type: ignore
        rows = perf.summary() if perf is not None else []
        with st.expander("Ver timings (últimos cálculos)", expanded=False):
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
            else:
                st.caption("Aún no hay timings. Navega por el dashboard para generar cálculos.")
            cpa, cpb = st.columns(2)
            with cpa:
                if st.button("Reset timings", use_container_width=True):
                    with contextlib.suppress(Exception):
                        perf.reset()
                    st.rerun()
            with cpb:
                # Clear deterministic compute cache
                if st.button("Vaciar cache compute", use_container_width=True):
                    cache: DiskCache = st.session_state.get("_disk_cache")  # type: ignore
                    try:
                        if cache is not None and cache.base_dir.exists():
                            for pp in cache.base_dir.rglob("*"):
                                if pp.is_file():
                                    pp.unlink()
                    except Exception:
                        pass
                    st.rerun()

    stored2 = store.get(
        DatasetContext(service_origin=service_origin, service_origin_n1=service_origin_n1)
    )
    data_path = stored2.path if stored2 is not None else None
    data_ready = stored2 is not None

    return (
        data_path,
        int(st.session_state.get("_controls", defaults)["min_n"]),
        service_origin,
        service_origin_n1,
        service_origin_n2,
        pop_year,
        pop_month,
        st.session_state.get("_nps_group_choice", POP_ALL),
        settings.knowledge_dir,
        theme_mode,
        sheet_name,
        data_ready,
        st.session_state.get("_touchpoint_source", TOUCHPOINT_SOURCE_DOMAIN),
    )


def page_executive(
    df: pd.DataFrame,
    theme: Theme,
    store_dir: Path,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
) -> None:
    section(
        "Resumen del periodo",
        "Qué está pasando, dónde mirar primero y por qué (lenguaje de negocio).",
    )

    s = executive_summary(df)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi("Muestras", f"{s.n:,}", hint="Respuestas válidas")
    with c2:
        val = "-" if s.n == 0 else f"{s.nps_avg:.2f}"
        kpi("NPS medio (0-10)", val, hint="Media del score")
    with c3:
        kpi("Detractores (<=6)", f"{s.detractor_rate*100:.1f}%", hint="Riesgo")
    with c4:
        kpi("Promotores (>=9)", f"{s.promoter_rate*100:.1f}%", hint="Lealtad")

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    col_a, col_b = st.columns([2, 1])
    with col_a:
        card("Tendencia", "<div class='nps-muted'>Evolución del NPS medio.</div>", flat=True)
        tab_w, tab_dm, tab_adv = st.tabs(
            ["Semanal (media)", "Diaria (mix negocio)", "Detalle (semaforo)"]
        )
        with tab_w:
            fig = chart_nps_trend(df, theme, freq="W")
            if fig is None:
                st.info("No hay suficientes datos para construir una tendencia.")
            else:
                st.plotly_chart(apply_plotly_theme(fig, theme), use_container_width=True)

        with tab_dm:
            days = st.slider(
                "Ventana (días)",
                min_value=14,
                max_value=120,
                value=60,
                step=7,
                help=(
                    "Vista diaria pensada para negocio: muestra el mix de "
                    "Detractores (0-6), Pasivos (7-8) y Promotores (9-10)."
                ),
            )
            st.markdown(
                "<div class='nps-card nps-muted'>"
                "<b>Cómo leerlo:</b> más <b>rojo</b> (detractores) empeora NPS; "
                "más <b>verde</b> (promotores) lo mejora. "
                "Usa la barra de <b>volumen</b> (n) para no sobre-interpretar días con pocas respuestas."
                "</div>",
                unsafe_allow_html=True,
            )

            # Load only the requested window with predicate pushdown (partitioned parquet).
            end_day = pd.to_datetime(df["Fecha"], errors="coerce").max()
            end_day = end_day.floor("D") if end_day is not None and end_day == end_day else None
            if end_day is not None:
                start_day = end_day - pd.Timedelta(days=int(days) - 1)
                df_win = load_context_df(
                    store_dir,
                    service_origin,
                    service_origin_n1,
                    service_origin_n2,
                    st.session_state.get("_nps_group_choice", POP_ALL),
                    CHART_COLUMNS["daily_mix"],
                    date_start=str(start_day.date()),
                    date_end=str(end_day.date()),
                )
            else:
                df_win = df

            fig_mix = chart_daily_mix_business(df_win, theme, days=int(days))
            if fig_mix is None:
                st.info("No hay suficientes datos para construir la vista diaria.")
            else:
                st.plotly_chart(apply_plotly_theme(fig_mix, theme), use_container_width=True)
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                fig_vol = chart_daily_volume(df_win, theme, days=int(days))
                if fig_vol is not None:
                    st.plotly_chart(apply_plotly_theme(fig_vol, theme), use_container_width=True)
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                st.caption(
                    "Lectura diaria: NPS clásico (promotores - detractores) y % detractores."
                )
                fig_k = chart_daily_kpis(df_win, theme, days=int(days))
                if fig_k is not None:
                    st.plotly_chart(apply_plotly_theme(fig_k, theme), use_container_width=True)

                with st.expander("WoW: entender los días que importan (LLM)", expanded=False):
                    st.caption(
                        "Selecciona un día extremo (muy bueno o muy malo) y genera un prompt "
                        "para pedirle al GPT una explicación con hipótesis y acciones."
                    )

                    df_llm_win = load_context_df(
                        store_dir,
                        service_origin,
                        service_origin_n1,
                        service_origin_n2,
                        st.session_state.get("_nps_group_choice", POP_ALL),
                        CHART_COLUMNS["daily_llm"],
                        date_start=str(start_day.date()) if end_day is not None else None,
                        date_end=str(end_day.date()) if end_day is not None else None,
                    )
                    metrics = _daily_metrics(df_llm_win, days=int(days))
                    if metrics.empty:
                        st.info("No hay suficientes datos diarios para construir el asistente.")
                    else:
                        worst = metrics.sort_values(
                            ["det_pct", "n"], ascending=[False, False]
                        ).head(3)
                        best = metrics.sort_values(
                            ["classic_nps", "n"], ascending=[False, False]
                        ).head(3)
                        picks = []
                        for _, r in worst.iterrows():
                            picks.append(
                                (
                                    f"🔻 Peor día {r['day'].strftime('%Y-%m-%d')} — %detr={r['det_pct']:.1f} · NPS={r['classic_nps']:.1f} · n={int(r['n'])}",
                                    r["day"],
                                )
                            )
                        for _, r in best.iterrows():
                            picks.append(
                                (
                                    f"🔺 Mejor día {r['day'].strftime('%Y-%m-%d')} — %detr={r['det_pct']:.1f} · NPS={r['classic_nps']:.1f} · n={int(r['n'])}",
                                    r["day"],
                                )
                            )
                        labels = [p[0] for p in picks]
                        label = st.selectbox("Día a explicar", labels)
                        chosen_day = picks[labels.index(label)][1]

                        day_df = df.copy()
                        day_df["_day"] = day_df["Fecha"].dt.floor("D")
                        slice_df = day_df.loc[day_df["_day"] == chosen_day].copy()

                        # Small business facts for the chosen day
                        row = metrics.loc[metrics["day"] == chosen_day].head(1)
                        if row.empty:
                            st.warning("No se pudo preparar el día seleccionado.")
                        else:
                            rr = row.iloc[0]
                            # Verbative samples
                            verb = []
                            if "Comment" in slice_df.columns:
                                verb = slice_df["Comment"].dropna().astype(str).head(12).tolist()

                            # Top levers that day (if available)
                            tops = []
                            if "Palanca" in slice_df.columns:
                                vc = slice_df["Palanca"].astype(str).value_counts().head(5)
                                tops = [f"{idx} (n={int(v)})" for idx, v in vc.items()]

                            prompt = (
                                "Necesito que analices un día extremo de NPS térmico y me devuelvas:\n"
                                "1) Resumen del periodo (max 10 líneas)\n"
                                "2) JSON válido con el esquema de NPS Lens (schema_version=1.0)\n\n"
                                f"Contexto:\n- service_origin: {service_origin}\n- service_origin_n1: {service_origin_n1}\n- service_origin_n2: {service_origin_n2 or '-'}\n- día: {chosen_day.strftime('%Y-%m-%d')}\n\n"
                                "Hechos del día (métricas):\n"
                                f"- n: {int(rr['n'])}\n"
                                f"- % detractores (0-6): {rr['det_pct']:.1f}%\n"
                                f"- % pasivos (7-8): {rr['pas_pct']:.1f}%\n"
                                f"- % promotores (9-10): {rr['pro_pct']:.1f}%\n"
                                f"- NPS clásico (promotores - detractores): {rr['classic_nps']:.1f} pp\n\n"
                                "Hipótesis: explica qué pudo provocar este comportamiento (muy malo o muy bueno), "
                                "separa fricción digital vs operativa vs pricing si aplica, y propone acciones.\n\n"
                                "Palancas más presentes ese día (por volumen):\n"
                                + "\n".join([f"- {t}" for t in tops])
                                + "\n\n"
                                "Verbatims (muestras):\n"
                                + "\n".join([f"- {v}" for v in verb])
                                + "\n\n"
                                "Requisitos de respuesta:\n"
                                "- No inventes métricas.\n"
                                "- Si falta evidencia, dilo en riesgos y baja confidence.\n"
                                "- Incluye 3-5 acciones concretas con owner/eta.\n"
                            )

                            _clipboard_copy_widget(prompt, label="Copiar prompt del día")
                            st.download_button(
                                "Descargar prompt (md)",
                                data=prompt,
                                file_name=f"prompt_dia_{chosen_day.strftime('%Y%m%d')}.md",
                            )

        with tab_adv:
            days = st.slider(
                "Ventana (días)",
                min_value=14,
                max_value=120,
                value=60,
                step=7,
                key="ladder_days_adv",
                help=(
                    "Detalle avanzado con semáforo: rojo=detractores, amarillo=pasivos, verde=promotores. "
                    "La intensidad representa cuántas respuestas hubo ese día."
                ),
            )
            st.markdown(
                "<div class='nps-card nps-muted'>"
                "<b>Detalle semáforo:</b> cada columna es un día. "
                "Rojo=0-6, Amarillo=7-8, Verde=9-10. "
                "Más intenso = más respuestas ese día en esa categoría."
                "</div>",
                unsafe_allow_html=True,
            )

            # Same windowed load for the semáforo detail.
            if end_day is not None:
                df_sema = load_context_df(
                    store_dir,
                    service_origin,
                    service_origin_n1,
                    service_origin_n2,
                    st.session_state.get("_nps_group_choice", POP_ALL),
                    CHART_COLUMNS["daily_semaforo"],
                    date_start=str(start_day.date()),
                    date_end=str(end_day.date()),
                )
            else:
                df_sema = df

            fig2 = chart_daily_score_semaforo(df_sema, theme, days=int(days))
            if fig2 is None:
                st.info("No hay suficientes datos para construir la escalera.")
            else:
                st.plotly_chart(apply_plotly_theme(fig2, theme), use_container_width=True)

    with col_b:
        det = s.top_detractor_driver
        pro = s.top_promoter_driver
        card(
            "Lectura rápida",
            (
                "<ul style='margin:0; padding-left: 18px;'>"
                f"<li><b>Zona de fricción</b>: {det}</li>"
                f"<li><b>Zona fuerte</b>: {pro}</li>"
                "</ul>"
                "<div class='nps-muted' style='margin-top:10px;'>"
                "Siguiente paso: abre <b>Drivers & oportunidades</b> y prioriza por impacto."
                "</div>"
            ),
        )

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    section("Informe de negocio", "Copy/paste listo para comité / daily.")

    # Default time windows for a simple business comparison
    w_cur, w_base = default_windows(df, days=14)
    comp = None
    if w_cur is not None and w_base is not None:
        cur_df = slice_by_window(df, w_cur)
        base_df = slice_by_window(df, w_base)
        if len(cur_df) >= 50 and len(base_df) >= 50:
            comp = compare_periods(cur_df, base_df)

    # Reuse opportunity/topic narratives to populate the report
    opps = cached_rank_opportunities(df, min_n=200)
    opp_df = pd.DataFrame([o.__dict__ for o in opps])
    opp_bullets = explain_opportunities(opp_df, max_items=4) if not opp_df.empty else []

    comment_col = "Comment" if "Comment" in df.columns else "Comentario"
    topics = extract_topics(df[comment_col].astype(str), n_clusters=8)
    topics_df = pd.DataFrame([t.__dict__ for t in topics])
    topic_bullets = explain_topics(topics_df, max_items=5) if not topics_df.empty else []

    report_md = build_executive_story(
        summary=s,
        comparison=comp,
        top_opportunities=opp_bullets,
        top_topics=topic_bullets,
    )
    st.text_area("Informe de negocio", report_md, height=260)
    st.download_button(
        "Descargar informe .md",
        data=report_md.encode("utf-8"),
        file_name="informe_negocio_nps_lens.md",
        mime="text/markdown",
    )

    section("✨ Insights LLM integrados")
    _render_llm_insights(theme)


def page_comparisons(df: pd.DataFrame, theme: Theme) -> None:
    st.subheader("Comparativas (periodo actual vs periodo base)")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "Esta vista responde a una pregunta típica de negocio: <b>¿qué cambió?</b> "
        "Elige dos ventanas de tiempo y mira <b>qué palancas se deterioran o mejoran</b>."
        "</div>",
        unsafe_allow_html=True,
    )

    w_cur, w_base = default_windows(df, days=14)
    if w_cur is None or w_base is None:
        st.info("No hay columna de Fecha válida para hacer comparativas.")
        return

    c1, c2 = st.columns(2)
    with c1:
        cur_dates = st.date_input("Periodo actual", value=(w_cur.start, w_cur.end))
    with c2:
        base_dates = st.date_input("Periodo base", value=(w_base.start, w_base.end))

    # Streamlit can return a single date or a tuple
    if not isinstance(cur_dates, tuple) or not isinstance(base_dates, tuple):
        st.warning("Selecciona un rango (inicio y fin) para ambos periodos.")
        return

    cur_w = type(w_cur)(cur_dates[0], cur_dates[1])
    base_w = type(w_base)(base_dates[0], base_dates[1])
    cur_df = slice_by_window(df, cur_w)
    base_df = slice_by_window(df, base_w)

    comp = compare_periods(cur_df, base_df)
    st.markdown(
        "<div class='nps-card'>"
        f"<div><b>Periodo actual</b>: {comp.label_current} (n={comp.n_current:,})</div>"
        f"<div><b>Periodo base</b>: {comp.label_baseline} (n={comp.n_baseline:,})</div>"
        f"<div style='margin-top:6px'><b>Δ NPS</b>: {comp.delta_nps:+.2f} · "
        f"<b>Δ detractores</b>: {comp.delta_detr_pp:+.1f} pp</div>"
        "</div>",
        unsafe_allow_html=True,
    )

    section("Qué palancas cambian", "Deltas vs periodo base por dimensión seleccionada.")
    dim = st.selectbox("Dimensión", ["Palanca", "Subpalanca", "Canal", "UsuarioDecisión"], index=0)
    delta = driver_delta_table(cur_df, base_df, dimension=dim, min_n=50)
    if delta.empty:
        st.info(
            "No hay suficiente N para comparar en esa dimensión. "
            "Prueba ampliar la ventana o bajar min_n."
        )
        return
    fig = chart_driver_delta(delta, theme)
    if fig is not None:
        st.plotly_chart(apply_plotly_theme(fig, theme), use_container_width=True)
    with st.expander("Ver tabla de deltas", expanded=False):
        st.dataframe(delta.head(30), use_container_width=True)


def page_cohorts(df: pd.DataFrame, theme: Theme) -> None:
    st.subheader("Cohortes: dónde duele según segmento / usuario")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "La idea: no todos los usuarios viven lo mismo. "
        "Esta vista te ayuda a encontrar <b>bolsas de fricción</b> (cohortes) "
        "para priorizar acciones."
        "</div>",
        unsafe_allow_html=True,
    )

    row_dim = st.selectbox("Filas", ["Palanca", "Subpalanca", "Canal"], index=0)
    col_dim = st.selectbox("Columnas", ["UsuarioDecisión", "Segmento"], index=0)
    min_n = st.slider("Mínimo N por celda", 10, 200, 30, step=10)

    fig = chart_cohort_heatmap(df, theme, row_dim=row_dim, col_dim=col_dim, min_n=min_n)
    if fig is None:
        st.info(
            "No hay suficiente información para construir la matriz "
            "(revisa columnas y N mínimo)."
        )
        return
    st.plotly_chart(apply_plotly_theme(fig, theme), use_container_width=True)

    st.markdown(
        "<div class='nps-card'>"
        "<b>Cómo usar esto:</b> busca columnas con valores bajos de NPS de forma consistente. "
        "Eso suele indicar una fricción localizada (segmento/rol) "
        "y ayuda a afinar el plan de mejora."
        "</div>",
        unsafe_allow_html=True,
    )


def page_drivers(df: pd.DataFrame, theme: Theme, min_n: int) -> None:
    st.subheader("Drivers & oportunidades (lenguaje de negocio)")

    left, right = st.columns([1, 1])
    with left:
        dim = st.selectbox("Cortar por", ["Palanca", "Subpalanca", "Canal", "UsuarioDecisión"])
        stats_df = cached_driver_table(df, dimension=dim)

        section("Mayores gaps vs global", "Dónde el NPS se separa del global.")
        if stats_df.empty:
            st.info("No hay datos suficientes para calcular drivers.")
        else:
            # Biggest negative gaps first
            stats_df = stats_df.sort_values("gap_vs_overall", ascending=True)
            fig = chart_driver_bar(stats_df, theme)
            if fig is not None:
                st.plotly_chart(apply_plotly_theme(fig, theme), use_container_width=True)

        with st.expander("Ver tabla detallada"):
            st.dataframe(stats_df.head(30), use_container_width=True)

    with right:
        section(
            "Oportunidades priorizadas",
            "Ranking por impacto estimado x confianza (solo NPS en la dimensión seleccionada).",
        )
        opps = cached_rank_opportunities(df, min_n=min_n, dimensions=[dim])
        opp_df = pd.DataFrame([o.__dict__ for o in opps])

        if opp_df.empty:
            st.warning("No se detectaron oportunidades con el umbral actual.")
        else:
            # Mini chart: impact with confidence intensity (design tokens)
            try:
                from nps_lens.ui.charts import chart_opportunities_bar

                cfig = chart_opportunities_bar(
                    opp_df.assign(
                        label=lambda d: d.apply(lambda r: f"{r['dimension']}={r['value']}", axis=1)
                    ),
                    theme,
                    top_k=10,
                )
                if cfig is not None:
                    st.plotly_chart(apply_plotly_theme(cfig, theme), use_container_width=True)
            except Exception:
                # Never block the page on chart errors
                pass

            bullets = explain_opportunities(opp_df, max_items=5)
            st.markdown(
                (
                    "<div class='nps-card'><ul>"
                    + "".join([f"<li>{b}</li>" for b in bullets])
                    + "</ul></div>"
                ),
                unsafe_allow_html=True,
            )
            st.caption(
                "Nota de coherencia: este ranking usa solo NPS y la dimensión elegida. "
                "La PPT de incidencias usa hotspots operativos Helix+NPS (detractores, histórico completo), "
                "por lo que los términos pueden diferir."
            )

        with st.expander("Ver ranking completo"):
            st.dataframe(opp_df.head(25), use_container_width=True)


def page_text(df: pd.DataFrame, theme: Theme) -> None:
    st.subheader("Texto & temas: qué se repite y cómo suena")

    comment_col = "Comment" if "Comment" in df.columns else "Comentario"
    texts = df[comment_col].astype(str)

    topics = extract_topics(texts, n_clusters=10)
    topics_df = pd.DataFrame([t.__dict__ for t in topics])

    c1, c2 = st.columns([1, 1])
    with c1:
        section("Temas con más volumen", "Clusters de texto para entender fricciones.")
        fig = chart_topic_bars(topics_df, theme)
        if fig is None:
            st.info("No hay texto suficiente para extraer temas.")
        else:
            st.plotly_chart(apply_plotly_theme(fig, theme), use_container_width=True)

    with c2:
        section("Explicación en lenguaje natural", "Resumen de lo que significan los temas.")
        bullets = explain_topics(topics_df, max_items=6)
        st.markdown(
            (
                "<div class='nps-card'><ul>"
                + "".join([f"<li>{b}</li>" for b in bullets])
                + "</ul></div>"
            ),
            unsafe_allow_html=True,
        )

    with st.expander("Ver clusters (incluye ejemplos)"):
        st.dataframe(topics_df, use_container_width=True)


def _llm_select_opportunity(df: pd.DataFrame, min_n: int):
    opps = cached_rank_opportunities(df, min_n=min_n)
    if not opps:
        st.warning("No hay oportunidades con el umbral actual.")
        return None, None, None

    labels = [
        (
            f"{o.dimension}={o.value} | impacto~+{o.potential_uplift:.1f} | "
            f"conf~{o.confidence:.2f} | n={o.n}"
        )
        for o in opps[:40]
    ]
    choice = st.selectbox("Oportunidad priorizada", labels)
    selected = opps[labels.index(choice)]
    # Defensive slicing: labels may differ from raw df values by whitespace/casing.
    # If we fail to slice, the LLM section looks "empty" after selecting an opportunity.
    dim = str(selected.dimension)
    val = str(selected.value)
    if dim in df.columns:
        ser = df[dim].astype(str)
        slice_df = df.loc[ser.str.strip() == val.strip()].copy()
        if slice_df.empty:
            slice_df = df.loc[ser.str.strip().str.lower() == val.strip().lower()].copy()
    else:
        slice_df = df.iloc[0:0].copy()

    if slice_df.empty:
        st.warning(
            "No se encontraron filas para la oportunidad seleccionada tras normalizar valores. "
            "Se mostrará el prompt igualmente, pero el pack tendrá evidencia limitada."
        )
    return selected, slice_df, opps


def _llm_build_pack(
    df: pd.DataFrame,
    settings: Settings,
    selected,
    slice_df: pd.DataFrame,
    out_dir: Path,
):
    """Build the Deep-Dive Pack + files to support manual LLM workflow."""
    # Lazy import: LLM stack is heavy; only load when this page is opened.
    from nps_lens.llm.pack import build_insight_pack, export_pack, render_pack_markdown

    # If the slice is empty (data quality / labeling mismatch), fall back to a lightweight
    # sample so the user can still copy/paste a prompt and iterate.
    if slice_df is None or slice_df.empty:
        slice_df = df.head(0).copy()

    causal = best_effort_ate_logit(
        df=df,
        treatment_col=selected.dimension,
        treatment_value=selected.value,
        control_cols=["Canal", "Palanca", "Subpalanca"],
    )

    context = {
        "service_origin": str(settings.default_service_origin),
        "service_origin_n1": str(settings.default_service_origin_n1),
        "driver_dim": str(selected.dimension),
        "driver_val": str(selected.value),
    }
    title = f"Oportunidad priorizada: {selected.dimension}={selected.value}"
    driver = {"dimension": str(selected.dimension), "value": str(selected.value)}

    pack = build_insight_pack(
        title=title,
        context=context,
        nps_slice=slice_df,
        driver=driver,
        causal=causal,
        examples=10,
    )
    out = export_pack(pack, out_dir=out_dir)
    md = render_pack_markdown(pack)
    return md, out, pack, context


def _llm_render_copy_prompt(md: str, out: dict[str, Path]) -> None:
    section(
        "1) Copiar prompt para el LLM",
        "Elige una pregunta (lenguaje de negocio) y copia el prompt. Pégalo en tu ChatGPT y vuelve para pegar la respuesta.",
    )

    question = st.selectbox(
        "Pregunta para el LLM",
        options=LLM_BUSINESS_QUESTIONS,
        index=0,
        help="El prompt copiado incluirá esta pregunta + el pack con evidencia.",
    )

    # IMPORTANT: Force JSON-only output so users can paste directly into the app.
    prompt = (
        "SISTEMA (instrucciones del analista)\n"
        f"{LLM_SYSTEM_PROMPT}\n\n"
        "INSTRUCCIONES\n"
        f"- {question}\n\n"
        "REGLA CRITICA\n"
        "- RESPONDE SOLO con UN objeto JSON valido (sin texto antes o despues, sin markdown).\n"
        '- Usa comillas dobles normales ("), sin comillas tipograficas.\n'
        "- Sin trailing commas. No uses NaN/Infinity/None: usa null si aplica.\n\n"
        "DEEP-DIVE PACK\n"
        f"{md}"
    )

    # IMPORTANT UX:
    # Some corporate browsers / Streamlit hosting environments may block JS clipboard APIs or
    # even hide HTML components. Therefore we always render the prompt in a plain text widget
    # (copyable via Ctrl/Cmd+C), and optionally also show the JS copy button.
    c1, c2 = st.columns([2, 1])
    with c1:
        with contextlib.suppress(Exception):
            _clipboard_copy_widget(prompt, label="Copiar prompt")

        st.text_area(
            "Prompt (copia y pega en tu ChatGPT)",
            value=prompt,
            height=260,
            help="Selecciona el texto y usa Ctrl/Cmd+C para copiar.",
        )

        with st.expander("Ver prompt en bloque de código", expanded=False):
            # Streamlit's code blocks often include a built-in copy icon.
            st.code(prompt)

    with c2:
        st.download_button(
            "Descargar pack .md",
            data=md.encode("utf-8"),
            file_name=out["md"].name,
            mime="text/markdown",
            use_container_width=True,
        )


def _llm_render_paste_and_parse(default_text: str) -> tuple[str, Optional[dict[str, Any]]]:
    st.divider()
    st.subheader("Pegar respuesta del LLM y guardarla en Knowledge Cache")

    section("2) Pega el insight del LLM para integrarlo en la narrativa")
    st.markdown(
        "<div class='nps-card nps-card--flat'>"
        "<b>Como usarlo</b><br/>"
        "<span class='nps-muted'>Pega aqui la respuesta del LLM (idealmente el JSON "
        "con el esquema de Insight). Al guardarlo, aparecera en <b>Resumen</b> "
        "y se incluira en el briefing exportable.</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    # IMPORTANT: Do not pass `value=` on every rerun.
    # If we always supply a `value`, Streamlit will overwrite user edits on each rerun
    # (paste appears to do nothing, which feels like a disabled input).
    # We persist the value via session_state using `key=`.
    # If we repaired JSON on a previous run, apply it *before* the widget is instantiated.
    # Streamlit forbids mutating a widget's session_state key after the widget exists.
    if "llm_answer_pending" in st.session_state:
        st.session_state["llm_answer"] = str(st.session_state.pop("llm_answer_pending") or "")

    if "llm_answer" not in st.session_state:
        st.session_state["llm_answer"] = default_text or ""
    elif (default_text or "") and not str(st.session_state.get("llm_answer", "")).strip():
        # If the user hasn't typed anything yet, allow a fresh default to populate.
        st.session_state["llm_answer"] = default_text

    st.text_area(
        "Respuesta del LLM",
        key="llm_answer",
        height=240,
        help=(
            "Pega aqui la respuesta del LLM (idealmente el JSON con el esquema de Insight). "
            "La app la analizara y podras guardarla en la Knowledge Cache."
        ),
    )
    answer = str(st.session_state.get("llm_answer", ""))
    parsed = _try_parse_json(answer)

    if parsed is not None:
        ok, errs = _validate_insight_schema(parsed)
        if ok:
            with st.expander("Vista previa del insight detectado", expanded=True):
                st.write(
                    {
                        "insight_id": parsed.get("insight_id"),
                        "title": parsed.get("title"),
                        "confidence": parsed.get("confidence"),
                        "severity": parsed.get("severity"),
                        "tags": parsed.get("tags"),
                    }
                )
                st.caption(str(parsed.get("executive_summary", ""))[:600])
        else:
            st.info("Se detectó JSON, pero aún no cumple el esquema: " + "; ".join(errs))
    elif answer.strip():
        st.info("Pega aquí el JSON del LLM. Se validará automáticamente al guardar.")

    return answer, parsed


def _llm_actions_row() -> bool:
    """Single action: save for dashboard + knowledge cache.

    This eliminates user confusion and guarantees coherence between the executive
    dashboard narrative and the persisted knowledge cache for the current context.
    """

    return st.button(
        "Guardar (dashboard + knowledge cache)",
        type="primary",
        use_container_width=True,
        help=(
            "Guarda el insight para que aparezca en Resumen y quede persistido "
            "en la knowledge cache del contexto."
        ),
    )


def _llm_add_to_dashboard(parsed: Optional[dict[str, Any]], *, rerun: bool = False) -> None:
    if parsed is None:
        st.error("Pega un JSON valido para poder integrarlo en el dashboard.")
        return

    ok, errs, norm = validate_insight_response(parsed)
    if not ok or norm is None:
        st.error("El JSON no cumple el esquema: " + "; ".join(errs))
        return

    insights = list(st.session_state.get("llm_insights", []))
    insights = [i for i in insights if i.get("insight_id") != norm.get("insight_id")]
    insights.insert(0, norm)
    st.session_state["llm_insights"] = insights[:20]
    st.success("Listo. Ya forma parte del discurso en Resumen.")
    if rerun:
        st.rerun()


def _llm_save_to_cache(
    cache_path: Path,
    context: dict[str, Any],
    pack,
    answer: str,
    settings: Settings,
    selected,
) -> None:
    from nps_lens.llm.knowledge_cache import KnowledgeCache, stable_signature

    service_origin = str(context.get("service_origin") or settings.default_service_origin)
    service_origin_n1 = str(context.get("service_origin_n1") or settings.default_service_origin_n1)
    kc = KnowledgeCache.for_context(
        settings.knowledge_dir, service_origin=service_origin, service_origin_n1=service_origin_n1
    )
    sig = stable_signature(context=context, title=pack.title)
    record = {
        "signature": sig,
        "insight_id": pack.insight_id,
        "title": pack.title,
        "context": context,
        "llm_answer": answer,
        "created_at_utc": pack.created_at.isoformat() + "Z",
        "tags": [
            service_origin,
            service_origin_n1,
            selected.dimension,
            selected.value,
        ],
    }
    kc.upsert(sig, record)
    st.success("Guardado. Se usara para deduplicacion y contexto futuro.")


def page_llm(df: pd.DataFrame, settings: Settings, min_n: int, cache_path: Path) -> None:
    st.subheader("WoW: Deep-Dive Pack para ChatGPT (copy/paste + memoria)")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "Selecciona una oportunidad, genera un pack con contexto + evidencia y llevalo a tu LLM. "
        "Despues pega la respuesta aqui para que la app recuerde decisiones y no repita insights."
        "</div>",
        unsafe_allow_html=True,
    )

    selected, slice_df, _ = _llm_select_opportunity(df, min_n=min_n)
    if selected is None or slice_df is None:
        return

    md, out, pack, context = _llm_build_pack(
        df, settings, selected, slice_df, out_dir=cache_path.parent / "packs"
    )
    _llm_render_copy_prompt(md, out)

    answer, parsed = _llm_render_paste_and_parse("")
    do_save = _llm_actions_row()

    if do_save:
        raw = str(st.session_state.get("llm_answer", ""))
        obj, repaired, err = _parse_json_with_repair(raw)

        if obj is None:
            st.error("No pude reparar/detectar un JSON válido automáticamente.")
            with st.expander("Validador JSON (detalle técnico)", expanded=True):
                st.write(err or "JSON inválido.")
                if repaired:
                    st.caption("Intento de reparación (lo que la app intentó parsear):")
                    st.code(repaired, language="json")
            return

        # If we repaired anything, schedule the textbox update for the next rerun.
        # (We cannot modify st.session_state['llm_answer'] after the widget is instantiated.)
        if repaired and repaired.strip() and repaired.strip() != raw.strip():
            st.session_state["llm_answer_pending"] = repaired

        ok, errs = _validate_insight_schema(obj)
        if not ok:
            st.error("El JSON se pudo parsear, pero no cumple el esquema: " + "; ".join(errs))
            with st.expander("JSON detectado (reparado)", expanded=False):
                st.code(repaired or "", language="json")
            return

        # 1) Dashboard (session)
        _llm_add_to_dashboard(obj, rerun=False)

        # 2) Knowledge cache (persisted, per-context) - store the repaired canonical JSON
        _llm_save_to_cache(
            cache_path=cache_path,
            context=context,
            pack=pack,
            answer=repaired or raw,
            settings=settings,
            selected=selected,
        )

        # Apply the repaired JSON back into the text area and refresh.
        st.rerun()
        # Refresh UI so the integrated insights section updates immediately.
        st.rerun()


def _normalize_empty_n2(n2: str) -> str:
    v = str(n2 or "").strip()
    if v in {"-", "—", "–"}:
        return ""
    return v


NPS_GROUP_OPTIONS = ["Todos", "Detractores", "Neutros", "Promotores"]


def _norm_group_value(v: object) -> str:
    return str(v or "").strip().lower()


def filter_df_by_nps_group(df: pd.DataFrame, group_choice: str) -> pd.DataFrame:
    """Filter NPS dataframe by selected population.

    group_choice values: Todos | Detractores | Neutros | Promotores.
    Robust to Spanish/English labels in the dataset.
    """
    choice = (group_choice or "Todos").strip().lower()
    if choice == "todos":
        return df
    if "NPS Group" not in df.columns:
        return df

    target = {
        "detractores": "detractor",
        "neutros": "passive",
        "promotores": "promoter",
    }.get(choice)
    if target is None:
        return df

    def _bucket(val: object) -> str:
        s = _norm_group_value(val)
        if "det" in s or "detr" in s:
            return "detractor"
        if "pas" in s or "neut" in s or "pass" in s:
            return "passive"
        if "pro" in s or "promo" in s:
            return "promoter"
        return "unknown"

    mask = df["NPS Group"].apply(_bucket) == target
    return df.loc[mask].copy()


def page_quality(df: pd.DataFrame, helix_df: Optional[pd.DataFrame] = None) -> None:
    st.subheader("Datos & calidad")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "Esta sección es técnica, pero útil cuando los números no cuadran: "
        "faltantes, duplicados y columnas clave."
        "</div>",
        unsafe_allow_html=True,
    )

    tabs = st.tabs(["NPS", "Helix"] if helix_df is not None else ["NPS"])

    with tabs[0]:
        st.caption(f"Filas: {len(df):,} · Columnas: {len(df.columns)}")

        c1, c2 = st.columns([1, 1])
        with c1:
            show_full = st.toggle(
                "Mostrar dataset completo",
                value=False,
                help="Por rendimiento, por defecto se muestra una muestra. "
                "Actívalo si quieres ver todas las filas (puede tardar).",
            )
        with c2:
            sample_n = st.selectbox(
                "Tamaño de muestra",
                options=[50, 100, 200, 500, 1000],
                index=2,
                disabled=show_full,
                help="Número de filas a mostrar cuando no está activado el dataset completo.",
            )

        view_df = df if show_full else df.head(int(sample_n))
        st.dataframe(view_df, use_container_width=True, height=520)

        st.caption(
            "Nota: la tabla es desplazable. Si activas el dataset completo, "
            "Streamlit renderiza una vista virtualizada: verás todas las filas al hacer scroll."
        )

    if helix_df is not None:
        with tabs[1]:
            st.caption(f"Filas: {len(helix_df):,} · Columnas: {len(helix_df.columns)}")
            c1, c2 = st.columns([1, 1])
            with c1:
                show_full_h = st.toggle(
                    "Mostrar Helix completo",
                    value=False,
                    help="Por rendimiento, por defecto se muestra una muestra.",
                    key="helix_show_full",
                )
            with c2:
                sample_n_h = st.selectbox(
                    "Tamaño de muestra Helix",
                    options=[50, 100, 200, 500, 1000],
                    index=1,
                    disabled=show_full_h,
                    key="helix_sample_n",
                )
            view_h = helix_df if show_full_h else helix_df.head(int(sample_n_h))
            st.dataframe(view_h, use_container_width=True, height=520)


def page_nps_helix_linking(
    nps_df: pd.DataFrame,
    store_dir: Path,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    nps_group_choice: str,
    settings: Settings,
    theme_mode: str,
    touchpoint_source: str,
) -> None:
    st.subheader("🔗 NPS ↔ Helix — causalidad pragmática (multi-fuente)")

    # Use the global app theme for any Plotly figures built directly in this page.
    theme = get_theme(theme_mode)
    touchpoint_source_label = {
        TOUCHPOINT_SOURCE_DOMAIN: "Lógica actual de dominio",
        TOUCHPOINT_SOURCE_HELIX_N2: "N2 final asignado en Helix",
        TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: "Journeys ejecutivos de detracción",
    }.get(touchpoint_source, "Lógica actual de dominio")
    st.caption(f"Modo causal activo: {touchpoint_source_label}.")

    # IMPORTANT: context is only used to load the already-ingested population.
    # Once persisted, analysis should *not* re-filter by service origin / N1 / N2 again.
    helix_store = HelixIncidentStore(settings.data_dir / "helix")
    hctx = DatasetContext(
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        service_origin_n2=_normalize_empty_n2(service_origin_n2),
    )
    hstored = helix_store.get(hctx)
    if hstored is None:
        st.info(
            "No hay incidencias Helix persistidas para este contexto. Súbelas en la barra lateral para activar el análisis."
        )
        return

    helix_df = helix_store.load_df(hstored)

    if helix_df.empty:
        st.warning("No hay incidencias Helix persistidas para este contexto.")
        return

    # Ventana temporal: usa el rango del NPS cargado (y deja afinar).
    nps_df["Fecha"] = pd.to_datetime(nps_df["Fecha"], errors="coerce")
    dmin = pd.to_datetime(nps_df["Fecha"].min()).date()
    dmax = pd.to_datetime(nps_df["Fecha"].max()).date()
    colw = st.columns(2)
    with colw[0]:
        start = st.date_input("Desde", value=dmin, min_value=dmin, max_value=dmax, key="nh_start")
    with colw[1]:
        end = st.date_input("Hasta", value=dmax, min_value=dmin, max_value=dmax, key="nh_end")
    min_sim = float(LINK_MIN_SIMILARITY)

    # Población global (sidebar): rige TODO el contenido de esta pestaña.
    st.caption(f"Población activa: **{nps_group_choice}** (control global en la barra lateral)")

    choice_norm = (nps_group_choice or "Todos").strip().lower()
    show_all_groups = choice_norm == "todos"

    focus_group = {
        "detractores": "detractor",
        "neutros": "passive",
        "promotores": "promoter",
    }.get(choice_norm, "detractor")

    nps_slice = nps_df[(nps_df["Fecha"].dt.date >= start) & (nps_df["Fecha"].dt.date <= end)].copy()

    # Global population filter (sidebar). This governs all analysis inside this page.
    focus_population = filter_nps_by_group(nps_slice, nps_group_choice)
    helix_df = helix_df.copy()
    # Canonical date col for helix store is 'Fecha' (set in ingestion); if missing, fallback to bbva_closeddate/startdatetime.
    if "Fecha" not in helix_df.columns:
        for c in ["bbva_closeddate", "bbva_startdatetime", "Submit Date", "Last Modified Date"]:
            if c in helix_df.columns:
                # Helix/API extracts may encode timestamps as Unix epoch milliseconds.
                ser = helix_df[c]
                dt = pd.to_datetime(ser, errors="coerce")
                if float(dt.notna().mean()) < 0.4:
                    num = pd.to_numeric(ser, errors="coerce")
                    if float(num.notna().mean()) >= 0.6:
                        med = float(num.dropna().median())
                        if med >= 1e12:
                            dt = pd.to_datetime(num, unit="ms", errors="coerce")
                        elif med >= 1e9:
                            dt = pd.to_datetime(num, unit="s", errors="coerce")
                helix_df["Fecha"] = dt
                break
    else:
        # If already present but poorly parsed (common when epoch-ms was ingested as object), attempt epoch recovery.
        dt = pd.to_datetime(helix_df["Fecha"], errors="coerce")
        if float(dt.notna().mean()) < 0.4 and "Submit Date" in helix_df.columns:
            ser = helix_df["Submit Date"]
            num = pd.to_numeric(ser, errors="coerce")
            if float(num.notna().mean()) >= 0.6:
                med = float(num.dropna().median())
                if med >= 1e12:
                    dt = pd.to_datetime(num, unit="ms", errors="coerce")
                elif med >= 1e9:
                    dt = pd.to_datetime(num, unit="s", errors="coerce")
        helix_df["Fecha"] = dt

    helix_slice = helix_df[
        (helix_df["Fecha"].dt.date >= start) & (helix_df["Fecha"].dt.date <= end)
    ].copy()

    if nps_slice.empty:
        st.warning("No hay respuestas NPS en el rango seleccionado.")
        return
    if helix_slice.empty:
        st.warning("No hay incidencias Helix en el rango seleccionado.")
        return

    # Grupo foco para el linking semántico (por defecto: detractores)
    nps_slice["NPS Group"] = nps_slice.get("NPS Group", "").astype(str)
    score = pd.to_numeric(nps_slice.get("NPS", np.nan), errors="coerce")
    grp = nps_slice["NPS Group"].str.upper()
    if focus_group == "promoter":
        mask_focus = (grp == "PROMOTER") | (score >= 9)
        focus_name = "promotores"
    elif focus_group == "passive":
        mask_focus = (grp == "PASSIVE") | ((score >= 7) & (score <= 8))
        focus_name = "pasivos"
    else:
        mask_focus = (grp == "DETRACTOR") | (score <= 6)
        focus_name = "detractores"

    focus_df = nps_slice[mask_focus].copy()
    if focus_df.empty:
        st.info(
            f"No hay {focus_name} en el rango seleccionado. El linking semántico se activa cuando existan registros de ese grupo."
        )
    else:
        st.caption(
            "El análisis usa: (1) contexto determinista, (2) ventana temporal, (3) mapping semántico de incidencias a tópicos NPS, "
            "y (4) evidencia (links) detractor↔incidencia con similitud."
        )

    # 1) Linking + asignación de incidencias a tópico NPS
    assign_df, links_df = link_incidents_to_nps_topics(
        focus_df,
        helix_slice,
        min_similarity=float(min_sim),
        top_k_per_incident=int(LINK_TOP_K_PER_INCIDENT),
        max_days_apart=int(LINK_MAX_DAYS_APART),
    )
    overall_weekly, by_topic_weekly = weekly_aggregates(
        nps_slice, helix_slice, assign_df, focus_group=focus_group
    )
    overall_daily, by_topic_daily = daily_aggregates(
        nps_slice, helix_slice, assign_df, focus_group=focus_group
    )

    # Design tokens (Plotly colors)
    dtokens = DesignTokens.default()
    pal = palette(dtokens, theme_mode)
    # Continuous scales aligned to design tokens
    risk_scale = plotly_risk_scale(dtokens, theme_mode)
    view_mode = st.radio(
        "Navegación de la sesión",
        options=[
            "1) Situación del periodo",
            "2) Priorización y palancas",
            "3) Narrativa y presentación",
            "4) Evidencia y paquete GPT",
        ],
        horizontal=True,
        key="nh_view_mode",
    )
    show_overview = view_mode.startswith("1)")
    show_priorities = view_mode.startswith("2)")
    show_ppt = view_mode.startswith("3)")
    show_evidence = view_mode.startswith("4)")
    lag_days = pd.DataFrame()

    # 2) Timeline causal (global)
    if show_overview:
        st.markdown("### Situación del periodo")
        trend_grain = st.radio(
            "Granularidad de tendencia",
            options=["Diaria", "Semanal"],
            index=0,
            horizontal=True,
            key="nh_timeline_grain",
            help="Vista diaria por defecto para capturar tendencias y puntos de inflexión con más detalle.",
        )
        use_daily_trend = trend_grain == "Diaria"
        trend_df = overall_daily if use_daily_trend and not overall_daily.empty else overall_weekly

        k1, k2, k3 = st.columns(3)
        with k1:
            kpi(
                "Respuestas analizadas",
                f"{int(pd.to_numeric(trend_df.get('responses', 0), errors='coerce').fillna(0).sum()):,}",
            )
        with k2:
            kpi(
                "Incidencias del periodo",
                f"{int(pd.to_numeric(trend_df.get('incidents', 0), errors='coerce').fillna(0).sum()):,}",
            )
        with k3:
            avg_focus = float(
                pd.to_numeric(trend_df.get("focus_rate", 0.0), errors="coerce").fillna(0.0).mean()
            )
            kpi(f"% {focus_name} medio", f"{avg_focus * 100.0:.2f}%")

        st.markdown(f"### Timeline causal ({trend_grain.lower()})")
        px, go = _plotly()
        fig = go.Figure()
        if show_all_groups:
            # Show the 3 group rates on the same plot (comparison mode).
            empty_assign = pd.DataFrame(columns=["incident_id", "nps_topic"])
            if use_daily_trend and not overall_daily.empty:
                ow_det, _ = daily_aggregates(
                    nps_slice, helix_slice, empty_assign, focus_group="detractor"
                )
                ow_pas, _ = daily_aggregates(
                    nps_slice, helix_slice, empty_assign, focus_group="passive"
                )
                ow_pro, _ = daily_aggregates(
                    nps_slice, helix_slice, empty_assign, focus_group="promoter"
                )
                x_col = "date"
            else:
                ow_det, _ = weekly_aggregates(
                    nps_slice, helix_slice, empty_assign, focus_group="detractor"
                )
                ow_pas, _ = weekly_aggregates(
                    nps_slice, helix_slice, empty_assign, focus_group="passive"
                )
                ow_pro, _ = weekly_aggregates(
                    nps_slice, helix_slice, empty_assign, focus_group="promoter"
                )
                x_col = "week"
            fig.add_trace(
                go.Scatter(
                    x=ow_det[x_col],
                    y=ow_det["focus_rate"],
                    name="% detractores",
                    mode="lines+markers",
                    line=dict(color=pal["color.primary.bg.alert"], width=2),
                    marker=dict(color=pal["color.primary.bg.alert"], size=6),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=ow_pas[x_col],
                    y=ow_pas["focus_rate"],
                    name="% pasivos",
                    mode="lines+markers",
                    line=dict(color=pal["color.primary.bg.warning"], width=2),
                    marker=dict(color=pal["color.primary.bg.warning"], size=6),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=ow_pro[x_col],
                    y=ow_pro["focus_rate"],
                    name="% promotores",
                    mode="lines+markers",
                    line=dict(color=pal["color.primary.bg.success"], width=2),
                    marker=dict(color=pal["color.primary.bg.success"], size=6),
                )
            )
            bar_x = (
                trend_df["date"]
                if use_daily_trend and "date" in trend_df.columns
                else trend_df["week"]
            )
        else:
            if use_daily_trend and "date" in trend_df.columns:
                dline = trend_df.sort_values("date").copy()
                dline["focus_rate_smooth"] = (
                    pd.to_numeric(dline["focus_rate"], errors="coerce")
                    .fillna(0.0)
                    .rolling(7, min_periods=1)
                    .mean()
                )
                line_mode = "lines" if len(dline) > 90 else "lines+markers"
                fig.add_trace(
                    go.Scatter(
                        x=dline["date"],
                        y=dline["focus_rate"],
                        name=f"% {focus_name} (diario)",
                        mode=line_mode,
                        line=dict(color=pal["color.primary.accent.value-07.default"], width=1.5),
                        marker=dict(color=pal["color.primary.accent.value-07.default"], size=5),
                        opacity=0.45,
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=dline["date"],
                        y=dline["focus_rate_smooth"],
                        name=f"% {focus_name} (media 7d)",
                        mode="lines",
                        line=dict(color=pal["color.primary.accent.value-07.default"], width=3),
                    )
                )
                bar_x = dline["date"]
            else:
                bar_x = trend_df["week"]
                line_mode = "lines+markers"
                fig.add_trace(
                    go.Scatter(
                        x=trend_df["week"],
                        y=trend_df["focus_rate"],
                        name=f"% {focus_name}",
                        mode=line_mode,
                        line=dict(color=pal["color.primary.accent.value-07.default"], width=2),
                        marker=dict(color=pal["color.primary.accent.value-07.default"], size=6),
                    )
                )
        fig.add_trace(
            go.Bar(
                x=bar_x,
                y=trend_df["incidents"],
                name="# incidencias",
                yaxis="y2",
                opacity=0.75,
                marker=dict(color=pal["color.primary.accent.value-01.default"]),
            )
        )
        fig.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=10, b=10),
            yaxis=dict(
                title=("Tasa por grupo" if show_all_groups else f"% {focus_name}"),
                tickformat=".0%",
            ),
            yaxis2=dict(title="Incidencias", overlaying="y", side="right"),
            legend=dict(orientation="h"),
        )
        st.plotly_chart(apply_plotly_theme(fig, theme), use_container_width=True)
        if use_daily_trend and "date" in trend_df.columns:
            st.caption(
                "La línea principal usa media móvil de 7 días para resaltar tendencia sin perder el detalle diario."
            )

    # 3) Ranking causal por tópico NPS
    if show_priorities:
        st.markdown("### Ranking de hipótesis causal (tópicos NPS)")
    rank = causal_rank_by_topic(by_topic_weekly)
    # 3.1) Changepoints en detracción por tópico + lag (incidencias preceden X semanas)
    # Changepoints con estabilidad (bootstrap) para etiquetar alto/medio/bajo
    cp_by_topic = detect_detractor_changepoints_with_bootstrap(
        by_topic_weekly,
        pen=6.0,
        n_boot=200,
        block_size=2,
        tol_periods=1,
    )
    lag_by_topic = estimate_best_lag_by_topic(by_topic_weekly, max_lag_weeks=6)
    lead_share = incidents_lead_changepoints_flag(by_topic_weekly, cp_by_topic, window_weeks=4)

    # 3.2) Knowledge Cache (aprendizaje incremental): boost/penalización por confirmaciones previas
    kc_entries = kc_load_entries(settings.knowledge_dir)
    kc_adj = kc_score_adjustments(kc_entries, service_origin, service_origin_n1, service_origin_n2)

    if not rank.empty:

        # Enriquecer ranking con changepoints + lag + learning
        rank2 = (
            rank.merge(cp_by_topic, on="nps_topic", how="left")
            .merge(lag_by_topic, on="nps_topic", how="left")
            .merge(lead_share, on="nps_topic", how="left")
        )
        if not kc_adj.empty:
            rank2 = rank2.merge(kc_adj, on="nps_topic", how="left")
        else:
            rank2["factor"] = 1.0
            rank2["confirmed"] = 0
            rank2["rejected"] = 0
        rank2["factor"] = rank2.get("factor", 1.0).fillna(1.0).astype(float)
        rank2["confidence_learned"] = (rank2["score"].astype(float) * rank2["factor"]).clip(0, 1)
        rank2 = rank2.sort_values(
            ["confidence_learned", "incidents", "responses"], ascending=False
        ).reset_index(drop=True)

        show = rank2.copy()

        show["focus_rate"] = (show["focus_rate"] * 100).round(2)
        show["delta_focus_rate"] = (show["delta_focus_rate"] * 100).round(2)
        show["confidence_learned"] = show["confidence_learned"].round(3)
        show["factor"] = show["factor"].round(3)
        show["corr"] = show["corr"].round(3)
        show["incidents_lead_changepoint_share"] = (
            show["incidents_lead_changepoint_share"] * 100
        ).round(0)
        show["score"] = show["score"].round(3)
        show["max_cp_stability"] = show.get("max_cp_stability", np.nan).astype(float).round(3)
        if show_priorities:
            st.dataframe(
                show[
                    [
                        "nps_topic",
                        "confidence_learned",
                        "score",
                        "factor",
                        "confirmed",
                        "rejected",
                        "best_lag_weeks",
                        "corr",
                        "incidents_lead_changepoint_share",
                        "max_cp_level",
                        "max_cp_stability",
                        "changepoints",
                        "incidents",
                        "responses",
                        "focus_rate",
                        "delta_focus_rate",
                        "weeks",
                    ]
                ].rename(
                    columns={
                        "nps_topic": "Tópico NPS",
                        "confidence_learned": "Confidence (learned)",
                        "score": "Confidence (raw)",
                        "factor": "Learning factor",
                        "confirmed": "✓ Confirmed",
                        "rejected": "✗ Rejected",
                        "best_lag_weeks": "Lag (semanas)",
                        "corr": "Corr@Lag",
                        "incidents_lead_changepoint_share": "Incidencias→CP (share)",
                        "max_cp_level": "CP Significance",
                        "max_cp_stability": "CP Stability",
                        "changepoints": "Changepoints",
                        "incidents": "Incidencias (asignadas)",
                        "responses": "Respuestas",
                        "focus_rate": f"% {focus_name.capitalize()}",
                        "delta_focus_rate": f"Δ % {focus_name.capitalize()} (high-inc vs low-inc)",
                        "weeks": "Semanas",
                    }
                ),
                use_container_width=True,
                height=320,
            )
            # Bar chart top 15
            topn = show.head(15).copy()
            px, go = _plotly()
            topn["rank"] = np.arange(1, len(topn) + 1)
            topn["topic_label"] = topn.apply(
                lambda r: (
                    f"TOP {int(r['rank'])} · {r['nps_topic']}"
                    if int(r["rank"]) <= 3
                    else str(r["nps_topic"])
                ),
                axis=1,
            )
            topn["topic_label"] = topn["topic_label"].astype(str).str.slice(0, 72)
            topn_plot = topn[::-1].copy()
            colors = []
            for rk in topn_plot["rank"].tolist():
                if int(rk) == 1:
                    colors.append(pal["color.primary.bg.alert"])
                elif int(rk) == 2:
                    colors.append(pal["color.primary.bg.warning"])
                elif int(rk) == 3:
                    colors.append(pal["color.primary.bg.success"])
                else:
                    colors.append(
                        pal.get(
                            "color.neutral.bg.01",
                            pal.get("color.primary.bg.bar", "#CAD1D8"),
                        )
                    )
            fig2 = go.Figure()
            fig2.add_trace(
                go.Bar(
                    x=topn_plot["confidence_learned"],
                    y=topn_plot["topic_label"],
                    orientation="h",
                    marker=dict(color=colors),
                    text=[f"{float(v):.2f}" for v in topn_plot["confidence_learned"].tolist()],
                    textposition="outside",
                    hovertemplate=("Tópico=%{y}<br>confidence learned=%{x:.2f}<extra></extra>"),
                )
            )
            fig2.update_layout(
                height=440,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis=dict(range=[0, 1], title="confidence learned"),
                yaxis=dict(title="Tópicos trending"),
            )
            st.plotly_chart(apply_plotly_theme(fig2, theme), use_container_width=True)
    elif show_priorities:
        st.info("No hay suficiente señal para rankear tópicos (prueba con un rango más amplio).")

    # 3.3) Racional de negocio: incidencias -> riesgo NPS -> recuperación + plan
    rank_for_rationale = rank2 if "rank2" in locals() and not rank2.empty else rank
    rationale_df = build_incident_nps_rationale(
        by_topic_weekly,
        focus_group=focus_group,
        rank_df=rank_for_rationale,
        min_topic_responses=80,
        recovery_factor=0.65,
    )
    rationale_summary = summarize_incident_nps_rationale(rationale_df)
    chain_candidates_df = build_incident_attribution_chains(
        links_df,
        focus_df,
        helix_slice,
        rationale_df=rationale_df,
        top_k=0,
        max_incident_examples=0,
        max_comment_examples=0,
        min_links_per_topic=1,
        touchpoint_source=touchpoint_source,
    )
    chain_candidates_df = _annotate_chain_candidates(chain_candidates_df)
    selected_chain_keys = _sync_chain_selection_state(
        chain_candidates_df,
        key_prefix="nh_chain_candidates",
        default_limit=3,
    )
    chain_df = _cap_chain_evidence_rows(
        _select_chain_rows(chain_candidates_df, selected_chain_keys),
        max_incident_examples=5,
        max_comment_examples=2,
    )
    linked_topics_total = (
        int(
            links_df.get("nps_topic", pd.Series(dtype=str))
            .astype(str)
            .str.strip()
            .replace("", np.nan)
            .dropna()
            .nunique()
        )
        if links_df is not None and not links_df.empty
        else 0
    )
    assigned_incidents_total = (
        int(
            assign_df.get("incident_id", pd.Series(dtype=str))
            .astype(str)
            .str.strip()
            .replace("", np.nan)
            .dropna()
            .nunique()
        )
        if assign_df is not None and not assign_df.empty
        else 0
    )
    linked_pairs_total = (
        int(len(links_df[["incident_id", "nps_id"]].drop_duplicates()))
        if links_df is not None
        and not links_df.empty
        and {"incident_id", "nps_id"}.issubset(set(links_df.columns))
        else 0
    )
    linked_comments_total = (
        int(
            links_df.get("nps_id", pd.Series(dtype=str))
            .astype(str)
            .str.strip()
            .replace("", np.nan)
            .dropna()
            .nunique()
        )
        if links_df is not None and not links_df.empty
        else 0
    )
    ppt_story_md = (
        build_incident_ppt_story(
            rationale_summary,
            rationale_df,
            attribution_df=chain_df,
            focus_name=focus_name,
            top_k=6,
        )
        if not rationale_df.empty
        else ""
    )
    period_label = f"{start} -> {end}"
    ppt_8slides_md = build_ppt_8slide_script(
        rationale_summary,
        rationale_df,
        attribution_df=chain_df,
        touchpoint_source=touchpoint_source,
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        focus_name=focus_name,
        period_label=period_label,
        top_k=6,
    )

    if show_priorities or show_ppt:
        mode_label_map = {
            TOUCHPOINT_SOURCE_DOMAIN: "Touchpoint operativo actual",
            TOUCHPOINT_SOURCE_HELIX_N2: "Touchpoint N2 asignado en Helix",
            TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: "Journeys ejecutivos de detracción",
        }
        mode_summary_map = {
            TOUCHPOINT_SOURCE_DOMAIN: "La lectura une incidencia operativa, touchpoint afectado, palanca NPS y voz del cliente con el vocabulario actual del dominio.",
            TOUCHPOINT_SOURCE_HELIX_N2: "La lectura causal se apoya en el touchpoint final asignado en Helix para evitar vacíos o etiquetas débiles del dominio.",
            TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: "La lectura causal se reorganiza en journeys de comité para explicar dónde se rompe la experiencia y por qué cae el NPS.",
        }
        section(
            "Mapa causal priorizado",
            "Dónde se rompe la experiencia, qué evidencia lo sostiene y qué entrará en comité.",
        )
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            kpi(
                "NPS en riesgo",
                f"{rationale_summary.nps_points_at_risk:.2f} pts",
                hint="Estimación de pérdida asociada a incidencias",
            )
        with k2:
            kpi(
                "NPS recuperable",
                f"{rationale_summary.nps_points_recoverable:.2f} pts",
                hint="Potencial recuperable con plan priorizado",
            )
        with k3:
            kpi(
                "Concentración top-3",
                f"{rationale_summary.top3_incident_share*100:.1f}%",
                hint="Incidencias concentradas en 3 tópicos",
            )
        with k4:
            lag_lbl = (
                f"{rationale_summary.median_lag_weeks:.1f} semanas"
                if rationale_summary.median_lag_weeks == rationale_summary.median_lag_weeks
                else "n/d"
            )
            kpi(
                "Tiempo de reacción",
                lag_lbl,
                hint="Mediana del lag incidencia -> cambio de NPS",
            )

        impact_cards = []
        current_card: Optional[dict[str, Any]] = None
        if not chain_candidates_df.empty:
            chain_view_all = chain_candidates_df.copy().reset_index(drop=True)
            chain_view_all["rank"] = np.arange(1, len(chain_view_all) + 1)
            chain_view_all["title"] = chain_view_all["nps_topic"].astype(str)
            chain_view_all["statement"] = chain_view_all["chain_story"].astype(str)
            impact_cards = chain_view_all.to_dict(orient="records")
        executive_banner(
            kicker="Narrativa causal",
            title=(
                f"{len(chain_candidates_df)} cadenas defendibles para {focus_name}"
                if not chain_candidates_df.empty
                else "Sin cadenas defendibles en esta ventana"
            ),
            summary=(
                f"{mode_summary_map.get(str(touchpoint_source), 'Lectura causal activa.')} "
                f"La política Helix↔VoC está fijada en similitud ≥ {LINK_MIN_SIMILARITY:.2f}, "
                f"top-{LINK_TOP_K_PER_INCIDENT} por incidencia y ventana de ±{LINK_MAX_DAYS_APART} días."
            ),
            metrics=[
                ("Modo causal", mode_label_map.get(str(touchpoint_source), str(touchpoint_source))),
                ("Incidencias con match", str(assigned_incidents_total)),
                ("Comentarios enlazados", str(linked_comments_total)),
                ("Links validados", str(linked_pairs_total)),
            ],
        )
        if impact_cards:
            pills(
                [
                    f"Solo cadena completa defendible",
                    f"{linked_topics_total} tópicos linkados",
                    f"{len(chain_candidates_df)} cadenas causales",
                ]
            )
            card(
                "Diagnóstico de cobertura",
                (
                    "<div style='line-height:1.55;'>"
                    f"<strong>{assigned_incidents_total}</strong> incidencias con match semántico, "
                    f"<strong>{linked_comments_total}</strong> comentarios enlazados, "
                    f"<strong>{linked_pairs_total}</strong> links validados y "
                    f"<strong>{len(chain_candidates_df)}</strong> cadenas defendibles en el modo activo."
                    "</div>"
                ),
                flat=True,
            )
            st.markdown("#### Selección para comité")
            label_map = {
                str(rec.get("chain_key", "")): str(rec.get("selection_label", rec.get("nps_topic", "")))
                for rec in impact_cards
            }
            selected_chain_keys = st.multiselect(
                "Temas que entrarán en la PPT",
                options=list(label_map.keys()),
                default=selected_chain_keys,
                format_func=lambda key: label_map.get(str(key), str(key)),
                max_selections=3,
                key="nh_chain_candidates_selected",
                help="Selecciona hasta 3 cadenas causales defendibles. Estas serán las que alimenten la narrativa y la PPT.",
            )
            if not selected_chain_keys:
                selected_chain_keys = _sync_chain_selection_state(
                    chain_candidates_df,
                    key_prefix="nh_chain_candidates",
                    default_limit=3,
                )
            chain_df = _cap_chain_evidence_rows(
                _select_chain_rows(chain_candidates_df, selected_chain_keys),
                max_incident_examples=5,
                max_comment_examples=2,
            )

            chosen_labels = [
                label_map.get(str(k), str(k))
                for k in selected_chain_keys
                if str(k) in label_map
            ]
            if chosen_labels:
                pills([f"PPT {idx + 1}: {lbl}" for idx, lbl in enumerate(chosen_labels)])
            if len(chosen_labels) < 3:
                st.info(
                    "La presentación mostrará las cadenas seleccionadas. Si eliges menos de 3, las restantes quedarán vacías."
                )

            st.markdown("#### Cadena activa")
            nav_prev, nav_meta, nav_next = st.columns([1, 3, 1])
            current_idx = int(st.session_state.get("nh_chain_candidates_view_idx", 0) or 0)
            total_cards = len(impact_cards)
            current_idx = max(0, min(current_idx, total_cards - 1))
            with nav_prev:
                if st.button(
                    "Anterior",
                    use_container_width=True,
                    key="nh_chain_candidates_prev",
                    disabled=total_cards <= 1,
                ):
                    current_idx = (current_idx - 1) % total_cards
                    st.session_state["nh_chain_candidates_view_idx"] = current_idx
            with nav_next:
                if st.button(
                    "Ver siguiente",
                    use_container_width=True,
                    key="nh_chain_candidates_next",
                    disabled=total_cards <= 1,
                ):
                    current_idx = (current_idx + 1) % total_cards
                    st.session_state["nh_chain_candidates_view_idx"] = current_idx
            current_idx = int(st.session_state.get("nh_chain_candidates_view_idx", current_idx) or 0)
            current_idx = max(0, min(current_idx, total_cards - 1))
            current_card = impact_cards[current_idx]
            with nav_meta:
                selected_note = (
                    "Seleccionada para PPT"
                    if str(current_card.get("chain_key", "")) in set(map(str, selected_chain_keys))
                    else "No seleccionada para PPT"
                )
                st.markdown(
                    f"**Cadena {current_idx + 1} de {total_cards}** · {selected_note}"
                )
                st.caption(str(current_card.get("selection_label", current_card.get("nps_topic", ""))))
            impact_chain([current_card])
        elif not rationale_df.empty:
            st.info(
                "Hay impacto estadístico, pero no se encontraron cadenas defendibles con link explícito entre Helix y VoC para mostrar en comité."
            )

        if rationale_df.empty:
            st.info(
                "No hay señal suficiente para construir el racional de negocio (prueba ampliando ventana o bajando umbral)."
            )
        elif show_priorities and current_card is not None:
            st.markdown("#### Priorización del tema activo")
            active_df = pd.DataFrame([current_card]).copy()
            show_cols = [
                "nps_topic",
                "priority",
                "confidence",
                "nps_points_at_risk",
                "nps_points_recoverable",
                "focus_probability_with_incident",
                "nps_delta_expected",
                "total_nps_impact",
                "causal_score",
                "touchpoint",
                "delta_focus_rate_pp",
                "incident_rate_per_100_responses",
                "incidents",
                "responses",
                "action_lane",
                "owner_role",
                "eta_weeks",
            ]
            for col in show_cols:
                if col not in active_df.columns:
                    active_df[col] = np.nan if col not in {"action_lane", "owner_role", "nps_topic", "touchpoint"} else ""
            if "focus_probability_with_incident" in active_df.columns:
                active_df["focus_probability_with_incident"] = active_df[
                    "focus_probability_with_incident"
                ].where(
                    pd.to_numeric(
                        active_df["focus_probability_with_incident"], errors="coerce"
                    ).notna(),
                    active_df.get("detractor_probability", np.nan),
                )

            active_df["priority"] = pd.to_numeric(active_df["priority"], errors="coerce").round(3)
            active_df["confidence"] = pd.to_numeric(active_df["confidence"], errors="coerce").round(3)
            active_df["nps_points_at_risk"] = pd.to_numeric(
                active_df["nps_points_at_risk"], errors="coerce"
            ).round(2)
            active_df["nps_points_recoverable"] = pd.to_numeric(
                active_df["nps_points_recoverable"], errors="coerce"
            ).round(2)
            active_df["focus_probability_with_incident"] = pd.to_numeric(
                active_df["focus_probability_with_incident"], errors="coerce"
            ).round(3)
            active_df["nps_delta_expected"] = pd.to_numeric(
                active_df["nps_delta_expected"], errors="coerce"
            ).round(2)
            active_df["total_nps_impact"] = pd.to_numeric(
                active_df["total_nps_impact"], errors="coerce"
            ).round(2)
            active_df["causal_score"] = pd.to_numeric(
                active_df["causal_score"], errors="coerce"
            ).round(3)
            active_df["delta_focus_rate_pp"] = pd.to_numeric(
                active_df["delta_focus_rate_pp"], errors="coerce"
            ).round(2)
            active_df["incident_rate_per_100_responses"] = pd.to_numeric(
                active_df["incident_rate_per_100_responses"], errors="coerce"
            ).round(2)
            active_df["incidents"] = pd.to_numeric(active_df["incidents"], errors="coerce").round(0)
            active_df["responses"] = pd.to_numeric(active_df["responses"], errors="coerce").round(0)
            active_df["eta_weeks"] = pd.to_numeric(active_df["eta_weeks"], errors="coerce").round(1)

            active_metrics = [
                ("Prioridad", f"{float(active_df.iloc[0]['priority']):.2f}" if pd.notna(active_df.iloc[0]["priority"]) else "n/d"),
                ("NPS en riesgo", f"{float(active_df.iloc[0]['nps_points_at_risk']):.2f} pts" if pd.notna(active_df.iloc[0]["nps_points_at_risk"]) else "n/d"),
                ("NPS recuperable", f"{float(active_df.iloc[0]['nps_points_recoverable']):.2f} pts" if pd.notna(active_df.iloc[0]["nps_points_recoverable"]) else "n/d"),
                ("Owner", str(active_df.iloc[0]["owner_role"] or "n/d")),
            ]
            executive_banner(
                kicker="Ficha operativa individual",
                title=str(active_df.iloc[0]["nps_topic"] or "Tema activo"),
                summary=(
                    "Todo el detalle inferior queda aislado al tema que estás viendo arriba. "
                    "No se mezclan otros tópicos en la lectura operativa."
                ),
                metrics=active_metrics,
            )

            tab_matrix, tab_detail = st.tabs(["Matriz visual", "Ficha cuantitativa"])
            with tab_matrix:
                cmat, crisk = st.columns(2)
                with cmat:
                    fig_pm = chart_incident_priority_matrix(active_df, theme=theme, top_k=1)
                    if fig_pm is not None:
                        st.plotly_chart(fig_pm, use_container_width=True)
                with crisk:
                    fig_rr = chart_incident_risk_recovery(active_df, theme=theme, top_k=1)
                    if fig_rr is not None:
                        st.plotly_chart(fig_rr, use_container_width=True)

            with tab_detail:
                st.dataframe(
                    active_df[show_cols].rename(
                        columns={
                            "nps_topic": "Tópico NPS",
                            "touchpoint": "Touchpoint",
                            "priority": "Prioridad",
                            "confidence": "Confianza",
                            "nps_points_at_risk": "NPS en riesgo (pts)",
                            "nps_points_recoverable": "NPS recuperable (pts)",
                            "focus_probability_with_incident": f"Prob. {focus_name} con incidencia",
                            "nps_delta_expected": "Delta NPS esperado",
                            "total_nps_impact": "Impacto total NPS (pts)",
                            "causal_score": "Causal score",
                            "delta_focus_rate_pp": f"Δ % {focus_name.capitalize()} (pp)",
                            "incident_rate_per_100_responses": "Incidencias por 100 respuestas",
                            "incidents": "Incidencias",
                            "responses": "Respuestas",
                            "action_lane": "Lane de acción",
                            "owner_role": "Owner (rol)",
                            "eta_weeks": "ETA (semanas)",
                        }
                    ),
                    use_container_width=True,
                    height=230,
                )

    if show_ppt:
        section(
            "Narrativa y presentación",
            "Salida lista para comité con las cadenas que has seleccionado en la priorización.",
        )
        if not chain_df.empty:
            executive_banner(
                kicker="Salida de comité",
                title=f"{len(chain_df)} cadenas seleccionadas para la presentación",
                summary=(
                    "La narrativa ejecutiva, el guion y la PPT se construyen solo con las cadenas que has marcado. "
                    "Aquí ya no hay fallback genérico: o hay evidencia defendible o no entra en comité."
                ),
                metrics=[
                    ("Cadenas seleccionadas", str(len(chain_df))),
                    ("NPS en riesgo", f"{rationale_summary.nps_points_at_risk:.2f} pts"),
                    ("NPS recuperable", f"{rationale_summary.nps_points_recoverable:.2f} pts"),
                    ("Impacto total", f"{rationale_summary.total_nps_impact:.2f} pts"),
                ],
            )
            pills(
                [
                    f"PPT {idx + 1}: {str(row.get('selection_label', row.get('nps_topic', '')))}"
                    for idx, row in enumerate(chain_df.to_dict(orient="records"))
                ]
            )
            impact_chain(chain_df.to_dict(orient="records"))
        else:
            executive_banner(
                kicker="Salida de comité",
                title="Narrativa sin evidencia causal seleccionada",
                summary=(
                    "Para construir una narrativa de comité potente necesitas seleccionar cadenas defendibles en "
                    "Priorización y palancas. Si no hay selección, la salida se mantiene descriptiva."
                ),
                metrics=[
                    ("Cadenas seleccionadas", "0"),
                    ("Links validados", str(linked_pairs_total)),
                    ("Modo causal", mode_label_map.get(str(touchpoint_source), str(touchpoint_source))),
                ],
            )

        tab_message, tab_story, tab_script = st.tabs(
            ["Mensaje ejecutivo", "Narrativa comité", "Guion 8 slides"]
        )
        with tab_message:
            card(
                "Mensaje principal",
                (
                    "<div style='line-height:1.65;'>"
                    f"El periodo analizado concentra <strong>{rationale_summary.nps_points_at_risk:.2f} puntos</strong> "
                    f"de NPS en riesgo y un potencial recuperable de <strong>{rationale_summary.nps_points_recoverable:.2f} puntos</strong>. "
                    f"Las cadenas seleccionadas representan la mejor combinación de impacto, evidencia y accionabilidad para comité."
                    "</div>"
                ),
            )
        with tab_story:
            _clipboard_copy_widget(ppt_story_md, label="Copiar narrativa PPT")
            st.text_area(
                "Narrativa de negocio",
                value=ppt_story_md,
                height=320,
            )
        with tab_script:
            _clipboard_copy_widget(ppt_8slides_md, label="Copiar guion 8 slides")
            st.text_area(
                "Script de presentación",
                value=ppt_8slides_md,
                height=380,
            )
            st.download_button(
                "Descargar guion 8 slides (.md)",
                data=ppt_8slides_md.encode("utf-8"),
                file_name="guion_8_slides_nps_helix.md",
                mime="text/markdown",
                use_container_width=True,
            )

        st.divider()
        section(
            "Presentación automática",
            "Genera y descarga la presentación en formato corporativo fijo con un clic.",
        )
    if show_ppt:
        ppt_sig = (
            f"{service_origin}|{service_origin_n1}|{service_origin_n2}|{start}|{end}|"
            f"{focus_name}|{len(overall_daily)}|{len(rationale_df)}|{'/'.join(selected_chain_keys)}"
        )
        template_mode = st.selectbox(
            "Formato de presentación",
            options=["Plantilla corporativa fija v1"],
            index=0,
            key="nh_ppt_template_mode",
            help="Formato bloqueado para mantener consistencia entre periodos.",
        )
        b1, b2 = st.columns(2)
        with b1:
            make_ppt = st.button(
                "Generar y guardar en Descargas (.pptx)",
                type="primary",
                use_container_width=True,
                key="nh_generate_pptx",
            )
        with b2:
            make_ppt_open = st.button(
                "Generar + abrir en Descargas",
                use_container_width=True,
                key="nh_generate_open_pptx",
            )
        if make_ppt or make_ppt_open:
            try:
                from nps_lens.reports import generate_business_review_ppt

                # PPT dataset is intentionally decoupled from the on-screen filtered slice.
                # For presentation narratives, use full historical context (SO + N1 + optional N2).
                nps_hist = load_context_df(
                    store_dir,
                    service_origin,
                    service_origin_n1,
                    service_origin_n2,
                    nps_group_choice,
                    VIEW_COLUMNS["llm"],
                    date_start=None,
                    date_end=None,
                    month_filter=None,
                )
                nps_hist["Fecha"] = pd.to_datetime(nps_hist.get("Fecha"), errors="coerce")
                nps_hist = nps_hist.dropna(subset=["Fecha"]).copy()
                helix_hist = helix_df.copy()
                helix_hist["Fecha"] = pd.to_datetime(helix_hist.get("Fecha"), errors="coerce")
                helix_hist = helix_hist.dropna(subset=["Fecha"]).copy()

                def _attach_daily_nps_mean(
                    base_df: pd.DataFrame, nps_df: pd.DataFrame
                ) -> pd.DataFrame:
                    """Attach daily mean NPS to a date-based aggregate dataframe."""
                    if base_df is None or base_df.empty:
                        return pd.DataFrame()
                    out = base_df.copy()
                    if "date" not in out.columns:
                        return out
                    src = nps_df.copy()
                    src["Fecha"] = pd.to_datetime(src.get("Fecha"), errors="coerce")
                    src["NPS"] = pd.to_numeric(src.get("NPS"), errors="coerce")
                    src = src.dropna(subset=["Fecha"])
                    if src.empty:
                        return out
                    daily_nps = (
                        src.assign(date=lambda d: d["Fecha"].dt.normalize())
                        .groupby("date", as_index=False)
                        .agg(nps_mean=("NPS", "mean"))
                    )
                    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
                    out = out.merge(daily_nps, on="date", how="left")
                    return out

                def _build_incident_evidence_payload(
                    links_src: pd.DataFrame,
                    nps_focus_src: pd.DataFrame,
                    helix_src: pd.DataFrame,
                ) -> pd.DataFrame:
                    return build_hotspot_evidence(
                        links_src,
                        nps_focus_src,
                        helix_src,
                        system_date=pd.Timestamp.now().date(),
                        max_hotspots=10,
                        min_term_occurrences=int(HOTSPOT_MIN_TERM_OCCURRENCES),
                        min_validated_similarity=float(LINK_MIN_SIMILARITY),
                        max_days_apart=int(LINK_MAX_DAYS_APART),
                    )

                def _build_incident_timeline_payload(
                    links_src: pd.DataFrame,
                    nps_focus_src: pd.DataFrame,
                    helix_src: pd.DataFrame,
                    incident_evidence_src: Optional[pd.DataFrame] = None,
                ) -> pd.DataFrame:
                    return build_hotspot_timeline(
                        links_src,
                        nps_focus_src,
                        helix_src,
                        incident_evidence_df=incident_evidence_src,
                        max_hotspots=10,
                        min_validated_similarity=float(LINK_MIN_SIMILARITY),
                        max_days_apart=int(LINK_MAX_DAYS_APART),
                    )

                def _align_evidence_to_best_axis(
                    nps_src: pd.DataFrame,
                    helix_src: pd.DataFrame,
                    evidence_src: pd.DataFrame,
                ) -> tuple[pd.DataFrame, str]:
                    if evidence_src is None or evidence_src.empty:
                        return evidence_src, ""
                    axis_info = select_best_business_axis_for_hotspots(
                        nps_src,
                        helix_src,
                        min_n=200,
                    )
                    axis = str(axis_info.get("best_axis", "Palanca"))
                    red_map = axis_info.get("red_labels", {})
                    labels = list(red_map.get(axis, [])) if isinstance(red_map, dict) else []
                    aligned = align_hotspot_evidence_to_axis(
                        evidence_src,
                        axis=axis,
                        red_labels=labels,
                        max_hotspots=10,
                    )
                    ratios = axis_info.get("axis_ratios", {})
                    r_pal = float(ratios.get("Palanca", 0.0)) if isinstance(ratios, dict) else 0.0
                    r_sub = (
                        float(ratios.get("Subpalanca", 0.0)) if isinstance(ratios, dict) else 0.0
                    )
                    note = (
                        f"Eje seleccionado para el racional: {axis} "
                        f"(cobertura Helix en rojos: Palanca {r_pal*100:.1f}% · "
                        f"Subpalanca {r_sub*100:.1f}%)."
                    )
                    return (
                        aligned if aligned is not None and not aligned.empty else evidence_src
                    ), note

                # Safe fallback to current view payload if historical payload can't be built.
                overall_weekly_ppt = overall_daily if not overall_daily.empty else overall_weekly
                overall_weekly_ppt = _attach_daily_nps_mean(overall_weekly_ppt, nps_slice)
                by_topic_daily_ppt = by_topic_daily
                by_topic_weekly_ppt = by_topic_weekly
                ranking_df_ppt = (
                    rank2 if "rank2" in locals() and not rank2.empty else rank_for_rationale
                )
                rationale_df_ppt = rationale_df
                rationale_summary_ppt = rationale_summary
                chain_df_ppt = chain_df
                ppt_story_md_ppt = ppt_story_md
                ppt_8slides_md_ppt = ppt_8slides_md
                ppt_start = start
                ppt_end = end
                lag_days_for_ppt = lag_days.copy()
                lag_weeks_for_ppt = (
                    rank2[["nps_topic", "best_lag_weeks"]].copy()
                    if "rank2" in locals() and not rank2.empty and "best_lag_weeks" in rank2.columns
                    else pd.DataFrame(columns=["nps_topic", "best_lag_weeks"])
                )
                changepoints_for_ppt = (
                    cp_by_topic.copy()
                    if "cp_by_topic" in locals() and isinstance(cp_by_topic, pd.DataFrame)
                    else pd.DataFrame(columns=["nps_topic", "changepoints"])
                )
                hotspot_focus_note = ""
                helix_for_hot_terms = helix_hist if not helix_hist.empty else helix_slice
                incident_evidence_ppt = _build_incident_evidence_payload(
                    links_df,
                    focus_df,
                    helix_for_hot_terms,
                )
                incident_evidence_ppt, hotspot_focus_note = _align_evidence_to_best_axis(
                    nps_slice,
                    helix_for_hot_terms,
                    incident_evidence_ppt,
                )
                incident_timeline_ppt = _build_incident_timeline_payload(
                    links_df,
                    focus_df,
                    helix_for_hot_terms,
                    incident_evidence_ppt,
                )

                if not nps_hist.empty and not helix_hist.empty:
                    ppt_start = pd.to_datetime(nps_hist["Fecha"].min(), errors="coerce").date()
                    ppt_end = pd.to_datetime(nps_hist["Fecha"].max(), errors="coerce").date()

                    nps_hist_work = nps_hist.copy()
                    nps_hist_work["NPS Group"] = nps_hist_work.get("NPS Group", "").astype(str)
                    score_hist = pd.to_numeric(nps_hist_work.get("NPS", np.nan), errors="coerce")
                    grp_hist = nps_hist_work["NPS Group"].str.upper()
                    if focus_group == "promoter":
                        mask_focus_hist = (grp_hist == "PROMOTER") | (score_hist >= 9)
                    elif focus_group == "passive":
                        mask_focus_hist = (grp_hist == "PASSIVE") | (
                            (score_hist >= 7) & (score_hist <= 8)
                        )
                    else:
                        mask_focus_hist = (grp_hist == "DETRACTOR") | (score_hist <= 6)

                    focus_hist = nps_hist_work[mask_focus_hist].copy()
                    assign_hist, links_hist = link_incidents_to_nps_topics(
                        focus_hist,
                        helix_hist,
                        min_similarity=float(min_sim),
                        top_k_per_incident=int(LINK_TOP_K_PER_INCIDENT),
                        max_days_apart=int(LINK_MAX_DAYS_APART),
                    )
                    ow_hist, btw_hist = weekly_aggregates(
                        nps_hist_work, helix_hist, assign_hist, focus_group=focus_group
                    )
                    od_hist, btd_hist = daily_aggregates(
                        nps_hist_work, helix_hist, assign_hist, focus_group=focus_group
                    )
                    od_hist = _attach_daily_nps_mean(od_hist, nps_hist_work)

                    rank_hist = causal_rank_by_topic(btw_hist)
                    cp_hist = detect_detractor_changepoints_with_bootstrap(
                        btw_hist,
                        pen=6.0,
                        n_boot=200,
                        block_size=2,
                        tol_periods=1,
                    )
                    lag_hist = estimate_best_lag_by_topic(btw_hist, max_lag_weeks=6)
                    lead_hist = incidents_lead_changepoints_flag(btw_hist, cp_hist, window_weeks=4)
                    rank2_hist = (
                        rank_hist.merge(cp_hist, on="nps_topic", how="left")
                        .merge(lag_hist, on="nps_topic", how="left")
                        .merge(lead_hist, on="nps_topic", how="left")
                    )
                    if not kc_adj.empty:
                        rank2_hist = rank2_hist.merge(kc_adj, on="nps_topic", how="left")
                    else:
                        rank2_hist["factor"] = 1.0
                    rank2_hist["factor"] = rank2_hist.get("factor", 1.0).fillna(1.0).astype(float)
                    score_hist = pd.to_numeric(
                        rank2_hist.get("score", pd.Series(0.0, index=rank2_hist.index)),
                        errors="coerce",
                    ).fillna(0.0)
                    rank2_hist["confidence_learned"] = (
                        score_hist.astype(float) * rank2_hist["factor"]
                    ).clip(0, 1)
                    rank2_hist = rank2_hist.sort_values(
                        ["confidence_learned", "incidents", "responses"], ascending=False
                    ).reset_index(drop=True)

                    rationale_hist = build_incident_nps_rationale(
                        btw_hist,
                        focus_group=focus_group,
                        rank_df=rank2_hist if not rank2_hist.empty else rank_hist,
                        min_topic_responses=80,
                        recovery_factor=0.65,
                    )
                    if not rationale_hist.empty:
                        rationale_summary_hist = summarize_incident_nps_rationale(rationale_hist)
                        period_label_hist = f"{ppt_start} -> {ppt_end}"
                        chain_hist_all = build_incident_attribution_chains(
                            links_hist,
                            focus_hist,
                            helix_hist,
                            rationale_df=rationale_hist,
                            top_k=0,
                            max_incident_examples=5,
                            max_comment_examples=2,
                            min_links_per_topic=1,
                            touchpoint_source=touchpoint_source,
                        )
                        chain_hist_all = _annotate_chain_candidates(chain_hist_all)
                        chain_hist = _select_chain_rows(chain_hist_all, selected_chain_keys)
                        if chain_hist.empty and not chain_hist_all.empty:
                            chain_hist = chain_hist_all.head(min(3, len(chain_hist_all))).copy()
                            st.warning(
                                "Alguno de los temas seleccionados no estaba disponible en el histórico completo. "
                                "La PPT ha usado las primeras cadenas defendibles disponibles en ese histórico."
                            )
                        ppt_story_md_hist = build_incident_ppt_story(
                            rationale_summary_hist,
                            rationale_hist,
                            attribution_df=chain_hist,
                            focus_name=focus_name,
                            top_k=6,
                        )
                        ppt_8slides_md_hist = build_ppt_8slide_script(
                            rationale_summary_hist,
                            rationale_hist,
                            attribution_df=chain_hist,
                            touchpoint_source=touchpoint_source,
                            service_origin=service_origin,
                            service_origin_n1=service_origin_n1,
                            focus_name=focus_name,
                            period_label=period_label_hist,
                            top_k=6,
                        )
                        lag_days_hist = (
                            estimate_best_lag_days_by_topic(
                                btd_hist,
                                max_lag_days=21,
                                min_points=30,
                            )
                            if can_use_daily_resample(
                                od_hist, min_days_with_responses=20, min_coverage=0.45
                            )
                            else pd.DataFrame()
                        )
                        overall_weekly_ppt = od_hist if not od_hist.empty else ow_hist
                        by_topic_daily_ppt = btd_hist
                        by_topic_weekly_ppt = btw_hist
                        ranking_df_ppt = rank2_hist if not rank2_hist.empty else rank_hist
                        rationale_df_ppt = rationale_hist
                        rationale_summary_ppt = rationale_summary_hist
                        chain_df_ppt = chain_hist
                        ppt_story_md_ppt = ppt_story_md_hist
                        ppt_8slides_md_ppt = ppt_8slides_md_hist
                        lag_days_for_ppt = lag_days_hist
                        lag_weeks_for_ppt = lag_hist
                        changepoints_for_ppt = cp_hist
                        incident_evidence_ppt = _build_incident_evidence_payload(
                            links_hist,
                            focus_hist,
                            helix_hist,
                        )
                        incident_evidence_ppt, hotspot_focus_note = _align_evidence_to_best_axis(
                            nps_hist_work,
                            helix_hist,
                            incident_evidence_ppt,
                        )
                        incident_timeline_ppt = _build_incident_timeline_payload(
                            links_hist,
                            focus_hist,
                            helix_hist,
                            incident_evidence_ppt,
                        )
                        st.caption(
                            f"La PPT usa histórico completo del contexto: {ppt_start} -> {ppt_end}."
                        )

                hotspot_summary_ppt = summarize_hotspot_counts(
                    incident_evidence_ppt,
                    incident_timeline_ppt,
                    max_hotspots=3,
                )
                st.session_state["_nh_hotspot_summary_ppt"] = hotspot_summary_ppt.to_dict(
                    orient="records"
                )
                if not hotspot_summary_ppt.empty:
                    severe = hotspot_summary_ppt[
                        (hotspot_summary_ppt["hotspot_incidents"] > 0)
                        & (hotspot_summary_ppt["chart_helix_records"] <= 0)
                    ]
                    if not severe.empty:
                        st.warning(
                            "Se detectó desalineación de conteos en un hotspot. "
                            "Se forzó cálculo centralizado para mantener coherencia de fuente."
                        )

                ppt_out = generate_business_review_ppt(
                    service_origin=service_origin,
                    service_origin_n1=service_origin_n1,
                    service_origin_n2=service_origin_n2,
                    period_start=ppt_start,
                    period_end=ppt_end,
                    focus_name=focus_name,
                    overall_weekly=overall_weekly_ppt,
                    rationale_df=rationale_df_ppt,
                    nps_points_at_risk=float(rationale_summary_ppt.nps_points_at_risk),
                    nps_points_recoverable=float(rationale_summary_ppt.nps_points_recoverable),
                    top3_incident_share=float(rationale_summary_ppt.top3_incident_share),
                    median_lag_weeks=float(rationale_summary_ppt.median_lag_weeks),
                    story_md=ppt_story_md_ppt,
                    script_8slides_md=ppt_8slides_md_ppt,
                    attribution_df=chain_df_ppt,
                    ranking_df=ranking_df_ppt,
                    by_topic_daily=by_topic_daily_ppt,
                    lag_days_by_topic=lag_days_for_ppt,
                    by_topic_weekly=by_topic_weekly_ppt,
                    lag_weeks_by_topic=lag_weeks_for_ppt,
                    template_name=str(template_mode),
                    corporate_fixed=True,
                    logo_path=_logo_path,
                    incident_evidence_df=incident_evidence_ppt,
                    changepoints_by_topic=changepoints_for_ppt,
                    incident_timeline_df=incident_timeline_ppt,
                    hotspot_focus_note=hotspot_focus_note,
                    touchpoint_source=touchpoint_source,
                )
                export_dir = settings.data_dir / "exports" / "ppt"
                export_dir.mkdir(parents=True, exist_ok=True)
                saved_path = export_dir / str(ppt_out.file_name)
                saved_path.write_bytes(ppt_out.content)
                downloads_path = None
                downloads_error = ""
                try:
                    downloads_dir = Path.home() / "Downloads"
                    downloads_dir.mkdir(parents=True, exist_ok=True)
                    downloads_path = downloads_dir / str(ppt_out.file_name)
                    downloads_path.write_bytes(ppt_out.content)
                except Exception as exc:
                    downloads_path = None
                    downloads_error = str(exc)

                st.session_state["_nh_ppt_export"] = {
                    "sig": ppt_sig,
                    "file_name": ppt_out.file_name,
                    "content": ppt_out.content,
                    "slides": int(ppt_out.slide_count),
                    "saved_path": str(saved_path),
                    "downloads_path": (str(downloads_path) if downloads_path is not None else ""),
                    "downloads_error": downloads_error,
                }
                st.success(
                    f"Presentación generada correctamente ({int(ppt_out.slide_count)} diapositivas)."
                )
                saved_folder = saved_path.parent.resolve()
                st.markdown(f"Copia local guardada en: [{saved_path}]({saved_folder.as_uri()})")
                if downloads_path is not None:
                    st.caption(f"Copia en Descargas: {downloads_path}")
                elif downloads_error:
                    st.warning(
                        "No se pudo guardar en Descargas. "
                        f"Detalle: {downloads_error}. Se mantiene la copia local de exportaciones."
                    )
                if make_ppt_open:
                    dl_path = downloads_path
                    if dl_path is None:
                        raise RuntimeError("No se pudo crear la copia en Descargas.")
                    if sys.platform == "darwin":
                        subprocess.Popen(["open", str(dl_path)])
                    elif os.name == "nt" and hasattr(os, "startfile"):
                        os.startfile(str(dl_path))  # type: ignore[attr-defined]
                    else:
                        subprocess.Popen(["xdg-open", str(dl_path)])
                    st.info(f"Archivo abierto desde Descargas: {dl_path}")
            except Exception as exc:
                st.error(
                    "No se pudo generar la presentación en este entorno. "
                    "Ejecuta `make setup` y `make verify-runtime` para validar dependencias."
                )
                with st.expander("Detalle técnico", expanded=False):
                    st.code(str(exc))

        cached_ppt = st.session_state.get("_nh_ppt_export")
        if isinstance(cached_ppt, dict) and str(cached_ppt.get("sig", "")) == ppt_sig:
            saved_path_raw = str(cached_ppt.get("saved_path", "")).strip()
            downloads_path_raw = str(cached_ppt.get("downloads_path", "")).strip()
            if st.button(
                "Abrir archivo local (.pptx)",
                use_container_width=True,
                key="nh_open_local_pptx",
            ):
                if not saved_path_raw:
                    st.error("No se encontró la ruta local de la presentación.")
                else:
                    target = Path(saved_path_raw)
                    if not target.exists():
                        st.error(f"El archivo no existe en disco: {target}")
                    else:
                        try:
                            if sys.platform == "darwin":
                                subprocess.Popen(["open", str(target)])
                            elif os.name == "nt" and hasattr(os, "startfile"):
                                os.startfile(str(target))  # type: ignore[attr-defined]
                            else:
                                subprocess.Popen(["xdg-open", str(target)])
                            st.success(f"Archivo abierto: {target.name}")
                        except Exception as exc:
                            st.error("No se pudo abrir el archivo automáticamente.")
                            st.caption(f"Error: {exc}")
            if saved_path_raw:
                with contextlib.suppress(Exception):
                    saved_folder = Path(saved_path_raw).expanduser().resolve().parent
                    st.markdown(
                        f"Copia local guardada en: [{saved_path_raw}]({saved_folder.as_uri()})"
                    )
            else:
                st.caption("Ruta local: no disponible")
            if downloads_path_raw:
                st.caption(f"Ruta en Descargas: {downloads_path_raw}")
            elif str(cached_ppt.get("downloads_error", "")).strip():
                st.caption(
                    "No se creó copia en Descargas. "
                    f"Detalle: {str(cached_ppt.get('downloads_error', '')).strip()}"
                )

            local_folder = (
                Path(saved_path_raw).expanduser().resolve().parent if saved_path_raw else None
            )
            downloads_folder = (
                Path(downloads_path_raw).expanduser().resolve().parent
                if downloads_path_raw
                else None
            )
            f1, f2 = st.columns(2)
            with f1:
                if local_folder is not None:
                    with contextlib.suppress(Exception):
                        st.markdown(
                            f"[Abrir carpeta de exportación local]({local_folder.as_uri()})"
                        )
                    if st.button(
                        "Abrir carpeta local",
                        use_container_width=True,
                        key="nh_open_local_folder",
                    ):
                        try:
                            if sys.platform == "darwin":
                                subprocess.Popen(["open", str(local_folder)])
                            elif os.name == "nt" and hasattr(os, "startfile"):
                                os.startfile(str(local_folder))  # type: ignore[attr-defined]
                            else:
                                subprocess.Popen(["xdg-open", str(local_folder)])
                        except Exception as exc:
                            st.error("No se pudo abrir la carpeta local.")
                            st.caption(f"Error: {exc}")
            with f2:
                if downloads_folder is not None:
                    with contextlib.suppress(Exception):
                        st.markdown(f"[Abrir carpeta Descargas]({downloads_folder.as_uri()})")
                    if st.button(
                        "Abrir carpeta Descargas",
                        use_container_width=True,
                        key="nh_open_downloads_folder",
                    ):
                        try:
                            if sys.platform == "darwin":
                                subprocess.Popen(["open", str(downloads_folder)])
                            elif os.name == "nt" and hasattr(os, "startfile"):
                                os.startfile(str(downloads_folder))  # type: ignore[attr-defined]
                            else:
                                subprocess.Popen(["xdg-open", str(downloads_folder)])
                        except Exception as exc:
                            st.error("No se pudo abrir la carpeta Descargas.")
                            st.caption(f"Error: {exc}")
            if st.button(
                "Guardar copia en Descargas y abrir",
                use_container_width=True,
                key="nh_save_downloads_pptx",
            ):
                try:
                    file_name = str(
                        cached_ppt.get("file_name", "presentacion_nps_incidencias.pptx")
                    )
                    payload = bytes(cached_ppt.get("content", b""))
                    downloads_dir = Path.home() / "Downloads"
                    downloads_dir.mkdir(parents=True, exist_ok=True)
                    out_path = downloads_dir / file_name
                    out_path.write_bytes(payload)
                    if sys.platform == "darwin":
                        subprocess.Popen(["open", str(out_path)])
                    elif os.name == "nt" and hasattr(os, "startfile"):
                        os.startfile(str(out_path))  # type: ignore[attr-defined]
                    else:
                        subprocess.Popen(["xdg-open", str(out_path)])
                    st.success(f"Archivo guardado y abierto: {out_path}")
                except Exception as exc:
                    st.error("No se pudo guardar la copia en Descargas.")
                    st.caption(f"Error: {exc}")

    # 4) Heatmap Tema x Día (incidencias asignadas)
    if show_priorities:
        st.markdown("### Heatmap: incidencias por bloque y tópico NPS (día)")
    if show_priorities and not by_topic_daily.empty:
        hdf = by_topic_daily.copy()
        hdf["block"] = hdf["nps_topic"].astype(str).str.split(">").str[0].str.strip()
        hdf["topic_short"] = (
            hdf["nps_topic"].astype(str).str.split(">", n=1).str[-1].str.strip().str.slice(0, 42)
        )
        hdf["topic_short"] = hdf["topic_short"].where(
            hdf["topic_short"].str.len() <= 42, hdf["topic_short"] + "…"
        )
        topic_rank = (
            hdf.groupby(["block", "nps_topic", "topic_short"], as_index=False)["incidents"]
            .sum()
            .sort_values(["incidents"], ascending=False)
            .head(24)
        )
        topic_rank = topic_rank.sort_values(["block", "incidents"], ascending=[True, False]).copy()
        topic_order = topic_rank["nps_topic"].tolist()
        label_map = {
            str(r["nps_topic"]): f"{str(r['block'])} | {str(r['topic_short'])}"
            for _, r in topic_rank.iterrows()
        }

        pivot = hdf[hdf["nps_topic"].isin(topic_order)].pivot_table(
            index="nps_topic",
            columns="date",
            values="incidents",
            aggfunc="sum",
            fill_value=0,
        )
        pivot = pivot.reindex(topic_order)
        if pivot.shape[0] > 0 and pivot.shape[1] > 0:
            px, go = _plotly()
            y_positions = list(range(pivot.shape[0]))
            tick_labels = [label_map.get(str(t), str(t)) for t in pivot.index.tolist()]
            zmax = (
                float(np.nanpercentile(pivot.values.astype(float), 95))
                if pivot.values.size
                else 1.0
            )
            hover_topics = np.tile(
                np.array(tick_labels, dtype=object).reshape(-1, 1), (1, pivot.shape[1])
            )
            heat = go.Figure(
                data=[
                    go.Heatmap(
                        x=pivot.columns.tolist(),
                        y=y_positions,
                        z=pivot.values.astype(float),
                        customdata=hover_topics,
                        zmin=0,
                        zmax=zmax if zmax > 0 else 1.0,
                        colorscale=risk_scale,
                        colorbar=dict(title="Incidencias"),
                        hovertemplate=(
                            "Fecha=%{x}<br>Tópico=%{customdata}<br>Incidencias=%{z:.0f}<extra></extra>"
                        ),
                    )
                ]
            )
            block_ranges = []
            cursor = 0
            for block, g in topic_rank.groupby("block", sort=False):
                n = len(g)
                start = cursor
                end = cursor + n - 1
                block_ranges.append((str(block), start, end))
                cursor += n
            for idx, (_, start, end) in enumerate(block_ranges):
                if idx % 2 == 0:
                    heat.add_hrect(
                        y0=start - 0.5,
                        y1=end + 0.5,
                        fillcolor=pal.get("color.neutral.bg.01", "#F4F7FB"),
                        opacity=0.16,
                        line_width=0,
                    )
                if start > 0:
                    heat.add_hline(
                        y=start - 0.5,
                        line_width=1,
                        line_dash="dot",
                        line_color=pal.get("color.neutral.border.01", "#D6DFEA"),
                    )
                y_center = (start + end) / 2
                heat.add_annotation(
                    x=-0.18,
                    y=y_center,
                    xref="paper",
                    yref="y",
                    text=f"<b>{block_ranges[idx][0]}</b>",
                    showarrow=False,
                    xanchor="right",
                    font=dict(size=11, color=pal.get("color.primary.text.01", "#0A1F44")),
                )
            heat.update_layout(
                height=min(860, 220 + 24 * pivot.shape[0]),
                margin=dict(l=260, r=20, t=10, b=10),
                yaxis=dict(
                    tickmode="array",
                    tickvals=y_positions,
                    ticktext=tick_labels,
                    automargin=True,
                ),
                xaxis=dict(title="Fecha"),
            )
            st.plotly_chart(apply_plotly_theme(heat, theme), use_container_width=True)
            st.caption(
                "Cada fila muestra: bloque | tópico. Las franjas y separadores agrupan visualmente por bloque para ubicar cada incidencia con rapidez."
            )
        else:
            st.info(
                "No hay datos suficientes para construir el heatmap en el periodo seleccionado."
            )

    # 4.1) Changepoints + Lag (incidencias preceden X semanas)
    if show_priorities:
        st.markdown("### Changepoints + Lag (incidencias preceden X semanas)")
    if show_priorities and not by_topic_weekly.empty and "rank2" in locals() and not rank2.empty:
        topic_for_lag = st.selectbox(
            "Tópico (para ver lag)", options=rank2["nps_topic"].tolist(), key="lag_topic_sel"
        )
        g = (
            by_topic_weekly[by_topic_weekly["nps_topic"] == topic_for_lag]
            .sort_values("week")
            .copy()
        )
        lag_row = rank2[rank2["nps_topic"] == topic_for_lag].head(1)
        lagw = (
            int(lag_row["best_lag_weeks"].iloc[0])
            if not lag_row.empty and pd.notna(lag_row["best_lag_weeks"].iloc[0])
            else 0
        )
        cps = (
            lag_row["changepoints"].iloc[0]
            if (not lag_row.empty and "changepoints" in lag_row.columns)
            else []
        )
        if not isinstance(cps, list):
            # may be NaN or string
            cps = [] if pd.isna(cps) else [str(cps)]
        g["incidents_shifted"] = g["incidents"].shift(lagw)
        px, go = _plotly()
        fig_lag = go.Figure()
        fig_lag.add_trace(
            go.Scatter(
                x=g["week"],
                y=g["focus_rate"],
                name=f"% {focus_name}",
                mode="lines+markers",
                line=dict(color=pal["color.primary.accent.value-07.default"], width=2),
                marker=dict(color=pal["color.primary.accent.value-07.default"], size=6),
            )
        )
        fig_lag.add_trace(
            go.Bar(
                x=g["week"],
                y=g["incidents_shifted"],
                name=f"# incidencias (shift {lagw}w)",
                yaxis="y2",
                opacity=0.70,
                marker=dict(color=pal["color.primary.accent.value-01.default"]),
            )
        )
        # changepoint vertical lines
        cp_level = (
            str(lag_row["max_cp_level"].iloc[0])
            if (not lag_row.empty and "max_cp_level" in lag_row.columns)
            else ""
        )
        cp_color = cp_level_color(dtokens, theme_mode, cp_level)
        for cp in cps[:8]:
            try:
                cp_dt = pd.to_datetime(cp)
                fig_lag.add_vline(x=cp_dt, line_width=2, line_dash="dot", line_color=cp_color)
            except Exception:
                pass
        fig_lag.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=10, b=10),
            yaxis=dict(title=f"% {focus_name}", tickformat=".0%"),
            yaxis2=dict(title="Incidencias (shifted)", overlaying="y", side="right"),
            legend=dict(orientation="h"),
        )
        st.plotly_chart(apply_plotly_theme(fig_lag, theme), use_container_width=True)
        st.caption(
            "Interpretación: el lag se elige maximizando la correlación entre incidencias(t) y detracción(t+lag). "
            "Las líneas punteadas son changepoints detectados en la serie de detracción por tópico. "
            "La significancia (High/Medium/Low) se estima por estabilidad con bootstrap (replicabilidad del punto de cambio)."
        )
    elif show_priorities:
        st.info("No hay datos suficientes para estimar changepoints/lag por tópico.")

    # 4.2) Lag en días (si la densidad de NPS permite daily resample)
    if show_priorities:
        st.markdown("### Lag en días (cuando la densidad de NPS lo permite)")
    if (show_priorities or show_evidence) and can_use_daily_resample(
        overall_daily, min_days_with_responses=20, min_coverage=0.45
    ):
        lag_days = estimate_best_lag_days_by_topic(by_topic_daily, max_lag_days=21, min_points=30)
        if show_priorities and not lag_days.empty and "rank2" in locals() and not rank2.empty:
            lag_days = lag_days.merge(rank2[["nps_topic"]], on="nps_topic", how="inner")
            lag_days["corr"] = lag_days["corr"].round(3)
            st.dataframe(
                lag_days.sort_values(["corr"], ascending=False)
                .head(25)
                .rename(
                    columns={
                        "nps_topic": "Tópico NPS",
                        "best_lag_days": "Lag (días)",
                        "corr": "Corr@Lag",
                        "points": "Puntos",
                    }
                ),
                use_container_width=True,
                height=320,
            )
        elif show_priorities:
            st.info("No hay tópicos suficientes para estimar lag diario.")

        if show_priorities and not lag_days.empty:
            topic_sel = st.selectbox(
                "Tópico para visualizar lag diario",
                options=lag_days.sort_values(["corr"], ascending=False)["nps_topic"].tolist(),
                key="lag_day_topic_sel",
            )
            lagd = (
                int(lag_days.set_index("nps_topic").loc[topic_sel, "best_lag_days"])
                if topic_sel in lag_days["nps_topic"].values
                else 0
            )
            gd = by_topic_daily[by_topic_daily["nps_topic"] == topic_sel].sort_values("date").copy()
            gd["incidents_shifted"] = gd["incidents"].shift(lagd)
            px, go = _plotly()
            figd = go.Figure()
            figd.add_trace(
                go.Scatter(
                    x=gd["date"],
                    y=gd["focus_rate"],
                    name=f"% {focus_name}",
                    mode="lines",
                    line=dict(color=pal["color.primary.accent.value-07.default"], width=2),
                )
            )
            figd.add_trace(
                go.Bar(
                    x=gd["date"],
                    y=gd["incidents_shifted"],
                    name=f"# incidencias (shift {lagd}d)",
                    yaxis="y2",
                    opacity=0.70,
                    marker=dict(color=pal["color.primary.accent.value-01.default"]),
                )
            )
            figd.update_layout(
                height=360,
                margin=dict(l=10, r=10, t=10, b=10),
                yaxis=dict(title=f"% {focus_name}", tickformat=".0%"),
                yaxis2=dict(title="Incidencias (shifted)", overlaying="y", side="right"),
                legend=dict(orientation="h"),
            )
            st.plotly_chart(apply_plotly_theme(figd, theme), use_container_width=True)
            st.caption(
                "Se activa el lag diario cuando hay suficiente densidad de respuestas por día (cobertura) y puntos válidos. "
                "El lag se elige maximizando corr(incidencias(t), detracción(t+lag))."
            )
    elif show_priorities:
        st.info(
            "El análisis diario no se activa: la densidad de NPS por día es insuficiente (pocos días con respuestas o baja cobertura). "
            "Usa el lag semanal (arriba) o amplía la ventana."
        )

    if not show_evidence:
        return

    # 5) Evidence wall
    st.markdown("### Evidence wall: detractores ↔ incidencias (links semánticos)")
    if focus_population.empty or links_df.empty:
        st.info(
            "No hay links validados (o no hay detractores) con la política estricta activa. Amplía la ventana temporal o revisa la calidad de texto."
        )
    else:
        topics = (
            rank["nps_topic"].tolist()
            if not rank.empty
            else sorted(focus_population["Palanca"].astype(str).unique().tolist())
        )
        chosen = st.selectbox(
            "Selecciona tópico NPS",
            options=(
                rank["nps_topic"].tolist()
                if not rank.empty
                else sorted(focus_population["Palanca"].astype(str).unique().tolist())
            ),
        )
        sub_links = links_df[links_df["nps_topic"] == chosen].head(50).copy()
        if sub_links.empty:
            st.info("No hay links validados para este tópico con la política estricta activa.")
        else:
            # Join snippets
            det2 = focus_population.copy()
            det2["nps_id"] = det2["ID"].astype(str)
            hel2 = helix_slice.copy()
            hel2["incident_id"] = hel2.get(
                "Incident Number", hel2.get("ID de la Incidencia", hel2.index)
            ).astype(str)

            det_snip = det2.set_index("nps_id")["Comment"].astype(str).fillna("")
            hel2["incident_summary"] = build_incident_display_text(hel2)
            inc_snip = hel2.set_index("incident_id")["incident_summary"].astype(str).fillna("")

            sub_links["Comentario detractor"] = (
                sub_links["nps_id"].map(det_snip).fillna("").str.slice(0, 220)
            )
            sub_links["Incidencia (descripción)"] = (
                sub_links["incident_id"].map(inc_snip).fillna("").str.slice(0, 220)
            )
            sub_links["similarity"] = sub_links["similarity"].round(3)

            st.dataframe(
                sub_links[
                    [
                        "similarity",
                        "Comentario detractor",
                        "Incidencia (descripción)",
                        "incident_id",
                        "nps_id",
                    ]
                ],
                use_container_width=True,
                height=420,
            )

    # 6) Deep-dive pack (Markdown + JSON)
    st.markdown("### 📦 LLM Deep-Dive Pack (Markdown + JSON)")
    pack = {
        "version": "1.0",
        "context": {
            "service_origin": service_origin,
            "service_origin_n1": service_origin_n1,
            "service_origin_n2": service_origin_n2,
            "date_start": str(start),
            "date_end": str(end),
            "min_similarity": float(min_sim),
        },
        "metrics_overall_weekly": overall_weekly.to_dict(orient="records"),
        "metrics_overall_daily": (
            overall_daily.to_dict(orient="records")
            if "overall_daily" in locals() and not overall_daily.empty
            else []
        ),
        "ranked_hypotheses": rank.head(20).to_dict(orient="records") if "rank" in locals() else [],
        "changepoints_by_topic": (
            cp_by_topic.to_dict(orient="records") if "cp_by_topic" in locals() else []
        ),
        "best_lag_by_topic": (
            lag_by_topic.to_dict(orient="records") if "lag_by_topic" in locals() else []
        ),
        "best_lag_days_by_topic": (
            lag_days.to_dict(orient="records")
            if "lag_days" in locals() and not lag_days.empty
            else []
        ),
        "knowledge_cache_context_entries": (
            kc_entries[
                (kc_entries["service_origin"] == str(service_origin))
                & (kc_entries["service_origin_n1"] == str(service_origin_n1))
                & (kc_entries["service_origin_n2"] == str(service_origin_n2 or ""))
            ].to_dict(orient="records")
            if "kc_entries" in locals() and not kc_entries.empty
            else []
        ),
        "business_rationale": {
            "summary": {
                "topics_analyzed": int(rationale_summary.topics_analyzed),
                "nps_points_at_risk": float(rationale_summary.nps_points_at_risk),
                "nps_points_recoverable": float(rationale_summary.nps_points_recoverable),
                "top3_incident_share": float(rationale_summary.top3_incident_share),
                "confidence_mean": float(rationale_summary.confidence_mean),
                "peak_focus_probability": float(rationale_summary.peak_focus_probability),
                "expected_nps_delta": float(rationale_summary.expected_nps_delta),
                "total_nps_impact": float(rationale_summary.total_nps_impact),
                "median_lag_weeks": (
                    None
                    if rationale_summary.median_lag_weeks != rationale_summary.median_lag_weeks
                    else float(rationale_summary.median_lag_weeks)
                ),
            },
            "priority_topics": (
                rationale_df.head(25).to_dict(orient="records") if not rationale_df.empty else []
            ),
            "attribution_chains": (
                chain_df.to_dict(orient="records") if "chain_df" in locals() and not chain_df.empty else []
            ),
            "ppt_story_md": ppt_story_md,
            "ppt_8slides_md": ppt_8slides_md,
        },
        "evidence_links_sample": (
            links_df.head(200).to_dict(orient="records") if not links_df.empty else []
        ),
        "notes": [
            "Causalidad pragmática: se prioriza temporalidad + fuerza + consistencia + plausibilidad semántica.",
            "Los links se validan con política fija: TF-IDF, similitud mínima, cruce semántico estricto y ventana temporal.",
        ],
    }
    pack_json = json.dumps(_json_sanitize(pack), ensure_ascii=False, indent=2)

    md_lines = []
    md_lines.append(f"# Deep-Dive Pack — NPS ↔ Helix ({service_origin} · {service_origin_n1})")
    if service_origin_n2:
        md_lines.append(f"**Service origin N2:** {service_origin_n2}")
    md_lines.append(f"**Ventana:** {start} → {end}")
    md_lines.append("")
    md_lines.append("## 1) Resumen del periodo")
    if not rank.empty:
        top = rank.head(3)
        for _, r in top.iterrows():
            md_lines.append(
                f"- **{r['nps_topic']}** · confidence={r['score']:.3f} · incidencias={int(r['incidents'])} · "
                f"Δ {focus_name}={float(r.get('delta_focus_rate', 0) or 0)*100:.2f} pp"
            )
    else:
        md_lines.append("- No hay ranking disponible (insuficiente señal).")
    md_lines.append("")
    md_lines.append("## 2) Evidencia cuantitativa (semanal)")
    md_lines.append(f"**Global:** % {focus_name} vs incidencias")
    md_lines.append("")
    md_lines.append(
        f"| Semana | Respuestas | {focus_name.capitalize()} | % {focus_name} | Incidencias |"
    )
    md_lines.append("|---|---:|---:|---:|---:|")
    for rec in overall_weekly.sort_values("week").to_dict(orient="records"):
        md_lines.append(
            f"| {pd.to_datetime(rec['week']).date()} | {int(rec.get('responses',0))} | {int(rec.get('focus_count',0))} | "
            f"{(float(rec.get('focus_rate',0))*100):.2f}% | {int(rec.get('incidents',0))} |"
        )
    md_lines.append("")
    if "lag_days" in locals() and (not lag_days.empty):
        md_lines.append("## 2.1) Lag estimado (diario, si aplica)")
        md_lines.append("| Tópico | Lag (días) | Corr@Lag | Puntos |")
        md_lines.append("|---|---:|---:|---:|")
        for rec in lag_days.sort_values("corr", ascending=False).head(10).to_dict(orient="records"):
            md_lines.append(
                f"| {rec.get('nps_topic','')} | {int(rec.get('best_lag_days',0) or 0)} | {float(rec.get('corr',0) or 0):.3f} | {int(rec.get('points',0) or 0)} |"
            )
        md_lines.append("")
    md_lines.append("## 3) Racional de negocio (riesgo -> recuperacion)")
    md_lines.append(f"- NPS en riesgo estimado: **{rationale_summary.nps_points_at_risk:.2f} pts**")
    md_lines.append(
        f"- NPS recuperable estimado: **{rationale_summary.nps_points_recoverable:.2f} pts**"
    )
    md_lines.append(
        f"- Impacto total atribuido en NPS: **{rationale_summary.total_nps_impact:.2f} pts**"
    )
    md_lines.append(
        f"- Probabilidad máxima del foco con incidencia: **{rationale_summary.peak_focus_probability*100:.0f}%**"
    )
    md_lines.append(
        f"- Delta NPS esperado: **{rationale_summary.expected_nps_delta:+.1f} pts**"
    )
    md_lines.append(
        f"- Concentración de incidencias en top-3: **{rationale_summary.top3_incident_share*100:.1f}%**"
    )
    if not rationale_df.empty:
        md_lines.append("")
        md_lines.append(
            "| Tópico | Touchpoint | Prioridad | Confianza | Prob. foco | Delta NPS | Impacto total | Lane | Owner | ETA (w) |"
        )
        md_lines.append("|---|---|---:|---:|---:|---:|---:|---|---|---:|")
        for rec in rationale_df.head(8).to_dict(orient="records"):
            md_lines.append(
                f"| {rec.get('nps_topic','')} | {rec.get('touchpoint','')} | {float(rec.get('priority',0.0)):.3f} | {float(rec.get('confidence',0.0)):.3f} | "
                f"{float(rec.get('focus_probability_with_incident',0.0))*100:.1f}% | {float(rec.get('nps_delta_expected',0.0)):+.2f} | {float(rec.get('total_nps_impact',0.0)):.2f} | "
                f"{rec.get('action_lane','')} | {rec.get('owner_role','')} | {int(rec.get('eta_weeks',0) or 0)} |"
            )
    if "chain_df" in locals() and not chain_df.empty:
        md_lines.append("")
        md_lines.append("### Cadenas causales defendibles")
        for rec in chain_df.to_dict(orient="records"):
            md_lines.append(
                f"- **{rec.get('nps_topic','')}** | Touchpoint: **{rec.get('touchpoint','')}** | Links: **{int(rec.get('linked_pairs',0) or 0)}**"
            )
            for inc in list(rec.get("incident_examples", []))[:5]:
                md_lines.append(f"  - Helix: {inc}")
            for com in list(rec.get("comment_examples", []))[:2]:
                md_lines.append(f"  - VoC: {com}")
    if ppt_story_md:
        md_lines.append("")
        md_lines.append("### Narrativa sugerida para comité")
        md_lines.append("```markdown")
        md_lines.append(ppt_story_md.strip())
        md_lines.append("```")
    if ppt_8slides_md:
        md_lines.append("")
        md_lines.append("### Guion estándar de 8 slides")
        md_lines.append("```markdown")
        md_lines.append(ppt_8slides_md.strip())
        md_lines.append("```")
    md_lines.append("")
    md_lines.append("## 4) Evidencia cualitativa (links)")
    if not links_df.empty:
        for rec in links_df.head(20).to_dict(orient="records"):
            md_lines.append(
                f"- sim={rec['similarity']:.3f} · tópico={rec['nps_topic']} · nps_id={rec['nps_id']} ↔ incident_id={rec['incident_id']}"
            )
    else:
        md_lines.append("- No hay links validados con la política estricta activa.")
    md_lines.append("")
    md_lines.append("## 5) Preguntas sugeridas al LLM")
    md_lines.append(
        "- ¿Hay desfase temporal consistente (incidencia → detractor) en los tópicos top?"
    )
    md_lines.append("- ¿Qué subtemas emergen en los verbatims y en las incidencias? ¿Coinciden?")
    md_lines.append(
        "- ¿Qué acciones (quick wins / fixes estructurales / instrumentación) tienen mejor ROI esperado?"
    )
    md_lines.append("")
    md_lines.append("## 6) Trazabilidad técnica")
    md_lines.append("- Fuente NPS: dataset persistido para el contexto seleccionado.")
    md_lines.append(
        "- Fuente Helix: Helix_Raw filtrado estrictamente por Company/N1 y (si aplica) N2 token-set exacto."
    )
    md_lines.append(
        f"- Linking estricto: TF-IDF + cosine similarity (min={LINK_MIN_SIMILARITY:.2f}), "
        f"top-k por incidencia={LINK_TOP_K_PER_INCIDENT}, ventana temporal ±{LINK_MAX_DAYS_APART} días."
    )

    md = "\n".join(md_lines)
    wow_prompt = build_wow_prompt(
        objective=(
            "Demostrar semanalmente como la resolucion de incidencias impacta el NPS termico "
            "y priorizar palancas de mejora continua con ownership claro."
        ),
        business_story_md=ppt_story_md or "Narrativa no disponible para esta ventana.",
        top_topics_df=rationale_df.head(10) if not rationale_df.empty else pd.DataFrame(),
        deep_dive_pack_json=pack_json,
    )

    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "Descargar Pack (Markdown)", data=md.encode("utf-8"), file_name="deep_dive_pack.md"
        )
    with c2:
        st.download_button(
            "Descargar Pack (JSON)",
            data=pack_json.encode("utf-8"),
            file_name="deep_dive_pack.json",
        )

    with st.expander("Prompt WoW para GPT (copy/paste manual)", expanded=False):
        _clipboard_copy_widget(wow_prompt, label="Copiar prompt WoW")
        st.text_area(
            "Prompt recomendado",
            value=wow_prompt,
            height=320,
        )


def main() -> None:
    # Streamlit doesn't automatically load .env.
    # This is the source of truth for service_origin_buug / service_origin_n1 / service_origin_n2 options.
    #
    # Policy:
    # - If .env does not exist, copy .env.example -> .env automatically (one-time bootstrap).
    # - Never overwrite an existing .env.
    # - Then load .env (or, if still missing, rely on runtime-injected env vars and fail-fast).
    here = Path(__file__).resolve()
    repo_root = here.parents[1]  # repo_root/app/streamlit_app.py
    env_path = repo_root / ".env"
    env_example_path = repo_root / ".env.example"
    if (not env_path.exists()) and env_example_path.exists():
        try:
            shutil.copyfile(str(env_example_path), str(env_path))
        except Exception as exc:
            raise RuntimeError(f"Failed to bootstrap .env from .env.example: {exc}") from exc

    # IMPORTANT: Streamlit may execute with different working directories depending on how it's launched.
    # We therefore resolve the .env path robustly (cwd + parent directories) and then load it.
    dotenv_path = find_dotenv(usecwd=True)
    if not dotenv_path:
        candidates = [
            env_path,  # repo root
            here.parents[0] / ".env",  # app/
        ]
        for cand in candidates:
            if cand.exists():
                dotenv_path = str(cand)
                break
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)
    else:
        # Still allow env vars injected by the runtime; Settings.from_env will fail-fast if missing.
        load_dotenv(override=False)
    settings = Settings.from_env()

    (
        data_path,
        min_n,
        service_origin,
        service_origin_n1,
        service_origin_n2,
        pop_year,
        pop_month,
        nps_group_choice,
        cache_path,
        theme_mode,
        sheet_name,
        data_ready,
        touchpoint_source,
    ) = render_sidebar(settings)

    theme = get_theme(theme_mode)
    apply_theme(theme)

    ctx_key = f"{service_origin}__{service_origin_n1}__{service_origin_n2}__{pop_year}__{pop_month}__{nps_group_choice}__{touchpoint_source}"
    if st.session_state.get("_llm_ctx") != ctx_key:
        st.session_state["_llm_ctx"] = ctx_key
        st.session_state["llm_insights"] = load_llm_insights_for_context(
            settings, service_origin=service_origin, service_origin_n1=service_origin_n1
        )

    st.markdown(
        """
<div class="nps-card nps-card--flat">
  <div style="font-size:28px; font-weight:900;">NPS Lens</div>
  <div class="nps-muted" style="margin-top:4px;">
    Lectura de negocio y accionable del NPS térmico (Senda · México)
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    pills(
        [
            f"Service origin: {service_origin}",
            f"N1: {service_origin_n1}",
            f"N2: {service_origin_n2 or '-'}",
            f"Año: {pop_year}",
            f"Mes: {month_format_es(pop_month)}",
        ]
    )
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    if not data_ready:
        st.info(
            "No hay dataset cargado para este **contexto** (geografía + canal). "
            "Ve a la barra lateral, sube el Excel y pulsa **Importar / actualizar dataset**."
        )
        st.stop()

    store_dir = settings.data_dir / "store"

    # Global population time window (Año/Mes) applied everywhere.
    pop_date_start, pop_date_end, pop_month_filter = population_date_window(pop_year, pop_month)

    st.divider()

    with st.expander("📘 Qué estás viendo", expanded=False):
        st.markdown(
            "Este dashboard está pensado para una lectura de **negocio** del NPS térmico. "
            "Empieza en **Resumen**, luego ve a **Drivers** para priorizar y usa **Insights LLM** "
            "para convertir hallazgos en narrativa y acciones.\n\n"
            "**Tip:** en cada sección verás una breve explicación de *qué significa* "
            "y *qué decisión habilita*."
        )

    t_situacion, t_prioridades, t_llm, t_datos = st.tabs(
        ["📊 Situación", "🎯 Prioridades", "✨ Insights GPT", "🧾 Datos"]
    )

    with t_situacion:
        df_resumen = load_context_df(
            store_dir,
            service_origin,
            service_origin_n1,
            service_origin_n2,
            nps_group_choice,
            VIEW_COLUMNS["resumen"] or tuple(),
            date_start=pop_date_start,
            date_end=pop_date_end,
            month_filter=pop_month_filter,
        )
        st.markdown(
            "<div class='nps-card nps-card--flat'>"
            "<b>Flujo recomendado</b><br/>"
            "<span class='nps-muted'>Primero revisa la situación del periodo y después baja al vínculo "
            "incidencias ↔ NPS para definir prioridades y generar la presentación.</span>"
            "</div>",
            unsafe_allow_html=True,
        )
        s1, s2 = st.tabs(["Panorama del periodo", "Incidencias ↔ NPS"])
        with s1:
            page_executive(
                df_resumen,
                theme,
                store_dir,
                service_origin,
                service_origin_n1,
                service_origin_n2,
            )
        with s2:
            page_nps_helix_linking(
                nps_df=df_resumen,
                store_dir=store_dir,
                service_origin=service_origin,
                service_origin_n1=service_origin_n1,
                service_origin_n2=service_origin_n2,
                nps_group_choice=nps_group_choice,
                settings=settings,
                theme_mode=theme_mode,
                touchpoint_source=touchpoint_source,
            )

    with t_prioridades:
        df_prior = load_context_df(
            store_dir,
            service_origin,
            service_origin_n1,
            service_origin_n2,
            nps_group_choice,
            VIEW_COLUMNS["drivers"],
            date_start=pop_date_start,
            date_end=pop_date_end,
            month_filter=pop_month_filter,
        )
        p1, p2, p3 = st.tabs(["Drivers", "Temas de clientes", "Segmentos"])
        with p1:
            page_drivers(df_prior, theme, min_n=min_n)
        with p2:
            df_texto = load_context_df(
                store_dir,
                service_origin,
                service_origin_n1,
                service_origin_n2,
                nps_group_choice,
                VIEW_COLUMNS["texto"],
                date_start=pop_date_start,
                date_end=pop_date_end,
                month_filter=pop_month_filter,
            )
            page_text(df_texto, theme)
        with p3:
            page_cohorts(df_prior, theme)
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            section(
                "Comparativa de periodos", "Evolución frente al periodo base para validar avance."
            )
            page_comparisons(df_prior, theme)

    with t_llm:
        df_llm = load_context_df(
            store_dir,
            service_origin,
            service_origin_n1,
            service_origin_n2,
            nps_group_choice,
            VIEW_COLUMNS["llm"],
            date_start=pop_date_start,
            date_end=pop_date_end,
            month_filter=pop_month_filter,
        )
        page_llm(df_llm, settings=settings, min_n=min_n, cache_path=cache_path)

    with t_datos:
        df_datos = load_context_df(
            store_dir,
            service_origin,
            service_origin_n1,
            service_origin_n2,
            nps_group_choice,
            tuple(),
            date_start=pop_date_start,
            date_end=pop_date_end,
            month_filter=pop_month_filter,
        )
        helix_store = HelixIncidentStore(settings.data_dir / "helix")
        hctx = DatasetContext(
            service_origin=service_origin,
            service_origin_n1=service_origin_n1,
            service_origin_n2=_normalize_empty_n2(service_origin_n2),
        )
        hstored = helix_store.get(hctx)
        helix_df = helix_store.load_df(hstored) if hstored is not None else None
        page_quality(df_datos, helix_df=helix_df)


if __name__ == "__main__":
    main()
