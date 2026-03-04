from __future__ import annotations

import contextlib
import json
import base64
import hashlib
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from nps_lens.analytics.causal import best_effort_ate_logit
from nps_lens.analytics.changepoints import detect_nps_changepoints
from nps_lens.analytics.drivers import driver_table
from nps_lens.analytics.journey import build_routes
from nps_lens.analytics.opportunities import rank_opportunities
from nps_lens.analytics.text_mining import extract_topics
from nps_lens.config import Settings
from nps_lens.core.store import DatasetContext, DatasetStore
from nps_lens.ingest.nps_thermal import read_nps_thermal_excel
from nps_lens.ui.business import default_windows, driver_delta_table, slice_by_window
from nps_lens.ui.charts import (
    chart_daily_mix_business,
    chart_daily_kpis,
    chart_daily_volume,
    chart_daily_score_semaforo,
    chart_cohort_heatmap,
    chart_driver_bar,
    chart_driver_delta,
    chart_nps_trend,
    chart_topic_bars,
)
from nps_lens.ui.components import card, kpi, pills, section
from nps_lens.ui.narratives import (
    build_executive_story,
    compare_periods,
    executive_summary,
    explain_opportunities,
    explain_topics,
)
from nps_lens.ui.theme import Theme, apply_theme, get_theme

st.set_page_config(page_title="NPS Lens — Senda MX", layout="wide")

REPO_ROOT = Path(__file__).resolve().parents[1]

LLM_SYSTEM_PROMPT = """Eres el analista oficial de Insights para BBVA Banca de Empresas. Tu trabajo es:

1. Leer un "LLM Deep-Dive Pack" (Markdown o JSON) generado por la plataforma de Voz del Cliente.
2. Detectar insights no obvios y causas raíz plausibles basadas únicamente en la evidencia provista (cuantitativa y/o cualitativa), sin inventar datos ni afirmar hechos no sustentados.
3. Devolver SOLO un JSON válido (sin texto adicional) con el esquema requerido.

REGLA CRÍTICA — SOLO JSON:
- Tu respuesta debe ser exclusivamente un objeto JSON (sin explicaciones, sin títulos, sin Markdown, sin bloques de código).
- Usa comillas dobles estándar " (no comillas tipográficas).
- Sin trailing commas.
- No incluyas comillas dobles dentro de strings. Si necesitas comillas en un texto, escápalas como \".
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
    "Devuelve SOLO el JSON del esquema (sin texto adicional). Enfocate en causas raiz y acciones priorizadas.",
    "Identifica 3 hipotesis causales no obvias (con checks) y propone fixes/experimentos rapidos con owners y ETA.",
    "Que palancas atacaria un director esta semana? Prioriza 3 acciones, que medir y riesgos por falta de evidencia.",
    "Agrupa los verbatims en temas, explica el impacto en negocio y sugiere instrumentacion para confirmarlo.",
]



DEFAULT_OPP_DIMS = ("Canal", "Palanca", "Subpalanca", "UsuarioDecisión", "Segmento")


@st.cache_data(show_spinner=False)
def load_context_df(
    store_dir: Path,
    geo: str,
    channel: str,
    columns: tuple[str, ...],
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
) -> pd.DataFrame:
    """Load dataset for a context with column projection.

    Uses the DatasetStore (JSONL source of truth + partitioned Parquet cache).
    The `columns` tuple is part of the cache key, so each view can request only
    the columns it needs (min CPU/RAM).
    """
    store = DatasetStore(store_dir)
    stored = store.get(DatasetContext(geo=geo, channel=channel))
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

    # Lightweight dtype optimization (safe even with partial columns)
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    if "NPS" in df.columns:
        df["NPS"] = pd.to_numeric(df["NPS"], errors="coerce")

    for c in ["Canal", "Palanca", "Subpalanca", "UsuarioDecisión", "Segmento", "NPS Group"]:
        if c in df.columns:
            df[c] = df[c].astype("category")

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
    "drivers": ("Fecha", "NPS", "NPS Group", "Canal", "Palanca", "Subpalanca", "UsuarioDecisión", "Segmento"),
    "texto": ("Fecha", "NPS", "NPS Group", "Comment", "Canal", "Palanca", "Subpalanca", "UsuarioDecisión", "Segmento"),
    "journey": ("Fecha", "NPS", "NPS Group", "Comment", "Canal", "Palanca", "Subpalanca"),
    "alertas": ("Fecha", "NPS", "NPS Group", "Canal", "Palanca", "Subpalanca"),
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



# Column sets per chart (granular manifest). Each chart requests only what it needs.
CHART_COLUMNS = {
    "trend_weekly": ("Fecha", "NPS"),
    "daily_mix": ("Fecha", "NPS"),
    "daily_volume": ("Fecha", "NPS"),
    "daily_kpis": ("Fecha", "NPS"),
    "daily_semaforo": ("Fecha", "NPS"),
    "daily_llm": ("Fecha", "NPS", "NPS Group", "Comment", "Palanca", "Subpalanca", "Canal", "UsuarioDecisión", "Segmento"),
    "drivers_bar": ("NPS", "Palanca", "Subpalanca", "Canal", "UsuarioDecisión", "Segmento"),
    "drivers_delta": ("Fecha", "NPS", "Palanca", "Subpalanca", "Canal", "UsuarioDecisión", "Segmento"),
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




def load_llm_insights_for_context(settings: Settings, geo: str, channel: str) -> list[dict[str, Any]]:
    """Load persisted LLM insights for the selected context."""
    from nps_lens.llm import KnowledgeCache

    kc = KnowledgeCache.for_context(settings.knowledge_dir, geo=geo, channel=channel)
    data = kc.load()
    entries = data.get("entries", [])
    # entries are dicts; keep as-is
    return list(entries)

@st.cache_data(show_spinner=False)
def cached_driver_table(df: pd.DataFrame, dimension: str) -> pd.DataFrame:
    stats = driver_table(df, dimension=dimension)
    stats_df = pd.DataFrame([s.__dict__ for s in stats])
    if "gap_vs_overall" not in stats_df.columns:
        raise KeyError("gap_vs_overall missing from driver_table output")
    return stats_df

@st.cache_data(show_spinner=False)
def cached_rank_opportunities(df: pd.DataFrame, min_n: int):
    dims = [d for d in DEFAULT_OPP_DIMS if d in df.columns]
    return rank_opportunities(df, dimensions=dims, min_n=min_n)

@st.cache_data(show_spinner=False)
def cached_extract_topics(texts, n_clusters: int = 8):
    return extract_topics(texts, n_clusters=n_clusters)

@st.cache_data(show_spinner=False)
def cached_build_routes(df: pd.DataFrame):
    return build_routes(df)

@st.cache_data(show_spinner=False)
def cached_detect_changepoints(df: pd.DataFrame):
    return detect_nps_changepoints(df)



def _copy_to_clipboard(payload: str, *, toast: str = "Copiado") -> None:
    """Copy text to clipboard via a tiny JS snippet (Streamlit has no native clipboard API)."""
    # Use JSON encoding to safely escape quotes/newlines.
    js = (
        "<script>"
        f"navigator.clipboard.writeText({json.dumps(payload)});"
        "</script>"
    )
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
    <div style=\"display:flex; gap:12px; align-items:center;\">
      <button
        id=\"nps_copy_{uid}\"
        style=\"
          width: 100%;
          padding: 10px 14px;
          border-radius: 12px;
          border: 0;
          cursor: pointer;
          font-weight: 650;
          background: var(--nps-accent, #1f77ff);
          color: white;
        \"
        title=\"Copiar al portapapeles\"
      >{label}</button>
      <span id=\"nps_copy_msg_{uid}\" style=\"font-size:12px; color: var(--nps-muted, #6b7280);\"></span>
    </div>
    <script>
      (function() {{
        const btn = document.getElementById(\"nps_copy_{uid}\");
        const msg = document.getElementById(\"nps_copy_msg_{uid}\");
        const txt = atob(\"{payload_b64}\");
        async function doCopy() {{
          try {{
            await navigator.clipboard.writeText(txt);
            msg.textContent = \"Copiado ✅\";
            const old = btn.textContent;
            btn.textContent = \"Copiado\";
            setTimeout(() => {{ btn.textContent = old; msg.textContent = \"\"; }}, 1800);
          }} catch (e) {{
            msg.textContent = \"No se pudo copiar. Selecciona el texto y usa Ctrl/Cmd+C.\";
          }}
        }}
        btn.addEventListener(\"click\", doCopy);
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
            "driver \"Funcionamiento\""  (good)
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


def _parse_json_with_repair(text: str) -> tuple[Optional[dict[str, Any]], Optional[str], Optional[str]]:
    """Parse JSON from pasted text. Repairs automatically when possible.

    Returns: (obj, repaired_json_text, error_message)
    """
    import json

    if not text or not text.strip():
        return None, None, None

    # First try: strict parse after minimal normalization
    candidate = _repair_json_text(text)
    try:
        obj = json.loads(candidate)
        if isinstance(obj, dict):
            return obj, candidate, None
        return None, None, "El JSON detectado no es un objeto (dict)."
    except json.JSONDecodeError as e:
        return None, candidate, f"JSON invalido (linea {e.lineno}, col {e.colno}): {e.msg}"
    except Exception as e:
        return None, candidate, f"JSON invalido: {e}"


def _try_parse_json(text: str) -> Optional[dict[str, Any]]:
    """Backward-compatible wrapper used in previews."""
    obj, _, _ = _parse_json_with_repair(text)
    return obj


def _validate_insight_schema(obj: dict[str, Any]) -> tuple[bool, list[str]]:
    required = [
        "schema_version",
        "insight_id",
        "title",
        "executive_summary",
        "confidence",
        "severity",
        "root_causes",
        "segments_most_affected",
        "journey_route",
        "assumptions",
        "risks",
        "next_questions",
        "tags",
    ]
    missing = [k for k in required if k not in obj]
    if missing:
        return False, [f"Faltan campos: {', '.join(missing)}"]

    if not isinstance(obj.get("root_causes"), list) or len(obj["root_causes"]) == 0:
        return False, ["root_causes debe ser una lista no vacía"]

    return True, []


def _render_llm_insights(theme: Theme) -> None:
    insights = st.session_state.get("llm_insights", [])
    if not insights:
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

    for ins in insights:
        actions_html = ""
        try:
            actions = []
            for rc in ins.get("root_causes", [])[:3]:
                for a in rc.get("actions", [])[:2]:
                    action_txt = (
                        f"• {a.get('action', '')} "
                        f"<span class='nps-muted'>({a.get('owner', '')}, {a.get('eta', '')})</span>"
                    )
                    actions.append(action_txt)
            if actions:
                actions_html = (
                    "<div style='margin-top:10px'><b>Acciones sugeridas</b><br/>"
                    + "<br/>".join(actions)
                    + "</div>"
                )
        except Exception:
            actions_html = ""

        body = (
            f"<div style='font-size:18px; font-weight:800'>{ins.get('title','')}</div>"
            f"<div class='nps-muted' style='margin-top:6px'>{ins.get('executive_summary','')}</div>"
            f"{actions_html}"
        )
        card("Insight LLM", body, flat=False)

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    if st.button("🧹 Limpiar insights integrados", use_container_width=True):
        st.session_state["llm_insights"] = []
        st.rerun()


def render_sidebar(  # noqa: PLR0915
    settings: Settings,
) -> tuple[Optional[Path], int, str, str, Path, str, Optional[str], bool]:
    """Single-source sidebar: Context (geo/channel) -> dataset -> controls.

    - Context is the top-level hierarchy.
    - Only Excel upload is supported (no local path, no examples).
    - Uploaded data is normalized and persisted as JSONL per (geo, channel).
    - The app always reads from the persisted JSONL to ensure consistency.
    """
    store = DatasetStore(settings.data_dir / "store")

    # Build option sets: prefer existing contexts, but allow expanding the list.
    existing = store.list_contexts()
    geo_options = sorted({*(c.geo for c in existing), settings.default_geo, "ES", "CO", "PE", "AR"})
    channel_options = sorted({*(c.channel for c in existing), settings.default_channel, "Gema"})

    # Default context: first stored dataset, else Settings defaults.
    if "_ctx" not in st.session_state:
        ctx0 = store.default_context() or DatasetContext(settings.default_geo, settings.default_channel)
        st.session_state["_ctx"] = {"geo": ctx0.geo, "channel": ctx0.channel}

    ctx_state = st.session_state["_ctx"]
    cur_geo = str(ctx_state.get("geo", settings.default_geo))
    cur_channel = str(ctx_state.get("channel", settings.default_channel))

    defaults = st.session_state.get(
        "_controls",
        {
            "theme_mode": "light",
            "min_n": 200,
        },
    )

    with st.sidebar:
        st.header("Contexto")
        geo = st.selectbox("Geografía", geo_options, index=geo_options.index(cur_geo) if cur_geo in geo_options else 0)
        channel = st.selectbox(
            "Canal",
            channel_options,
            index=channel_options.index(cur_channel) if cur_channel in channel_options else 0,
        )

        # Persist context selection
        st.session_state["_ctx"] = {"geo": geo, "channel": channel}

        st.divider()
        st.header("Dataset (Excel)")
        st.caption("Este Excel pertenece al contexto seleccionado (geografía + canal).")

        up = st.file_uploader("Subir Excel NPS térmico (.xlsx)", type=["xlsx", "xlsm", "xls"])
        sheet_name = (st.text_input("Hoja (opcional)", value="") or None)

        # Show current dataset status if exists
        ctx = DatasetContext(geo=geo, channel=channel)
        stored = store.get(ctx)
        if stored is not None:
            meta = json.loads(stored.meta_path.read_text(encoding="utf-8"))
            st.success(f"Dataset activo: {meta.get('rows', '?'):,} filas · actualizado {meta.get('updated_at_utc', '?')}")
        else:
            st.info("No hay dataset persistido para este contexto. Sube el Excel para empezar.")

        if st.button("Importar / actualizar dataset", type="primary", use_container_width=True):
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
                    geo=geo,
                    channel=channel,
                    sheet_name=sheet_name,
                )
                # Persist normalized dataframe as the single source of truth
                store.save_df(ctx, res.df, source=f"excel:{upload_path.name}")
                st.session_state["_last_import_issues"] = [asdict(i) for i in res.issues]
                st.rerun()

        # Surface validation issues after import (if any)
        issues = st.session_state.get("_last_import_issues") or []
        if issues:
            with st.expander("Avisos / errores del último import", expanded=False):
                st.json(issues)

        
        st.divider()
        st.header("Experiencia")
        theme_mode = st.selectbox(
            "Modo visual",
            ["light", "dark"],
            index=0 if defaults["theme_mode"] == "light" else 1,
        )
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

    # Resolve dataset path for current context
    stored = store.get(DatasetContext(geo=geo, channel=channel))
    data_path = stored.path if stored is not None else None
    data_ready = stored is not None
    return (
        data_path,
        int(st.session_state.get("_controls", defaults)["min_n"]),
        geo,
        channel,
        settings.knowledge_dir,
        theme_mode,
        sheet_name,
        data_ready,
    )

def page_executive(df: pd.DataFrame, theme: Theme, store_dir: Path, geo: str, channel: str) -> None:
    section(
        "Resumen ejecutivo",
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
                st.plotly_chart(fig, use_container_width=True)

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
                    geo,
                    channel,
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
                st.plotly_chart(fig_mix, use_container_width=True)
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                fig_vol = chart_daily_volume(df_win, theme, days=int(days))
                if fig_vol is not None:
                    st.plotly_chart(fig_vol, use_container_width=True)
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                st.caption("Lectura ejecutiva diaria: NPS clásico (promotores - detractores) y % detractores.")
                fig_k = chart_daily_kpis(df_win, theme, days=int(days))
                if fig_k is not None:
                    st.plotly_chart(fig_k, use_container_width=True)

                with st.expander("WoW: entender los días que importan (LLM)", expanded=False):
                    st.caption(
                        "Selecciona un día extremo (muy bueno o muy malo) y genera un prompt "
                        "para pedirle al GPT una explicación con hipótesis y acciones."
                    )

                    df_llm_win = load_context_df(
                        store_dir,
                        geo,
                        channel,
                        CHART_COLUMNS["daily_llm"],
                        date_start=str(start_day.date()) if end_day is not None else None,
                        date_end=str(end_day.date()) if end_day is not None else None,
                    )
                    metrics = _daily_metrics(df_llm_win, days=int(days))
                    if metrics.empty:
                        st.info("No hay suficientes datos diarios para construir el asistente.")
                    else:
                        worst = metrics.sort_values(["det_pct", "n"], ascending=[False, False]).head(3)
                        best = metrics.sort_values(["classic_nps", "n"], ascending=[False, False]).head(3)
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
                                verb = (
                                    slice_df["Comment"].dropna().astype(str).head(12).tolist()
                                )

                            # Top levers that day (if available)
                            tops = []
                            if "Palanca" in slice_df.columns:
                                vc = slice_df["Palanca"].astype(str).value_counts().head(5)
                                tops = [f"{idx} (n={int(v)})" for idx, v in vc.items()]

                            prompt = (
                                "Necesito que analices un día extremo de NPS térmico y me devuelvas:\n"
                                "1) Resumen ejecutivo (max 10 líneas)\n"
                                "2) JSON válido con el esquema de NPS Lens (schema_version=1.0)\n\n"
                                f"Contexto:\n- geo: MX\n- channel: Senda\n- día: {chosen_day.strftime('%Y-%m-%d')}\n\n"
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
                    geo,
                    channel,
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
                st.plotly_chart(fig2, use_container_width=True)


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
    section("Informe ejecutivo", "Copy/paste listo para comité / daily.")

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
    st.text_area("Informe ejecutivo", report_md, height=260)
    st.download_button(
        "Descargar informe .md",
        data=report_md.encode("utf-8"),
        file_name="informe_ejecutivo_nps_lens.md",
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
        st.plotly_chart(fig, use_container_width=True)
    with st.expander("Ver tabla de deltas"):
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
    st.plotly_chart(fig, use_container_width=True)

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
                st.plotly_chart(fig, use_container_width=True)

        with st.expander("Ver tabla detallada"):
            st.dataframe(stats_df.head(30), use_container_width=True)

    with right:
        section("Oportunidades priorizadas", "Ranking por impacto estimado x confianza.")
        opps = cached_rank_opportunities(df, min_n=min_n)
        opp_df = pd.DataFrame([o.__dict__ for o in opps])

        if opp_df.empty:
            st.warning("No se detectaron oportunidades con el umbral actual.")
        else:
            bullets = explain_opportunities(opp_df, max_items=5)
            st.markdown(
                (
                    "<div class='nps-card'><ul>"
                    + "".join([f"<li>{b}</li>" for b in bullets])
                    + "</ul></div>"
                ),
                unsafe_allow_html=True,
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
            st.plotly_chart(fig, use_container_width=True)

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


def page_journey(df: pd.DataFrame) -> None:
    st.subheader("Journey & causas raíz (MVP)")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "Esta vista construye rutas tipo <b>palanca → subpalanca → tema</b> y las ordena por "
        "<b>frecuencia</b> y <b>asociación con detractores</b>. "
        "Es una aproximación best-effort: sirve para priorizar investigaciones, "
        "no como prueba definitiva."
        "</div>",
        unsafe_allow_html=True,
    )

    routes = cached_build_routes(df)
    routes_df = pd.DataFrame([r.__dict__ for r in routes])
    if routes_df.empty:
        st.info("No se pudieron construir rutas con los datos actuales.")
        return

    section("Top rutas por score", "Rutas tipo palanca -> subpalanca -> tema.")
    st.dataframe(routes_df.head(25), use_container_width=True)

    st.markdown(
        "<div class='nps-card'><b>Cómo leerlo:</b> una ruta con score alto suele "
        "ser una combinación "
        "repetida y con mayor tasa de detractores. Úsala para abrir tickets/hipótesis." 
        "</div>",
        unsafe_allow_html=True,
    )


def page_changes(df: pd.DataFrame) -> None:
    st.subheader("Alertas: cambios relevantes en el tiempo")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "Detecta puntos donde el NPS cambia de nivel para una palanca seleccionada. "
        "Útil para vincular con releases, incidencias o campañas en esa ventana." 
        "</div>",
        unsafe_allow_html=True,
    )

    levers = sorted(df["Palanca"].astype(str).fillna("(vacío)").unique())[:80]
    lever = st.selectbox("Palanca", levers)
    cp = detect_nps_changepoints(df, dim_col="Palanca", value=lever, freq="D", pen=8.0)

    if cp is None:
        st.info("No hay suficientes datos para detectar cambios para esa selección.")
        return

    st.markdown(
        "<div class='nps-card'>"
        f"<div><b>Palanca:</b> {cp.value}</div>"
        f"<div><b>Puntos detectados:</b> {', '.join([str(p) for p in cp.points])}</div>"
        f"<div class='nps-muted' style='margin-top:6px'>{cp.note}</div>"
        "</div>",
        unsafe_allow_html=True,
    )



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
    slice_df = df.loc[df[selected.dimension].astype(str) == selected.value].copy()
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
    from nps_lens.llm import build_insight_pack, export_pack, render_pack_markdown

    causal = best_effort_ate_logit(
        df=df,
        treatment_col=selected.dimension,
        treatment_value=selected.value,
        control_cols=["Canal", "Palanca", "Subpalanca"],
    )

    context = {
        "geo": str(settings.default_geo),
        "channel": str(settings.default_channel),
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
        "- Usa comillas dobles normales (\"), sin comillas tipograficas.\n"
        "- Sin trailing commas. No uses NaN/Infinity/None: usa null si aplica.\n\n"
        "DEEP-DIVE PACK\n"
        f"{md}"
    )

    c1, c2 = st.columns([2, 1])
    with c1:
        # Use a browser-side button to guarantee clipboard copy (requires user gesture).
        _clipboard_copy_widget(prompt, label="Copiar prompt")
        with st.expander("Ver prompt (fallback manual)", expanded=False):
            st.text_area(
                "Prompt",
                value=prompt,
                height=220,
                help="Si el navegador bloquea la copia automática, selecciona el texto y usa Ctrl/Cmd+C.",
            )
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

    ok, errs = _validate_insight_schema(parsed)
    if not ok:
        st.error("El JSON no cumple el esquema: " + "; ".join(errs))
        return

    insights = list(st.session_state.get("llm_insights", []))
    insights = [i for i in insights if i.get("insight_id") != parsed.get("insight_id")]
    insights.insert(0, parsed)
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
    from nps_lens.llm import KnowledgeCache, stable_signature

    geo = str(context.get('geo') or settings.default_geo)
    channel = str(context.get('channel') or settings.default_channel)
    kc = KnowledgeCache.for_context(settings.knowledge_dir, geo=geo, channel=channel)
    sig = stable_signature(context=context, title=pack.title)
    record = {
        "signature": sig,
        "insight_id": pack.insight_id,
        "title": pack.title,
        "context": context,
        "llm_answer": answer,
        "created_at_utc": pack.created_at.isoformat() + "Z",
        "tags": [
            settings.default_geo,
            settings.default_channel,
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

    md, out, pack, context = _llm_build_pack(df, settings, selected, slice_df, out_dir=cache_path.parent / "packs")
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

        # If we repaired anything, update the textbox so the user sees the canonical JSON.
        if repaired and repaired.strip() and repaired.strip() != raw.strip():
            st.session_state["llm_answer"] = repaired

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
# Refresh UI so the integrated insights section updates immediately.
        st.rerun()



def page_quality(df: pd.DataFrame) -> None:
    st.subheader("Datos & calidad")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "Esta sección es técnica, pero útil cuando los números no cuadran: "
        "faltantes, duplicados y columnas clave."
        "</div>",
        unsafe_allow_html=True,
    )

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


def main() -> None:
    settings = Settings.from_env()

    (
        data_path,
        min_n,
        geo,
        channel,
        cache_path,
        theme_mode,
        sheet_name,
        data_ready,
    ) = render_sidebar(settings)

    theme = get_theme(theme_mode)
    apply_theme(theme)

    ctx_key = f"{geo}__{channel}"
    if st.session_state.get("_llm_ctx") != ctx_key:
        st.session_state["_llm_ctx"] = ctx_key
        st.session_state["llm_insights"] = load_llm_insights_for_context(settings, geo=geo, channel=channel)

    st.markdown(
        """
<div class="nps-card nps-card--flat">
  <div style="font-size:28px; font-weight:900;">NPS Lens</div>
  <div class="nps-muted" style="margin-top:4px;">
    Lectura ejecutiva y accionable del NPS térmico (Senda · México)
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    pills([f"Geo: {geo}", f"Canal: {channel}", f"Modo: {theme_mode}"])
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)


    if not data_ready:
        st.info(
            "No hay dataset cargado para este **contexto** (geografía + canal). "
            "Ve a la barra lateral, sube el Excel y pulsa **Importar / actualizar dataset**."
        )
        st.stop()

    store_dir = settings.data_dir / "store"


    st.divider()

    with st.expander("📘 Qué estás viendo", expanded=False):
        st.markdown(
            "Este dashboard está pensado para una lectura **ejecutiva** del NPS térmico. "
            "Empieza en **Resumen**, luego ve a **Drivers** para priorizar y usa **Insights LLM** "
            "para convertir hallazgos en narrativa y acciones.\n\n"

            "**Tip:** en cada sección verás una breve explicación de *qué significa* "
            "y *qué decisión habilita*."
        )

    t_resumen, t_drivers, t_texto, t_journey, t_alertas, t_llm, t_datos = st.tabs(
        [
            "🏠 Resumen",
            "🎯 Drivers",
            "📝 Texto",
            "🧭 Journey",
            "🚨 Alertas",
            "✨ Insights LLM",
            "🧾 Datos",
        ]
    )

    with t_resumen:
        df = load_context_df(store_dir, geo, channel, VIEW_COLUMNS["resumen"] or tuple())
        st.markdown(
            "<div class='nps-card nps-card--flat'>"
            "<b>Cómo leer esta pestaña</b><br/>"
            "<span class='nps-muted'>Empieza por el resumen, luego compara ventanas de tiempo "
            "y revisa cohortes si necesitas aislar el problema por segmento/canal.</span>"
            "</div>",
            unsafe_allow_html=True,
        )
        s1, s2, s3 = st.tabs(["Resumen", "Comparativas", "Cohortes"])
        with s1:
            page_executive(df, theme, store_dir, geo, channel)
        with s2:
            page_comparisons(df, theme)
        with s3:
            page_cohorts(df, theme)

    with t_drivers:
        df = load_context_df(store_dir, geo, channel, VIEW_COLUMNS["drivers"])
        page_drivers(df, theme, min_n=min_n)

    with t_texto:
        df = load_context_df(store_dir, geo, channel, VIEW_COLUMNS["texto"])
        page_text(df, theme)

    with t_journey:
        df = load_context_df(store_dir, geo, channel, VIEW_COLUMNS["journey"])
        page_journey(df)

    with t_alertas:
        df = load_context_df(store_dir, geo, channel, VIEW_COLUMNS["alertas"])
        page_changes(df)

    with t_llm:
        df = load_context_df(store_dir, geo, channel, VIEW_COLUMNS["llm"])
        page_llm(df, settings=settings, min_n=min_n, cache_path=cache_path)

    with t_datos:
        df = load_context_df(store_dir, geo, channel, tuple())
        page_quality(df)




if __name__ == "__main__":
    main()