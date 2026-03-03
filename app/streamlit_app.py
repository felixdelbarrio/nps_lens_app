from __future__ import annotations

import contextlib
import json
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
from nps_lens.ui.business import default_windows, driver_delta_table, slice_by_window
from nps_lens.ui.charts import (
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
DEFAULT_NPS_CSV = REPO_ROOT / "data" / "examples" / "nps_thermal_senda_mx_sample.csv"

LLM_SYSTEM_PROMPT = (
    "Eres el analista oficial de NPS Lens para canal movil Empresas. "
    "Tu trabajo es leer un 'LLM Deep-Dive Pack' (Markdown o JSON) y devolver: "
    "(1) un resumen ejecutivo en lenguaje de negocio (max 10 lineas) y "
    "(2) un JSON estricto valido (sin markdown) con el esquema requerido. "
    "No inventes datos; si falta evidencia, indicalo en risks con 'insufficient_evidence' y baja confianza. "
    "El JSON debe ser el ultimo bloque y no debe tener trailing commas."
)


LLM_BUSINESS_QUESTIONS = [
    "Devuelve un resumen ejecutivo (max 10 lineas) y el JSON del esquema. Enfocate en causas raiz y acciones priorizadas.",
    "Identifica 3 hipotesis causales no obvias (con checks) y propone fixes/experimentos rapidos con owners y ETA.",
    "Que palancas atacaria un director esta semana? Prioriza 3 acciones, que medir y riesgos por falta de evidencia.",
    "Agrupa los verbatims en temas, explica el impacto en negocio y sugiere instrumentacion para confirmarlo.",
]



DEFAULT_OPP_DIMS = ("Canal", "Palanca", "Subpalanca", "UsuarioDecisión", "Segmento")


@st.cache_data(show_spinner=False)
def load_nps_data(path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Load NPS data and optimize dtypes (cached).

    This reduces cold-start time and memory usage, and avoids re-reading on every rerun.
    """
    if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        df = pd.read_excel(path, sheet_name=sheet_name or 0)
    else:
        df = pd.read_csv(path, low_memory=False)

    df["Fecha"] = pd.to_datetime(df.get("Fecha"), errors="coerce")
    df["NPS"] = pd.to_numeric(df.get("NPS"), errors="coerce")

    # Defensive: ensure taxonomy columns exist
    for col in ["Palanca", "Subpalanca", "Canal", "UsuarioDecisión", "Segmento", "Comment"]:
        if col not in df.columns:
            df[col] = None

    return optimize_df(df)


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

def _try_parse_json(text: str) -> Optional[dict[str, Any]]:
    """Best-effort parse of a JSON object embedded in free text."""
    import json

    if not text or not text.strip():
        return None

    # First try: whole text is JSON
    from contextlib import suppress

    with suppress(Exception):
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj

    # Second try: find first balanced {...}
    s = text
    start = s.find("{")
    if start == -1:
        return None

    depth = 0
    for i in range(start, len(s)):
        ch = s[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = s[start : i + 1]
                try:
                    obj = json.loads(candidate)
                    return obj if isinstance(obj, dict) else None
                except Exception:
                    return None
    return None


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
) -> tuple[Path, int, str, str, Path, str, Optional[str], bool]:
    """Sidebar controls with form submit to prevent rerun storms."""
    defaults = st.session_state.get(
        "_controls",
        {
            "theme_mode": "light",
            "source": "Ejemplo",
            "data_path_str": str(DEFAULT_NPS_CSV),
            "sheet_name": None,
            "min_n": 200,
            "geo": settings.default_geo,
            "channel": settings.default_channel,
        },
    )

    with st.sidebar:
        st.header("Experiencia")
        theme_mode = st.selectbox(
            "Modo visual",
            ["light", "dark"],
            index=0 if defaults["theme_mode"] == "light" else 1,
        )

        st.divider()
        st.header("Datos")
        source = st.radio(
            "Fuente",
            ["Subir Excel", "Ruta local", "Ejemplo"],
            index=["Subir Excel", "Ruta local", "Ejemplo"].index(
                defaults["source"]
            ),
            horizontal=False,
        )

        sheet_name: Optional[str] = None
        data_path_str = defaults["data_path_str"]

        # Upload / path selection (kept outside the form submit because file upload
        # already causes a rerun)
        if source == "Subir Excel":
            up = st.file_uploader("Excel NPS térmico (.xlsx)", type=["xlsx", "xlsm", "xls"])
            if up is None:
                st.caption(
                    "Sugerencia: sube tu Excel (p.ej. \"NPS Térmico Senda - "
                    "01Enero-02Febrero.xlsx\")."
                )
                data_path_str = str(DEFAULT_NPS_CSV)
            else:
                uploads_dir = REPO_ROOT / "data" / "uploads"
                uploads_dir.mkdir(parents=True, exist_ok=True)
                out_path = uploads_dir / up.name
                out_path.write_bytes(up.getbuffer())
                data_path_str = str(out_path)
                sheet_name = (
                    st.text_input(
                        "Hoja (opcional)",
                        value=defaults.get("sheet_name") or "",
                    )
                    or None
                )
        elif source == "Ruta local":
            data_path_str = st.text_input("Ruta a CSV/XLSX", value=data_path_str)
            sheet_name = (
                st.text_input(
                    "Hoja (solo Excel)",
                    value=defaults.get("sheet_name") or "",
                )
                or None
            )
        else:
            data_path_str = str(DEFAULT_NPS_CSV)
            sheet_name = None


        # Data loading gate: avoid heavy IO and parsing on cold-start until user confirms.
        data_key = f"{source}|{data_path_str}|{sheet_name or ''}"
        if st.session_state.get("_data_key") != data_key:
            st.session_state["_data_key"] = data_key
            st.session_state["_data_ready"] = False

        data_ready = bool(st.session_state.get("_data_ready", False))
        if st.button("Cargar datos", type="primary", use_container_width=True):
            st.session_state["_data_ready"] = True
            data_ready = True

        st.caption(
            "La app puede tardar al leer y optimizar el dataset. "
            "Por eso solo cargamos datos cuando tú lo indicas."
        )

        # Form to apply computational controls in one shot
        with st.form("apply_controls", clear_on_submit=False):
            min_n = st.slider(
                "Mínimo N para oportunidades",
                50,
                1500,
                int(defaults["min_n"]),
                step=50,
            )

            st.divider()
            st.header("Contexto")
            geo = st.text_input("Geografía", value=str(defaults["geo"]))
            channel = st.text_input("Canal", value=str(defaults["channel"]))

            applied = st.form_submit_button("Aplicar")

        if applied:
            st.session_state["_controls"] = {
                "theme_mode": theme_mode,
                "source": source,
                "data_path_str": data_path_str,
                "sheet_name": sheet_name,
                "min_n": int(min_n),
                "geo": geo,
                "channel": channel,
            }

        st.divider()
        st.header("Knowledge Cache")
        cache_path = Path(settings.knowledge_dir) / "insights_cache.json"
        st.caption(str(cache_path))

    c = st.session_state.get("_controls", defaults)
    # Use latest values (applied) but always respect current data selection
    return (
        Path(data_path_str),
        int(c["min_n"]),
        str(c["geo"]),
        str(c["channel"]),
        cache_path,
        theme_mode,
        sheet_name,
        data_ready,
    )



def page_executive(df: pd.DataFrame, theme: Theme) -> None:
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
        fig = chart_nps_trend(df, theme, freq="W")
        if fig is None:
            st.info("No hay suficientes datos para construir una tendencia.")
        else:
            st.plotly_chart(fig, use_container_width=True)

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

    prompt = (
        "INSTRUCCIONES\n"
        f"- {question}\n\n"
        "REGLAS DE RESPUESTA\n"
        "- Entrega (1) resumen ejecutivo (max 10 lineas) y (2) JSON estricto con el esquema.\n"
        "- No inventes datos. Si falta evidencia, indica 'insufficient_evidence' en risks y baja confianza.\n"
        "- El JSON debe ser el ultimo bloque y sin markdown.\n\n"
        "DEEP-DIVE PACK\n"
        f"{md}"
    )

    c1, c2 = st.columns([2, 1])
    with c1:
        if st.button("Copiar prompt", type="primary", use_container_width=True):
            _copy_to_clipboard(prompt, toast="Prompt copiado. Pégalo en tu ChatGPT.")
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

    answer = st.text_area("Respuesta del LLM", value=default_text or "", height=240, help="Si has pulsado Generar, la respuesta aparece aqui. Si usas modo manual, pega aqui el JSON del LLM.")
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
            st.warning(
                "Se detecto JSON, pero no coincide con el esquema esperado: " + "; ".join(errs)
            )
    elif answer.strip():
        st.warning("No pude detectar un JSON valido dentro del texto pegado.")

    return answer, parsed


def _llm_actions_row() -> tuple[bool, bool]:
    c1, c2 = st.columns([1, 1])
    with c1:
        add_to_dash = st.button("Anadir al dashboard", type="primary", use_container_width=True)
    with c2:
        save_cache = st.button("Guardar en knowledge cache", use_container_width=True)
    return add_to_dash, save_cache


def _llm_add_to_dashboard(parsed: Optional[dict[str, Any]]) -> None:
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

    kc = KnowledgeCache(cache_path)
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
    add_to_dash, save_cache = _llm_actions_row()

    if add_to_dash:
        _llm_add_to_dashboard(parsed)

    if save_cache:
        _llm_save_to_cache(
            cache_path=cache_path,
            context=context,
            pack=pack,
            answer=answer,
            settings=settings,
            selected=selected,
        )


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
    st.dataframe(df.head(50), use_container_width=True)


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
            "Selecciona la fuente de datos en la barra lateral y pulsa **Cargar datos**. "
            "Mientras tanto, aquí tienes una guía rápida de lo que verás."
        )
        with st.expander("¿Qué es este dashboard y qué datos espera?", expanded=True):
            st.markdown(
                "- **NPS**: score 0-10 por respuesta. Promotores (9-10), Pasivos (7-8), "
                "Detractores (0-6)."
                "- **NPS Lens** convierte ese dataset en: tendencias, drivers, temas de texto "
                "y oportunidades."
                "- Para mejores resultados, incluye columnas como: `Fecha`, `Canal`, `Geo`, "
                "`Palanca`, `Subpalanca`, `NPS` y `Comment`."
            )
        st.stop()

    with st.spinner("Cargando y optimizando datos..."):
        df = load_nps_data(data_path, sheet_name=sheet_name)


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
            page_executive(df, theme)
        with s2:
            page_comparisons(df, theme)
        with s3:
            page_cohorts(df, theme)

    with t_drivers:
        page_drivers(df, theme, min_n=min_n)

    with t_texto:
        page_text(df, theme)

    with t_journey:
        page_journey(df)

    with t_alertas:
        page_changes(df)

    with t_llm:
        page_llm(df, settings=settings, min_n=min_n, cache_path=cache_path)

    with t_datos:
        page_quality(df)




if __name__ == "__main__":
    main()
