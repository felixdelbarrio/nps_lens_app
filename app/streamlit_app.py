from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st

from nps_lens.analytics import (
    best_effort_ate_logit,
    build_routes,
    detect_nps_changepoints,
    driver_table,
    extract_topics,
    rank_opportunities,
)
from nps_lens.config import Settings
from nps_lens.design import streamlit_css
from nps_lens.ingest.base import IngestResult
from nps_lens.ingest.nps_thermal import read_nps_thermal_excel
from nps_lens.llm import KnowledgeCache, build_insight_pack, export_pack, stable_signature
from nps_lens.ui import (
    PERIODICITIES,
    build_executive_story,
    chart_cohort_heatmap,
    chart_driver_bar,
    chart_driver_delta,
    chart_nps_trend,
    chart_topic_bars,
    compare_periods,
    driver_delta_table,
    executive_summary,
    explain_opportunities,
    explain_topics,
    pandas_freq_for_periodicity,
    period_windows,
    slice_by_window,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NPS_CSV = REPO_ROOT / "data" / "examples" / "nps_thermal_senda_mx_sample.csv"
UPLOAD_DIR = REPO_ROOT / "data" / "uploads"


def _card(title: str, body_html: str) -> None:
    st.markdown(
        f"""
        <div class="nps-card">
          <div class="nps-kpi-label">{title}</div>
          <div style="margin-top:6px;">{body_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _badge(text: str, accent: bool = False) -> str:
    cls = "nps-badge nps-badge--accent" if accent else "nps-badge"
    return f"<span class=\"{cls}\">{text}</span>"


def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Defensive: ensure taxonomy columns exist
    for col in [
        "Palanca",
        "Subpalanca",
        "Canal",
        "UsuarioDecisión",
        "Segmento",
        "Comment",
        "NPS Group",
        "ID",
    ]:
        if col not in out.columns:
            out[col] = None
    out["Fecha"] = pd.to_datetime(out.get("Fecha"), errors="coerce")
    out["NPS"] = pd.to_numeric(out.get("NPS"), errors="coerce")
    if "ID" in out.columns:
        out["ID"] = out["ID"].astype(str)
    return out


@st.cache_data(show_spinner=False)
def load_nps_from_path(
    path: str,
    geo: str,
    channel: str,
    sheet_name: Optional[str] = None,
) -> Tuple[pd.DataFrame, list[dict]]:
    """Load NPS from a CSV/XLSX path with minimal friction."""

    p = Path(path)
    issues: list[dict] = []
    if p.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        sheet = sheet_name or "Hoja1"
        res: IngestResult = read_nps_thermal_excel(
            str(p),
            geo=geo,
            channel=channel,
            sheet_name=sheet,
        )
        issues = [asdict(i) for i in res.issues]
        return _ensure_cols(res.df), issues

    df = pd.read_csv(p)
    df = _ensure_cols(df)
    df["geo"] = geo
    df["channel"] = channel
    return df, issues


def _save_uploaded_file(uploaded: st.runtime.uploaded_file_manager.UploadedFile) -> Path:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    safe = uploaded.name.replace("/", "_")
    out = UPLOAD_DIR / safe
    out.write_bytes(uploaded.getbuffer())
    return out


def _excel_sheets(path: Path) -> list[str]:
    try:
        xf = pd.ExcelFile(path)
        return [str(s) for s in xf.sheet_names]
    except Exception:
        return ["Hoja1"]


def _render_issues(issues: list[dict]) -> None:
    if not issues:
        return
    errs = [i for i in issues if i.get("level") == "ERROR"]
    warns = [i for i in issues if i.get("level") == "WARN"]
    with st.expander(
        f"Calidad de datos: {len(errs)} errores, {len(warns)} avisos",
        expanded=bool(errs),
    ):
        if errs:
            st.error("Errores (bloquean análisis)")
            st.dataframe(pd.DataFrame(errs), use_container_width=True)
        if warns:
            st.warning("Avisos")
            st.dataframe(pd.DataFrame(warns), use_container_width=True)


def render_sidebar(
    settings: Settings,
) -> tuple[str, Optional[str], int, str, str, str, Path]:
    with st.sidebar:
        st.header("Datos")
        mode = st.radio(
            "Fuente",
            ["Subir Excel", "Ruta local", "Ejemplo"],
            index=0,
            help="Para evitar fricción, lo más fácil es subir el Excel directamente aquí.",
        )

        sheet_name: Optional[str] = None
        data_path_str: str
        if mode == "Subir Excel":
            up = st.file_uploader(
                "Sube el Excel de NPS térmico",
                type=["xlsx", "xlsm", "xls"],
            )
            if up is None:
                st.caption(
                    "Sugerencia: sube tu Excel (por ejemplo: 'NPS Térmico Senda - 01Enero-02Febrero.xlsx')."
                )
                data_path_str = str(DEFAULT_NPS_CSV)
            else:
                saved = _save_uploaded_file(up)
                sheets = _excel_sheets(saved)
                if sheets:
                    sheet_name = st.selectbox("Hoja", sheets, index=0)
                data_path_str = str(saved)

        elif mode == "Ruta local":
            data_path_str = st.text_input(
                "Ruta a CSV/XLSX",
                value=str(DEFAULT_NPS_CSV),
                help="Puedes pegar una ruta local. Se aceptan .xlsx/.xlsm/.xls y .csv.",
            )
            p = Path(data_path_str)
            if p.suffix.lower() in {".xlsx", ".xlsm", ".xls"} and p.exists():
                sheets = _excel_sheets(p)
                if sheets:
                    sheet_name = st.selectbox("Hoja", sheets, index=0)
        else:
            data_path_str = str(DEFAULT_NPS_CSV)

        min_n = st.slider("Mínimo N para oportunidades", 50, 1500, 200, step=50)

        st.divider()
        st.header("Contexto")
        geo = st.text_input("Geografía", value=settings.default_geo)
        channel = st.text_input("Canal", value=settings.default_channel)

        st.divider()
        st.header("Periodo (negocio)")
        periodicity = st.selectbox(
            "Cómo leer el tiempo",
            PERIODICITIES,
            index=0,
            help="Afecta tendencia, informe ejecutivo y comparativas por defecto.",
        )

        st.divider()
        st.header("Knowledge Cache")
        cache_path = Path(settings.knowledge_dir) / "insights_cache.json"
        st.caption(str(cache_path))

    return data_path_str, sheet_name, int(min_n), geo, channel, periodicity, cache_path


def page_executive(df: pd.DataFrame, repo_root: Path, periodicity: str) -> None:
    st.subheader("Resumen ejecutivo")

    s = executive_summary(df)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _card("Muestras", f"<div class='nps-kpi'>{s.n:,}</div>")
    with c2:
        val = "—" if s.n == 0 else f"{s.nps_avg:.2f}"
        _card("NPS medio (0–10)", f"<div class='nps-kpi'>{val}</div>")
    with c3:
        _card("Detractores (<=6)", f"<div class='nps-kpi'>{s.detractor_rate*100:.1f}%</div>")
    with c4:
        _card("Promotores (>=9)", f"<div class='nps-kpi'>{s.promoter_rate*100:.1f}%</div>")

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    col_a, col_b = st.columns([2, 1])
    with col_a:
        st.markdown(f"{_badge('Tendencia', True)}")
        fig = chart_nps_trend(df, repo_root, freq=pandas_freq_for_periodicity(periodicity))
        if fig is None:
            st.info("No hay suficientes datos para construir una tendencia.")
        else:
            st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.markdown(f"{_badge('Lectura rápida', True)}", unsafe_allow_html=True)
        det = s.top_detractor_driver
        pro = s.top_promoter_driver
        st.markdown(
            f"""
            <div class="nps-card">
              <div class="nps-muted">
                Esta vista está pensada para negocio: qué está pasando, dónde mirar primero y por qué.
              </div>
              <div style="height:10px"></div>
              <ul>
                <li><b>Zona de fricción (peor media por palanca):</b> {det}</li>
                <li><b>Zona fuerte (mejor media por palanca):</b> {pro}</li>
              </ul>
              <div class="nps-muted">Siguiente paso recomendado: abrir “Drivers & Oportunidades” y priorizar por impacto.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    st.markdown(f"{_badge('Informe ejecutivo (copiar/pegar)', True)}", unsafe_allow_html=True)

    # Default time windows aligned to business periodicity
    pw = period_windows(df, periodicity)
    comp = None
    if pw is not None:
        cur_df = slice_by_window(df, pw.current)
        base_df = slice_by_window(df, pw.baseline)
        if len(cur_df) >= 50 and len(base_df) >= 50:
            comp = compare_periods(cur_df, base_df)

    # Reuse opportunity/topic narratives to populate the report
    opps = rank_opportunities(df, dimensions=["Palanca", "Subpalanca", "Canal"], min_n=200)
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


def page_comparisons(df: pd.DataFrame, repo_root: Path, periodicity: str) -> None:
    st.subheader("Comparativas (periodo actual vs periodo base)")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "Esta vista responde a una pregunta típica de negocio: <b>¿qué cambió?</b> "
        "Elige dos ventanas de tiempo y mira <b>qué palancas se deterioran o mejoran</b>."
        "</div>",
        unsafe_allow_html=True,
    )

    mode = st.radio(
        "Modo",
        ["Automático (periodo de negocio)", "Manual (rango de fechas)"],
        horizontal=True,
    )

    if mode == "Automático (periodo de negocio)":
        pw = period_windows(df, periodicity)
        if pw is None:
            st.info("No hay columna de Fecha válida para construir periodos.")
            return
        cur_df = slice_by_window(df, pw.current)
        base_df = slice_by_window(df, pw.baseline)
        label_cur = pw.label_current
        label_base = pw.label_baseline
    else:
        pw_fallback = period_windows(df, "Manual")
        if pw_fallback is None:
            st.info("No hay columna de Fecha válida para hacer comparativas.")
            return
        w_cur = pw_fallback.current
        w_base = pw_fallback.baseline
        c1, c2 = st.columns(2)
        with c1:
            cur_dates = st.date_input("Periodo actual", value=(w_cur.start, w_cur.end))
        with c2:
            base_dates = st.date_input("Periodo base", value=(w_base.start, w_base.end))

        if not isinstance(cur_dates, tuple) or not isinstance(base_dates, tuple):
            st.warning("Selecciona un rango (inicio y fin) para ambos periodos.")
            return

        cur_w = type(w_cur)(cur_dates[0], cur_dates[1])
        base_w = type(w_base)(base_dates[0], base_dates[1])
        cur_df = slice_by_window(df, cur_w)
        base_df = slice_by_window(df, base_w)
        label_cur = f"{cur_w.start.isoformat()} → {cur_w.end.isoformat()}"
        label_base = f"{base_w.start.isoformat()} → {base_w.end.isoformat()}"

    comp = compare_periods(cur_df, base_df)
    st.markdown(
        "<div class='nps-card'>"
        f"<div><b>Periodo actual</b>: {label_cur} (n={comp.n_current:,})</div>"
        f"<div><b>Periodo base</b>: {label_base} (n={comp.n_baseline:,})</div>"
        f"<div style='margin-top:6px'><b>Δ NPS</b>: {comp.delta_nps:+.2f} · "
        f"<b>Δ detractores</b>: {comp.delta_detr_pp:+.1f} pp</div>"
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown(f"{_badge('Qué palancas cambian', True)}", unsafe_allow_html=True)
    dim = st.selectbox("Dimensión", ["Palanca", "Subpalanca", "Canal", "UsuarioDecisión"], index=0)
    delta = driver_delta_table(cur_df, base_df, dimension=dim, min_n=50)
    if delta.empty:
        st.info("No hay suficiente N para comparar en esa dimensión. Prueba ampliar la ventana o bajar min_n.")
        return
    fig = chart_driver_delta(delta, repo_root)
    if fig is not None:
        st.plotly_chart(fig, use_container_width=True)
    with st.expander("Ver tabla de deltas"):
        st.dataframe(delta.head(30), use_container_width=True)


def page_cohorts(df: pd.DataFrame, repo_root: Path) -> None:
    st.subheader("Cohortes: dónde duele según segmento / usuario")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "La idea: no todos los usuarios viven lo mismo. "
        "Esta vista te ayuda a encontrar <b>bolsas de fricción</b> (cohortes) para priorizar acciones."
        "</div>",
        unsafe_allow_html=True,
    )

    row_dim = st.selectbox("Filas", ["Palanca", "Subpalanca", "Canal"], index=0)
    col_dim = st.selectbox("Columnas", ["UsuarioDecisión", "Segmento"], index=0)
    min_n = st.slider("Mínimo N por celda", 10, 200, 30, step=10)

    fig = chart_cohort_heatmap(df, repo_root, row_dim=row_dim, col_dim=col_dim, min_n=min_n)
    if fig is None:
        st.info("No hay suficiente información para construir la matriz (revisa columnas y N mínimo).")
        return
    st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        "<div class='nps-card'>"
        "<b>Cómo usar esto:</b> busca columnas con valores bajos de NPS de forma consistente. "
        "Eso suele indicar una fricción localizada (segmento/rol) y ayuda a afinar el plan de mejora."
        "</div>",
        unsafe_allow_html=True,
    )


def page_drivers(df: pd.DataFrame, repo_root: Path, min_n: int) -> None:
    st.subheader("Drivers & oportunidades (lenguaje de negocio)")

    left, right = st.columns([1, 1])
    with left:
        dim = st.selectbox("Cortar por", ["Palanca", "Subpalanca", "Canal", "UsuarioDecisión"])
        stats = driver_table(df, dimension=dim)
        stats_df = pd.DataFrame([s.__dict__ for s in stats])

        st.markdown(f"{_badge('Mayores gaps vs global', True)}", unsafe_allow_html=True)
        if stats_df.empty:
            st.info("No hay datos suficientes para calcular drivers.")
        else:
            # Biggest negative gaps first
            stats_df = stats_df.sort_values("gap_vs_overall", ascending=True)
            fig = chart_driver_bar(stats_df, repo_root)
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)

        with st.expander("Ver tabla detallada"):
            st.dataframe(stats_df.head(30), use_container_width=True)

    with right:
        st.markdown(f"{_badge('Oportunidades priorizadas', True)}", unsafe_allow_html=True)
        opps = rank_opportunities(df, dimensions=["Palanca", "Subpalanca", "Canal"], min_n=min_n)
        opp_df = pd.DataFrame([o.__dict__ for o in opps])

        if opp_df.empty:
            st.warning("No se detectaron oportunidades con el umbral actual.")
        else:
            bullets = explain_opportunities(opp_df, max_items=5)
            st.markdown(
                "<div class='nps-card'><ul>" + "".join([f"<li>{b}</li>" for b in bullets]) + "</ul></div>",
                unsafe_allow_html=True,
            )

        with st.expander("Ver ranking completo"):
            st.dataframe(opp_df.head(25), use_container_width=True)


def page_text(df: pd.DataFrame, repo_root: Path) -> None:
    st.subheader("Texto & temas: qué se repite y cómo suena")

    comment_col = "Comment" if "Comment" in df.columns else "Comentario"
    texts = df[comment_col].astype(str)

    topics = extract_topics(texts, n_clusters=10)
    topics_df = pd.DataFrame([t.__dict__ for t in topics])

    c1, c2 = st.columns([1, 1])
    with c1:
        st.markdown(f"{_badge('Temas con más volumen', True)}", unsafe_allow_html=True)
        fig = chart_topic_bars(topics_df, repo_root)
        if fig is None:
            st.info("No hay texto suficiente para extraer temas.")
        else:
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown(f"{_badge('Explicación en lenguaje natural', True)}", unsafe_allow_html=True)
        bullets = explain_topics(topics_df, max_items=6)
        st.markdown(
            "<div class='nps-card'><ul>" + "".join([f"<li>{b}</li>" for b in bullets]) + "</ul></div>",
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
        "Es una aproximación best-effort: sirve para priorizar investigaciones, no como prueba definitiva." 
        "</div>",
        unsafe_allow_html=True,
    )

    routes = build_routes(df)
    routes_df = pd.DataFrame([r.__dict__ for r in routes])
    if routes_df.empty:
        st.info("No se pudieron construir rutas con los datos actuales.")
        return

    st.markdown(f"{_badge('Top rutas por score', True)}", unsafe_allow_html=True)
    st.dataframe(routes_df.head(25), use_container_width=True)

    st.markdown(
        "<div class='nps-card'><b>Cómo leerlo:</b> una ruta con score alto suele ser una combinación "
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


def page_llm(df: pd.DataFrame, settings: Settings, min_n: int, cache_path: Path) -> None:
    st.subheader("WoW: Deep-Dive Pack para ChatGPT (copy/paste + memoria)")

    st.markdown(
        "<div class='nps-card nps-muted'>"
        "Selecciona una oportunidad, genera un pack con contexto + evidencia y llévalo a tu LLM. "
        "Después pega la respuesta aquí para que la app recuerde decisiones y no repita insights." 
        "</div>",
        unsafe_allow_html=True,
    )

    opps = rank_opportunities(df, dimensions=["Palanca", "Subpalanca", "Canal"], min_n=min_n)
    if not opps:
        st.warning("No hay oportunidades con el umbral actual.")
        return

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

    causal = best_effort_ate_logit(
        df=df,
        treatment_col=selected.dimension,
        treatment_value=selected.value,
        control_cols=["Canal", "Palanca", "Subpalanca"],
    )

    context = {
        "geo": settings.default_geo,
        "channel": settings.default_channel,
        "driver_dim": selected.dimension,
        "driver_val": selected.value,
    }

    pack = build_insight_pack(
        title=f"Oportunidad priorizada: {selected.dimension}={selected.value}",
        context=context,
        nps_slice=slice_df,
        driver={"dimension": selected.dimension, "value": selected.value},
        causal=causal,
    )

    out = export_pack(pack, Path("reports/examples"))
    md = out["md"].read_text(encoding="utf-8")

    st.markdown(f"{_badge('Prompt listo para copiar', True)}", unsafe_allow_html=True)
    st.text_area("Deep-Dive Pack (Markdown)", md, height=420)

    st.download_button(
        "Descargar pack .md",
        data=md.encode("utf-8"),
        file_name=out["md"].name,
        mime="text/markdown",
    )

    st.divider()
    st.subheader("Pegar respuesta del LLM y guardarla en Knowledge Cache")
    answer = st.text_area("Respuesta del LLM", "", height=200)

    if st.button("Guardar en cache"):
        kc = KnowledgeCache(cache_path)
        sig = stable_signature(context=context, title=pack.title)
        record = {
            "signature": sig,
            "insight_id": pack.insight_id,
            "title": pack.title,
            "context": context,
            "llm_answer": answer,
            "created_at_utc": pack.created_at.isoformat() + "Z",
            "tags": [settings.default_geo, settings.default_channel, selected.dimension, selected.value],
        }
        kc.upsert(sig, record)
        st.success("Guardado. Se usará para deduplicación y contexto futuro.")


def page_quality(df: pd.DataFrame) -> None:
    st.subheader("Datos & calidad")
    st.markdown(
        "<div class='nps-card nps-muted'>"
        "Esta sección es técnica, pero útil cuando los números no cuadran: faltantes, duplicados y columnas clave." 
        "</div>",
        unsafe_allow_html=True,
    )

    st.caption(f"Filas: {len(df):,} · Columnas: {len(df.columns)}")
    st.dataframe(df.head(50), use_container_width=True)


def main() -> None:
    settings = Settings.from_env()

    st.set_page_config(page_title="NPS Lens — Senda MX", layout="wide")
    st.markdown(streamlit_css(REPO_ROOT), unsafe_allow_html=True)

    st.title("NPS Lens — NPS térmico (Senda · México)")
    st.caption("Una lectura de negocio sobre los hechos + la evidencia analítica detrás.")

    data_path, sheet_name, min_n, geo, channel, periodicity, cache_path = render_sidebar(settings)
    df, issues = load_nps_from_path(data_path, geo=geo, channel=channel, sheet_name=sheet_name)
    _render_issues(issues)

    st.divider()

    page = st.radio(
        "Navegación",
        [
            "Resumen ejecutivo",
            "Comparativas",
            "Cohortes",
            "Drivers & oportunidades",
            "Texto & temas",
            "Journey & causas",
            "Alertas (cambios)",
            "LLM Deep-Dive Pack",
            "Datos & calidad",
        ],
        horizontal=True,
    )

    if page == "Resumen ejecutivo":
        page_executive(df, REPO_ROOT, periodicity)
    elif page == "Comparativas":
        page_comparisons(df, REPO_ROOT, periodicity)
    elif page == "Cohortes":
        page_cohorts(df, REPO_ROOT)
    elif page == "Drivers & oportunidades":
        page_drivers(df, REPO_ROOT, min_n=min_n)
    elif page == "Texto & temas":
        page_text(df, REPO_ROOT)
    elif page == "Journey & causas":
        page_journey(df)
    elif page == "Alertas (cambios)":
        page_changes(df)
    elif page == "LLM Deep-Dive Pack":
        page_llm(df, settings=settings, min_n=min_n, cache_path=cache_path)
    else:
        page_quality(df)


if __name__ == "__main__":
    main()
