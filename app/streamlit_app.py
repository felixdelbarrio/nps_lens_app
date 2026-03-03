from __future__ import annotations

from pathlib import Path
from typing import Optional

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
from nps_lens.llm import KnowledgeCache, build_insight_pack, export_pack, stable_signature
from nps_lens.design import streamlit_css


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_nps_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df["NPS"] = pd.to_numeric(df["NPS"], errors="coerce")
    return df


def main() -> None:
    settings = Settings.from_env()
    st.set_page_config(page_title="NPS Lens — Senda MX", layout="wide")

    st.markdown(streamlit_css(REPO_ROOT), unsafe_allow_html=True)

    st.title("NPS Lens — NPS térmico (Senda · México)")

    with st.sidebar:
        st.header("Dataset")
        data_path = st.text_input(
            "Ruta CSV NPS térmico",
            value=str(REPO_ROOT / "data" / "examples" / "nps_thermal_senda_mx_sample.csv"),
        )
        min_n = st.slider("Mínimo N para oportunidades", 50, 1500, 200, step=50)
        st.divider()
        st.header("Knowledge Cache")
        cache_path = Path(settings.knowledge_dir) / "insights_cache.json"
        st.caption(str(cache_path))

    df = load_nps_data(Path(data_path))
    st.caption(f"Filas: {len(df):,} · Columnas: {len(df.columns)}")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Drivers", "Change-points", "Texto", "Journey routes", "LLM Deep-Dive Pack"]
    )

    with tab1:
        st.subheader("Drivers (contribución / gaps vs global)")
        dim = st.selectbox("Dimensión", ["Palanca", "Subpalanca", "Canal", "UsuarioDecisión"])
        stats = driver_table(df, dimension=dim)
        st.dataframe(pd.DataFrame([s.__dict__ for s in stats]).head(30), use_container_width=True)

        st.subheader("Oportunidades (impacto estimado × confianza)")
        opps = rank_opportunities(df, dimensions=["Palanca", "Subpalanca", "Canal"], min_n=min_n)
        opp_df = pd.DataFrame([o.__dict__ for o in opps]).head(20)
        st.dataframe(opp_df, use_container_width=True)

    with tab2:
        st.subheader("Detección de cambios (NPS resampleado)")
        lever = st.selectbox("Palanca (para change-point)", sorted(df["Palanca"].astype(str).unique())[:50])
        cp = detect_nps_changepoints(df, dim_col="Palanca", value=lever, freq="D", pen=8.0)
        if cp is None:
            st.info("No hay suficientes datos para detectar change-points en esa selección.")
        else:
            st.json({"dimension": cp.dimension, "value": cp.value, "points": [str(p) for p in cp.points], "note": cp.note})

    with tab3:
        st.subheader("Minería de texto (topic clusters)")
        topics = extract_topics(df["Comment"], n_clusters=10)
        st.dataframe(pd.DataFrame([t.__dict__ for t in topics]), use_container_width=True)

    with tab4:
        st.subheader("Journey routes (palanca → subpalanca → topic)")
        routes = build_routes(df)
        st.dataframe(pd.DataFrame([r.__dict__ for r in routes]), use_container_width=True)

    with tab5:
        st.subheader("Generar prompt pack para LLM (copy/paste + export)")
        opps = rank_opportunities(df, dimensions=["Palanca", "Subpalanca", "Canal"], min_n=min_n)
        if not opps:
            st.warning("No hay oportunidades con el umbral actual.")
            return
        labels = [f"{o.dimension}={o.value} (uplift~{o.potential_uplift:.1f}, conf~{o.confidence:.2f}, n={o.n})" for o in opps[:30]]
        choice = st.selectbox("Oportunidad", labels)
        idx = labels.index(choice)
        o = opps[idx]

        slice_df = df.loc[df[o.dimension].astype(str) == o.value].copy()
        causal = best_effort_ate_logit(
            df=df,
            treatment_col=o.dimension,
            treatment_value=o.value,
            control_cols=["Canal", "Palanca", "Subpalanca"],
        )

        context = {
            "geo": settings.default_geo,
            "channel": settings.default_channel,
            "driver_dim": o.dimension,
            "driver_val": o.value,
        }
        pack = build_insight_pack(
            title=f"Oportunidad priorizada: {o.dimension}={o.value}",
            context=context,
            nps_slice=slice_df,
            driver={"dimension": o.dimension, "value": o.value},
            causal=causal,
        )
        md = export_pack(pack, Path("reports/examples"))["md"].read_text(encoding="utf-8")
        st.text_area("Prompt/Pack (Markdown)", md, height=420)

        st.divider()
        st.subheader("Pegar respuesta del LLM y guardar en Knowledge Cache")
        answer = st.text_area("Respuesta del LLM", "", height=180)
        if st.button("Guardar en cache"):
            kc = KnowledgeCache(Path(settings.knowledge_dir) / "insights_cache.json")
            sig = stable_signature(context=context, title=pack.title)
            record = {
                "signature": sig,
                "insight_id": pack.insight_id,
                "title": pack.title,
                "context": context,
                "llm_answer": answer,
                "created_at_utc": pack.created_at.isoformat() + "Z",
                "tags": [settings.default_geo, settings.default_channel, o.dimension, o.value],
            }
            kc.upsert(sig, record)
            st.success("Guardado. Se usará para deduplicación y contexto futuro.")


if __name__ == "__main__":
    main()
