from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd
import typer
from dotenv import load_dotenv
from rich import print as rprint

from nps_lens.analytics import (
    best_effort_ate_logit,
    build_routes,
    detect_nps_changepoints,
    driver_table,
    rank_opportunities,
)
from nps_lens.config import Settings
from nps_lens.ingest import read_incidents_csv, read_nps_thermal_excel, read_reviews_csv
from nps_lens.llm import KnowledgeCache, build_insight_pack, export_pack
from nps_lens.logging import setup_logging

app = typer.Typer(add_completion=False)


@app.command()
def profile_nps(
    excel_path: Path = typer.Argument(..., exists=True),
    geo: str = "MX",
    channel: str = "Senda",
) -> None:
    """Ingesta + profiling rápido desde Excel de NPS térmico."""
    load_dotenv()
    setup_logging(Settings.from_env().log_level)
    res = read_nps_thermal_excel(str(excel_path), geo=geo, channel=channel)
    if res.issues:
        rprint(res.issues)
    rprint(res.df.head())
    rprint({"rows": len(res.df), "cols": list(res.df.columns)})


@app.command()
def build_example_pack(
    sample_csv: Path = Path("data/examples/nps_thermal_senda_mx_sample.csv"),
    out_dir: Path = Path("reports/examples"),
) -> None:
    """Genera un ejemplo de LLM Deep-Dive Pack (MD + JSON)."""
    df = pd.read_csv(sample_csv)
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    # choose an opportunity driver
    opps = rank_opportunities(df, dimensions=["Palanca", "Subpalanca", "Canal"], min_n=200)
    if not opps:
        raise typer.Exit(code=2)

    top = opps[0]
    slice_df = df.loc[df[top.dimension].astype(str) == top.value].copy()
    causal = best_effort_ate_logit(
        df=df,
        treatment_col=top.dimension,
        treatment_value=top.value,
        control_cols=["Canal", "Palanca", "Subpalanca"],
    )
    context = {"geo": "MX", "channel": "Senda", "driver_dim": top.dimension, "driver_val": top.value}
    pack = build_insight_pack(
        title=f"Oportunidad priorizada: {top.dimension}={top.value}",
        context=context,
        nps_slice=slice_df,
        driver={"dimension": top.dimension, "value": top.value},
        causal=causal,
    )
    exported = export_pack(pack, out_dir)
    rprint(exported)


if __name__ == "__main__":
    app()
