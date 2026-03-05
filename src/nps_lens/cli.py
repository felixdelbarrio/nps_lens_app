from __future__ import annotations

from pathlib import Path

import pandas as pd
import typer
from dotenv import load_dotenv
from rich import print as rprint

from nps_lens.analytics.causal import best_effort_ate_logit
from nps_lens.analytics.opportunities import rank_opportunities
from nps_lens.application.service import AppService
from nps_lens.config import Settings
from nps_lens.core.disk_cache import DiskCache
from nps_lens.core.perf import PerfTracker
from nps_lens.core.store import DatasetStore
from nps_lens.ingest import read_nps_thermal_excel
from nps_lens.llm.pack import build_insight_pack, export_pack
from nps_lens.logging import setup_logging
from nps_lens.platform.batch import load_batch_config, run_platform_batch

app = typer.Typer(add_completion=False)

EXCEL_PATH_ARG = typer.Argument(..., exists=True)
CONFIG_PATH_ARG = typer.Argument(..., exists=True, help="Path to batch config JSON")
OUT_ROOT_OPT = typer.Option(Path("artifacts"), help="Root directory for exported artifacts")



@app.command()
def profile_nps(
    excel_path: Path = EXCEL_PATH_ARG,
    service_origin: str = "BBVA México",
    service_origin_n1: str = "Senda",
) -> None:
    """Ingesta + profiling rápido desde Excel de NPS térmico."""
    load_dotenv()
    setup_logging(Settings.from_env().log_level)
    res = read_nps_thermal_excel(str(excel_path), service_origin=service_origin, service_origin_n1=service_origin_n1)
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
    context = {
        "service_origin": "BBVA México",
        "service_origin_n1": "Senda",
        "driver_dim": top.dimension,
        "driver_val": top.value,
    }
    pack = build_insight_pack(
        title=f"Oportunidad priorizada: {top.dimension}={top.value}",
        context=context,
        nps_slice=slice_df,
        driver={"dimension": top.dimension, "value": top.value},
        causal=causal,
    )
    exported = export_pack(pack, out_dir)
    rprint(exported)


@app.command()
def platform_batch(
    config_path: Path = CONFIG_PATH_ARG,
    out_root: Path = OUT_ROOT_OPT,
) -> None:
    """Run the project as a platform (batch mode) and export versioned artifacts."""
    load_dotenv()
    settings = Settings.from_env()
    setup_logging(settings.log_level)

    store = DatasetStore(settings.data_dir)
    app_svc = AppService(disk_cache=DiskCache(settings.data_dir / "cache" / "compute"), perf=PerfTracker())

    specs = load_batch_config(config_path)
    summary = run_platform_batch(specs=specs, store=store, app=app_svc, out_root=out_root)
    rprint(summary)


if __name__ == "__main__":
    app()
