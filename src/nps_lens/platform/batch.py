from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from nps_lens.application.service import AppService
from nps_lens.core.store import DatasetContext, DatasetStore
from nps_lens.ingest import read_incidents_csv, read_nps_thermal_excel, read_reviews_csv
from nps_lens.llm.pack import build_insight_pack, export_pack
from nps_lens.platform.artifacts import ensure_artifact_dirs, update_manifest, write_json_atomic


@dataclass
class BatchRunSpec:
    """A single platform run (context + inputs + outputs)."""

    excel_path: Path
    service_origin: str
    service_origin_n1: str
    service_origin_n2: str = ""

    # Optional multi-source inputs
    incidents_csv: Optional[Path] = None
    reviews_csv: Optional[Path] = None

    # Controls
    top_k_packs: int = 5
    min_n: int = 200
    dimensions: Tuple[str, ...] = ("Palanca", "Subpalanca", "Canal")

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "BatchRunSpec":
        excel = Path(str(d.get("excel_path") or d.get("excel") or "")).expanduser()
        if not excel:
            raise ValueError("Missing excel_path")
        return BatchRunSpec(
            excel_path=excel,
            service_origin=str(d.get("service_origin") or d.get("geo") or "").strip(),
            service_origin_n1=str(d.get("service_origin_n1") or d.get("channel") or "").strip(),
            service_origin_n2=str(d.get("service_origin_n2") or "").strip(),
            incidents_csv=Path(str(d["incidents_csv"])) if d.get("incidents_csv") else None,
            reviews_csv=Path(str(d["reviews_csv"])) if d.get("reviews_csv") else None,
            top_k_packs=int(d.get("top_k_packs", 5)),
            min_n=int(d.get("min_n", 200)),
            dimensions=tuple(d.get("dimensions") or ("Palanca", "Subpalanca", "Canal")),
        )


def load_batch_config(path: Path) -> List[BatchRunSpec]:
    """Load a platform batch config.

    Format (JSON):
      {"runs": [ { ...run spec... }, ... ]}
    """
    obj = json.loads(path.read_text(encoding="utf-8"))
    runs = obj.get("runs") if isinstance(obj, dict) else None
    if not isinstance(runs, list) or not runs:
        raise ValueError("Invalid config: expected { 'runs': [ ... ] }")
    return [BatchRunSpec.from_dict(x) for x in runs]


def _kpi_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
    scores = pd.to_numeric(df.get("NPS"), errors="coerce").dropna()
    if scores.empty:
        return {"n": int(len(df)), "nps": None, "promoter_rate": None, "detractor_rate": None}
    promoters = float((scores >= 9).mean())
    detractors = float((scores <= 6).mean())
    nps = float((promoters - detractors) * 100.0)
    return {
        "n": int(len(scores)),
        "nps": nps,
        "promoter_rate": promoters,
        "detractor_rate": detractors,
    }


def run_platform_batch(
    *,
    specs: List[BatchRunSpec],
    store: DatasetStore,
    app: AppService,
    out_root: Path,
) -> Dict[str, Any]:
    """Execute batch runs and export deterministic artifacts."""
    results: List[Dict[str, Any]] = []

    from nps_lens import PIPELINE_VERSION

    for spec in specs:
        ctx = DatasetContext(
            service_origin=spec.service_origin,
            service_origin_n1=spec.service_origin_n1,
            service_origin_n2=spec.service_origin_n2,
        )

        # --- Ingest sources ---
        nps_res = read_nps_thermal_excel(
            str(spec.excel_path),
            service_origin=spec.service_origin,
            service_origin_n1=spec.service_origin_n1,
            service_origin_n2=spec.service_origin_n2,
        )
        nps_df = nps_res.df

        incidents_df: Optional[pd.DataFrame] = None
        if spec.incidents_csv is not None and spec.incidents_csv.exists():
            inc_res = read_incidents_csv(str(spec.incidents_csv))
            incidents_df = inc_res.df

        reviews_df: Optional[pd.DataFrame] = None
        if spec.reviews_csv is not None and spec.reviews_csv.exists():
            rev_res = read_reviews_csv(str(spec.reviews_csv))
            reviews_df = rev_res.df

        # --- Persist dataset (canonical storage + meta with dataset_id) ---
        _stored = store.save_df(ctx, nps_df, source=str(spec.excel_path))
        meta = store.read_meta(ctx)
        dataset_id = str((meta or {}).get("dataset_id") or "unknown")

        # --- Platform artifacts layout ---
        context = {
            "service_origin": spec.service_origin,
            "service_origin_n1": spec.service_origin_n1,
            "service_origin_n2": spec.service_origin_n2,
        }
        paths = ensure_artifact_dirs(
            out_root=out_root,
            dataset_id=dataset_id,
            pipeline_version=PIPELINE_VERSION,
            context=context,
        )

        # --- KPIs ---
        kpis = _kpi_snapshot(nps_df)

        # --- Drivers (cached) ---
        drivers: Dict[str, Any] = {}
        for dim in spec.dimensions:
            try:
                df_stats = app.driver_stats(nps_df, dimension=str(dim), dataset_id=dataset_id)
                drivers[str(dim)] = df_stats.head(50).to_dict(orient="records")
            except Exception as e:
                drivers[str(dim)] = {"error": str(e)}

        # --- Routes (cached) ---
        routes_out: Any = None
        try:
            routes_out = app.routes(nps_df, incidents_df=incidents_df, dataset_id=dataset_id)
        except Exception as e:
            routes_out = {"error": str(e)}

        # --- Build top-k packs (rank opportunities) ---
        from nps_lens.analytics.opportunities import rank_opportunities
        from nps_lens.analytics.causal import best_effort_ate_logit

        opps = rank_opportunities(nps_df, dimensions=list(spec.dimensions), min_n=int(spec.min_n))
        exported_packs: List[Dict[str, str]] = []
        for top in opps[: int(spec.top_k_packs)]:
            slice_df = nps_df.loc[nps_df[top.dimension].astype(str) == top.value].copy()
            causal = best_effort_ate_logit(
                df=nps_df,
                treatment_col=top.dimension,
                treatment_value=top.value,
                control_cols=["Canal", "Palanca", "Subpalanca"],
            )
            pack = build_insight_pack(
                title=f"Oportunidad priorizada: {top.dimension}={top.value}",
                context={**context, "driver_dim": top.dimension, "driver_val": top.value},
                nps_slice=slice_df,
                driver={"dimension": top.dimension, "value": top.value},
                causal=causal,
            )
            out = export_pack(pack, paths.insights_dir)
            exported_packs.append({"insight_id": pack.insight_id, "md": str(out["md"]), "json": str(out["json"])})

        # --- Write KPI + manifest ---
        payload = {
            "context": context,
            "dataset_id": dataset_id,
            "pipeline_version": PIPELINE_VERSION,
            "kpis": kpis,
            "drivers": drivers,
            "routes": routes_out if isinstance(routes_out, dict) else {"routes": routes_out},
            "packs": exported_packs,
            "sources": {
                "nps_excel": str(spec.excel_path),
                "incidents_csv": str(spec.incidents_csv) if spec.incidents_csv else "",
                "reviews_csv": str(spec.reviews_csv) if spec.reviews_csv else "",
            },
            "issues": {
                "nps": [i.to_dict() for i in (nps_res.issues or [])],
            },
        }
        write_json_atomic(paths.kpis_path, payload)
        update_manifest(
            paths=paths,
            dataset_meta=meta or {},
            run_params={
                "top_k_packs": int(spec.top_k_packs),
                "min_n": int(spec.min_n),
                "dimensions": list(spec.dimensions),
            },
            perf=app.perf.snapshot(),
        )

        results.append(
            {
                "context": context,
                "artifact_dir": str(paths.context_dir),
                "kpis": str(paths.kpis_path),
                "packs": exported_packs,
            }
        )

    return {"runs": results}
