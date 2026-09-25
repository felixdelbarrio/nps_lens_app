from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from nps_lens.core.metrics import summarize
from nps_lens.core.nps_math import valid_nps_scores


@dataclass(frozen=True)
class DriverStat:
    dimension: str
    value: str
    n: int
    nps: float
    detractor_rate: float
    promoter_rate: float
    gap_vs_base: float
    detractors: int
    valid_n: int
    sample_share: float


def compute_nps_from_scores(scores: pd.Series) -> float:
    summary = summarize(pd.DataFrame({"NPS": scores}))
    return summary.nps_classic_pp if summary.n else float("nan")


def grouped_driver_stats(
    df: pd.DataFrame,
    dimension: str,
    *,
    survey_score_col: str = "NPS",
) -> pd.DataFrame:
    """Vectorized grouped NPS stats used by driver and nps_gaps ranking."""
    if dimension not in df.columns:
        return pd.DataFrame(
            columns=[
                dimension,
                "n",
                "valid_n",
                "det_count",
                "nps",
                "detractor_rate",
                "promoter_rate",
            ]
        )

    work = pd.DataFrame(
        {dimension: df[dimension], "_score": valid_nps_scores(df[survey_score_col])}
    )
    work["_valid"] = work["_score"].notna()
    work["_det"] = work["_score"] <= 6.0
    work["_pro"] = work["_score"] >= 9.0

    grouped = (
        work.groupby(dimension, dropna=False, observed=True)
        .agg(
            n=("_score", "size"),
            valid_n=("_valid", "sum"),
            det_count=("_det", "sum"),
            pro_count=("_pro", "sum"),
        )
        .reset_index()
    )
    valid = grouped["valid_n"].replace({0: np.nan}).astype(float)
    grouped["detractor_rate"] = grouped["det_count"].astype(float) / valid
    grouped["promoter_rate"] = grouped["pro_count"].astype(float) / valid
    grouped["nps"] = (grouped["promoter_rate"] - grouped["detractor_rate"]) * 100.0
    grouped.loc[grouped["valid_n"] <= 0, ["nps", "detractor_rate", "promoter_rate"]] = np.nan
    return grouped[
        [dimension, "n", "valid_n", "det_count", "nps", "detractor_rate", "promoter_rate"]
    ]


def driver_table(
    df: pd.DataFrame,
    dimension: str,
    survey_score_col: str = "NPS",
    *,
    base_nps: float | None = None,
) -> list[DriverStat]:
    if dimension not in df.columns:
        return []
    reference = (
        float(base_nps)
        if base_nps is not None and np.isfinite(float(base_nps))
        else compute_nps_from_scores(df[survey_score_col])
    )
    grouped = grouped_driver_stats(df, dimension, survey_score_col=survey_score_col)
    out: list[DriverStat] = []
    for _, row in grouped.iterrows():
        n = int(row["n"])
        nps = float(row["nps"]) if pd.notna(row["nps"]) else float("nan")
        detr = float(row["detractor_rate"]) if pd.notna(row["detractor_rate"]) else float("nan")
        prom = float(row["promoter_rate"]) if pd.notna(row["promoter_rate"]) else float("nan")
        out.append(
            DriverStat(
                detractors=int(row["det_count"]),
                valid_n=int(row["valid_n"]),
                sample_share=n / len(df) if len(df) else 0.0,
                dimension=dimension,
                value=str(row[dimension]),
                n=n,
                nps=nps,
                detractor_rate=detr,
                promoter_rate=prom,
                gap_vs_base=float(nps - reference) if not np.isnan(nps) else float("nan"),
            )
        )
    out.sort(key=lambda x: (np.nan_to_num(x.gap_vs_base, nan=-1e9), x.n), reverse=True)
    return out
