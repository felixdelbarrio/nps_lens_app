from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DriverStat:
    dimension: str
    value: str
    n: int
    nps: float
    detractor_rate: float
    promoter_rate: float
    gap_vs_overall: float


def compute_nps_from_scores(scores: pd.Series) -> float:
    s = pd.to_numeric(scores, errors="coerce").dropna()
    if s.empty:
        return float("nan")
    promoters = (s >= 9).mean()
    detractors = (s <= 6).mean()
    return float((promoters - detractors) * 100.0)


def grouped_driver_stats(
    df: pd.DataFrame,
    dimension: str,
    *,
    survey_score_col: str = "NPS",
) -> pd.DataFrame:
    """Vectorized grouped NPS stats used by driver and opportunities ranking."""
    if dimension not in df.columns:
        return pd.DataFrame(
            columns=[dimension, "n", "valid_n", "nps", "detractor_rate", "promoter_rate"]
        )

    work = pd.DataFrame(
        {dimension: df[dimension], "_score": pd.to_numeric(df[survey_score_col], errors="coerce")}
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
    return grouped[[dimension, "n", "valid_n", "nps", "detractor_rate", "promoter_rate"]]


def driver_table(
    df: pd.DataFrame,
    dimension: str,
    survey_score_col: str = "NPS",
    *,
    overall_nps: float | None = None,
) -> list[DriverStat]:
    if dimension not in df.columns:
        return []
    overall = (
        float(overall_nps)
        if overall_nps is not None and np.isfinite(float(overall_nps))
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
                dimension=dimension,
                value=str(row[dimension]),
                n=n,
                nps=nps,
                detractor_rate=detr,
                promoter_rate=prom,
                gap_vs_overall=float(nps - overall) if not np.isnan(nps) else float("nan"),
            )
        )
    out.sort(key=lambda x: (np.nan_to_num(x.gap_vs_overall, nan=-1e9), x.n), reverse=True)
    return out
