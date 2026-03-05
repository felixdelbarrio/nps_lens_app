from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ColumnProfile:
    name: str
    dtype: str
    missing: int
    missing_pct: float
    nunique: int
    sample_values: list[str]


def profile_dataframe(df: pd.DataFrame, max_samples: int = 5) -> list[ColumnProfile]:
    profiles: list[ColumnProfile] = []
    n = len(df) if len(df) else 1
    for col in df.columns:
        s = df[col]
        missing = int(s.isna().sum())
        nunique = int(s.nunique(dropna=True))
        sample = s.dropna().astype(str).head(max_samples).tolist()
        profiles.append(
            ColumnProfile(
                name=str(col),
                dtype=str(s.dtype),
                missing=missing,
                missing_pct=float(missing / n),
                nunique=nunique,
                sample_values=sample,
            )
        )
    return profiles


def detect_outliers_zscore(
    df: pd.DataFrame, column: str, z: float = 4.0
) -> pd.DataFrame:
    if column not in df.columns:
        return df.iloc[0:0].copy()
    x = pd.to_numeric(df[column], errors="coerce")
    mu = float(np.nanmean(x))
    sigma = float(np.nanstd(x))
    if sigma == 0.0 or np.isnan(sigma):
        return df.iloc[0:0].copy()
    zscores = (x - mu) / sigma
    out = df.loc[np.abs(zscores) >= z].copy()
    return cast(pd.DataFrame, out)
