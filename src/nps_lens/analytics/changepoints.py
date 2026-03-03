from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

import pandas as pd
import ruptures as rpt

from nps_lens.analytics.drivers import compute_nps_from_scores


@dataclass(frozen=True)
class ChangePoint:
    dimension: str
    value: str
    points: list[date]
    method: str
    note: str


def detect_nps_changepoints(
    df: pd.DataFrame,
    date_col: str = "Fecha",
    dim_col: str = "Palanca",
    value: Optional[str] = None,
    freq: str = "D",
    model: str = "l2",
    pen: float = 8.0,
) -> Optional[ChangePoint]:
    if date_col not in df.columns or dim_col not in df.columns or "NPS" not in df.columns:
        return None

    data = df.copy()
    data[date_col] = pd.to_datetime(data[date_col], errors="coerce")
    data = data.dropna(subset=[date_col])
    if value is not None:
        data = data.loc[data[dim_col] == value]
    if data.empty:
        return None

    ts = (
        data.set_index(date_col)
        .resample(freq)["NPS"]
        .apply(compute_nps_from_scores)
        .dropna()
        .astype(float)
    )
    if len(ts) < 10:
        return None

    algo = rpt.Pelt(model=model).fit(ts.values.reshape(-1, 1))
    bkps = algo.predict(pen=pen)
    # bkps include last index
    pts: list[date] = []
    for idx in bkps[:-1]:
        pts.append(ts.index[idx - 1].date())
    return ChangePoint(
        dimension=dim_col,
        value=str(value) if value is not None else "*",
        points=pts,
        method=f"Pelt({model})",
        note=f"freq={freq} pen={pen}",
    )
