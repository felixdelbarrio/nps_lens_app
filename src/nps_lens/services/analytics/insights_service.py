from __future__ import annotations

from typing import Any, Optional

import pandas as pd


def _rate(
    values: pd.Series[Any], *, lower: Optional[float] = None, upper: Optional[float] = None
) -> float:
    scores = pd.to_numeric(values, errors="coerce").dropna()
    if scores.empty:
        return 0.0
    mask = pd.Series(True, index=scores.index)
    if lower is not None:
        mask &= scores >= lower
    if upper is not None:
        mask &= scores <= upper
    return float(mask.mean())


def daily_nps_explanation(current_df: pd.DataFrame) -> list[str]:
    if current_df is None or current_df.empty or "NPS" not in current_df.columns:
        return []
    scores = pd.to_numeric(current_df["NPS"], errors="coerce").dropna()
    if scores.empty:
        return []
    fallback = [
        "NPS clásico = promotores menos detractores; se usa para seguir la señal neta del periodo."
    ]
    if "Fecha" not in current_df.columns:
        return fallback

    daily = current_df.copy()
    daily["Fecha"] = pd.to_datetime(daily["Fecha"], errors="coerce")
    daily = daily.dropna(subset=["Fecha"]).sort_values("Fecha")
    if daily.empty:
        return fallback

    grouped = (
        daily.assign(day=daily["Fecha"].dt.normalize())
        .groupby("day")
        .agg(
            n=("NPS", "size"),
            detractor_rate=("NPS", lambda series: _rate(series, upper=6)),
            promoter_rate=("NPS", lambda series: _rate(series, lower=9)),
        )
        .reset_index()
        .sort_values("day")
    )
    if grouped.empty:
        return fallback

    grouped["classic_nps"] = (
        pd.to_numeric(grouped["promoter_rate"], errors="coerce")
        - pd.to_numeric(grouped["detractor_rate"], errors="coerce")
    ) * 100.0
    start_classic = float(grouped["classic_nps"].iloc[0])
    end_classic = float(grouped["classic_nps"].iloc[-1])
    start_detr = float(grouped["detractor_rate"].iloc[0])
    end_detr = float(grouped["detractor_rate"].iloc[-1])
    return [
        f"El periodo arranca con NPS clásico **{start_classic:.1f}** y termina en **{end_classic:.1f}**.",
        f"El peso detractor pasa de **{start_detr * 100.0:.1f}%** a **{end_detr * 100.0:.1f}%**.",
        fallback[0],
    ]
