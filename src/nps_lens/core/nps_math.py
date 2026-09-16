from __future__ import annotations

from typing import Any, Literal, cast

import numpy as np
import pandas as pd

FocusGroup = Literal["detractor", "passive", "promoter"]

_DAILY_METRICS_COLUMNS = [
    "day",
    "n",
    "det_pct",
    "pas_pct",
    "pro_pct",
    "classic_nps",
    "detractor_rate",
    "passive_rate",
    "promoter_rate",
    "nps_avg",
]


def normalize_focus_group(focus_group: str) -> FocusGroup:
    value = str(focus_group or "detractor").strip().lower()
    if value == "promoter":
        return "promoter"
    if value == "passive":
        return "passive"
    return "detractor"


def _score_series(df: pd.DataFrame, *, score_col: str = "NPS") -> pd.Series[Any]:
    if score_col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[score_col], errors="coerce")


def normalize_nps_scores(scores: pd.Series[Any]) -> pd.Series[Any]:
    """Coerce numeric scores in the 0–10 range to their nearest integer.

    NPS is non-negative, so ``floor(value + 0.5)`` provides deterministic
    half-up rounding and avoids Python's banker rounding at ``x.5``.
    """

    numeric = pd.to_numeric(scores, errors="coerce")
    valid = numeric.between(0, 10) & np.isfinite(numeric)
    rounded: pd.Series[Any] = cast(Any, np.floor(numeric + 0.5))
    return rounded.where(valid)


def classify_nps_scores(scores: pd.Series[Any]) -> pd.Series[Any]:
    """Derive the snapshot group exclusively from a valid integer NPS score."""
    score = pd.to_numeric(scores, errors="coerce")
    valid = score.between(0, 10) & score.mod(1).eq(0)
    groups = pd.Series("", index=scores.index, dtype="string")
    groups.loc[valid & score.le(6)] = "DETRACTOR"
    groups.loc[valid & score.between(7, 8)] = "PASIVO"
    groups.loc[valid & score.ge(9)] = "PROMOTOR"
    return groups


def _focus_mask_from_series(
    score: pd.Series[Any],
    *,
    focus_group: FocusGroup,
) -> pd.Series[Any]:
    labels = {"detractor": "DETRACTOR", "passive": "PASIVO", "promoter": "PROMOTOR"}
    return classify_nps_scores(score).eq(labels[focus_group])


def focus_mask(
    df: pd.DataFrame,
    *,
    focus_group: str,
    score_col: str = "NPS",
) -> pd.Series[Any]:
    fg = normalize_focus_group(focus_group)
    score = _score_series(df, score_col=score_col)
    return _focus_mask_from_series(score, focus_group=fg)


def filter_by_nps_group(
    df: pd.DataFrame,
    group_mode: str,
    *,
    score_col: str = "NPS",
) -> pd.DataFrame:
    gm = str(group_mode or "Todos").strip().lower()
    if gm in {"todos", "all"}:
        return df
    if df is None or df.empty:
        return df
    if gm.startswith("prom"):
        return df.loc[focus_mask(df, focus_group="promoter", score_col=score_col)]
    if gm.startswith("neu") or gm.startswith("pas"):
        return df.loc[focus_mask(df, focus_group="passive", score_col=score_col)]
    return df.loc[focus_mask(df, focus_group="detractor", score_col=score_col)]


def daily_metrics(
    df: pd.DataFrame,
    *,
    days: int | None = None,
    date_col: str = "Fecha",
    score_col: str = "NPS",
) -> pd.DataFrame:
    if df is None or df.empty or date_col not in df.columns or score_col not in df.columns:
        return pd.DataFrame(columns=_DAILY_METRICS_COLUMNS)

    day = pd.to_datetime(df[date_col], errors="coerce").dt.floor("D")
    score = _score_series(df, score_col=score_col)
    score = score.where(classify_nps_scores(score).ne(""))
    work = pd.DataFrame({"day": day, "score": score}).dropna(subset=["day", "score"])
    if work.empty:
        return pd.DataFrame(columns=_DAILY_METRICS_COLUMNS)

    if days is not None:
        end = pd.Timestamp(work["day"].max())
        start = end - pd.Timedelta(days=max(int(days), 1) - 1)
        work = work.loc[work["day"] >= start]
        if work.empty:
            return pd.DataFrame(columns=_DAILY_METRICS_COLUMNS)

    work = work.assign(
        is_det=work["score"] <= 6.0,
        is_pas=(work["score"] >= 7.0) & (work["score"] <= 8.0),
        is_pro=work["score"] >= 9.0,
    )

    agg = (
        work.groupby("day", as_index=False)
        .agg(
            n=("score", "size"),
            detractor_rate=("is_det", "mean"),
            passive_rate=("is_pas", "mean"),
            promoter_rate=("is_pro", "mean"),
            nps_avg=("score", "mean"),
        )
        .sort_values("day")
    )
    agg["det_pct"] = agg["detractor_rate"] * 100.0
    agg["pas_pct"] = agg["passive_rate"] * 100.0
    agg["pro_pct"] = agg["promoter_rate"] * 100.0
    agg["classic_nps"] = (agg["promoter_rate"] - agg["detractor_rate"]) * 100.0
    return agg[_DAILY_METRICS_COLUMNS].copy()


def grouped_focus_rates(
    df: pd.DataFrame,
    *,
    frequency: Literal["D", "W"] = "D",
    date_col: str = "Fecha",
    score_col: str = "NPS",
) -> pd.DataFrame:
    if df is None or df.empty or date_col not in df.columns:
        period_col = "date" if frequency == "D" else "week"
        return pd.DataFrame(
            columns=[period_col, "responses", "detractor_rate", "passive_rate", "promoter_rate"]
        )

    date = pd.to_datetime(df[date_col], errors="coerce")
    period_col = "date" if frequency == "D" else "week"
    bucket = date.dt.normalize() if frequency == "D" else date.dt.to_period("W").dt.start_time
    work = pd.DataFrame(
        {
            period_col: bucket,
            "_score": _score_series(df, score_col=score_col),
        }
    ).dropna(subset=[period_col])
    if work.empty:
        return pd.DataFrame(
            columns=[period_col, "responses", "detractor_rate", "passive_rate", "promoter_rate"]
        )

    work["_is_detractor"] = _focus_mask_from_series(work["_score"], focus_group="detractor")
    work["_is_passive"] = _focus_mask_from_series(work["_score"], focus_group="passive")
    work["_is_promoter"] = _focus_mask_from_series(work["_score"], focus_group="promoter")

    out = (
        work.groupby(period_col, as_index=False)
        .agg(
            responses=(period_col, "size"),
            detractor_rate=("_is_detractor", "mean"),
            passive_rate=("_is_passive", "mean"),
            promoter_rate=("_is_promoter", "mean"),
        )
        .sort_values(period_col)
    )
    return out
