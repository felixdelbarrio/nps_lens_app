from __future__ import annotations

from typing import Literal

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


def _score_series(df: pd.DataFrame, *, score_col: str = "NPS") -> pd.Series:
    if score_col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[score_col], errors="coerce")


def _group_series(df: pd.DataFrame, *, group_col: str = "NPS Group") -> pd.Series:
    if group_col not in df.columns:
        return pd.Series("", index=df.index, dtype="string")
    return df[group_col].astype(str).str.strip().str.lower()


def _focus_mask_from_series(score: pd.Series, group: pd.Series, *, focus_group: FocusGroup) -> pd.Series:
    if focus_group == "promoter":
        return group.str.contains("promot", na=False) | (score >= 9.0)
    if focus_group == "passive":
        return group.str.contains("pas|neu", na=False) | ((score >= 7.0) & (score <= 8.0))
    return group.str.contains("detr", na=False) | (score <= 6.0)


def focus_mask(
    df: pd.DataFrame,
    *,
    focus_group: str,
    score_col: str = "NPS",
    group_col: str = "NPS Group",
) -> pd.Series:
    fg = normalize_focus_group(focus_group)
    score = _score_series(df, score_col=score_col)
    group = _group_series(df, group_col=group_col)
    return _focus_mask_from_series(score, group, focus_group=fg)


def filter_by_nps_group(
    df: pd.DataFrame,
    group_mode: str,
    *,
    score_col: str = "NPS",
    group_col: str = "NPS Group",
) -> pd.DataFrame:
    gm = str(group_mode or "Todos").strip().lower()
    if gm in {"todos", "all"}:
        return df
    if df is None or df.empty:
        return df
    if gm.startswith("prom"):
        return df.loc[focus_mask(df, focus_group="promoter", score_col=score_col, group_col=group_col)]
    if gm.startswith("neu") or gm.startswith("pas"):
        return df.loc[focus_mask(df, focus_group="passive", score_col=score_col, group_col=group_col)]
    return df.loc[focus_mask(df, focus_group="detractor", score_col=score_col, group_col=group_col)]


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
    score = _score_series(df, score_col=score_col).clip(lower=0.0, upper=10.0)
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
    group_col: str = "NPS Group",
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
            "_group": _group_series(df, group_col=group_col),
        }
    ).dropna(subset=[period_col])
    if work.empty:
        return pd.DataFrame(
            columns=[period_col, "responses", "detractor_rate", "passive_rate", "promoter_rate"]
        )

    work["_is_detractor"] = _focus_mask_from_series(
        work["_score"], work["_group"], focus_group="detractor"
    )
    work["_is_passive"] = _focus_mask_from_series(
        work["_score"], work["_group"], focus_group="passive"
    )
    work["_is_promoter"] = _focus_mask_from_series(
        work["_score"], work["_group"], focus_group="promoter"
    )

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
