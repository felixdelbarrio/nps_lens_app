from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from nps_lens.ui.business import driver_delta_table

HistoricDimension = Literal["Palanca", "Subpalanca", "palanca", "subpalanca"]

HISTORIC_CHANGE_COLUMNS: tuple[str, ...] = (
    "value",
    "delta_nps",
    "nps_current",
    "nps_baseline",
    "n_current",
    "n_baseline",
)


@dataclass(frozen=True)
class HistoricChangeSpec:
    dimension: Literal["Palanca", "Subpalanca"]
    min_n: int = 50
    top_n: int | None = None


def normalize_historic_dimension(
    dimension: HistoricDimension | str,
) -> Literal["Palanca", "Subpalanca"]:
    raw = str(dimension or "").strip().lower()
    if raw == "palanca":
        return "Palanca"
    if raw == "subpalanca":
        return "Subpalanca"
    raise ValueError(f"Unsupported historic-change dimension: {dimension!r}")


def get_changes_vs_historic(
    current_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    *,
    dimension: HistoricDimension | str,
    score_col: str = "NPS",
    min_n: int = 50,
    top_n: int | None = None,
) -> pd.DataFrame:
    """Single source of truth for Insights and PPT historic-change datasets.

    The function owns the aggregation, ranking and optional top-N cut used by
    both the dashboard Insights section and the executive PPT slides 5/6.
    Presentation layers must not recompute deltas, counts or ranking.
    """
    normalized_dimension = normalize_historic_dimension(dimension)
    out = driver_delta_table(
        current_df,
        baseline_df,
        dimension=normalized_dimension,
        score_col=score_col,
        min_n=min_n,
    )
    if out.empty:
        return pd.DataFrame(columns=list(HISTORIC_CHANGE_COLUMNS))

    ranked = out.loc[:, list(HISTORIC_CHANGE_COLUMNS)].copy()
    ranked = ranked.sort_values(
        ["delta_nps", "n_current", "n_baseline", "value"],
        ascending=[True, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    if top_n is not None:
        ranked = ranked.head(max(int(top_n), 0)).reset_index(drop=True)
    return ranked


def format_nps(value: object, decimals: int = 2) -> str:
    return _format_locale_number(value, decimals=decimals)


def format_delta(value: object, decimals: int = 2) -> str:
    return _format_locale_number(value, decimals=decimals)


def format_count(value: object) -> str:
    return _format_locale_number(value, decimals=0)


def _format_locale_number(value: object, *, decimals: int) -> str:
    try:
        f = float(value)
    except Exception:
        return "n/d"
    if pd.isna(f):
        return "n/d"
    rendered = f"{f:,.{max(int(decimals), 0)}f}"
    return rendered.replace(",", "_").replace(".", ",").replace("_", ".")
