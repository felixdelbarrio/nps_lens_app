from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from nps_lens.analytics.drivers import DriverStat, compute_nps_from_scores, driver_table
from nps_lens.domain.normalization import clean_label


def rank_nps_gaps(
    df: pd.DataFrame,
    dimensions: Sequence[str],
    survey_score_col: str = "NPS",
    min_n: int = 200,
    *,
    base_nps: float | None = None,
) -> list[DriverStat]:
    """Observed negative gaps against the supplied classic-NPS base."""
    reference = (
        float(base_nps)
        if base_nps is not None
        else compute_nps_from_scores(df[survey_score_col])
    )
    rows = [
        row
        for dimension in dimensions
        for row in driver_table(df, dimension, survey_score_col, base_nps=reference)
        if row.n >= min_n and row.gap_vs_base < 0 and clean_label(row.value)
    ]
    return sorted(rows, key=lambda row: (row.gap_vs_base, -row.n, row.dimension, row.value))
