from __future__ import annotations

import pandas as pd

from nps_lens.analytics.drivers import compute_nps_from_scores, driver_table


def test_compute_nps_basic() -> None:
    s = pd.Series([10, 9, 8, 6, 0])  # 2 promoters, 2 detractors
    assert abs(compute_nps_from_scores(s) - 0.0) < 1e-6


def test_driver_table_returns_rows() -> None:
    df = pd.DataFrame(
        {
            "NPS": [10, 9, 5, 6, 8, 10],
            "Palanca": ["A", "A", "B", "B", "B", "A"],
        }
    )
    out = driver_table(df, "Palanca")
    assert len(out) == 2
    assert {o.value for o in out} == {"A", "B"}
