from __future__ import annotations

import pandas as pd

from nps_lens.core.nps_math import daily_metrics, filter_by_nps_group, grouped_focus_rates


def test_filter_by_nps_group_uses_label_or_score() -> None:
    df = pd.DataFrame(
        {
            "NPS": [10, 8, 6, None],
            "NPS Group": ["Promoter", "Neutral", "Detractor", "Promoter"],
            "Fecha": pd.to_datetime(["2026-01-01"] * 4),
        }
    )
    det = filter_by_nps_group(df, "Detractores")
    pro = filter_by_nps_group(df, "Promotores")
    pas = filter_by_nps_group(df, "Neutros")

    assert len(det) == 1
    assert len(pro) == 2
    assert len(pas) == 1


def test_daily_metrics_computes_expected_columns() -> None:
    df = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-01-01", "2026-01-01", "2026-01-02", "2026-01-02"]),
            "NPS": [10, 6, 7, 9],
        }
    )
    out = daily_metrics(df, days=2)
    assert list(out.columns) == [
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
    day1 = out.loc[out["day"] == pd.Timestamp("2026-01-01")].iloc[0]
    assert int(day1["n"]) == 2
    assert abs(float(day1["classic_nps"]) - 0.0) < 1e-9


def test_grouped_focus_rates_daily() -> None:
    df = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-01-01", "2026-01-01", "2026-01-02"]),
            "NPS": [10, 6, 8],
            "NPS Group": ["Promoter", "Detractor", "Passive"],
        }
    )
    out = grouped_focus_rates(df, frequency="D")
    assert {"date", "responses", "detractor_rate", "passive_rate", "promoter_rate"} <= set(
        out.columns
    )
    first = out.loc[out["date"] == pd.Timestamp("2026-01-01")].iloc[0]
    assert int(first["responses"]) == 2
    assert abs(float(first["promoter_rate"]) - 0.5) < 1e-9
    assert abs(float(first["detractor_rate"]) - 0.5) < 1e-9
