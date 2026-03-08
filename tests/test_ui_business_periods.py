from __future__ import annotations

import pandas as pd

from nps_lens.ui.business import default_windows, slice_by_window
from nps_lens.ui.narratives import compare_periods


def test_default_windows_uses_latest_month_and_all_previous_history() -> None:
    df = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(
                [
                    "2026-01-05",
                    "2026-02-10",
                    "2026-02-27",
                    "2026-03-02",
                    "2026-03-08",
                ]
            ),
            "NPS": [8, 7, 6, 9, 5],
        }
    )

    current, base = default_windows(df)

    assert current is not None
    assert base is not None
    assert current.start.isoformat() == "2026-03-01"
    assert current.end.isoformat() == "2026-03-08"
    assert base.start.isoformat() == "2026-01-05"
    assert base.end.isoformat() == "2026-02-28"


def test_default_windows_falls_back_to_historical_prior_to_current_month() -> None:
    df = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(
                [
                    "2025-11-14",
                    "2025-12-20",
                    "2026-01-15",
                    "2026-03-03",
                    "2026-03-08",
                ]
            ),
            "NPS": [8, 7, 6, 9, 5],
        }
    )

    current, base = default_windows(df)

    assert current is not None
    assert base is not None
    assert current.start.isoformat() == "2026-03-01"
    assert current.end.isoformat() == "2026-03-08"
    assert base.start.isoformat() == "2025-11-14"
    assert base.end.isoformat() == "2026-02-28"

    comp = compare_periods(slice_by_window(df, current), slice_by_window(df, base))
    assert "Mes actual" in comp.label_current
    assert "Base histórica anterior" in comp.label_baseline


def test_default_windows_respects_selected_context_month() -> None:
    df = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(
                [
                    "2025-11-14",
                    "2025-12-20",
                    "2026-01-15",
                    "2026-02-03",
                    "2026-02-22",
                    "2026-03-08",
                ]
            ),
            "NPS": [8, 7, 6, 7, 9, 5],
        }
    )

    current, base = default_windows(df, pop_year="2026", pop_month="02")

    assert current is not None
    assert base is not None
    assert current.start.isoformat() == "2026-02-01"
    assert current.end.isoformat() == "2026-02-22"
    assert base.start.isoformat() == "2025-11-14"
    assert base.end.isoformat() == "2026-01-31"

    comp = compare_periods(slice_by_window(df, current), slice_by_window(df, base))
    assert "Febrero 2026" in comp.label_current
    assert "Base histórica anterior a Febrero 2026" in comp.label_baseline
