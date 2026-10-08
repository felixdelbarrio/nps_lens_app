from __future__ import annotations

import pandas as pd
import pytest

from nps_lens.analytics.nps_gaps import nps_gaps, select_gap_population
from nps_lens.reports.executive_ppt import _build_dimension_view_model
from nps_lens.services.dashboard_service import DashboardService
from nps_lens.ui.theme import get_theme


@pytest.mark.parametrize("dimension", ["Palanca", "Subpalanca"])
@pytest.mark.parametrize("channel", ["Todos", "Web", "App"])
def test_app_and_ppt_use_identical_global_baseline_gaps(dimension, channel):
    current = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-08-01"] * 4),
            "NPS": [0, 0, 10, 0],
            "Palanca": ["A", "A", "B", "Nuevo"],
            "Subpalanca": ["a", "a", "b", "nuevo"],
            "Canal": ["Web", "App", "App", "Web"],
        }
    )
    baseline = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-07-01"] * 4),
            "NPS": [0, 10, 10, 10],
            "Palanca": ["A", "B", "B", "B"],
            "Subpalanca": ["a", "b", "b", "b"],
            "Canal": ["Web", "App", "App", "App"],
        }
    )
    result = nps_gaps(current, baseline, dimension, channel=channel)
    app = object.__new__(DashboardService)._build_gap_payload(
        current, baseline, dimension, get_theme("light"), channel=channel
    )
    ppt = _build_dimension_view_model(
        dimension=dimension,
        selected_raw=current,
        current_source_period=current,
        baseline_source_period=baseline,
        topic_channel=channel,
    )
    assert app["base_nps"] == ppt.base_nps == 50
    assert ppt.base_n == 4
    expected = result.rows.loc[result.rows.gap_vs_base.lt(0)].head(4).reset_index(drop=True)
    pd.testing.assert_frame_equal(ppt.gap_table_df.reset_index(drop=True), expected)
    for row in ppt.gap_table_df.to_dict("records"):
        actual = next(item for item in app["table"] if item["value"] == row["value"])
        assert actual["gap_vs_base"] == row["gap_vs_base"] == row["nps"] - 50
    if channel != "App":
        assert result.rows.value.tolist()[:2] == (
            ["A", "Nuevo"] if dimension == "Palanca" else ["a", "nuevo"]
        )
        assert result.rows.gap_vs_base.tolist()[:2] == [-150, -150]


def test_shared_gap_windows_use_latest_month_for_accumulated_scope():
    frame = pd.DataFrame(
        {"Fecha": pd.to_datetime(["2025-12-01", "2026-01-01", "2026-08-18"]), "NPS": [10, 0, 0]}
    )
    result = select_gap_population(frame, pop_year="2026", pop_month="Todos")
    assert result.current.Fecha.tolist() == [pd.Timestamp("2026-08-18")]
    assert len(result.baseline) == 2
    assert result.base_window.end.isoformat() == "2026-07-31"
