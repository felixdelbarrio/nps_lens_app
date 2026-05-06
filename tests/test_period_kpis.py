from __future__ import annotations

import pandas as pd

from nps_lens.services.analytics.kpis_service import build_period_kpis, format_percentage


def test_period_kpis_centralize_classic_nps_start_and_end_of_selected_period() -> None:
    frame = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(
                [
                    "2026-01-01",
                    "2026-01-01",
                    "2026-02-01",
                    "2026-02-01",
                    "2026-03-01",
                    "2026-03-01",
                    "2026-03-31",
                    "2026-03-31",
                ]
            ),
            "NPS": [0, 0, 10, 10, 10, 10, 0, 0],
            "Comment": ["útil"] * 8,
        }
    )

    scope = build_period_kpis(
        history_df=frame,
        current_df=frame[frame["Fecha"].dt.month == 3],
        pop_year="2026",
        pop_month="03",
        context_label="Marzo 2026",
    )

    cumulative = scope["cumulative"]
    assert cumulative["base_kpis"]["classic_nps"] == 0.0
    assert cumulative["base_display"]["classic_nps"] == "0,00"

    temporal = scope["period"]["temporal"]
    assert temporal["base_display"]["classic_nps"] == "100,00"
    assert temporal["display"]["classic_nps"] == "-100,00"
    assert temporal["deltas"]["classic_nps"]["display"] == "↓ -200,00 pts"
    assert temporal["display"]["detractor_rate"] == "100,00%"
    assert scope["period"]["display"]["comments"] == "4"


def test_period_kpi_percentage_formatter_has_no_visual_drift_space() -> None:
    assert format_percentage(0.1523) == "15,23%"
