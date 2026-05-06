from __future__ import annotations

import pandas as pd

from nps_lens.services.analytics.kpis_service import build_period_kpis, format_percentage


def test_period_kpis_use_official_temporal_taxonomy_and_aggregated_period_nps() -> None:
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
            "NPS": [0, 0, 10, 0, 10, 10, 0, 0],
            "Comment": ["útil"] * 7 + [""],
        }
    )

    scope = build_period_kpis(
        history_df=frame,
        current_df=frame[frame["Fecha"].dt.month == 3],
        pop_year="2026",
        pop_month="03",
        context_label="Marzo 2026",
    )

    assert set(scope) == {"historical", "period", "cumulative", "temporal"}
    assert scope["historical"]["period_type"] == "historical_previous"
    assert scope["period"]["period_type"] == "current_period"
    assert scope["cumulative"]["period_type"] == "cumulative_to_current"
    assert scope["temporal"]["period_type"] == "internal_period_evolution"

    historical = scope["historical"]
    assert historical["kpis"]["classic_nps"] == -50.0
    assert historical["deltas"] is None
    assert historical["show_deltas"] is False

    period = scope["period"]
    assert period["kpis"]["comments"] == 3
    assert period["kpis"]["nps_average"] == 5.0
    assert period["kpis"]["classic_nps"] == 0.0
    assert period["kpis"]["detractor_rate"] == 0.5
    assert period["kpis"]["promoter_rate"] == 0.5
    assert period["show_deltas"] is True
    assert period["deltas"]["classic_nps"]["display"] == "+50,00 pts"
    assert list(period["display"].keys()) == [
        "comments",
        "nps_average",
        "classic_nps",
        "detractor_rate",
        "promoter_rate",
    ]

    cumulative = scope["cumulative"]
    assert cumulative["kpis"]["classic_nps"] == -25.0
    assert cumulative["deltas"] is None
    assert cumulative["show_deltas"] is False

    temporal = scope["temporal"]
    assert temporal["base_display"]["classic_nps"] == "100,00"
    assert temporal["display"]["classic_nps"] == "-100,00"
    assert temporal["deltas"]["classic_nps"]["display"] == "-200,00 pts"
    assert temporal["display"]["detractor_rate"] == "100,00%"
    assert scope["period"]["temporal"] == temporal


def test_period_kpi_percentage_formatter_has_no_visual_drift_space() -> None:
    assert format_percentage(0.1523) == "15,23%"
