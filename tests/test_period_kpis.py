from __future__ import annotations

import pandas as pd

from nps_lens.services.analytics.kpis_service import (
    build_period_aggregates,
    build_period_kpis,
    cumulative_until_period,
    format_percentage,
)


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

    assert set(scope) == {"historical", "period", "cumulative", "temporal", "period_aggregates"}
    assert scope["historical"]["period_type"] == "historical_previous"
    assert scope["period"]["period_type"] == "current_period"
    assert scope["cumulative"]["period_type"] == "cumulative_to_current"
    assert scope["temporal"]["period_type"] == "internal_period_evolution"

    historical = scope["historical"]
    assert historical["label"] == "Febrero 2026"
    assert historical["kpis"]["samples"] == 2
    assert historical["kpis"]["classic_nps"] == 0.0
    assert historical["deltas"] is None
    assert historical["show_deltas"] is False

    period = scope["period"]
    assert period["kpis"]["comments"] == 3
    assert period["kpis"]["nps_average"] == 5.0
    assert period["kpis"]["classic_nps"] == 0.0
    assert period["kpis"]["detractor_rate"] == 0.5
    assert period["kpis"]["promoter_rate"] == 0.5
    assert period["show_deltas"] is True
    assert period["base_label"] == "Febrero 2026"
    assert period["deltas"]["classic_nps"]["display"] == "+0,00 pts"
    assert list(period["display"].keys()) == [
        "comments",
        "nps_average",
        "classic_nps",
        "detractor_rate",
        "promoter_rate",
    ]

    cumulative = scope["cumulative"]
    assert cumulative["note"] == "KPIs agregados para el periodo del 2026-01-01 al 2026-03-31."
    assert cumulative["kpis"]["samples"] == 8
    assert cumulative["kpis"]["classic_nps"] == -25.0
    assert cumulative["deltas"] is None
    assert cumulative["show_deltas"] is False

    temporal = scope["temporal"]
    assert temporal["base_display"]["classic_nps"] == "100,00"
    assert temporal["display"]["classic_nps"] == "-100,00"
    assert temporal["deltas"]["classic_nps"]["display"] == "-200,00 pts"
    assert temporal["display"]["detractor_rate"] == "100,00%"
    assert scope["period"]["temporal"] == temporal

    aggregates = scope["period_aggregates"]
    assert [item["label"] for item in aggregates] == ["Enero 2026", "Febrero 2026", "Marzo 2026"]
    assert aggregates[1]["nps_average"] == historical["kpis"]["nps_average"]
    assert aggregates[2]["nps_average"] == period["kpis"]["nps_average"]


def test_cumulative_until_period_includes_complete_unique_history_to_selected_month() -> None:
    frame = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(
                [
                    "2026-01-05",
                    "2026-02-05",
                    "2026-03-05",
                    "2026-04-05",
                ]
            ),
            "NPS": [0, 10, 10, 0],
            "Comment": ["ene", "feb", "mar", "abr"],
        }
    )

    cumulative = cumulative_until_period(frame, "2026", "03")
    assert cumulative["Comment"].tolist() == ["ene", "feb", "mar"]

    aggregates = build_period_aggregates(frame, "2026", "03")
    assert [item["label"] for item in aggregates] == ["Enero 2026", "Febrero 2026", "Marzo 2026"]


def test_period_kpi_percentage_formatter_has_no_visual_drift_space() -> None:
    assert format_percentage(0.1523) == "15,23%"
