import pandas as pd
import pytest

from nps_lens.analytics.incident_rationale import (
    RATIONALE_COLUMNS,
    build_incident_nps_rationale,
)

FORBIDDEN = {
    "score_mean_difference",
    "focus_rate_difference_pp",
    "confidence",
    "confidence_mean",
    "causal_score",
    "priority",
    "nps_points_at_risk",
    "nps_points_recoverable",
    "attributable_focus_cases",
    "total_nps_impact",
    "action_lane",
}


def _weekly() -> pd.DataFrame:
    weeks = pd.date_range("2026-01-05", periods=8, freq="W-MON")
    return pd.DataFrame(
        {
            "week": list(weeks) * 2,
            "nps_topic": ["Pagos > SPEI"] * 8 + ["Acceso > Login"] * 8,
            "responses": [100, 200] * 8,
            "nps_mean": [8.2, 8.1, 8.0, 8.0, 6.2, 6.0, 5.8, 6.0] + [8.4] * 8,
            "focus_rate": [0.12, 0.13, 0.12, 0.14, 0.22, 0.24, 0.25, 0.23] + [0.11] * 8,
            "incidents": [1, 1, 1, 2, 9, 8, 10, 9] + [1, 1, 2, 1, 2, 1, 2, 1],
        }
    )


def test_rationale_contains_only_observed_or_reproducible_statistics() -> None:
    result = build_incident_nps_rationale(_weekly(), min_topic_responses=100)
    assert list(result.columns) == RATIONALE_COLUMNS
    assert FORBIDDEN.isdisjoint(result.columns)
    pagos = result.set_index("nps_topic").loc["Pagos > SPEI"]
    assert pagos["focus_rate_high_incidence"] > pagos["focus_rate_low_incidence"]
    assert pagos["low_incident_weeks"] > 0 and pagos["high_incident_weeks"] > 0


def test_rates_are_weighted_by_responses_and_sparse_inputs_are_omitted() -> None:
    frame = pd.DataFrame(
        {
            "week": pd.date_range("2026-01-05", periods=4, freq="W-MON"),
            "nps_topic": ["A"] * 4,
            "responses": [10, 90, 10, 90],
            "focus_rate": [0.0, 1.0, 0.0, 0.5],
            "nps_mean": [10.0, 8.0, 4.0, 6.0],
            "incidents": [0, 0, 10, 10],
        }
    )
    row = build_incident_nps_rationale(frame, min_topic_responses=1).iloc[0]
    assert row["focus_rate_low_incidence"] == pytest.approx(0.9)
    assert row["focus_rate_high_incidence"] == pytest.approx(0.45)
    assert build_incident_nps_rationale(frame, min_topic_responses=1000).empty
    assert build_incident_nps_rationale(pd.DataFrame()).empty
