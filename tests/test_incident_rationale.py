from __future__ import annotations

import pandas as pd

from nps_lens.analytics.incident_rationale import (
    build_incident_nps_rationale,
    summarize_incident_nps_rationale,
)


def test_build_incident_nps_rationale_prioritizes_high_impact_topic() -> None:
    weeks = pd.date_range("2026-01-05", periods=8, freq="W-MON")
    by_topic = pd.DataFrame(
        {
            "week": list(weeks) + list(weeks),
            "nps_topic": ["Pagos > SPEI"] * 8 + ["Acceso > Login"] * 8,
            "responses": [120] * 16,
            # Topic A: clear worsening when incidents rise
            "focus_rate": [0.12, 0.13, 0.12, 0.14, 0.22, 0.24, 0.25, 0.23]
            + [0.11, 0.10, 0.12, 0.11, 0.12, 0.11, 0.10, 0.11],
            "incidents": [1, 1, 1, 2, 9, 8, 10, 9] + [1, 1, 2, 1, 2, 1, 2, 1],
        }
    )
    rank = pd.DataFrame(
        {
            "nps_topic": ["Pagos > SPEI", "Acceso > Login"],
            "score": [0.82, 0.30],
            "corr": [0.71, 0.12],
            "best_lag_weeks": [1, 4],
            "max_cp_stability": [0.73, 0.20],
            "incidents_lead_changepoint_share": [0.75, 0.20],
        }
    )

    out = build_incident_nps_rationale(
        by_topic,
        focus_group="detractor",
        rank_df=rank,
        min_topic_responses=100,
        recovery_factor=0.65,
    )

    assert not out.empty
    assert out.iloc[0]["nps_topic"] == "Pagos > SPEI"
    assert float(out.iloc[0]["nps_points_at_risk"]) > float(out.iloc[1]["nps_points_at_risk"])
    assert float(out.iloc[0]["priority"]) >= float(out.iloc[1]["priority"])

    summary = summarize_incident_nps_rationale(out)
    assert summary.topics_analyzed == 2
    assert summary.nps_points_at_risk > 0
    assert summary.nps_points_recoverable > 0


def test_build_incident_nps_rationale_handles_empty_input() -> None:
    out = build_incident_nps_rationale(pd.DataFrame())
    assert out.empty
    summary = summarize_incident_nps_rationale(out)
    assert summary.topics_analyzed == 0
    assert summary.nps_points_at_risk == 0.0
