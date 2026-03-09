from __future__ import annotations

import pandas as pd

from nps_lens.analytics.incident_rationale import (
    _action_plan,
    _clip01,
    _focus_group_norm,
    _norm_by_max,
    _rank_lookup,
    _risk_delta,
    _safe_num,
    _touchpoint_from_topic,
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
            "nps_mean": [8.2, 8.1, 8.1, 8.0, 6.1, 5.9, 5.8, 6.0]
            + [8.4, 8.5, 8.3, 8.4, 8.2, 8.3, 8.4, 8.3],
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
    assert float(out.iloc[0]["focus_probability_with_incident"]) > float(
        out.iloc[0]["focus_rate_base"]
    )
    assert float(out.iloc[0]["nps_delta_expected"]) < 0.0
    assert float(out.iloc[0]["total_nps_impact"]) > 0.0
    assert float(out.iloc[0]["causal_score"]) >= float(out.iloc[1]["causal_score"])

    summary = summarize_incident_nps_rationale(out)
    assert summary.topics_analyzed == 2
    assert summary.nps_points_at_risk > 0
    assert summary.nps_points_recoverable > 0
    assert summary.peak_focus_probability > 0
    assert summary.total_nps_impact > 0


def test_build_incident_nps_rationale_handles_empty_input() -> None:
    out = build_incident_nps_rationale(pd.DataFrame())
    assert out.empty
    summary = summarize_incident_nps_rationale(out)
    assert summary.topics_analyzed == 0
    assert summary.nps_points_at_risk == 0.0


def test_incident_rationale_helper_functions_cover_normalization_paths() -> None:
    assert _clip01("bad") == 0.0
    assert _clip01(1.4) == 1.0
    assert _safe_num("bad", default=3.0) == 3.0
    assert _safe_num("2.5") == 2.5
    assert _focus_group_norm("PROMOTER") == "promoter"
    assert _focus_group_norm("passive") == "passive"
    assert _focus_group_norm("other") == "detractor"
    assert _risk_delta(-0.2, "promoter") == 0.2
    assert _risk_delta(-0.2, "detractor") == 0.0
    assert _touchpoint_from_topic("Pagos > SPEI") == "Pagos"
    assert _touchpoint_from_topic("") == "Journey sin etiquetar"

    norm = _norm_by_max(pd.Series([0, 5, 10]))
    assert norm.tolist() == [0.0, 0.5, 1.0]
    assert _norm_by_max(pd.Series([0, 0])).tolist() == [0.0, 0.0]

    assert _action_plan(0.8, 3.0, 0.7) == ("Fix estructural", "Producto + Tecnologia", 6)
    assert _action_plan(0.5, 1.0, 0.2) == ("Quick win operativo", "Canal + Operaciones", 2)
    assert _action_plan(0.2, 5.0, 0.2) == ("Instrumentacion + validacion", "VoC + Analitica", 3)

    lookup = _rank_lookup(
        pd.DataFrame(
            {
                "nps_topic": ["Pagos > SPEI"],
                "score": [0.8],
                "corr": [0.5],
                "best_lag_weeks": [2],
                "max_cp_stability": [0.6],
                "incidents_lead_changepoint_share": [70],
            }
        )
    )
    assert lookup["Pagos > SPEI"]["lead_share"] == 70.0
    assert _rank_lookup(pd.DataFrame()) == {}


def test_build_incident_nps_rationale_handles_promoter_focus_and_sparse_topics() -> None:
    weeks = pd.date_range("2026-01-05", periods=6, freq="W-MON")
    by_topic = pd.DataFrame(
        {
            "week": list(weeks) * 2,
            "nps_topic": ["Canales > Web"] * 6 + ["Ruido > Poco"] * 6,
            "responses": [120] * 6 + [5] * 6,
            "focus_rate": [0.60, 0.58, 0.57, 0.40, 0.39, 0.38] + [0.4] * 6,
            "incidents": [1, 1, 2, 7, 8, 9] + [0, 0, 0, 0, 0, 0],
            "nps_mean": [8.0, 8.1, 8.2, 6.4, 6.2, 6.1] + [7.0] * 6,
        }
    )

    out = build_incident_nps_rationale(
        by_topic,
        focus_group="promoter",
        rank_df=None,
        min_topic_responses=50,
        recovery_factor=0.5,
    )

    assert not out.empty
    assert out.iloc[0]["nps_topic"] == "Canales > Web"
    assert float(out.iloc[0]["nps_points_at_risk"]) > 0.0
    assert out["nps_topic"].tolist() == ["Canales > Web"]
