from __future__ import annotations

import pandas as pd

from nps_lens.analytics.incident_rationale import IncidentRationaleSummary
from nps_lens.ui.narratives import build_incident_ppt_story, build_ppt_8slide_script


def test_build_ppt_8slide_script_contains_all_slides() -> None:
    summary = IncidentRationaleSummary(
        topics_analyzed=3,
        nps_points_at_risk=1.8,
        nps_points_recoverable=1.1,
        top3_incident_share=0.72,
        confidence_mean=0.64,
        median_lag_weeks=1.5,
        peak_focus_probability=0.46,
        expected_nps_delta=-3.8,
        total_nps_impact=2.2,
    )
    rationale_df = pd.DataFrame(
        [
            {
                "nps_topic": "Pagos > SPEI",
                "touchpoint": "Pagos",
                "priority": 0.81,
                "confidence": 0.74,
                "focus_probability_with_incident": 0.46,
                "nps_delta_expected": -3.8,
                "total_nps_impact": 1.2,
                "causal_score": 0.79,
                "delta_focus_rate_pp": 6.2,
                "nps_points_at_risk": 0.9,
                "nps_points_recoverable": 0.6,
                "action_lane": "Fix estructural",
                "owner_role": "Producto + Tecnologia",
                "eta_weeks": 6,
            }
        ]
    )
    out = build_ppt_8slide_script(
        summary,
        rationale_df,
        service_origin="BBVA México",
        service_origin_n1="Empresas Mobile",
        focus_name="detractores",
        period_label="2026-01-01 -> 2026-02-01",
    )
    assert "Slide 1" in out
    assert "Slide 8" in out
    assert "Impact Chain" in out
    assert "impacto total en Score" in out
    assert "Gobierno y métricas" in out


def test_build_ppt_8slide_script_uses_centralized_attribution_summary() -> None:
    summary = IncidentRationaleSummary(
        topics_analyzed=3,
        nps_points_at_risk=1.8,
        nps_points_recoverable=1.1,
        top3_incident_share=0.72,
        confidence_mean=0.64,
        median_lag_weeks=1.5,
        peak_focus_probability=0.46,
        expected_nps_delta=-3.8,
        total_nps_impact=2.2,
    )
    rationale_df = pd.DataFrame(
        [
            {
                "nps_topic": "Pagos > SPEI",
                "priority": 0.81,
                "confidence": 0.74,
                "causal_score": 0.79,
                "nps_points_at_risk": 0.9,
                "nps_points_recoverable": 0.6,
                "action_lane": "Fix estructural",
                "owner_role": "Producto + Tecnologia",
                "eta_weeks": 6,
            }
        ]
    )
    attribution_df = pd.DataFrame([{"nps_topic": "Pagos / Transferencias", "linked_pairs": 46}])

    out = build_ppt_8slide_script(
        summary,
        rationale_df,
        attribution_df=attribution_df,
        attribution_summary={
            "chains_total": 8,
            "topics_total": 5,
            "linked_incidents_total": 54,
            "linked_comments_total": 82,
            "linked_pairs_total": 132,
        },
        service_origin="BBVA México",
        service_origin_n1="Empresas Mobile",
        focus_name="detractores",
        period_label="2026-01-01 -> 2026-02-01",
    )

    assert "8 cadenas defendibles" in out
    assert "54 incidencias con match" in out
    assert "82 comentarios enlazados" in out
    assert "132 links validados" in out


def test_build_incident_ppt_story_uses_centralized_attribution_summary() -> None:
    summary = IncidentRationaleSummary(
        topics_analyzed=3,
        nps_points_at_risk=1.8,
        nps_points_recoverable=1.1,
        top3_incident_share=0.72,
        confidence_mean=0.64,
        median_lag_weeks=1.5,
        peak_focus_probability=0.46,
        expected_nps_delta=-3.8,
        total_nps_impact=2.2,
    )
    rationale_df = pd.DataFrame([{"nps_topic": "Pagos > SPEI"}])

    out = build_incident_ppt_story(
        summary,
        rationale_df,
        attribution_df=pd.DataFrame(),
        attribution_summary={
            "chains_total": 8,
            "topics_total": 5,
            "linked_incidents_total": 54,
            "linked_comments_total": 82,
            "linked_pairs_total": 132,
        },
        focus_name="detractores",
    )

    assert "8 cadenas defendibles" in out
    assert "54 incidencias con match" in out
    assert "82 comentarios enlazados" in out
    assert "132 links validados" in out
