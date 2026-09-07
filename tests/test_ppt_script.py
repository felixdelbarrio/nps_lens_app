from __future__ import annotations

import pandas as pd

from nps_lens.analytics.incident_rationale import IncidentRationaleSummary
from nps_lens.ui.narratives import build_incident_ppt_story, build_ppt_8slide_script


def _summary() -> IncidentRationaleSummary:
    return IncidentRationaleSummary(
        topics_analyzed=3,
        responses=420,
        incidents=54,
        top3_incident_share=0.72,
        median_lag_weeks=1.5,
    )


def _rationale() -> pd.DataFrame:
    return pd.DataFrame(
        [{
            "nps_topic": "Pagos > SPEI",
            "responses": 120,
            "incidents": 18,
            "incident_rate_per_100_responses": 15.0,
            "focus_rate_difference_pp": 6.2,
            "score_mean_difference": -1.8,
        }]
    )


def _scope() -> dict[str, int]:
    return {
        "chains_total": 8,
        "topics_total": 5,
        "linked_incidents_total": 54,
        "linked_comments_total": 82,
        "linked_pairs_total": 132,
    }


def test_build_ppt_8slide_script_contains_observed_evidence() -> None:
    out = build_ppt_8slide_script(
        _summary(), _rationale(), attribution_summary=_scope(),
        service_origin="BBVA México", service_origin_n1="Empresas Mobile",
        focus_name="detractores", period_label="2026-01-01 -> 2026-02-01",
    )
    assert "Slide 1" in out
    assert "Slide 8" in out
    assert "Vínculos semánticos" in out
    assert "420 respuestas" in out
    assert "54 incidencias" in out
    assert "132 vínculos semánticos" in out
    assert "no prueban causalidad" in out


def test_incident_story_uses_link_counts_and_observed_values() -> None:
    attribution = pd.DataFrame([{
        "nps_topic": "Pagos / Transferencias",
        "linked_pairs": 46,
        "avg_similarity": 0.84,
        "avg_nps": 3.2,
    }])
    out = build_incident_ppt_story(
        _summary(), _rationale(), attribution_df=attribution,
        attribution_summary=_scope(), focus_name="detractores",
    )
    assert "132 vínculos semánticos" in out
    assert "54 incidencias" in out
    assert "82 comentarios" in out
    assert "Pagos / Transferencias" in out
    assert "similitud media 0,84" in out


def test_causal_narratives_do_not_publish_removed_heuristics() -> None:
    outputs = [
        build_incident_ppt_story(
            _summary(), _rationale(), attribution_summary=_scope(), focus_name="detractores"
        ),
        build_ppt_8slide_script(
            _summary(), _rationale(), attribution_summary=_scope(),
            service_origin="BBVA México", service_origin_n1="Empresas Mobile",
            focus_name="detractores", period_label="enero 2026",
        ),
    ]
    forbidden = ("Confianza", "NPS recuperable", "NPS en riesgo", "Cambio esperado", "prioridad")
    assert all(term.lower() not in output.lower() for output in outputs for term in forbidden)
