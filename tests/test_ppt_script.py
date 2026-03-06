from __future__ import annotations

import pandas as pd

from nps_lens.analytics.incident_rationale import IncidentRationaleSummary
from nps_lens.ui.narratives import build_ppt_8slide_script


def test_build_ppt_8slide_script_contains_all_slides() -> None:
    summary = IncidentRationaleSummary(
        topics_analyzed=3,
        nps_points_at_risk=1.8,
        nps_points_recoverable=1.1,
        top3_incident_share=0.72,
        confidence_mean=0.64,
        median_lag_weeks=1.5,
    )
    rationale_df = pd.DataFrame(
        [
            {
                "nps_topic": "Pagos > SPEI",
                "priority": 0.81,
                "confidence": 0.74,
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
    assert "NPS en riesgo" in out
    assert "Decisiones requeridas al comité" in out
