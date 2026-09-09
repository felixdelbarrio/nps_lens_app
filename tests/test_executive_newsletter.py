from __future__ import annotations

from datetime import date

import pandas as pd

from nps_lens.reports.executive_newsletter import build_executive_newsletter
from nps_lens.services.analytics.kpis_service import build_period_kpis


def test_newsletter_uses_monthly_kpis_and_connects_voc_with_incidents() -> None:
    history = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(["2026-06-10", "2026-06-11", "2026-07-01", "2026-07-10", "2026-07-20"]),
            "Canal": ["Web"] * 5,
            "Palanca": ["Uso", "Uso", "Funcionamiento continuo", "Funcionamiento continuo", "Pagos/transferencias"],
            "NPS": [9, 6, 0, 4, 9],
            "Comment": ["bien", "regular", "no funciona", "falla login", "pago correcto"],
        }
    )
    current = history[history["Fecha"].dt.month == 7]
    period_kpis = build_period_kpis(
        history_df=history,
        current_df=current,
        pop_year="2026",
        pop_month="07",
        context_label="Julio 2026",
    )
    linking = {
        "scenarios": {
            "cards": [{
                "title": "Funcionamiento continuo / Fallas en login",
                "avg_nps": 2.0,
                "linked_pairs": 3,
                "comment_records": [{"comment": "no funciona"}],
                "incident_records": [{"incident_id": "INC0001", "summary": "Error de acceso"}],
            }]
        }
    }

    result = build_executive_newsletter(
        current_df=current,
        period_kpis=period_kpis,
        linking=linking,
        topic_channel="Web",
        period_start=date(2026, 7, 1),
        period_end=date(2026, 7, 20),
    )

    assert result["period"] == "1–20 julio 2026"
    assert result["focus"]["title"] == "Funcionamiento continuo"
    assert [item["label"] for item in result["scorecard"]] == [
        "Comentarios", "NPS clásico mensual", "Score medio", "Promotores", "Detractores"
    ]
    assert result["scorecard"][1]["value"] == "-33,33"
    assert result["connections"][0]["semantic_links"] == 3
    assert result["connections"][0]["incidents"][0]["id"] == "INC0001"
    assert result["quotes"] == ["no funciona"]
    assert "no implica causalidad" in result["connections"][0]["caveat"]
