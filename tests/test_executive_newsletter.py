from __future__ import annotations

from datetime import date

import pandas as pd

from nps_lens.reports.executive_newsletter import build_executive_newsletter
from nps_lens.services.analytics.kpis_service import build_period_kpis


def test_newsletter_uses_monthly_kpis_and_keeps_only_the_short_editorial_model() -> None:
    history = pd.DataFrame(
        {
            "Fecha": pd.to_datetime(
                ["2026-06-10", "2026-06-11", "2026-07-01", "2026-07-10", "2026-07-20"]
            ),
            "Canal": ["Web"] * 5,
            "Palanca": [
                "Uso",
                "Uso",
                "Funcionamiento continuo",
                "Funcionamiento continuo",
                "Pagos/transferencias",
            ],
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
            "cards": [
                {
                    "title": "Funcionamiento continuo / Fallas en login",
                    "avg_nps": 2.0,
                    "linked_pairs": 3,
                    "comment_records": [{"comment": "no funciona"}],
                    "incident_records": [{"incident_id": "INC0001", "summary": "Error de acceso"}],
                }
            ]
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
    assert result["headline"] == "El principal foco de fricción está en funcionamiento continuo"
    assert [item["label"] for item in result["scorecard"]] == [
        "Comentarios",
        "NPS clásico mensual",
        "Score medio",
        "Promotores",
        "Detractores",
    ]
    assert result["scorecard"][1]["value"] == "-33,33"
    assert result["quotes"] == ["no funciona"]
    assert result["signals"][0]["label"] == "Funcionamiento continuo"
    assert "insights" not in result
    assert "focus" not in result
    assert "connections" not in result
