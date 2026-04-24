from __future__ import annotations

import math

import pandas as pd

from nps_lens.analytics.helix_operational_metrics import (
    build_helix_operational_benchmark,
    enrich_chain_with_operational_metrics,
    enrich_rationale_with_operational_metrics,
    summarize_operational_metrics_for_incidents,
)


def test_build_helix_operational_benchmark_aggregates_support_orgs_and_eta() -> None:
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC-1", "INC-2", "INC-3"],
            "Assigned Support Organization": [
                "Producto, Tecnologia",
                "Operaciones",
                "",
            ],
            "CreatedDate": ["2026-03-01", "2026-03-01", "2026-03-01"],
            "Resolved Date": ["2026-03-08", "2026-03-15", None],
        }
    )

    benchmark = build_helix_operational_benchmark(helix)

    assert benchmark.incident_to_support_orgs["INC-1"] == ("Producto", "Tecnologia")
    assert benchmark.incident_to_support_orgs["INC-2"] == ("Operaciones",)
    assert math.isclose(benchmark.support_org_eta_weeks["Producto"], 1.0)
    assert math.isclose(benchmark.support_org_eta_weeks["Tecnologia"], 1.0)
    assert math.isclose(benchmark.support_org_eta_weeks["Operaciones"], 2.0)
    assert benchmark.overall_eta_weeks is not None
    assert math.isclose(benchmark.overall_eta_weeks, 1.5)

    metrics = summarize_operational_metrics_for_incidents(["INC-1", "INC-2"], benchmark)
    assert metrics.owner_role == "Producto · Tecnologia · Operaciones"
    assert math.isclose(metrics.eta_weeks, (1.0 + 1.0 + 2.0) / 3.0)


def test_enrich_rationale_with_operational_metrics_overrides_heuristic_values_when_helix_has_data() -> (
    None
):
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC-10"],
            "Assigned Support Organization": ["Canal Digital"],
            "CreatedDate": ["2026-03-01"],
            "Resolved Date": ["2026-03-22"],
        }
    )
    benchmark = build_helix_operational_benchmark(helix)
    rationale_df = pd.DataFrame(
        [
            {
                "nps_topic": "Operativa > Pagos",
                "owner_role": "VoC + Analitica",
                "eta_weeks": 3.0,
            }
        ]
    )
    links_df = pd.DataFrame(
        {
            "nps_topic": ["Operativa > Pagos"],
            "incident_id": ["INC-10"],
        }
    )

    enriched = enrich_rationale_with_operational_metrics(
        rationale_df,
        links_df=links_df,
        benchmark=benchmark,
    )

    assert enriched.iloc[0]["owner_role"] == "Canal Digital"
    assert math.isclose(float(enriched.iloc[0]["eta_weeks"]), 3.0)


def test_enrich_chain_with_operational_metrics_uses_incident_records() -> None:
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC-20", "INC-21"],
            "Assigned Support Organization": ["Producto", "Tecnologia"],
            "CreatedDate": ["2026-03-01", "2026-03-01"],
            "Resolved Date": ["2026-03-08", "2026-03-29"],
        }
    )
    benchmark = build_helix_operational_benchmark(helix)
    chain_df = pd.DataFrame(
        [
            {
                "incident_records": [{"incident_id": "INC-20"}, {"incident_id": "INC-21"}],
                "owner_role": "",
                "eta_weeks": float("nan"),
            }
        ]
    )

    enriched = enrich_chain_with_operational_metrics(chain_df, benchmark=benchmark)

    assert enriched.iloc[0]["owner_role"] == "Producto · Tecnologia"
    assert math.isclose(float(enriched.iloc[0]["eta_weeks"]), 2.5)
