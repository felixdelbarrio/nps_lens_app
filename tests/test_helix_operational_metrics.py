from __future__ import annotations

import pandas as pd

from nps_lens.analytics.helix_operational_metrics import (
    build_helix_operational_benchmark,
    enrich_chain_with_operational_metrics,
    enrich_rationale_with_operational_metrics,
    summarize_operational_metrics_for_incidents,
)
from nps_lens.ingest.helix_dates import coerce_helix_datetime_series
from nps_lens.ingest.helix_incidents import read_helix_incidents_excel
from nps_lens.testing.fixtures import fixture_excel


def test_build_helix_operational_benchmark_aggregates_support_orgs_only() -> None:
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC-1", "INC-1", "INC-2", "INC-3"],
            "Assigned Support Organization": [
                "Producto",
                "Tecnologia",
                "Operaciones",
                "",
            ],
            "Status": ["Assigned", "Resolved", "Resolved", "Closed"],
        }
    )

    benchmark = build_helix_operational_benchmark(helix)

    assert benchmark.incident_to_support_orgs["INC-1"] == ("Producto", "Tecnologia")
    assert benchmark.incident_to_support_orgs["INC-2"] == ("Operaciones",)
    metrics = summarize_operational_metrics_for_incidents(["INC-1", "INC-2"], benchmark)
    assert metrics.support_organizations == "Producto · Tecnologia · Operaciones"


def test_build_helix_operational_benchmark_handles_iteracion_17_fixture() -> None:
    path = fixture_excel("issues_20260506_173749.xlsx")
    result = read_helix_incidents_excel(
        str(path),
        service_origin="BBVA México",
        service_origin_n1="ENTERPRISE WEB",
        service_origin_n2="",
    )

    benchmark = build_helix_operational_benchmark(result.df)

    assert len(result.df) == 2255
    assert len(benchmark.incident_to_support_orgs) == 2255


def test_helix_mixed_datetime_values_are_homogeneous_and_controlled() -> None:
    parsed = coerce_helix_datetime_series(
        pd.Series(
            [
                "2026-05-06T05:00:00+00:00",
                1778052089000,
                1778052089,
                46148,
                "06/05/2026 07:30",
                None,
                "no-es-fecha",
            ]
        )
    )

    assert str(parsed.dtype) == "datetime64[ns]"
    assert parsed.notna().sum() == 5


def test_enrich_rationale_with_real_support_organization() -> None:
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC-10"],
            "Assigned Support Organization": ["Canal Digital"],
        }
    )
    benchmark = build_helix_operational_benchmark(helix)
    rationale_df = pd.DataFrame(
        [
            {
                "nps_topic": "Operativa > Pagos",
                "support_organizations": "VoC + Analitica",
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

    assert enriched.iloc[0]["support_organizations"] == "Canal Digital"
    assert "historical_resolution_weeks" not in enriched.columns


def test_enrich_chain_with_operational_metrics_uses_all_linked_incidents() -> None:
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC-20", "INC-21"],
            "Assigned Support Organization": ["Producto", "Tecnologia"],
        }
    )
    benchmark = build_helix_operational_benchmark(helix)
    chain_df = pd.DataFrame(
        [
            {
                "incident_records": [{"incident_id": "INC-20"}],
                "evidence_pairs": [("INC-20", "n1"), ("INC-21", "n1")],
                "support_organizations": "",
            }
        ]
    )

    enriched = enrich_chain_with_operational_metrics(chain_df, benchmark=benchmark)

    assert enriched.iloc[0]["support_organizations"] == "Producto · Tecnologia"
    assert "historical_resolution_weeks" not in enriched.columns
