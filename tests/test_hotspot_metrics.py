from __future__ import annotations

from datetime import date

import pandas as pd

from nps_lens.analytics.hotspot_metrics import (
    align_hotspot_evidence_to_axis,
    build_hotspot_daily_breakdown,
    build_hotspot_evidence,
    build_hotspot_timeline,
    select_best_business_axis_for_hotspots,
    summarize_hotspot_counts,
)


def test_hotspot_metrics_bundle_keeps_timeline_counts_coherent() -> None:
    nps_focus = pd.DataFrame(
        {
            "ID": ["n1", "n2", "n3"],
            "Fecha": pd.to_datetime(["2026-02-02", "2026-02-05", "2026-02-07"]),
            "Palanca": ["Pagos", "Pagos", "Acceso"],
            "Subpalanca": ["Transferencias", "SPEI", "Token"],
            "Comment": [
                "Las transferencias SPEI no funcionan",
                "Transferencias rechazadas en app",
                "El token falla al autenticar",
            ],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC1", "INC2", "INC3"],
            "Fecha": pd.to_datetime(["2026-02-02", "2026-02-05", "2026-02-07"]),
            "Detailed Description": [
                "Falla en transferencias SPEI en empresas",
                "Error de transferencias y SPEI intermitente",
                "Incidencia de token de autenticacion",
            ],
            "Product Categorization Tier 1": ["Pagos", "Pagos", "Acceso"],
            "Product Categorization Tier 2": ["Transferencias", "SPEI", "Autenticacion"],
            "Product Categorization Tier 3": ["SPEI", "Transferencias", "Token"],
            "service": ["Empresas", "Empresas", "Empresas"],
        }
    )
    links = pd.DataFrame(
        {
            "incident_id": ["INC1", "INC2", "INC3"],
            "nps_id": ["n1", "n2", "n3"],
            "nps_topic": ["Pagos > Transferencias", "Pagos > SPEI", "Acceso > Token"],
            "similarity": [0.91, 0.88, 0.73],
        }
    )

    evidence = build_hotspot_evidence(
        links,
        nps_focus,
        helix,
        system_date=date(2026, 2, 15),
        max_hotspots=3,
    )
    timeline = build_hotspot_timeline(
        links,
        nps_focus,
        helix,
        incident_evidence_df=evidence,
        max_hotspots=3,
    )
    summary = summarize_hotspot_counts(evidence, timeline, max_hotspots=3)

    assert not evidence.empty
    assert not timeline.empty
    assert not summary.empty

    transfer_row = summary[summary["hot_term"].astype(str).str.contains("transfer", na=False)].head(
        1
    )
    assert not transfer_row.empty

    row = transfer_row.iloc[0]
    assert int(row["hotspot_incidents"]) >= 2
    assert int(row["chart_helix_records"]) >= 2
    assert int(row["chart_nps_comments"]) >= 2

    hot_rows = timeline[
        (timeline["incident_id"].astype(str) == "") & (timeline["hot_term"] == row["hot_term"])
    ]
    assert not hot_rows.empty
    assert int(hot_rows["helix_records"].sum()) == int(row["chart_helix_records"])
    assert int(hot_rows["nps_comments"].sum()) == int(row["chart_nps_comments"])


def test_build_hotspot_daily_breakdown_uses_rank_map_and_timeline_rows() -> None:
    daily = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-02-02", "2026-02-05"]),
            "incidents": [1, 3],
        }
    )
    evidence = pd.DataFrame(
        {
            "incident_id": ["INC1", "INC2", "INC3"],
            "incident_date": pd.to_datetime(["2026-02-02", "2026-02-05", "2026-02-05"]),
            "hot_rank": [1, 1, 2],
            "hot_term": ["transferencias", "transferencias", "token"],
            "similarity": [0.9, 0.8, 0.7],
        }
    )
    timeline = pd.DataFrame(
        {
            "incident_id": ["INC1", "INC2", "INC3", "", ""],
            "hot_term": ["", "", "", "transferencias", "token"],
            "date": pd.to_datetime(
                ["2026-02-02", "2026-02-05", "2026-02-05", "2026-02-05", "2026-02-05"]
            ),
            "helix_records": [1, 2, 1, 1, 1],
            "nps_comments": [0, 1, 1, 1, 1],
        }
    )

    out, term_by_rank = build_hotspot_daily_breakdown(
        daily,
        evidence,
        timeline,
        max_hotspots=3,
    )

    assert term_by_rank[1] == "transferencias"
    assert term_by_rank[2] == "token"

    day2 = out[out["date"] == pd.Timestamp("2026-02-02")].iloc[0]
    day5 = out[out["date"] == pd.Timestamp("2026-02-05")].iloc[0]

    assert float(day2["hotspot_1"]) == 1.0
    assert float(day2["hotspot_2"]) == 0.0
    assert float(day2["no_hotspot"]) == 0.0

    assert float(day5["hotspot_1"]) == 2.0
    assert float(day5["hotspot_2"]) == 1.0
    assert float(day5["no_hotspot"]) == 0.0


def test_build_hotspot_daily_breakdown_uses_full_timeline_scope_not_only_representatives() -> None:
    daily = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-02-10", "2026-02-11"]),
            "incidents": [3, 2],
        }
    )
    # Evidence keeps only representative incidents for each hotspot.
    evidence = pd.DataFrame(
        {
            "incident_id": ["INC1", "INC4"],
            "hot_rank": [1, 2],
            "hot_term": ["transferencias", "token"],
            "similarity": [0.9, 0.8],
        }
    )
    # Timeline carries all incidents by day and hotspot-level incident_ids for full assignment.
    timeline = pd.DataFrame(
        {
            "incident_id": ["INC1", "INC2", "INC3", "INC4", "INC5", "", "", ""],
            "hot_term": ["", "", "", "", "", "transferencias", "transferencias", "token"],
            "date": pd.to_datetime(
                [
                    "2026-02-10",
                    "2026-02-10",
                    "2026-02-10",
                    "2026-02-11",
                    "2026-02-11",
                    "2026-02-10",
                    "2026-02-11",
                    "2026-02-11",
                ]
            ),
            "helix_records": [1, 1, 1, 1, 1, 3, 1, 1],
            "incident_ids": [
                "",
                "",
                "",
                "",
                "",
                "INC1 | INC2 | INC3",
                "INC5",
                "INC4",
            ],
        }
    )

    out, term_by_rank = build_hotspot_daily_breakdown(
        daily,
        evidence,
        timeline,
        max_hotspots=3,
    )

    assert term_by_rank[1] == "transferencias"
    assert term_by_rank[2] == "token"

    day10 = out[out["date"] == pd.Timestamp("2026-02-10")].iloc[0]
    day11 = out[out["date"] == pd.Timestamp("2026-02-11")].iloc[0]

    assert float(day10["hotspot_1"]) == 3.0
    assert float(day10["hotspot_2"]) == 0.0
    assert float(day10["no_hotspot"]) == 0.0

    assert float(day11["hotspot_1"]) == 1.0
    assert float(day11["hotspot_2"]) == 1.0
    assert float(day11["no_hotspot"]) == 0.0


def test_summarize_hotspot_counts_keeps_evidence_and_chart_totals_separate() -> None:
    evidence = pd.DataFrame(
        {
            "hot_rank": [1, 1],
            "hot_term": ["transferencias", "transferencias"],
            "incident_id": ["INC1", "INC2"],
            "hotspot_incidents": [60, 60],
            "hotspot_comments": [114, 114],
            "hotspot_links": [90, 90],
            "detractor_comment": ["c1", "c2"],
        }
    )
    timeline = pd.DataFrame(
        {
            "incident_id": ["", ""],
            "hot_term": ["transferencias", "transferencias"],
            "date": pd.to_datetime(["2026-02-10", "2026-02-11"]),
            "helix_records": [1, 2],
            "nps_comments": [3, 4],
        }
    )

    out = summarize_hotspot_counts(evidence, timeline, max_hotspots=3)
    assert not out.empty
    row = out.iloc[0]
    assert int(row["hotspot_incidents"]) == 60
    assert int(row["hotspot_comments"]) == 114
    assert int(row["chart_helix_records"]) == 3
    assert int(row["chart_nps_comments"]) == 7
    assert int(row["days_with_evidence"]) == 2


def test_select_best_business_axis_for_hotspots_prefers_axis_with_higher_helix_coverage() -> None:
    nps_df = pd.DataFrame(
        {
            "NPS": [2, 3, 4, 8, 9, 2, 4, 5],
            "Palanca": [
                "Pagos/ Transferencias",
                "Pagos/ Transferencias",
                "Seguridad",
                "Uso",
                "Uso",
                "Pagos/ Transferencias",
                "Seguridad",
                "Banca",
            ],
            "Subpalanca": [
                "Problemas con transferencias",
                "Límite de transferencias",
                "Robo en la cuenta",
                "Diseño UX",
                "Diseño UX",
                "Problemas con transferencias",
                "Robo en la cuenta",
                "Atención personalizada",
            ],
        }
    )
    helix_df = pd.DataFrame(
        {
            "Incident Number": [f"INC{i}" for i in range(1, 8)],
            "Fecha": pd.to_datetime(
                [
                    "2026-02-01",
                    "2026-02-02",
                    "2026-02-03",
                    "2026-02-04",
                    "2026-02-05",
                    "2026-02-06",
                    "2026-02-07",
                ]
            ),
            "Detailed Description": [
                "Falla en pagos y transferencias en banca empresas",
                "Error en transferencias SPEI",
                "Rechazo de pagos intermitente",
                "Problema de seguridad en token",
                "Transferencias con error",
                "Intermitencia general",
                "Pagos no procesados",
            ],
        }
    )
    out = select_best_business_axis_for_hotspots(
        nps_df,
        helix_df,
        min_n=1,
        min_token_ratio=0.6,
    )
    assert out["best_axis"] in {"Palanca", "Subpalanca"}
    ratios = out["axis_ratios"]
    assert float(ratios["Palanca"]) >= float(ratios["Subpalanca"])


def test_align_hotspot_evidence_to_axis_prioritizes_aligned_terms() -> None:
    evidence = pd.DataFrame(
        {
            "incident_id": ["INC1", "INC2", "INC3"],
            "incident_date": pd.to_datetime(["2026-02-01", "2026-02-02", "2026-02-03"]),
            "nps_topic": [
                "Pagos/ Transferencias > Problemas con transferencias",
                "Seguridad > Robo en la cuenta",
                "Pagos/ Transferencias > Límite de transferencias",
            ],
            "incident_summary": ["s1", "s2", "s3"],
            "detractor_comment": ["c1", "c2", "c3"],
            "similarity": [0.7, 0.6, 0.8],
            "hot_term": ["pagos", "seguridad", "transferencias"],
            "hot_rank": [2, 1, 3],
            "mention_incidents": [10, 4, 8],
            "mention_comments": [11, 5, 9],
            "hotspot_incidents": [2, 1, 2],
            "hotspot_comments": [2, 1, 2],
            "hotspot_links": [2, 1, 2],
        }
    )

    out = align_hotspot_evidence_to_axis(
        evidence,
        axis="Palanca",
        red_labels=["Pagos/ Transferencias"],
        max_hotspots=2,
    )

    assert not out.empty
    kept_terms = set(out["hot_term"].astype(str).tolist())
    assert "seguridad" not in kept_terms
    assert {"pagos", "transferencias"}.issubset(kept_terms)
    assert set(out["hot_rank"].astype(int).tolist()).issubset({1, 2})
