from __future__ import annotations

import pandas as pd

from nps_lens.analytics.nps_helix_link import build_incident_display_text, link_incidents_to_nps_topics


def test_link_incidents_to_nps_topics_sparse_chunked_path() -> None:
    nps = pd.DataFrame(
        {
            "ID": ["n1", "n2", "n3", "n4"],
            "Fecha": pd.to_datetime(["2026-01-10", "2026-01-11", "2026-01-12", "2026-01-13"]),
            "NPS": [2, 3, 4, 5],
            "Palanca": ["Pagos", "Pagos", "Acceso", "Acceso"],
            "Subpalanca": ["SPEI", "Transferencias", "Login", "Login"],
            "Comment": [
                "error en spei y transferencia rechazada",
                "spei no funciona hoy",
                "no puedo entrar al login",
                "login bloqueado",
            ],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC1", "INC2"],
            "Fecha": pd.to_datetime(["2026-01-12", "2026-01-13"]),
            "summary": ["falla de spei en transferencias", "incidente de login bloqueado"],
            "Product Categorization Tier 1": ["Pagos", "Acceso"],
            "Product Categorization Tier 2": ["SPEI", "Login"],
            "Product Categorization Tier 3": ["Transferencias", "Autenticacion"],
        }
    )

    assign_df, links_df = link_incidents_to_nps_topics(
        nps,
        helix,
        min_similarity=0.01,
        top_k_per_incident=2,
        max_nps_rows_for_evidence=3,
        evidence_chunk_size=1,
    )

    assert not assign_df.empty
    assert {"incident_id", "nps_topic", "similarity"}.issubset(assign_df.columns)
    assert not links_df.empty
    assert links_df["similarity"].min() >= 0.01


def test_link_incidents_to_nps_topics_handles_empty_text() -> None:
    nps = pd.DataFrame({"ID": ["n1"], "Palanca": [""], "Subpalanca": [""], "Comment": [""]})
    helix = pd.DataFrame({"Incident Number": ["INC1"], "summary": [""]})
    assign_df, links_df = link_incidents_to_nps_topics(nps, helix, min_similarity=0.2)
    assert assign_df.empty
    assert links_df.empty


def test_build_incident_display_text_prefers_detailed_description_variants() -> None:
    helix = pd.DataFrame(
        {
            "summary": ["Resumen básico 1", "Resumen básico 2", ""],
            "Short Description": ["Corta 1", "Corta 2", "Corta 3"],
            "Detailed Description": ["Detalle principal", "", ""],
            "Detailed Decription": ["", "Detalle typo", ""],
        }
    )
    out = build_incident_display_text(helix)
    assert out.tolist() == ["Detalle principal", "Detalle typo", "Corta 3"]


def test_link_incidents_to_nps_topics_uses_detailed_description_for_matching() -> None:
    nps = pd.DataFrame(
        {
            "ID": ["n1"],
            "Fecha": pd.to_datetime(["2026-01-10"]),
            "NPS": [2],
            "Palanca": ["Acceso"],
            "Subpalanca": ["Login"],
            "Comment": ["Falla biometria facial al entrar en la app empresas"],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC-BIO-1"],
            "Fecha": pd.to_datetime(["2026-01-12"]),
            "summary": ["Incidencia general de canal"],
            "Detailed Description": ["Error en biometria facial durante login de empresas"],
            "Product Categorization Tier 1": [""],
            "Product Categorization Tier 2": [""],
            "Product Categorization Tier 3": [""],
        }
    )

    assign_df, links_df = link_incidents_to_nps_topics(
        nps,
        helix,
        min_similarity=0.001,
        top_k_per_incident=1,
        max_nps_rows_for_evidence=1,
        evidence_chunk_size=1,
    )

    assert not assign_df.empty
    assert (assign_df["incident_id"] == "INC-BIO-1").any()
    assert not links_df.empty
    assert (links_df["incident_id"] == "INC-BIO-1").any()
