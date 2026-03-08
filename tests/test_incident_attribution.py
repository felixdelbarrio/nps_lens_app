from __future__ import annotations

import pandas as pd

from nps_lens.analytics.incident_attribution import (
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    TOUCHPOINT_SOURCE_HELIX_N2,
    build_incident_attribution_chains,
)


def test_build_incident_attribution_chains_keeps_only_presentable_linked_topics() -> None:
    links_df = pd.DataFrame(
        {
            "nps_id": ["n1", "n1b", "n1c", "n2", "n2b", "n_bad"],
            "incident_id": ["INC00001", "INC00003", "INC00025", "INC00040", "INC00041", "INC00099"],
            "similarity": [0.88, 0.86, 0.84, 0.81, 0.79, 0.91],
            "nps_topic": [
                "Acceso > Login",
                "Acceso > Login",
                "Acceso > Login",
                "Acceso > Login",
                "Acceso > Login",
                "Sin Comentarios > Sin Comentarios",
            ],
            "incident_topic": [
                "Digital > Login > Acceso",
                "Digital > Login > Acceso",
                "Digital > Login > Acceso",
                "Digital > Login > Acceso",
                "Digital > Login > Acceso",
                "General",
            ],
        }
    )
    nps_focus = pd.DataFrame(
        {
            "ID": ["n1", "n1b", "n1c", "n2", "n2b", "n_bad"],
            "Fecha": pd.to_datetime(
                ["2026-02-01", "2026-02-01", "2026-02-01", "2026-02-02", "2026-02-02", "2026-02-02"]
            ),
            "NPS": [1, 1, 2, 2, 3, 2],
            "Palanca": ["Acceso", "Acceso", "Acceso", "Acceso", "Acceso", "Sin Comentarios"],
            "Subpalanca": ["Login", "Login", "Login", "Login", "Login", "Sin Comentarios"],
            "Comment": [
                "No puedo entrar a la aplicación de empresas",
                "Nada más entro y la web me saca",
                "No permite acceder con mis credenciales",
                "Se desloguea apenas inicia",
                "La sesión se cae al entrar",
                "Sin comentarios",
            ],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": [
                "INC00001",
                "INC00003",
                "INC00025",
                "INC00040",
                "INC00041",
                "INC00099",
            ],
            "Fecha": pd.to_datetime(
                ["2026-02-01", "2026-02-01", "2026-02-01", "2026-02-02", "2026-02-02", "2026-02-02"]
            ),
            "Detailed Description": [
                "Problema en el login: no puedo acceder y se desloguea al entrar",
                "No puedo acceder al login corporativo",
                "Nada mas entras se desloguea",
                "Error al autenticar usuario en acceso web",
                "Falla de sesión al entrar en portal empresas",
                "Incidencia genérica sin detalle",
            ],
            "Product Categorization Tier 1": [
                "Acceso",
                "Acceso",
                "Acceso",
                "Acceso",
                "Acceso",
                "General",
            ],
            "Product Categorization Tier 2": ["Login", "Login", "Login", "Login", "Login", ""],
            "Product Categorization Tier 3": [
                "Autenticación",
                "Autenticación",
                "Autenticación",
                "Autenticación",
                "Autenticación",
                "",
            ],
        }
    )
    rationale_df = pd.DataFrame(
        [
            {
                "nps_topic": "Acceso > Login",
                "touchpoint": "Login",
                "priority": 0.91,
                "confidence": 0.82,
                "causal_score": 0.86,
                "focus_probability_with_incident": 0.47,
                "nps_delta_expected": -4.3,
                "total_nps_impact": 1.7,
                "nps_points_at_risk": 1.7,
                "nps_points_recoverable": 1.1,
                "delta_focus_rate_pp": 29.0,
                "incident_rate_per_100_responses": 8.5,
                "incidents": 5,
                "responses": 120,
                "action_lane": "Fix estructural",
                "owner_role": "Producto + Tecnologia",
                "eta_weeks": 6,
            }
        ]
    )

    out = build_incident_attribution_chains(
        links_df,
        nps_focus,
        helix,
        rationale_df=rationale_df,
        top_k=3,
    )

    assert len(out) == 1
    assert out.iloc[0]["nps_topic"] == "Acceso > Login"
    assert out.iloc[0]["touchpoint"] == "Login"
    assert out.iloc[0]["linked_pairs"] == 5
    assert out.iloc[0]["linked_incidents"] == 5
    assert out.iloc[0]["linked_comments"] == 5
    assert len(out.iloc[0]["incident_records"]) == 5
    assert len(out.iloc[0]["incident_examples"]) == 5
    assert len(out.iloc[0]["comment_examples"]) == 2
    assert out.iloc[0]["incident_records"][0]["incident_id"] == "INC00001"
    assert "problema" in out.iloc[0]["incident_examples"][0].lower()
    assert "No puedo entrar" in out.iloc[0]["comment_examples"][0]
    assert "5 incidencias Helix" in out.iloc[0]["chain_story"]
    assert "2 comentarios VoC" in out.iloc[0]["chain_story"]
    assert out.iloc[0]["action_lane"] == "Fix estructural"
    assert out.iloc[0]["owner_role"] == "Producto + Tecnologia"


def test_build_incident_attribution_chains_can_use_assigned_helix_n2_as_touchpoint() -> None:
    links_df = pd.DataFrame(
        {
            "nps_id": ["n1"],
            "incident_id": ["INC00001"],
            "similarity": [0.88],
            "nps_topic": ["Operativa > Creditos"],
        }
    )
    nps_focus = pd.DataFrame(
        {
            "ID": ["n1"],
            "Fecha": pd.to_datetime(["2026-02-01"]),
            "NPS": [1],
            "Palanca": ["Operativa"],
            "Subpalanca": [""],
            "Comment": ["No puedo completar la firma del credito"],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC00001"],
            "Fecha": pd.to_datetime(["2026-02-01"]),
            "Detailed Description": ["Error en la firma digital del credito"],
            "Product Categorization Tier 1": ["Operativa"],
            "Product Categorization Tier 2": ["Firma digital"],
            "Product Categorization Tier 3": ["Creditos"],
        }
    )

    out = build_incident_attribution_chains(
        links_df,
        nps_focus,
        helix,
        top_k=3,
        touchpoint_source=TOUCHPOINT_SOURCE_HELIX_N2,
    )

    assert len(out) == 1
    assert out.iloc[0]["touchpoint"] == "Firma digital"


def test_build_incident_attribution_chains_can_aggregate_to_executive_journeys() -> None:
    links_df = pd.DataFrame(
        {
            "nps_id": ["n1", "n2"],
            "incident_id": ["INC00001", "INC00002"],
            "similarity": [0.91, 0.83],
            "nps_topic": ["Acceso > Login", "Operativa > Pagos"],
        }
    )
    nps_focus = pd.DataFrame(
        {
            "ID": ["n1", "n2"],
            "Fecha": pd.to_datetime(["2026-02-01", "2026-02-02"]),
            "NPS": [1, 2],
            "Palanca": ["Acceso", "Operativa"],
            "Subpalanca": ["Login", "Pagos"],
            "Comment": [
                "No puedo entrar en la aplicación",
                "La operación no se completa y da timeout",
            ],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC00001", "INC00002"],
            "Fecha": pd.to_datetime(["2026-02-01", "2026-02-02"]),
            "Detailed Description": [
                "Fallo de autenticación y OTP al iniciar sesión",
                "Error en pagos y transferencias por timeout",
            ],
            "Product Categorization Tier 1": ["Acceso", "Operativa"],
            "Product Categorization Tier 2": ["Login", "Pagos"],
            "Product Categorization Tier 3": ["Autenticación", "Transacciones"],
        }
    )

    out = build_incident_attribution_chains(
        links_df,
        nps_focus,
        helix,
        top_k=0,
        touchpoint_source=TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    )

    assert len(out) == 2
    assert set(out["nps_topic"].tolist()) == {"Acceso bloqueado", "Operativa crítica fallida"}
    assert set(out["presentation_mode"].tolist()) == {TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS}


def test_build_incident_attribution_chains_can_return_all_examples_when_limit_is_zero() -> None:
    links_df = pd.DataFrame(
        {
            "nps_id": ["n1", "n2", "n3"],
            "incident_id": ["INC00001", "INC00002", "INC00003"],
            "similarity": [0.91, 0.84, 0.82],
            "nps_topic": ["Acceso > Login", "Acceso > Login", "Acceso > Login"],
        }
    )
    nps_focus = pd.DataFrame(
        {
            "ID": ["n1", "n2", "n3"],
            "Fecha": pd.to_datetime(["2026-02-01", "2026-02-01", "2026-02-02"]),
            "NPS": [1, 2, 3],
            "Palanca": ["Acceso", "Acceso", "Acceso"],
            "Subpalanca": ["Login", "Login", "Login"],
            "Comment": [
                "No puedo entrar",
                "Se desloguea al iniciar",
                "Me falla el OTP",
            ],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC00001", "INC00002", "INC00003"],
            "Fecha": pd.to_datetime(["2026-02-01", "2026-02-01", "2026-02-02"]),
            "Detailed Description": [
                "Problema en el login",
                "No puedo acceder al portal",
                "Falla el OTP de acceso",
            ],
            "Product Categorization Tier 1": ["Acceso", "Acceso", "Acceso"],
            "Product Categorization Tier 2": ["Login", "Login", "Login"],
            "Product Categorization Tier 3": ["Autenticación", "Autenticación", "Autenticación"],
        }
    )

    out = build_incident_attribution_chains(
        links_df,
        nps_focus,
        helix,
        top_k=0,
        max_incident_examples=0,
        max_comment_examples=0,
    )

    assert len(out) == 1
    assert len(out.iloc[0]["incident_examples"]) == 3
    assert len(out.iloc[0]["comment_examples"]) == 3


def test_build_incident_attribution_chains_filters_compound_generic_topics() -> None:
    links_df = pd.DataFrame(
        {
            "nps_id": ["n1"],
            "incident_id": ["INC00001"],
            "similarity": [0.88],
            "nps_topic": ["Sin Comentarios > Sin Comentarios"],
        }
    )
    nps_focus = pd.DataFrame(
        {
            "ID": ["n1"],
            "Fecha": pd.to_datetime(["2026-02-01"]),
            "NPS": [2],
            "Palanca": ["Sin Comentarios"],
            "Subpalanca": ["Sin Comentarios"],
            "Comment": ["La app no funciona"],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC00001"],
            "Fecha": pd.to_datetime(["2026-02-01"]),
            "Detailed Description": ["Incidencia genérica"],
            "Product Categorization Tier 1": ["General"],
            "Product Categorization Tier 2": [""],
            "Product Categorization Tier 3": [""],
        }
    )

    out = build_incident_attribution_chains(
        links_df,
        nps_focus,
        helix,
        top_k=0,
    )

    assert out.empty
