from __future__ import annotations

import pandas as pd

from nps_lens.analytics.incident_attribution import (
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2,
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    _catalog_keywords,
    _catalog_slug,
    _dedupe_executive_journey_catalog,
    _default_executive_journey_catalog,
    _normalize_executive_journey_entry,
    build_broken_journey_catalog,
    build_broken_journey_topic_map,
    build_incident_attribution_chains,
    executive_journey_catalog_df,
    executive_journey_catalog_path,
    load_executive_journey_catalog,
    remap_links_to_journeys,
    remap_topic_timeseries_to_journeys,
    save_executive_journey_catalog,
    summarize_attribution_chains,
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
            "NPS Group": [
                "DETRACTOR",
                "DETRACTOR",
                "DETRACTOR",
                "DETRACTOR",
                "DETRACTOR",
                "DETRACTOR",
            ],
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
            "_text_norm": [
                "no puedo entrar a la aplicacion de empresas",
                "nada mas entro y la web me saca",
                "no permite acceder con mis credenciales",
                "se desloguea apenas inicia",
                "la sesion se cae al entrar",
                "sin comentarios",
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
    assert len(out.iloc[0]["comment_records"]) == 2
    assert out.iloc[0]["incident_records"][0]["incident_id"] == "INC00001"
    assert "problema" in out.iloc[0]["incident_examples"][0].lower()
    assert "No puedo entrar" in out.iloc[0]["comment_examples"][0]
    assert out.iloc[0]["comment_records"][0] == {
        "comment_id": "n1",
        "date": "01-02-2026",
        "nps": "1",
        "group": "DETRACTOR",
        "palanca": "Acceso",
        "subpalanca": "Login",
        "comment": "no puedo entrar a la aplicacion de empresas",
    }
    assert "5 incidencias Helix" in out.iloc[0]["chain_story"]
    assert "2 comentarios VoC" in out.iloc[0]["chain_story"]
    assert out.iloc[0]["action_lane"] == "Fix estructural"
    assert out.iloc[0]["owner_role"] == "Producto + Tecnologia"


def test_build_incident_attribution_chains_can_use_bbva_source_service_n2_as_touchpoint() -> None:
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
            "BBVA_SourceServiceN2": ["NET CASH"],
        }
    )

    out = build_incident_attribution_chains(
        links_df,
        nps_focus,
        helix,
        top_k=3,
        touchpoint_source=TOUCHPOINT_SOURCE_BBVA_SOURCE_N2,
    )

    assert len(out) == 1
    assert out.iloc[0]["touchpoint"] == "NET CASH"


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


def test_executive_journey_catalog_can_be_saved_and_reloaded(tmp_path) -> None:
    save_executive_journey_catalog(
        tmp_path,
        service_origin="BBVA México",
        service_origin_n1="Empresas",
        rows=[
            {
                "id": "",
                "title": "Firma bloqueada",
                "what_occurs": "No se puede firmar una operación",
                "expected_evidence": "Comentarios de firma + incidencias de firma",
                "impact_label": "Alto",
                "touchpoint": "Firma",
                "palanca": "Operativa",
                "subpalanca": "Firma",
                "route": "Operativa -> firma -> error -> detracción",
                "cx_readout": "Bloquea la operativa crítica.",
                "confidence_label": "Alto",
                "keywords": "firma, token, bloqueo",
            }
        ],
    )

    loaded = load_executive_journey_catalog(
        tmp_path,
        service_origin="BBVA México",
        service_origin_n1="Empresas",
    )

    assert len(loaded) == 1
    assert loaded[0]["title"] == "Firma bloqueada"
    assert loaded[0]["id"] == "executive-firma-bloqueada"
    assert loaded[0]["keywords"] == ("firma", "token", "bloqueo")


def test_executive_journey_catalog_helpers_normalize_and_dedupe() -> None:
    defaults = _default_executive_journey_catalog()
    assert defaults
    assert _catalog_slug("BBVA México / Empresas") == "bbva-mexico-empresas"
    assert _catalog_keywords("firma, token,\nFirma ; bloqueo") == (
        "firma",
        "token",
        "bloqueo",
    )

    normalized = _normalize_executive_journey_entry(
        {
            "id": "",
            "title": "  Journey de  Firma ",
            "touchpoint": " Firma ",
            "palanca": " Operativa ",
            "subpalanca": " Firma ",
            "keywords": "",
        },
        position=0,
    )
    assert normalized is not None
    assert normalized["id"] == "executive-journey-de-firma"
    assert normalized["keywords"] == ("journey de firma", "firma", "operativa")

    deduped = _dedupe_executive_journey_catalog(
        [
            {"id": "dup", "title": "A", "keywords": ("a",)},
            {"id": "dup", "title": "B", "keywords": ("b",)},
        ]
    )
    assert [row["id"] for row in deduped] == ["dup", "dup-02"]


def test_executive_journey_catalog_load_handles_missing_invalid_and_table_render(tmp_path) -> None:
    path = executive_journey_catalog_path(
        tmp_path,
        service_origin="BBVA México",
        service_origin_n1="Empresas",
    )
    missing = load_executive_journey_catalog(
        tmp_path,
        service_origin="BBVA México",
        service_origin_n1="Empresas",
    )
    assert missing

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not-json", encoding="utf-8")
    invalid = load_executive_journey_catalog(
        tmp_path,
        service_origin="BBVA México",
        service_origin_n1="Empresas",
    )
    assert invalid

    path.write_text('"not-a-list"', encoding="utf-8")
    invalid_shape = load_executive_journey_catalog(
        tmp_path,
        service_origin="BBVA México",
        service_origin_n1="Empresas",
    )
    assert invalid_shape

    save_executive_journey_catalog(
        tmp_path,
        service_origin="BBVA México",
        service_origin_n1="Empresas",
        rows=[
            {},
            {
                "id": "dup",
                "title": "Acceso roto",
                "what_occurs": "No se puede acceder",
                "expected_evidence": "Login",
                "impact_label": "Alto",
                "touchpoint": "Login",
                "palanca": "Acceso",
                "subpalanca": "Login",
                "route": "Acceso -> login -> error",
                "cx_readout": "Bloquea acceso",
                "confidence_label": "Alto",
                "keywords": ["login", "acceso"],
            },
            {
                "id": "dup",
                "title": "Acceso roto 2",
                "what_occurs": "No se puede acceder",
                "expected_evidence": "OTP",
                "impact_label": "Medio",
                "touchpoint": "Login",
                "palanca": "Acceso",
                "subpalanca": "OTP",
                "route": "Acceso -> otp -> error",
                "cx_readout": "Bloquea acceso",
                "confidence_label": "Medio",
                "keywords": "otp, acceso",
            },
        ],
    )
    loaded = load_executive_journey_catalog(
        tmp_path,
        service_origin="BBVA México",
        service_origin_n1="Empresas",
    )
    table = executive_journey_catalog_df(loaded)

    assert len(loaded) == 2
    assert [row["id"] for row in loaded] == ["dup", "dup-02"]
    assert list(table["keywords"]) == ["login, acceso", "otp, acceso"]


def test_build_incident_attribution_chains_can_use_persisted_executive_catalog() -> None:
    links_df = pd.DataFrame(
        {
            "nps_id": ["n1"],
            "incident_id": ["INC00001"],
            "similarity": [0.91],
            "nps_topic": ["Operativa > Firma"],
        }
    )
    nps_focus = pd.DataFrame(
        {
            "ID": ["n1"],
            "Fecha": pd.to_datetime(["2026-02-01"]),
            "NPS": [1],
            "Palanca": ["Operativa"],
            "Subpalanca": ["Firma"],
            "Comment": ["La firma falla con el token y no me deja operar"],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC00001"],
            "Fecha": pd.to_datetime(["2026-02-01"]),
            "Detailed Description": ["Error de firma con token en operativa empresas"],
            "Product Categorization Tier 1": ["Operativa"],
            "Product Categorization Tier 2": ["Firma"],
            "Product Categorization Tier 3": ["Token"],
        }
    )

    out = build_incident_attribution_chains(
        links_df,
        nps_focus,
        helix,
        top_k=0,
        touchpoint_source=TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
        executive_journey_catalog=[
            {
                "id": "executive_signature_blocked",
                "title": "Firma bloqueada",
                "what_occurs": "No se puede firmar",
                "expected_evidence": "Comentarios de firma + incidencias de token",
                "impact_label": "Alto",
                "touchpoint": "Firma",
                "palanca": "Operativa",
                "subpalanca": "Firma",
                "route": "Operativa -> firma -> token -> detracción",
                "cx_readout": "Bloquea la operativa crítica.",
                "confidence_label": "Alto",
                "keywords": ["firma", "token", "operativa"],
            }
        ],
    )

    assert len(out) == 1
    assert out.iloc[0]["nps_topic"] == "Firma bloqueada"


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
    assert len(out.iloc[0]["comment_records"]) == 3


def test_summarize_attribution_chains_uses_same_chain_level_source_for_totals() -> None:
    attribution_df = pd.DataFrame(
        [
            {
                "nps_topic": "Pagos / Transferencias",
                "linked_incidents": 26,
                "linked_comments": 32,
                "linked_pairs": 46,
            },
            {
                "nps_topic": "Acceso / Login",
                "linked_incidents": 8,
                "linked_comments": 10,
                "linked_pairs": 14,
            },
            {
                "nps_topic": "Pagos / Transferencias",
                "linked_incidents": 4,
                "linked_comments": 6,
                "linked_pairs": 7,
            },
        ]
    )

    summary = summarize_attribution_chains(attribution_df)

    assert summary == {
        "chains_total": 3,
        "topics_total": 2,
        "linked_incidents_total": 38,
        "linked_comments_total": 48,
        "linked_pairs_total": 67,
    }


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


def test_broken_journey_catalog_groups_related_links_without_manual_table() -> None:
    links_df = pd.DataFrame(
        {
            "nps_id": ["n1", "n2", "n3", "n4"],
            "incident_id": ["INC00001", "INC00002", "INC00003", "INC00004"],
            "similarity": [0.92, 0.90, 0.89, 0.87],
            "nps_topic": [
                "Acceso > Login",
                "Acceso > Login",
                "Operativa > Pagos",
                "Operativa > Pagos",
            ],
        }
    )
    nps_focus = pd.DataFrame(
        {
            "ID": ["n1", "n2", "n3", "n4"],
            "Fecha": pd.to_datetime(["2026-02-01", "2026-02-02", "2026-02-03", "2026-02-04"]),
            "NPS": [1, 2, 2, 3],
            "NPS Group": ["DETRACTOR", "DETRACTOR", "DETRACTOR", "DETRACTOR"],
            "Palanca": ["Acceso", "Acceso", "Operativa", "Operativa"],
            "Subpalanca": ["Login", "Login", "Pagos", "Pagos"],
            "_text_norm": [
                "no puedo entrar el login falla y pide otp",
                "la autenticacion expulsa al usuario de empresas",
                "la transferencia no se completa por timeout",
                "error al firmar pagos y transferencias",
            ],
            "Comment": [
                "No puedo entrar",
                "Me expulsa al autenticar",
                "La transferencia no se completa",
                "Falla la firma de pagos",
            ],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC00001", "INC00002", "INC00003", "INC00004"],
            "Fecha": pd.to_datetime(["2026-02-01", "2026-02-02", "2026-02-03", "2026-02-04"]),
            "Detailed Description": [
                "Error de login y OTP en acceso digital",
                "Problema de autenticacion en portal empresas",
                "Timeout en pagos SPEI y transferencias",
                "Error de firma en pagos empresariales",
            ],
            "BBVA_SourceServiceN2": ["Auth", "Auth", "Pagos", "Pagos"],
            "Product Categorization Tier 1": ["Acceso", "Acceso", "Operativa", "Operativa"],
            "Product Categorization Tier 2": ["Login", "Login", "Pagos", "Pagos"],
            "Product Categorization Tier 3": [
                "Autenticación",
                "Autenticación",
                "Transferencias",
                "Firma",
            ],
        }
    )

    catalog, journey_links = build_broken_journey_catalog(links_df, nps_focus, helix)

    assert len(catalog) == 2
    assert set(catalog["touchpoint"].tolist()) == {"Login", "Pagos"}
    assert set(journey_links["journey_label"].astype(str).tolist()) == {
        "Acceso / Login",
        "Operativa / Pagos",
    }


def test_broken_journey_remap_reuses_detected_clusters_in_timeseries_and_chains() -> None:
    links_df = pd.DataFrame(
        {
            "nps_id": ["n1", "n2", "n3", "n4"],
            "incident_id": ["INC00001", "INC00002", "INC00003", "INC00004"],
            "similarity": [0.92, 0.90, 0.89, 0.87],
            "nps_topic": [
                "Acceso > Login",
                "Acceso > Login",
                "Operativa > Pagos",
                "Operativa > Pagos",
            ],
        }
    )
    nps_focus = pd.DataFrame(
        {
            "ID": ["n1", "n2", "n3", "n4"],
            "Fecha": pd.to_datetime(["2026-02-01", "2026-02-08", "2026-02-01", "2026-02-08"]),
            "NPS": [1, 2, 2, 3],
            "NPS Group": ["DETRACTOR", "DETRACTOR", "DETRACTOR", "DETRACTOR"],
            "Palanca": ["Acceso", "Acceso", "Operativa", "Operativa"],
            "Subpalanca": ["Login", "Login", "Pagos", "Pagos"],
            "_text_norm": [
                "no puedo entrar el login falla y pide otp",
                "la autenticacion expulsa al usuario de empresas",
                "la transferencia no se completa por timeout",
                "error al firmar pagos y transferencias",
            ],
            "Comment": [
                "No puedo entrar",
                "Me expulsa al autenticar",
                "La transferencia no se completa",
                "Falla la firma de pagos",
            ],
        }
    )
    helix = pd.DataFrame(
        {
            "Incident Number": ["INC00001", "INC00002", "INC00003", "INC00004"],
            "Fecha": pd.to_datetime(["2026-02-01", "2026-02-08", "2026-02-01", "2026-02-08"]),
            "Detailed Description": [
                "Error de login y OTP en acceso digital",
                "Problema de autenticacion en portal empresas",
                "Timeout en pagos SPEI y transferencias",
                "Error de firma en pagos empresariales",
            ],
            "BBVA_SourceServiceN2": ["Auth", "Auth", "Pagos", "Pagos"],
            "Product Categorization Tier 1": ["Acceso", "Acceso", "Operativa", "Operativa"],
            "Product Categorization Tier 2": ["Login", "Login", "Pagos", "Pagos"],
            "Product Categorization Tier 3": [
                "Autenticación",
                "Autenticación",
                "Transferencias",
                "Firma",
            ],
        }
    )
    weekly = pd.DataFrame(
        {
            "week": pd.to_datetime(["2026-01-26", "2026-02-02", "2026-01-26", "2026-02-02"]),
            "nps_topic": [
                "Acceso > Login",
                "Acceso > Login",
                "Operativa > Pagos",
                "Operativa > Pagos",
            ],
            "responses": [10, 12, 9, 11],
            "focus_count": [4, 5, 3, 4],
            "nps_mean": [3.0, 3.2, 4.1, 4.3],
            "focus_rate": [0.40, 0.42, 0.33, 0.36],
            "incidents": [2, 2, 1, 2],
        }
    )

    catalog, journey_links = build_broken_journey_catalog(links_df, nps_focus, helix)
    topic_map = build_broken_journey_topic_map(journey_links)
    links_mode = remap_links_to_journeys(links_df, journey_links)
    weekly_mode = remap_topic_timeseries_to_journeys(weekly, topic_map)
    rationale_df = pd.DataFrame(
        {
            "nps_topic": ["Acceso / Login", "Operativa / Pagos"],
            "priority": [0.82, 0.74],
            "confidence": [0.78, 0.70],
            "causal_score": [0.81, 0.71],
            "focus_probability_with_incident": [0.41, 0.34],
            "nps_delta_expected": [-4.1, -3.6],
            "total_nps_impact": [1.6, 1.1],
            "nps_points_at_risk": [1.6, 1.1],
            "nps_points_recoverable": [1.0, 0.7],
            "delta_focus_rate_pp": [22.0, 17.0],
            "incident_rate_per_100_responses": [8.0, 6.0],
            "incidents": [4, 3],
            "responses": [22, 20],
        }
    )

    chains = build_incident_attribution_chains(
        links_mode,
        nps_focus,
        helix,
        rationale_df=rationale_df,
        top_k=0,
        touchpoint_source=TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
        journey_catalog_df=catalog,
        journey_links_df=journey_links,
    )

    assert set(weekly_mode["nps_topic"].tolist()) == {"Acceso / Login", "Operativa / Pagos"}
    assert set(chains["presentation_mode"].tolist()) == {TOUCHPOINT_SOURCE_BROKEN_JOURNEYS}
    assert set(chains["nps_topic"].tolist()) == {"Acceso / Login", "Operativa / Pagos"}
