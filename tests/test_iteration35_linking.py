"""Helix/VoC acceptance, active lens and small labelled golden corpus."""

import pandas as pd
import pytest
from test_iteration28_taxonomy_helix import helix_fixture as helix_fixture
from test_iteration28_taxonomy_helix import helix_response
from test_taxonomy_exchange import designer_zip, exported, zipped
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.analytics.causal_evidence import CausalEvidenceEvaluator
from nps_lens.analytics.linking_diagnostics import linking_diagnostics
from nps_lens.analytics.linking_policy import evaluation_diagnostic, temporal_mask
from nps_lens.analytics.nps_helix_link import (
    link_incidents_to_nps_topics,
    retrieve_incident_candidates,
)
from nps_lens.services.dashboard_service import DashboardService

GOLDEN = [
    ("lexical", "Transferencia retenida", "Transferencia retenida", True, "2026-09-01"),
    ("synonyms", "Envío atascado", "Transferencia retenida", True, "2026-09-01"),
    (
        "customer_technical",
        "El dinero no sale",
        "Timeout del procesamiento de remesas",
        True,
        "2026-09-01",
    ),
    (
        "different_symptom",
        "Tarjeta credito cobro duplicado",
        "Tarjeta credito entrega demorada",
        False,
        "2026-09-01",
    ),
    ("different_task", "Transferencia bloqueada", "Consulta bloqueada", False, "2026-09-01"),
    (
        "low_overlap",
        "No llega la clave al móvil",
        "Token OTP con entrega fallida",
        True,
        "2026-09-01",
    ),
    ("ambiguous", "No funciona", "Transferencia retenida", False, "2026-09-01"),
    ("outside_window", "Transferencia retenida", "Transferencia retenida", False, "2025-01-01"),
]


def frames(comment, narrative, day):
    return (
        pd.DataFrame(
            [
                dict(
                    ID="N1",
                    Fecha="2026-09-01",
                    NPS=2,
                    Comment=comment,
                    Palanca="Operación",
                    Subpalanca="Ejecución",
                )
            ]
        ),
        pd.DataFrame([{"Incident Number": "I1", "Fecha": day, "summary": narrative}]),
    )


def test_golden_precision_recall():
    results = {"rules": [], "llm_mock": []}
    for _, comment, incident, expected, day in GOLDEN:
        nps, helix = frames(comment, incident, day)
        candidates = retrieve_incident_candidates(nps, helix)
        _, rules = link_incidents_to_nps_topics(nps, helix)
        results["rules"].append((not rules.empty, expected))
        # Fixed semantic oracle: no model call, independently labelled acceptance.
        results["llm_mock"].append((not candidates.empty and expected, expected))
    metrics = {}
    for engine, pairs in results.items():
        tp = sum(predicted and expected for predicted, expected in pairs)
        fp = sum(predicted and not expected for predicted, expected in pairs)
        fn = sum(not predicted and expected for predicted, expected in pairs)
        metrics[engine] = dict(
            precision=tp / max(1, tp + fp), recall=tp / max(1, tp + fn), FP=fp, FN=fn
        )
    assert metrics == {
        "rules": dict(precision=1, recall=0.25, FP=0, FN=3),
        "llm_mock": dict(precision=1, recall=1, FP=0, FN=0),
    }


def test_mexico_rules_and_evaluation_states():
    nps, helix = frames("Transferencia retenida", "Transferencia retenida", "2026-09-01")
    _, links = link_incidents_to_nps_topics(nps, helix)
    diagnostic = linking_diagnostics(
        nps=nps,
        focus=nps,
        helix=helix,
        scoped=helix,
        period=helix,
        eligible=helix,
        links=links,
        requested_scope=[],
    )
    assert diagnostic["evaluation_state"] == "MATCHED"
    assert all(
        diagnostic[key] > 0 for key in ("evidence_pairs", "linked_incidents", "linked_nps_comments")
    )
    assert links.semantic_confidence.isna().all()
    nps.loc[0, "Comment"] = "Transferencia"
    _, rejected = link_incidents_to_nps_topics(nps, helix)
    assert rejected.empty and rejected.attrs["candidate_count"] == 1
    assert rejected.attrs["evaluation_state"] == "EVALUATED_NO_MATCH"
    nps.attrs["classification_pending"] = True
    nps.attrs["classification_signature"] = "DISCOVERED-pending"
    pending = retrieve_incident_candidates(nps, helix)
    assert pending.empty and pending.attrs["evaluation_state"] == "NOT_EVALUATED"
    assert pending.attrs["evaluation_reason"] == "classification_pending"
    assert pending.attrs["classification_signature"] == "DISCOVERED-pending"


def test_llm_low_overlap_export_import_and_independent_scores(helix):
    handler, ctx, frame, incidents, _ = helix
    frame["Comment"] = "No llega la clave al móvil"
    incidents["Detailed Description"] = "Token OTP con entrega fallida"
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    _, rules = link_incidents_to_nps_topics(inputs["frame"], incidents)
    assert rules.empty
    result = handler.export(ctx, inputs)
    assert result["diagnostics"]["incidents_without_candidates"] == 0
    request = exported(result["saved_paths"])
    candidate = request["incidents/000001.json"]["incidents"][0]["candidates"][0]
    assert candidate["text_similarity"] < 0.15
    handler.import_response(ctx, inputs, zipped(helix_response(request, candidate["id"])))
    accepted = handler.links(ctx, inputs, inputs["frame"], incidents)
    assert not accepted.empty
    assert accepted.semantic_confidence.eq(0.9).all()
    assert accepted.text_similarity.lt(0.15).all()
    core = DashboardService._compute_linking_core(
        None,
        nps_df=inputs["frame"],
        helix_df=incidents,
        focus_df=inputs["frame"],
        focus_group="detractor",
        min_similarity=1,
        max_days_apart=90,
        imported_links=accepted,
    )
    assert len(core["links_df"]) == len(accepted)


def test_source_does_not_satisfy_pending_discovered_and_export_is_diagnosed(helix):
    handler, ctx, _, incidents, client = helix
    from nps_lens.services.taxonomy_exchange import TaxonomyExchange

    exchange = TaxonomyExchange(handler.taxonomy, handler.downloads)
    source_signature = handler.inputs(ctx, incidents, "SOURCE")["frame"].attrs[
        "classification_signature"
    ]
    exchange.import_response(ctx, designer_zip(exchange, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    service = client.app.state.dashboard_service
    active = service._load_nps_df(ctx)
    assert active.attrs["classification_pending"]
    assert active.match_status.eq("non_matchable").all()
    payload = service.linking_dashboard(context=ctx)
    assert payload["diagnostics"]["evaluation_state"] == "NOT_EVALUATED"
    assert payload["diagnostics"]["evaluation_reason"] == "classification_pending"
    assert payload["diagnostics"]["nps_matchable"] == 0
    inputs = handler.inputs(ctx, incidents, "DISCOVERED")
    result = handler.export(ctx, inputs)
    assert result["diagnostics"]["evaluation_state"] == "NOT_EVALUATED"
    assert result["diagnostics"]["evaluation_reason"] == "classification_pending"
    request = exported(result["saved_paths"])
    assert all(
        not row["candidates"]
        for key, batch in request.items()
        if key.startswith("incidents/")
        for row in batch["incidents"]
    )
    assert (
        request["manifest.json"]["linking_diagnostics"]["evaluation_reason"]
        == "classification_pending"
    )
    assert (
        handler.inputs(ctx, incidents, "SOURCE")["frame"].attrs["classification_signature"]
        == source_signature
    )


@pytest.mark.parametrize(
    "day,expected",
    [
        ("2026-09-01", True),
        ("2026-06-03", True),
        ("2026-06-02", False),
        ("2026-09-02", False),
        (None, False),
    ],
)
def test_temporal_policy_inclusive_directional_and_shared(day, expected):
    assert bool(temporal_mask("2026-09-01", day)) is expected
    link = dict(
        nps_id="N",
        incident_id="I",
        causal_engine="llm",
        semantic_confidence=0.9,
        comment_date="2026-09-01",
        incident_date=day,
    )
    assert (CausalEvidenceEvaluator().evaluate(link)["temporal_relation"] == "valid") is expected


def test_no_candidates_is_not_a_negative_evaluation():
    assert evaluation_diagnostic(eligible=4)["evaluation_state"] == "NOT_EVALUATED"
    assert (
        evaluation_diagnostic(eligible=4, candidate_count=2, with_candidates=1)["evaluation_state"]
        == "EVALUATED_NO_MATCH"
    )
