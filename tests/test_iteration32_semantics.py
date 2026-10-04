"""Deterministic semantic-contract regressions; no LLM calls."""

from __future__ import annotations

import copy

import pandas as pd
import pytest
from test_iteration28_taxonomy_helix import helix_fixture as helix_fixture
from test_iteration28_taxonomy_helix import helix_response
from test_taxonomy_exchange import (
    TAXONOMY,
    classifier_files,
    classifier_zip,
    designer_zip,
    exported,
    reviewed_taxonomy,
    zipped,
)
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.analytics.linking_policy import LINK_MAX_DAYS_APART, LINK_TOP_K_PER_INCIDENT
from nps_lens.analytics.nps_helix_link import (
    build_incident_text,
    link_incidents_to_nps_topics,
    retrieve_incident_candidates,
)
from nps_lens.services.classification_protocol import (
    CompactComment,
    category_catalog,
    taxonomy_fingerprint,
)
from nps_lens.services.taxonomy_discovery import TaxonomyResponse


def semantic_catalog():
    return reviewed_taxonomy(TAXONOMY, {}) | {"review": {}}


def test_criterion_is_required_and_changes_canonical_fingerprint():
    taxonomy = semantic_catalog()
    taxonomy.pop("review")
    TaxonomyResponse.model_validate(taxonomy)
    reordered = copy.deepcopy(taxonomy)
    reordered["taxonomy"].reverse()
    for branch in reordered["taxonomy"]:
        branch["sublevers"].reverse()
    assert taxonomy_fingerprint(taxonomy) == taxonomy_fingerprint(reordered)
    reordered["taxonomy"][0]["sublevers"][0]["criterion"] = "Una frontera distinta."
    assert taxonomy_fingerprint(taxonomy) != taxonomy_fingerprint(reordered)
    for value in ("", " "):
        reordered["taxonomy"][0]["sublevers"][0]["criterion"] = value
        with pytest.raises(ValueError):
            TaxonomyResponse.model_validate(reordered)
    with pytest.raises(ValueError):
        TaxonomyResponse.model_validate(TAXONOMY)


def test_category_resolution_is_atomic_without_explanations():
    catalog = category_catalog(TAXONOMY)
    for key, category in catalog.items():
        row = CompactComment(id="1", primary=key, secondary=[])
        assert row.expand(catalog)["primary_classification"] == {
            "lever": category["lever"],
            "sublever": category["sublever"],
        }
    with pytest.raises(ValueError):
        CompactComment(id="1", primary="c999").expand(catalog)


@pytest.mark.parametrize("missing", ["", " "])
def test_partial_llm_pair_never_reaches_consumable_lens(exchange, missing):
    handler, ctx, frame, _ = exchange
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    handler.import_response(
        ctx, classifier_zip(classifier_files(request["manifest.json"], request)), "classifier"
    )
    artifact = handler.taxonomy.classification_artifact(ctx, "DISCOVERED")
    artifact["sublever"][0] = missing
    output = handler.taxonomy.resolve(ctx, mode="DISCOVERED")
    assert output.iloc[0].Palanca == output.iloc[0].Subpalanca == ""
    applied = handler.apply(ctx, frame, "DISCOVERED")
    assert applied.iloc[0].Palanca == applied.iloc[0].Subpalanca == ""
    # Historical SOURCE legitimately permits partial labels.
    frame.loc[0, "Palanca"] = "Histórico"
    assert handler.taxonomy.resolve(ctx, mode="SOURCE").iloc[0].Palanca == "Histórico"


def test_semantic_fingerprint_flows_and_rejects_mismatch(exchange):
    handler, ctx, _, _ = exchange
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    fingerprint = handler.taxonomy.state(ctx)["taxonomy_fingerprint"]
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    assert request["manifest.json"]["taxonomy_fingerprint"] == fingerprint
    response = classifier_files(request["manifest.json"], request)
    response["manifest"]["taxonomy_fingerprint"] = "different"
    with pytest.raises(ValueError):
        handler.import_response(ctx, classifier_zip(response), "classifier")
    assert handler.progress(ctx)["received"] == 0


def test_retrieval_is_shared_bounded_and_temporal():
    date = pd.Timestamp("2026-09-01")
    nps = pd.DataFrame(
        {
            "_business_key": [f"n{i}" for i in range(12)],
            "Comment": "transferencia retenida pendiente",
            "Fecha": date,
            "Palanca": "Pagos",
            "Subpalanca": "Transferencias",
        }
    )
    nps.loc[0, "Fecha"] = date + pd.Timedelta(days=LINK_MAX_DAYS_APART + 1)
    nps.loc[1, "Fecha"] = pd.NaT
    incidents = pd.DataFrame(
        {
            "Incident Number": ["I"],
            "Detailed Description": ["transferencia retenida pendiente"],
            "Submit Date": [date],
        }
    )
    candidates = retrieve_incident_candidates(nps, incidents)
    assert len(candidates) == LINK_TOP_K_PER_INCIDENT
    assert not set(candidates.nps_id) & {"n0", "n1"}
    pd.testing.assert_frame_equal(candidates, link_incidents_to_nps_topics(nps, incidents)[1])
    pd.testing.assert_frame_equal(
        candidates, retrieve_incident_candidates(nps.iloc[::-1], incidents)
    )


def test_helix_serializes_candidates_only_and_checks_membership(helix):
    handler, ctx, frame, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    assert not any(name.startswith("comments/") for name in request)
    rows = [
        row
        for name, payload in request.items()
        if name.startswith("incidents/")
        for row in payload["incidents"]
    ]
    assert all(0 < len(row["candidates"]) <= LINK_TOP_K_PER_INCIDENT for row in rows)
    supplied = {c["id"] for c in rows[0]["candidates"]}
    assert len(supplied) < len(frame)
    response = helix_response(request, rows[0]["candidates"][0]["id"])
    link = response["results/000001.json"]["classifications"][0]["links"][0]
    link["nps_id"] = next(row["id"] for row in inputs["comments"] if row["id"] not in supplied)
    with pytest.raises(ValueError, match="no candidato"):
        handler.import_response(ctx, inputs, zipped(response))
    assert handler.status(ctx, inputs)["received"] == 0


def test_compact_incident_retains_negation_after_long_narrative_and_unknown_labels():
    narrative = (
        "Detalle funcional " * 90 + "el token funciona correctamente; no es un problema de acceso"
    )
    frame = pd.DataFrame(
        {
            "Detailed Description": [
                "Producto: Token\nEmail: persona@example.com\nDescripción: "
                + narrative
                + "\nComprobaciones: se validó la firma correctamente\nNueva etiqueta: saldo incoherente"
            ]
        }
    )
    text = build_incident_text(frame).iloc[0]
    assert "el token funciona correctamente" in text
    assert "no es un problema de acceso" in text
    assert "se validó la firma correctamente" in text
    assert "saldo incoherente" in text
    assert "Producto: Token" not in text and "persona@example.com" not in text


def test_helix_accepts_cross_category_link_but_rejects_changed_dates(helix):
    handler, ctx, frame, incidents, _ = helix
    frame.loc[1, ["Palanca", "Subpalanca"]] = ["Acceso", "Token"]
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    different = next(
        key
        for key, pair in request["taxonomies.json"]["SOURCE"].items()
        if pair["lever"] == "Acceso"
    )
    for name, payload in response.items():
        if name.startswith("results/"):
            for row in payload["classifications"]:
                row["primary"] = different
    # A changed incident date cannot import a link selected under the old window.
    changed = incidents.copy()
    changed["Submit Date"] += pd.Timedelta(days=LINK_MAX_DAYS_APART + 1)
    with pytest.raises(ValueError, match="incidencias han cambiado"):
        handler.import_response(ctx, handler.inputs(ctx, changed, "SOURCE"), zipped(response))
    assert handler.status(ctx, inputs)["received"] == 0
    assert handler.import_response(ctx, inputs, zipped(response))["links"] == len(incidents)
    changed_status = handler.status(ctx, handler.inputs(ctx, changed, "SOURCE"))
    assert changed_status["received"] == len(incidents)
    assert changed_status["link_pending"] == len(incidents)


def test_complete_semantic_identity_designer_comments_helix_application(helix):
    helix_handler, ctx, frame, incidents, _ = helix
    from nps_lens.services.taxonomy_exchange import TaxonomyExchange

    handler = TaxonomyExchange(helix_handler.taxonomy, helix_handler.downloads)
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    fingerprint = handler.taxonomy.state(ctx)["taxonomy_fingerprint"]
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    handler.import_response(
        ctx, classifier_zip(classifier_files(request["manifest.json"], request)), "classifier"
    )
    assert (
        handler.taxonomy.classification_artifact(ctx, "DISCOVERED")["taxonomy_fingerprint"]
        == fingerprint
    )
    inputs = helix_handler.inputs(ctx, incidents, "DISCOVERED")
    request = exported(helix_handler.export(ctx, inputs)["saved_paths"])
    assert request["manifest.json"]["taxonomy_fingerprint"] == fingerprint
    response = helix_response(request, inputs["comments"][0]["id"])
    bad = copy.deepcopy(response)
    bad["manifest.json"]["taxonomy_fingerprint"] = "different"
    with pytest.raises(ValueError):
        helix_handler.import_response(ctx, inputs, zipped(bad))
    assert helix_handler.import_response(ctx, inputs, zipped(response))["ready"]
    assert all(
        row["taxonomy_fingerprint"] == fingerprint
        for row in helix_handler.current(ctx, inputs)["DISCOVERED"].values()
    )
    assert len(helix_handler.links(ctx, inputs, frame, incidents)) == len(incidents)
    consumed = handler.taxonomy.resolve(ctx, mode="DISCOVERED")
    assert consumed.Palanca.eq("Atención").all() and consumed.Subpalanca.eq("Resolución").all()


def test_restored_source_preserves_historical_partial_pair(exchange):
    handler, ctx, frame, _ = exchange
    frame.loc[0, "Palanca"] = "Histórico"
    frame["Fecha"] = pd.Timestamp("2026-09-01")
    handler.taxonomy.configure(ctx, {"policy": "SOURCE_AND_ACTIVE"})
    snapshot = handler.taxonomy.snapshot(ctx)
    handler.taxonomy.restore(ctx, snapshot)
    row = handler.taxonomy.resolve(ctx, mode="SOURCE").iloc[0]
    assert row.Palanca == "Histórico" and row.Subpalanca == ""
