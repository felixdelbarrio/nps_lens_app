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

from nps_lens.services.equivalence_exchange import EquivalenceExchange
from nps_lens.services.semantic_validation import validate_quote
from nps_lens.services.taxonomy_prompts import (
    MAX_PROJECT_INSTRUCTION_CHARS,
    PROJECT_INSTRUCTIONS,
    SEMANTIC_CRITERIA,
)


@pytest.mark.parametrize("change", ["unknown_field", "obsolete"])
def test_comments_reject_obsolete_response_atomically(exchange, change):
    handler, context, _, _ = exchange
    handler.import_response(context, designer_zip(handler, context), "designer")
    handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    request = exported(handler.export(context, "classifier")["saved_paths"])
    response = classifier_files(request["manifest.json"], request)
    if change == "unknown_field":
        response["results"]["000003"]["classifications"][-1]["unknown_field"] = "Unsupported output"
    else:
        response["manifest"]["instructions_version"] = "obsolete"
    with pytest.raises(ValueError):
        handler.import_response(context, classifier_zip(response), "classifier")
    assert handler.progress(context)["received"] == 0


@pytest.mark.parametrize("field", ["incident_quote", "comment_quote"])
def test_helix_links_require_both_original_sources(helix, field):
    handler, context, _, incidents, _ = helix
    inputs = handler.inputs(context, incidents, "SOURCE")
    request = exported(handler.export(context, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    response["results/000001.json"]["classifications"][-1]["links"][0][field] = "Inventado"
    with pytest.raises(ValueError, match="cita literal"):
        handler.import_response(context, inputs, zipped(response))
    assert handler.status(context, inputs)["received"] == 0


def test_identical_incidents_cannot_drift_between_batches(helix):
    handler, context, _, incidents, _ = helix
    inputs = handler.inputs(context, incidents, "SOURCE")
    request = exported(handler.export(context, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    handler.import_response(
        context,
        inputs,
        zipped(
            {
                "manifest.json": response["manifest.json"],
                "results/000001.json": response["results/000001.json"],
            }
        ),
    )
    changed = copy.deepcopy(response["results/000002.json"])
    changed["classifications"][0].update(primary=None, secondary=[], links=[])
    with pytest.raises(ValueError, match="idénticas"):
        handler.import_response(
            context,
            inputs,
            zipped({"manifest.json": response["manifest.json"], "results/000002.json": changed}),
        )
    assert handler.status(context, inputs)["received"] == 200


def test_designer_review_accepts_corpus_level_evidence_beyond_decision_limit(exchange):
    handler, context, _, _ = exchange
    request = exported(handler.export(context, "designer")["saved_path"])
    taxonomy = reviewed_taxonomy(TAXONOMY, request)
    quotes = [
        row["Comment"]
        for name, value in request.items()
        if name.startswith("comments/")
        for row in value["comments"]
        if row["Comment"].strip()
    ][:4]
    taxonomy["review"]["quotes"] = quotes
    taxonomy["review"]["reason"] = "Fronteras contrastadas con evidencia distribuida. " + (
        "x" * 2_050
    )

    result = handler.import_response(
        context,
        zipped({"manifest.json": request["manifest.json"], "taxonomy.json": taxonomy}),
        "designer",
    )

    assert result == {"stage": "designer", "imported": True}
    assert handler.taxonomy.state(context)["designer_review"]["quotes"] == quotes


def test_designer_review_cannot_cite_another_corpus(exchange):
    handler, context, _, _ = exchange
    request = exported(handler.export(context, "designer")["saved_path"])
    taxonomy = reviewed_taxonomy(TAXONOMY, request)
    taxonomy["review"]["quotes"] = ["Este ejemplo pertenece a otro corpus"]
    with pytest.raises(ValueError, match="ajena"):
        handler.import_response(
            context,
            zipped({"manifest.json": request["manifest.json"], "taxonomy.json": taxonomy}),
            "designer",
        )
    assert not handler.taxonomy.state(context).get("discovered_taxonomy")


def test_equivalences_require_explanation_and_current_policy(exchange):
    handler, context, frame, _ = exchange
    frame["Palanca"] = "Atención"
    exchange = EquivalenceExchange(handler.taxonomy, handler.downloads)
    request = exported(exchange.export_concepts(context, pd.DataFrame())["saved_path"])
    response = {
        "manifest.json": request["manifest.json"],
        "equivalences.json": {
            "dimensions": {
                "nps.Palanca": [
                    {"canonical": "Soporte", "aliases": ["Atención"], "reason": " " * 30}
                ]
            }
        },
    }
    before = handler.taxonomy.registry(context).to_dict()
    with pytest.raises(ValueError):
        exchange.import_concepts(context, pd.DataFrame(), zipped(response))
    assert handler.taxonomy.registry(context).to_dict() == before


def test_link_quotes_cannot_invent_or_invert_source_evidence():
    text = "Al firmar falla; validé el token y funciona OK."
    validate_quote("validé el token y funciona OK", text)
    with pytest.raises(ValueError, match="literal"):
        validate_quote("El token falla", text)


@pytest.mark.parametrize(
    "quote,text",
    [
        (
            "muchas veces no me habren los enlaces. queda en blanco",
            "muchas veces no me habren los enlaces.  queda en blanco en la parte de carga",
        ),
        (
            "Sr. , muy buena atencion, resolvio rapido el problema",
            "Sr. Facundo Jonas , muy buena atencion, resolvio rapido el problema",
        ),
        (
            "Necesitamos resumen Nº [dato omitido] correspondiente al mes Diciembre 2025",
            "Necesitamos resumen Nº 282-023686/3 correspondiente al mes Diciembre 2025",
        ),
    ],
)
def test_link_quotes_accept_safe_extract_redactions_and_whitespace(quote, text):
    validate_quote(quote, text)


def test_project_instructions_fit_chatgpt_without_losing_shared_safeguards():
    assert all(
        len(instructions) <= MAX_PROJECT_INSTRUCTION_CHARS
        for instructions in PROJECT_INSTRUCTIONS.values()
    )
    assert all(SEMANTIC_CRITERIA in instructions for instructions in PROJECT_INSTRUCTIONS.values())


def test_stale_helix_results_remain_selected_and_cannot_drive_llm(helix, monkeypatch):
    handler, context, _, incidents, client = helix
    inputs = handler.inputs(context, incidents, "SOURCE")
    request = exported(handler.export(context, inputs)["saved_paths"])
    handler.import_response(
        context, inputs, zipped(helix_response(request, inputs["comments"][0]["id"]))
    )
    monkeypatch.setattr("nps_lens.services.helix_exchange.INSTRUCTIONS_VERSION", "next-policy")
    assert handler.status(context, inputs)["pending"] == len(incidents)
    dashboard = client.app.state.dashboard_service
    monkeypatch.setattr(dashboard, "_load_helix_df", lambda *args, **kwargs: incidents)
    state = handler.taxonomy.state(context)
    state["causal_engine"] = "llm"
    handler.taxonomy.save_state(context, state)
    engine = dashboard.analysis_engine("helix", context)
    assert engine["selected_engine"] == "llm"
    assert not engine["ready"]
    assert engine["pending"] == len(incidents)


def test_new_comment_invalidates_previous_absence_of_links(helix):
    handler, context, frame, incidents, _ = helix
    inputs = handler.inputs(context, incidents, "SOURCE")
    request = exported(handler.export(context, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    for name, batch in response.items():
        if name.startswith("results/"):
            for row in batch["classifications"]:
                row["links"] = []
    assert handler.import_response(context, inputs, zipped(response))["ready"]
    frame.loc[1, "Comment"] = "Nueva evidencia relevante que antes no existía"
    updated = handler.inputs(context, incidents, "SOURCE")
    assert updated["scopes"] != inputs["scopes"]
    assert handler.status(context, updated)["pending"] == len(incidents)
