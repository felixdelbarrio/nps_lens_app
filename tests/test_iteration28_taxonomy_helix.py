from __future__ import annotations

import copy

import pandas as pd
import pytest
from test_taxonomy_exchange import (
    TAXONOMY,
    classifier_files,
    classifier_zip,
    designer_zip,
    exported,
    zipped,
)
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.domain.models import UploadContext
from nps_lens.services.helix_exchange import HelixExchange
from nps_lens.services.taxonomy_exchange import TaxonomyExchange


def discover(handler, context):
    handler.import_response(context, designer_zip(handler, context), "designer")
    handler.taxonomy.configure(context, {"active": "DISCOVERED"})
    files = exported(handler.export(context, "classifier")["saved_paths"])
    handler.import_response(
        context, classifier_zip(classifier_files(files["manifest.json"], files)), "classifier"
    )


def test_discovered_pending_preserves_existing_and_reclassifies_changed_text(exchange):
    handler, context, frame, _ = exchange
    discover(handler, context)
    assert handler.export(context, stage="classifier")["saved_paths"] == []
    frame.loc[0, "Comment"] = "Ahora no funciona"
    frame.loc[len(frame)] = ["new-key", "Otra incidencia", "", "", "Web"]
    pending = exported(handler.export(context, stage="classifier")["saved_paths"])
    sent = [
        row
        for name, batch in pending.items()
        if name.startswith("comments/")
        for row in batch["comments"]
    ]
    assert len(sent) == 2
    assert {row["Comment"] for row in sent} == {"Ahora no funciona", "Otra incidencia"}
    response = classifier_files(pending["manifest.json"], pending)
    handler.import_response(context, classifier_zip(response), "classifier")
    resolved = handler.taxonomy.resolve(context, mode="DISCOVERED")
    assert len(resolved) == 406
    assert resolved.Palanca.eq("Atención").all()


def test_comment_classification_survives_non_semantic_record_updates(exchange):
    handler, context, frame, _ = exchange
    discover(handler, context)
    frame.loc[0, ["NPS", "Fecha", "Canal"]] = [10, pd.Timestamp("2026-09-30"), "App"]
    frame.loc[0, "_business_key"] = "reingested-without-stable-external-id"

    assert handler.progress(context)["pending"] == 0
    assert handler.export(context, stage="classifier")["saved_paths"] == []


def test_manual_origin_priority_renames_deletes_and_empty_categories(exchange):
    handler, context, frame, _ = exchange
    frame["Palanca"], frame["Subpalanca"] = "Origen", "Anterior"
    tax = handler.taxonomy
    assert tax.manual_draft(context) == {
        "taxonomy": [{"lever": "Origen", "sublevers": ["Anterior"]}]
    }
    tax.save_manual(
        context,
        [
            {
                "lever": "Nueva",
                "sublevers": ["Nueva sub", "Sin respuestas"],
                "previous_lever": "Origen",
                "previous_sublevers": ["Anterior", ""],
            }
        ],
    )
    assert tax.resolve(context, mode="COMPLETED").Palanca.eq("Nueva").all()
    assert tax.resolve(context, mode="SOURCE").Palanca.eq("Origen").all()
    assert "Sin respuestas" in tax.catalog(context, "COMPLETED")["taxonomy"][0]["sublevers"]
    tax.save_manual(context, [{"lever": "Nueva", "sublevers": ["Sin respuestas"]}])
    assert tax.resolve(context, mode="COMPLETED").Palanca.eq("").all()


def test_manual_uses_discovered_skeleton_without_origin(exchange):
    handler, context, _, _ = exchange
    discover(handler, context)
    assert handler.taxonomy.manual_draft(context) == TAXONOMY
    handler.taxonomy.save_manual(context, TAXONOMY["taxonomy"])
    assert handler.taxonomy.resolve(context, mode="COMPLETED").Palanca.eq("Atención").all()


@pytest.fixture(name="helix")
def helix_fixture(exchange):
    handler, context, frame, client = exchange
    frame["Palanca"], frame["Subpalanca"] = "Atención", "Resolución"
    frame["Comment"] = [f"No resuelven transferencia retenida {i}" for i in range(len(frame))]
    frame["Fecha"], frame["NPS"] = pd.Timestamp("2026-09-01"), 2
    incidents = pd.DataFrame(
        {
            "Incident Number": [f"INC-{i}" for i in range(201)],
            "Detailed Description": "No resuelven transferencia retenida",
            "Submit Date": pd.Timestamp("2026-09-01"),
            "BBVA_SourceServiceN2": "Web",
        }
    )
    service = HelixExchange(handler.taxonomy, handler.downloads)
    return service, context, frame, incidents, client


def helix_response(request, nps_id, entity="Resolución"):
    return {
        "manifest.json": request["manifest.json"],
        **{
            name.replace("incidents/", "results/"): {
                "classifications": [
                    {
                        "id": row["id"],
                        "primary": next(
                            key
                            for catalog in request["taxonomies.json"].values()
                            for key, pair in catalog.items()
                            if pair["lever"] == "Atención" and pair["sublever"] == "Resolución"
                        ),
                        "secondary": [],
                        "links": (
                            [
                                {
                                    "nps_id": nps_id,
                                    "confidence": 0.9,
                                    "incident_quote": row["description"] or "Sin descripción",
                                    "comment_quote": next(
                                        comment["Comment"]
                                        for comment in row["candidates"]
                                        if comment["id"] == nps_id
                                    ),
                                }
                            ]
                            if any(c["id"] == nps_id for c in row["candidates"])
                            else []
                        ),
                    }
                    for row in payload["incidents"]
                ]
            }
            for name, payload in request.items()
            if name.startswith("incidents/")
        },
    }


def test_helix_partial_pending_atomic_and_stale(helix):
    handler, ctx, frame, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    with pytest.raises(ValueError, match="Importa"):
        handler.links(ctx, inputs, frame, incidents)
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    partial = {
        "manifest.json": response["manifest.json"],
        "results/000001.json": response["results/000001.json"],
    }
    assert handler.import_response(ctx, inputs, zipped(partial))["pending"] == 1
    assert not handler.status(ctx, inputs)["ready"]
    pending = exported(handler.export(ctx, inputs)["saved_paths"])
    assert pending["incidents/000001.json"]["incidents"][0]["id"] == "INC-200"
    invalid = copy.deepcopy(response)
    invalid["results/000002.json"]["classifications"][0]["primary"] = "Inventada"
    with pytest.raises(ValueError, match="taxonomía"):
        handler.import_response(ctx, inputs, zipped(invalid))
    assert handler.status(ctx, inputs)["pending"] == 1
    assert handler.import_response(ctx, inputs, zipped(response))["ready"]
    assert handler.import_response(ctx, inputs, zipped(response))["ready"]
    links = handler.links(ctx, inputs, frame, incidents)
    assert len(links) == 201
    assert links.incident_topic.eq("Atención > Resolución").all()
    with pytest.raises(ValueError, match="dataset"):
        handler.import_response(UploadContext("Foreign", "", ""), inputs, zipped(response))
    handler.taxonomy.save_manual(ctx, handler.taxonomy.manual_draft(ctx)["taxonomy"])
    altered = handler.inputs(ctx, incidents, "COMPLETED")
    assert handler.import_response(ctx, altered, zipped(response))["ready"]
    assert handler.taxonomy.state(ctx)["active"] == "SOURCE"
    incidents.loc[0, "Detailed Description"] = "Otro síntoma"
    updated = handler.inputs(ctx, incidents, "SOURCE")
    assert handler.status(ctx, updated)["pending"] == 1
    with pytest.raises(ValueError, match="cambiado"):
        handler.import_response(ctx, updated, zipped(response))


def test_helix_classification_survives_operational_status_updates(helix):
    handler, ctx, _, incidents, _ = helix
    incidents["Status"] = "Assigned"
    incidents["Last Modified Date"] = pd.Timestamp("2026-09-01")
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    assert handler.import_response(ctx, inputs, zipped(response))["ready"]

    incidents["Status"] = "Resolved"
    incidents["Last Modified Date"] = pd.Timestamp("2026-09-30")
    incidents["BBVA_SourceServiceN2"] = "App"
    updated = handler.inputs(ctx, incidents, "SOURCE")

    assert [row["description"] for row in updated["incidents"]] == [
        row["description"] for row in inputs["incidents"]
    ]
    assert handler.status(ctx, updated)["received"] == len(incidents)
    assert handler.export(ctx, updated)["saved_paths"] == []


@pytest.mark.parametrize(
    "method,entity",
    [
        ("palanca_touchpoint", "Atención"),
        ("domain_touchpoint", "Resolución"),
        ("bbva_source_service_n2", "Web"),
        ("broken_journeys", "Resolución interrumpida"),
        ("executive_journeys", "Reclamación sin resolver"),
    ],
)
def test_helix_five_methods_and_engine_without_rules(helix, method, entity, monkeypatch):
    handler, ctx, frame, incidents, client = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"], entity)
    assert handler.import_response(ctx, inputs, zipped(response))["ready"]
    dashboard = client.app.state.dashboard_service
    monkeypatch.setattr(dashboard, "_load_helix_df", lambda *args, **kwargs: incidents)
    params = {"service_origin": "Bank", "mode": "SOURCE", "method": method, "engine": "llm"}
    assert client.put("/api/taxonomy/helix/engine", params=params).status_code == 200
    assert handler.taxonomy.state(ctx)["causal_engine"] == "llm"
    links = handler.links(ctx, inputs, frame, incidents)
    payload = dashboard._build_touchpoint_mode_payload(
        touchpoint_source=method,
        links_df=links,
        focus_df=frame,
        helix_df=incidents,
        by_topic_weekly=pd.DataFrame(),
    )
    assert not payload["links_mode_df"].empty
    frame.loc[0, "Comment"] = "Changed comment"
    blocked = client.put("/api/taxonomy/helix/engine", params=params)
    assert blocked.status_code == 409
    assert (
        client.put("/api/taxonomy/helix/engine", params={**params, "engine": "rules"}).status_code
        == 200
    )


def test_local_endpoints_are_unavailable_on_webapp(helix):
    handler, ctx, frame, incidents, client = helix
    from dataclasses import replace

    client.app.state.settings = replace(client.app.state.settings, auth_mode="iap")
    for path in ("/manual", "/helix"):
        result = client.get("/api/taxonomy" + path)
        assert result.status_code in (401, 403, 404)


def test_new_manual_categories_do_not_assign_unclassified_comments(exchange):
    handler, context, _, _ = exchange
    handler.taxonomy.save_manual(
        context,
        [
            {
                "lever": "Nueva",
                "sublevers": ["Nueva sub"],
                "previous_lever": "",
                "previous_sublevers": [""],
            }
        ],
    )
    assert handler.taxonomy.resolve(context, mode="COMPLETED").Palanca.eq("").all()


def test_llm_causal_pipeline_does_not_call_business_classifier(helix, monkeypatch):
    handler, ctx, frame, incidents, client = helix
    frame["Fecha"] = pd.Timestamp("2026-09-01")
    frame["NPS"] = 2
    incidents["Submit Date"] = pd.Timestamp("2026-09-01")
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    handler.import_response(
        ctx, inputs, zipped(helix_response(request, inputs["comments"][0]["id"]))
    )
    handler.taxonomy.configure(ctx, {"active": "SOURCE"})
    state = handler.taxonomy.state(ctx)
    state.update(causal_engine="llm")
    handler.taxonomy.save_state(ctx, state)
    dashboard = client.app.state.dashboard_service
    monkeypatch.setattr(dashboard, "_load_helix_df", lambda *args, **kwargs: incidents)
    monkeypatch.setattr(
        dashboard, "_load_nps_df", lambda *args, **kwargs: handler.taxonomy.resolve(ctx)
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("Business classifier must not run in LLM mode")

    monkeypatch.setattr(
        "nps_lens.services.dashboard_service.link_incidents_to_nps_topics", forbidden
    )
    result = dashboard._causal_analysis_bundle(
        context=ctx,
        pop_year="Todos",
        pop_month="Todos",
        score_channel="Todos",
        min_similarity=0.15,
        max_days_apart=30,
        touchpoint_source="domain_touchpoint",
    )
    assert result["ready"]
    assert len(result["core"]["links_df"]) == 201
    assert result["mode_payload"]["links_mode_df"].nps_topic.eq("Resolución").all()


def test_three_taxonomies_are_independent_and_selection_reuses_assignments(helix):
    handler, ctx, frame, incidents, _ = helix
    exchange = TaxonomyExchange(handler.taxonomy, handler.downloads)
    discover(exchange, ctx)
    handler.taxonomy.save_manual(ctx, handler.taxonomy.manual_draft(ctx)["taxonomy"])
    source = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, source)["saved_paths"])
    handler.import_response(
        ctx, source, zipped(helix_response(request, source["comments"][0]["id"]))
    )
    for mode in ["COMPLETED", "DISCOVERED"]:
        current = handler.inputs(ctx, incidents, mode)
        assert handler.status(ctx, current)["pending"] == 201
        request = exported(handler.export(ctx, current)["saved_paths"])
        assert list(request["manifest.json"]["taxonomy_scopes"]) == [mode]
        response = helix_response(request, current["comments"][0]["id"])
        assert handler.import_response(ctx, current, zipped(response))["ready"]
    for mode in ["SOURCE", "COMPLETED", "DISCOVERED"]:
        assert handler.status(ctx, handler.inputs(ctx, incidents, mode))["ready"]
    handler.taxonomy.save_manual(
        ctx,
        [
            {
                "lever": "Manual",
                "sublevers": ["Propia"],
                "previous_lever": "Atención",
                "previous_sublevers": ["Resolución"],
            }
        ],
    )
    for mode in ["SOURCE", "DISCOVERED"]:
        assert handler.status(ctx, handler.inputs(ctx, incidents, mode))["pending"] == 0
    latest_status = handler.status(ctx, handler.inputs(ctx, incidents, "COMPLETED"))
    assert latest_status["pending"] == 0
    assert latest_status["link_pending"] == 201


def test_partial_classifier_export_omits_already_imported_comments(exchange):
    handler, ctx, _, _ = exchange
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    response = classifier_files(request["manifest.json"], request)
    handler.import_response(
        ctx,
        classifier_zip(
            {"manifest": response["manifest"], "results": {"000001": response["results"]["000001"]}}
        ),
        "classifier",
    )
    for _ in range(5):
        pending = exported(handler.export(ctx, "classifier")["saved_paths"])
    assert sum(batch["count"] for batch in pending["manifest.json"]["batches"]) == 205
    handler.import_response(
        ctx, classifier_zip(classifier_files(pending["manifest.json"], pending)), "classifier"
    )
    assert handler.taxonomy.resolve(ctx, mode="DISCOVERED").Palanca.eq("Atención").all()


def test_manual_replacement_requires_review_and_unique_revision(helix):
    handler, ctx, _, incidents, client = helix
    params = {"service_origin": ctx.service_origin}
    initial = client.get("/api/taxonomy/manual", params=params).json()
    assert initial["templates"] == ["NONE", "SOURCE"]
    assert not initial["exists"]
    payload = {**initial, "template": "SOURCE"}
    assert client.put("/api/taxonomy/manual", params=params, json=payload).status_code == 200
    current = client.get("/api/taxonomy/manual", params=params).json()
    old_inputs = handler.inputs(ctx, incidents, "COMPLETED")
    request = exported(handler.export(ctx, old_inputs)["saved_paths"])
    handler.import_response(
        ctx, old_inputs, zipped(helix_response(request, old_inputs["comments"][0]["id"]))
    )
    replacement = {**current, "template": "SOURCE"}
    assert client.put("/api/taxonomy/manual", params=params, json=replacement).status_code == 409
    assert handler.status(ctx, old_inputs)["ready"]
    replacement["confirmed"] = True
    assert client.put("/api/taxonomy/manual", params=params, json=replacement).status_code == 200
    latest = client.get("/api/taxonomy/manual", params=params).json()
    assert latest["revision"] != current["revision"]
    latest_status = handler.status(ctx, handler.inputs(ctx, incidents, "COMPLETED"))
    assert latest_status["pending"] == 0
    assert latest_status["link_pending"] == 201
    assert client.put("/api/taxonomy/manual", params=params, json=replacement).status_code == 409
    assert handler.taxonomy.state(ctx)["active"] == "SOURCE"


def test_manual_missing_catalog_can_be_recreated(exchange):
    handler, ctx, _, client = exchange
    tax = handler.taxonomy
    tax.save_manual(ctx, [{"lever": "Manual", "sublevers": ["Tema"]}], template="NONE")
    state = tax.state(ctx)
    sig = state["artifacts"]["COMPLETED"]
    item = dict(tax.artifact(sig))
    item.pop("taxonomy")
    with tax.repository._connect() as db:
        from nps_lens.services.taxonomy_exchange import encode

        db.execute(
            "UPDATE taxonomy_artifacts SET payload=? WHERE signature=?",
            (encode(item).decode(), sig),
        )
    tax._cache.clear()
    response = client.get("/api/taxonomy/manual", params={"service_origin": ctx.service_origin})
    assert response.status_code == 200
    assert response.json()["exists"] is False
    assert response.json()["templates"] == ["NONE"]


def test_helix_flat_annotations_kpis_and_concise_validation(helix):
    handler, ctx, _, incidents, _ = helix
    incidents.loc[0, "Detailed Description"] = "Consulta sin detalles suficientes"
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    first = response["results/000001.json"]["classifications"][0]
    first["entity"] = "unexpected"
    with pytest.raises(ValueError, match="formato inválido"):
        handler.import_response(ctx, inputs, zipped(response))
    first.pop("entity")
    first.update(primary=None, secondary=[], links=[])
    status = handler.import_response(ctx, inputs, zipped(response))
    assert status["total"] == status["received"] == 201
    assert status["classified"] == status["links"] == 200
    assert status["unassigned"] == 1
    assert sum(row["count"] for row in status["categories"]) == 200
    assert "entity" not in next(iter(handler.current(ctx, inputs)["SOURCE"].values()))
    first["links"] = "private invalid value"
    with pytest.raises(ValueError) as error:
        handler.import_response(ctx, inputs, zipped(response))
    assert "results/000001.json" in str(error.value)
    assert "private invalid value" not in str(error.value)
    assert len(str(error.value)) < 300
    assert handler.status(ctx, inputs)["received"] == 201


def test_helix_tolerates_known_nonsemantic_llm_annotations_but_rejects_schema_drift(helix):
    handler, ctx, _, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    first = response["results/000001.json"]["classifications"][0]
    first["evidence"] = {
        "quotes": ["texto"],
        "reason": "explicación no contractual",
    }
    first["rationale"] = "explicación no contractual"
    first["links"][0]["reason"] = "explicación no contractual"

    status = handler.import_response(ctx, inputs, zipped(response))
    assert status["received"] == len(incidents)
    stored = handler.current(ctx, inputs)["SOURCE"][first["id"]]
    assert "evidence" not in stored and "rationale" not in stored
    assert "reason" not in stored["links"][0]

    changed_incidents = incidents.assign(
        **{"Detailed Description": incidents["Detailed Description"] + " cambiado"}
    )
    fresh = handler.inputs(ctx, changed_incidents, "SOURCE")
    request = exported(handler.export(ctx, fresh)["saved_paths"])
    response = helix_response(request, fresh["comments"][0]["id"])
    response["results/000001.json"]["classifications"][0]["entity"] = "unexpected"
    with pytest.raises(ValueError, match="formato inválido"):
        handler.import_response(ctx, fresh, zipped(response))


def test_partial_imports_accumulate_across_jobs_after_restart(exchange):
    handler, ctx, _, _ = exchange
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    for received in (200, 400, 405):
        request = exported(handler.export(ctx, "classifier")["saved_paths"])
        response = classifier_files(request["manifest.json"], request)
        response["results"] = {"000001": response["results"]["000001"]}
        result = handler.import_response(ctx, classifier_zip(response), "classifier")
        assert result["progress"] == {"total": 405, "received": received, "pending": 405 - received}
        assert handler.taxonomy.resolve(ctx, mode="DISCOVERED").Palanca.ne("").sum() == received
        handler = TaxonomyExchange(handler.taxonomy, handler.downloads)
        assert handler.progress(ctx)["received"] == received
