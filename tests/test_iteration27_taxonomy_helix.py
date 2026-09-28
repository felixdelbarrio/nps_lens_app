from __future__ import annotations

import copy

import pandas as pd
import pytest
from test_taxonomy_exchange import TAXONOMY, classifier_files, exported, zipped

from nps_lens.domain.models import UploadContext
from nps_lens.services.helix_exchange import HelixExchange
from nps_lens.services.taxonomy_exchange import encode

pytest_plugins = ["test_taxonomy_exchange"]


def discover(handler, context):
    result = handler.import_response(context, encode(TAXONOMY))
    files = exported(result["saved_path"])
    handler.import_response(context, zipped(classifier_files(files["manifest.json"], files)))


def test_discovered_pending_preserves_existing_and_reclassifies_changed_text(exchange):
    handler, context, frame, _ = exchange
    discover(handler, context)
    with pytest.raises(ValueError, match="comentarios"):
        handler.export(context, pending=True)
    frame.loc[0, "Comment"] = "Ahora no funciona"
    frame.loc[len(frame)] = ["new-key", "Otra incidencia", "", "", "Web"]
    pending = exported(handler.export(context, pending=True)["saved_path"])
    sent = [
        row
        for name, batch in pending.items()
        if name.startswith("comments/")
        for row in batch["comments"]
    ]
    assert len(sent) == 2
    assert {row["Comment"] for row in sent} == {"Ahora no funciona", "Otra incidencia"}
    response = classifier_files(pending["manifest.json"], pending)
    handler.import_response(context, zipped(response))
    resolved = handler.taxonomy.resolve(context, mode="DISCOVERED")
    assert len(resolved) == 406
    assert resolved.Palanca.eq("Atención").all()


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
    assert (
        "Sin respuestas" in tax.catalog(context, "COMPLETED_NORMALIZED")["taxonomy"][0]["sublevers"]
    )
    tax.save_manual(context, [{"lever": "Nueva", "sublevers": ["Sin respuestas"]}])
    assert tax.resolve(context, mode="COMPLETED").Palanca.eq("").all()


def test_manual_uses_discovered_skeleton_without_origin(exchange):
    handler, context, _, _ = exchange
    discover(handler, context)
    assert handler.taxonomy.manual_draft(context) == TAXONOMY
    handler.taxonomy.save_manual(context, TAXONOMY["taxonomy"])
    assert handler.taxonomy.resolve(context, mode="COMPLETED").Palanca.eq("Atención").all()


@pytest.fixture
def helix(exchange):
    handler, context, frame, client = exchange
    frame["Palanca"], frame["Subpalanca"] = "Atención", "Resolución"
    incidents = pd.DataFrame(
        {
            "Incident Number": [f"INC-{i}" for i in range(201)],
            "Detailed Description": "No resuelven el problema",
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
                        "lever": "Atención",
                        "sublever": "Resolución",
                        "entity": entity,
                        "rationale": "Coincidencia de síntoma; no demuestra causalidad.",
                        "links": [{"nps_id": nps_id, "confidence": 0.9}],
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
    inputs = handler.inputs(ctx, incidents, "SOURCE", "domain_touchpoint")
    with pytest.raises(ValueError, match="Importa"):
        handler.links(ctx, inputs, frame, incidents)
    request = exported(handler.export(ctx, inputs)["saved_path"])
    response = helix_response(request, inputs["comments"][0]["id"])
    partial = {
        "manifest.json": response["manifest.json"],
        "results/000001.json": response["results/000001.json"],
    }
    assert handler.import_response(ctx, inputs, zipped(partial))["pending"] == 1
    assert not handler.status(ctx, inputs)["ready"]
    pending = exported(handler.export(ctx, inputs)["saved_path"])
    assert pending["incidents/000001.json"]["incidents"][0]["id"] == "INC-200"
    invalid = copy.deepcopy(response)
    invalid["results/000002.json"]["classifications"][0]["lever"] = "Inventada"
    with pytest.raises(ValueError, match="taxonomía"):
        handler.import_response(ctx, inputs, zipped(invalid))
    assert handler.status(ctx, inputs)["pending"] == 1
    assert handler.import_response(ctx, inputs, zipped(response))["ready"]
    assert handler.import_response(ctx, inputs, zipped(response))["ready"]
    links = handler.links(ctx, inputs, frame, incidents)
    assert len(links) == 201
    assert links.llm_entity.eq("Resolución").all()
    with pytest.raises(ValueError, match="dataset"):
        handler.import_response(UploadContext("Foreign", "", ""), inputs, zipped(response))
    altered = handler.inputs(ctx, incidents, "NORMALIZED", "domain_touchpoint")
    with pytest.raises(ValueError, match="cambiado"):
        handler.import_response(ctx, altered, zipped(response))
    incidents.loc[0, "Detailed Description"] = "Otro síntoma"
    updated = handler.inputs(ctx, incidents, "SOURCE", "domain_touchpoint")
    assert handler.status(ctx, updated)["pending"] == 1
    with pytest.raises(ValueError, match="cambiado"):
        handler.import_response(ctx, updated, zipped(response))


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
    inputs = handler.inputs(ctx, incidents, "SOURCE", method)
    request = exported(handler.export(ctx, inputs)["saved_path"])
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
    assert payload["links_mode_df"].nps_topic.eq(entity).all()
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
    for path in ("/manual", "/helix?mode=SOURCE&method=domain_touchpoint"):
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
    inputs = handler.inputs(ctx, incidents, "SOURCE", "domain_touchpoint")
    request = exported(handler.export(ctx, inputs)["saved_path"])
    handler.import_response(
        ctx, inputs, zipped(helix_response(request, inputs["comments"][0]["id"]))
    )
    handler.taxonomy.configure(ctx, {"active": "SOURCE"})
    state = handler.taxonomy.state(ctx)
    state.update(causal_engine="llm", causal_method="domain_touchpoint")
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
