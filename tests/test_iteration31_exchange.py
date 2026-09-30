"""Production ZIP bounds and business invariants of the compact protocol."""

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
    zipped,
)
from test_taxonomy_exchange import exchange_fixture as exchange_fixture

from nps_lens.services.classification_protocol import category_catalog
from nps_lens.services.helix_exchange import HelixExchange
from nps_lens.services.taxonomy_exchange import digest, encode
from nps_lens.services.taxonomy_service import TaxonomyService


@pytest.fixture(autouse=True)
def production_batches(exchange, monkeypatch):
    monkeypatch.setattr("nps_lens.services.taxonomy_exchange.CLASSIFICATION_BATCH_ROWS", 1_000)


def set_comments(exchange, monkeypatch, comments):
    handler, ctx, _, _ = exchange
    frame = pd.DataFrame(
        {
            "_business_key": [f"private-{i:06d}" for i in range(len(comments))],
            "Comment": comments,
            "Palanca": "Atención",
            "Subpalanca": "Resolución",
            "Canal": "Web",
        }
    )
    monkeypatch.setattr(handler.taxonomy, "source", lambda context: frame.copy())
    return handler, ctx, frame


def restart(handler, frame, monkeypatch):
    service = TaxonomyService(handler.repository, handler.taxonomy.equivalences_path)
    monkeypatch.setattr(service, "source", lambda context: frame.copy())
    return type(handler)(service, handler.downloads)


def test_exact_text_fanout_local_empty_restart_and_individual_fingerprints(exchange, monkeypatch):
    handler, ctx, frame = set_comments(
        exchange, monkeypatch, [None, "", "NO", "NO", "no", "texto", "texto ", " "]
    )
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    sent = request["comments/000001.json"]["comments"]
    assert [row["Comment"] for row in sent] == ["NO", "no", "texto", "texto ", " "]
    assert handler.progress(ctx)["received"] == 2
    assert "private-" not in str(request)
    local = handler.assignments(ctx, frame, "DISCOVERED")
    assert local[frame.iloc[0]._business_key] == {
        "primary_classification": {
            "lever": "Sin clasificación temática",
            "sublever": "Información insuficiente",
        },
        "secondary_classifications": [],
    }
    response = classifier_files(request["manifest.json"], request)
    response["results"]["000001"]["classifications"][0]["secondary"] = ["c003"]
    handler = restart(handler, frame, monkeypatch)
    result = handler.import_response(ctx, classifier_zip(response), "classifier")
    assert result["progress"] == {"total": 8, "received": 8, "pending": 0}
    values = handler.assignments(ctx, frame, "DISCOVERED")
    assert values[frame.iloc[2]._business_key] == values[frame.iloc[3]._business_key]
    assert values[frame.iloc[3]._business_key]["secondary_classifications"] == [
        {"lever": "Sin clasificación temática", "sublever": "Tema no cubierto"}
    ]
    assert handler.export(ctx, "classifier")["saved_paths"] == []
    frame.loc[3, "Comment"] = "changed"
    handler = restart(handler, frame, monkeypatch)
    assert handler.progress(ctx)["received"] == 7
    new = exported(handler.export(ctx, "classifier")["saved_paths"])
    assert new["comments/000001.json"]["comments"] == [{"id": "1", "Comment": "changed"}]
    with pytest.raises(ValueError, match="corpus"):
        handler.import_response(ctx, classifier_zip(response), "classifier")


@pytest.mark.parametrize("fallback", [True, False])
def test_all_empty_resolves_only_when_exact_fallback_exists(exchange, monkeypatch, fallback):
    handler, ctx, frame = set_comments(exchange, monkeypatch, [None, "", ""])
    if fallback:
        handler.import_response(ctx, designer_zip(handler, ctx), "designer")
        handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    result = handler.export(ctx, "classifier")
    if fallback:
        assert result == {
            "stage": "complete",
            "saved_paths": [],
            "saved_directory": None,
            "batches": 0,
        }
        assert handler.progress(ctx)["pending"] == 0
        assert restart(handler, frame, monkeypatch).progress(ctx)["received"] == 3
    else:
        request = exported(result["saved_paths"])
        assert request["comments/000001.json"]["comments"] == [{"id": "1", "Comment": ""}]
        assert handler.progress(ctx)["pending"] == 3


def test_classifier_all_pending_batches_fanout_and_restart(exchange, monkeypatch):
    comments = [f"comment {i}" for i in range(4_003)] + ["comment 0", "comment 4001"]
    handler, ctx, frame = set_comments(exchange, monkeypatch, comments)
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    assert request["manifest.json"]["schema_version"] == "nps-lens-comments/3"
    assert [b["count"] for b in request["manifest.json"]["batches"]] == [1_000] * 4 + [3]
    assert request["manifest.json"]["taxonomy_sha256"] == digest(request["taxonomy.json"])
    for batch in request["manifest.json"]["batches"]:
        assert batch["sha256"] == digest(request[f"comments/{batch['id']}.json"])
    response = classifier_files(request["manifest.json"], request)
    response["results"] = {"000001": response["results"]["000001"]}
    result = handler.import_response(ctx, classifier_zip(response), "classifier")
    assert result["progress"]["received"] == 1_001
    handler = restart(handler, frame, monkeypatch)
    next_request = exported(handler.export(ctx, "classifier")["saved_paths"])
    pending = [
        row["Comment"]
        for name, batch in next_request.items()
        if name.startswith("comments/")
        for row in batch["comments"]
    ]
    assert len(pending) == 3_003
    assert not set(pending).intersection(comments[:1_000])
    response = classifier_files(next_request["manifest.json"], next_request)
    assert (
        handler.import_response(ctx, classifier_zip(response), "classifier")["progress"]["pending"]
        == 0
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"primary": "c999"},
        {"primary": None},
        {"secondary": ["c999"]},
        {"secondary": ["c001"]},
        {"secondary": ["c002", "c002"]},
        {"secondary": ["c002", "c003", "c004"]},
        {"secondary": "c002"},
    ],
)
def test_classifier_rejects_compact_category_errors_atomically(exchange, changes):
    handler, ctx, _, _ = exchange
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    response = classifier_files(request["manifest.json"], request)
    response["results"]["000001"]["classifications"][-1].update(changes)
    with pytest.raises(ValueError):
        handler.import_response(ctx, classifier_zip(response), "classifier")
    assert handler.progress(ctx)["received"] == 0


def test_catalog_ids_deterministic_and_legacy_persisted_labels_survive(exchange):
    assert category_catalog(TAXONOMY) == category_catalog(
        {
            "taxonomy": [
                {"lever": b["lever"], "sublevers": list(reversed(b["sublevers"]))}
                for b in reversed(TAXONOMY["taxonomy"])
            ]
        }
    )
    handler, ctx, frame, _ = exchange
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    # Pre-v3 artifact, using the existing domain format and fingerprints.
    with handler.repository._connect() as db:
        handler._persist_assignments(
            db,
            ctx,
            frame,
            {
                "mode": "DISCOVERED",
                "taxonomy": TAXONOMY,
                "manual_revision": "",
                "instructions_version": "pre-v3",
            },
            {
                frame.iloc[0]._business_key: {
                    "primary_classification": {"lever": "Atención", "sublever": "Resolución"},
                    "secondary_classifications": [],
                }
            },
        )
    assert handler.progress(ctx)["received"] == 1
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    assert sum(b["count"] for b in request["manifest.json"]["batches"]) == len(frame) - 1
    response = classifier_files(request["manifest.json"], request)
    response["manifest"]["schema_version"] = "nps-lens-comments/2"
    with pytest.raises(ValueError, match="incompatible.*comments/3"):
        handler.import_response(ctx, classifier_zip(response), "classifier")
    assert handler.progress(ctx)["received"] == 1


def test_helix_all_pending_no_dedup_or_local_empty_and_restart(helix, monkeypatch):
    handler, ctx, frame, original, _ = helix
    incidents = pd.concat([original.iloc[:1]] * 2_005, ignore_index=True)
    incidents["Incident Number"] = [f"INC-{i}" for i in range(len(incidents))]
    incidents.loc[0, "Detailed Description"] = ""
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    assert request["manifest.json"]["schema_version"] == "nps-lens-helix/3"
    assert [b["count"] for b in request["manifest.json"]["batches"]] == [1_000, 1_000, 5]
    assert request["incidents/000001.json"]["incidents"][0]["id"] == "INC-0"
    assert handler.status(ctx, inputs)["received"] == 0
    assert request["comments/000001.json"]["comments"][0] == {
        "id": inputs["comments"][0]["id"],
        "Comment": inputs["comments"][0]["Comment"],
        "primary": "c001",
        "secondary": [],
    }
    response = helix_response(request, inputs["comments"][0]["id"])
    response = {
        key: value
        for key, value in response.items()
        if key in ("manifest.json", "results/000001.json")
    }
    response["results/000001.json"]["classifications"][0].update(
        primary=None, secondary=[], links=[]
    )
    assert handler.import_response(ctx, inputs, zipped(response))["received"] == 1_000
    handler = restart(handler, frame, monkeypatch)
    assert handler.status(ctx, inputs)["unassigned"] == 1
    next_request = exported(handler.export(ctx, inputs)["saved_paths"])
    assert [b["count"] for b in next_request["manifest.json"]["batches"]] == [1_000, 5]
    assert next_request["incidents/000001.json"]["incidents"][0]["id"] == "INC-1000"
    response = helix_response(next_request, inputs["comments"][0]["id"])
    assert handler.import_response(ctx, inputs, zipped(response))["ready"]
    assert handler.import_response(ctx, inputs, zipped(response))["ready"]
    persisted = handler.current(ctx, inputs)["SOURCE"]["INC-1000"]
    assert persisted["lever"] == "Atención" and persisted["links"][0]["confidence"] == 0.9
    assert persisted["rationale"] == "Coincidencia de síntoma; no demuestra causalidad."


@pytest.mark.parametrize(
    "changes",
    [
        {"primary": "c999"},
        {"primary": None},
        {"secondary": ["c001"]},
        {"rationale": ""},
        {"rationale": "a" * 2_001},
        {"links": [{"nps_id": "unknown", "confidence": 0.8}]},
        {"links": [{"nps_id": "unknown", "confidence": 1.1}]},
        {"links": [{"nps_id": "unknown", "confidence": 0.8}] * 21},
    ],
)
def test_helix_rejects_invalid_category_rationale_or_links(helix, changes):
    handler, ctx, _, incidents, _ = helix
    inputs = handler.inputs(ctx, incidents, "SOURCE")
    request = exported(handler.export(ctx, inputs)["saved_paths"])
    response = helix_response(request, inputs["comments"][0]["id"])
    response["results/000001.json"]["classifications"][-1].update(changes)
    with pytest.raises(ValueError):
        handler.import_response(ctx, inputs, zipped(response))
    assert handler.status(ctx, inputs)["received"] == 0
    response["manifest.json"]["schema_version"] = "nps-lens-helix/2"
    with pytest.raises(ValueError, match="incompatible.*helix/3"):
        handler.import_response(ctx, inputs, zipped(response))


def test_helix_secondary_evidence_and_link_coherence(exchange, monkeypatch):
    handler, ctx, frame = set_comments(exchange, monkeypatch, ["dos temas", "otro"])
    frame.loc[1, ["Palanca", "Subpalanca"]] = ["Acceso", "Token"]
    request = exported(handler.export(ctx, "classifier")["saved_paths"])
    response = classifier_files(request["manifest.json"], request)
    response["results"]["000001"]["classifications"][0]["secondary"] = ["c001"]
    handler.import_response(ctx, classifier_zip(response), "classifier")
    helix = HelixExchange(handler.taxonomy, handler.downloads)
    incidents = pd.DataFrame({"Incident Number": ["INC"], "Detailed Description": ["dos temas"]})
    inputs = helix.inputs(ctx, incidents, "SOURCE")
    request = exported(helix.export(ctx, inputs)["saved_paths"])
    assert request["comments/000001.json"]["comments"][0]["secondary"] == ["c001"]
    response = helix_response(request, inputs["comments"][0]["id"])
    row = response["results/000001.json"]["classifications"][0]
    row.update(primary="c001", secondary=["c002"])
    invalid = copy.deepcopy(response)
    invalid["results/000001.json"]["classifications"][0]["links"] *= 2
    with pytest.raises(ValueError, match="duplicados"):
        helix.import_response(ctx, inputs, zipped(invalid))
    mismatched = copy.deepcopy(response)
    mismatched_row = mismatched["results/000001.json"]["classifications"][0]
    mismatched_row.update(primary="c001", secondary=[])
    mismatched_row["links"][0]["nps_id"] = inputs["comments"][1]["id"]
    with pytest.raises(ValueError, match="otra categoría"):
        helix.import_response(ctx, inputs, zipped(mismatched))
    assert helix.import_response(ctx, inputs, zipped(response))["multiple"] == 1
    assert helix.current(ctx, inputs)["SOURCE"]["INC"]["secondary_classifications"] == [
        {"lever": "Atención", "sublever": "Resolución"}
    ]


@pytest.mark.parametrize("stage", ["classifier", "helix", "evidence"])
def test_utf8_byte_bounds_and_oversized_item_is_not_truncated(exchange, monkeypatch, stage):
    handler, ctx, frame = set_comments(
        exchange, monkeypatch, ["漢" * 60_000 + str(i) for i in range(2)]
    )
    incidents = pd.DataFrame(
        {"Incident Number": ["I1", "I2"], "Detailed Description": ["漢" * 60_000] * 2}
    )
    helix = HelixExchange(handler.taxonomy, handler.downloads)
    if stage == "helix":
        frame["Comment"] = ["one", "two"]

    def export():
        return (
            handler.export(ctx, "classifier")
            if stage == "classifier"
            else helix.export(ctx, helix.inputs(ctx, incidents, "SOURCE"))
        )

    request = exported(export()["saved_paths"])
    prefix = "incidents/" if stage == "helix" else "comments/"
    batches = [v for k, v in request.items() if k.startswith(prefix)]
    assert len(batches) == 2
    assert all(len(encode(batch)) <= 300_000 for batch in batches)
    if stage == "helix":
        incidents.loc[0, "Detailed Description"] = "漢" * 100_001
    else:
        frame.loc[0, "Comment"] = "漢" * 100_001
    with pytest.raises(ValueError, match="no se truncará"):
        export()


def test_classifier_conflicting_jobs_do_not_overwrite_fanout(exchange, monkeypatch):
    handler, ctx, frame = set_comments(exchange, monkeypatch, ["igual", "igual"])
    handler.import_response(ctx, designer_zip(handler, ctx), "designer")
    handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    first = exported(handler.export(ctx, "classifier")["saved_paths"])
    second = exported(handler.export(ctx, "classifier")["saved_paths"])
    response = classifier_files(first["manifest.json"], first)
    handler.import_response(ctx, classifier_zip(response), "classifier")
    response = classifier_files(second["manifest.json"], second)
    response["results"]["000001"]["classifications"][0]["primary"] = "c002"
    with pytest.raises(ValueError, match="respuesta diferente"):
        handler.import_response(ctx, classifier_zip(response), "classifier")
    assert handler.apply(ctx, frame, "DISCOVERED").Palanca.eq("Atención").all()


@pytest.mark.parametrize("mode", ["SOURCE", "COMPLETED", "DISCOVERED"])
def test_empty_assignments_keep_lens_and_catalog_invalidation(exchange, monkeypatch, mode):
    handler, ctx, frame = set_comments(exchange, monkeypatch, ["", None])
    if mode == "SOURCE":
        frame["Palanca"], frame["Subpalanca"] = (
            "Sin clasificación temática",
            "Información insuficiente",
        )
    elif mode == "COMPLETED":
        handler.taxonomy.save_manual(ctx, TAXONOMY["taxonomy"], template="NONE")
    else:
        handler.import_response(ctx, designer_zip(handler, ctx), "designer")
        handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    handler.taxonomy.configure(ctx, {"active": mode})
    assert handler.export(ctx, "classifier")["saved_paths"] == []
    assert handler.progress(ctx)["received"] == 2
    if mode == "SOURCE":
        frame["Palanca"], frame["Subpalanca"] = "Otra", "Categoría"
    elif mode == "COMPLETED":
        handler.taxonomy.save_manual(ctx, TAXONOMY["taxonomy"], template="NONE")
    else:
        changed = copy.deepcopy(TAXONOMY)
        changed["taxonomy"][0]["lever"] = "Otra"
        handler.import_response(ctx, designer_zip(handler, ctx, changed), "designer")
        handler.taxonomy.configure(ctx, {"active": "DISCOVERED"})
    assert handler.progress(ctx)["received"] == 0
