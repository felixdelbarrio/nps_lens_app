from __future__ import annotations

import io
import json
import zipfile
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from nps_lens.api.app import create_app
from nps_lens.domain.models import UploadContext
from nps_lens.services.taxonomy_discovery import TaxonomyDiscoveryError
from nps_lens.services.taxonomy_exchange import TaxonomyExchange, encode, read_zip
from nps_lens.services.taxonomy_prompts import FALLBACK_LEVER, FALLBACK_SUBLEVERS
from nps_lens.settings import Settings

TAXONOMY = {
    "taxonomy": [
        {"lever": "Atención", "sublevers": ["Resolución"]},
        {"lever": FALLBACK_LEVER, "sublevers": list(FALLBACK_SUBLEVERS)},
    ]
}


def zipped(files):
    result = io.BytesIO()
    with zipfile.ZipFile(result, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in files.items():
            archive.writestr(name, encode(payload))
    return result.getvalue()


def exported(path):
    with zipfile.ZipFile(path) as archive:
        return {
            name: json.loads(archive.read(name))
            for name in archive.namelist()
            if name.endswith(".json")
        }


@pytest.fixture
def exchange(tmp_path, monkeypatch):
    settings = replace(
        Settings.from_env(),
        database_path=tmp_path / "test.db",
        data_dir=tmp_path,
        equivalences_path=tmp_path / "equivalences.json",
        auth_mode="local",
    )
    app = create_app(settings)
    service = app.state.dashboard_service.taxonomy
    frame = pd.DataFrame(
        {
            "_business_key": [f"private-{i:05d}" for i in range(405)],
            "Comment": ["No resuelven mi problema. á漢字 " + str(i) for i in range(405)],
            "Palanca": "",
            "Subpalanca": "",
            "Canal": "Web",
        }
    )
    monkeypatch.setattr(service, "source", lambda context: frame.copy())
    # API Downloads writes are isolated; never pollute real user downloads in tests.
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    return (
        TaxonomyExchange(service, tmp_path / "Downloads"),
        UploadContext("Bank", "Web", ""),
        frame,
        TestClient(app),
    )


def classifier_files(manifest, inputs):
    return {
        "manifest.json": manifest,
        **{
            name.replace("comments/", "results/"): {
                "classifications": [
                    {
                        "id": row["id"],
                        "primary_classification": {"lever": "Atención", "sublever": "Resolución"},
                    }
                    for row in payload["comments"]
                ]
            }
            for name, payload in inputs.items()
            if name.startswith("comments/")
        },
    }


def test_zip_api_roundtrip_restart_partial_atomic_and_idempotent(exchange):
    handler, context, frame, client = exchange
    params = {"service_origin": "Bank", "service_origin_n1": "Web"}
    response = client.post("/api/taxonomy/discovery/export", params=params)
    assert response.status_code == 200, response.text
    request = exported(response.json()["saved_path"])
    assert request["manifest.json"]["stage"] == "designer"
    assert len(request["manifest.json"]["batches"]) == 3
    sent = [
        row
        for name, payload in request.items()
        if name.startswith("comments/")
        for row in payload["comments"]
    ]
    assert [row["Comment"] for row in sent] == frame["Comment"].tolist()
    assert all(set(row) == {"id", "Comment"} for row in sent)
    assert "private-" not in json.dumps(request)
    designer = zipped({"manifest.json": request["manifest.json"], "taxonomy.json": TAXONOMY})
    result = client.post(
        "/api/taxonomy/discovery/import",
        params=params,
        files={"file": ("response.zip", designer, "application/zip")},
    )
    assert result.status_code == 200, result.text
    classification = exported(result.json()["saved_path"])
    assert classification["taxonomy.json"] == TAXONOMY
    files = classifier_files(classification["manifest.json"], classification)
    partial = {
        "manifest.json": files["manifest.json"],
        "results/000001.json": files["results/000001.json"],
    }
    first = handler.import_response(context, zipped(partial))
    assert first["received"] == 1 and first["stage"] == "classifier"
    assert "DISCOVERED" not in handler.taxonomy.state(context)["artifacts"]
    handler = TaxonomyExchange(handler.taxonomy, handler.downloads)
    assert handler.import_response(context, zipped(partial))["received"] == 1
    broken = dict(files)
    broken["results/000003.json"] = {"classifications": []}
    with pytest.raises(ValueError):
        handler.import_response(context, zipped(broken))
    assert handler.import_response(context, zipped(partial))["received"] == 1
    complete = handler.import_response(context, zipped(files))
    assert complete["stage"] == "complete" and complete["received"] == 3
    assert handler.import_response(context, zipped(files))["stage"] == "complete"
    resolved = handler.taxonomy.resolve(context, frame, "DISCOVERED")
    assert len(resolved) == 405
    assert resolved["Palanca"].eq("Atención").all()
    assert resolved["Subpalanca"].eq("Resolución").all()


def test_changed_corpus_and_foreign_job_rejected(exchange):
    handler, context, frame, _ = exchange
    original = exported(handler.export(context)["saved_path"])
    response = {"manifest.json": original["manifest.json"], "taxonomy.json": TAXONOMY}
    with pytest.raises(ValueError, match="dataset"):
        handler.import_response(UploadContext("Other", "Web", ""), zipped(response))
    frame.loc[0, "Comment"] = "Changed"
    with pytest.raises(ValueError, match="corpus"):
        handler.import_response(context, zipped(response))


@pytest.mark.parametrize(
    "name", ["../taxonomy.json", "/taxonomy.json", "a\\b.json", "a//b.json", "script.py", "folder/"]
)
def test_unsafe_zip_names(name):
    with pytest.raises(ValueError):
        read_zip(zipped({name: {}}))


def test_duplicate_json_and_duplicate_members():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("manifest.json", '{"a":1,"a":2}')
    with pytest.raises(ValueError, match="duplicadas"):
        read_zip(output.getvalue())
    with pytest.warns(UserWarning), zipfile.ZipFile(output, "a") as archive:
        archive.writestr("manifest.json", "{}")
    with pytest.raises(ValueError, match="duplicados"):
        read_zip(output.getvalue())


def test_invalid_taxonomy_and_unknown_category_leave_state_unchanged(exchange):
    handler, context, _, _ = exchange
    request = exported(handler.export(context)["saved_path"])
    with pytest.raises((ValueError, TaxonomyDiscoveryError)):
        handler.import_response(
            context,
            zipped({"manifest.json": request["manifest.json"], "taxonomy.json": {"taxonomy": []}}),
        )
    result = handler.import_response(
        context, zipped({"manifest.json": request["manifest.json"], "taxonomy.json": TAXONOMY})
    )
    inputs = exported(result["saved_path"])
    files = classifier_files(inputs["manifest.json"], inputs)
    files["results/000001.json"]["classifications"][0]["primary_classification"][
        "lever"
    ] = "Inventada"
    with pytest.raises(Exception):
        handler.import_response(context, zipped(files))
    assert "DISCOVERED" not in handler.taxonomy.state(context)["artifacts"]
