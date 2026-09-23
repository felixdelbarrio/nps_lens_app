from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from nps_lens.api.app import create_app
from nps_lens.settings import Settings
from nps_lens.testing.fixtures import fixture_excel


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        database_path=tmp_path / "data" / "api.sqlite3",
        frontend_dist_dir=tmp_path / "frontend-dist",
        frontend_public_dir=tmp_path / "frontend-public",
        api_host="127.0.0.1",
        api_port=8000,
        default_service_origin="BBVA México",
        default_service_origin_n1="Senda",
        allowed_service_origins=["BBVA México"],
        allowed_service_origin_n1={"BBVA México": ["Senda"]},
        column_aliases_path=tmp_path / "data" / "config" / "nps_column_aliases.json",
        default_downloads_path=str(tmp_path / "downloads"),
        log_level="INFO",
    )


def test_project_instructions_are_local_read_only(tmp_path):
    from nps_lens.services.taxonomy_prompts import INSTRUCTIONS_VERSION, PROJECT_INSTRUCTIONS

    with TestClient(create_app(_settings(tmp_path))) as client:
        response = client.get("/api/taxonomy/discovery/instructions")
        assert client.post("/api/taxonomy/discovery/connect").status_code == 404
    assert response.status_code == 200
    assert response.json() == {"version": INSTRUCTIONS_VERSION, **PROJECT_INSTRUCTIONS}
    assert "manifest.json" in response.json()["designer"]


def test_telemetry_excludes_unknown_paths_and_parameter_values(tmp_path: Path) -> None:
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    client.get("/api/unknown/confidential-customer?email=private@example.com")
    client.get("/api/dashboard/data/confidential-dataset")
    payload = app.state.telemetry.to_json_bytes()
    assert b"confidential-customer" not in payload
    assert b"private@example.com" not in payload
    assert b"confidential-dataset" not in payload
    assert b"<unmatched>" in payload
    assert b"/api/dashboard/data/{dataset_kind}" in payload


def test_api_uploads_and_returns_accumulative_summary(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    march = fixture_excel("NPS Térmico Senda - 03Marzo.xlsx")

    with march.open("rb") as handle:
        response = client.post(
            "/api/uploads/nps",
            data={
                "service_origin": "BBVA México",
                "service_origin_n1": "Senda",
                "service_origin_n2": "",
            },
            files={
                "file": (
                    march.name,
                    handle,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    assert any(issue["code"] == "extra_columns_detected" for issue in payload["issues"])

    summary = client.get("/api/summary").json()
    uploads = client.get("/api/uploads").json()

    assert summary["total_records"] == payload["inserted_rows"] + payload["updated_rows"]
    assert uploads[0]["upload_id"] == payload["upload_id"]


def test_api_replaces_a_duplicate_upload_on_explicit_request(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    march = fixture_excel("NPS Térmico Senda - 03Marzo.xlsx")
    upload_ids: list[str] = []
    for _ in range(2):
        with march.open("rb") as handle:
            response = client.post(
                "/api/uploads/nps",
                data={
                    "service_origin": "BBVA México",
                    "service_origin_n1": "Senda",
                    "service_origin_n2": "",
                },
                files={"file": (march.name, handle, "application/vnd.ms-excel")},
            )
        assert response.status_code == 200
        upload_ids.append(response.json()["upload_id"])

    before = client.get("/api/summary").json()["total_records"]
    response = client.post(f"/api/uploads/nps/{upload_ids[1]}/replace")

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert client.get("/api/summary").json()["total_records"] == before
    uploads = {upload["upload_id"]: upload for upload in client.get("/api/uploads").json()}
    assert uploads[upload_ids[0]]["status"] == "replaced"


def test_api_returns_clear_failure_for_missing_critical_columns(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    invalid = tmp_path / "invalid.xlsx"
    pd.DataFrame(
        {
            "Fecha": ["2026-03-01"],
            "NPS": [2],
            "Comment": ["Falta el canal"],
        }
    ).to_excel(invalid, index=False)

    with invalid.open("rb") as handle:
        response = client.post(
            "/api/uploads/nps",
            data={
                "service_origin": "BBVA México",
                "service_origin_n1": "Senda",
                "service_origin_n2": "",
            },
            files={
                "file": (
                    invalid.name,
                    handle,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "failed"
    assert any(issue["code"] == "missing_required_column" for issue in payload["issues"])


def test_api_deletes_one_nps_ingestion_and_all_owner_data(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    source = tmp_path / "nps.xlsx"
    pd.DataFrame(
        {
            "Fecha": ["2026-09-01"],
            "NPS": [9],
            "Canal": ["App"],
            "Comment": ["Correcto"],
        }
    ).to_excel(source, index=False)

    with source.open("rb") as handle:
        upload = client.post(
            "/api/uploads/nps",
            data={"service_origin": "BBVA México"},
            files={"file": (source.name, handle, "application/vnd.ms-excel")},
        ).json()
    assert client.get("/api/summary", params={"service_origin": "BBVA México"}).json()[
        "total_records"
    ] == 1

    deleted = client.delete(
        f"/api/uploads/nps/{upload['upload_id']}",
        params={"service_origin": "BBVA México"},
    )
    assert deleted.status_code == 200
    assert deleted.json()["removed_records"] == 1

    cleared = client.delete("/api/data", params={"service_origin": "BBVA México"})
    assert cleared.status_code == 200
    assert cleared.json()["owner_support_company"] == "BBVA México"


def test_api_deletes_a_specific_incident_ingestion(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    source = tmp_path / "helix.xlsx"
    pd.DataFrame(
        {
            "Owner Support Company": ["BBVA México"],
            "BBVA_SourceServiceN1": ["WEB"],
            "Submit Date": ["2026-09-01"],
            "Record ID": ["R1"],
        }
    ).to_excel(source, index=False)

    with source.open("rb") as handle:
        upload = client.post(
            "/api/uploads/helix",
            data={"service_origin": "BBVA México"},
            files={"file": (source.name, handle, "application/vnd.ms-excel")},
        ).json()
    assert upload["status"] == "completed"
    assert len(
        client.get(
            "/api/uploads/helix", params={"service_origin": "BBVA México"}
        ).json()
    ) == 1

    deleted = client.delete(
        f"/api/uploads/helix/{upload['upload_id']}",
        params={"service_origin": "BBVA México"},
    )
    assert deleted.status_code == 200
    assert deleted.json()["removed_uploads"] == 1
    assert client.get(
        "/api/uploads/helix", params={"service_origin": "BBVA México"}
    ).json() == []


def test_nps_column_alias_settings_are_validated_and_persisted(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings))
    company_context = {"service_origin": "BBVA México"}
    original = client.get("/api/settings/nps-column-aliases", params=company_context)
    assert original.status_code == 200
    payload = original.json()
    canal = next(field for field in payload["fields"] if field["canonical"] == "Canal")
    canal["aliases"].append("Touch point")

    saved = client.put("/api/settings/nps-column-aliases", params=company_context, json=payload)
    assert saved.status_code == 200
    assert settings.column_aliases_path.exists()
    assert (
        client.get("/api/settings/nps-column-aliases", params=company_context).json() == saved.json()
    )
    assert saved.json()["service_origin_n1"] == ""

    canal["aliases"].append("touch-point")
    rejected = client.put("/api/settings/nps-column-aliases", params=company_context, json=payload)
    assert rejected.status_code == 400
    assert "duplicado" in rejected.json()["detail"]


def test_gcp_iap_domain_and_admin_boundaries(tmp_path: Path) -> None:
    settings = replace(
        _settings(tmp_path),
        auth_mode="gcp_iap",
        allowed_email_domain="bbva.com",
        admin_emails=("admin@bbva.com",),
    )
    client = TestClient(create_app(settings))
    assert client.get("/api/summary").status_code == 403
    outsider = {"X-Goog-Authenticated-User-Email": "accounts.google.com:user@example.com"}
    assert client.get("/api/summary", headers=outsider).status_code == 403

    viewer = {"X-Goog-Authenticated-User-Email": "accounts.google.com:user@bbva.com"}
    assert client.get("/api/summary", headers=viewer).status_code == 200
    assert client.get("/api/settings/equivalences", headers=viewer).status_code == 403

    admin = {"X-Goog-Authenticated-User-Email": "accounts.google.com:admin@bbva.com"}
    assert client.get("/api/settings/equivalences", headers=admin).status_code == 200
    telemetry = client.get("/api/telemetry/export", headers=admin)
    assert telemetry.status_code == 200
    saved_path = Path(telemetry.headers["x-nps-lens-saved-path"])
    assert saved_path.parent == tmp_path / "downloads"
    assert saved_path.name.startswith("nps-lens-telemetria-")
    assert saved_path.suffix == ".json"
    assert saved_path.read_bytes() == telemetry.content
