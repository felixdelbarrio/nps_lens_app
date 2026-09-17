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


def test_application_shutdown_disconnects_discovery(tmp_path: Path, monkeypatch) -> None:
    from unittest.mock import Mock

    app = create_app(_settings(tmp_path))
    disconnect = Mock()
    monkeypatch.setattr(app.state.dashboard_service.taxonomy, "disconnect_discovery", disconnect)
    with TestClient(app) as client:
        assert client.get("/api/health").status_code == 200
        disconnect.assert_not_called()
    disconnect.assert_called_once()


def test_discovery_provider_survives_unrelated_settings_refresh(tmp_path: Path) -> None:
    settings = replace(_settings(tmp_path), taxonomy_discovery_method="chatgpt_browser")
    app = create_app(settings)
    dashboard = app.state.dashboard_service
    provider = dashboard.taxonomy.discovery_provider
    dashboard.refresh_taxonomy_discovery()
    assert dashboard.taxonomy.discovery_provider is provider
    assert provider.session_status() == "not_connected"


def test_local_discovery_never_constructs_browser(tmp_path: Path, monkeypatch) -> None:
    from unittest.mock import Mock

    browser = Mock(side_effect=AssertionError("must not open Chrome"))
    monkeypatch.setattr("nps_lens.services.dashboard_service.ChatGPTBrowserClient", browser)
    app = create_app(replace(_settings(tmp_path), taxonomy_discovery_method="local"))
    with TestClient(app) as client:
        assert client.post("/api/taxonomy/discovery/connect").status_code == 400
    browser.assert_not_called()


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


def test_nps_column_alias_settings_are_validated_and_persisted(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = TestClient(create_app(settings))
    senda_context = {"service_origin": "BBVA México", "service_origin_n1": "Senda"}
    web_context = {"service_origin": "BBVA México", "service_origin_n1": "Web"}
    original = client.get("/api/settings/nps-column-aliases", params=senda_context)
    assert original.status_code == 200
    payload = original.json()
    canal = next(field for field in payload["fields"] if field["canonical"] == "Canal")
    canal["aliases"].append("Touch point")

    saved = client.put("/api/settings/nps-column-aliases", params=senda_context, json=payload)
    assert saved.status_code == 200
    assert settings.column_aliases_path.exists()
    assert (
        client.get("/api/settings/nps-column-aliases", params=senda_context).json() == saved.json()
    )
    other_context = client.get("/api/settings/nps-column-aliases", params=web_context).json()
    assert "Touch point" not in next(
        field["aliases"] for field in other_context["fields"] if field["canonical"] == "Canal"
    )
    assert other_context["service_origin_n1"] == "Web"

    canal["aliases"].append("touch-point")
    rejected = client.put("/api/settings/nps-column-aliases", params=senda_context, json=payload)
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
