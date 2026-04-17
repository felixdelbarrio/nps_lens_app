from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from nps_lens.api.app import create_app
from nps_lens.settings import Settings

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "excel"


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
        log_level="INFO",
    )


def test_api_uploads_and_returns_accumulative_summary(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    march = FIXTURES_DIR / "NPS Térmico Senda - 03Marzo.xlsx"

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


def test_api_returns_clear_failure_for_missing_critical_columns(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    invalid = tmp_path / "invalid.xlsx"
    pd.DataFrame(
        {
            "Fecha": ["2026-03-01"],
            "NPS": [2],
            "Canal": ["Web"],
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
