from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient
from pptx import Presentation

from nps_lens.api.app import create_app
from nps_lens.settings import Settings
from nps_lens.testing.fixtures import fixture_excel


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        database_path=tmp_path / "data" / "dashboard.sqlite3",
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


def _upload_nps_march(client: TestClient) -> dict[str, object]:
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
    return payload


def _upload_nps_jan_feb(client: TestClient) -> dict[str, object]:
    jan_feb = fixture_excel("NPS Térmico Senda - 01Enero-02Febrero.xlsx")
    with jan_feb.open("rb") as handle:
        response = client.post(
            "/api/uploads/nps",
            data={
                "service_origin": "BBVA México",
                "service_origin_n1": "Senda",
                "service_origin_n2": "",
            },
            files={
                "file": (
                    jan_feb.name,
                    handle,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    return payload


def _build_helix_fixture(path: Path) -> Path:
    pd.DataFrame(
        {
            "BBVA_SourceServiceCompany": ["BBVA México", "BBVA México", "BBVA España"],
            "BBVA_SourceServiceN1": ["Senda", "Senda", "Senda"],
            "BBVA_SourceServiceN2": ["", "", ""],
            "CreatedDate": ["2026-03-01", "2026-03-03", "2026-03-04"],
            "Incident Number": ["INC-1", "INC-2", "INC-3"],
            "Record ID": ["RID-1", "RID-2", "RID-3"],
            "Detailed Description": [
                "Cliente no puede acceder al portal",
                "Fallo en autenticacion web",
                "Contexto ajeno",
            ],
            "Short Description": ["Acceso", "Autenticacion", "Otro"],
        }
    ).to_excel(path, index=False)
    return path


def test_dashboard_context_nps_and_dataset_views_are_restored(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    upload = _upload_nps_march(client)

    context_response = client.get(
        "/api/dashboard/context",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
        },
    )
    assert context_response.status_code == 200
    context_payload = context_response.json()
    assert "2026" in context_payload["available_years"]
    assert "03" in context_payload["available_months_by_year"]["2026"]
    assert context_payload["nps_dataset"]["available"] is True
    assert context_payload["nps_dataset"]["rows"] == upload["inserted_rows"]
    assert "Downloads" in context_payload["preferences"]["downloads_path"]
    assert (
        context_payload["preferences"]["helix_base_url"]
        == "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/"
    )
    assert any(
        option["value"] == "executive_journeys"
        for option in context_payload["causal_method_options"]
    )

    dashboard_response = client.get(
        "/api/dashboard/nps",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
            "pop_year": "2026",
            "pop_month": "03",
            "nps_group": "Todos",
        },
    )
    assert dashboard_response.status_code == 200
    dashboard_payload = dashboard_response.json()
    assert dashboard_payload["context_label"]
    assert dashboard_payload["kpis"]["samples"] > 0
    assert dashboard_payload["overview"]["topics_table"] is not None
    assert dashboard_payload["controls"]["dimensions"] == [
        "Palanca",
        "Subpalanca",
        "Canal",
        "UsuarioDecisión",
    ]
    assert "report_markdown" not in dashboard_payload

    data_response = client.get(
        "/api/dashboard/data/nps",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
            "pop_year": "2026",
            "pop_month": "03",
            "nps_group": "Todos",
            "limit": 5,
        },
    )
    assert data_response.status_code == 200
    data_payload = data_response.json()
    assert data_payload["dataset_kind"] == "nps"
    assert data_payload["total_rows"] > 0
    assert "Browser" in data_payload["columns"]
    assert len(data_payload["rows"]) == 5


def test_dashboard_supports_helix_upload_and_contextual_table(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    _upload_nps_march(client)
    helix_fixture = _build_helix_fixture(tmp_path / "helix.xlsx")

    with helix_fixture.open("rb") as handle:
        upload_response = client.post(
            "/api/uploads/helix",
            data={
                "service_origin": "BBVA México",
                "service_origin_n1": "Senda",
                "service_origin_n2": "",
            },
            files={
                "file": (
                    helix_fixture.name,
                    handle,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )

    assert upload_response.status_code == 200
    upload_payload = upload_response.json()
    assert upload_payload["status"] == "completed"
    assert upload_payload["row_count"] == 2
    assert upload_payload["dataset"]["available"] is True

    context_response = client.get(
        "/api/dashboard/context",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
        },
    )
    context_payload = context_response.json()
    assert context_payload["helix_dataset"]["available"] is True
    assert context_payload["helix_dataset"]["rows"] == 2
    assert pd.notna(pd.to_datetime(context_payload["helix_dataset"]["updated_at"], errors="coerce"))

    data_response = client.get(
        "/api/dashboard/data/helix",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
            "limit": 10,
        },
    )
    assert data_response.status_code == 200
    data_payload = data_response.json()
    assert data_payload["dataset_kind"] == "helix"
    assert data_payload["total_rows"] == 2
    assert data_payload["columns"][:8] == [
        "BBVA_SourceServiceCompany",
        "BBVA_SourceServiceN1",
        "BBVA_SourceServiceN2",
        "CreatedDate",
        "Incident Number",
        "Record ID",
        "Detailed Description",
        "Short Description",
    ]
    assert data_payload["rows"][0]["BBVA_SourceServiceN1"] == "Senda"
    assert (
        data_payload["rows"][0]["Incident Number__href"]
        == "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/RID-1"
    )

    linking_response = client.get(
        "/api/dashboard/linking",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
            "pop_year": "2026",
            "pop_month": "03",
            "nps_group": "Todos",
        },
    )
    assert linking_response.status_code == 200
    linking_payload = linking_response.json()
    assert linking_payload["available"] is True
    assert linking_payload["kpis"]["incidents"] == 2
    assert linking_payload["causal_method"]["value"] == "domain_touchpoint"
    assert linking_payload["navigation"][1]["label"] == "Touchpoints afectados por Subpalanca"
    assert "situation" in linking_payload
    assert "entity_summary" in linking_payload
    assert "scenarios" in linking_payload
    assert "deep_dive" in linking_payload
    assert linking_payload["navigation"][3]["label"] == "Análisis de Tópicos de NPS afectados"
    assert linking_payload["deep_dive"]["title"] == "Análisis de Tópicos de NPS afectados"
    assert linking_payload["deep_dive"]["topic_filter"]["default"] == "Todos"
    assert [tab["label"] for tab in linking_payload["deep_dive"]["tabs"]] == [
        "Ranking de hipótesis",
        "Evidence wall",
    ]


def test_dashboard_report_endpoint_returns_a_valid_powerpoint(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    _upload_nps_march(client)
    helix_fixture = _build_helix_fixture(tmp_path / "helix-report.xlsx")

    with helix_fixture.open("rb") as handle:
        upload_response = client.post(
            "/api/uploads/helix",
            data={
                "service_origin": "BBVA México",
                "service_origin_n1": "Senda",
                "service_origin_n2": "",
            },
            files={
                "file": (
                    helix_fixture.name,
                    handle,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )

    assert upload_response.status_code == 200

    report_response = client.get(
        "/api/dashboard/report/pptx",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
            "pop_year": "2026",
            "pop_month": "03",
            "nps_group": "Todos",
            "min_n": 200,
            "min_similarity": 0.25,
            "max_days_apart": 10,
            "touchpoint_source": "domain_touchpoint",
        },
    )

    assert report_response.status_code == 200
    assert (
        report_response.headers["content-type"]
        == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    )
    assert "attachment;" in report_response.headers["content-disposition"]
    assert report_response.headers["x-nps-lens-saved-path"].endswith(".pptx")
    assert Path(report_response.headers["x-nps-lens-saved-path"]).exists()

    presentation = Presentation(BytesIO(report_response.content))
    assert len(presentation.slides) >= 8


def test_dashboard_report_endpoint_respects_selected_period_and_baseline_history(
    tmp_path: Path,
) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    _upload_nps_jan_feb(client)
    _upload_nps_march(client)
    helix_fixture = _build_helix_fixture(tmp_path / "helix-report-periods.xlsx")

    with helix_fixture.open("rb") as handle:
        upload_response = client.post(
            "/api/uploads/helix",
            data={
                "service_origin": "BBVA México",
                "service_origin_n1": "Senda",
                "service_origin_n2": "",
            },
            files={
                "file": (
                    helix_fixture.name,
                    handle,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )

    assert upload_response.status_code == 200

    report_response = client.get(
        "/api/dashboard/report/pptx",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
            "pop_year": "2026",
            "pop_month": "03",
            "nps_group": "Todos",
            "min_n": 50,
            "min_similarity": 0.25,
            "max_days_apart": 10,
            "touchpoint_source": "domain_touchpoint",
        },
    )

    assert report_response.status_code == 200

    presentation = Presentation(BytesIO(report_response.content))
    assert len(presentation.slides) >= 9

    slide_2_texts: list[str] = []
    for shape in presentation.slides[1].shapes:
        if getattr(shape, "has_text_frame", False):
            for paragraph in shape.text_frame.paragraphs:
                slide_2_texts.append(paragraph.text or "")
    slide_2_text = " ".join(slide_2_texts)

    assert "2026-03" in slide_2_text
    assert "2026-01" not in slide_2_text
    assert "2026-02" not in slide_2_text

    all_texts: list[str] = []
    for slide in presentation.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    all_texts.append(paragraph.text or "")

    assert any("3. Qué ha cambiado respecto al pasado" in text for text in all_texts)
