from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Callable
from zipfile import ZipFile

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from pptx import Presentation

from nps_lens.analytics.drivers import compute_nps_from_scores
from nps_lens.api.app import create_app
from nps_lens.domain.helix_links import build_helix_incident_url_lookup, enrich_helix_incident_links
from nps_lens.domain.models import UploadContext
from nps_lens.reports.executive_ppt import BusinessPptResult
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


_LINK_NARRATIVES = (
    "La pantalla de acceso queda en blanco al validar credenciales",
    "La autenticación rechaza la clave con error de validación",
)


def _upload_nps_link_evidence(client: TestClient) -> None:
    """Known narrative pairs for linking tests, independent of the real Excel corpus."""
    data = pd.DataFrame(
        {
            "Fecha": ["2026-03-01", "2026-03-03", "2026-03-04"],
            "ID": ["link-access", "link-auth", "unrelated"],
            "NPS": [2, 3, 1],
            "Comment": [
                *_LINK_NARRATIVES,
                "El depósito de cheques está bloqueado",
            ],
            "Canal": ["Web"] * 3,
            "Palanca": ["Acceso", "Acceso", "Cheques"],
            "Subpalanca": ["Portal", "Autenticación", "Depósito"],
        }
    )
    content = BytesIO()
    data.to_excel(content, index=False)
    response = client.post(
        "/api/uploads/nps",
        data={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
        },
        files={
            "file": (
                "link-evidence.xlsx",
                content.getvalue(),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "completed"


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


def _build_helix_fixture(
    path: Path,
    narratives: tuple[str, str] = (
        "Cliente no puede acceder al portal",
        "Fallo en autenticacion web",
    ),
) -> Path:
    pd.DataFrame(
        {
            "Owner Support Company": ["BBVA México", "BBVA México", "BBVA España"],
            "BBVA_SourceServiceCompany": ["BBVA México", "BBVA México", "BBVA España"],
            "BBVA_SourceServiceN1": ["Senda", "Senda", "Senda"],
            "BBVA_SourceServiceN2": ["", "", ""],
            "CreatedDate": ["2026-03-01", "2026-03-03", "2026-03-04"],
            "Incident Number": ["INC-1", "INC-2", "INC-3"],
            "Record ID": ["RID-1", "RID-2", "RID-3"],
            "Detailed Description": [
                *narratives,
                "Contexto ajeno",
            ],
            "Short Description": ["Acceso", "Autenticacion", "Otro"],
        }
    ).to_excel(path, index=False)
    return path


def _build_helix_out_of_period_fixture(path: Path) -> Path:
    pd.DataFrame(
        {
            "Owner Support Company": ["BBVA México", "BBVA México"],
            "BBVA_SourceServiceCompany": ["BBVA México", "BBVA México"],
            "BBVA_SourceServiceN1": ["Senda", "Senda"],
            "BBVA_SourceServiceN2": ["", ""],
            "CreatedDate": ["2026-02-01", "2026-02-03"],
            "Incident Number": ["INC-FEB-1", "INC-FEB-2"],
            "Record ID": ["RID-FEB-1", "RID-FEB-2"],
            "Detailed Description": list(_LINK_NARRATIVES),
            "Short Description": ["Acceso febrero", "Autenticacion febrero"],
        }
    ).to_excel(path, index=False)
    return path


def _build_helix_mixed_dates_fixture(path: Path) -> Path:
    pd.DataFrame(
        {
            "Owner Support Company": ["BBVA México", "BBVA México", "BBVA México"],
            "BBVA_SourceServiceCompany": ["BBVA México", "BBVA México", "BBVA México"],
            "BBVA_SourceServiceN1": ["Senda", "Senda", "Senda"],
            "BBVA_SourceServiceN2": ["", "", ""],
            "CreatedDate": [
                "2026-03-01T05:00:00+00:00",
                1778052089000,
                "10/03/2026 11:00",
            ],
            "Resolved Date": [
                "2026-03-08T05:00:00+00:00",
                1778656889000,
                None,
            ],
            "Incident Number": ["INC-MIX-1", "INC-MIX-2", "INC-MIX-3"],
            "Record ID": ["RID-MIX-1", "RID-MIX-2", "RID-MIX-3"],
            "Detailed Description": [
                "Cliente no puede acceder al portal",
                "Fallo en autenticacion web",
                "Incidencia sin fecha de cierre",
            ],
            "Short Description": ["Acceso", "Autenticacion", "Sin cierre"],
            "Assigned Support Organization": ["Producto", "Tecnologia", "Operaciones"],
        }
    ).to_excel(path, index=False)
    return path


def _persist_artifact_in_tmp(tmp_path: Path) -> Callable[[bytes, str], Path]:
    def _persist(content: bytes, file_name: str) -> Path:
        target = tmp_path / file_name
        target.write_bytes(content)
        return target

    return _persist


def _ppt_texts(content: bytes) -> list[str]:
    prs = Presentation(BytesIO(content))
    texts: list[str] = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    texts.append(paragraph.text or "")
    return texts


def test_dashboard_context_nps_and_dataset_views_are_restored(tmp_path: Path) -> None:
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
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
    assert "Web" in context_payload["score_channels"]
    assert context_payload["nps_dataset"]["available"] is True
    assert context_payload["nps_dataset"]["rows"] == upload["inserted_rows"]
    assert "Downloads" in context_payload["preferences"]["downloads_path"]
    assert (
        context_payload["preferences"]["helix_base_url"]
        == "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/"
    )
    assert context_payload["preferences"]["report_dimension_analysis"] == "palanca"
    assert context_payload["preferences"]["touchpoint_source"] == "broken_journeys"
    assert any(
        option["value"] == "broken_journeys" for option in context_payload["causal_method_options"]
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
    assert "opportunities" not in dashboard_payload

    def keys(value):
        if isinstance(value, dict):
            return set(value).union(*(keys(item) for item in value.values()))
        if isinstance(value, list):
            return set().union(*(keys(item) for item in value))
        return set()

    assert {"confidence", "priority", "potential_uplift", "causal_score"}.isdisjoint(
        keys(dashboard_payload)
    )
    assert "comparison" not in dashboard_payload
    for gap in dashboard_payload["gaps"]["table"]:
        assert gap["gap_vs_base"] == pytest.approx(
            gap["nps"] - dashboard_payload["gaps"]["base_nps"]
        )
        assert 0 <= gap["detractors"] <= gap["valid_n"] <= gap["n"]
    assert dashboard_payload["context_label"]
    assert dashboard_payload["kpis"]["samples"] > 0
    assert dashboard_payload["kpis"]["neutral_rate"] is not None
    assert "Canal: Todos" in dashboard_payload["context_pills"]
    assert dashboard_payload["gaps"]["base_nps"] is None
    assert dashboard_payload["gaps"]["table"] == []
    assert dashboard_payload["scope"]["cumulative"]["label"].startswith("Datos acumulados hasta")
    assert dashboard_payload["scope"]["cumulative"]["note"].startswith(
        "KPIs agregados para el periodo del "
    )
    assert dashboard_payload["scope"]["historical"]["label"] == "Febrero 2026"
    assert dashboard_payload["overview"]["period_aggregates_figure"] is not None
    assert dashboard_payload["scope"]["period_aggregates"][-1]["label"] == "Marzo 2026"
    assert (
        dashboard_payload["scope"]["period_aggregates"][-1]["nps_average"]
        == dashboard_payload["scope"]["period"]["kpis"]["nps_average"]
    )
    assert dashboard_payload["overview"]["daily_volume_mix_figure"] is not None
    assert dashboard_payload["overview"]["topics_table"] is not None
    assert dashboard_payload["controls"]["dimensions"] == ["Palanca", "Subpalanca"]
    assert "report_markdown" not in dashboard_payload

    records = app.state.repository.load_records_df(
        UploadContext(
            service_origin="BBVA México",
            service_origin_n1="Senda",
            service_origin_n2="",
        )
    )
    scope_records = app.state.dashboard_service._apply_population_filters(
        records,
        "2026",
        "03",
    )
    scores = pd.to_numeric(scope_records["NPS"], errors="coerce").dropna()
    expected_neutral_rate = float(((scores >= 7) & (scores <= 8)).mean())
    assert abs(dashboard_payload["kpis"]["neutral_rate"] - expected_neutral_rate) < 1e-9

    filtered_dashboard_response = client.get(
        "/api/dashboard/nps",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
            "pop_year": "2026",
            "pop_month": "03",
            "score_channel": "Web",
            "nps_group": "Detractores",
        },
    )
    assert filtered_dashboard_response.status_code == 200
    filtered_dashboard_payload = filtered_dashboard_response.json()
    assert filtered_dashboard_payload["kpis"] == dashboard_payload["kpis"]
    assert filtered_dashboard_payload["gaps"] == dashboard_payload["gaps"]

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


def test_channel_only_selects_gap_topics_and_never_changes_nps_calculations(
    tmp_path: Path,
) -> None:
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    _upload_nps_jan_feb(client)
    _upload_nps_march(client)
    params = {
        "service_origin": "BBVA México",
        "service_origin_n1": "Senda",
        "service_origin_n2": "",
        "pop_year": "2026",
        "pop_month": "03",
        "nps_group": "Todos",
        "score_channel": "Web",
        "gap_dimension": "Palanca",
    }

    response = client.get("/api/dashboard/nps", params=params)
    assert response.status_code == 200
    payload = response.json()
    unfiltered = client.get(
        "/api/dashboard/nps",
        params={**params, "score_channel": "Todos"},
    ).json()
    context = UploadContext("BBVA México", "Senda", "")
    records = app.state.dashboard_service._load_nps_df(context)
    current = app.state.dashboard_service._apply_population_filters(records, "2026", "03")

    assert payload["kpis"] == unfiltered["kpis"]
    assert payload["scope"] == unfiltered["scope"]
    assert payload["gaps"]["base_nps"] == unfiltered["gaps"]["base_nps"]
    spans_multiple_channels = False
    for row in payload["gaps"]["table"]:
        topic_population = current.loc[current["Palanca"].eq(row["value"])]
        spans_multiple_channels |= topic_population["Canal"].nunique() > 1
        assert row["n"] == len(topic_population)
        assert row["nps"] == pytest.approx(compute_nps_from_scores(topic_population["NPS"]))
    assert spans_multiple_channels


def test_generate_ppt_report_with_valid_nps_and_no_helix_omits_causal_section(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    _upload_nps_march(client)
    service = app.state.dashboard_service
    monkeypatch.setattr(service, "_persist_artifact", _persist_artifact_in_tmp(tmp_path))

    report = service.generate_ppt_report(
        context=UploadContext(
            service_origin="BBVA México",
            service_origin_n1="Senda",
            service_origin_n2="",
        ),
        pop_year="2026",
        pop_month="03",
        nps_group="Todos",
        score_channel="Web",
    )

    assert report.content
    assert report.slide_count > 0
    texts = _ppt_texts(report.content)
    assert not any("Evidencia Helix ↔ VoC no concluyente" in text for text in texts)
    assert not any("Journeys de detracción" in text for text in texts)


def test_generate_ppt_report_uses_helix_inside_causal_window_across_month_boundary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    _upload_nps_link_evidence(client)
    helix_fixture = _build_helix_out_of_period_fixture(tmp_path / "helix-february.xlsx")
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
    service = app.state.dashboard_service
    monkeypatch.setattr(service, "_persist_artifact", _persist_artifact_in_tmp(tmp_path))

    report = service.generate_ppt_report(
        context=UploadContext(
            service_origin="BBVA México",
            service_origin_n1="Senda",
            service_origin_n2="",
        ),
        pop_year="2026",
        pop_month="03",
        nps_group="Todos",
        score_channel="Web",
    )

    assert report.content
    assert report.slide_count > 6
    params = {
        "service_origin": "BBVA México",
        "service_origin_n1": "Senda",
        "pop_year": "2026",
        "pop_month": "03",
        "score_channel": "Web",
    }
    linked = client.get("/api/dashboard/linking", params=params).json()
    assert linked["diagnostics"]["evidence_pairs"] == 2
    outside = client.get("/api/dashboard/linking", params={**params, "max_days_apart": "10"}).json()
    assert outside["diagnostics"]["evidence_pairs"] == 0
    texts = _ppt_texts(report.content)
    assert not any("Evidencia Helix ↔ VoC no concluyente" in text for text in texts)
    assert any("Journeys rotos" in text for text in texts)


def test_dashboard_supports_helix_upload_and_contextual_table(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    _upload_nps_link_evidence(client)
    helix_fixture = _build_helix_fixture(tmp_path / "helix.xlsx", _LINK_NARRATIVES)

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
        "Owner Support Company",
        "BBVA_SourceServiceCompany",
        "BBVA_SourceServiceN1",
        "BBVA_SourceServiceN2",
        "CreatedDate",
        "Incident Number",
        "Record ID",
        "Detailed Description",
    ]
    assert data_payload["rows"][0]["BBVA_SourceServiceN1"] == "Senda"
    assert (
        data_payload["rows"][0]["Incident Number__href"]
        == "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/RID-1"
    )

    filtered_data_response = client.get(
        "/api/dashboard/data/helix",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
            "pop_year": "2025",
            "pop_month": "12",
            "score_channel": "Web",
            "limit": 10,
        },
    )
    filtered_data_payload = filtered_data_response.json()
    assert filtered_data_payload["total_rows"] == 2
    assert "Causal Match Eligible" in filtered_data_payload["columns"]

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
    assert "Canal: Todos" in linking_payload["context_pills"]
    assert linking_payload["kpis"]["incidents"] == 2
    assert linking_payload["causal_method"]["value"] == "broken_journeys"
    assert linking_payload["navigation"][1]["label"] == "Journeys rotos"
    assert linking_payload["diagnostics"]["helix_quality_eligible"] == 2
    assert linking_payload["diagnostics"]["evidence_pairs"] == 2
    assert "situation" in linking_payload
    assert "narrative" in linking_payload["situation"]
    assert "entity_summary" in linking_payload
    assert "scenarios" in linking_payload
    assert "deep_dive" not in linking_payload
    assert len(linking_payload["navigation"]) == 3
    assert "associations" not in linking_payload["situation"]
    evidence_rows = linking_payload["situation"]["evidence"]["rows"]
    assert {(row["Incident ID"], row["Detractor Comment"]) for row in evidence_rows} == {
        ("INC-1", _LINK_NARRATIVES[0]),
        ("INC-2", _LINK_NARRATIVES[1]),
    }
    assert list(evidence_rows[0]) == [
        "NPS Topic",
        "Incident ID",
        "Incident ID__href",
        "Incident Summary",
        "Detractor Comment",
        "Confianza semántica",
    ]
    identity = linking_payload["scenarios"]["cards"][0]["identity_rows"]
    assert [row["label"] for row in identity] == [
        "Tópico NPS ancla",
        "Organizaciones de las incidencias enlazadas",
    ]
    assert len({row["NPS Topic"] for row in evidence_rows}) <= 10
    assert (
        max(
            sum(row["NPS Topic"] == topic for row in evidence_rows)
            for topic in {row["NPS Topic"] for row in evidence_rows}
        )
        <= 10
    )
    assert linking_payload["scenarios"]["cards"][0]["anchor_topic"]
    assert linking_payload["entity_summary"]["table"][0]["Tópico NPS ancla"]
    serialized_linking = json.dumps(linking_payload, ensure_ascii=False).casefold()
    for removed_field in (
        "score_mean_difference",
        "focus_rate_difference_pp",
        "confidence",
        "priority",
        "causal_score",
        "potential_uplift",
        "nps_points_at_risk",
        "nps_points_recoverable",
        "total_nps_impact",
        "action_lane",
    ):
        assert f'"{removed_field}"' not in serialized_linking


def test_helix_reingestion_keeps_latest_operational_state(tmp_path: Path) -> None:
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    context = {
        "service_origin": "BBVA México",
        "service_origin_n1": "Senda",
        "service_origin_n2": "",
    }

    for status in ("Assigned", "Resolved"):
        source = _build_helix_fixture(tmp_path / f"helix-{status}.xlsx")
        frame = pd.read_excel(source)
        frame["Status"] = status
        frame.to_excel(source, index=False)
        with source.open("rb") as handle:
            response = client.post(
                "/api/uploads/helix",
                data=context,
                files={"file": (source.name, handle, "application/vnd.ms-excel")},
            )
        assert response.status_code == 200
        assert response.json()["status"] == "completed"

    stored = app.state.dashboard_service._load_helix_df(UploadContext(**context))
    assert len(stored) == 2
    assert stored["Status"].eq("Resolved").all()


def test_dashboard_linking_endpoint_does_not_500_with_problematic_helix_dates(
    tmp_path: Path,
) -> None:
    client = TestClient(create_app(_settings(tmp_path)))
    _upload_nps_march(client)
    helix_fixture = _build_helix_mixed_dates_fixture(tmp_path / "helix-mixed-dates.xlsx")

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
    assert upload_response.json()["dataset"]["available"] is True

    linking_response = client.get(
        "/api/dashboard/linking",
        params={
            "service_origin": "BBVA México",
            "service_origin_n1": "Senda",
            "service_origin_n2": "",
            "pop_year": "2026",
            "pop_month": "03",
            "nps_group": "Todos",
            "score_channel": "Web",
        },
    )

    assert linking_response.status_code == 200
    payload = linking_response.json()
    assert "El dataset Helix aún no está cargado" not in payload.get("empty_state", "")


def test_generate_ppt_report_does_not_silently_hide_failed_causal_analysis(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    _upload_nps_march(client)
    helix_fixture = _build_helix_mixed_dates_fixture(tmp_path / "helix-mixed-dates-report.xlsx")
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

    service = app.state.dashboard_service
    monkeypatch.setattr(service, "_persist_artifact", _persist_artifact_in_tmp(tmp_path))

    def _boom(*args, **kwargs):
        raise RuntimeError("causal unavailable")

    monkeypatch.setattr(service, "_compute_linking_core", _boom)

    with pytest.raises(RuntimeError, match="causal unavailable"):
        service.generate_ppt_report(
            context=UploadContext(
                service_origin="BBVA México",
                service_origin_n1="Senda",
                service_origin_n2="",
            ),
            pop_year="2026",
            pop_month="03",
            nps_group="Todos",
            score_channel="Web",
        )


def test_helix_links_resolve_incident_number_through_record_id() -> None:
    base_url = "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/"
    incident_number = "INC000104366753"
    record_id = "IDGH5CDNHIEUEAT2Q5F3T2Q5F3CHLN"
    frame = pd.DataFrame(
        {
            "Incident Number": [incident_number],
            "Record ID": [record_id],
        }
    )

    lookup = build_helix_incident_url_lookup(frame, base_url=base_url)
    expected_url = f"{base_url}{record_id}"

    assert lookup[incident_number] == expected_url
    enriched = enrich_helix_incident_links(frame, base_url=base_url)
    assert enriched.loc[0, "Incident Number__href"] == expected_url


def test_helix_links_build_urls_from_record_id_and_do_not_fallback_to_incident_number() -> None:
    base_url = "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/"
    explicit_url = f"{base_url}EXPLICIT_RECORD"
    frame = pd.DataFrame(
        {
            "Incident Number": ["INC-EXPLICIT", "INC-NO-RECORD"],
            "Record ID": ["RID-SHOULD-NOT-WIN", ""],
            "Incident URL": [explicit_url, base_url],
        }
    )

    lookup = build_helix_incident_url_lookup(frame, base_url=base_url)

    assert lookup["INC-EXPLICIT"] == f"{base_url}RID-SHOULD-NOT-WIN"
    assert "INC-NO-RECORD" not in lookup


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
    # Observed topics paginate; unaccepted evidence never creates case sections.
    topic_pages = [
        slide
        for slide in presentation.slides
        if any(shape.name == "Topic heading" for shape in slide.shapes)
    ]
    assert topic_pages
    assert len(presentation.slides) == 5 + len(topic_pages)
    assert not any(
        shape.name in {"Topic separator", "Scenario metric label"}
        for slide in presentation.slides
        for shape in slide.shapes
    )
    headings = [
        shape.text
        for slide in topic_pages
        for shape in slide.shapes
        if shape.name == "Topic heading"
    ]
    assert len(headings) == len(set(headings))
    assert not any("Journeys rotos" in text for text in _ppt_texts(report_response.content))


def test_publication_embeds_the_executive_report_with_causal_slides(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app = create_app(_settings(tmp_path))
    client = TestClient(app)
    _upload_nps_march(client)
    service = app.state.dashboard_service
    captured: dict[str, object] = {}
    dashboard_request: dict[str, object] = {}
    linking_request: dict[str, object] = {}
    dataset_requests: dict[str, dict[str, object]] = {}

    def _dashboard(**kwargs):
        dashboard_request.update(kwargs)
        return {"kpis": {}}

    monkeypatch.setattr(service, "nps_dashboard", _dashboard)

    def _linking(**kwargs):
        linking_request.update(kwargs)
        return {
            "available": True,
            "kpis": {
                "top3_incident_share": 0.7,
                "median_lag_weeks": 1.0,
            },
            "entity_summary": {
                "kpis": [{"label": "Journeys de detracción", "value": "1"}],
                "table": [{"nps_topic": "Acceso", "linked_pairs": 4}],
            },
            "scenarios": {
                "cards": [
                    {
                        "rank": 1,
                        "nps_topic": "Acceso bloqueado",
                        "linked_pairs": 4,
                        "focus_rate_high_incidence": 0.6,
                    }
                ]
            },
        }

    monkeypatch.setattr(
        service,
        "linking_dashboard",
        _linking,
    )

    def _dataset(*, dataset_kind, **kwargs):
        dataset_requests[dataset_kind] = kwargs
        return {"dataset_kind": dataset_kind, "rows": []}

    monkeypatch.setattr(service, "dataset_rows", _dataset)

    def _executive_report(**kwargs):
        captured.update(kwargs)
        return BusinessPptResult(
            "informe-ejecutivo.pptx",
            b"EXECUTIVE",
            10,
            compact_file_name="informe-ejecutivo-sin-evolucion-nps.pptx",
            compact_content=b"COMPACT",
        )

    monkeypatch.setattr(service, "generate_ppt_report", _executive_report)

    artifact = service.generate_publication(
        context=UploadContext("BBVA México", "Senda", ""),
        pop_year="2026",
        pop_month="03",
        nps_group="Promotores",
    )

    assert captured["touchpoint_source"] == "broken_journeys"
    assert captured["report_dimension_analysis"] == ""
    assert dashboard_request["nps_group"] == "Promotores"
    assert dashboard_request["score_channel"] == "Todos"
    assert linking_request["nps_group"] == "Todos"
    assert linking_request["score_channel"] == "Todos"
    assert captured["nps_group"] == "Todos"
    assert dataset_requests["nps"]["nps_group"] == "Promotores"
    assert dataset_requests["nps"]["score_channel"] == "Todos"
    with ZipFile(BytesIO(artifact.content)) as archive:
        assert archive.read("informe-ejecutivo.pptx") == b"EXECUTIVE"
        assert archive.read("informe-ejecutivo-sin-evolucion-nps.pptx") == b"COMPACT"
        assert b"presentaci\xc3\xb3n ejecutiva" in archive.read("newsletter.html")
        publication = json.loads(archive.read("publication.json"))
        assert publication["static_views"] == {
            "default": {"nps_group": "Promotores", "score_channel": "Todos"},
            "immutable": True,
        }
        assert "comments" in publication["screens"]
        comment_controls = publication["screens"]["comments"]["controls"]
        assert comment_controls["defaults"] == {
            "channel": "Todos",
            "group": "Detractores",
            "dimension": "Palanca",
        }
        assert set(publication["screens"]["comments"]["gaps"]["Web"]) == {
            "Palanca",
            "Subpalanca",
        }
        assert "comparison" not in publication["screens"]["dashboard"]
        assert "gaps" not in publication["screens"]["dashboard"]
        assert "topics_table" not in publication["screens"]["dashboard"]["overview"]
        assert "deep_dive" not in publication["screens"]["linking"]
        assert publication["filters"]["causal_nps_group"] == "Todos"
        assert publication["manifest"]["report_without_evolution"] == (
            "informe-ejecutivo-sin-evolucion-nps.pptx"
        )
        assert "equivalence_registry" not in publication["manifest"]
    assert client.get("/api/dashboard/report/exclusive.pptx").status_code == 404


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
    assert len(presentation.slides) >= 6

    slide_2_texts: list[str] = []
    for shape in presentation.slides[1].shapes:
        if getattr(shape, "has_text_frame", False):
            for paragraph in shape.text_frame.paragraphs:
                slide_2_texts.append(paragraph.text or "")
    slide_2_text = " ".join(slide_2_texts)

    assert "balance acumulado histórico" in slide_2_text.lower()
    assert "2026-01-01" in slide_2_text
    assert "base histórica (2026-01-01 -> 2026-02-22)" in slide_2_text

    all_texts: list[str] = []
    for slide in presentation.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False):
                for paragraph in shape.text_frame.paragraphs:
                    all_texts.append(" ".join((paragraph.text or "").split()))

    assert any("lidera el deterioro entre los tópicos observados" in text for text in all_texts)
    assert not any("Qué ha cambiado en Subpalanca" in text for text in all_texts)


def test_views_share_canonical_evidence(tmp_path, monkeypatch):
    from nps_lens.analytics.incident_attribution import (
        TOUCHPOINT_SOURCE_DOMAIN,
        TOUCHPOINT_SOURCE_PALANCA,
    )
    from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
    from nps_lens.services.dashboard_service import DashboardService

    settings = _settings(tmp_path)
    service = DashboardService(SqliteNpsRepository(settings.database_path), settings)
    context = UploadContext("BBVA México", "Senda")
    frame = pd.DataFrame(
        {
            "_business_key": ["a"],
            "ID": ["a"],
            "Fecha": pd.to_datetime(["2026-03-01"]),
            "NPS": [2],
            "Comment": ["transferencias rechazadas"],
            "Canal": ["Web"],
            "Palanca": ["Pagos"],
            "Subpalanca": ["Transferencias"],
        }
    )
    frame.attrs["classification_signature"] = "source-artifact"
    helix = pd.DataFrame(
        {
            "Incident Number": ["i"],
            "Fecha": pd.to_datetime(["2026-03-01"]),
            "summary": ["transferencias rechazadas"],
        }
    )
    monkeypatch.setattr(service, "_load_nps_df", lambda _: frame)
    monkeypatch.setattr(service, "_load_helix_df", lambda *a, **kw: helix)
    monkeypatch.setattr(service, "analysis_engine", lambda *a, **kw: {"selected_engine": "rules"})
    calls = []
    original = service._compute_linking_core

    def counted(**kwargs):
        calls.append(1)
        return original(**kwargs)

    monkeypatch.setattr(service, "_compute_linking_core", counted)
    args = dict(
        context=context,
        pop_year="2026",
        pop_month="03",
        score_channel="Todos",
        min_similarity=0.15,
        max_days_apart=90,
    )
    first = service._causal_analysis_bundle(**args, touchpoint_source=TOUCHPOINT_SOURCE_PALANCA)
    second = service._causal_analysis_bundle(**args, touchpoint_source=TOUCHPOINT_SOURCE_DOMAIN)
    assert first["ready"] and second["ready"]
    assert len(calls) == 1
    assert first["core"] is second["core"]
    assert set(first["core"]["links_df"]["classification_signature"]) == {"source-artifact"}
