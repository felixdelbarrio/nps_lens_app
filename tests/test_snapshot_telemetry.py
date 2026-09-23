from __future__ import annotations

import hashlib
import json
from io import BytesIO
from zipfile import ZipFile

from nps_lens.core.telemetry import TelemetryCollector
from nps_lens.platform.publication import build_publication_archive, build_static_data_snapshot


def test_telemetry_is_bounded_and_does_not_export_payloads() -> None:
    collector = TelemetryCollector(max_events=100)
    for index in range(140):
        collector.record(
            method="GET",
            route="/api/dashboard/nps",
            status=200,
            duration_ms=index,
            cpu_ms=1.5,
            response_bytes=42,
        )
    payload = json.loads(collector.to_json_bytes())
    assert len(payload["events"]) == 100
    assert payload["privacy"]["query_parameters_recorded"] is False
    assert "query" not in payload["events"][0]
    assert "body" not in payload["events"][0]


def test_publication_is_self_contained_and_never_exceeds_budget() -> None:
    rows = [
        {
            "id": index,
            "comment": hashlib.sha256(str(index).encode()).hexdigest() * 10,
        }
        for index in range(800)
    ]
    snapshot = build_static_data_snapshot(
        {"columns": ["id", "comment"], "rows": list(rows)},
        {"columns": ["id", "comment"], "rows": list(rows)},
        page_size=20,
    )
    publication: dict[str, object] = {
        "schema_version": "5.0",
        "screens": {
            "dashboard": {"kpis": {"samples": 800, "delta_nps": float("nan")}},
            "linking": {"diagnostics": {"nps_total": 800, "nps_non_matchable": 123}},
            "data": {"nps": {"deferred": True}, "helix": {"deferred": True}},
        },
        "snapshots": {
            "data": snapshot,
            "taxonomy": {"records": "unused" * 300_000},
        },
        "manifest": {},
    }
    assert set(snapshot["datasets"]["nps"]) == {"columns", "total_rows", "page"}
    assert len(snapshot["datasets"]["nps"]["columns"]) <= 14
    assert len(snapshot["datasets"]["helix"]["columns"]) <= 14
    artifact = build_publication_archive(
        publication,
        report_name="informe.pptx",
        report_content=b"PPTX" * 100,
        compact_report_name="informe-sin-evolucion-nps.pptx",
        compact_report_content=b"COMPACT" * 100,
        file_name="publication.zip",
        max_bytes=25_000,
    )
    assert artifact.size_bytes <= 25_000
    with ZipFile(BytesIO(artifact.content)) as archive:
        assert set(archive.namelist()) == {
            "publication.json",
            "newsletter.html",
            "informe.pptx",
            "informe-sin-evolucion-nps.pptx",
        }
        publication_json = archive.read("publication.json")
        assert b"NaN" not in publication_json
        assert b"Infinity" not in publication_json
        contract = json.loads(publication_json)
        assert contract["screens"]["dashboard"]["kpis"]["delta_nps"] is None
        assert contract["screens"]["linking"]["diagnostics"]["nps_non_matchable"] == 123
        assert set(contract["snapshots"]) == {"data"}
        assert contract["manifest"]["size_budget_bytes"] == 25_000
        assert contract["manifest"]["publication_json_bytes"] < 100_000
        assert contract["manifest"]["truncated"] is True
        assert b"Banca de Empresas e Instituciones" in archive.read("newsletter.html")


def test_wide_exports_retain_ingestion_provenance_in_publication() -> None:
    extra = {f"raw_{index}": index for index in range(30)}
    nps = {
        **extra,
        "ID": "opinion-42",
        "Comment": "",
        "NPS": 4,
        "match_status": "non_matchable",
        "_source_id": "duplicate-id",
        "_identity_source": "Opinion Identifier",
    }
    helix = {
        **extra,
        "Incident Number": "INC42",
        "_source_sheet": "Argentina",
        "incident_occurred_at": "2026-09-01",
        "incident_registered_at": "2026-09-03",
        "incident_occurred_at_source": "bbva_startdatetime",
    }
    snapshot = build_static_data_snapshot(
        {"columns": list(nps), "rows": [nps], "total_rows": 1000},
        {"columns": list(helix), "rows": [helix]},
    )
    for kind, original in [("nps", nps), ("helix", helix)]:
        dataset = snapshot["datasets"][kind]
        assert len(dataset["columns"]) == 14
        published = dataset["page"]["rows"][0]
        for key, value in original.items():
            if not key.startswith("raw_"):
                assert published[key] == value
    assert snapshot["datasets"]["nps"]["total_rows"] == 1000
