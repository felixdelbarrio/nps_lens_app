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
        "schema_version": "3.0",
        "screens": {
            "dashboard": {"kpis": {"samples": 800, "delta_nps": float("nan")}},
            "linking": {},
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
        assert set(contract["snapshots"]) == {"data"}
        assert contract["manifest"]["size_budget_bytes"] == 25_000
        assert contract["manifest"]["publication_json_bytes"] < 100_000
        assert contract["manifest"]["truncated"] is True
        assert b"Banca de Empresas e Instituciones" in archive.read("newsletter.html")
