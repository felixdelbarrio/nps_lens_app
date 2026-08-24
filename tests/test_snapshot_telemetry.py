from __future__ import annotations

import hashlib
import json
from io import BytesIO
from zipfile import ZipFile

from nps_lens.core.telemetry import TelemetryCollector
from nps_lens.platform.publication import build_publication_archive


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
    publication: dict[str, object] = {
        "screens": {
            "dashboard": {"kpis": {"samples": 800}},
            "linking": {},
            "data": {
                "nps": {"rows": list(rows)},
                "helix": {"rows": list(rows)},
            },
        },
        "manifest": {},
    }
    artifact = build_publication_archive(
        publication,
        report_name="informe.pptx",
        report_content=b"PPTX" * 100,
        file_name="publication.zip",
        max_bytes=25_000,
    )
    assert artifact.size_bytes <= 25_000
    with ZipFile(BytesIO(artifact.content)) as archive:
        assert set(archive.namelist()) == {
            "publication.json",
            "newsletter.html",
            "informe.pptx",
        }
        contract = json.loads(archive.read("publication.json"))
        assert contract["manifest"]["size_budget_bytes"] == 25_000
        assert contract["manifest"]["truncated"] is True
        assert b"Banca de Empresas e Instituciones" in archive.read("newsletter.html")
