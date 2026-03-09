from __future__ import annotations

from nps_lens.llm.insight_response import validate_insight_response


def test_validate_insight_response_ok_and_normalizes_ranges():
    obj = {
        "schema_version": "1.0",
        "insight_id": "bbva-be-2026w10-test-001",
        "title": "Un insight",
        "executive_summary": "Resumen",
        "confidence": 1.7,  # should clamp to 1.0
        "severity": 9,  # should clamp to 5
        "journey_route": "palanca>sub",
        "segments_most_affected": ["seg1"],
        "root_causes": [
            {
                "cause": "Causa concreta",
                "why": "Mecanismo",
                "evidence": {
                    "quant": [{"metric": "NPS", "value": "8.5", "context": "Periodo actual"}],
                    "qual": ["dato1"],
                },
                "assumptions": ["as1"],
                "actions": [{"action": "Fix", "owner": "Tech", "eta": "2w"}],
                "tests_or_checks": ["Validar muestra"],
            }
        ],
        "assumptions": ["a"],
        "risks": ["r"],
        "next_questions": ["q"],
        "tags": {
            "geo": "mx",
            "channel": "mobile",
            "lever": "unknown",
            "sublever": "unknown",
            "period": "2026w10",
            "route_signature": "test",
        },
    }

    ok, errs, norm = validate_insight_response(obj)
    assert ok
    assert errs == []
    assert norm is not None
    assert norm["confidence"] == 1.0
    assert norm["severity"] == 5
    assert norm["root_causes"][0]["evidence"]["qual"] == ["dato1"]
    assert norm["tags"]["channel"] == "mobile"


def test_validate_insight_response_missing_fields():
    ok, errs, norm = validate_insight_response({"schema_version": "1.0"})
    assert not ok
    assert norm is None
    assert errs
