"""Fail closed on metric or classification-context contradictions before rendering."""

from __future__ import annotations

import math
from typing import Any, Hashable, Iterable, Mapping


class ReportCoherenceError(ValueError):
    pass


def validate_classification_context(
    active_fingerprint: str,
    artifacts: Mapping[str, Mapping[str, Any]],
    *,
    expected_scope: Mapping[str, Any],
    causal_engine: str,
) -> None:
    for name, artifact in artifacts.items():
        if artifact.get("taxonomy_fingerprint") != active_fingerprint:
            raise ReportCoherenceError(
                f"Taxonomía incompatible en {name}; regenera su clasificación con la taxonomía activa."
            )
        if artifact.get("scope") != expected_scope:
            raise ReportCoherenceError(
                f"Ámbito incompatible en {name}; regenera la clasificación de compañía/canal/periodo."
            )
        if artifact.get("causal_engine") != causal_engine:
            raise ReportCoherenceError(
                f"Motor causal incompatible en {name}; regenera el análisis."
            )


def assert_metric_equal(headline: Any, table: Any, name: str) -> None:
    if headline is None and table is None:
        return
    try:
        a, b = float(headline), float(table)
        if math.isnan(a) and math.isnan(b):
            return
        if math.isfinite(a) and math.isfinite(b) and math.isclose(a, b, abs_tol=1e-8, rel_tol=1e-8):
            return
    except (TypeError, ValueError):
        pass
    raise ReportCoherenceError(
        f"Métrica incoherente ({name}): titular y tabla discrepan; regenera el análisis."
    )


def validate_metric_payload(payload: Any) -> None:
    """Check every KPI block, including nested temporal and percentage comparisons."""
    if isinstance(payload, list):
        for item in payload:
            validate_metric_payload(item)
    elif isinstance(payload, dict):
        actual, baseline, deltas = (payload.get(k) for k in ("kpis", "base_kpis", "deltas"))
        for values in (actual, baseline):
            if isinstance(values, dict):
                pro, det, neutral = (
                    values.get(k) for k in ("promoter_rate", "detractor_rate", "neutral_rate")
                )
                if pro is not None and det is not None and values.get("classic_nps") is not None:
                    assert_metric_equal(
                        values["classic_nps"], (pro - det) * 100, "NPS clásico y pesos porcentuales"
                    )
                if pro is not None and det is not None and neutral is not None:
                    assert_metric_equal(pro + det + neutral, 1, "Pesos porcentuales")
        if isinstance(actual, dict) and isinstance(baseline, dict) and isinstance(deltas, dict):
            for name, delta in deltas.items():
                a, b = actual.get(name), baseline.get(name)
                expected = float(a) - float(b) if a is not None and b is not None else None
                assert_metric_equal(delta.get("value"), expected, name)
                from nps_lens.services.analytics.kpis_service import format_delta, format_kpi_value

                if "display" in delta and delta["display"] != format_delta(expected, kpi_key=name):
                    raise ReportCoherenceError(
                        f"Variación formateada incoherente ({name}); regenera el análisis."
                    )
                for values, display_key in ((actual, "display"), (baseline, "base_display")):
                    display = payload.get(display_key, {})
                    if name in display and display[name] != format_kpi_value(
                        name, values.get(name)
                    ):
                        raise ReportCoherenceError(
                            f"Valor formateado incoherente ({name}); regenera el análisis."
                        )
        for value in payload.values():
            if isinstance(value, (dict, list)):
                validate_metric_payload(value)


def validate_delta_rows(rows: Iterable[Mapping[Hashable, Any]]) -> None:
    for row in rows:
        if all(key in row for key in ("delta_nps", "nps_current", "nps_baseline")):
            current, baseline = row["nps_current"], row["nps_baseline"]
            expected = (
                float(current) - float(baseline)
                if current is not None and baseline is not None
                else None
            )
            assert_metric_equal(row["delta_nps"], expected, "Delta NPS por tema")
