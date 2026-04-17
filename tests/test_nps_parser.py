from __future__ import annotations

from pathlib import Path

import pandas as pd

from nps_lens.ingest.nps_thermal import read_nps_thermal_excel

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "excel"


def test_parser_normalizes_headers_tolerates_extra_columns_and_deduplicates(tmp_path: Path) -> None:
    path = tmp_path / "schema-drift.xlsx"
    pd.DataFrame(
        {
            " subpalanca ": ["Fallo", "Fallo"],
            "Browser": ["Chrome", "Chrome"],
            "Comentario": [10, 10],
            "ID": ["abc", "abc"],
            "Palanca": ["Acceso", "Acceso"],
            "Canal": ["Web", "Web"],
            "NPS": [2, 2],
            "Fecha": ["2026-03-01 10:00:00", "2026-03-01 10:00:00"],
        }
    ).to_excel(path, index=False)

    result = read_nps_thermal_excel(
        str(path),
        service_origin="BBVA México",
        service_origin_n1="Senda",
    )

    assert not any(issue.level == "ERROR" for issue in result.issues)
    assert "Comment" in result.df.columns
    assert "Browser" in result.df.columns
    assert result.df["Comment"].tolist() == ["10"]
    assert len(result.df) == 1
    assert any(issue.code == "extra_columns_detected" for issue in result.issues)
    assert result.meta["duplicate_rows_in_file"] == 1


def test_parser_returns_clear_error_when_critical_columns_are_missing(tmp_path: Path) -> None:
    path = tmp_path / "invalid.xlsx"
    pd.DataFrame(
        {
            "Fecha": ["2026-03-01"],
            "NPS": [10],
            "Canal": ["Web"],
        }
    ).to_excel(path, index=False)

    result = read_nps_thermal_excel(
        str(path),
        service_origin="BBVA México",
        service_origin_n1="Senda",
    )

    assert any(issue.level == "ERROR" for issue in result.issues)
    missing_columns = {
        issue.column for issue in result.issues if issue.code == "missing_required_column"
    }
    assert {"Palanca", "Subpalanca"}.issubset(missing_columns)


def test_regression_parser_handles_march_file_with_schema_drift() -> None:
    path = FIXTURES_DIR / "NPS Térmico Senda - 03Marzo.xlsx"

    result = read_nps_thermal_excel(
        str(path),
        service_origin="BBVA México",
        service_origin_n1="Senda",
    )

    assert not any(issue.level == "ERROR" for issue in result.issues)
    assert int(result.meta["normalized_rows"]) == len(result.df) == 26618
    assert "Browser" in result.df.columns
    assert "Operating System" in result.df.columns
    assert result.df["Comment"].map(lambda value: isinstance(value, str)).all()
    assert any(issue.code == "extra_columns_detected" for issue in result.issues)
