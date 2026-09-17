from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd
import pytest

from nps_lens.domain.column_aliases import ColumnAliasRegistry, normalize_column_header
from nps_lens.ingest.nps_thermal import read_nps_thermal_excel


def _write_excel(tmp_path: Path, columns: dict[str, list[object]], name: str = "nps.xlsx") -> Path:
    path = tmp_path / name
    pd.DataFrame(columns).to_excel(path, index=False)
    return path


def _read(path: Path, aliases_path: Optional[Path] = None):
    return read_nps_thermal_excel(
        str(path),
        service_origin="BBVA Argentina",
        service_origin_n1="AR44 PLATAFORMA SENDA ARG",
        column_aliases_path=aliases_path,
    )


def test_canonical_headers_are_resolved_directly(tmp_path: Path) -> None:
    result = _read(
        _write_excel(
            tmp_path,
            {"Fecha": ["2026-09-01"], "NPS": [9], "Canal": ["App"]},
        )
    )

    assert not any(issue.level == "ERROR" for issue in result.issues)
    assert result.df.loc[0, "NPS"] == 9
    assert result.meta["applied_column_aliases"] == []


def test_configured_alias_is_applied_and_traced(tmp_path: Path) -> None:
    payload = ColumnAliasRegistry.default().to_dict()
    for field in payload["fields"]:
        if field["canonical"] == "Canal":
            field["aliases"].append("Touch point")
    aliases_path = tmp_path / "aliases.json"
    ColumnAliasRegistry.from_dict(payload).save(aliases_path)
    result = _read(
        _write_excel(
            tmp_path,
            {"Fecha": ["2026-09-01"], "NPS": [8], "Touch point": ["Web"]},
        ),
        aliases_path,
    )

    assert result.df.loc[0, "Canal"] == "Web"
    assert {"source": "Touch point", "canonical": "Canal"} in result.meta["applied_column_aliases"]
    assert any(issue.code == "column_aliases_applied" for issue in result.issues)


def test_canonical_header_wins_when_alias_is_also_present(tmp_path: Path) -> None:
    result = _read(
        _write_excel(
            tmp_path,
            {
                "Fecha": ["2026-09-01"],
                "NPS": [10],
                "OPI-Sense": [0],
                "Canal": ["App"],
            },
        )
    )

    assert not any(issue.code == "ambiguous_column_alias" for issue in result.issues)
    assert result.df.loc[0, "NPS"] == 10


def test_duplicate_and_colliding_alias_configuration_is_rejected() -> None:
    duplicated = ColumnAliasRegistry.default().to_dict()
    duplicated["fields"][0]["aliases"].append(" DATE ")
    with pytest.raises(ValueError, match="duplicado"):
        ColumnAliasRegistry.from_dict(duplicated)

    collision = ColumnAliasRegistry.default().to_dict()
    collision["fields"][1]["aliases"].append("Date")
    with pytest.raises(ValueError, match="colisiona"):
        ColumnAliasRegistry.from_dict(collision)


def test_two_aliases_for_same_field_are_ambiguous(tmp_path: Path) -> None:
    result = _read(
        _write_excel(
            tmp_path,
            {
                "Fecha": ["2026-09-01"],
                "OPI-Sense": [10],
                "NPS Response": [0],
                "Canal": ["App"],
            },
        )
    )

    issue = next(issue for issue in result.issues if issue.code == "ambiguous_column_alias")
    assert issue.column == "NPS"
    assert issue.details["columns"] == ["OPI-Sense", "NPS Response"]


def test_header_normalization_handles_case_accents_spaces_and_separators(tmp_path: Path) -> None:
    assert normalize_column_header("  USUÁRIO-Decisión__Final ") == normalize_column_header(
        "usuario decision final"
    )
    result = _read(
        _write_excel(
            tmp_path,
            {
                "gf_cust-survey response date": ["2026-09-01"],
                "nps_response": [7],
                "CHANNEL": ["App"],
            },
        )
    )
    assert not any(issue.level == "ERROR" for issue in result.issues)
    assert set(["Fecha", "NPS", "Canal"]).issubset(result.df.columns)


def test_missing_required_errors_and_missing_optional_warns(tmp_path: Path) -> None:
    missing_required = _read(
        _write_excel(tmp_path, {"Fecha": ["2026-09-01"], "NPS": [5]}, "required.xlsx")
    )
    assert any(
        issue.code == "missing_required_column" and issue.column == "Canal"
        for issue in missing_required.issues
    )

    missing_optional = _read(
        _write_excel(
            tmp_path,
            {"Fecha": ["2026-09-01"], "NPS": [5], "Canal": ["App"]},
            "optional.xlsx",
        )
    )
    assert any(
        issue.code == "optional_column_missing" and issue.column == "Comment"
        for issue in missing_optional.issues
    )


def test_senda_headers_produce_canonical_internal_schema(tmp_path: Path) -> None:
    result = _read(
        _write_excel(
            tmp_path,
            {
                "Date": ["2026-09-01"],
                "OPI-Sense": [9],
                "Channel": ["App"],
                "Text": ["Funciona bien"],
                "Id": ["ar-1"],
            },
        )
    )

    assert not any(issue.level == "ERROR" for issue in result.issues)
    assert result.df.loc[0, ["NPS", "Canal", "Comment", "ID"]].tolist() == [
        9,
        "App",
        "Funciona bien",
        "ar-1",
    ]
    assert {item["source"] for item in result.meta["applied_column_aliases"]} == {
        "Date",
        "OPI-Sense",
        "Channel",
        "Text",
        "Id",
    }


def test_aliases_are_isolated_by_buug_and_n1(tmp_path: Path) -> None:
    path = tmp_path / "aliases.json"
    argentina_payload = ColumnAliasRegistry.default().to_dict()
    mexico_payload = ColumnAliasRegistry.default().to_dict()
    next(field for field in argentina_payload["fields"] if field["canonical"] == "Canal")[
        "aliases"
    ].append("Canal AR")
    next(field for field in mexico_payload["fields"] if field["canonical"] == "Canal")[
        "aliases"
    ].append("Canal MX")

    ColumnAliasRegistry.from_dict(argentina_payload).save_for_context(
        path, "BBVA Argentina", "SENDA"
    )
    ColumnAliasRegistry.from_dict(mexico_payload).save_for_context(path, "BBVA México", "WEB")

    argentina = ColumnAliasRegistry.load(path, "BBVA Argentina", "SENDA").to_dict()
    mexico = ColumnAliasRegistry.load(path, "BBVA México", "WEB").to_dict()
    unconfigured = ColumnAliasRegistry.load(path, "BBVA Perú", "APP").to_dict()
    argentina_canal = next(
        field["aliases"] for field in argentina["fields"] if field["canonical"] == "Canal"
    )
    mexico_canal = next(
        field["aliases"] for field in mexico["fields"] if field["canonical"] == "Canal"
    )
    default_canal = next(
        field["aliases"] for field in unconfigured["fields"] if field["canonical"] == "Canal"
    )

    assert "Canal AR" in argentina_canal and "Canal MX" not in argentina_canal
    assert "Canal MX" in mexico_canal and "Canal AR" not in mexico_canal
    assert "Canal AR" not in default_canal and "Canal MX" not in default_canal


def test_ingest_selects_aliases_from_its_buug_and_n1(tmp_path: Path) -> None:
    aliases_path = tmp_path / "aliases.json"
    payload = ColumnAliasRegistry.default().to_dict()
    next(field for field in payload["fields"] if field["canonical"] == "NPS")["aliases"].append(
        "Score AR"
    )
    ColumnAliasRegistry.from_dict(payload).save_for_context(aliases_path, "BBVA Argentina", "SENDA")
    excel = _write_excel(
        tmp_path,
        {"Fecha": ["2026-09-01"], "Score AR": [9], "Canal": ["App"]},
    )

    argentina = read_nps_thermal_excel(
        str(excel),
        service_origin="BBVA Argentina",
        service_origin_n1="SENDA",
        column_aliases_path=aliases_path,
    )
    mexico = read_nps_thermal_excel(
        str(excel),
        service_origin="BBVA México",
        service_origin_n1="WEB",
        column_aliases_path=aliases_path,
    )

    assert not any(issue.level == "ERROR" for issue in argentina.issues)
    assert any(
        issue.code == "missing_required_column" and issue.column == "NPS" for issue in mexico.issues
    )


def test_legacy_global_configuration_migrates_without_losing_fallback(tmp_path: Path) -> None:
    path = tmp_path / "aliases.json"
    legacy_payload = ColumnAliasRegistry.default().to_dict()
    next(field for field in legacy_payload["fields"] if field["canonical"] == "Comment")[
        "aliases"
    ].append("Legacy comment")
    ColumnAliasRegistry.from_dict(legacy_payload).save(path)

    scoped_payload = ColumnAliasRegistry.default().to_dict()
    next(field for field in scoped_payload["fields"] if field["canonical"] == "Comment")[
        "aliases"
    ].append("Scoped comment")
    ColumnAliasRegistry.from_dict(scoped_payload).save_for_context(path, "BBVA Argentina", "SENDA")

    stored = json.loads(path.read_text(encoding="utf-8"))
    fallback = ColumnAliasRegistry.load(path, "BBVA Colombia", "APP").to_dict()
    scoped = ColumnAliasRegistry.load(path, "BBVA Argentina", "SENDA").to_dict()
    fallback_comments = next(
        field["aliases"] for field in fallback["fields"] if field["canonical"] == "Comment"
    )
    scoped_comments = next(
        field["aliases"] for field in scoped["fields"] if field["canonical"] == "Comment"
    )

    assert stored["schema_version"] == "2.0"
    assert "Legacy comment" in fallback_comments
    assert "Scoped comment" in scoped_comments
    assert "Legacy comment" not in scoped_comments
