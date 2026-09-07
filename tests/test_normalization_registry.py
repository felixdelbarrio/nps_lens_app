from pathlib import Path

import pandas as pd

from nps_lens.analytics.opportunities import rank_opportunities
from nps_lens.domain.normalization import EquivalenceRegistry, equivalence_key
from nps_lens.ingest.nps_thermal import read_nps_thermal_excel


def test_equivalence_key_unifies_joiners_case_accents_and_spacing() -> None:
    values = [
        "Pagos y transferencias",
        "PAGOS/ TRANSFERENCIAS",
        " pagos /transferencias ",
    ]
    assert len({equivalence_key(value) for value in values}) == 1


def test_default_registry_unifies_business_labels() -> None:
    registry = EquivalenceRegistry.default()
    assert registry.normalize("Palanca", "Pagos y transferencias") == "Pagos/transferencias"
    assert registry.normalize("Palanca", "Pagos/ Transferencias") == "Pagos/transferencias"
    assert "NPS Group" not in registry.to_dict()["dimensions"]


def test_registry_rejects_ambiguous_aliases() -> None:
    payload = {
        "dimensions": {
            "Palanca": [
                {"canonical": "Uno", "aliases": ["igual"]},
                {"canonical": "Dos", "aliases": ["IGUAL"]},
            ]
        }
    }
    try:
        EquivalenceRegistry.from_dict(payload)
    except ValueError as exc:
        assert "asignada" in str(exc)
    else:
        raise AssertionError("Expected an ambiguous-alias error")


def test_parser_accepts_current_senda_export_headers(tmp_path: Path) -> None:
    source = pd.DataFrame(
        {
            "gf_cust_survey_response_date": ["2026-08-01", "2026-08-02"],
            "gf_cust_survey_opinion_id": ["a", "b"],
            "user_type": ["NEUTROS", "DETRACTOR"],
            "nps_response": [8, 2],
            "comment_response": ["Transferencia", "Pago"],
            "toma_desicion": ["", ""],
            "Canal": ["WEB", "Web"],
            "Palanca": ["Pagos y transferencias", "Pagos/ Transferencias"],
            "Subpalanca": ["Fallas en el Login", "Fallas en el login"],
        }
    )
    path = tmp_path / "senda.xlsx"
    source.to_excel(path, index=False)

    result = read_nps_thermal_excel(
        str(path),
        service_origin="BBVA México",
        service_origin_n1="ENTERPRISE WEB",
    )

    assert not [issue for issue in result.issues if issue.level == "ERROR"]
    assert result.df["Palanca"].unique().tolist() == ["Pagos/transferencias"]
    assert result.df["Subpalanca"].unique().tolist() == ["Fallas en el login"]
    assert result.df["NPS Group"].tolist() == ["PASIVO", "DETRACTOR"]


def test_opportunities_never_expose_an_empty_business_label() -> None:
    frame = pd.DataFrame(
        {
            "NPS": [0] * 250 + [10] * 250,
            "Palanca": [""] * 250 + ["Pagos y transferencias"] * 250,
        }
    )
    opportunities = rank_opportunities(frame, ["Palanca"], min_n=200)
    assert all(item.value for item in opportunities)
    assert all(item.why != "'Palanca='" for item in opportunities)
