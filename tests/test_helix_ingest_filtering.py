import pandas as pd

from nps_lens.ingest.helix_incidents import read_helix_incidents_excel


def test_helix_ingest_filters_only_by_owner_support_company(tmp_path):
    # Build a small incidents export
    df = pd.DataFrame(
        {
            "Owner Support Company": ["BBVA México", "BBVA México", "BBVA España"],
            "BBVA_SourceServiceCompany": ["Servicio Global", "Servicio Global", "BBVA México"],
            "BBVA_SourceServiceN1": ["SN1A", "SN1A", "SN1A"],
            "BBVA_SourceServiceN2": ["SN2X", "SN2X, SN2Y", "SN2X"],
            "CreatedDate": ["2026-03-01", "2026-03-02", "2026-03-03"],
            "Descripción": ["a", "b", "c"],
        }
    )

    p = tmp_path / "helix.xlsx"
    df.to_excel(p, index=False)

    # N1/N2 are source attributes for optional causal attribution, not ingestion context.
    res = read_helix_incidents_excel(
        str(p),
        service_origin="BBVA México",
        service_origin_n1="SN1A",
        service_origin_n2="SN2X",
        sheet_name=None,
    )

    assert res.df is not None
    assert len(res.df) == 2
    assert res.df["BBVA_SourceServiceN2"].tolist() == ["SN2X", "SN2X, SN2Y"]
    assert res.df["BBVA_SourceServiceCompany"].iloc[0] == "Servicio Global"


def test_helix_ingest_keeps_all_n1_values_for_owner(tmp_path):
    df = pd.DataFrame(
        {
            "Owner Support Company": ["BBVA México", "BBVA México"],
            "BBVA_SourceServiceCompany": ["BBVA México", "BBVA México"],
            "BBVA_SourceServiceN1": ["SN1A", "SN1B"],
            "BBVA_SourceServiceN2": ["", "SN2X"],
            "CreatedDate": ["2026-03-01", "2026-03-02"],
        }
    )
    p = tmp_path / "helix.xlsx"
    df.to_excel(p, index=False)

    res = read_helix_incidents_excel(
        str(p),
        service_origin="BBVA México",
        service_origin_n1="SN1A",
        service_origin_n2="",
        sheet_name=None,
    )
    assert res.df is not None
    assert len(res.df) == 2


def test_helix_ingest_rejects_file_without_owner_support_company(tmp_path):
    df = pd.DataFrame(
        {
            "BBVA_SourceServiceCompany": ["BBVA México"],
            "BBVA_SourceServiceN1": ["SN1A"],
        }
    )
    path = tmp_path / "helix-without-owner.xlsx"
    df.to_excel(path, index=False)

    result = read_helix_incidents_excel(
        str(path),
        service_origin="BBVA México",
        service_origin_n1="SN1A",
        service_origin_n2="",
    )

    assert any(issue.level == "ERROR" for issue in result.issues)
    assert any(issue.column == "Owner Support Company" for issue in result.issues)
