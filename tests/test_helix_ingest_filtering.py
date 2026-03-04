import pandas as pd


from nps_lens.ingest.helix_incidents import read_helix_incidents_excel


def test_helix_ingest_filters_by_context_and_n2_strict(tmp_path):
    # Build a small incidents export
    df = pd.DataFrame(
        {
            "BBVA_SourceServiceCompany": ["BBVA México", "BBVA México", "BBVA España"],
            "BBVA_SourceServiceN1": ["SN1A", "SN1A", "SN1A"],
            "BBVA_SourceServiceN2": ["SN2X", "SN2X, SN2Y", "SN2X"],
            "CreatedDate": ["2026-03-01", "2026-03-02", "2026-03-03"],
            "Descripción": ["a", "b", "c"],
        }
    )

    p = tmp_path / "helix.xlsx"
    df.to_excel(p, index=False)

    # With N2 selected, we require strict token-set equality.
    res = read_helix_incidents_excel(
        str(p),
        service_origin="BBVA México",
        service_origin_n1="SN1A",
        service_origin_n2="SN2X",
        sheet_name=None,
    )

    assert res.df is not None
    assert len(res.df) == 1
    assert set(res.df["BBVA_SourceServiceN2"].iloc[0].split(", ")) == {"SN2X"}


def test_helix_ingest_filters_without_n2(tmp_path):
    df = pd.DataFrame(
        {
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
    assert len(res.df) == 1
