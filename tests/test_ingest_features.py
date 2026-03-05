import pandas as pd

from nps_lens.ingest.features import add_precomputed_features
from nps_lens.core.store import DatasetContext


def test_add_precomputed_features_n2_key_and_text_norm():
    df = pd.DataFrame(
        {
            "service_origin_n2": ["SN2X, SN2Y", "SN2Y,SN2X", ""],
            "Comment": [" Hola   Mundo ", None, ""],
        }
    )
    out, added = add_precomputed_features(df)
    assert "_service_origin_n2_key" in out.columns
    assert "_text_norm" in out.columns
    assert "_service_origin_n2_key" in added
    assert out.loc[0, "_service_origin_n2_key"] == "SN2X,SN2Y"
    assert out.loc[1, "_service_origin_n2_key"] == "SN2X,SN2Y"
    assert out.loc[0, "_text_norm"] == "hola mundo"


def test_datasetcontext_norm_n2_is_order_insensitive():
    assert DatasetContext._norm_n2("SN2Y, SN2X") == "SN2X,SN2Y"
    assert DatasetContext._norm_n2("  ") == ""
