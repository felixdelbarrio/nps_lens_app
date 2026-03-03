from __future__ import annotations

import pandas as pd

from nps_lens.ingest.base import require_columns


def test_require_columns_flags_missing() -> None:
    df = pd.DataFrame({"a": [1]})
    issues = require_columns(df, ["a", "b"])
    assert any(i.level == "ERROR" for i in issues)
