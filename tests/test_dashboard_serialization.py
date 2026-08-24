from __future__ import annotations

import json

import numpy as np
import pandas as pd

from nps_lens.services.dashboard_service import DashboardService


def test_serialize_rows_produces_strict_json_safe_scalars() -> None:
    frame = pd.DataFrame(
        {
            "finite": [1.5, np.float64(2.5)],
            "missing_float": [np.nan, 3.0],
            "positive_inf": [np.inf, 4.0],
            "negative_inf": [-np.inf, 5.0],
            "timestamp": [pd.Timestamp("2026-08-21 09:30:07"), pd.NaT],
            "nullable_int": pd.Series([1, pd.NA], dtype="Int64"),
            "nullable_bool": pd.Series([True, pd.NA], dtype="boolean"),
            "text": ["ok", None],
        }
    )

    rows = DashboardService._serialize_rows(frame)

    assert rows[0]["finite"] == 1.5
    assert rows[0]["missing_float"] is None
    assert rows[0]["positive_inf"] is None
    assert rows[0]["negative_inf"] is None
    assert rows[0]["timestamp"] == "2026-08-21T09:30:07"
    assert rows[0]["nullable_int"] == 1
    assert rows[0]["nullable_bool"] is True
    assert rows[1]["timestamp"] is None
    assert rows[1]["nullable_int"] is None
    assert rows[1]["nullable_bool"] is None
    assert rows[1]["text"] is None

    # Mirrors Starlette's strict JSON behavior: NaN/inf must not be emitted.
    json.dumps(rows, allow_nan=False)
