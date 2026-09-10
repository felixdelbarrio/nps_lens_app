"""Stable response identity independent of mutable categorical fields."""

from __future__ import annotations

from typing import Any

import pandas as pd


def hash_rows(frame: pd.DataFrame, columns: list[str], prefix: str = "") -> pd.Series[Any]:
    """Hash normalized rows in native pandas code instead of Python per-row callbacks."""
    stable = frame.reindex(columns=columns).copy()
    for column in stable.columns:
        if pd.api.types.is_datetime64_any_dtype(stable[column]):
            stable[column] = stable[column].dt.strftime("%Y-%m-%dT%H:%M:%S.%f").fillna("")
        else:
            stable[column] = stable[column].astype("string").fillna("")
    hashes = pd.util.hash_pandas_object(stable, index=False, categorize=True)
    return hashes.map(lambda value: prefix + format(int(value), "016x"))


def business_keys(frame: pd.DataFrame) -> pd.Series[Any]:
    frame = frame.assign(NPS=pd.to_numeric(frame["NPS"], errors="coerce").astype(float))
    external = (
        frame.get("ID", pd.Series("", index=frame.index))
        .astype("string")
        .fillna("")
        .str.strip()
        .ne("")
    )
    context = ["service_origin", "service_origin_n1", "service_origin_n2"]
    keys = pd.Series("", index=frame.index, dtype="string")
    keys.loc[external] = hash_rows(frame.loc[external], ["ID", *context], "id:")
    fallback = ["Fecha", "NPS", "Comment", "UsuarioDecisión", *context]
    if (~external).any():
        keys.loc[~external] = hash_rows(frame.loc[~external], fallback, "fp:")
    return keys
