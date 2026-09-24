"""Stable response identity independent of mutable categorical fields."""

from __future__ import annotations

from typing import Any

import pandas as pd

from nps_lens.domain.column_aliases import normalize_column_header


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


def resolve_response_identity(frame: pd.DataFrame) -> tuple[pd.Series[Any], str]:
    """Choose the most complete, unique response identifier; never trust a shared Id.

    Repeated identical responses are allowed. Conflicting legacy IDs get a content
    fingerprint, so they cannot overwrite another response during ingestion.
    """
    priorities = ("Opinion Identifier", "Opinion Unique Code", "ID", "GF CUST SURVEY OPINION ID")
    candidates = []
    for priority, name in enumerate(priorities):
        for column in frame.columns:
            if normalize_column_header(column) != normalize_column_header(name):
                continue
            values = frame[column].astype("string").fillna("").str.strip()
            values = values.mask(
                values.str.casefold().isin({"nan", "none", "null", "nat", "<na>"}), ""
            )
            populated = values.ne("")
            count = int(populated.sum())
            unique = int(values[populated].nunique())
            candidates.append(
                (unique / count if count else 0, count, -priority, str(column), values)
            )
    fallback = hash_rows(frame, ["Fecha", "NPS", "Comment", "UsuarioDecisión"], "fp:")
    if not candidates:
        return fallback, "fingerprint"
    _, _, _, source, values = max(candidates, key=lambda item: item[:3])
    # Detect shared identifiers against immutable response content, not taxonomy.
    conflicts = (
        pd.DataFrame({"id": values, "content": fallback})
        .groupby("id")["content"]
        .transform("nunique")
        .gt(1)
    )
    invalid = values.eq("") | conflicts
    return values.mask(invalid, fallback), source


def analytical_response_ids(frame: pd.DataFrame) -> pd.Series[Any]:
    """Use persisted response identity consistently in links and evidence references.

    External IDs can repeat in legacy/restored corpora. Never collapse distinct
    business keys or choose an arbitrary response when enriching a link.
    """
    column = "_business_key" if "_business_key" in frame else "ID"
    values = frame.get(column, pd.Series(frame.index, index=frame.index))
    return values.fillna("").astype(str).str.strip()
