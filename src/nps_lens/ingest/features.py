from __future__ import annotations

import re
from typing import Tuple

import pandas as pd

from nps_lens.core.store import DatasetContext

_WS_RE = re.compile(r"\s+")


def _norm_text(value: object) -> str:
    """Cheap, deterministic text normalizer.

    Goals:
    - improve caching stability (same semantic text -> same normalized form)
    - speed up downstream text mining by avoiding repeated per-row ops

    We deliberately keep it cheap (no heavy NLP deps).
    """

    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value)
    s = s.strip().lower()
    s = _WS_RE.sub(" ", s)
    return s


def add_precomputed_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, list[str]]:
    """Add stable derived columns used across the app.

    Returns:
        (df_out, features_added)

    Columns are prefixed with "_" to avoid collisions with source schemas.
    """

    df_out = df.copy()
    added: list[str] = []

    # service_origin_n2: stable token-set key (order-insensitive)
    if "service_origin_n2" in df_out.columns and "_service_origin_n2_key" not in df_out.columns:
        df_out["_service_origin_n2_key"] = df_out["service_origin_n2"].apply(DatasetContext._norm_n2)
        added.append("_service_origin_n2_key")

    # normalized comment blob (cheap)
    # - used by topic extraction and verbatim sampling
    comment_cols = [c for c in ("Comment", "Comentario", "Texto", "Descripción") if c in df_out.columns]
    if comment_cols and "_text_norm" not in df_out.columns:
        col = "Comment" if "Comment" in comment_cols else comment_cols[0]
        df_out["_text_norm"] = df_out[col].apply(_norm_text)
        added.append("_text_norm")

    return df_out, added
