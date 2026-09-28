"""Taxonomy detection and content signatures shared by all lenses."""

from __future__ import annotations

import json
from hashlib import sha256
from typing import Any, Mapping

import pandas as pd

from nps_lens.analytics.text_mining import preprocess_text

ENGINE_VERSION = "2"
MODES = ("SOURCE", "COMPLETED", "DISCOVERED")
SOURCE_COLUMNS = {
    "Canal": "source_channel",
    "Palanca": "source_lever",
    "Subpalanca": "source_sublever",
}


def text_series(frame: pd.DataFrame) -> pd.Series:
    return frame.get("Comment", pd.Series("", index=frame.index)).map(preprocess_text)


def labels(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame.get(column, pd.Series("", index=frame.index)).astype("string").fillna("")


def detect_taxonomy(frame: pd.DataFrame) -> dict[str, Any]:
    pal = (
        labels(frame, SOURCE_COLUMNS["Palanca"] if "source_lever" in frame else "Palanca")
        .str.strip()
        .ne("")
    )
    sub = (
        labels(frame, SOURCE_COLUMNS["Subpalanca"] if "source_sublever" in frame else "Subpalanca")
        .str.strip()
        .ne("")
    )
    text = text_series(frame).str.contains(r"\b\w{2,}\b", regex=True)
    complete = int((pal & sub).sum())
    state = (
        "NO_TEXT"
        if not text.any()
        else "COMPLETE" if complete == len(frame) else "PARTIAL" if (pal | sub).any() else "MISSING"
    )
    return {
        "state": state,
        "rows": len(frame),
        "classified": complete,
        "missing": len(frame) - complete,
        "usable_comments": int(text.sum()),
    }


def signature(
    frame: pd.DataFrame,
    mode: str,
    config: Mapping[str, Any],
    equivalences: str = "",
) -> str:
    # Score, dates, visual filters and Helix equivalences are deliberately excluded.
    columns = ["_business_key", "Comment"]
    if mode == "COMPLETED":
        columns += ["Palanca", "Subpalanca"]
    data = frame.reindex(columns=columns).astype("string").fillna("").sort_values("_business_key")
    digest = sha256(pd.util.hash_pandas_object(data, index=False).values.tobytes())
    digest.update(
        json.dumps(
            [
                ENGINE_VERSION,
                mode,
                dict(config),
                equivalences if mode == "COMPLETED" else "",
            ],
            sort_keys=True,
        ).encode()
    )
    return digest.hexdigest()
