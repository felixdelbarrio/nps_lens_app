"""Canonical topic paths for editorial output; source categories remain untouched."""

import pandas as pd


def topic_paths(frame: pd.DataFrame) -> pd.Series:
    parent = (
        frame.get("Palanca", pd.Series("", index=frame.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    child = (
        frame.get("Subpalanca", pd.Series("", index=frame.index))
        .astype("string")
        .fillna("")
        .str.strip()
    )
    labels = child.mask(child.eq(""), parent)
    distinct = parent.ne("") & child.ne("") & parent.ne(child)
    return labels.mask(distinct, parent + " > " + child)
