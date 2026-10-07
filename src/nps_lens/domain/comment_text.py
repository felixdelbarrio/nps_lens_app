"""Shared population of responses containing an actual comment."""

from __future__ import annotations

import pandas as pd

COMMENT_COLUMNS = ("comment_txt", "Comment", "Comentario", "Comentarios", "comentario")


def nonempty_comment_mask(frame: pd.DataFrame) -> pd.Series[bool] | None:
    for column in COMMENT_COLUMNS:
        if column in frame.columns:
            text = frame[column].fillna("").astype(str).str.strip().str.casefold()
            return ~text.isin({"", "nan", "none", "null", "<na>"})
    return None
