from __future__ import annotations

from collections.abc import Collection

import pandas as pd

_ALL_CHANNELS = {"", "all", "todos"}


def _keys(values: pd.Series) -> pd.Series:
    return values.fillna("").astype(str).str.strip().str.casefold()


def topics_observed_in_channel(
    frame: pd.DataFrame,
    dimension: str,
    channel: str,
) -> frozenset[str] | None:
    """Return topic keys observed in a channel; None means no channel restriction."""
    channel_key = str(channel or "").strip().casefold()
    if channel_key in _ALL_CHANNELS or "Canal" not in frame.columns:
        return None
    if frame.empty or dimension not in frame.columns:
        return frozenset()
    topic_keys = _keys(frame[dimension])
    channel_keys = _keys(frame["Canal"])
    if not channel_keys.ne("").any():
        return None
    channel_mask = channel_keys.eq(channel_key)
    return frozenset(topic_keys[channel_mask & topic_keys.ne("")].unique())


def restrict_to_topics(
    frame: pd.DataFrame,
    dimension: str,
    topic_keys: Collection[str] | None,
) -> pd.DataFrame:
    """Keep the full-channel metric population for the selected topic set."""
    if topic_keys is None:
        return frame
    if frame.empty or dimension not in frame.columns or not topic_keys:
        return frame.iloc[:0]
    return frame.loc[_keys(frame[dimension]).isin(topic_keys)]
