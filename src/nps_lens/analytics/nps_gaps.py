"""Shared topic gaps against the global historical NPS population."""

from dataclasses import dataclass

import pandas as pd

from nps_lens.analytics.channel_topic_scope import restrict_to_topics, topics_observed_in_channel
from nps_lens.analytics.drivers import driver_table
from nps_lens.core.metrics import summarize
from nps_lens.ui.business import PeriodWindow, default_windows, slice_by_window


@dataclass(frozen=True)
class NpsGaps:
    rows: pd.DataFrame
    base_nps: float | None
    base_n: int


def nps_gaps(
    current: pd.DataFrame, baseline: pd.DataFrame, dimension: str, *, channel: str = "Todos"
) -> NpsGaps:
    base = summarize(baseline)
    base_nps = base.nps_classic_pp if base.n else None
    base_n = base.n
    selected = restrict_to_topics(
        current, dimension, topics_observed_in_channel(current, dimension, channel)
    )
    rows = pd.DataFrame(
        (
            [stat.__dict__ for stat in driver_table(selected, dimension, base_nps=base_nps)]
            if base_nps is not None
            else []
        ),
    )
    if not rows.empty:
        rows = rows.sort_values(
            ["gap_vs_base", "n", "value"], ascending=[True, False, True], kind="stable"
        ).reset_index(drop=True)
    return NpsGaps(rows, base_nps, base_n)


@dataclass(frozen=True)
class GapPopulation:
    current: pd.DataFrame
    baseline: pd.DataFrame
    current_window: PeriodWindow | None
    base_window: PeriodWindow | None


def select_gap_population(
    history: pd.DataFrame, *, pop_year: str = "Todos", pop_month: str = "Todos"
) -> GapPopulation:
    current, base = default_windows(history, pop_year=pop_year, pop_month=pop_month)
    return GapPopulation(
        slice_by_window(history, current) if current else history.iloc[:0],
        slice_by_window(history, base) if base else history.iloc[:0],
        current,
        base,
    )
