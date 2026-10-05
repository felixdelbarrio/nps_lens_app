"""Temporal envelope of the existing analysis and its historical comparisons."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd

from nps_lens.analytics.linking_policy import LINK_MAX_DAYS_APART
from nps_lens.ui.business import default_windows
from nps_lens.ui.population import POP_ALL, population_date_window


@dataclass(frozen=True)
class AnalysisHorizon:
    comment_start: date | None
    comment_end: date | None
    max_days_apart: int

    @property
    def helix_start(self) -> date | None:
        return (
            self.comment_start - timedelta(days=self.max_days_apart) if self.comment_start else None
        )

    def mask(self, dates: Any, *, helix: bool = False) -> Any:
        dates = pd.to_datetime(dates, errors="coerce", utc=True).dt.date
        start = self.helix_start if helix else self.comment_start
        return (
            dates.between(start, self.comment_end)
            if start and self.comment_end
            else dates.notna() & False
        )

    def payload(self) -> dict[str, Any]:
        return {
            "comment_start": self.comment_start.isoformat() if self.comment_start else None,
            "comment_end": self.comment_end.isoformat() if self.comment_end else None,
            "helix_start": self.helix_start.isoformat() if self.helix_start else None,
            "helix_end": self.comment_end.isoformat() if self.comment_end else None,
            "max_days_apart": self.max_days_apart,
        }


def analysis_horizon(frame: pd.DataFrame, **scope: Any) -> AnalysisHorizon:
    """Include selected population and default_windows' actual historical baseline.

    Comparisons consume the whole population, independently of channel/NPS group.
    These execution filters therefore cannot narrow their classification corpus.
    """
    year, month = scope.get("pop_year") or POP_ALL, scope.get("pop_month") or POP_ALL
    days = int(scope.get("max_days_apart", LINK_MAX_DAYS_APART))
    if not 0 <= days <= 365:
        raise ValueError("La ventana debe estar entre 0 y 365 días.")
    current, baseline = default_windows(frame, pop_year=year, pop_month=month)
    if current is None:
        return AnalysisHorizon(None, None, days)
    dates = pd.to_datetime(frame["Fecha"], errors="coerce").dropna().dt.date
    start, end, month_filter = population_date_window(year, month)
    selected = dates
    if start and end:
        selected = selected.loc[
            selected.between(date.fromisoformat(start), date.fromisoformat(end))
        ]
    if month_filter:
        selected = selected.loc[pd.to_datetime(selected).dt.month.eq(int(month_filter))]
    return AnalysisHorizon(
        min(
            current.start,
            baseline.start if baseline else current.start,
            selected.min() if len(selected) else current.start,
        ),
        max(current.end, selected.max() if len(selected) else current.end),
        days,
    )
