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
    link_comment_start: date | None
    link_comment_end: date | None

    @property
    def helix_start(self) -> date | None:
        return (
            self.link_comment_start - timedelta(days=self.max_days_apart)
            if self.link_comment_start
            else None
        )

    def mask(self, dates: Any, *, helix: bool = False) -> Any:
        dates = pd.to_datetime(dates, errors="coerce", utc=True).dt.date
        start = self.helix_start if helix else self.comment_start
        end = self.link_comment_end if helix else self.comment_end
        return dates.between(start, end) if start and end else dates.notna() & False

    def payload(self) -> dict[str, Any]:
        return {
            "comment_start": self.comment_start.isoformat() if self.comment_start else None,
            "comment_end": self.comment_end.isoformat() if self.comment_end else None,
            "helix_start": self.helix_start.isoformat() if self.helix_start else None,
            "helix_end": self.link_comment_end.isoformat() if self.link_comment_end else None,
            "link_comment_start": (
                self.link_comment_start.isoformat() if self.link_comment_start else None
            ),
            "link_comment_end": (
                self.link_comment_end.isoformat() if self.link_comment_end else None
            ),
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
        return AnalysisHorizon(None, None, days, None, None)
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
        (date.fromisoformat(start) if start else selected.min()) if len(selected) else None,
        selected.max() if len(selected) else None,
    )


def required_comments(frame: pd.DataFrame, **scope: Any) -> tuple[pd.DataFrame, AnalysisHorizon]:
    horizon = analysis_horizon(frame, **scope)
    return (
        frame.loc[horizon.mask(frame["Fecha"])] if "Fecha" in frame else frame,
        horizon,
    )


def eligible_helix(
    incidents: pd.DataFrame, horizon: AnalysisHorizon, assignments: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Context-loaded incidents sharing the channel, temporal and quality linking policy."""
    from nps_lens.analytics.nps_helix_link import annotate_incident_link_quality
    from nps_lens.domain.helix import SOURCE_SERVICE_N1, SOURCE_SERVICE_N2
    from nps_lens.domain.normalization import equivalence_key
    from nps_lens.ingest.helix_dates import incident_occurrence_dates

    keys = {equivalence_key(value) for value in assignments if equivalence_key(value)}
    if keys:
        n1 = incidents.get(SOURCE_SERVICE_N1, pd.Series("", index=incidents.index)).map(
            equivalence_key
        )
        n2 = (
            incidents.get(SOURCE_SERVICE_N2, pd.Series("", index=incidents.index))
            .astype(str)
            .map(lambda value: any(equivalence_key(token) in keys for token in value.split(",")))
        )
        incidents = incidents.loc[n1.isin(keys) | n2]
    window = annotate_incident_link_quality(
        incidents.loc[horizon.mask(incident_occurrence_dates(incidents)[0], helix=True)]
    )
    eligible = window.loc[window["Causal Match Eligible"]]
    eligible.attrs["window_total"] = len(window)
    return incidents, eligible
