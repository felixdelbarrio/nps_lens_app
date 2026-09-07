from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IncidentRationaleSummary:
    topics_analyzed: int
    responses: int
    incidents: int
    top3_incident_share: float
    median_lag_weeks: float


RATIONALE_COLUMNS = [
    "nps_topic",
    "touchpoint",
    "weeks",
    "responses",
    "incidents",
    "incident_rate_per_100_responses",
    "low_incident_weeks",
    "high_incident_weeks",
    "low_incident_threshold",
    "high_incident_threshold",
    "focus_rate_low_incidence",
    "focus_rate_high_incidence",
    "focus_rate_difference_pp",
    "score_mean_low_incidence",
    "score_mean_high_incidence",
    "score_mean_difference",
    "best_lag_weeks",
    "corr",
    "max_cp_stability",
    "incidents_lead_changepoint_share",
]


def _empty_rationale_df() -> pd.DataFrame:
    return pd.DataFrame(columns=RATIONALE_COLUMNS)


def _safe_num(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return float(default)
    return result if np.isfinite(result) else float(default)


def _touchpoint_from_topic(topic: object) -> str:
    parts = [part.strip() for part in str(topic or "").split(">") if part.strip()]
    return parts[0] if parts else "Journey sin etiquetar"


def _rank_lookup(rank_df: Optional[pd.DataFrame]) -> dict[str, dict[str, float]]:
    """Extract only reproducible temporal statistics from the topic analysis."""
    if rank_df is None or rank_df.empty or "nps_topic" not in rank_df.columns:
        return {}
    result: dict[str, dict[str, float]] = {}
    for _, row in rank_df.iterrows():
        topic = str(row.get("nps_topic", "")).strip()
        if topic:
            result[topic] = {
                "corr": _safe_num(row.get("corr"), np.nan),
                "best_lag_weeks": _safe_num(row.get("best_lag_weeks"), np.nan),
                "max_cp_stability": _safe_num(row.get("max_cp_stability"), np.nan),
                "incidents_lead_changepoint_share": _safe_num(
                    row.get("incidents_lead_changepoint_share"), np.nan
                ),
            }
    return result


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    valid = values.notna() & weights.gt(0)
    if not bool(valid.any()):
        return float("nan")
    return float(np.average(values.loc[valid].astype(float), weights=weights.loc[valid]))


def build_incident_nps_rationale(
    by_topic_weekly: pd.DataFrame,
    *,
    focus_group: str = "detractor",
    rank_df: Optional[pd.DataFrame] = None,
    min_topic_responses: int = 80,
) -> pd.DataFrame:
    """Compare low and high incident weeks with transparent, weighted statistics."""
    del focus_group  # focus_rate already represents the selected group.
    required = {"nps_topic", "responses", "focus_rate", "incidents"}
    if by_topic_weekly.empty or not required.issubset(by_topic_weekly.columns):
        return _empty_rationale_df()
    frame = by_topic_weekly.copy()
    frame["week"] = pd.to_datetime(frame.get("week"), errors="coerce")
    frame["nps_topic"] = frame["nps_topic"].astype(str).str.strip()
    frame["responses"] = pd.to_numeric(frame["responses"], errors="coerce").fillna(0).clip(lower=0)
    frame["incidents"] = pd.to_numeric(frame["incidents"], errors="coerce").fillna(0).clip(lower=0)
    frame["focus_rate"] = pd.to_numeric(frame["focus_rate"], errors="coerce").clip(0, 1)
    frame["nps_mean"] = pd.to_numeric(frame.get("nps_mean"), errors="coerce")
    frame = frame[frame["nps_topic"].ne("") & frame["responses"].gt(0)]
    temporal = _rank_lookup(rank_df)
    rows: list[dict[str, Any]] = []
    for topic, group in frame.groupby("nps_topic", observed=True):
        group = group.sort_values("week")
        responses = int(group["responses"].sum())
        weeks = int(group["week"].nunique()) if group["week"].notna().any() else len(group)
        if (
            responses < int(min_topic_responses)
            or weeks < 3
            or group["focus_rate"].notna().sum() < 3
        ):
            continue
        low_threshold = float(group["incidents"].quantile(0.30))
        high_threshold = float(group["incidents"].quantile(0.70))
        low = group["incidents"].le(low_threshold)
        high = group["incidents"].ge(high_threshold)
        if bool((low & high).any()):
            median = float(group["incidents"].median())
            low, high = group["incidents"].le(median), group["incidents"].gt(median)
            low_threshold = high_threshold = median
        if not bool(low.any()) or not bool(high.any()):
            continue
        focus_low = _weighted_mean(group.loc[low, "focus_rate"], group.loc[low, "responses"])
        focus_high = _weighted_mean(group.loc[high, "focus_rate"], group.loc[high, "responses"])
        score_low = _weighted_mean(group.loc[low, "nps_mean"], group.loc[low, "responses"])
        score_high = _weighted_mean(group.loc[high, "nps_mean"], group.loc[high, "responses"])
        stats = temporal.get(str(topic), {})
        incidents = int(group["incidents"].sum())
        rows.append(
            {
                "nps_topic": str(topic),
                "touchpoint": _touchpoint_from_topic(topic),
                "weeks": weeks,
                "responses": responses,
                "incidents": incidents,
                "incident_rate_per_100_responses": incidents / responses * 100,
                "low_incident_weeks": int(low.sum()),
                "high_incident_weeks": int(high.sum()),
                "low_incident_threshold": low_threshold,
                "high_incident_threshold": high_threshold,
                "focus_rate_low_incidence": focus_low,
                "focus_rate_high_incidence": focus_high,
                "focus_rate_difference_pp": (focus_high - focus_low) * 100,
                "score_mean_low_incidence": score_low,
                "score_mean_high_incidence": score_high,
                "score_mean_difference": score_high - score_low,
                "best_lag_weeks": stats.get("best_lag_weeks", np.nan),
                "corr": stats.get("corr", np.nan),
                "max_cp_stability": stats.get("max_cp_stability", np.nan),
                "incidents_lead_changepoint_share": stats.get(
                    "incidents_lead_changepoint_share", np.nan
                ),
            }
        )
    if not rows:
        return _empty_rationale_df()
    return (
        pd.DataFrame(rows)
        .sort_values(
            ["incidents", "responses", "focus_rate_difference_pp", "nps_topic"],
            ascending=[False, False, False, True],
        )
        .reset_index(drop=True)[RATIONALE_COLUMNS]
    )


def summarize_incident_nps_rationale(rationale_df: pd.DataFrame) -> IncidentRationaleSummary:
    if rationale_df is None or rationale_df.empty:
        return IncidentRationaleSummary(0, 0, 0, 0.0, float("nan"))
    incidents = pd.to_numeric(rationale_df["incidents"], errors="coerce").fillna(0)
    total_incidents = int(incidents.sum())
    lags = pd.to_numeric(rationale_df["best_lag_weeks"], errors="coerce").dropna()
    return IncidentRationaleSummary(
        topics_analyzed=len(rationale_df),
        responses=int(pd.to_numeric(rationale_df["responses"], errors="coerce").fillna(0).sum()),
        incidents=total_incidents,
        top3_incident_share=(
            float(incidents.nlargest(3).sum()) / total_incidents if total_incidents else 0.0
        ),
        median_lag_weeks=float(lags.median()) if not lags.empty else float("nan"),
    )
