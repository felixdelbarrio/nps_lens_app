from __future__ import annotations

from typing import Optional

import pandas as pd

from nps_lens.analytics.causal import CausalHypothesis, best_effort_ate_logit
from nps_lens.analytics.changepoints import ChangePoint, detect_nps_changepoints
from nps_lens.analytics.drivers import DriverStat, driver_table
from nps_lens.analytics.journey import RouteCandidate, build_routes
from nps_lens.analytics.opportunities import Opportunity, rank_opportunities
from nps_lens.analytics.text_mining import TopicCluster, extract_topics
from nps_lens.core.metrics import NpsSummary, daily_kpis, daily_mix, summarize


def compute_summary(df: pd.DataFrame) -> NpsSummary:
    return summarize(df)


def compute_driver_table(df: pd.DataFrame, dimension: str) -> list[DriverStat]:
    return driver_table(df, dimension=dimension)


def compute_opportunities(df: pd.DataFrame, dimensions: list[str], min_n: int) -> list[Opportunity]:
    return rank_opportunities(df, dimensions=dimensions, min_n=min_n)


def compute_topics(
    texts: pd.Series[str],
    n_clusters: int = 10,
    max_features: int = 3000,
) -> list[TopicCluster]:
    return extract_topics(texts=texts, n_clusters=n_clusters, max_features=max_features)


def compute_routes(
    nps_df: pd.DataFrame,
    incidents_df: Optional[pd.DataFrame] = None,
    lever_col: str = "Palanca",
    sublever_col: str = "Subpalanca",
    comment_col: str = "Comment",
) -> list[RouteCandidate]:
    return build_routes(
        nps_df=nps_df,
        incidents_df=incidents_df,
        lever_col=lever_col,
        sublever_col=sublever_col,
        comment_col=comment_col,
    )


def compute_changepoints(
    df: pd.DataFrame, dim_col: str, value: str, freq: str = "D", pen: float = 8.0
) -> Optional[ChangePoint]:
    return detect_nps_changepoints(df, dim_col=dim_col, value=value, freq=freq, pen=pen)


def compute_causal_best_effort(
    df: pd.DataFrame,
    treatment_col: str,
    treatment_value: str,
    outcome_col: str = "is_detractor",
    control_cols: Optional[list[str]] = None,
) -> Optional[CausalHypothesis]:
    return best_effort_ate_logit(
        df=df,
        treatment_col=treatment_col,
        treatment_value=treatment_value,
        outcome_col=outcome_col,
        control_cols=control_cols,
    )


def compute_daily_mix(df: pd.DataFrame) -> pd.DataFrame:
    return daily_mix(df)


def compute_daily_kpis(df: pd.DataFrame) -> pd.DataFrame:
    return daily_kpis(df)
