"""UI helpers for Streamlit.

Kept separate from analytics so we can swap UI frameworks later.
"""

from nps_lens.ui.business import (
    PERIODICITIES,
    PeriodWindow,
    PeriodWindows,
    default_windows,
    driver_delta_table,
    pandas_freq_for_periodicity,
    period_windows,
    slice_by_window,
)
from nps_lens.ui.charts import (
    chart_cohort_heatmap,
    chart_driver_bar,
    chart_driver_delta,
    chart_nps_trend,
    chart_topic_bars,
)
from nps_lens.ui.narratives import (
    PeriodComparison,
    build_executive_story,
    compare_periods,
    executive_summary,
    explain_opportunities,
    explain_topics,
)

__all__ = [
    "chart_driver_bar",
    "chart_driver_delta",
    "chart_cohort_heatmap",
    "chart_nps_trend",
    "chart_topic_bars",
    "PeriodComparison",
    "compare_periods",
    "build_executive_story",
    "executive_summary",
    "explain_opportunities",
    "explain_topics",
    "PeriodWindow",
    "PeriodWindows",
    "PERIODICITIES",
    "default_windows",
    "period_windows",
    "pandas_freq_for_periodicity",
    "slice_by_window",
    "driver_delta_table",
]
