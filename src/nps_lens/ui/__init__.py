"""UI helpers for Streamlit.

Kept separate from analytics so we can swap UI frameworks later.
"""

from nps_lens.ui.charts import (
    chart_driver_bar,
    chart_driver_delta,
    chart_nps_trend,
    chart_cohort_heatmap,
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
from nps_lens.ui.business import (
    PeriodWindow,
    default_windows,
    driver_delta_table,
    slice_by_window,
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
    "default_windows",
    "slice_by_window",
    "driver_delta_table",
]
