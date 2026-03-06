"""Population (transversal filters) primitives used by the Streamlit app.

Design goals:
- Single source of truth for selector constants (no scattered magic strings).
- Zero runtime dependency on locale settings.
- Fast (pure functions, no pandas), Python 3.9 compatible.
"""

from __future__ import annotations

import calendar
from datetime import date
from typing import Dict, Optional, Tuple

# Global option used across selectors.
POP_ALL = "Todos"


# Month keys are 2-digit strings to match zfilled month numbers ("01".."12").
MONTH_LABELS_ES: Dict[str, str] = {
    "01": "Enero",
    "02": "Febrero",
    "03": "Marzo",
    "04": "Abril",
    "05": "Mayo",
    "06": "Junio",
    "07": "Julio",
    "08": "Agosto",
    "09": "Septiembre",
    "10": "Octubre",
    "11": "Noviembre",
    "12": "Diciembre",
}


def month_format_es(value: str) -> str:
    """Format month selectbox values.

    - "Todos" stays as-is.
    - "03" -> "Marzo".
    - Unknown values are returned unchanged.
    """

    if value == POP_ALL:
        return value
    label = MONTH_LABELS_ES.get(value)
    if label is None:
        return value
    return label


def population_date_window(
    pop_year: str, pop_month: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Compute the global population date window.

    Returns a triple: (date_start, date_end, month_filter)

    - date_start/date_end are ISO strings ("YYYY-MM-DD") to be passed into
      store.load_table() (predicate pushdown via Fecha_day).
    - month_filter is only used for the special case: pop_year == "Todos" and
      pop_month != "Todos" (cannot express as a single contiguous window).

    Design:
    - Pure stdlib, no pandas.
    - Inclusive end date (matches store filter f <= date_end).
    """

    year = (pop_year or "").strip()
    month = (pop_month or "").strip()

    if year == POP_ALL:
        if month == POP_ALL:
            return None, None, None
        # Cross-year "all years" + one month: handled post-load in pandas.
        return None, None, month

    # From here: a concrete year.
    try:
        y = int(year)
    except Exception:
        # Fail-safe: treat as no filter.
        return None, None, None

    if month == POP_ALL:
        return date(y, 1, 1).isoformat(), date(y, 12, 31).isoformat(), None

    try:
        m = int(month)
        if not (1 <= m <= 12):
            raise ValueError
    except Exception:
        return date(y, 1, 1).isoformat(), date(y, 12, 31).isoformat(), None

    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, 1).isoformat(), date(y, m, last_day).isoformat(), None
