from __future__ import annotations

import pandas as pd

from nps_lens.ui.charts import chart_driver_bar
from nps_lens.ui.theme import get_theme


def test_driver_gap_chart_makes_zero_gaps_explicit() -> None:
    frame = pd.DataFrame(
        [
            {"value": "Uso", "n": 49, "nps": -100.0, "gap_vs_base": 0.0},
            {
                "value": "Funcionamiento continuo",
                "n": 63,
                "nps": -100.0,
                "gap_vs_base": 0.0,
            },
        ]
    )

    figure = chart_driver_bar(frame, get_theme("light"))

    assert figure is not None
    assert len(figure.data) == 2
    assert list(figure.data[1].text) == ["0 pts", "0 pts"]
    assert figure.layout.xaxis.range == (-1.0, 1.0)
    assert "Sin brechas" in figure.layout.annotations[0].text
