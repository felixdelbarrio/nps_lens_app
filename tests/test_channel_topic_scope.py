from __future__ import annotations

import pandas as pd

from nps_lens.analytics.channel_topic_scope import (
    restrict_to_topics,
    topics_observed_in_channel,
)


def test_channel_selects_topics_without_filtering_the_metric_population() -> None:
    frame = pd.DataFrame(
        {
            "Canal": ["Web", "App", "App"],
            "Palanca": ["Acceso", "Acceso", "Pagos"],
            "NPS": [0, 10, 0],
        }
    )

    topic_keys = topics_observed_in_channel(frame, "Palanca", "Web")
    metric_population = restrict_to_topics(frame, "Palanca", topic_keys)

    assert topic_keys == {"acceso"}
    assert metric_population["Canal"].tolist() == ["Web", "App"]
    assert metric_population["NPS"].mean() == 5.0


def test_missing_channel_taxonomy_keeps_the_full_population() -> None:
    frame = pd.DataFrame({"Canal": ["", ""], "Palanca": ["Acceso", "Pagos"]})

    assert topics_observed_in_channel(frame, "Palanca", "Web") is None
    assert restrict_to_topics(frame, "Palanca", None) is frame
