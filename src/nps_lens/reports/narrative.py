"""Experience sequence shared by evidence, slides and editorial publication.

Topic scores use all valid opinions of observed topics, scenario scores only linked
comments. Topics without evidence stay in the overview, never acquire invented cases.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from typing import Any

import pandas as pd

from nps_lens.analytics.channel_topic_scope import restrict_to_topics, topics_observed_in_channel
from nps_lens.analytics.drivers import grouped_driver_stats
from nps_lens.analytics.signal_quality import actionable_rows
from nps_lens.core.nps_math import valid_nps_scores
from nps_lens.domain.comment_scores import score_group_label


def text(value: Any) -> str:
    if value is None or (pd.api.types.is_scalar(value) and pd.isna(value)):
        return ""
    return str(value).strip()


def normalized(value: object) -> str:
    return " ".join(text(value).casefold().split())


def score(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def topic_name(row: Mapping[Any, Any]) -> str:
    name = next(
        (
            text(row.get(key))
            for key in ("anchor_topic", "palanca", "nps_topic")
            if text(row.get(key))
        ),
        "Sin tópico",
    )
    return name.split(" > ")[0].strip()


def experience_topics(
    current: pd.DataFrame, *, channel: str = "Todos", dimension: str = "Palanca"
) -> pd.DataFrame:
    columns = ["value", "n", "score", "nps", "detractor_rate"]
    if current.empty or not {dimension, "NPS"}.issubset(current.columns):
        return pd.DataFrame(columns=columns)
    keys = topics_observed_in_channel(current, dimension, channel)
    source = actionable_rows(restrict_to_topics(current, dimension, keys)).copy()
    source["NPS"] = valid_nps_scores(source["NPS"])
    stats = grouped_driver_stats(source, dimension)
    means = source.groupby(dimension, observed=True)["NPS"].mean()
    stats["score"] = stats[dimension].map(means)
    stats = stats.rename(columns={dimension: "value"})
    stats["_name"] = stats["value"].map(normalized)
    return stats.sort_values(
        ["score", "n", "_name", "value"],
        ascending=[True, False, True, True],
        na_position="last",
        kind="stable",
    )[columns].reset_index(drop=True)


def ordered_scenarios(chains: pd.DataFrame, topics: pd.DataFrame) -> pd.DataFrame:
    if chains.empty:
        return chains.copy()
    eligible = actionable_rows(chains).copy()
    ranks = {normalized(row["value"]): index for index, row in enumerate(topics.to_dict("records"))}
    records = eligible.to_dict("records")
    keys = []
    for row in records:
        name = topic_name(row)
        average = score(row.get("avg_nps"))
        if average is not None and not 0 <= average <= 10:
            average = None
        identity = text(row.get("scenario_id")) or text(row.get("chain_id"))
        if not identity:
            comments = row.get("comment_records")
            ids = sorted(
                str(r.get("comment_id", ""))
                for r in (comments if isinstance(comments, list) else [])
                if isinstance(r, dict)
            )
            identity = hashlib.sha256(
                (
                    str(row.get("nps_topic", ""))
                    + str(row.get("affected_task", ""))
                    + str(row.get("observed_symptom", ""))
                    + "|".join(ids)
                ).encode()
            ).hexdigest()[:16]
        keys.append(
            (
                ranks.get(normalized(name), len(ranks)),
                normalized(name),
                average,
                -(score(row.get("linked_comments")) or 0),
                normalized(row.get("nps_topic")),
                identity,
                name,
            )
        )
    for index, column in enumerate(
        (
            "_topic_rank",
            "_topic_name",
            "_scenario_score",
            "_volume",
            "_name",
            "scenario_id",
            "narrative_topic",
        )
    ):
        eligible[column] = [key[index] for key in keys]
    eligible = eligible.sort_values(
        ["_topic_rank", "_topic_name", "_scenario_score", "_volume", "_name", "scenario_id"],
        na_position="last",
        kind="stable",
    )
    eligible["narrative_rank"] = range(1, len(eligible) + 1)
    return eligible.drop(
        columns=["_topic_rank", "_topic_name", "_scenario_score", "_volume", "_name"]
    ).reset_index(drop=True)


def unique_comments(value: object) -> list[dict[str, Any]]:
    records = [r for r in value if isinstance(r, dict)] if isinstance(value, list) else []
    scores = valid_nps_scores(pd.Series([record.get("nps") for record in records], dtype=object))
    records = [
        {**record, "nps": int(value) if pd.notna(value) else None}
        for record, value in zip(records, scores, strict=True)
    ]
    ordered = sorted(
        records,
        key=lambda r: (
            score(r.get("nps")) is None,
            score(r.get("nps")) or 0,
            normalized(r.get("comment")),
            str(r.get("comment_id") or ""),
        ),
    )
    unique: dict[str, dict[str, Any]] = {}
    for record in ordered:
        identity = str(record.get("comment_id") or normalized(record.get("comment")))
        unique.setdefault(identity, record)
    return list(unique.values())


def comment_groups(value: object) -> list[dict[str, Any]]:
    groups: dict[float | None, list[dict[str, Any]]] = {}
    for record in unique_comments(value):
        groups.setdefault(score(record.get("nps")), []).append(record)
    return [
        {
            "score": value,
            "count": len(records),
            "label": score_group_label(value, len(records)),
            "records": records,
        }
        for value, records in groups.items()
    ]
