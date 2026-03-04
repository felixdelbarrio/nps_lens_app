from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from nps_lens.analytics.text_mining import extract_topics


@dataclass(frozen=True)
class RouteCandidate:
    route_signature: str
    n: int
    detractor_rate: float
    score: float
    evidence: dict[str, object]


def build_routes(
    nps_df: pd.DataFrame,
    incidents_df: Optional[pd.DataFrame] = None,
    lever_col: str = "Palanca",
    sublever_col: str = "Subpalanca",
    comment_col: str = "Comment",
) -> list[RouteCandidate]:
    """MVP journey routes:
    palanca -> subpalanca -> topic -> incident_category (if incidents provided)
    """
    data = nps_df.copy()
    data["is_detractor"] = (pd.to_numeric(data["NPS"], errors="coerce") <= 6).astype(int)
    # topics from comments
    topics = extract_topics(data[comment_col], n_clusters=8)
    # map row -> topic cluster via simple keyword matching on top terms
    # (best-effort; avoids heavy per-row clustering)
    cluster_terms: dict[int, list[str]] = {t.cluster_id: t.top_terms[:5] for t in topics}

    def assign_topic(txt: object) -> str:
        if not isinstance(txt, str) or not txt:
            return "__NO_TEXT__"
        low = txt.lower()
        for cid, terms in cluster_terms.items():
            for term in terms:
                if term in low:
                    return f"topic_{cid}"
        return "__OTHER__"

    data["topic"] = data[comment_col].map(assign_topic)

    # incident category join via simple geo/channel + time window + lever keywords
    if incidents_df is not None and not incidents_df.empty:
        # Future: join incidents by geo/canal/ventana temporal + similitud semántica.
        # MVP keeps incident data available for drill-down, but does not hard-join at row-level.
        _ = incidents_df

    # Robust counting: some views load a projected dataset without "ID".
    # In that case, count rows via a stable existing column.
    count_col = "ID" if "ID" in data.columns else "is_detractor"
    grouped = (
        data.groupby([lever_col, sublever_col, "topic"], dropna=False)
        .agg(n=(count_col, "count"), detractor_rate=("is_detractor", "mean"))
        .reset_index()
    )
    routes: list[RouteCandidate] = []
    for _, r in grouped.iterrows():
        n = int(r["n"])
        if n < 150:
            continue
        detr = float(r["detractor_rate"])
        # score: detractor concentration * volume
        score = float(detr * np.log1p(n))
        sig = f"{r[lever_col]}::{r[sublever_col]}::{r['topic']}"
        evidence: dict[str, object] = {
            "lever": str(r[lever_col]),
            "sublever": str(r[sublever_col]),
            "topic": str(r["topic"]),
        }
        routes.append(
            RouteCandidate(
                route_signature=sig,
                n=n,
                detractor_rate=detr,
                score=score,
                evidence=evidence,
            )
        )

    routes.sort(key=lambda x: (x.score, x.n), reverse=True)
    return routes[:25]
