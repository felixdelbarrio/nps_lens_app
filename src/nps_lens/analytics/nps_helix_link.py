from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import pandas as pd
import ruptures as rpt
from sklearn.feature_extraction.text import TfidfVectorizer

from nps_lens.analytics.linking_policy import (
    LINK_MAX_DAYS_APART,
    LINK_MIN_SIMILARITY,
    LINK_TOP_K_PER_INCIDENT,
)
from nps_lens.core.nps_math import focus_mask, normalize_focus_group


def _safe_corr(xx: np.ndarray, yy: np.ndarray) -> float:
    """Numerically stable Pearson correlation for finite arrays."""
    if xx.size == 0 or yy.size == 0:
        return float("nan")
    x = np.asarray(xx, dtype=float)
    y = np.asarray(yy, dtype=float)
    if x.size != y.size:
        return float("nan")
    if x.size < 2:
        return float("nan")
    x_mean = float(np.mean(x))
    y_mean = float(np.mean(y))
    x0 = x - x_mean
    y0 = y - y_mean
    x_var = float(np.dot(x0, x0))
    y_var = float(np.dot(y0, y0))
    denom = float(np.sqrt(x_var * y_var))
    if not np.isfinite(denom) or denom <= 0.0:
        return float("nan")
    return float(np.dot(x0, y0) / denom)


def estimate_best_lag_by_topic(
    by_topic_weekly: pd.DataFrame,
    max_lag_weeks: int = 6,
    min_points: int = 8,
) -> pd.DataFrame:
    """Estimate best positive lag where incidents *precede* focus_rate.

    Returns per topic:
      - best_lag_weeks (0..max_lag_weeks)
      - corr (Pearson) at that lag
      - points (used pairs)
    """
    if by_topic_weekly.empty:
        return pd.DataFrame(columns=["nps_topic", "best_lag_weeks", "corr", "points"])

    rows = []
    df = by_topic_weekly.copy()
    df = df.sort_values(["nps_topic", "week"])
    for topic, g in df.groupby("nps_topic"):
        g = g.sort_values("week")
        x = g["incidents"].astype(float).values
        y = g["focus_rate"].astype(float).values
        best = (0, float("nan"), 0)
        for lag in range(0, int(max_lag_weeks) + 1):
            if lag == 0:
                xx, yy = x, y
            else:
                xx, yy = x[:-lag], y[lag:]
            mask = np.isfinite(xx) & np.isfinite(yy)
            if mask.sum() < int(min_points):
                continue
            c = _safe_corr(xx[mask], yy[mask])
            if not np.isfinite(c):
                continue
            if (not np.isfinite(best[1])) or (c > best[1]):
                best = (lag, c, int(mask.sum()))
        rows.append(
            {"nps_topic": topic, "best_lag_weeks": best[0], "corr": best[1], "points": best[2]}
        )
    out = pd.DataFrame(rows)
    return out


def estimate_best_lag_days_by_topic(
    by_topic_daily: pd.DataFrame,
    max_lag_days: int = 21,
    min_points: int = 30,
) -> pd.DataFrame:
    """Daily version of lag estimation.

    Expects by_topic_daily with columns:
      - date (datetime-like)
      - nps_topic
      - focus_rate
      - incidents

    We search lag in days (0..max_lag_days) maximizing corr(incidents(t), focus_rate(t+lag)).
    """
    if by_topic_daily.empty:
        return pd.DataFrame(columns=["nps_topic", "best_lag_days", "corr", "points"])

    rows = []
    df = by_topic_daily.copy()
    df = df.sort_values(["nps_topic", "date"])
    for topic, g in df.groupby("nps_topic"):
        g = g.sort_values("date")
        x = g["incidents"].astype(float).values
        y = g["focus_rate"].astype(float).values
        best = (0, float("nan"), 0)
        for lag in range(0, int(max_lag_days) + 1):
            if lag == 0:
                xx, yy = x, y
            else:
                xx, yy = x[:-lag], y[lag:]
            mask = np.isfinite(xx) & np.isfinite(yy)
            if mask.sum() < int(min_points):
                continue
            c = _safe_corr(xx[mask], yy[mask])
            if not np.isfinite(c):
                continue
            if (not np.isfinite(best[1])) or (c > best[1]):
                best = (lag, c, int(mask.sum()))
        rows.append(
            {"nps_topic": topic, "best_lag_days": best[0], "corr": best[1], "points": best[2]}
        )
    return pd.DataFrame(rows)


def detect_detractor_changepoints_by_topic(
    by_topic_weekly: pd.DataFrame,
    pen: float = 6.0,
    model: str = "l2",
    min_points: int = 10,
) -> pd.DataFrame:
    """Detect changepoints on focus_rate series per topic (weekly).

    Returns rows:
      - nps_topic
      - changepoints (list of dates as ISO strings)
    """
    if by_topic_weekly.empty:
        return pd.DataFrame(columns=["nps_topic", "changepoints"])

    rows = []
    df = by_topic_weekly.copy().sort_values(["nps_topic", "week"])
    for topic, g in df.groupby("nps_topic"):
        g = g.sort_values("week")
        ts = g["focus_rate"].astype(float).dropna()
        if len(ts) < int(min_points):
            rows.append({"nps_topic": topic, "changepoints": []})
            continue
        algo = rpt.Pelt(model=model).fit(ts.values.reshape(-1, 1))
        bkps = algo.predict(pen=float(pen))
        pts = []
        # bkps include last index; map to week index in g aligned to ts
        week_index = g.loc[ts.index, "week"].tolist()
        for idx in bkps[:-1]:
            w = week_index[idx - 1]
            try:
                pts.append(pd.to_datetime(w).date().isoformat())
            except Exception:
                pts.append(str(w))
        rows.append({"nps_topic": topic, "changepoints": pts})
    return pd.DataFrame(rows)


def detect_detractor_changepoints_with_bootstrap(
    by_topic_weekly: pd.DataFrame,
    pen: float = 6.0,
    model: str = "l2",
    min_points: int = 10,
    n_boot: int = 200,
    block_size: int = 2,
    tol_periods: int = 1,
    random_state: int = 7,
) -> pd.DataFrame:
    """Detect changepoints and estimate their stability via moving-block bootstrap.

    Stability is the fraction of bootstrap runs where a changepoint is detected within +/- tol_periods
    positions of the original changepoint.

    Labels:
      - High: stability >= 0.70
      - Medium: stability >= 0.40
      - Low: otherwise

    Returns per topic:
      - changepoints: list[str] (ISO dates)
      - changepoint_stability: list[float]
      - changepoint_level: list[str]
      - max_cp_stability: float
      - max_cp_level: str
    """
    if by_topic_weekly.empty:
        return pd.DataFrame(
            columns=[
                "nps_topic",
                "changepoints",
                "changepoint_stability",
                "changepoint_level",
                "max_cp_stability",
                "max_cp_level",
            ]
        )

    rng = np.random.RandomState(int(random_state))
    rows = []
    df = by_topic_weekly.copy().sort_values(["nps_topic", "week"])
    for topic, g in df.groupby("nps_topic"):
        g = g.sort_values("week")
        ts = g["focus_rate"].astype(float).dropna()
        if len(ts) < int(min_points):
            rows.append(
                {
                    "nps_topic": topic,
                    "changepoints": [],
                    "changepoint_stability": [],
                    "changepoint_level": [],
                    "max_cp_stability": np.nan,
                    "max_cp_level": "",
                }
            )
            continue

        algo = rpt.Pelt(model=model).fit(ts.values.reshape(-1, 1))
        bkps = algo.predict(pen=float(pen))
        cp_pos = [int(i) for i in bkps[:-1] if int(i) > 0]
        week_index = g.loc[ts.index, "week"].tolist()
        cp_weeks = []
        for idx in cp_pos:
            w = week_index[idx - 1]
            try:
                cp_weeks.append(pd.to_datetime(w).date().isoformat())
            except Exception:
                cp_weeks.append(str(w))

        # Bootstrap stability
        n = len(ts)
        if not cp_pos:
            rows.append(
                {
                    "nps_topic": topic,
                    "changepoints": [],
                    "changepoint_stability": [],
                    "changepoint_level": [],
                    "max_cp_stability": np.nan,
                    "max_cp_level": "",
                }
            )
            continue

        hits = np.zeros(len(cp_pos), dtype=float)
        # Precompute start indices for blocks
        starts_max = max(1, n - int(block_size))
        for _ in range(int(n_boot)):
            # moving-block bootstrap: sample contiguous blocks
            idxs = []
            while len(idxs) < n:
                s = int(rng.randint(0, starts_max))
                idxs.extend(list(range(s, min(n, s + int(block_size)))))
            idxs = idxs[:n]
            boot = ts.values[idxs]
            try:
                algo_b = rpt.Pelt(model=model).fit(boot.reshape(-1, 1))
                bkps_b = algo_b.predict(pen=float(pen))
                cp_b = [int(i) for i in bkps_b[:-1] if int(i) > 0]
            except Exception:
                cp_b = []
            if not cp_b:
                continue
            for j, cp in enumerate(cp_pos):
                if any(abs(int(b) - int(cp)) <= int(tol_periods) for b in cp_b):
                    hits[j] += 1.0

        stability = (hits / float(n_boot)).tolist()
        level = []
        for s in stability:
            if s >= 0.70:
                level.append("High")
            elif s >= 0.40:
                level.append("Medium")
            else:
                level.append("Low")

        max_s = float(np.max(stability)) if stability else np.nan
        max_level = "High" if max_s >= 0.70 else ("Medium" if max_s >= 0.40 else "Low")

        rows.append(
            {
                "nps_topic": topic,
                "changepoints": cp_weeks,
                "changepoint_stability": stability,
                "changepoint_level": level,
                "max_cp_stability": max_s,
                "max_cp_level": max_level,
            }
        )

    return pd.DataFrame(rows)


def incidents_lead_changepoints_flag(
    by_topic_weekly: pd.DataFrame,
    changepoints_df: pd.DataFrame,
    window_weeks: int = 4,
) -> pd.DataFrame:
    """For each topic, flag whether incidents peak tends to happen BEFORE changepoints.

    Heuristic: for each changepoint date cp, compare max incidents in [cp-window, cp)
    vs (cp, cp+window]. Lead if pre >= post.
    """
    if by_topic_weekly.empty or changepoints_df.empty:
        return pd.DataFrame(columns=["nps_topic", "incidents_lead_changepoint_share"])

    df = by_topic_weekly.copy()
    df["week"] = pd.to_datetime(df["week"], errors="coerce")
    out_rows = []
    cps = changepoints_df.set_index("nps_topic")["changepoints"].to_dict()
    for topic, g in df.groupby("nps_topic"):
        g = g.sort_values("week")
        cplist = cps.get(topic, []) or []
        if not cplist:
            out_rows.append({"nps_topic": topic, "incidents_lead_changepoint_share": np.nan})
            continue
        leads = []
        for cp_s in cplist:
            cp = pd.to_datetime(cp_s, errors="coerce")
            if pd.isna(cp):
                continue
            pre = g[(g["week"] >= cp - pd.Timedelta(days=7 * window_weeks)) & (g["week"] < cp)][
                "incidents"
            ].astype(float)
            post = g[(g["week"] > cp) & (g["week"] <= cp + pd.Timedelta(days=7 * window_weeks))][
                "incidents"
            ].astype(float)
            if pre.empty or post.empty:
                continue
            leads.append(float(pre.max()) >= float(post.max()))
        share = float(np.mean(leads)) if leads else np.nan
        out_rows.append({"nps_topic": topic, "incidents_lead_changepoint_share": share})
    return pd.DataFrame(out_rows)


def _split_csvish(value: object) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def tokenset(value: object) -> Tuple[str, ...]:
    toks = _split_csvish(value)
    return tuple(sorted({t for t in toks if t}))


def build_nps_topic(df: pd.DataFrame) -> pd.Series:
    pal = df.get("Palanca", pd.Series([""] * len(df), index=df.index)).astype(str)
    sub = df.get("Subpalanca", pd.Series([""] * len(df), index=df.index)).astype(str)
    topic = (pal.fillna("").str.strip() + " > " + sub.fillna("").str.strip()).str.strip()
    topic = topic.str.replace(r"^>\s*", "", regex=True).str.replace(r"\s*>$", "", regex=True)
    return topic.replace({"nan > nan": ""}).fillna("")


def _ordered_cols_ci(df: pd.DataFrame, candidates: list[str]) -> list[str]:
    lower_map = {str(c).strip().lower(): str(c) for c in df.columns}
    out: list[str] = []
    seen: set[str] = set()
    for cand in candidates:
        hit = lower_map.get(str(cand).strip().lower())
        if not hit:
            continue
        key = hit.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(hit)
    return out


def _txt_series(df: pd.DataFrame, col: str) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    s = df[col].astype(str).fillna("").str.strip()
    return s.replace({"nan": "", "NaN": "", "None": "", "NaT": ""})


def build_incident_display_text(df: pd.DataFrame) -> pd.Series:
    """Best descriptive text for a Helix incident (prefer detailed narrative)."""

    preferred = _ordered_cols_ci(
        df,
        [
            "Detailed Description",
            "Detailed Decription",  # common export typo
            "bbva_detaileddescription",
            "description",
            "Descripción",
            "Short Description",
            "bbva_shortdescription",
            "summary",
        ],
    )
    if not preferred:
        return pd.Series([""] * len(df), index=df.index)

    stacked = pd.concat([_txt_series(df, col) for col in preferred], axis=1)
    arr = stacked.to_numpy(dtype=object)
    if arr.size == 0:
        return pd.Series([""] * len(df), index=df.index)
    non_empty = arr != ""
    first_idx = non_empty.argmax(axis=1)
    has_any = non_empty.any(axis=1)
    vals = np.full(len(stacked), "", dtype=object)
    rows = np.arange(len(stacked))
    vals[has_any] = arr[rows[has_any], first_idx[has_any]]
    display = pd.Series(vals, index=df.index)
    return display.astype(str).str.replace(r"\s+", " ", regex=True).str.strip()


def build_incident_topic(df: pd.DataFrame) -> pd.Series:
    t1 = df.get("Product Categorization Tier 1", pd.Series([""] * len(df), index=df.index)).astype(
        str
    )
    t2 = df.get("Product Categorization Tier 2", pd.Series([""] * len(df), index=df.index)).astype(
        str
    )
    t3 = df.get("Product Categorization Tier 3", pd.Series([""] * len(df), index=df.index)).astype(
        str
    )
    base = (
        t1.fillna("").str.strip()
        + " > "
        + t2.fillna("").str.strip()
        + " > "
        + t3.fillna("").str.strip()
    ).str.replace(r"\s*>\s*>\s*", " > ", regex=True)
    base = base.str.replace(r"^>\s*", "", regex=True).str.replace(r"\s*>$", "", regex=True)
    # fallback to service / summary
    svc = (
        df.get("service", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    desc = build_incident_display_text(df)
    base = base.where(base.str.len() > 0, svc.where(svc.str.len() > 0, desc))
    return base.fillna("")


def build_nps_text(df: pd.DataFrame) -> pd.Series:
    comment = df.get("Comment", pd.Series([""] * len(df), index=df.index)).astype(str).fillna("")
    pal = df.get("Palanca", pd.Series([""] * len(df), index=df.index)).astype(str).fillna("")
    sub = df.get("Subpalanca", pd.Series([""] * len(df), index=df.index)).astype(str).fillna("")
    return (pal + " " + sub + " " + comment).str.replace(r"\s+", " ", regex=True).str.strip()


def build_incident_text(df: pd.DataFrame) -> pd.Series:
    parts = [build_incident_display_text(df)]
    aux_cols = _ordered_cols_ci(
        df,
        [
            "Resolution",
            "Resolución",
            "resolution",
            "summary",
            "Short Description",
            "bbva_shortdescription",
        ],
    )
    parts.extend([_txt_series(df, col) for col in aux_cols])
    if not parts or len(parts[0]) == 0:
        return pd.Series([""] * len(df), index=df.index)
    s = parts[0]
    for p in parts[1:]:
        s = s + " " + p
    return s.str.replace(r"\s+", " ", regex=True).str.strip()


@dataclass(frozen=True)
class EvidenceLink:
    nps_id: str
    incident_id: str
    similarity: float
    nps_topic: str
    incident_topic: str


def _safe_id(series: pd.Series) -> pd.Series:
    return series.astype(str).fillna("").replace({"nan": ""})


def _sparse_row_topk(row, k: int) -> tuple[np.ndarray, np.ndarray]:
    """Return top-k indices/values from a sparse row (descending by value)."""
    if row is None or row.nnz == 0:
        return np.array([], dtype=int), np.array([], dtype=float)
    idx = row.indices
    vals = row.data
    if len(vals) <= int(k):
        order = np.argsort(-vals)
        return idx[order], vals[order]
    pick = np.argpartition(vals, -int(k))[-int(k) :]
    order = pick[np.argsort(-vals[pick])]
    return idx[order], vals[order]


def _sample_positions(total: int, target: int) -> np.ndarray:
    if total <= target:
        return np.arange(total, dtype=int)
    # Deterministic down-sampling spread across the full index range.
    pos = np.linspace(0, total - 1, num=target, dtype=int)
    return np.unique(pos)


def link_incidents_to_nps_topics(
    nps_detractors: pd.DataFrame,
    helix_incidents: pd.DataFrame,
    min_similarity: float = LINK_MIN_SIMILARITY,
    max_features: int = 50000,
    top_k_per_incident: int = LINK_TOP_K_PER_INCIDENT,
    max_nps_rows_for_evidence: int = 12000,
    evidence_chunk_size: int = 128,
    max_days_apart: int | None = LINK_MAX_DAYS_APART,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return:
    - assignments per incident to best NPS topic (and similarity)
    - evidence links (incident to specific detractor comments) for the evidence wall
    """

    if nps_detractors.empty or helix_incidents.empty:
        return (
            pd.DataFrame(columns=["incident_id", "nps_topic", "similarity"]),
            pd.DataFrame(
                columns=["nps_id", "incident_id", "similarity", "nps_topic", "incident_topic"]
            ),
        )

    nps = nps_detractors.copy()
    helix = helix_incidents.copy()

    nps["nps_id"] = _safe_id(nps.get("ID", pd.Series(nps.index, index=nps.index)))
    helix["incident_id"] = _safe_id(
        helix.get(
            "Incident Number",
            helix.get("ID de la Incidencia", pd.Series(helix.index, index=helix.index)),
        )
    )
    nps["nps_date"] = pd.to_datetime(
        nps.get("Fecha", pd.Series([pd.NaT] * len(nps), index=nps.index)),
        errors="coerce",
    ).dt.normalize()
    helix["incident_date"] = pd.to_datetime(
        helix.get("Fecha", pd.Series([pd.NaT] * len(helix), index=helix.index)),
        errors="coerce",
    ).dt.normalize()

    nps["nps_topic"] = build_nps_topic(nps)
    helix["incident_topic"] = build_incident_topic(helix)

    nps_text = build_nps_text(nps).fillna("")
    helix_text = build_incident_text(helix).fillna("")
    corpus = nps_text.tolist() + helix_text.tolist()
    if not any(str(t).strip() for t in corpus):
        return (
            pd.DataFrame(columns=["incident_id", "nps_topic", "similarity", "incident_topic"]),
            pd.DataFrame(
                columns=["nps_id", "incident_id", "similarity", "nps_topic", "incident_topic"]
            ),
        )

    # Build topic docs from concatenated detractor text.
    topic_docs = (
        nps.assign(_txt=nps_text)
        .groupby("nps_topic", dropna=False)["_txt"]
        .apply(lambda s: " ".join([t for t in s.tolist() if t]))
    )
    topics = topic_docs.index.tolist()
    if not topics:
        return (
            pd.DataFrame(columns=["incident_id", "nps_topic", "similarity", "incident_topic"]),
            pd.DataFrame(
                columns=["nps_id", "incident_id", "similarity", "nps_topic", "incident_topic"]
            ),
        )

    # Vectorize once for NPS comments + incidents. Dynamic min_df keeps small extracts valid.
    min_df = 1 if len(corpus) < 250 else 2
    vec = TfidfVectorizer(
        lowercase=True,
        max_features=max_features,
        ngram_range=(1, 2),
        min_df=min_df,
        stop_words=None,
    )
    try:
        X = vec.fit_transform(corpus)
    except ValueError:
        # Empty vocabulary after cleaning
        return (
            pd.DataFrame(columns=["incident_id", "nps_topic", "similarity", "incident_topic"]),
            pd.DataFrame(
                columns=["nps_id", "incident_id", "similarity", "nps_topic", "incident_topic"]
            ),
        )

    X_nps = X[: len(nps)]
    X_inc = X[len(nps) :]
    X_topics = vec.transform(topic_docs.values.tolist())

    # Assignment incident -> topic with sparse similarity (no dense NxM matrix).
    sim_topic = X_inc @ X_topics.T
    assign_rows: list[dict[str, object]] = []
    for i in range(sim_topic.shape[0]):
        idx, vals = _sparse_row_topk(sim_topic.getrow(i), 1)
        if len(idx) == 0:
            continue
        topic_idx = int(idx[0])
        sim = float(vals[0])
        assign_rows.append(
            {
                "incident_id": str(helix.iloc[i]["incident_id"]),
                "nps_topic": str(topics[topic_idx]),
                "similarity": sim,
                "incident_topic": str(helix.iloc[i]["incident_topic"]),
            }
        )
    assign_df = pd.DataFrame(assign_rows)
    if assign_df.empty:
        assign_df = pd.DataFrame(
            columns=["incident_id", "nps_topic", "similarity", "incident_topic"]
        )
    assign_df = assign_df[assign_df["similarity"] >= float(min_similarity)].reset_index(drop=True)

    # Evidence links: incident -> top detractor comments with sparse/chunked similarity.
    # Optional deterministic down-sampling keeps worst-case memory/CPU bounded.
    nps_pos = _sample_positions(int(len(nps)), int(max_nps_rows_for_evidence))
    X_nps_ev = X_nps[nps_pos]
    nps_ev = nps.iloc[nps_pos].copy()

    links: List[EvidenceLink] = []
    chunk = max(1, int(evidence_chunk_size))
    per_incident_k = max(1, int(top_k_per_incident))
    max_days = int(max_days_apart) if max_days_apart is not None else None
    for start in range(0, X_inc.shape[0], chunk):
        end = min(start + chunk, X_inc.shape[0])
        sim_block = X_inc[start:end] @ X_nps_ev.T
        for bi in range(sim_block.shape[0]):
            row = sim_block.getrow(bi)
            idx, vals = _sparse_row_topk(row, per_incident_k)
            if len(idx) == 0:
                continue
            inc_row = start + bi
            inc_id = str(helix.iloc[inc_row]["incident_id"])
            inc_topic = str(helix.iloc[inc_row]["incident_topic"])
            inc_date = pd.to_datetime(helix.iloc[inc_row].get("incident_date"), errors="coerce")
            for j, sim in zip(idx.tolist(), vals.tolist()):
                s = float(sim)
                if s < float(min_similarity):
                    continue
                nps_row = nps_ev.iloc[int(j)]
                if max_days is not None:
                    nps_date = pd.to_datetime(nps_row.get("nps_date"), errors="coerce")
                    if pd.isna(inc_date) or pd.isna(nps_date):
                        continue
                    if int(abs((inc_date - nps_date).days)) > max_days:
                        continue
                links.append(
                    EvidenceLink(
                        nps_id=str(nps_row["nps_id"]),
                        incident_id=inc_id,
                        similarity=s,
                        nps_topic=str(nps_row["nps_topic"]),
                        incident_topic=inc_topic,
                    )
                )

    links_df = pd.DataFrame([e.__dict__ for e in links])
    if not links_df.empty:
        links_df = links_df.sort_values(["similarity"], ascending=False).reset_index(drop=True)

    return assign_df, links_df


def weekly_aggregates(
    nps_df: pd.DataFrame,
    helix_df: pd.DataFrame,
    incident_assignments: pd.DataFrame,
    date_col_nps: str = "Fecha",
    date_col_helix: str = "Fecha",
    focus_group: str = "detractor",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return weekly aggregates:
    - overall: detractor_rate + incidents
    - by_topic: detractor_rate + incidents (incidents mapped to NPS topic)
    """
    nps = nps_df.copy()
    helix = helix_df.copy()

    nps[date_col_nps] = pd.to_datetime(nps[date_col_nps], errors="coerce")
    helix[date_col_helix] = pd.to_datetime(helix[date_col_helix], errors="coerce")

    nps = nps.dropna(subset=[date_col_nps])
    helix = helix.dropna(subset=[date_col_helix])

    nps["week"] = nps[date_col_nps].dt.to_period("W").dt.start_time
    helix["week"] = helix[date_col_helix].dt.to_period("W").dt.start_time

    group = normalize_focus_group(focus_group)
    nps["is_focus"] = focus_mask(nps, focus_group=group)

    count_col = "ID" if "ID" in nps.columns else date_col_nps
    overall_nps = (
        nps.groupby("week")
        .agg(
            responses=(count_col, "count"),
            focus_count=("is_focus", "sum"),
            nps_mean=("NPS", "mean"),
        )
        .reset_index()
    )
    overall_nps["focus_rate"] = overall_nps["focus_count"] / overall_nps["responses"].replace(
        {0: np.nan}
    )

    overall_helix = helix.groupby("week").agg(incidents=("Incident Number", "count")).reset_index()
    overall = (
        pd.merge(overall_nps, overall_helix, on="week", how="outer").sort_values("week").fillna(0)
    )

    # By topic (NPS topics)
    nps["nps_topic"] = build_nps_topic(nps)
    by_topic_nps = (
        nps.groupby(["week", "nps_topic"])
        .agg(
            responses=(count_col, "count"),
            focus_count=("is_focus", "sum"),
            nps_mean=("NPS", "mean"),
        )
        .reset_index()
    )
    by_topic_nps["focus_rate"] = by_topic_nps["focus_count"] / by_topic_nps["responses"].replace(
        {0: np.nan}
    )

    by_topic = by_topic_nps.copy()
    if not incident_assignments.empty:
        ia = incident_assignments.copy()
        # merge incident dates
        ia = ia.merge(
            helix[["Incident Number", "week"]].astype({"Incident Number": str}),
            left_on="incident_id",
            right_on="Incident Number",
            how="left",
        )
        by_topic_inc = (
            ia.groupby(["week", "nps_topic"]).agg(incidents=("incident_id", "count")).reset_index()
        )
        by_topic = by_topic.merge(by_topic_inc, on=["week", "nps_topic"], how="left")
    # Ensure incidents column exists even when there are no incident assignments.
    # NOTE: DataFrame.get("incidents", 0) returns an int when missing, which does not
    # support .fillna; hence this explicit branch.
    if "incidents" not in by_topic.columns:
        by_topic["incidents"] = 0
    else:
        by_topic["incidents"] = by_topic["incidents"].fillna(0)
    return overall, by_topic


def daily_aggregates(
    nps_df: pd.DataFrame,
    helix_df: pd.DataFrame,
    incident_assignments: pd.DataFrame,
    date_col_nps: str = "Fecha",
    date_col_helix: str = "Fecha",
    focus_group: str = "detractor",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Daily aggregates analogous to weekly_aggregates.

    Returns:
      - overall_daily: date, responses, detractors, detractor_rate, incidents
      - by_topic_daily: date, nps_topic, responses, detractors, detractor_rate, incidents
    """
    nps = nps_df.copy()
    helix = helix_df.copy()
    nps[date_col_nps] = pd.to_datetime(nps[date_col_nps], errors="coerce")
    helix[date_col_helix] = pd.to_datetime(helix[date_col_helix], errors="coerce")
    nps = nps.dropna(subset=[date_col_nps])
    helix = helix.dropna(subset=[date_col_helix])
    nps["date"] = nps[date_col_nps].dt.normalize()
    helix["date"] = helix[date_col_helix].dt.normalize()

    group = normalize_focus_group(focus_group)
    nps["is_focus"] = focus_mask(nps, focus_group=group)

    count_col = "ID" if "ID" in nps.columns else date_col_nps
    overall_nps = (
        nps.groupby("date")
        .agg(
            responses=(count_col, "count"),
            focus_count=("is_focus", "sum"),
            nps_mean=("NPS", "mean"),
        )
        .reset_index()
    )
    overall_nps["focus_rate"] = overall_nps["focus_count"] / overall_nps["responses"].replace(
        {0: np.nan}
    )
    overall_helix = helix.groupby("date").agg(incidents=("Incident Number", "count")).reset_index()
    overall = (
        pd.merge(overall_nps, overall_helix, on="date", how="outer").sort_values("date").fillna(0)
    )

    nps["nps_topic"] = build_nps_topic(nps)
    by_topic_nps = (
        nps.groupby(["date", "nps_topic"])
        .agg(
            responses=(count_col, "count"),
            focus_count=("is_focus", "sum"),
            nps_mean=("NPS", "mean"),
        )
        .reset_index()
    )
    by_topic_nps["focus_rate"] = by_topic_nps["focus_count"] / by_topic_nps["responses"].replace(
        {0: np.nan}
    )
    by_topic = by_topic_nps.copy()
    if not incident_assignments.empty:
        ia = incident_assignments.copy()
        ia = ia.merge(
            helix[["Incident Number", "date"]].astype({"Incident Number": str}),
            left_on="incident_id",
            right_on="Incident Number",
            how="left",
        )
        by_topic_inc = (
            ia.groupby(["date", "nps_topic"]).agg(incidents=("incident_id", "count")).reset_index()
        )
        by_topic = by_topic.merge(by_topic_inc, on=["date", "nps_topic"], how="left")
    # Defensive: if there are no Helix incidents (or linking is disabled), the
    # merge above won't create an "incidents" column. Ensure it always exists
    # and is numeric to keep downstream charts stable.
    if "incidents" not in by_topic.columns:
        by_topic["incidents"] = 0
    else:
        by_topic["incidents"] = by_topic["incidents"].fillna(0)
    return overall, by_topic


def can_use_daily_resample(
    overall_daily: pd.DataFrame,
    min_days_with_responses: int = 20,
    min_coverage: float = 0.45,
) -> bool:
    """Heuristic to decide if daily analysis is meaningful.

    - Need at least `min_days_with_responses` days with responses
    - Need coverage: days_with_responses / total_days_in_range >= min_coverage
    """
    if overall_daily.empty or "date" not in overall_daily.columns:
        return False
    df = overall_daily.copy().sort_values("date")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return False
    days_with = int((df.get("responses", 0).astype(float) > 0).sum())
    if days_with < int(min_days_with_responses):
        return False
    dmin = df["date"].min()
    dmax = df["date"].max()
    total_days = int((dmax - dmin).days) + 1
    if total_days <= 0:
        return False
    coverage = float(days_with) / float(total_days)
    return coverage >= float(min_coverage)


def causal_rank_by_topic(by_topic: pd.DataFrame) -> pd.DataFrame:
    """Simple pragmatic causal score per topic from weekly aggregates."""
    if by_topic.empty:
        return pd.DataFrame(
            columns=[
                "nps_topic",
                "weeks",
                "responses",
                "focus_rate",
                "incidents",
                "delta_focus_rate",
                "score",
            ]
        )

    df = by_topic.copy()
    # Aggregate across weeks
    agg = df.groupby("nps_topic").agg(
        weeks=("week", "nunique"),
        responses=("responses", "sum"),
        focus_count=("focus_count", "sum"),
        incidents=("incidents", "sum"),
        avg_focus_rate=("focus_rate", "mean"),
        avg_incidents=("incidents", "mean"),
        max_incidents=("incidents", "max"),
    )
    agg["focus_rate"] = agg["focus_count"] / agg["responses"].replace({0: np.nan})

    # delta: weeks with high incidents (>= median non-zero) vs low
    deltas = []
    for topic, g in df.groupby("nps_topic"):
        if g["week"].nunique() < 2:
            deltas.append((topic, np.nan))
            continue
        inc = g["incidents"].values
        thr = np.median(inc)
        high = g.loc[g["incidents"] >= thr, "focus_rate"].astype(float)
        low = g.loc[g["incidents"] < thr, "focus_rate"].astype(float)
        d = float(high.mean() - low.mean()) if (len(high) and len(low)) else np.nan
        deltas.append((topic, d))
    delta_df = pd.DataFrame(deltas, columns=["nps_topic", "delta_focus_rate"]).set_index(
        "nps_topic"
    )

    out = agg.join(delta_df, how="left").reset_index()
    # Pragmatic score: incidents presence * delta detractor_rate * support
    out["support"] = np.clip(np.log1p(out["responses"]) / 10.0, 0, 1)
    out["inc_signal"] = np.clip(np.log1p(out["incidents"]) / 5.0, 0, 1)
    out["effect"] = out["delta_focus_rate"].fillna(0).abs()
    out["score"] = (0.45 * out["inc_signal"] + 0.35 * out["effect"] + 0.20 * out["support"]).clip(
        0, 1
    )
    out = out.sort_values(["score", "incidents", "responses"], ascending=False).reset_index(
        drop=True
    )
    return out
