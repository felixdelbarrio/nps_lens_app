from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from nps_lens.analytics.evidence_highlights import contributing_terms
from nps_lens.analytics.linking_policy import (
    LINK_MAX_DAYS_APART,
    LINK_MIN_SIMILARITY,
    LINK_TOP_K_PER_INCIDENT,
)
from nps_lens.analytics.text_mining import preprocess_text
from nps_lens.core.nps_math import focus_mask, normalize_focus_group


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
    parts = [
        df.get(column, pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
        for column in ("Palanca", "Subpalanca")
    ]
    return (parts[0] + " > " + parts[1]).str.strip().str.replace(r"^>\s*|\s*>$", "", regex=True)


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
    # Fill missing values before casting to string. Casting NaN first produces
    # the literal text "nan", which prevents the fallback to service/summary.
    tiers = pd.concat(
        [
            df.get(column, pd.Series([""] * len(df), index=df.index))
            .fillna("")
            .astype(str)
            .str.strip()
            for column in (
                "Product Categorization Tier 1",
                "Product Categorization Tier 2",
                "Product Categorization Tier 3",
            )
        ],
        axis=1,
    )
    base = tiers.apply(lambda row: " > ".join(value for value in row.tolist() if value), axis=1)
    # fallback to service / summary
    svc = (
        df.get("service", pd.Series([""] * len(df), index=df.index))
        .fillna("")
        .astype(str)
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
    """Build a compact, high-signal semantic document for each Helix incident.

    Helix narratives contain long resolution templates and operational boilerplate.  Concatenating
    them without bounds dilutes the few words that a short customer comment can share with an
    incident.  The field order and per-field limits below preserve business meaning while keeping
    vectorisation cost proportional and scores comparable across exports.
    """

    signal_cols = _ordered_cols_ci(
        df,
        [
            "Description",
            "summary",
            "BBVA_SourceServiceN2",
            "service",
            "BBVA_RootCauseMain",
            "BBVA_RootCause1",
            "BBVA_RootCauseExecutive",
            "BBVA_FinalImpact",
            "BBVA_ExecutiveDescription",
            "Detailed Description",
            "Detailed Decription",
            "Resolution",
        ],
    )
    if not signal_cols:
        return pd.Series([""] * len(df), index=df.index)
    limits = [180, 180, 100, 100, 160, 240, 260, 320, 320, 240]
    parts = []
    for position, column in enumerate(signal_cols):
        limit = limits[min(position, len(limits) - 1)]
        cleaned = (
            _txt_series(df, column)
            .str.replace(r"<[^>]+>", " ", regex=True)
            .str.replace(r"https?://\S+|www\.\S+", " ", regex=True)
            .str.replace(r"\b(?:INC|WO|REQ)\d{5,}\b", " ", regex=True, flags=0)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.slice(0, limit)
        )
        parts.append(cleaned)
    compact = parts[0]
    for part in parts[1:]:
        compact = compact + " " + part
    return compact.str.replace(r"\s+", " ", regex=True).str.strip()


_PLACEHOLDER_INCIDENT_RE = re.compile(
    r"\b(?:ejemplo|example|dummy|placeholder|lorem\s+ipsum)\b",
    flags=re.IGNORECASE,
)
_WORD_RE = re.compile(r"[a-záéíóúüñ]{3,}", flags=re.IGNORECASE)
_VOWEL_RE = re.compile(r"[aeiouáéíóúü]", flags=re.IGNORECASE)


def _incident_link_quality_columns(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    semantic_text = build_incident_text(df).fillna("").astype(str).str.strip()
    display_text = build_incident_display_text(df).fillna("").astype(str).str.strip()
    title_columns = _ordered_cols_ci(df, ["Description", "summary", "Short Description"])
    description = _txt_series(df, title_columns[0] if title_columns else "")

    eligible: list[bool] = []
    reasons: list[str] = []
    for semantic, display, title in zip(semantic_text, display_text, description):
        combined = " ".join((str(title), str(display))).strip()
        words = _WORD_RE.findall(str(semantic))
        informative = [word for word in words if _VOWEL_RE.search(word)]
        placeholder = bool(_PLACEHOLDER_INCIDENT_RE.search(combined))
        if not str(semantic).strip() or not informative:
            eligible.append(False)
            reasons.append("Texto operativo vacío o no interpretable")
        elif placeholder and len(informative) <= 4:
            eligible.append(False)
            reasons.append("Registro de ejemplo o placeholder")
        else:
            eligible.append(True)
            reasons.append("")

    return (
        pd.Series(eligible, index=df.index, dtype=bool),
        pd.Series(reasons, index=df.index, dtype="string"),
    )


def annotate_incident_link_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Annotate whether each incident has enough real narrative for causal matching.

    The source row remains available for audit in the Helix dataset. Only synthetic,
    placeholder or effectively empty narratives are kept out of semantic linking.
    """

    out = df.copy()
    eligible, reasons = _incident_link_quality_columns(df)
    out["Causal Match Eligible"] = eligible
    out["Causal Exclusion Reason"] = reasons
    return out


def filter_linkable_incidents(df: pd.DataFrame) -> pd.DataFrame:
    """Return only incidents suitable for semantic causal analysis."""

    if "Causal Match Eligible" in df.columns:
        eligible = df["Causal Match Eligible"].fillna(False).astype(bool)
        return df.loc[eligible].copy()
    eligible, _ = _incident_link_quality_columns(df)
    return df.loc[eligible].copy()


@dataclass(frozen=True)
class EvidenceLink:
    nps_id: str
    incident_id: str
    similarity: float
    nps_topic: str
    incident_topic: str
    matched_terms: tuple[str, ...] = ()


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
    - evidence links from incidents to specific detractor comments
    """

    if nps_detractors.empty or helix_incidents.empty:
        return (
            pd.DataFrame(columns=["incident_id", "nps_topic", "similarity"]),
            pd.DataFrame(
                columns=["nps_id", "incident_id", "similarity", "nps_topic", "incident_topic"]
            ),
        )

    nps = nps_detractors.copy()
    helix = filter_linkable_incidents(helix_incidents)
    if helix.empty:
        return (
            pd.DataFrame(columns=["incident_id", "nps_topic", "similarity"]),
            pd.DataFrame(
                columns=["nps_id", "incident_id", "similarity", "nps_topic", "incident_topic"]
            ),
        )

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

    # Restrict the semantic search space before vectorisation.  Previously every incident in the
    # historical export competed for a period even when it could never pass the temporal policy.
    if max_days_apart is not None:
        dated_nps = nps["nps_date"].dropna()
        if not dated_nps.empty:
            delta = pd.Timedelta(days=max(0, int(max_days_apart)))
            relevant = helix["incident_date"].between(
                dated_nps.min() - delta, dated_nps.max() + delta
            )
            helix = helix.loc[relevant].copy()
            if helix.empty:
                return (
                    pd.DataFrame(
                        columns=["incident_id", "nps_topic", "similarity", "incident_topic"]
                    ),
                    pd.DataFrame(
                        columns=[
                            "nps_id",
                            "incident_id",
                            "similarity",
                            "nps_topic",
                            "incident_topic",
                        ]
                    ),
                )

    nps["nps_topic"] = build_nps_topic(nps)
    helix["incident_topic"] = build_incident_topic(helix)

    nps_text = build_nps_text(nps).fillna("").map(preprocess_text)
    helix_text = build_incident_text(helix).fillna("").map(preprocess_text)
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
    word_vec = TfidfVectorizer(
        lowercase=True,
        strip_accents="unicode",
        sublinear_tf=True,
        max_features=max_features,
        ngram_range=(1, 2),
        min_df=min_df,
        stop_words=None,
    )
    char_vec = TfidfVectorizer(
        lowercase=True,
        strip_accents="unicode",
        sublinear_tf=True,
        analyzer="char_wb",
        ngram_range=(3, 5),
        min_df=min_df,
        max_features=min(max_features, 60_000),
    )
    try:
        word_matrix = word_vec.fit_transform(corpus)
        char_matrix = char_vec.fit_transform(corpus)
    except ValueError:
        # Empty vocabulary after cleaning
        return (
            pd.DataFrame(columns=["incident_id", "nps_topic", "similarity", "incident_topic"]),
            pd.DataFrame(
                columns=["nps_id", "incident_id", "similarity", "nps_topic", "incident_topic"]
            ),
        )

    split = len(nps)
    word_nps, word_inc = word_matrix[:split], word_matrix[split:]
    char_nps, char_inc = char_matrix[:split], char_matrix[split:]
    word_topics = word_vec.transform(topic_docs.values.tolist())
    char_topics = char_vec.transform(topic_docs.values.tolist())

    # Assignment incident -> topic with sparse similarity (no dense NxM matrix).
    sim_topic = (word_inc @ word_topics.T) * 0.35 + (char_inc @ char_topics.T) * 0.65
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
    word_nps_ev = word_nps[nps_pos]
    char_nps_ev = char_nps[nps_pos]
    nps_ev = nps.iloc[nps_pos].copy()

    word_features = word_vec.get_feature_names_out()
    char_features = char_vec.get_feature_names_out()
    incident_display = build_incident_display_text(helix).tolist()
    links: List[EvidenceLink] = []
    chunk = max(1, int(evidence_chunk_size))
    per_incident_k = max(1, int(top_k_per_incident))
    max_days = int(max_days_apart) if max_days_apart is not None else None
    for start in range(0, word_inc.shape[0], chunk):
        end = min(start + chunk, word_inc.shape[0])
        sim_block = (word_inc[start:end] @ word_nps_ev.T) * 0.35 + (
            char_inc[start:end] @ char_nps_ev.T
        ) * 0.65
        for bi in range(sim_block.shape[0]):
            inc_row = start + bi
            inc_id = str(helix.iloc[inc_row]["incident_id"])
            inc_topic = str(helix.iloc[inc_row]["incident_topic"])
            inc_date = pd.to_datetime(helix.iloc[inc_row].get("incident_date"), errors="coerce")
            row = sim_block.getrow(bi)
            candidate_idx = row.indices
            candidate_vals = row.data
            if max_days is not None:
                if pd.isna(inc_date):
                    continue
                candidate_dates = pd.to_datetime(
                    nps_ev.iloc[candidate_idx]["nps_date"], errors="coerce"
                ).to_numpy(dtype="datetime64[ns]")
                day_delta = np.abs(
                    (candidate_dates - np.datetime64(inc_date.to_datetime64()))
                    / np.timedelta64(1, "D")
                )
                valid = np.isfinite(day_delta) & (day_delta <= max_days)
                candidate_idx = candidate_idx[valid]
                candidate_vals = candidate_vals[valid]
            if not len(candidate_idx):
                continue
            if len(candidate_vals) > per_incident_k:
                pick = np.argpartition(candidate_vals, -per_incident_k)[-per_incident_k:]
                pick = pick[np.argsort(-candidate_vals[pick])]
                idx, vals = candidate_idx[pick], candidate_vals[pick]
            else:
                order = np.argsort(-candidate_vals)
                idx, vals = candidate_idx[order], candidate_vals[order]
            for j, sim in zip(idx.tolist(), vals.tolist()):
                s = float(sim)
                if s < float(min_similarity):
                    continue
                nps_row = nps_ev.iloc[int(j)]
                links.append(
                    EvidenceLink(
                        nps_id=str(nps_row["nps_id"]),
                        incident_id=inc_id,
                        similarity=s,
                        nps_topic=str(nps_row["nps_topic"]),
                        incident_topic=inc_topic,
                        matched_terms=contributing_terms(
                            incident_display[inc_row],
                            set(
                                word_features[
                                    word_inc.getrow(inc_row)
                                    .multiply(word_nps_ev.getrow(int(j)))
                                    .indices
                                ]
                            ),
                            set(
                                char_features[
                                    char_inc.getrow(inc_row)
                                    .multiply(char_nps_ev.getrow(int(j)))
                                    .indices
                                ]
                            ),
                        ),
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
    overall_voc = (
        nps.groupby("week")
        .agg(
            responses=(count_col, "count"),
            focus_count=("is_focus", "sum"),
            nps_mean=("NPS", "mean"),
        )
        .reset_index()
    )
    overall_voc["focus_rate"] = overall_voc["focus_count"] / overall_voc["responses"].replace(
        {0: np.nan}
    )

    overall_helix = helix.groupby("week").agg(incidents=("Incident Number", "count")).reset_index()
    overall = (
        pd.merge(overall_voc, overall_helix, on="week", how="outer").sort_values("week").fillna(0)
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
    overall_voc = (
        nps.groupby("date")
        .agg(
            responses=(count_col, "count"),
            focus_count=("is_focus", "sum"),
            nps_mean=("NPS", "mean"),
        )
        .reset_index()
    )
    overall_voc["focus_rate"] = overall_voc["focus_count"] / overall_voc["responses"].replace(
        {0: np.nan}
    )
    overall_helix = helix.groupby("date").agg(incidents=("Incident Number", "count")).reset_index()
    overall = (
        pd.merge(overall_voc, overall_helix, on="date", how="outer").sort_values("date").fillna(0)
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
