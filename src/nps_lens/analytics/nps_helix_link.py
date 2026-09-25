from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer, strip_accents_unicode

from nps_lens.analytics.linking_policy import (
    LINK_CONTEXT_TERMS,
    LINK_MAX_DAYS_APART,
    LINK_MIN_SHARED_TERMS,
    LINK_MIN_SIMILARITY,
    LINK_TOP_K_PER_INCIDENT,
)
from nps_lens.analytics.text_mining import STOPWORDS_ES, preprocess_text
from nps_lens.core.nps_math import focus_mask, normalize_focus_group, valid_nps_scores
from nps_lens.domain.normalization import semantic_series
from nps_lens.domain.record_identity import analytical_response_ids
from nps_lens.ingest.helix_dates import incident_occurrence_dates


def build_nps_topic(df: pd.DataFrame) -> pd.Series:
    parts = [
        semantic_series(df.get(column, pd.Series("", index=df.index)))
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
    return semantic_series(df[col])


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
    base = pd.Series("", index=df.index, dtype="string")
    for column in _ordered_cols_ci(
        df, [f"Product Categorization Tier {tier}" for tier in (1, 2, 3)]
    ):
        part = _txt_series(df, column)
        base = base + (" > " + part).where(base.ne("") & part.ne(""), part)
    for column in _ordered_cols_ci(
        df,
        [
            "BBVA_SourceServiceN2",
            "BBVA_SourceServiceN1",
            "SourceService",
            "service",
            "summary",
            "Description",
        ],
    ):
        base = base.mask(base.eq(""), _txt_series(df, column))
    return base.mask(base.eq(""), build_incident_display_text(df))


def build_nps_text(df: pd.DataFrame) -> pd.Series:
    # Taxonomy describes the response; it is not evidence supplied by the customer.
    return _txt_series(df, "Comment")


def nps_matchable_mask(df: pd.DataFrame) -> pd.Series:
    return build_nps_text(df).str.contains(r"[^\W\d_]", regex=True, na=False)


_DETAIL_LABEL = re.compile(
    r"(?im)^[ \t]*(?:[-*•][ \t]*)?(?:\d+[.)][ \t]*)?([^:\n]{2,65})[ \t]*:[ \t]*"
)
_SIGNAL_LABEL = re.compile(
    r"s[ií]ntoma|impacto|error|causa|resoluci[oó]n|soluci[oó]n|descripci[oó]n|problema|symptom|impact|resolution|description|cause",
    re.IGNORECASE,
)


def _detail_signal(text: str) -> str:
    labels = list(_DETAIL_LABEL.finditer(text))
    if not labels:
        return text[:320]
    parts = []
    for index, match in enumerate(labels):
        if _SIGNAL_LABEL.search(match.group(1)):
            end = labels[index + 1].start() if index + 1 < len(labels) else len(text)
            value = text[match.end() : end].strip()
            if value:
                parts.append(value[:240])
    return " ".join(parts)[:640]


def build_incident_text(df: pd.DataFrame) -> pd.Series:
    """Compact semantic evidence, with field-specific limits and no repeated values."""
    fields = [
        ("summary", 180),
        ("Description", 180),
        ("Short Description", 180),
        ("BBVA_ExecutiveDescription", 320),
        ("BBVA_FinalImpact", 320),
        ("BBVA_RootCauseMain", 160),
        ("BBVA_RootCause1", 240),
        ("BBVA_RootCauseExecutive", 260),
        ("Resolution", 240),
        ("Detailed Description", 640),
        ("Detailed Decription", 640),
    ]
    compact = pd.Series("", index=df.index, dtype="string")
    seen = []
    for name, limit in fields:
        for column in _ordered_cols_ci(df, [name]):
            part = _txt_series(df, column)
            if name.startswith("Detailed"):
                # Parse each distinct template once, retaining labelled narrative only.
                lookup = {value: _detail_signal(value) for value in part.unique()}
                part = part.map(lookup).astype("string")
            part = (
                part.str.replace(r"<[^>]+>", " ", regex=True)
                .str.replace(r"https?://\S+|www\.\S+", " ", regex=True)
                .str.replace(r"\b(?:INC|WO|REQ)\d{5,}\b", " ", regex=True)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )
            key = part.str.casefold()
            duplicate = pd.Series(False, index=df.index)
            for previous in seen:
                duplicate |= key.eq(previous)
            seen.append(key)
            compact = compact + " " + part.mask(duplicate, "").str.slice(0, limit)
    return compact.str.replace(r"\s+", " ", regex=True).str.strip()


_PLACEHOLDER_INCIDENT_RE = re.compile(
    r"\b(?:ejemplo|example|dummy|placeholder|lorem\s+ipsum)\b",
    flags=re.IGNORECASE,
)
_WORD_RE = re.compile(r"[a-záéíóúüñ]{3,}", flags=re.IGNORECASE)


def _incident_link_quality_columns(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    semantic_text = df.get("_incident_semantic_text")
    if semantic_text is None:
        semantic_text = build_incident_text(df)
    display_text = build_incident_display_text(df)
    title_columns = _ordered_cols_ci(df, ["Description", "summary", "Short Description"])
    description = _txt_series(df, title_columns[0] if title_columns else "")
    words = semantic_text.str.findall(_WORD_RE).str.join(" ")
    informative = words.str.count(r"\b[^\W\d_]*[aeiouáéíóúü][^\W\d_]*\b", flags=re.IGNORECASE)
    placeholder = (description + " " + display_text).str.contains(_PLACEHOLDER_INCIDENT_RE)
    empty = semantic_text.eq("") | informative.eq(0)
    synthetic = placeholder & informative.le(4)
    reasons = pd.Series("", index=df.index, dtype="string")
    reasons.loc[synthetic] = "Registro de ejemplo o placeholder"
    reasons.loc[empty] = "Texto operativo vacío o no interpretable"
    return ~(empty | synthetic), reasons


def annotate_incident_link_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Annotate whether each incident has enough real narrative for causal matching.

    The source row remains available for audit in the Helix dataset. Only synthetic,
    placeholder or effectively empty narratives are kept out of semantic linking.
    """

    out = df.copy(deep=False)
    out["_incident_semantic_text"] = build_incident_text(df)
    eligible, reasons = _incident_link_quality_columns(out)
    out["Causal Match Eligible"] = eligible
    out["Causal Exclusion Reason"] = reasons
    return out


def filter_linkable_incidents(df: pd.DataFrame) -> pd.DataFrame:
    """Return only incidents suitable for semantic causal analysis."""

    if "Causal Match Eligible" in df.columns:
        eligible = df["Causal Match Eligible"].fillna(False).astype(bool)
        return df.loc[eligible].copy()
    annotated = annotate_incident_link_quality(df)
    return annotated.loc[annotated["Causal Match Eligible"]].copy()


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


def link_incidents_to_nps_topics(
    nps_detractors: pd.DataFrame,
    helix_incidents: pd.DataFrame,
    min_similarity: float = LINK_MIN_SIMILARITY,
    max_features: int = 50000,
    top_k_per_incident: int = LINK_TOP_K_PER_INCIDENT,
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

    nps_text = build_nps_text(nps_detractors)
    matchable = nps_text.str.contains(r"[^\W\d_]", regex=True, na=False)
    nps = nps_detractors.loc[matchable].copy()
    nps_text = nps_text.loc[matchable].map(preprocess_text)
    helix = filter_linkable_incidents(helix_incidents)
    if helix.empty or nps.empty:
        return (
            pd.DataFrame(columns=["incident_id", "nps_topic", "similarity"]),
            pd.DataFrame(
                columns=["nps_id", "incident_id", "similarity", "nps_topic", "incident_topic"]
            ),
        )

    nps["nps_id"] = analytical_response_ids(nps)
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
    helix["incident_date"] = incident_occurrence_dates(helix)[0].dt.normalize()

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

    helix_text = helix.get("_incident_semantic_text")
    if helix_text is None:
        helix_text = build_incident_text(helix)
    helix_text = helix_text.map(preprocess_text)
    corpus = nps_text.tolist() + helix_text.tolist()
    if not any(str(t).strip() for t in corpus):
        return (
            pd.DataFrame(columns=["incident_id", "nps_topic", "similarity", "incident_topic"]),
            pd.DataFrame(
                columns=["nps_id", "incident_id", "similarity", "nps_topic", "incident_topic"]
            ),
        )

    # Literal narrative evidence only. Character fragments inflated similarity on
    # unrelated words and accounted for most of the vectorizer's memory footprint.
    stopwords = sorted(
        {strip_accents_unicode(word) for word in STOPWORDS_ES}
        | {
            "error",
            "falla",
            "fallo",
            "problema",
            "incidencia",
            "incidente",
            "cliente",
            "clientes",
            "usuario",
            "usuarios",
            "sistema",
            "servicio",
            "bbva",
            "app",
            "aplicacion",
            "empresas",
            "empresa",
            "banca",
            "banco",
            "web",
            "portal",
            "puedo",
            "puede",
            "hacer",
            "realizar",
            "funciona",
            "funcionar",
            "tengo",
            "tiene",
            "tener",
            "hoy",
            "datos",
            "titulo",
            "descripcion",
            "pyme",
        }
    )
    word_vec = TfidfVectorizer(
        lowercase=True,
        strip_accents="unicode",
        sublinear_tf=True,
        max_features=max_features,
        ngram_range=(1, 2),
        min_df=1,
        token_pattern=r"(?u)\b[^\W\d_]{3,}\b",
        stop_words=stopwords,
        dtype=np.float32,
    )
    try:
        word_matrix = word_vec.fit_transform(corpus)
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
    incident_ids = helix["incident_id"].to_numpy()
    incident_topics = helix["incident_topic"].to_numpy()
    incident_dates = helix["incident_date"].to_numpy(dtype="datetime64[ns]")
    # Evidence links: incident -> top detractor comments with sparse/chunked similarity.
    nps_ids = nps["nps_id"].to_numpy()
    nps_topics = nps["nps_topic"].to_numpy()
    nps_dates = nps["nps_date"].to_numpy(dtype="datetime64[ns]")

    word_features = word_vec.get_feature_names_out()
    # Gate before top-k: two distinct shared content words, not a bigram counted twice.
    unigram_columns = np.flatnonzero(np.char.find(word_features.astype(str), " ") < 0)
    incidence = word_inc[:, unigram_columns].astype(bool).astype(np.int32)
    responses = word_nps[:, unigram_columns].astype(bool).astype(np.int32)
    specific_columns = [
        i
        for i, column in enumerate(unigram_columns)
        if word_features[column] not in LINK_CONTEXT_TERMS
    ]
    specific_incidence = incidence[:, specific_columns]
    specific_responses = responses[:, specific_columns]
    links: List[EvidenceLink] = []
    chunk = max(1, int(evidence_chunk_size))
    per_incident_k = max(1, int(top_k_per_incident))
    max_days = int(max_days_apart) if max_days_apart is not None else None
    for start in range(0, word_inc.shape[0], chunk):
        end = min(start + chunk, word_inc.shape[0])
        shared = incidence[start:end] @ responses.T
        specific = specific_incidence[start:end] @ specific_responses.T
        sim_block = (
            (word_inc[start:end] @ word_nps.T)
            .multiply(shared >= LINK_MIN_SHARED_TERMS)
            .multiply(specific > 0)
            .tocsr()
        )
        sim_block.eliminate_zeros()
        for bi in range(sim_block.shape[0]):
            inc_row = start + bi
            inc_id = str(incident_ids[inc_row])
            inc_topic = str(incident_topics[inc_row])
            inc_date = incident_dates[inc_row]
            row = sim_block.getrow(bi)
            candidate_idx = row.indices
            candidate_vals = row.data
            if max_days is not None:
                if pd.isna(inc_date):
                    continue
                candidate_dates = nps_dates[candidate_idx]
                day_delta = np.abs((candidate_dates - inc_date) / np.timedelta64(1, "D"))
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
                links.append(
                    EvidenceLink(
                        nps_id=str(nps_ids[j]),
                        incident_id=inc_id,
                        similarity=s,
                        nps_topic=str(nps_topics[j]),
                        incident_topic=inc_topic,
                        matched_terms=tuple(
                            sorted(
                                feature
                                for feature in word_features[
                                    word_inc.getrow(inc_row)
                                    .multiply(word_nps.getrow(int(j)))
                                    .indices
                                ]
                                if " " not in feature
                            )
                        ),
                    )
                )

    links_df = pd.DataFrame([e.__dict__ for e in links], columns=EvidenceLink.__dataclass_fields__)
    links_df = (
        links_df.sort_values("similarity", ascending=False)
        .drop_duplicates(["incident_id", "nps_id"])
        .reset_index(drop=True)
    )
    # Every assignment must have an accepted, dated comment as its evidence.
    assign_df = links_df.drop_duplicates("incident_id")[
        ["incident_id", "nps_topic", "similarity", "incident_topic"]
    ].reset_index(drop=True)
    return assign_df, links_df


def _period_aggregates(
    nps_df: pd.DataFrame,
    helix_df: pd.DataFrame,
    incident_assignments: pd.DataFrame,
    date_col_nps: str = "Fecha",
    date_col_helix: str = "Fecha",
    focus_group: str = "detractor",
    *,
    frequency: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    nps = nps_df.copy()
    helix = helix_df.copy()

    nps[date_col_nps] = pd.to_datetime(nps[date_col_nps], errors="coerce")
    helix[date_col_helix] = (
        incident_occurrence_dates(helix)[0]
        if date_col_helix == "Fecha"
        else pd.to_datetime(helix[date_col_helix], errors="coerce")
    )

    nps["NPS"] = valid_nps_scores(nps["NPS"])
    nps = nps.dropna(subset=[date_col_nps, "NPS"])
    helix = helix.dropna(subset=[date_col_helix])

    bucket = "week" if frequency == "W" else "date"
    for frame, column in ((nps, date_col_nps), (helix, date_col_helix)):
        frame[bucket] = frame[column].dt.to_period(frequency).dt.start_time

    group = normalize_focus_group(focus_group)
    nps["is_focus"] = focus_mask(nps, focus_group=group)

    overall_voc = (
        nps.groupby(bucket)
        .agg(
            responses=("NPS", "size"),
            focus_count=("is_focus", "sum"),
            nps_mean=("NPS", "mean"),
        )
        .reset_index()
    )
    overall_voc["focus_rate"] = overall_voc["focus_count"] / overall_voc["responses"].replace(
        {0: np.nan}
    )

    overall_helix = (
        helix.groupby(bucket).agg(incidents=("Incident Number", "nunique")).reset_index()
    )
    overall = pd.merge(overall_voc, overall_helix, on=bucket, how="outer").sort_values(bucket)

    overall[["responses", "focus_count", "incidents"]] = overall[
        ["responses", "focus_count", "incidents"]
    ].fillna(0)

    # By topic (NPS topics)
    nps["nps_topic"] = build_nps_topic(nps)
    by_topic_nps = (
        nps.groupby([bucket, "nps_topic"])
        .agg(
            responses=("NPS", "size"),
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
            helix[["Incident Number", bucket]]
            .drop_duplicates("Incident Number")
            .astype({"Incident Number": str}),
            left_on="incident_id",
            right_on="Incident Number",
            how="left",
        )
        by_topic_inc = (
            ia.groupby([bucket, "nps_topic"])
            .agg(incidents=("incident_id", "nunique"))
            .reset_index()
        )
        by_topic = by_topic.merge(by_topic_inc, on=[bucket, "nps_topic"], how="left")
    # Ensure incidents column exists even when there are no incident assignments.
    # NOTE: DataFrame.get("incidents", 0) returns an int when missing, which does not
    # support .fillna; hence this explicit branch.
    if "incidents" not in by_topic.columns:
        by_topic["incidents"] = 0
    else:
        by_topic["incidents"] = by_topic["incidents"].fillna(0)
    return overall, by_topic


def weekly_aggregates(
    nps_df: pd.DataFrame,
    helix_df: pd.DataFrame,
    incident_assignments: pd.DataFrame,
    date_col_nps: str = "Fecha",
    date_col_helix: str = "Fecha",
    focus_group: str = "detractor",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return _period_aggregates(
        nps_df,
        helix_df,
        incident_assignments,
        date_col_nps,
        date_col_helix,
        focus_group,
        frequency="W",
    )


def daily_aggregates(
    nps_df: pd.DataFrame,
    helix_df: pd.DataFrame,
    incident_assignments: pd.DataFrame,
    date_col_nps: str = "Fecha",
    date_col_helix: str = "Fecha",
    focus_group: str = "detractor",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return _period_aggregates(
        nps_df,
        helix_df,
        incident_assignments,
        date_col_nps,
        date_col_helix,
        focus_group,
        frequency="D",
    )
