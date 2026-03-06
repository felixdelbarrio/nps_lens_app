from __future__ import annotations

import re
import unicodedata
from collections import Counter
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd

from nps_lens.analytics.drivers import driver_table
from nps_lens.analytics.linking_policy import (
    HOTSPOT_MIN_TERM_OCCURRENCES,
    LINK_MAX_DAYS_APART,
    LINK_MIN_SIMILARITY,
)
from nps_lens.analytics.nps_helix_link import build_incident_display_text, build_nps_topic

HOTSPOT_EVIDENCE_COLUMNS = [
    "incident_id",
    "incident_date",
    "nps_topic",
    "incident_summary",
    "detractor_comment",
    "similarity",
    "hot_term",
    "hot_rank",
    "mention_incidents",
    "mention_comments",
    "hotspot_incidents",
    "hotspot_comments",
    "hotspot_links",
]

HOTSPOT_TIMELINE_COLUMNS = [
    "incident_id",
    "hot_term",
    "date",
    "helix_records",
    "nps_comments",
    "nps_comments_moderate",
    "nps_comments_high",
    "nps_comments_critical",
    "incident_ids",
]

HOTSPOT_SUMMARY_COLUMNS = [
    "hot_rank",
    "hot_term",
    "mention_incidents",
    "mention_comments",
    "hotspot_incidents",
    "hotspot_comments",
    "hotspot_links",
    "chart_helix_records",
    "chart_nps_comments",
    "days_with_evidence",
]

_HOTSPOT_STOPWORDS = {
    "de",
    "del",
    "la",
    "el",
    "los",
    "las",
    "con",
    "sin",
    "por",
    "para",
    "que",
    "una",
    "uno",
    "unos",
    "unas",
    "this",
    "that",
    "from",
    "with",
    "without",
    "error",
    "issue",
    "incidencia",
    "incidente",
    "cliente",
    "app",
    "aplicacion",
    "mobile",
    "banca",
    "sintoma",
    "hora",
    "mensaje",
    "servicio",
    "usuario",
    "acotamiento",
    "identifica",
    "proceder",
    "realizar",
    "realiza",
    "caso",
    "soporte",
    "inc",
    "ird",
    "bbva",
    "cuenta",
    "cuentas",
    "clientes",
    "sistema",
    "cambios",
    "momento",
    "portal",
    "desde",
    "tiene",
    "medio",
    "tiempo",
    "telefono",
    "usuarios",
    "ventana",
}

_AXIS_STOPWORDS = {
    "de",
    "del",
    "la",
    "el",
    "los",
    "las",
    "con",
    "sin",
    "por",
    "para",
    "que",
    "una",
    "uno",
    "unos",
    "unas",
    "and",
    "the",
}


def _norm_txt(v: object) -> str:
    txt = " ".join(str(v or "").split()).lower()
    return unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")


def _tokenize(v: object) -> list[str]:
    raw = re.findall(r"[a-z0-9]{4,}", _norm_txt(v))
    return [t for t in raw if t not in _HOTSPOT_STOPWORDS and not t.isdigit()]


def _term_key(v: object) -> str:
    base = str(v or "").strip().lower()
    if base.endswith("es") and len(base) > 4:
        base = base[:-2]
    elif base.endswith("s") and len(base) > 3:
        base = base[:-1]
    return base


def _norm_phrase(v: object) -> str:
    s = _norm_txt(v)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _phrase_tokens(v: object) -> set[str]:
    return {
        t
        for t in re.findall(r"[a-z0-9]{3,}", _norm_phrase(v))
        if t not in _AXIS_STOPWORDS
    }


def _topic_axis_value(topic: object, axis: str) -> str:
    raw = str(topic or "")
    parts = [p.strip() for p in raw.split(">", 1)]
    if str(axis or "").strip().lower().startswith("sub"):
        return parts[1] if len(parts) > 1 else ""
    return parts[0] if parts else ""


def _comment_column(df: pd.DataFrame) -> str:
    if "Comment" in df.columns:
        return "Comment"
    if "Comentario" in df.columns:
        return "Comentario"
    return ""


def _nps_severity(score: object) -> str:
    try:
        s = float(score)
    except Exception:
        return "moderate"
    if not np.isfinite(s):
        return "moderate"
    if s <= 2.0:
        return "critical"
    if s <= 4.0:
        return "high"
    return "moderate"


def _unique_join(values: pd.Series) -> str:
    parts = [str(v).strip() for v in values.astype(str).tolist() if str(v).strip()]
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return " | ".join(out)


def _prepare_nps_ref(nps_focus_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if nps_focus_df is None or nps_focus_df.empty:
        return pd.DataFrame(
            columns=[
                "nps_id",
                "date",
                "comment_txt",
                "comment_norm",
                "nps_score",
                "severity",
                "nps_topic",
            ]
        )

    nps_ref = nps_focus_df.copy()
    nps_ref["nps_id"] = nps_ref.get("ID", nps_ref.index).astype(str)
    nps_ref["date"] = pd.to_datetime(nps_ref.get("Fecha"), errors="coerce").dt.normalize()
    comment_col = _comment_column(nps_ref)
    nps_ref["comment_txt"] = (
        nps_ref.get(comment_col, pd.Series([""] * len(nps_ref), index=nps_ref.index))
        .astype(str)
        .fillna("")
    )
    nps_ref["comment_norm"] = nps_ref["comment_txt"].map(_norm_txt)
    nps_ref["nps_score"] = pd.to_numeric(nps_ref.get("NPS"), errors="coerce")
    nps_ref["severity"] = nps_ref["nps_score"].map(_nps_severity)
    nps_ref["nps_topic"] = build_nps_topic(nps_ref).astype(str).fillna("")
    return nps_ref


def _prepare_helix_ref(helix_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if helix_df is None or helix_df.empty:
        return pd.DataFrame(columns=["incident_id", "incident_date", "incident_summary", "summary_norm"])

    helix_ref = helix_df.copy()
    helix_ref["incident_id"] = helix_ref.get(
        "Incident Number", helix_ref.get("ID de la Incidencia", helix_ref.index)
    ).astype(str)
    helix_ref["incident_id"] = helix_ref["incident_id"].astype(str).str.strip()
    helix_ref["incident_date"] = pd.to_datetime(helix_ref.get("Fecha"), errors="coerce")
    helix_ref = helix_ref[helix_ref["incident_id"] != ""].copy()
    if helix_ref.empty:
        return pd.DataFrame(columns=["incident_id", "incident_date", "incident_summary", "summary_norm"])

    helix_ref["incident_summary"] = build_incident_display_text(helix_ref).astype(str).fillna("")
    helix_ref["summary_norm"] = helix_ref["incident_summary"].map(_norm_txt)
    return helix_ref


def _term_regex(term: object) -> str:
    t = _norm_txt(term)
    if not t:
        return ""
    return rf"\b{re.escape(t)}\b"


def _strict_term_links(
    links_enriched: pd.DataFrame,
    *,
    term_pattern: str,
    min_similarity: float,
    max_days_apart: int | None,
) -> pd.DataFrame:
    if links_enriched is None or links_enriched.empty or not term_pattern:
        return pd.DataFrame(columns=list(links_enriched.columns) if links_enriched is not None else [])

    d = links_enriched.copy()
    inc_match = d["incident_summary_norm"].str.contains(term_pattern, regex=True, na=False)
    com_match = d["detractor_comment_norm"].str.contains(term_pattern, regex=True, na=False)
    sim_ok = pd.to_numeric(d.get("similarity"), errors="coerce").fillna(0.0) >= float(min_similarity)
    keep = inc_match & com_match & sim_ok

    if max_days_apart is not None:
        inc_date = pd.to_datetime(d.get("incident_date"), errors="coerce")
        nps_date = pd.to_datetime(d.get("nps_date"), errors="coerce")
        delta_days = (inc_date - nps_date).abs().dt.days
        keep = keep & delta_days.notna() & (delta_days <= int(max_days_apart))

    out = d[keep].copy()
    if out.empty:
        return out
    out = out.drop_duplicates(["incident_id", "nps_id"])
    out = out.sort_values(["similarity"], ascending=False)
    return out


def _month_slice(helix_ref: pd.DataFrame, system_date: Optional[date]) -> pd.DataFrame:
    if helix_ref.empty:
        return helix_ref

    ref = pd.Timestamp(system_date) if system_date is not None else pd.Timestamp.now().tz_localize(None)
    month_start = pd.Timestamp(ref.date().replace(day=1))
    month_end = (month_start + pd.offsets.MonthEnd(0)).normalize()

    month_df = helix_ref[
        (helix_ref["incident_date"] >= month_start) & (helix_ref["incident_date"] <= month_end)
    ].copy()
    if not month_df.empty:
        return month_df

    latest = pd.to_datetime(helix_ref["incident_date"], errors="coerce").dropna().max()
    if pd.isna(latest):
        return month_df
    fallback_start = pd.Timestamp(date(int(latest.year), int(latest.month), 1))
    fallback_end = (fallback_start + pd.offsets.MonthEnd(0)).normalize()
    return helix_ref[
        (helix_ref["incident_date"] >= fallback_start)
        & (helix_ref["incident_date"] <= fallback_end)
    ].copy()


def _select_hot_terms(
    helix_month: pd.DataFrame,
    helix_ref: pd.DataFrame,
    nps_ref: pd.DataFrame,
    *,
    max_hotspots: int,
    min_term_occurrences: int,
) -> list[str]:
    if helix_month.empty:
        return []

    allowed_terms: set[str] = set()
    for c in ["Palanca", "Subpalanca"]:
        if c in nps_ref.columns:
            for txt in nps_ref[c].astype(str).fillna("").tolist():
                allowed_terms.update(_tokenize(txt))
    for c in [
        "Product Categorization Tier 1",
        "Product Categorization Tier 2",
        "Product Categorization Tier 3",
        "service",
    ]:
        if c in helix_ref.columns:
            for txt in helix_ref[c].astype(str).fillna("").tolist():
                allowed_terms.update(_tokenize(txt))

    hot_counter: Counter[str] = Counter()
    for txt in helix_month["summary_norm"].tolist():
        toks = set(_tokenize(txt))
        for tok in toks:
            hot_counter[str(tok)] += 1
    if not hot_counter:
        return []

    candidate_terms = [str(t) for t, cnt in hot_counter.items() if int(cnt) >= int(min_term_occurrences)]
    if not candidate_terms:
        candidate_terms = [str(t) for t, _ in hot_counter.most_common(20)]

    if allowed_terms:
        filtered = [t for t in candidate_terms if t in allowed_terms]
        if filtered:
            candidate_terms = filtered

    nps_term_counts: dict[str, int] = {}
    for term in candidate_terms:
        tnorm = _norm_txt(term)
        if not tnorm:
            nps_term_counts[str(term)] = 0
            continue
        tpat = rf"\b{re.escape(tnorm)}\b"
        nh = int(nps_ref["comment_norm"].str.contains(tpat, regex=True, na=False).sum()) if not nps_ref.empty else 0
        nps_term_counts[str(term)] = nh

    scored_terms = sorted(
        candidate_terms,
        key=lambda t: (
            nps_term_counts.get(str(t), 0) > 0,
            int(hot_counter.get(str(t), 0)) * max(1, int(nps_term_counts.get(str(t), 0))),
            int(nps_term_counts.get(str(t), 0)),
            int(hot_counter.get(str(t), 0)),
        ),
        reverse=True,
    )

    hot_terms: list[str] = []
    seen_keys: set[str] = set()

    for term in scored_terms:
        if int(nps_term_counts.get(str(term), 0)) <= 0:
            continue
        key = _term_key(term)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        hot_terms.append(str(term))
        if len(hot_terms) >= int(max_hotspots):
            break

    if len(hot_terms) < int(max_hotspots):
        for term in scored_terms:
            key = _term_key(term)
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            hot_terms.append(str(term))
            if len(hot_terms) >= int(max_hotspots):
                break

    return hot_terms


def select_best_business_axis_for_hotspots(
    nps_df: Optional[pd.DataFrame],
    helix_df: Optional[pd.DataFrame],
    *,
    min_n: int = 200,
    min_token_ratio: float = 0.60,
) -> dict[str, object]:
    """Choose Palanca/Subpalanca axis by Helix description coverage on red gaps."""
    base = {
        "best_axis": "Palanca",
        "axis_ratios": {"Palanca": 0.0, "Subpalanca": 0.0},
        "red_labels": {"Palanca": [], "Subpalanca": []},
        "label_hits": {"Palanca": {}, "Subpalanca": {}},
    }
    if nps_df is None or nps_df.empty or helix_df is None or helix_df.empty:
        return base

    helix_ref = _prepare_helix_ref(helix_df)
    if helix_ref.empty:
        return base

    helix_texts = helix_ref["incident_summary"].astype(str).fillna("").tolist()
    total_inc = int(len(helix_texts))
    if total_inc <= 0:
        return base
    helix_norm = [_norm_phrase(t) for t in helix_texts]
    helix_tokens = [_phrase_tokens(t) for t in helix_texts]

    ratios: dict[str, float] = {"Palanca": 0.0, "Subpalanca": 0.0}
    red_labels: dict[str, list[str]] = {"Palanca": [], "Subpalanca": []}
    label_hits: dict[str, dict[str, int]] = {"Palanca": {}, "Subpalanca": {}}

    for axis in ["Palanca", "Subpalanca"]:
        if axis not in nps_df.columns:
            continue
        stats = pd.DataFrame([s.__dict__ for s in driver_table(nps_df, axis)])
        if stats.empty:
            continue
        stats["gap_vs_overall"] = pd.to_numeric(stats.get("gap_vs_overall"), errors="coerce")
        stats["n"] = pd.to_numeric(stats.get("n"), errors="coerce")
        red = stats[(stats["gap_vs_overall"] < 0.0) & (stats["n"] >= int(min_n))].copy()
        if red.empty:
            continue
        red = red.sort_values(["gap_vs_overall"], ascending=True)
        labels = red["value"].astype(str).tolist()
        red_labels[axis] = labels
        label_hits[axis] = {str(lbl): 0 for lbl in labels}

        label_rows: list[tuple[str, str, set[str]]] = []
        for lab in labels:
            nlab = _norm_phrase(lab)
            toks = _phrase_tokens(lab)
            if nlab:
                label_rows.append((str(lab), nlab, toks))

        matched_idx: set[int] = set()
        for idx, (txt_norm, tok_set) in enumerate(zip(helix_norm, helix_tokens)):
            best_label = ""
            best_score = 0.0
            for lab, nlab, ltoks in label_rows:
                phrase_hit = bool(nlab and (nlab in txt_norm))
                tok_ratio = (
                    float(len(tok_set & ltoks)) / float(len(ltoks))
                    if ltoks
                    else 0.0
                )
                hit = phrase_hit or (tok_ratio >= float(min_token_ratio))
                if not hit:
                    continue
                score = max(tok_ratio, 1.0 if phrase_hit else 0.0)
                if score > best_score:
                    best_score = score
                    best_label = lab
            if best_label:
                matched_idx.add(int(idx))
                label_hits[axis][best_label] = int(label_hits[axis].get(best_label, 0)) + 1

        ratios[axis] = float(len(matched_idx)) / float(total_inc)

    best_axis = max(["Palanca", "Subpalanca"], key=lambda a: float(ratios.get(a, 0.0)))
    return {
        "best_axis": str(best_axis),
        "axis_ratios": ratios,
        "red_labels": red_labels,
        "label_hits": label_hits,
    }


def align_hotspot_evidence_to_axis(
    incident_evidence_df: Optional[pd.DataFrame],
    *,
    axis: str,
    red_labels: list[str],
    max_hotspots: int = 3,
) -> pd.DataFrame:
    if incident_evidence_df is None or incident_evidence_df.empty:
        return pd.DataFrame(columns=HOTSPOT_EVIDENCE_COLUMNS)

    d = incident_evidence_df.copy()
    if "hot_term" not in d.columns:
        return d

    norm_labels = {_norm_phrase(v) for v in list(red_labels or []) if _norm_phrase(v)}
    if not norm_labels:
        # No red labels available for this axis; keep original order.
        return d

    d["hot_term"] = d["hot_term"].astype(str).str.strip()
    d["hot_rank"] = pd.to_numeric(d.get("hot_rank"), errors="coerce").fillna(999).astype(int)
    d["mention_incidents"] = pd.to_numeric(d.get("mention_incidents"), errors="coerce").fillna(0).astype(int)
    d["mention_comments"] = pd.to_numeric(d.get("mention_comments"), errors="coerce").fillna(0).astype(int)
    d["nps_topic"] = d.get("nps_topic", "").astype(str)
    d["_axis_value"] = d["nps_topic"].map(lambda v: _topic_axis_value(v, axis))
    d["_axis_norm"] = d["_axis_value"].map(_norm_phrase)
    d["_aligned"] = d["_axis_norm"].isin(norm_labels)

    grp = (
        d.groupby("hot_term", as_index=False)
        .agg(
            orig_rank=("hot_rank", "min"),
            aligned=("_aligned", "max"),
            aligned_rows=("_aligned", "sum"),
            mention_comments=("mention_comments", "max"),
            mention_incidents=("mention_incidents", "max"),
        )
    )
    if grp.empty:
        return d

    grp["aligned"] = grp["aligned"].astype(bool)
    grp = grp.sort_values(
        ["aligned", "aligned_rows", "mention_comments", "mention_incidents", "orig_rank"],
        ascending=[False, False, False, False, True],
    )
    selected_terms = grp["hot_term"].astype(str).head(int(max_hotspots)).tolist()
    if not selected_terms:
        return d

    term_rank = {term: idx for idx, term in enumerate(selected_terms, start=1)}
    out = d[d["hot_term"].astype(str).isin(set(selected_terms))].copy()
    out["hot_rank"] = out["hot_term"].map(term_rank).astype(int)
    out = out.sort_values(["hot_rank", "similarity"], ascending=[True, False]).reset_index(drop=True)
    cols = [c for c in HOTSPOT_EVIDENCE_COLUMNS if c in out.columns]
    return out[cols].copy()


def build_hotspot_evidence(
    links_df: Optional[pd.DataFrame],
    nps_focus_df: Optional[pd.DataFrame],
    helix_df: Optional[pd.DataFrame],
    *,
    system_date: Optional[date] = None,
    max_hotspots: int = 3,
    min_term_occurrences: int = HOTSPOT_MIN_TERM_OCCURRENCES,
    min_validated_similarity: float = LINK_MIN_SIMILARITY,
    max_days_apart: int | None = LINK_MAX_DAYS_APART,
) -> pd.DataFrame:
    cols = HOTSPOT_EVIDENCE_COLUMNS

    helix_ref = _prepare_helix_ref(helix_df)
    if helix_ref.empty:
        return pd.DataFrame(columns=cols)

    links = links_df.copy() if links_df is not None else pd.DataFrame()
    links["incident_id"] = links.get("incident_id", "").astype(str).str.strip()
    links["nps_id"] = links.get("nps_id", "").astype(str).str.strip()
    links["nps_topic"] = links.get("nps_topic", "").astype(str)
    links["similarity"] = pd.to_numeric(links.get("similarity", 0.0), errors="coerce").fillna(0.0)

    nps_ref = _prepare_nps_ref(nps_focus_df)
    if links.empty or nps_ref.empty:
        return pd.DataFrame(columns=cols)

    comment_map = (
        nps_ref.set_index("nps_id")["comment_txt"].astype(str).fillna("")
        if not nps_ref.empty
        else pd.Series(dtype=str)
    )
    nps_date_map = (
        nps_ref.set_index("nps_id")["date"]
        if not nps_ref.empty
        else pd.Series(dtype="datetime64[ns]")
    )

    helix_latest = (
        helix_ref.sort_values(["incident_id", "incident_date"], ascending=[True, True])
        .drop_duplicates(["incident_id"], keep="last")
        .copy()
    )
    summary_map = helix_latest.set_index("incident_id")["incident_summary"]
    date_map = helix_latest.set_index("incident_id")["incident_date"]

    helix_month = _month_slice(helix_ref, system_date)
    hot_terms = _select_hot_terms(
        helix_month,
        helix_ref,
        nps_ref,
        max_hotspots=max_hotspots,
        min_term_occurrences=min_term_occurrences,
    )

    out = (
        links[["incident_id", "nps_id", "nps_topic", "similarity"]].copy()
        if not links.empty
        else pd.DataFrame(columns=["incident_id", "nps_id", "nps_topic", "similarity"])
    )
    out["incident_summary"] = out["incident_id"].map(summary_map).fillna("")
    out["incident_summary_norm"] = out["incident_summary"].map(_norm_txt)
    out["detractor_comment"] = out["nps_id"].map(comment_map).fillna("")
    out["detractor_comment_norm"] = out["detractor_comment"].map(_norm_txt)
    out["incident_date"] = out["incident_id"].map(date_map)
    out["nps_date"] = out["nps_id"].map(nps_date_map)
    out["nps_topic"] = out["nps_topic"].astype(str).fillna("")
    out["_has_comment"] = out["detractor_comment"].astype(str).str.strip() != ""

    incident_topic_map = (
        out.groupby("incident_id", as_index=False)["nps_topic"].first().set_index("incident_id")["nps_topic"]
        if not out.empty
        else pd.Series(dtype=str)
    )
    incident_sim_map = (
        out.groupby("incident_id", as_index=False)["similarity"].max().set_index("incident_id")["similarity"]
        if not out.empty
        else pd.Series(dtype=float)
    )

    selected_rows: list[dict[str, object]] = []

    for rank_idx, term in enumerate(hot_terms, start=1):
        tpat = _term_regex(term)
        if not tpat:
            continue

        term_links = _strict_term_links(
            out,
            term_pattern=tpat,
            min_similarity=float(min_validated_similarity),
            max_days_apart=max_days_apart,
        )

        helix_term_all = helix_ref[helix_ref["summary_norm"].str.contains(tpat, regex=True, na=False)].copy()
        if not helix_term_all.empty:
            helix_term_all = helix_term_all.sort_values(["incident_date"], ascending=False)

        nps_term = (
            nps_ref[nps_ref["comment_norm"].str.contains(tpat, regex=True, na=False)].copy()
            if not nps_ref.empty
            else pd.DataFrame(columns=nps_ref.columns)
        )

        mention_incidents = int(
            helix_term_all["incident_id"].astype(str).str.strip().replace("", np.nan).dropna().nunique()
        )
        mention_comments = int(
            nps_term["nps_id"].astype(str).str.strip().replace("", np.nan).dropna().nunique()
        )
        dominant_topic = ""
        if not nps_term.empty and "nps_topic" in nps_term.columns:
            topic_series = nps_term["nps_topic"].astype(str).str.strip()
            topic_series = topic_series[topic_series != ""]
            if not topic_series.empty:
                dominant_topic = str(topic_series.value_counts(dropna=True).idxmax()).strip()
        # Keep hotspot only when the term is genuinely hot in both worlds (Helix + NPS).
        if mention_incidents <= 0 or mention_comments <= 0:
            continue
        hotspot_incidents = int(
            term_links["incident_id"].astype(str).str.strip().replace("", np.nan).dropna().nunique()
        )
        hotspot_comments = int(
            term_links["nps_id"].astype(str).str.strip().replace("", np.nan).dropna().nunique()
        )
        hotspot_links = int(len(term_links))

        rep_incidents: list[str] = []
        if not term_links.empty:
            rep_incidents.extend(
                [iid for iid in term_links["incident_id"].astype(str).str.strip().tolist() if iid]
            )
        if not helix_term_all.empty:
            rep_incidents.extend(
                [iid for iid in helix_term_all["incident_id"].astype(str).str.strip().tolist() if iid]
            )

        dedup_rep: list[str] = []
        seen_rep: set[str] = set()
        for iid in rep_incidents:
            if iid in seen_rep:
                continue
            seen_rep.add(iid)
            dedup_rep.append(iid)
            if len(dedup_rep) >= 3:
                break

        for inc_id in dedup_rep:
            hit_inc = term_links[term_links["incident_id"].astype(str).str.strip() == inc_id]
            chosen = hit_inc.head(1) if not hit_inc.empty else pd.DataFrame()

            topic_val = str(incident_topic_map.get(inc_id, "")).strip()
            if not topic_val:
                topic_val = str(dominant_topic or "").strip()
            if not topic_val:
                topic_val = f"Hotspot Helix: {term}"
            sim_val = float(incident_sim_map.get(inc_id, 0.0) or 0.0)
            date_val = date_map.get(inc_id, pd.NaT)
            summary_val = str(summary_map.get(inc_id, "") or "")

            if not chosen.empty:
                row0 = chosen.iloc[0]
                topic_val = str(row0.get("nps_topic", "") or topic_val)
                sim_val = float(row0.get("similarity", sim_val) or sim_val)
                summary_val = str(row0.get("incident_summary", "") or summary_val)
                date_val = row0.get("incident_date", date_val)

            detr_comment = ""
            hit_comments = hit_inc[hit_inc["_has_comment"]].head(1)
            if not hit_comments.empty:
                detr_comment = str(hit_comments.iloc[0].get("detractor_comment", "") or "")
            elif not nps_term.empty and "comment_txt" in nps_term.columns:
                # Mention-based fallback keeps narrative anchored in real NPS evidence.
                detr_comment = str(nps_term.iloc[0].get("comment_txt", "") or "")

            selected_rows.append(
                {
                    "incident_id": inc_id,
                    "incident_date": date_val,
                    "nps_topic": topic_val,
                    "incident_summary": summary_val,
                    "detractor_comment": detr_comment,
                    "similarity": sim_val,
                    "hot_term": str(term),
                    "hot_rank": int(rank_idx),
                    "mention_incidents": int(mention_incidents),
                    "mention_comments": int(mention_comments),
                    "hotspot_incidents": hotspot_incidents,
                    "hotspot_comments": hotspot_comments,
                    "hotspot_links": hotspot_links,
                }
            )

    if not selected_rows:
        return pd.DataFrame(columns=cols)

    res = pd.DataFrame(selected_rows)
    res["hot_rank"] = pd.to_numeric(res.get("hot_rank"), errors="coerce").fillna(999).astype(int)
    res["mention_incidents"] = (
        pd.to_numeric(res.get("mention_incidents"), errors="coerce").fillna(0).astype(int)
    )
    res["mention_comments"] = (
        pd.to_numeric(res.get("mention_comments"), errors="coerce").fillna(0).astype(int)
    )
    res["hotspot_incidents"] = pd.to_numeric(res.get("hotspot_incidents"), errors="coerce").fillna(0).astype(int)
    res["hotspot_comments"] = pd.to_numeric(res.get("hotspot_comments"), errors="coerce").fillna(0).astype(int)
    res["hotspot_links"] = pd.to_numeric(res.get("hotspot_links"), errors="coerce").fillna(0).astype(int)
    res = res.sort_values(["hot_rank", "similarity"], ascending=[True, False]).reset_index(drop=True)
    return res[cols].copy()


def build_hotspot_timeline(
    links_df: Optional[pd.DataFrame],
    nps_focus_df: Optional[pd.DataFrame],
    helix_df: Optional[pd.DataFrame],
    *,
    incident_evidence_df: Optional[pd.DataFrame] = None,
    max_hotspots: int = 3,
    min_validated_similarity: float = LINK_MIN_SIMILARITY,
    max_days_apart: int | None = LINK_MAX_DAYS_APART,
) -> pd.DataFrame:
    cols = HOTSPOT_TIMELINE_COLUMNS

    helix_ref = _prepare_helix_ref(helix_df)
    if helix_ref.empty:
        return pd.DataFrame(columns=cols)

    helix_ref["date"] = pd.to_datetime(helix_ref["incident_date"], errors="coerce").dt.normalize()
    helix_ref = helix_ref.dropna(subset=["date"])
    if helix_ref.empty:
        return pd.DataFrame(columns=cols)

    helix_daily = (
        helix_ref.groupby(["incident_id", "date"], as_index=False)
        .agg(
            helix_records=("incident_id", "size"),
            incident_ids=("incident_id", _unique_join),
        )
        .sort_values(["incident_id", "date"])
    )

    nps_ref = _prepare_nps_ref(nps_focus_df)
    nps_ref = nps_ref.dropna(subset=["date"]) if not nps_ref.empty else nps_ref

    links = links_df.copy() if links_df is not None else pd.DataFrame()
    links["incident_id"] = links.get("incident_id", "").astype(str).str.strip()
    links["nps_id"] = links.get("nps_id", "").astype(str).str.strip()
    links["nps_topic"] = links.get("nps_topic", "").astype(str)
    links["similarity"] = pd.to_numeric(links.get("similarity", 0.0), errors="coerce").fillna(0.0)

    nps_daily = pd.DataFrame(
        columns=[
            "incident_id",
            "date",
            "nps_comments",
            "nps_comments_moderate",
            "nps_comments_high",
            "nps_comments_critical",
        ]
    )
    if not links.empty and not nps_ref.empty:
        nps_daily = links.merge(nps_ref[["nps_id", "date", "severity"]], on="nps_id", how="left")
        nps_daily = nps_daily.dropna(subset=["date"])
        if not nps_daily.empty:
            nps_daily = nps_daily.drop_duplicates(["incident_id", "nps_id", "date"])
            nps_daily["sev_moderate"] = (nps_daily["severity"].astype(str) == "moderate").astype(int)
            nps_daily["sev_high"] = (nps_daily["severity"].astype(str) == "high").astype(int)
            nps_daily["sev_critical"] = (nps_daily["severity"].astype(str) == "critical").astype(int)
            nps_daily = (
                nps_daily.groupby(["incident_id", "date"], as_index=False)
                .agg(
                    nps_comments=("nps_id", "nunique"),
                    nps_comments_moderate=("sev_moderate", "sum"),
                    nps_comments_high=("sev_high", "sum"),
                    nps_comments_critical=("sev_critical", "sum"),
                )
            )

    out = helix_daily.merge(nps_daily, on=["incident_id", "date"], how="left")
    out["helix_records"] = (
        pd.to_numeric(out.get("helix_records"), errors="coerce").fillna(0.0).astype(int)
    )
    out["nps_comments"] = (
        pd.to_numeric(out.get("nps_comments"), errors="coerce").fillna(0.0).astype(int)
    )
    out["nps_comments_moderate"] = (
        pd.to_numeric(out.get("nps_comments_moderate"), errors="coerce").fillna(0.0).astype(int)
    )
    out["nps_comments_high"] = (
        pd.to_numeric(out.get("nps_comments_high"), errors="coerce").fillna(0.0).astype(int)
    )
    out["nps_comments_critical"] = (
        pd.to_numeric(out.get("nps_comments_critical"), errors="coerce").fillna(0.0).astype(int)
    )
    out["incident_ids"] = out.get("incident_ids", "").astype(str).fillna("")
    out = out[(out["helix_records"] > 0) | (out["nps_comments"] > 0)].copy()
    out["hot_term"] = ""
    out = out[cols].copy()

    top_terms: list[str] = []
    if (
        incident_evidence_df is not None
        and not incident_evidence_df.empty
        and "hot_term" in incident_evidence_df.columns
    ):
        ev = incident_evidence_df.copy()
        ev["hot_term"] = ev["hot_term"].astype(str).str.strip()
        if "hot_rank" in ev.columns:
            ev["hot_rank"] = pd.to_numeric(ev["hot_rank"], errors="coerce")
            ev = ev.sort_values(["hot_rank"], na_position="last")
        top_terms = ev[ev["hot_term"] != ""]["hot_term"].drop_duplicates().head(int(max_hotspots)).tolist()

    hot_rows: list[pd.DataFrame] = []
    for term in top_terms:
        tpat = _term_regex(term)
        if not tpat:
            continue
        # Timeline for hotspot zooms uses full cross-source mention scope by term.
        # Strict validated links remain available in hotspot summary counters.
        helix_term_all = helix_ref[
            helix_ref["summary_norm"].str.contains(tpat, regex=True, na=False)
        ].copy()
        helix_term_daily = (
            helix_term_all.groupby("date", as_index=False).agg(
                helix_records=("incident_id", "size"),
                incident_ids=("incident_id", _unique_join),
            )
            if not helix_term_all.empty
            else pd.DataFrame(columns=["date", "helix_records", "incident_ids"])
        )

        nps_term_daily = pd.DataFrame(
            columns=[
                "date",
                "nps_comments",
                "nps_comments_moderate",
                "nps_comments_high",
                "nps_comments_critical",
            ]
        )
        if not nps_ref.empty:
            severity_map = nps_ref.set_index("nps_id")["severity"] if "nps_id" in nps_ref.columns else pd.Series(dtype=str)
            nps_term = nps_ref[nps_ref["comment_norm"].str.contains(tpat, regex=True, na=False)].copy()
            if not nps_term.empty:
                nps_term = nps_term.drop_duplicates(["date", "nps_id"])[["nps_id", "date"]].copy()
            if not nps_term.empty:
                nps_term["severity"] = nps_term["nps_id"].map(severity_map).fillna("moderate")
                nps_term["sev_moderate"] = (nps_term["severity"].astype(str) == "moderate").astype(int)
                nps_term["sev_high"] = (nps_term["severity"].astype(str) == "high").astype(int)
                nps_term["sev_critical"] = (nps_term["severity"].astype(str) == "critical").astype(int)
                nps_term_daily = (
                    nps_term.groupby("date", as_index=False).agg(
                        nps_comments=("nps_id", "nunique"),
                        nps_comments_moderate=("sev_moderate", "sum"),
                        nps_comments_high=("sev_high", "sum"),
                        nps_comments_critical=("sev_critical", "sum"),
                    )
                )

        # Enforce cross-source evidence for hotspot timeline rows.
        if helix_term_daily.empty or nps_term_daily.empty:
            continue

        term_daily = helix_term_daily.merge(nps_term_daily, on="date", how="outer")
        if term_daily.empty:
            continue

        term_daily["helix_records"] = (
            pd.to_numeric(term_daily.get("helix_records"), errors="coerce").fillna(0.0).astype(int)
        )
        term_daily["nps_comments"] = (
            pd.to_numeric(term_daily.get("nps_comments"), errors="coerce").fillna(0.0).astype(int)
        )
        term_daily["nps_comments_moderate"] = (
            pd.to_numeric(term_daily.get("nps_comments_moderate"), errors="coerce")
            .fillna(0.0)
            .astype(int)
        )
        term_daily["nps_comments_high"] = (
            pd.to_numeric(term_daily.get("nps_comments_high"), errors="coerce")
            .fillna(0.0)
            .astype(int)
        )
        term_daily["nps_comments_critical"] = (
            pd.to_numeric(term_daily.get("nps_comments_critical"), errors="coerce")
            .fillna(0.0)
            .astype(int)
        )
        term_daily["incident_ids"] = term_daily.get("incident_ids", "").astype(str).fillna("")
        term_daily = term_daily[(term_daily["helix_records"] > 0) | (term_daily["nps_comments"] > 0)].copy()
        if term_daily.empty:
            continue

        term_daily["incident_id"] = ""
        term_daily["hot_term"] = str(term).strip()
        hot_rows.append(term_daily[cols])

    if hot_rows:
        out = pd.concat([out] + hot_rows, ignore_index=True)

    if out.empty:
        return pd.DataFrame(columns=cols)
    return out.sort_values(["hot_term", "incident_id", "date"]).reset_index(drop=True)[cols].copy()


def summarize_hotspot_counts(
    incident_evidence_df: Optional[pd.DataFrame],
    incident_timeline_df: Optional[pd.DataFrame],
    *,
    max_hotspots: int = 3,
) -> pd.DataFrame:
    cols = HOTSPOT_SUMMARY_COLUMNS

    e = incident_evidence_df.copy() if incident_evidence_df is not None else pd.DataFrame()
    t = incident_timeline_df.copy() if incident_timeline_df is not None else pd.DataFrame()

    if e.empty and t.empty:
        return pd.DataFrame(columns=cols)

    terms: list[tuple[int, str]] = []
    if not e.empty and "hot_term" in e.columns:
        e["hot_term"] = e["hot_term"].astype(str).str.strip()
        e["hot_rank"] = pd.to_numeric(e.get("hot_rank"), errors="coerce").fillna(999).astype(int)
        ee = e[e["hot_term"] != ""].sort_values(["hot_rank"])
        for _, r in ee[["hot_rank", "hot_term"]].drop_duplicates().head(int(max_hotspots)).iterrows():
            terms.append((int(r["hot_rank"]), str(r["hot_term"])))

    if not terms and not t.empty and "hot_term" in t.columns:
        t2 = t.copy()
        t2["hot_term"] = t2["hot_term"].astype(str).str.strip()
        for idx, term in enumerate(t2[t2["hot_term"] != ""]["hot_term"].drop_duplicates().head(int(max_hotspots)).tolist(), start=1):
            terms.append((idx, str(term)))

    if not terms:
        return pd.DataFrame(columns=cols)

    rows: list[dict[str, int | str]] = []
    for rank, term in terms:
        ev = e[e.get("hot_term", "").astype(str).str.strip() == str(term)] if not e.empty else pd.DataFrame()
        tl = t[t.get("hot_term", "").astype(str).str.strip() == str(term)] if not t.empty else pd.DataFrame()
        has_hotspot_inc_col = bool(not ev.empty and "hotspot_incidents" in ev.columns)
        has_hotspot_com_col = bool(not ev.empty and "hotspot_comments" in ev.columns)
        has_hotspot_lnk_col = bool(not ev.empty and "hotspot_links" in ev.columns)

        mention_incidents = (
            int(
                pd.to_numeric(
                    ev.get("mention_incidents", pd.Series([0] * len(ev), index=ev.index)),
                    errors="coerce",
                )
                .fillna(0)
                .max()
            )
            if not ev.empty
            else 0
        )
        mention_comments = (
            int(
                pd.to_numeric(
                    ev.get("mention_comments", pd.Series([0] * len(ev), index=ev.index)),
                    errors="coerce",
                )
                .fillna(0)
                .max()
            )
            if not ev.empty
            else 0
        )
        hotspot_incidents = (
            int(
                pd.to_numeric(
                    ev.get("hotspot_incidents", pd.Series([0] * len(ev), index=ev.index)),
                    errors="coerce",
                )
                .fillna(0)
                .max()
            )
            if not ev.empty
            else 0
        )
        hotspot_comments = (
            int(
                pd.to_numeric(
                    ev.get("hotspot_comments", pd.Series([0] * len(ev), index=ev.index)),
                    errors="coerce",
                )
                .fillna(0)
                .max()
            )
            if not ev.empty
            else 0
        )
        hotspot_links = (
            int(
                pd.to_numeric(
                    ev.get("hotspot_links", pd.Series([0] * len(ev), index=ev.index)),
                    errors="coerce",
                )
                .fillna(0)
                .max()
            )
            if not ev.empty
            else 0
        )

        if mention_incidents <= 0 and hotspot_incidents > 0:
            mention_incidents = int(hotspot_incidents)
        if mention_comments <= 0 and hotspot_comments > 0:
            mention_comments = int(hotspot_comments)
        if (not has_hotspot_inc_col) and hotspot_incidents <= 0 and not ev.empty and "incident_id" in ev.columns:
            hotspot_incidents = int(ev["incident_id"].astype(str).str.strip().replace("", np.nan).dropna().nunique())
        if (not has_hotspot_com_col) and hotspot_comments <= 0 and not ev.empty and "detractor_comment" in ev.columns:
            hotspot_comments = int(ev["detractor_comment"].astype(str).str.strip().replace("", np.nan).dropna().nunique())
        if (not has_hotspot_lnk_col) and hotspot_links <= 0 and not ev.empty:
            hotspot_links = int(len(ev))

        chart_helix = 0
        chart_comments = 0
        days = 0
        if not tl.empty and {"helix_records", "nps_comments", "date"}.issubset(set(tl.columns)):
            helix_s = pd.to_numeric(tl.get("helix_records"), errors="coerce").fillna(0.0)
            comm_s = pd.to_numeric(tl.get("nps_comments"), errors="coerce").fillna(0.0)
            chart_helix = int(helix_s.sum())
            chart_comments = int(comm_s.sum())
            day_mask = (helix_s > 0) | (comm_s > 0)
            days = int(pd.to_datetime(tl.loc[day_mask, "date"], errors="coerce").dropna().dt.normalize().nunique())

        rows.append(
            {
                "hot_rank": int(rank),
                "hot_term": str(term),
                "mention_incidents": int(mention_incidents),
                "mention_comments": int(mention_comments),
                "hotspot_incidents": int(hotspot_incidents),
                "hotspot_comments": int(hotspot_comments),
                "hotspot_links": int(hotspot_links),
                "chart_helix_records": int(chart_helix),
                "chart_nps_comments": int(chart_comments),
                "days_with_evidence": int(days),
            }
        )

    out = pd.DataFrame(rows)
    out = out.sort_values(["hot_rank"]).reset_index(drop=True)
    return out[cols].copy()


def build_hotspot_daily_breakdown(
    daily_signals: Optional[pd.DataFrame],
    incident_evidence_df: Optional[pd.DataFrame],
    incident_timeline_df: Optional[pd.DataFrame],
    *,
    max_hotspots: int = 3,
) -> tuple[pd.DataFrame, dict[int, str]]:
    cols = ["date", "incidents", "no_hotspot", "hotspot_1", "hotspot_2", "hotspot_3"]

    if daily_signals is None or daily_signals.empty:
        return pd.DataFrame(columns=cols), {}
    if "date" not in daily_signals.columns or "incidents" not in daily_signals.columns:
        return pd.DataFrame(columns=cols), {}

    base = daily_signals[["date", "incidents"]].copy()
    base["date"] = pd.to_datetime(base["date"], errors="coerce")
    base = base.dropna(subset=["date"])
    if base.empty:
        return pd.DataFrame(columns=cols), {}

    base["date"] = base["date"].dt.normalize()
    base["incidents"] = pd.to_numeric(base["incidents"], errors="coerce").fillna(0.0).clip(lower=0.0)
    base = (
        base.groupby("date", as_index=False)
        .agg(incidents=("incidents", "sum"))
        .sort_values("date")
        .reset_index(drop=True)
    )

    term_by_rank: dict[int, str] = {}
    rank_map = pd.DataFrame(columns=["incident_id", "hot_rank", "hot_term"])

    if incident_evidence_df is not None and not incident_evidence_df.empty:
        e = incident_evidence_df.copy()
        e["incident_id"] = e.get("incident_id", "").astype(str).str.strip()
        e["hot_rank"] = pd.to_numeric(e.get("hot_rank"), errors="coerce")
        e["hot_term"] = e.get("hot_term", "").astype(str).str.strip()
        e["similarity"] = pd.to_numeric(e.get("similarity", 0.0), errors="coerce").fillna(0.0)
        e = e.dropna(subset=["hot_rank"]).copy()
        e = e[e["hot_rank"].between(1, int(max_hotspots), inclusive="both")]
        e = e[e["incident_id"] != ""]

        if not e.empty:
            term_rank = (
                e.sort_values(["hot_rank", "similarity"], ascending=[True, False])
                [["hot_rank", "hot_term"]]
                .drop_duplicates(["hot_rank"]) 
            )
            for _, r in term_rank.iterrows():
                rk = int(float(r["hot_rank"]))
                if 1 <= rk <= int(max_hotspots):
                    term_by_rank[rk] = str(r["hot_term"]).strip()

            rank_map = (
                e.sort_values(["hot_rank", "similarity"], ascending=[True, False])
                .drop_duplicates(["incident_id"])
                [["incident_id", "hot_rank", "hot_term"]]
                .copy()
            )
            rank_map["hot_rank"] = rank_map["hot_rank"].astype(int)

    for rk in [1, 2, 3]:
        base[f"hotspot_{rk}"] = 0.0

    used_term_timeline = False
    if (
        incident_timeline_df is not None
        and not incident_timeline_df.empty
        and {"date", "helix_records"}.issubset(set(incident_timeline_df.columns))
    ):
        t = incident_timeline_df.copy()
        t["incident_id"] = (
            t.get("incident_id", pd.Series([""] * len(t), index=t.index)).astype(str).str.strip()
        )
        t["hot_term"] = (
            t.get("hot_term", pd.Series([""] * len(t), index=t.index)).astype(str).str.strip()
        )
        t["incident_ids"] = (
            t.get("incident_ids", pd.Series([""] * len(t), index=t.index)).astype(str).fillna("")
        )
        t["date"] = pd.to_datetime(t.get("date"), errors="coerce").dt.normalize()
        t["helix_records"] = pd.to_numeric(t.get("helix_records"), errors="coerce").fillna(0.0)
        t = t[t["helix_records"] > 0].dropna(subset=["date"])

        term_to_rank = {str(v).strip().lower(): int(k) for k, v in term_by_rank.items() if str(v).strip()}
        base_inc = t[t["incident_id"] != ""][["date", "incident_id", "helix_records"]].copy()

        if not base_inc.empty and term_to_rank:
            hot_term_rows = t[t["hot_term"] != ""].copy()
            mapping_rows: list[dict[str, object]] = []
            for _, row in hot_term_rows.iterrows():
                rk = term_to_rank.get(str(row.get("hot_term", "")).strip().lower())
                if rk is None:
                    continue
                raw_ids = [p.strip() for p in str(row.get("incident_ids", "")).split("|")]
                ids = [iid for iid in raw_ids if iid]
                if not ids:
                    continue
                d = pd.to_datetime(row.get("date"), errors="coerce")
                if pd.isna(d):
                    continue
                for iid in ids:
                    mapping_rows.append(
                        {
                            "date": pd.Timestamp(d).normalize(),
                            "incident_id": str(iid).strip(),
                            "hot_rank": int(rk),
                        }
                    )

            if mapping_rows:
                map_df = pd.DataFrame(mapping_rows).drop_duplicates(["date", "incident_id", "hot_rank"])
                map_df = map_df.sort_values(["hot_rank"]).drop_duplicates(["date", "incident_id"], keep="first")
                assigned = base_inc.merge(map_df, on=["date", "incident_id"], how="left")
                hot = assigned.dropna(subset=["hot_rank"]).copy()
                if not hot.empty:
                    hot["hot_rank"] = hot["hot_rank"].astype(int)
                    hot = (
                        hot.groupby(["date", "hot_rank"], as_index=False)
                        .agg(cnt=("helix_records", "sum"))
                    )
                    pivot = (
                        hot.pivot(index="date", columns="hot_rank", values="cnt")
                        .fillna(0.0)
                        .rename(columns={1: "hotspot_1", 2: "hotspot_2", 3: "hotspot_3"})
                        .reset_index()
                    )
                    base = base.merge(pivot, on="date", how="left", suffixes=("", "_new"))
                    for rk in [1, 2, 3]:
                        c = f"hotspot_{rk}"
                        cnew = f"{c}_new"
                        if cnew in base.columns:
                            base[c] = pd.to_numeric(base[cnew], errors="coerce").fillna(0.0)
                            base = base.drop(columns=[cnew])
                    used_term_timeline = True

        if (not used_term_timeline) and (not rank_map.empty):
            t_inc = t[t["incident_id"] != ""].copy()
            if not t_inc.empty:
                hot = t_inc.merge(rank_map[["incident_id", "hot_rank"]], on="incident_id", how="inner")
                if not hot.empty:
                    hot = (
                        hot.groupby(["date", "hot_rank"], as_index=False)
                        .agg(cnt=("helix_records", "sum"))
                    )
                    pivot = (
                        hot.pivot(index="date", columns="hot_rank", values="cnt")
                        .fillna(0.0)
                        .rename(columns={1: "hotspot_1", 2: "hotspot_2", 3: "hotspot_3"})
                        .reset_index()
                    )
                    base = base.merge(pivot, on="date", how="left", suffixes=("", "_new"))
                    for rk in [1, 2, 3]:
                        c = f"hotspot_{rk}"
                        cnew = f"{c}_new"
                        if cnew in base.columns:
                            base[c] = pd.to_numeric(base[cnew], errors="coerce").fillna(0.0)
                            base = base.drop(columns=[cnew])

    for rk in [1, 2, 3]:
        c = f"hotspot_{rk}"
        base[c] = pd.to_numeric(base[c], errors="coerce").fillna(0.0).clip(lower=0.0)

    base["no_hotspot"] = (
        base["incidents"] - base["hotspot_1"] - base["hotspot_2"] - base["hotspot_3"]
    ).clip(lower=0.0)

    return base[cols].copy(), term_by_rank
