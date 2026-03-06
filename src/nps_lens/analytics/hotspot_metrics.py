from __future__ import annotations

import re
import unicodedata
from collections import Counter
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd

from nps_lens.analytics.nps_helix_link import build_incident_display_text

HOTSPOT_EVIDENCE_COLUMNS = [
    "incident_id",
    "incident_date",
    "nps_topic",
    "incident_summary",
    "detractor_comment",
    "similarity",
    "hot_term",
    "hot_rank",
    "hotspot_incidents",
    "hotspot_comments",
    "hotspot_links",
]

HOTSPOT_TIMELINE_COLUMNS = ["incident_id", "hot_term", "date", "helix_records", "nps_comments"]

HOTSPOT_SUMMARY_COLUMNS = [
    "hot_rank",
    "hot_term",
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


def _comment_column(df: pd.DataFrame) -> str:
    if "Comment" in df.columns:
        return "Comment"
    if "Comentario" in df.columns:
        return "Comentario"
    return ""


def _prepare_nps_ref(nps_focus_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if nps_focus_df is None or nps_focus_df.empty:
        return pd.DataFrame(columns=["nps_id", "date", "comment_txt", "comment_norm"])

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


def build_hotspot_evidence(
    links_df: Optional[pd.DataFrame],
    nps_focus_df: Optional[pd.DataFrame],
    helix_df: Optional[pd.DataFrame],
    *,
    system_date: Optional[date] = None,
    max_hotspots: int = 3,
    min_term_occurrences: int = 2,
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

    comment_map = (
        nps_ref.set_index("nps_id")["comment_txt"].astype(str).fillna("")
        if not nps_ref.empty
        else pd.Series(dtype=str)
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
        term_norm = _norm_txt(term)
        if not term_norm:
            continue
        tpat = rf"\b{re.escape(term_norm)}\b"

        term_links = (
            out[
                out["incident_summary_norm"].str.contains(tpat, regex=True, na=False)
                | out["detractor_comment_norm"].str.contains(tpat, regex=True, na=False)
            ].copy()
            if not out.empty
            else pd.DataFrame(columns=out.columns)
        )
        if not term_links.empty:
            term_links = term_links.sort_values(["_has_comment", "similarity"], ascending=[False, False])

        helix_term_all = helix_ref[helix_ref["summary_norm"].str.contains(tpat, regex=True, na=False)].copy()
        if not helix_term_all.empty:
            helix_term_all = helix_term_all.sort_values(["incident_date"], ascending=False)

        nps_term = (
            nps_ref[nps_ref["comment_norm"].str.contains(tpat, regex=True, na=False)].copy()
            if not nps_ref.empty
            else pd.DataFrame(columns=nps_ref.columns)
        )

        if term_links.empty and helix_term_all.empty and nps_term.empty:
            continue

        hotspot_incidents = int(helix_term_all["incident_id"].astype(str).str.strip().replace("", np.nan).dropna().nunique())
        if hotspot_incidents <= 0:
            hotspot_incidents = int(
                term_links["incident_id"].astype(str).str.strip().replace("", np.nan).dropna().nunique()
            )

        hotspot_comments = int(nps_term["nps_id"].astype(str).str.strip().replace("", np.nan).dropna().nunique())
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

            topic_val = str(incident_topic_map.get(inc_id, "")).strip() or f"Hotspot Helix: {term}"
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
                    "hotspot_incidents": hotspot_incidents,
                    "hotspot_comments": hotspot_comments,
                    "hotspot_links": hotspot_links,
                }
            )

    if not selected_rows:
        if not out.empty:
            fallback = out.sort_values(["_has_comment", "similarity"], ascending=[False, False]).head(3)
            fb = fallback.copy()
            fb["hot_term"] = ""
            fb["hot_rank"] = np.arange(1, len(fb) + 1)
            fb["hotspot_incidents"] = 1
            fb["hotspot_comments"] = fb["detractor_comment"].astype(str).str.strip().ne("").astype(int)
            fb["hotspot_links"] = 1
            return fb[cols].copy()

        helix_fb = helix_latest.sort_values("incident_date", ascending=False).head(3).copy()
        if helix_fb.empty:
            return pd.DataFrame(columns=cols)
        helix_fb["nps_topic"] = ""
        helix_fb["detractor_comment"] = ""
        helix_fb["similarity"] = 0.0
        helix_fb["hot_term"] = ""
        helix_fb["hot_rank"] = np.arange(1, len(helix_fb) + 1)
        helix_fb["hotspot_incidents"] = 1
        helix_fb["hotspot_comments"] = 0
        helix_fb["hotspot_links"] = 0
        return helix_fb[
            [
                "incident_id",
                "incident_date",
                "nps_topic",
                "incident_summary",
                "detractor_comment",
                "similarity",
                "hot_term",
                "hot_rank",
                "hotspot_incidents",
                "hotspot_comments",
                "hotspot_links",
            ]
        ].copy()

    res = pd.DataFrame(selected_rows)
    res["hot_rank"] = pd.to_numeric(res.get("hot_rank"), errors="coerce").fillna(999).astype(int)
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
        .agg(helix_records=("incident_id", "size"))
        .sort_values(["incident_id", "date"])
    )

    nps_ref = _prepare_nps_ref(nps_focus_df)
    nps_ref = nps_ref.dropna(subset=["date"]) if not nps_ref.empty else nps_ref

    nps_daily = pd.DataFrame(columns=["incident_id", "date", "nps_comments"])
    if links_df is not None and not links_df.empty and not nps_ref.empty:
        links = links_df.copy()
        links["incident_id"] = links.get("incident_id", "").astype(str)
        links["nps_id"] = links.get("nps_id", "").astype(str)
        nps_daily = links.merge(nps_ref[["nps_id", "date"]], on="nps_id", how="left")
        nps_daily = nps_daily.dropna(subset=["date"])
        if not nps_daily.empty:
            nps_daily = (
                nps_daily.groupby(["incident_id", "date"], as_index=False)
                .agg(nps_comments=("nps_id", "nunique"))
            )

    out = helix_daily.merge(nps_daily, on=["incident_id", "date"], how="left")
    out["helix_records"] = pd.to_numeric(out.get("helix_records"), errors="coerce").fillna(0.0).astype(int)
    out["nps_comments"] = pd.to_numeric(out.get("nps_comments"), errors="coerce").fillna(0.0).astype(int)
    out = out[(out["helix_records"] > 0) | (out["nps_comments"] > 0)].copy()
    out["hot_term"] = ""
    out = out[cols].copy()

    top_terms: list[str] = []
    if incident_evidence_df is not None and not incident_evidence_df.empty and "hot_term" in incident_evidence_df.columns:
        ev = incident_evidence_df.copy()
        ev["hot_term"] = ev["hot_term"].astype(str).str.strip()
        if "hot_rank" in ev.columns:
            ev["hot_rank"] = pd.to_numeric(ev["hot_rank"], errors="coerce")
            ev = ev.sort_values(["hot_rank"], na_position="last")
        top_terms = ev[ev["hot_term"] != ""]["hot_term"].drop_duplicates().head(int(max_hotspots)).tolist()

    hot_rows: list[pd.DataFrame] = []
    for term in top_terms:
        term_norm = _norm_txt(term)
        if not term_norm:
            continue
        pat = rf"\b{re.escape(term_norm)}\b"

        helix_term = helix_ref[helix_ref["summary_norm"].str.contains(pat, regex=True, na=False)].copy()
        helix_term_daily = (
            helix_term.groupby("date", as_index=False).agg(helix_records=("incident_id", "size"))
            if not helix_term.empty
            else pd.DataFrame(columns=["date", "helix_records"])
        )

        nps_term_daily = pd.DataFrame(columns=["date", "nps_comments"])
        if not nps_ref.empty:
            nps_term = nps_ref[nps_ref["comment_norm"].str.contains(pat, regex=True, na=False)].copy()
            if not nps_term.empty:
                nps_term_daily = (
                    nps_term.groupby("date", as_index=False).agg(nps_comments=("nps_id", "nunique"))
                )

        term_daily = helix_term_daily.merge(nps_term_daily, on="date", how="outer")
        if term_daily.empty:
            continue

        term_daily["helix_records"] = pd.to_numeric(term_daily.get("helix_records"), errors="coerce").fillna(0.0).astype(int)
        term_daily["nps_comments"] = pd.to_numeric(term_daily.get("nps_comments"), errors="coerce").fillna(0.0).astype(int)
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

        hotspot_incidents = int(pd.to_numeric(ev.get("hotspot_incidents"), errors="coerce").fillna(0).max()) if not ev.empty else 0
        hotspot_comments = int(pd.to_numeric(ev.get("hotspot_comments"), errors="coerce").fillna(0).max()) if not ev.empty else 0
        hotspot_links = int(pd.to_numeric(ev.get("hotspot_links"), errors="coerce").fillna(0).max()) if not ev.empty else 0

        if hotspot_incidents <= 0 and not ev.empty and "incident_id" in ev.columns:
            hotspot_incidents = int(ev["incident_id"].astype(str).str.strip().replace("", np.nan).dropna().nunique())
        if hotspot_comments <= 0 and not ev.empty and "detractor_comment" in ev.columns:
            hotspot_comments = int(ev["detractor_comment"].astype(str).str.strip().replace("", np.nan).dropna().nunique())
        if hotspot_links <= 0 and not ev.empty:
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

    if (
        incident_timeline_df is not None
        and not incident_timeline_df.empty
        and not rank_map.empty
        and {"incident_id", "date", "helix_records"}.issubset(set(incident_timeline_df.columns))
    ):
        t = incident_timeline_df.copy()
        t["incident_id"] = t.get("incident_id", "").astype(str).str.strip()
        t["date"] = pd.to_datetime(t.get("date"), errors="coerce").dt.normalize()
        t["helix_records"] = pd.to_numeric(t.get("helix_records"), errors="coerce").fillna(0.0)
        t = t[(t["incident_id"] != "") & (t["helix_records"] > 0)].dropna(subset=["date"])

        if not t.empty:
            hot = t.merge(rank_map[["incident_id", "hot_rank"]], on="incident_id", how="inner")
            if not hot.empty:
                hot = (
                    hot.groupby(["date", "hot_rank"], as_index=False)
                    .agg(cnt=("incident_id", "nunique"))
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
