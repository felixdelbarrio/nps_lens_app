from __future__ import annotations

import re
import unicodedata
from typing import Optional

import numpy as np
import pandas as pd

from nps_lens.analytics.nps_helix_link import build_incident_display_text

TOUCHPOINT_SOURCE_DOMAIN = "domain_touchpoint"
TOUCHPOINT_SOURCE_PALANCA = "palanca_touchpoint"
TOUCHPOINT_SOURCE_BBVA_SOURCE_N2 = "bbva_source_service_n2"
TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS = "executive_journeys"

TOUCHPOINT_MODE_OPTIONS = (
    TOUCHPOINT_SOURCE_PALANCA,
    TOUCHPOINT_SOURCE_DOMAIN,
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2,
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
)

TOUCHPOINT_MODE_MENU_LABELS = {
    TOUCHPOINT_SOURCE_PALANCA: "Causalidad por Palanca",
    TOUCHPOINT_SOURCE_DOMAIN: "Causalidad por Subpalanca",
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2: "Causalidad por BBVA_SourceServiceN2",
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: "Journeys ejecutivos de detracción",
}

TOUCHPOINT_MODE_CONTEXT_LABELS = {
    TOUCHPOINT_SOURCE_PALANCA: "Palanca",
    TOUCHPOINT_SOURCE_DOMAIN: "Subpalanca",
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2: "BBVA_SourceServiceN2",
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: "Journeys ejecutivos de detracción",
}

TOUCHPOINT_MODE_BANNER_LABELS = {
    TOUCHPOINT_SOURCE_PALANCA: "Causalidad por Palanca",
    TOUCHPOINT_SOURCE_DOMAIN: "Causalidad por Subpalanca",
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2: "Causalidad por BBVA_SourceServiceN2",
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: "Journeys ejecutivos de detracción",
}

TOUCHPOINT_MODE_SUMMARIES = {
    TOUCHPOINT_SOURCE_PALANCA: "La lectura causal fija el touchpoint exclusivamente desde Palanca para mantener una taxonomía simple y homogénea.",
    TOUCHPOINT_SOURCE_DOMAIN: "La lectura causal fija el touchpoint exclusivamente desde Subpalanca para reflejar el nivel operativo fino del dolor reportado.",
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2: "La lectura causal se apoya exclusivamente en BBVA_SourceServiceN2 para reflejar el servicio origen reportado por Helix.",
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: "La lectura causal se reorganiza en journeys de comité para explicar dónde se rompe la experiencia y por qué cae el NPS.",
}

EXECUTIVE_JOURNEY_CATALOG = (
    {
        "id": "executive_access_blocked",
        "title": "Acceso bloqueado",
        "what_occurs": "El cliente no puede acceder a la aplicación o portal",
        "expected_evidence": "Comentarios sobre login + incidencias de autenticación",
        "impact_label": "Muy alto",
        "touchpoint": "Login / autenticación",
        "palanca": "Acceso",
        "subpalanca": "Bloqueo / OTP",
        "route": (
            "Acceso -> Login / autenticación -> error / bloqueo / OTP -> "
            "incidencia Helix -> comentario detractor"
        ),
        "cx_readout": (
            "Cuando el acceso falla, el cliente no puede iniciar su relación digital, "
            "generando detracción inmediata."
        ),
        "confidence_label": "Alto",
        "keywords": (
            "acceso",
            "login",
            "autentic",
            "entrar",
            "ingresar",
            "sesion",
            "otp",
            "codigo",
            "credencial",
            "desloguea",
            "acceder",
        ),
    },
    {
        "id": "executive_critical_operation_failed",
        "title": "Operativa crítica fallida",
        "what_occurs": "Transferencias, pagos o firma no se completan",
        "expected_evidence": "Comentarios de operación fallida + incidencias transaccionales",
        "impact_label": "Alto",
        "touchpoint": "Transferencias / pagos / firma",
        "palanca": "Operativa",
        "subpalanca": "Error funcional / timeout",
        "route": (
            "Operativa -> transferencias / pagos / firma -> error funcional o timeout -> "
            "incidencia transaccional -> comentario detractor"
        ),
        "cx_readout": (
            "La incapacidad de completar operaciones financieras genera pérdida directa "
            "de confianza en el canal digital."
        ),
        "confidence_label": "Alto",
        "keywords": (
            "transfer",
            "pago",
            "firma",
            "firmar",
            "operacion",
            "operativa",
            "transaccion",
            "transacciones",
            "spei",
            "deposito",
            "retiro",
            "completar",
            "timeout",
        ),
    },
    {
        "id": "executive_degraded_performance",
        "title": "Rendimiento degradado",
        "what_occurs": "Lentitud o cuelgues durante el uso",
        "expected_evidence": "Comentarios de lentitud + incidencias de performance",
        "impact_label": "Medio-alto",
        "touchpoint": "Lentitud / cuelgues",
        "palanca": "Uso recurrente",
        "subpalanca": "Degradación del servicio",
        "route": (
            "Uso recurrente -> lentitud / cuelgues -> degradación del servicio -> "
            "comentario de mala experiencia -> detractor o pasivo"
        ),
        "cx_readout": (
            "No siempre bloquea la operación, pero erosiona progresivamente la percepción "
            "de calidad del servicio."
        ),
        "confidence_label": "Medio",
        "keywords": (
            "lento",
            "lenta",
            "lentitud",
            "cuelga",
            "cuelgue",
            "cuelgues",
            "bloqueada",
            "bloqueado",
            "performance",
            "rendimiento",
            "carga",
            "cargar",
            "tarda",
            "demora",
        ),
    },
)
EXECUTIVE_JOURNEY_BY_ID = {str(item["id"]): item for item in EXECUTIVE_JOURNEY_CATALOG}

CHAIN_COLUMNS = [
    "nps_topic",
    "touchpoint",
    "palanca",
    "subpalanca",
    "linked_incidents",
    "linked_comments",
    "linked_pairs",
    "avg_similarity",
    "avg_nps",
    "detractor_probability",
    "nps_delta_expected",
    "total_nps_impact",
    "nps_points_at_risk",
    "nps_points_recoverable",
    "priority",
    "confidence",
    "causal_score",
    "incident_records",
    "incident_examples",
    "comment_examples",
    "chain_story",
    "delta_focus_rate_pp",
    "incident_rate_per_100_responses",
    "incidents",
    "responses",
    "action_lane",
    "owner_role",
    "eta_weeks",
    "presentation_mode",
    "journey_route",
    "journey_expected_evidence",
    "journey_cx_readout",
    "journey_impact_label",
    "journey_confidence_label",
]

_GENERIC_LABELS = {
    "",
    "nan",
    "none",
    "na",
    "n/a",
    "sin comentario",
    "sin comentarios",
    "sincomentario",
    "sincomentarios",
    "no comment",
    "no comments",
}


def _norm(value: object) -> str:
    txt = " ".join(str(value or "").split()).strip().lower()
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"[^a-z0-9> ]+", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _is_generic(value: object) -> bool:
    norm = _norm(value).strip()
    if not norm:
        return True
    flattened = norm.replace(" > ", " ").replace(">", " ").strip()
    if flattened in _GENERIC_LABELS:
        return True
    parts = [p.strip() for p in norm.split(">") if p.strip()]
    return bool(parts) and all(p in _GENERIC_LABELS for p in parts)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not np.isfinite(out):
        return float(default)
    return float(out)


def _limit_ranked_examples(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    try:
        max_items = int(limit)
    except Exception:
        return df.copy()
    if max_items <= 0:
        return df.copy()
    return df.head(max_items).copy()


def _mode_text(series: pd.Series) -> str:
    if series is None:
        return ""
    txt = pd.Series(series).astype(str).str.strip()
    txt = txt[txt.ne("") & txt.ne("nan") & txt.ne("None")]
    if txt.empty:
        return ""
    mode = txt.mode(dropna=True)
    if mode.empty:
        return ""
    return str(mode.iloc[0]).strip()


def _clip(value: object, max_len: int) -> str:
    txt = " ".join(str(value or "").split())
    if len(txt) <= int(max_len):
        return txt
    return txt[: max_len - 1].rstrip() + "…"


def _touchpoint(
    palanca: object,
    subpalanca: object,
    incident_topic: object,
    *,
    helix_source_service_n2: object = "",
    source: str = TOUCHPOINT_SOURCE_DOMAIN,
) -> str:
    source_norm = str(source or TOUCHPOINT_SOURCE_DOMAIN).strip()
    if source_norm == TOUCHPOINT_SOURCE_PALANCA:
        pal = str(palanca or "").strip()
        return pal if pal and not _is_generic(pal) else ""
    if source_norm == TOUCHPOINT_SOURCE_DOMAIN:
        sub = str(subpalanca or "").strip()
        return sub if sub and not _is_generic(sub) else ""
    if source_norm == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2:
        helix_source_n2 = str(helix_source_service_n2 or "").strip()
        return helix_source_n2 if helix_source_n2 and not _is_generic(helix_source_n2) else ""
    return ""


def _empty_chain_df() -> pd.DataFrame:
    return pd.DataFrame(columns=CHAIN_COLUMNS)


def _executive_journey_match(
    *,
    nps_topic: object,
    touchpoint: object,
    palanca: object,
    subpalanca: object,
    incident_topic: object,
    incident_summary: object,
    comment_txt: object,
) -> Optional[dict[str, object]]:
    haystack = _norm(
        " ".join(
            [
                str(nps_topic or ""),
                str(touchpoint or ""),
                str(palanca or ""),
                str(subpalanca or ""),
                str(incident_topic or ""),
                str(incident_summary or ""),
                str(comment_txt or ""),
            ]
        )
    )
    best: Optional[dict[str, object]] = None
    best_score = 0
    for journey in EXECUTIVE_JOURNEY_CATALOG:
        score = sum(1 for kw in journey["keywords"] if str(kw) in haystack)
        if score > best_score:
            best = dict(journey)
            best_score = score
    return best if best_score > 0 else None


def _prepare_nps_chain_ref(nps_focus_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if nps_focus_df is None or nps_focus_df.empty:
        return pd.DataFrame(
            columns=[
                "nps_id",
                "nps_score",
                "comment_txt",
                "palanca",
                "subpalanca",
                "nps_date",
                "nps_topic",
            ]
        )

    df = nps_focus_df.copy()
    df["nps_id"] = df.get("ID", df.index).astype(str).str.strip()
    df["nps_score"] = pd.to_numeric(df.get("NPS"), errors="coerce")
    comment_series = df.get("Comment")
    if comment_series is None:
        comment_series = df.get("Comentario", pd.Series([""] * len(df), index=df.index))
    df["comment_txt"] = comment_series.astype(str).fillna("").str.strip()
    df["palanca"] = (
        df.get("Palanca", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    df["subpalanca"] = (
        df.get("Subpalanca", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    df["nps_date"] = pd.to_datetime(df.get("Fecha"), errors="coerce")
    df["nps_topic"] = (
        (
            df["palanca"].fillna("").astype(str).str.strip()
            + " > "
            + df["subpalanca"].fillna("").astype(str).str.strip()
        )
        .str.replace(r"^>\s*", "", regex=True)
        .str.replace(r"\s*>$", "", regex=True)
    )
    return df[
        ["nps_id", "nps_score", "comment_txt", "palanca", "subpalanca", "nps_date", "nps_topic"]
    ].copy()


def _prepare_helix_chain_ref(helix_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if helix_df is None or helix_df.empty:
        return pd.DataFrame(
            columns=[
                "incident_id",
                "incident_date",
                "incident_summary",
                "incident_url",
                "incident_topic",
                "helix_source_service_n2",
            ]
        )

    df = helix_df.copy()
    df["incident_id"] = (
        df.get("Incident Number", df.get("ID de la Incidencia", df.index)).astype(str).str.strip()
    )
    df["incident_date"] = pd.to_datetime(df.get("Fecha"), errors="coerce")
    df["incident_summary"] = build_incident_display_text(df).astype(str).fillna("").str.strip()
    url_candidates = [
        "Incident URL",
        "Incident Link",
        "Record URL",
        "Record Link",
        "Document URL",
        "Document Link",
        "URL",
        "Link",
        "Href",
    ]
    lower_map = {str(col).strip().lower(): str(col) for col in df.columns}
    picked_url_col = ""
    for candidate in url_candidates:
        hit = lower_map.get(candidate.strip().lower(), "")
        if hit:
            picked_url_col = hit
            break
    if not picked_url_col:
        hyperlink_cols = [
            str(col)
            for col in df.columns
            if str(col).strip().lower().endswith("__hyperlink")
            and pd.Series(df.get(col, pd.Series(dtype=object))).astype(str).str.strip().ne("").any()
        ]
        if hyperlink_cols:
            picked_url_col = hyperlink_cols[0]
    if not picked_url_col:
        fallback_cols = [
            str(col)
            for col in df.columns
            if any(token in str(col).strip().lower() for token in ("url", "link", "href"))
        ]
        if fallback_cols:
            picked_url_col = fallback_cols[0]
    incident_url = (
        df.get(picked_url_col, pd.Series([""] * len(df), index=df.index))
        if picked_url_col and picked_url_col in df.columns
        else pd.Series([""] * len(df), index=df.index)
    )
    incident_url = (
        incident_url.astype(str)
        .fillna("")
        .str.strip()
        .where(
            incident_url.astype(str)
            .fillna("")
            .str.strip()
            .str.match(r"^(https?|file)://", case=False, na=False),
            "",
        )
    )
    df["incident_url"] = incident_url
    tier1 = (
        df.get("Product Categorization Tier 1", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    tier2 = (
        df.get("Product Categorization Tier 2", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    tier3 = (
        df.get("Product Categorization Tier 3", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    source_service_n2 = (
        df.get("BBVA_SourceServiceN2", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    df["incident_topic"] = (tier1 + " > " + tier2 + " > " + tier3).str.replace(
        r"\s*>\s*>\s*", " > ", regex=True
    )
    df["incident_topic"] = (
        df["incident_topic"]
        .str.replace(r"^>\s*", "", regex=True)
        .str.replace(r"\s*>$", "", regex=True)
    )
    df["helix_source_service_n2"] = source_service_n2
    return df[
        [
            "incident_id",
            "incident_date",
            "incident_summary",
            "incident_url",
            "incident_topic",
            "helix_source_service_n2",
        ]
    ].copy()


def build_incident_attribution_chains(
    links_df: Optional[pd.DataFrame],
    nps_focus_df: Optional[pd.DataFrame],
    helix_df: Optional[pd.DataFrame],
    *,
    rationale_df: Optional[pd.DataFrame] = None,
    top_k: int = 3,
    max_incident_examples: int = 5,
    max_comment_examples: int = 2,
    min_links_per_topic: int = 1,
    touchpoint_source: str = TOUCHPOINT_SOURCE_DOMAIN,
) -> pd.DataFrame:
    """Return presentable evidence chains backed by exact Helix↔VoC links."""

    if links_df is None or links_df.empty:
        return _empty_chain_df()

    nps_ref = _prepare_nps_chain_ref(nps_focus_df)
    helix_ref = _prepare_helix_chain_ref(helix_df)
    if nps_ref.empty or helix_ref.empty:
        return _empty_chain_df()

    links = links_df.copy()
    links["incident_id"] = links.get("incident_id", "").astype(str).str.strip()
    links["nps_id"] = links.get("nps_id", "").astype(str).str.strip()
    links["similarity"] = pd.to_numeric(links.get("similarity"), errors="coerce").fillna(0.0)
    links["nps_topic"] = links.get("nps_topic", "").astype(str).fillna("").str.strip()
    if links.empty:
        return _empty_chain_df()

    enriched = (
        links.merge(nps_ref, on=["nps_id"], how="left", suffixes=("", "_nps"))
        .merge(helix_ref, on=["incident_id"], how="left", suffixes=("", "_helix"))
        .copy()
    )
    if enriched.empty:
        return _empty_chain_df()

    topic_from_nps = enriched.get(
        "nps_topic_nps", pd.Series([""] * len(enriched), index=enriched.index)
    )
    enriched["nps_topic"] = (
        enriched["nps_topic"]
        .where(enriched["nps_topic"].astype(str).str.strip() != "", topic_from_nps.astype(str))
        .astype(str)
        .str.strip()
    )
    enriched["comment_txt"] = enriched["comment_txt"].astype(str).fillna("").str.strip()
    enriched["incident_summary"] = enriched["incident_summary"].astype(str).fillna("").str.strip()
    enriched["palanca"] = enriched["palanca"].astype(str).fillna("").str.strip()
    enriched["subpalanca"] = enriched["subpalanca"].astype(str).fillna("").str.strip()
    enriched["touchpoint"] = [
        _touchpoint(
            pal,
            sub,
            inc_topic,
            helix_source_service_n2=helix_src_n2,
            source=touchpoint_source,
        )
        for pal, sub, inc_topic, helix_src_n2 in zip(
            enriched["palanca"],
            enriched["subpalanca"],
            enriched.get("incident_topic", pd.Series([""] * len(enriched), index=enriched.index)),
            enriched.get(
                "helix_source_service_n2",
                pd.Series([""] * len(enriched), index=enriched.index),
            ),
        )
    ]
    if (
        str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
        == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS
    ):
        journey_matches = [
            _executive_journey_match(
                nps_topic=topic,
                touchpoint=tp,
                palanca=pal,
                subpalanca=sub,
                incident_topic=inc_topic,
                incident_summary=inc_summary,
                comment_txt=comment,
            )
            for topic, tp, pal, sub, inc_topic, inc_summary, comment in zip(
                enriched["nps_topic"],
                enriched["touchpoint"],
                enriched["palanca"],
                enriched["subpalanca"],
                enriched.get(
                    "incident_topic", pd.Series([""] * len(enriched), index=enriched.index)
                ),
                enriched["incident_summary"],
                enriched["comment_txt"],
            )
        ]
        enriched["journey_id"] = [
            str(m.get("id", "")) if isinstance(m, dict) else "" for m in journey_matches
        ]
        enriched["journey_title"] = [
            str(m.get("title", "")) if isinstance(m, dict) else "" for m in journey_matches
        ]
        enriched["journey_touchpoint"] = [
            str(m.get("touchpoint", "")) if isinstance(m, dict) else "" for m in journey_matches
        ]
        enriched["journey_palanca"] = [
            str(m.get("palanca", "")) if isinstance(m, dict) else "" for m in journey_matches
        ]
        enriched["journey_subpalanca"] = [
            str(m.get("subpalanca", "")) if isinstance(m, dict) else "" for m in journey_matches
        ]
        enriched["journey_route"] = [
            str(m.get("route", "")) if isinstance(m, dict) else "" for m in journey_matches
        ]
        enriched["journey_expected_evidence"] = [
            str(m.get("expected_evidence", "")) if isinstance(m, dict) else ""
            for m in journey_matches
        ]
        enriched["journey_cx_readout"] = [
            str(m.get("cx_readout", "")) if isinstance(m, dict) else "" for m in journey_matches
        ]
        enriched["journey_impact_label"] = [
            str(m.get("impact_label", "")) if isinstance(m, dict) else "" for m in journey_matches
        ]
        enriched["journey_confidence_label"] = [
            str(m.get("confidence_label", "")) if isinstance(m, dict) else ""
            for m in journey_matches
        ]
        enriched["touchpoint"] = enriched["journey_touchpoint"].where(
            enriched["journey_touchpoint"].astype(str).str.strip().ne(""),
            enriched["touchpoint"],
        )
        enriched = enriched[enriched["journey_id"].astype(str).str.strip().ne("")].copy()

    enriched = enriched[
        enriched["incident_id"].astype(str).str.strip().ne("")
        & enriched["nps_id"].astype(str).str.strip().ne("")
        & enriched["comment_txt"].astype(str).str.strip().ne("")
        & (~enriched["comment_txt"].map(_is_generic))
        & (~enriched["nps_topic"].map(_is_generic))
        & (~enriched["touchpoint"].map(_is_generic))
    ].copy()
    if enriched.empty:
        return _empty_chain_df()

    rationale = rationale_df.copy() if rationale_df is not None else pd.DataFrame()
    if not rationale.empty and "nps_topic" in rationale.columns:
        rationale["nps_topic"] = rationale["nps_topic"].astype(str).str.strip()
        enriched = enriched.merge(
            rationale.drop_duplicates(["nps_topic"]),
            on="nps_topic",
            how="left",
            suffixes=("", "_r"),
        )

    rows: list[dict[str, object]] = []
    group_col = (
        "journey_id"
        if str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
        == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS
        else "nps_topic"
    )
    for topic, grp in enriched.groupby(group_col, dropna=False, observed=True):
        grp = grp.copy()
        linked_pairs = int(len(grp[["incident_id", "nps_id"]].drop_duplicates()))
        if linked_pairs < int(min_links_per_topic):
            continue

        is_executive_mode = (
            str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
            == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS
        )
        if is_executive_mode:
            topic_label = (
                str(grp["journey_title"].mode(dropna=True).iloc[0])
                if "journey_title" in grp.columns
                and not grp["journey_title"].mode(dropna=True).empty
                else str(topic)
            )
            palanca = (
                str(grp["journey_palanca"].mode(dropna=True).iloc[0])
                if "journey_palanca" in grp.columns
                and not grp["journey_palanca"].mode(dropna=True).empty
                else ""
            )
            subpalanca = (
                str(grp["journey_subpalanca"].mode(dropna=True).iloc[0])
                if "journey_subpalanca" in grp.columns
                and not grp["journey_subpalanca"].mode(dropna=True).empty
                else ""
            )
            touchpoint = (
                str(grp["journey_touchpoint"].mode(dropna=True).iloc[0])
                if "journey_touchpoint" in grp.columns
                and not grp["journey_touchpoint"].mode(dropna=True).empty
                else ""
            )
            journey_route = (
                str(grp["journey_route"].mode(dropna=True).iloc[0])
                if "journey_route" in grp.columns
                and not grp["journey_route"].mode(dropna=True).empty
                else ""
            )
            journey_expected_evidence = (
                str(grp["journey_expected_evidence"].mode(dropna=True).iloc[0])
                if "journey_expected_evidence" in grp.columns
                and not grp["journey_expected_evidence"].mode(dropna=True).empty
                else ""
            )
            journey_cx_readout = (
                str(grp["journey_cx_readout"].mode(dropna=True).iloc[0])
                if "journey_cx_readout" in grp.columns
                and not grp["journey_cx_readout"].mode(dropna=True).empty
                else ""
            )
            journey_impact_label = (
                str(grp["journey_impact_label"].mode(dropna=True).iloc[0])
                if "journey_impact_label" in grp.columns
                and not grp["journey_impact_label"].mode(dropna=True).empty
                else ""
            )
            journey_confidence_label = (
                str(grp["journey_confidence_label"].mode(dropna=True).iloc[0])
                if "journey_confidence_label" in grp.columns
                and not grp["journey_confidence_label"].mode(dropna=True).empty
                else ""
            )
        else:
            topic_label = str(topic)
            palanca = (
                str(grp["palanca"].mode(dropna=True).iloc[0])
                if not grp["palanca"].mode(dropna=True).empty
                else ""
            )
            subpalanca = (
                str(grp["subpalanca"].mode(dropna=True).iloc[0])
                if not grp["subpalanca"].mode(dropna=True).empty
                else ""
            )
            touchpoint = (
                str(grp["touchpoint"].mode(dropna=True).iloc[0])
                if not grp["touchpoint"].mode(dropna=True).empty
                else _touchpoint(
                    palanca,
                    subpalanca,
                    "",
                    helix_source_service_n2=(
                        str(
                            grp.get("helix_source_service_n2", pd.Series([""]))
                            .astype(str)
                            .mode(dropna=True)
                            .iloc[0]
                        )
                        if "helix_source_service_n2" in grp.columns
                        and not grp.get("helix_source_service_n2", pd.Series(dtype=str))
                        .mode(dropna=True)
                        .empty
                        else ""
                    ),
                    source=touchpoint_source,
                )
            )
            journey_route = ""
            journey_expected_evidence = ""
            journey_cx_readout = ""
            journey_impact_label = ""
            journey_confidence_label = ""

        inc_ranked = grp.sort_values(
            ["similarity", "incident_date"], ascending=[False, False]
        ).drop_duplicates(["incident_id"])
        inc_ranked = _limit_ranked_examples(inc_ranked, max_incident_examples)
        comment_ranked = grp.sort_values(
            ["nps_score", "similarity"], ascending=[True, False], na_position="last"
        ).drop_duplicates(["nps_id"])
        comment_ranked = _limit_ranked_examples(comment_ranked, max_comment_examples)
        incident_records = [
            {
                "incident_id": str(r.get("incident_id", "")).strip(),
                "summary": " ".join(str(r.get("incident_summary", "") or "").split()),
                "url": str(r.get("incident_url", "") or "").strip(),
            }
            for _, r in inc_ranked.iterrows()
            if str(r.get("incident_id", "")).strip()
        ]
        incident_examples = [
            str(rec.get("summary", "")).strip()
            for rec in incident_records
            if str(rec.get("summary", "")).strip()
        ]
        comment_examples = [
            f"NPS {int(_safe_float(r.get('nps_score', np.nan), default=0.0))}: {' '.join(str(r.get('comment_txt','') or '').split())}"
            for _, r in comment_ranked.iterrows()
            if str(r.get("comment_txt", "")).strip()
        ]
        if not incident_records or not comment_examples:
            continue

        detractor_probability = _safe_float(
            grp.get("focus_probability_with_incident", pd.Series([np.nan])).max(), default=np.nan
        )
        nps_delta_expected = _safe_float(
            grp.get("nps_delta_expected", pd.Series([np.nan])).mean(), default=np.nan
        )
        total_nps_impact = _safe_float(
            grp.get("total_nps_impact", pd.Series([0.0])).max(), default=0.0
        )
        confidence = _safe_float(grp.get("confidence", pd.Series([0.0])).max(), default=0.0)
        causal_score = _safe_float(grp.get("causal_score", pd.Series([0.0])).max(), default=0.0)
        delta_focus_rate_pp = _safe_float(
            grp.get("delta_focus_rate_pp", pd.Series([np.nan])).max(), default=np.nan
        )
        incident_rate_per_100_responses = _safe_float(
            grp.get("incident_rate_per_100_responses", pd.Series([np.nan])).max(),
            default=np.nan,
        )
        avg_nps = _safe_float(
            pd.to_numeric(grp.get("nps_score"), errors="coerce").mean(), default=np.nan
        )
        avg_similarity = _safe_float(grp["similarity"].mean(), default=0.0)
        linked_incidents = int(grp["incident_id"].astype(str).str.strip().nunique())
        linked_comments = int(grp["nps_id"].astype(str).str.strip().nunique())
        incidents_total = _safe_float(
            grp.get("incidents", pd.Series([linked_incidents])).max(),
            default=float(linked_incidents),
        )
        responses_total = _safe_float(
            grp.get("responses", pd.Series([np.nan])).max(), default=np.nan
        )
        action_lane = _mode_text(grp.get("action_lane", pd.Series(dtype=object)))
        owner_role = _mode_text(grp.get("owner_role", pd.Series(dtype=object)))
        eta_weeks = _safe_float(grp.get("eta_weeks", pd.Series([np.nan])).max(), default=np.nan)

        incident_ids = [
            str(rec.get("incident_id", "")).strip()
            for rec in incident_records
            if str(rec.get("incident_id", "")).strip()
        ]
        incident_sample_count = len(incident_examples)
        comment_sample_count = len(comment_examples)
        incident_sample_label = (
            ", ".join(incident_ids[:3])
            if incident_ids
            else f"{incident_sample_count} incidencias Helix"
        )
        if len(incident_ids) > 3:
            incident_sample_label = f"{incident_sample_label} y {len(incident_ids) - 3} más"
        comment_sample_label = " | ".join(comment_examples[:2])
        if is_executive_mode:
            story = (
                f"{journey_route}. {journey_cx_readout} "
                f"Evidencia esperada: {journey_expected_evidence}. "
                f"Impacto esperado en NPS: {journey_impact_label}. "
                f"Nivel de confianza causal esperado: {journey_confidence_label}. "
                f"En la ventana analizada se sostienen {incident_sample_count} incidencias Helix "
                f"({incident_sample_label}) y {comment_sample_count} comentarios VoC como {comment_sample_label}."
            )
        else:
            story = (
                f"{incident_sample_count} incidencias Helix "
                f"({incident_sample_label}) degradan el touchpoint {touchpoint}, "
                f"se traducen en fricción sobre {palanca} / {subpalanca} y aparecen en "
                f"{comment_sample_count} comentarios VoC como {comment_sample_label}."
            )
        rows.append(
            {
                "nps_topic": topic_label,
                "touchpoint": touchpoint,
                "palanca": palanca,
                "subpalanca": subpalanca,
                "linked_incidents": linked_incidents,
                "linked_comments": linked_comments,
                "linked_pairs": linked_pairs,
                "avg_similarity": avg_similarity,
                "avg_nps": avg_nps,
                "detractor_probability": detractor_probability,
                "nps_delta_expected": nps_delta_expected,
                "total_nps_impact": total_nps_impact,
                "nps_points_at_risk": _safe_float(
                    grp.get("nps_points_at_risk", pd.Series([0.0])).max(), default=0.0
                ),
                "nps_points_recoverable": _safe_float(
                    grp.get("nps_points_recoverable", pd.Series([0.0])).max(), default=0.0
                ),
                "priority": _safe_float(grp.get("priority", pd.Series([0.0])).max(), default=0.0),
                "confidence": confidence,
                "causal_score": causal_score,
                "incident_records": incident_records,
                "incident_examples": incident_examples,
                "comment_examples": comment_examples,
                "chain_story": story,
                "delta_focus_rate_pp": delta_focus_rate_pp,
                "incident_rate_per_100_responses": incident_rate_per_100_responses,
                "incidents": incidents_total,
                "responses": responses_total,
                "action_lane": action_lane,
                "owner_role": owner_role,
                "eta_weeks": eta_weeks,
                "presentation_mode": (
                    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS
                    if is_executive_mode
                    else str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN)
                ),
                "journey_route": journey_route,
                "journey_expected_evidence": journey_expected_evidence,
                "journey_cx_readout": journey_cx_readout,
                "journey_impact_label": journey_impact_label,
                "journey_confidence_label": journey_confidence_label,
            }
        )

    if not rows:
        return _empty_chain_df()

    out = pd.DataFrame(rows)
    out = out.sort_values(
        ["priority", "linked_pairs", "causal_score", "total_nps_impact"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    if int(top_k) > 0:
        out = out.head(int(top_k)).reset_index(drop=True)
    return out[CHAIN_COLUMNS].copy()
