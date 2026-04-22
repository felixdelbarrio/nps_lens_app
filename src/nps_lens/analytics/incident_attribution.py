from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer

from nps_lens.analytics.nps_helix_link import build_incident_display_text
from nps_lens.domain.causal_methods import (
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2,
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
    TOUCHPOINT_SOURCE_DOMAIN,
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    TOUCHPOINT_SOURCE_PALANCA,
)

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

EXECUTIVE_JOURNEY_EDITOR_COLUMNS = [
    "id",
    "title",
    "what_occurs",
    "expected_evidence",
    "impact_label",
    "touchpoint",
    "palanca",
    "subpalanca",
    "route",
    "cx_readout",
    "confidence_label",
    "keywords",
]

CHAIN_COLUMNS = [
    "nps_topic",
    "anchor_topic",
    "source_topics",
    "touchpoint",
    "palanca",
    "subpalanca",
    "helix_source_service_n2",
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
    "comment_records",
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


def summarize_attribution_chains(attribution_df: Optional[pd.DataFrame]) -> dict[str, int]:
    """Centralized totals for the chain rows shown in app and PPT."""
    empty = {
        "chains_total": 0,
        "topics_total": 0,
        "linked_incidents_total": 0,
        "linked_comments_total": 0,
        "linked_pairs_total": 0,
    }
    if attribution_df is None or attribution_df.empty:
        return empty

    df = attribution_df.copy()

    def _sum_metric(column: str, *, fallback_column: str = "") -> int:
        if column in df.columns:
            values = pd.to_numeric(df[column], errors="coerce")
            if values.notna().any():
                return int(values.fillna(0).clip(lower=0).sum())
        if fallback_column and fallback_column in df.columns:
            return int(
                df[fallback_column]
                .map(lambda value: len(value) if isinstance(value, list) else 0)
                .fillna(0)
                .sum()
            )
        return 0

    topics = (
        df.get("nps_topic", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .str.strip()
        .replace("", np.nan)
        .dropna()
    )
    return {
        "chains_total": int(len(df)),
        "topics_total": int(topics.nunique()),
        "linked_incidents_total": _sum_metric(
            "linked_incidents", fallback_column="incident_records"
        ),
        "linked_comments_total": _sum_metric("linked_comments", fallback_column="comment_records"),
        "linked_pairs_total": _sum_metric("linked_pairs"),
    }


BROKEN_JOURNEY_COLUMNS = [
    "journey_id",
    "journey_label",
    "touchpoint",
    "palanca",
    "subpalanca",
    "helix_source_service_n2",
    "journey_keywords",
    "journey_route",
    "journey_expected_evidence",
    "journey_cx_readout",
    "journey_impact_label",
    "journey_confidence_label",
    "linked_pairs",
    "linked_incidents",
    "linked_comments",
    "avg_similarity",
    "avg_nps",
    "semantic_cohesion",
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

_BROKEN_JOURNEY_STOPWORDS = {
    "app",
    "apps",
    "bbva",
    "cliente",
    "clientes",
    "comentario",
    "comentarios",
    "con",
    "del",
    "desde",
    "dia",
    "dias",
    "el",
    "en",
    "error",
    "esta",
    "este",
    "la",
    "las",
    "los",
    "muy",
    "no",
    "para",
    "pero",
    "portal",
    "por",
    "que",
    "se",
    "sin",
    "sobre",
    "una",
    "uno",
    "web",
    "ya",
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


def _dominant_non_generic(series: pd.Series) -> str:
    if series is None:
        return ""
    txt = pd.Series(series).astype(str).fillna("").str.strip()
    txt = txt[txt.ne("") & ~txt.map(_is_generic)]
    if txt.empty:
        return ""
    mode = txt.mode(dropna=True)
    if not mode.empty:
        return str(mode.iloc[0]).strip()
    return str(txt.iloc[0]).strip()


def _non_generic_nunique(series: pd.Series) -> int:
    txt = pd.Series(series).astype(str).fillna("").str.strip()
    txt = txt[txt.ne("") & ~txt.map(_is_generic)]
    return int(txt.nunique())


def _clip(value: object, max_len: int) -> str:
    txt = " ".join(str(value or "").split())
    if len(txt) <= int(max_len):
        return txt
    return txt[: max_len - 1].rstrip() + "…"


def _catalog_slug(value: object, *, max_len: int = 48) -> str:
    txt = " ".join(str(value or "").split()).strip().lower()
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"[^a-z0-9]+", "-", txt).strip("-")
    if not txt:
        return "default"
    return txt[:max_len].strip("-") or "default"


def _default_executive_journey_catalog() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in EXECUTIVE_JOURNEY_CATALOG:
        row = dict(item)
        row["keywords"] = ", ".join(str(word).strip() for word in item.get("keywords", ()))
        rows.append(row)
    return rows


def _catalog_keywords(value: object) -> tuple[str, ...]:
    tokens = (
        [str(token).strip().lower() for token in value if str(token).strip()]
        if isinstance(value, (list, tuple))
        else [
            str(token).strip().lower()
            for token in re.split(r"[,;\n]+", str(value or ""))
            if str(token).strip()
        ]
    )
    out: list[str] = []
    for token in tokens:
        norm_token = _norm(token).replace(">", " ").strip()
        if not norm_token or norm_token in out:
            continue
        out.append(norm_token)
    return tuple(out)


def _normalize_executive_journey_entry(
    row: dict[str, object],
    *,
    position: int,
) -> Optional[dict[str, object]]:
    base = {str(key): row.get(key) for key in EXECUTIVE_JOURNEY_EDITOR_COLUMNS}
    if not any(str(value or "").strip() for value in base.values()):
        return None

    title = " ".join(str(base.get("title") or "").split()).strip()
    touchpoint = " ".join(str(base.get("touchpoint") or "").split()).strip()
    palanca = " ".join(str(base.get("palanca") or "").split()).strip()
    subpalanca = " ".join(str(base.get("subpalanca") or "").split()).strip()
    keywords = _catalog_keywords(base.get("keywords"))
    if not keywords:
        keywords = _catalog_keywords(
            [part for part in [title, touchpoint, palanca, subpalanca] if part]
        )

    journey_id = " ".join(str(base.get("id") or "").split()).strip().lower()
    if not journey_id:
        journey_id = f"executive-{_catalog_slug(title or touchpoint or palanca or f'journey-{position + 1}')}"

    return {
        "id": journey_id,
        "title": title or f"Journey {position + 1}",
        "what_occurs": " ".join(str(base.get("what_occurs") or "").split()).strip(),
        "expected_evidence": " ".join(str(base.get("expected_evidence") or "").split()).strip(),
        "impact_label": " ".join(str(base.get("impact_label") or "").split()).strip() or "Medio",
        "touchpoint": touchpoint,
        "palanca": palanca,
        "subpalanca": subpalanca,
        "route": " ".join(str(base.get("route") or "").split()).strip(),
        "cx_readout": " ".join(str(base.get("cx_readout") or "").split()).strip(),
        "confidence_label": " ".join(str(base.get("confidence_label") or "").split()).strip()
        or "Medio",
        "keywords": keywords,
    }


def _dedupe_executive_journey_catalog(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    seen: dict[str, int] = {}
    out: list[dict[str, object]] = []
    for row in rows:
        key = str(row.get("id") or "").strip().lower()
        seen[key] = seen.get(key, 0) + 1
        if seen[key] > 1:
            row = dict(row)
            row["id"] = f"{key}-{seen[key]:02d}"
        out.append(row)
    return out


def executive_journey_catalog_path(
    knowledge_dir: Path,
    *,
    service_origin: str,
    service_origin_n1: str,
) -> Path:
    directory = Path(knowledge_dir) / "journey_catalogs"
    file_name = f"{_catalog_slug(service_origin)}__{_catalog_slug(service_origin_n1)}.json"
    return directory / file_name


def load_executive_journey_catalog(
    knowledge_dir: Path,
    *,
    service_origin: str,
    service_origin_n1: str,
) -> list[dict[str, object]]:
    path = executive_journey_catalog_path(
        knowledge_dir,
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
    )
    if not path.exists():
        return _default_executive_journey_catalog()

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _default_executive_journey_catalog()

    if not isinstance(payload, list):
        return _default_executive_journey_catalog()

    normalized: list[dict[str, object]] = []
    for idx, item in enumerate(payload):
        if not isinstance(item, dict):
            continue
        row = _normalize_executive_journey_entry(item, position=idx)
        if row is not None:
            normalized.append(row)
    normalized = _dedupe_executive_journey_catalog(normalized)
    return normalized or _default_executive_journey_catalog()


def save_executive_journey_catalog(
    knowledge_dir: Path,
    *,
    service_origin: str,
    service_origin_n1: str,
    rows: list[dict[str, object]],
) -> Path:
    normalized: list[dict[str, object]] = []
    for idx, item in enumerate(rows):
        row = _normalize_executive_journey_entry(item, position=idx)
        if row is not None:
            normalized.append(row)
    normalized = _dedupe_executive_journey_catalog(normalized)
    if not normalized:
        normalized = _default_executive_journey_catalog()

    path = executive_journey_catalog_path(
        knowledge_dir,
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = []
    for row in normalized:
        item = dict(row)
        item["keywords"] = list(_catalog_keywords(item.get("keywords")))
        payload.append(item)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def executive_journey_catalog_df(catalog: list[dict[str, object]]) -> pd.DataFrame:
    rows = []
    for item in catalog:
        row = {str(key): item.get(key, "") for key in EXECUTIVE_JOURNEY_EDITOR_COLUMNS}
        row["keywords"] = ", ".join(_catalog_keywords(row.get("keywords")))
        rows.append(row)
    return pd.DataFrame(rows, columns=EXECUTIVE_JOURNEY_EDITOR_COLUMNS)


def _format_nps_score(value: object) -> str:
    score = _safe_float(value, default=np.nan)
    if not np.isfinite(score):
        return ""
    if float(score).is_integer():
        return str(int(score))
    return f"{score:.1f}"


def _format_nps_date(value: object) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if ts is None or pd.isna(ts):
        return ""
    return ts.strftime("%d-%m-%Y")


def _slug(value: object, *, max_len: int = 36) -> str:
    txt = _norm(value).replace(">", " ").replace("/", " ").replace(" ", "-")
    txt = re.sub(r"-+", "-", txt).strip("-")
    if not txt:
        return "unknown"
    return txt[:max_len].strip("-") or "unknown"


def _broken_journey_cluster_count(n_rows: int, candidate_axes: int) -> int:
    if n_rows <= 1:
        return 1
    if n_rows <= 4:
        return min(2, n_rows)
    heuristic = int(round(np.sqrt(float(n_rows))))
    bounded_axes = max(1, min(int(candidate_axes), heuristic if heuristic > 0 else 1))
    return max(2, min(8, min(n_rows, bounded_axes)))


def _broken_journey_title_case(value: object) -> str:
    tokens = [part for part in re.split(r"[\s_/]+", str(value or "").strip()) if part]
    return " ".join(tok.capitalize() for tok in tokens[:3]).strip()


def _broken_journey_keywords(vectorizer: TfidfVectorizer, matrix, mask: pd.Series) -> list[str]:
    feature_names = vectorizer.get_feature_names_out()
    if len(feature_names) == 0:
        return []
    weights = np.asarray(matrix[mask.to_numpy()].mean(axis=0)).ravel()
    if weights.size == 0:
        return []
    order = np.argsort(weights)[::-1]
    out: list[str] = []
    for idx in order.tolist():
        token = str(feature_names[idx]).strip().lower()
        if (
            not token
            or token in _BROKEN_JOURNEY_STOPWORDS
            or len(token) < 3
            or token.isdigit()
            or token.replace(" ", "") in _BROKEN_JOURNEY_STOPWORDS
        ):
            continue
        out.append(token)
        if len(out) >= 5:
            break
    return out


def _broken_journey_impact_label(linked_pairs: int, avg_nps: float) -> str:
    if linked_pairs >= 10 or avg_nps <= 2.0:
        return "Muy alto"
    if linked_pairs >= 6 or avg_nps <= 4.0:
        return "Alto"
    if linked_pairs >= 3:
        return "Medio"
    return "Bajo"


def _broken_journey_confidence_label(semantic_score: float, avg_similarity: float) -> str:
    if semantic_score >= 0.72 and avg_similarity >= 0.85:
        return "Alto"
    if semantic_score >= 0.55 and avg_similarity >= 0.75:
        return "Medio"
    return "Bajo"


def _derive_nps_group(group_value: object, score_value: object) -> str:
    group_txt = str(group_value or "").strip()
    if group_txt:
        return group_txt
    score = _safe_float(score_value, default=np.nan)
    if not np.isfinite(score):
        return ""
    if score <= 6:
        return "DETRACTOR"
    if score >= 9:
        return "PROMOTER"
    return "PASSIVE"


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


def _empty_broken_journey_df() -> pd.DataFrame:
    return pd.DataFrame(columns=BROKEN_JOURNEY_COLUMNS)


def _executive_journey_match(
    *,
    nps_topic: object,
    touchpoint: object,
    palanca: object,
    subpalanca: object,
    incident_topic: object,
    incident_summary: object,
    comment_txt: object,
    catalog: Optional[list[dict[str, object]]] = None,
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
    active_catalog = catalog or list(_default_executive_journey_catalog())
    for journey in active_catalog:
        keywords = _catalog_keywords(journey.get("keywords"))
        score = sum(1 for kw in keywords if str(kw) in haystack)
        if score > best_score:
            best = dict(journey)
            best["keywords"] = keywords
            best_score = score
    return best if best_score > 0 else None


def _prepare_nps_chain_ref(nps_focus_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if nps_focus_df is None or nps_focus_df.empty:
        return pd.DataFrame(
            columns=[
                "nps_id",
                "nps_score",
                "nps_group",
                "comment_txt",
                "comment_norm",
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
    comment_norm_series = df.get("_text_norm")
    if comment_norm_series is None:
        comment_norm_series = df["comment_txt"]
    df["comment_norm"] = comment_norm_series.astype(str).fillna("").str.strip()
    df["nps_group"] = (
        df.get("NPS Group", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
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
        [
            "nps_id",
            "nps_score",
            "nps_group",
            "comment_txt",
            "comment_norm",
            "palanca",
            "subpalanca",
            "nps_date",
            "nps_topic",
        ]
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


def _prepare_enriched_links(
    links_df: Optional[pd.DataFrame],
    nps_focus_df: Optional[pd.DataFrame],
    helix_df: Optional[pd.DataFrame],
    *,
    touchpoint_source: str = TOUCHPOINT_SOURCE_DOMAIN,
) -> pd.DataFrame:
    if links_df is None or links_df.empty:
        return pd.DataFrame()

    nps_ref = _prepare_nps_chain_ref(nps_focus_df)
    helix_ref = _prepare_helix_chain_ref(helix_df)
    if nps_ref.empty or helix_ref.empty:
        return pd.DataFrame()

    links = links_df.copy()
    links["incident_id"] = links.get("incident_id", "").astype(str).str.strip()
    links["nps_id"] = links.get("nps_id", "").astype(str).str.strip()
    links["similarity"] = pd.to_numeric(links.get("similarity"), errors="coerce").fillna(0.0)
    links["nps_topic"] = links.get("nps_topic", "").astype(str).fillna("").str.strip()
    links = links[
        links["incident_id"].astype(str).str.strip().ne("")
        & links["nps_id"].astype(str).str.strip().ne("")
    ].copy()
    if links.empty:
        return pd.DataFrame()

    enriched = (
        links.merge(nps_ref, on=["nps_id"], how="left", suffixes=("", "_nps"))
        .merge(helix_ref, on=["incident_id"], how="left", suffixes=("", "_helix"))
        .copy()
    )
    if enriched.empty:
        return pd.DataFrame()

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
    enriched["comment_norm"] = enriched["comment_norm"].astype(str).fillna("").str.strip()
    enriched["incident_summary"] = enriched["incident_summary"].astype(str).fillna("").str.strip()
    enriched["incident_topic"] = (
        enriched.get("incident_topic", pd.Series([""] * len(enriched), index=enriched.index))
        .astype(str)
        .fillna("")
        .str.strip()
    )
    enriched["palanca"] = enriched["palanca"].astype(str).fillna("").str.strip()
    enriched["subpalanca"] = enriched["subpalanca"].astype(str).fillna("").str.strip()
    enriched["helix_source_service_n2"] = (
        enriched.get(
            "helix_source_service_n2",
            pd.Series([""] * len(enriched), index=enriched.index),
        )
        .astype(str)
        .fillna("")
        .str.strip()
    )
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
            enriched["incident_topic"],
            enriched["helix_source_service_n2"],
        )
    ]
    enriched = enriched.sort_values(
        ["similarity", "incident_date", "nps_date"],
        ascending=[False, False, False],
        na_position="last",
    ).drop_duplicates(["incident_id", "nps_id"])
    return enriched.reset_index(drop=True)


def _grouping_key_for_source(touchpoint_source: str) -> str:
    source = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
    if source == TOUCHPOINT_SOURCE_PALANCA:
        return "palanca"
    if source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2:
        return "helix_source_service_n2"
    return "subpalanca"


def _group_label_for_source(grp: pd.DataFrame, *, touchpoint_source: str) -> str:
    grouping_key = _grouping_key_for_source(touchpoint_source)
    if grouping_key in grp.columns and not grp[grouping_key].mode(dropna=True).empty:
        return str(grp[grouping_key].mode(dropna=True).iloc[0]).strip()
    return ""


def _touchpoint_for_source(grp: pd.DataFrame, *, touchpoint_source: str) -> str:
    source = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
    if (
        source == TOUCHPOINT_SOURCE_PALANCA
        and "subpalanca" in grp.columns
        and not grp["subpalanca"].mode(dropna=True).empty
    ):
        return str(grp["subpalanca"].mode(dropna=True).iloc[0]).strip()
    if (
        source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2
        and "helix_source_service_n2" in grp.columns
        and not grp["helix_source_service_n2"].mode(dropna=True).empty
    ):
        return str(grp["helix_source_service_n2"].mode(dropna=True).iloc[0]).strip()
    if "touchpoint" in grp.columns and not grp["touchpoint"].mode(dropna=True).empty:
        return str(grp["touchpoint"].mode(dropna=True).iloc[0]).strip()
    return ""


def _source_topics_for_group(grp: pd.DataFrame) -> list[str]:
    topic_column = "source_nps_topic" if "source_nps_topic" in grp.columns else "nps_topic"
    if topic_column not in grp.columns:
        return []
    topic_counts = (
        grp.assign(
            __source_topic=grp[topic_column].astype(str).str.strip(),
            __similarity=pd.to_numeric(grp.get("similarity"), errors="coerce").fillna(0.0),
        )
        .loc[lambda frame: frame["__source_topic"].ne("")]
        .groupby("__source_topic", dropna=False, observed=True)
        .agg(linked_pairs=("incident_id", "count"), avg_similarity=("__similarity", "mean"))
        .reset_index()
        .sort_values(
            ["linked_pairs", "avg_similarity", "__source_topic"],
            ascending=[False, False, True],
        )
    )
    return topic_counts["__source_topic"].astype(str).tolist()


def _chain_story_for_source(
    *,
    touchpoint_source: str,
    topic_label: str,
    touchpoint: str,
    palanca: str,
    subpalanca: str,
    helix_source_service_n2: str,
    journey_route: str,
    journey_cx_readout: str,
    journey_expected_evidence: str,
    journey_impact_label: str,
    journey_confidence_label: str,
    incident_sample_count: int,
    incident_sample_label: str,
    comment_sample_count: int,
    comment_sample_label: str,
    anchor_topic: str,
) -> str:
    source = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
    if source == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS:
        return (
            f"{journey_route}. {journey_cx_readout} "
            f"Evidencia esperada: {journey_expected_evidence}. "
            f"Impacto esperado en NPS: {journey_impact_label}. "
            f"Nivel de confianza causal esperado: {journey_confidence_label}. "
            f"En la ventana analizada se sostienen {incident_sample_count} incidencias Helix "
            f"({incident_sample_label}) y {comment_sample_count} comentarios VoC como {comment_sample_label}."
        )
    if source == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
        return (
            f"{journey_route}. {journey_cx_readout} "
            f"Keywords del cluster: {journey_expected_evidence}. "
            f"En la ventana analizada se sostienen {incident_sample_count} incidencias Helix "
            f"({incident_sample_label}) y {comment_sample_count} comentarios VoC como {comment_sample_label}."
        )
    if source == TOUCHPOINT_SOURCE_PALANCA:
        return (
            f"{incident_sample_count} incidencias Helix ({incident_sample_label}) afectan el "
            f"touchpoint {touchpoint or 'detectado'} y se concentran en la palanca {topic_label}. "
            f"El tópico NPS ancla es {anchor_topic or 'n/d'} y aparecen en "
            f"{comment_sample_count} comentarios VoC como {comment_sample_label}."
        )
    if source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2:
        return (
            f"{incident_sample_count} incidencias Helix ({incident_sample_label}) convergen en el "
            f"Source Service N2 {helix_source_service_n2 or topic_label} y aparecen en "
            f"{comment_sample_count} comentarios VoC como {comment_sample_label}. "
            f"El tópico NPS ancla es {anchor_topic or 'n/d'}."
        )
    return (
        f"{incident_sample_count} incidencias Helix ({incident_sample_label}) degradan la "
        f"subpalanca {topic_label or subpalanca}, se manifiestan en el touchpoint "
        f"{touchpoint or topic_label} y aparecen en {comment_sample_count} comentarios VoC como "
        f"{comment_sample_label}."
    )


def build_broken_journey_catalog(
    links_df: Optional[pd.DataFrame],
    nps_focus_df: Optional[pd.DataFrame],
    helix_df: Optional[pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Infer broken journeys from validated Helix↔VoC links with lightweight semantics."""

    enriched = _prepare_enriched_links(
        links_df,
        nps_focus_df,
        helix_df,
        touchpoint_source=TOUCHPOINT_SOURCE_DOMAIN,
    )
    if enriched.empty:
        return _empty_broken_journey_df(), pd.DataFrame()

    enriched = enriched[
        enriched["comment_txt"].astype(str).str.strip().ne("")
        & (~enriched["comment_txt"].map(_is_generic))
        & (~enriched["nps_topic"].map(_is_generic))
    ].copy()
    if enriched.empty:
        return _empty_broken_journey_df(), pd.DataFrame()

    enriched["source_nps_topic"] = enriched["nps_topic"].astype(str).str.strip()
    enriched["semantic_text"] = (
        enriched["palanca"].fillna("")
        + " "
        + enriched["subpalanca"].fillna("")
        + " "
        + enriched["helix_source_service_n2"].fillna("")
        + " "
        + enriched["incident_topic"].fillna("")
        + " "
        + enriched["source_nps_topic"].fillna("")
        + " "
        + enriched["incident_summary"].fillna("")
        + " "
        + enriched["comment_norm"].where(
            enriched["comment_norm"].astype(str).str.strip().ne(""),
            enriched["comment_txt"],
        )
    ).astype(str)

    texts = enriched["semantic_text"].astype(str).str.strip().tolist()
    if not texts:
        return _empty_broken_journey_df(), pd.DataFrame()

    vectorizer = TfidfVectorizer(
        strip_accents="unicode",
        lowercase=True,
        ngram_range=(1, 2),
        max_features=384,
    )
    try:
        matrix = vectorizer.fit_transform(texts)
    except ValueError:
        return _empty_broken_journey_df(), pd.DataFrame()
    candidate_axes = max(
        1,
        _non_generic_nunique(enriched["subpalanca"]),
        _non_generic_nunique(enriched["helix_source_service_n2"]),
        _non_generic_nunique(enriched["palanca"]),
    )
    n_clusters = _broken_journey_cluster_count(len(enriched), int(candidate_axes))

    if len(enriched) == 1 or n_clusters <= 1 or matrix.shape[0] <= 1:
        labels = np.zeros(len(enriched), dtype=int)
        semantic_score = np.ones(len(enriched), dtype=float)
    else:
        max_components = min(24, max(1, min(matrix.shape[0] - 1, matrix.shape[1] - 1)))
        dense = (
            TruncatedSVD(n_components=max_components, random_state=7).fit_transform(matrix)
            if max_components >= 2
            else matrix.toarray()
        )
        model = MiniBatchKMeans(
            n_clusters=int(min(n_clusters, len(enriched))),
            random_state=7,
            n_init=10,
            batch_size=min(64, len(enriched)),
        )
        labels = model.fit_predict(dense)
        row_norm = np.linalg.norm(dense, axis=1)
        centers = model.cluster_centers_[labels]
        center_norm = np.linalg.norm(centers, axis=1)
        denom = np.where((row_norm * center_norm) > 0, row_norm * center_norm, 1.0)
        semantic_score = np.clip((dense * centers).sum(axis=1) / denom, 0.0, 1.0)

    enriched["journey_cluster"] = labels.astype(int)
    enriched["semantic_score"] = semantic_score.astype(float)

    cluster_rows: list[dict[str, object]] = []
    for cluster_id, grp in enriched.groupby("journey_cluster", dropna=False, observed=True):
        grp = grp.copy()
        palanca = _dominant_non_generic(grp["palanca"])
        subpalanca = _dominant_non_generic(grp["subpalanca"])
        helix_source_n2 = _dominant_non_generic(grp["helix_source_service_n2"])
        keywords = _broken_journey_keywords(
            vectorizer, matrix, enriched["journey_cluster"] == cluster_id
        )
        touchpoint = subpalanca or helix_source_n2
        if not touchpoint:
            touchpoint = " / ".join(
                _broken_journey_title_case(word) for word in keywords[:2]
            ).strip()
        if not touchpoint:
            touchpoint = palanca or str(grp["source_nps_topic"].mode(dropna=True).iloc[0]).strip()
        label = touchpoint
        if palanca and touchpoint and _norm(palanca) != _norm(touchpoint):
            label = f"{palanca} / {touchpoint}"
        label = label.strip() or "Journey roto sin etiqueta"
        linked_pairs = int(len(grp[["incident_id", "nps_id"]].drop_duplicates()))
        linked_incidents = int(grp["incident_id"].astype(str).str.strip().nunique())
        linked_comments = int(grp["nps_id"].astype(str).str.strip().nunique())
        avg_similarity = _safe_float(grp["similarity"].mean(), default=0.0)
        avg_nps = _safe_float(
            pd.to_numeric(grp["nps_score"], errors="coerce").mean(), default=np.nan
        )
        semantic_cohesion = _safe_float(grp["semantic_score"].mean(), default=0.0)
        impact_label = _broken_journey_impact_label(linked_pairs, avg_nps)
        confidence_label = _broken_journey_confidence_label(semantic_cohesion, avg_similarity)
        keyword_text = ", ".join(_broken_journey_title_case(word) for word in keywords[:4])
        cluster_rows.append(
            {
                "journey_cluster": int(cluster_id),
                "journey_label": label,
                "touchpoint": touchpoint,
                "palanca": palanca,
                "subpalanca": subpalanca,
                "helix_source_service_n2": helix_source_n2,
                "journey_keywords": keyword_text,
                "journey_route": (
                    f"Incidencia -> {touchpoint or 'touchpoint detectado'} -> "
                    f"{palanca or 'palanca'} / {subpalanca or 'señal semántica'} -> comentario VoC -> NPS"
                ),
                "journey_expected_evidence": (
                    f"Keywords semánticas: {keyword_text or 'n/d'}. "
                    f"Helix Source Service N2 dominante: {helix_source_n2 or 'n/d'}."
                ),
                "journey_cx_readout": (
                    f"{linked_pairs} links Helix↔VoC convergen en este journey roto; "
                    f"predominan {palanca or 'sin palanca'} / {subpalanca or 'sin subpalanca'} "
                    f"y el NPS medio asociado es {avg_nps:.1f}."
                ),
                "journey_impact_label": impact_label,
                "journey_confidence_label": confidence_label,
                "linked_pairs": linked_pairs,
                "linked_incidents": linked_incidents,
                "linked_comments": linked_comments,
                "avg_similarity": avg_similarity,
                "avg_nps": avg_nps,
                "semantic_cohesion": semantic_cohesion,
            }
        )

    catalog = pd.DataFrame(cluster_rows)
    if catalog.empty:
        return _empty_broken_journey_df(), pd.DataFrame()

    catalog = catalog.sort_values(
        ["linked_pairs", "semantic_cohesion", "avg_similarity", "journey_label"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    seen: dict[str, int] = {}
    journey_ids: list[str] = []
    for idx, row in catalog.iterrows():
        base = _slug(row.get("journey_label"))
        seen[base] = seen.get(base, 0) + 1
        suffix = f"-{seen[base]:02d}" if seen[base] > 1 else ""
        journey_ids.append(f"broken-journey-{idx + 1:02d}-{base}{suffix}")
    catalog["journey_id"] = journey_ids
    catalog = catalog[
        [
            "journey_cluster",
            "journey_id",
            "journey_label",
            "touchpoint",
            "palanca",
            "subpalanca",
            "helix_source_service_n2",
            "journey_keywords",
            "journey_route",
            "journey_expected_evidence",
            "journey_cx_readout",
            "journey_impact_label",
            "journey_confidence_label",
            "linked_pairs",
            "linked_incidents",
            "linked_comments",
            "avg_similarity",
            "avg_nps",
            "semantic_cohesion",
        ]
    ].copy()

    journey_links = enriched.merge(
        catalog, on="journey_cluster", how="left", suffixes=("", "_journey")
    )
    return catalog[BROKEN_JOURNEY_COLUMNS].copy(), journey_links


def build_broken_journey_topic_map(journey_links_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if journey_links_df is None or journey_links_df.empty:
        return pd.DataFrame(
            columns=[
                "source_nps_topic",
                "journey_id",
                "journey_label",
                "touchpoint",
                "palanca",
                "subpalanca",
            ]
        )

    df = journey_links_df.copy()
    df["source_nps_topic"] = df.get("source_nps_topic", "").astype(str).str.strip()
    df["journey_label"] = df.get("journey_label", "").astype(str).str.strip()
    df = df[
        df["source_nps_topic"].ne("")
        & df["journey_label"].ne("")
        & df.get("journey_id", pd.Series([""] * len(df), index=df.index))
        .astype(str)
        .str.strip()
        .ne("")
    ].copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "source_nps_topic",
                "journey_id",
                "journey_label",
                "touchpoint",
                "palanca",
                "subpalanca",
            ]
        )

    grouped = (
        df.groupby(
            [
                "source_nps_topic",
                "journey_id",
                "journey_label",
                "touchpoint",
                "palanca",
                "subpalanca",
            ],
            dropna=False,
            observed=True,
        )
        .agg(
            linked_pairs=("incident_id", "count"),
            avg_similarity=("similarity", "mean"),
        )
        .reset_index()
        .sort_values(
            ["source_nps_topic", "linked_pairs", "avg_similarity", "journey_label"],
            ascending=[True, False, False, True],
        )
        .drop_duplicates(["source_nps_topic"])
    )
    return grouped[
        ["source_nps_topic", "journey_id", "journey_label", "touchpoint", "palanca", "subpalanca"]
    ].reset_index(drop=True)


def build_causal_topic_map(
    links_df: Optional[pd.DataFrame],
    nps_focus_df: Optional[pd.DataFrame],
    helix_df: Optional[pd.DataFrame],
    *,
    touchpoint_source: str = TOUCHPOINT_SOURCE_DOMAIN,
    journey_links_df: Optional[pd.DataFrame] = None,
    executive_journey_catalog: Optional[list[dict[str, object]]] = None,
) -> pd.DataFrame:
    source = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
    if source == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
        local_links = journey_links_df.copy() if journey_links_df is not None else None
        if local_links is None or local_links.empty:
            _, local_links = build_broken_journey_catalog(links_df, nps_focus_df, helix_df)
        return build_broken_journey_topic_map(local_links).rename(
            columns={
                "journey_id": "entity_id",
                "journey_label": "entity_label",
            }
        )

    enriched = _prepare_enriched_links(
        links_df,
        nps_focus_df,
        helix_df,
        touchpoint_source=touchpoint_source,
    )
    if enriched.empty:
        return pd.DataFrame(
            columns=[
                "source_nps_topic",
                "entity_id",
                "entity_label",
                "touchpoint",
                "palanca",
                "subpalanca",
                "helix_source_service_n2",
            ]
        )

    enriched["source_nps_topic"] = enriched["nps_topic"].astype(str).str.strip()
    enriched["palanca"] = enriched["palanca"].astype(str).str.strip()
    enriched["subpalanca"] = enriched["subpalanca"].astype(str).str.strip()
    enriched["helix_source_service_n2"] = (
        enriched.get(
            "helix_source_service_n2", pd.Series([""] * len(enriched), index=enriched.index)
        )
        .astype(str)
        .str.strip()
    )
    if source == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS:
        active_catalog = executive_journey_catalog or _default_executive_journey_catalog()
        matches = [
            _executive_journey_match(
                nps_topic=topic,
                touchpoint=tp,
                palanca=pal,
                subpalanca=sub,
                incident_topic=inc_topic,
                incident_summary=inc_summary,
                comment_txt=comment,
                catalog=active_catalog,
            )
            for topic, tp, pal, sub, inc_topic, inc_summary, comment in zip(
                enriched["source_nps_topic"],
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
        enriched["entity_id"] = [
            str(match.get("id", "")) if isinstance(match, dict) else "" for match in matches
        ]
        enriched["entity_label"] = [
            str(match.get("title", "")) if isinstance(match, dict) else "" for match in matches
        ]
        enriched["touchpoint"] = [
            str(match.get("touchpoint", "")) if isinstance(match, dict) else "" for match in matches
        ]
        enriched["palanca"] = [
            str(match.get("palanca", "")) if isinstance(match, dict) else "" for match in matches
        ]
        enriched["subpalanca"] = [
            str(match.get("subpalanca", "")) if isinstance(match, dict) else "" for match in matches
        ]
    else:
        entity_series = (
            enriched["palanca"]
            if source == TOUCHPOINT_SOURCE_PALANCA
            else (
                enriched["helix_source_service_n2"]
                if source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2
                else enriched["subpalanca"]
            )
        )
        enriched["entity_id"] = entity_series.astype(str).str.strip()
        enriched["entity_label"] = entity_series.astype(str).str.strip()
        if source == TOUCHPOINT_SOURCE_PALANCA:
            enriched["touchpoint"] = enriched["subpalanca"].astype(str).str.strip()
        if source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2:
            enriched["touchpoint"] = enriched["helix_source_service_n2"].astype(str).str.strip()

    mapping = (
        enriched.assign(
            similarity=pd.to_numeric(enriched.get("similarity"), errors="coerce").fillna(0.0)
        )
        .loc[
            lambda frame: frame["source_nps_topic"].ne("")
            & frame["entity_id"].astype(str).str.strip().ne("")
            & frame["entity_label"].astype(str).str.strip().ne("")
        ]
        .groupby(
            [
                "source_nps_topic",
                "entity_id",
                "entity_label",
                "touchpoint",
                "palanca",
                "subpalanca",
                "helix_source_service_n2",
            ],
            dropna=False,
            observed=True,
        )
        .agg(linked_pairs=("incident_id", "count"), avg_similarity=("similarity", "mean"))
        .reset_index()
        .sort_values(
            ["source_nps_topic", "linked_pairs", "avg_similarity", "entity_label"],
            ascending=[True, False, False, True],
        )
        .drop_duplicates(["source_nps_topic"])
    )
    return mapping[
        [
            "source_nps_topic",
            "entity_id",
            "entity_label",
            "touchpoint",
            "palanca",
            "subpalanca",
            "helix_source_service_n2",
        ]
    ].reset_index(drop=True)


def remap_links_to_causal_entities(
    links_df: Optional[pd.DataFrame], topic_map_df: Optional[pd.DataFrame]
) -> pd.DataFrame:
    if links_df is None or links_df.empty or topic_map_df is None or topic_map_df.empty:
        return pd.DataFrame(columns=list((links_df.columns if links_df is not None else [])))

    mapping = topic_map_df.copy()
    mapping["source_nps_topic"] = mapping["source_nps_topic"].astype(str).str.strip()
    out = links_df.copy()
    out["nps_topic"] = out.get("nps_topic", "").astype(str).str.strip()
    out = out.merge(mapping, left_on="nps_topic", right_on="source_nps_topic", how="inner")
    if out.empty:
        return out
    out["source_nps_topic"] = out["nps_topic"].astype(str).str.strip()
    out["nps_topic"] = out["entity_label"].astype(str).str.strip()
    return out


def remap_topic_timeseries_to_causal_entities(
    by_topic_df: Optional[pd.DataFrame], topic_map_df: Optional[pd.DataFrame]
) -> pd.DataFrame:
    if by_topic_df is None or by_topic_df.empty:
        return pd.DataFrame(columns=list((by_topic_df.columns if by_topic_df is not None else [])))
    if topic_map_df is None or topic_map_df.empty:
        return pd.DataFrame(columns=list(by_topic_df.columns))

    df = by_topic_df.copy()
    mapping = topic_map_df.copy()
    df["nps_topic"] = df.get("nps_topic", "").astype(str).str.strip()
    mapping["source_nps_topic"] = mapping["source_nps_topic"].astype(str).str.strip()
    merged = df.merge(mapping, left_on="nps_topic", right_on="source_nps_topic", how="inner")
    if merged.empty:
        return pd.DataFrame(columns=list(by_topic_df.columns))

    merged["entity_label"] = merged["entity_label"].astype(str).str.strip()
    merged["responses"] = pd.to_numeric(merged.get("responses"), errors="coerce").fillna(0.0)
    merged["focus_count"] = pd.to_numeric(merged.get("focus_count"), errors="coerce").fillna(0.0)
    merged["incidents"] = pd.to_numeric(merged.get("incidents"), errors="coerce").fillna(0.0)
    merged["nps_mean"] = pd.to_numeric(merged.get("nps_mean"), errors="coerce")
    merged["nps_weighted_sum"] = merged["nps_mean"].fillna(0.0) * merged["responses"]

    time_col = "week" if "week" in merged.columns else ("date" if "date" in merged.columns else "")
    if not time_col:
        return pd.DataFrame(columns=list(by_topic_df.columns))

    grouped = (
        merged.groupby([time_col, "entity_label"], dropna=False, observed=True)
        .agg(
            responses=("responses", "sum"),
            focus_count=("focus_count", "sum"),
            incidents=("incidents", "sum"),
            nps_weighted_sum=("nps_weighted_sum", "sum"),
        )
        .reset_index()
    )
    grouped["nps_mean"] = grouped["nps_weighted_sum"] / grouped["responses"].replace({0: np.nan})
    grouped["focus_rate"] = grouped["focus_count"] / grouped["responses"].replace({0: np.nan})
    grouped["nps_topic"] = grouped["entity_label"].astype(str)
    keep_cols = [
        time_col,
        "nps_topic",
        "responses",
        "focus_count",
        "nps_mean",
        "focus_rate",
        "incidents",
    ]
    return grouped[keep_cols].sort_values([time_col, "nps_topic"]).reset_index(drop=True)


def remap_links_to_journeys(
    links_df: Optional[pd.DataFrame], journey_links_df: Optional[pd.DataFrame]
) -> pd.DataFrame:
    if links_df is None or links_df.empty or journey_links_df is None or journey_links_df.empty:
        return pd.DataFrame(columns=list((links_df.columns if links_df is not None else [])))

    journey_map = journey_links_df[
        [
            "incident_id",
            "nps_id",
            "journey_id",
            "journey_label",
            "touchpoint",
            "palanca",
            "subpalanca",
        ]
    ].drop_duplicates(["incident_id", "nps_id"])
    out = links_df.copy().merge(journey_map, on=["incident_id", "nps_id"], how="inner")
    if out.empty:
        return out
    out["source_nps_topic"] = out.get("nps_topic", "").astype(str).str.strip()
    out["nps_topic"] = out["journey_label"].astype(str).str.strip()
    return out


def remap_topic_timeseries_to_journeys(
    by_topic_df: Optional[pd.DataFrame], journey_topic_map: Optional[pd.DataFrame]
) -> pd.DataFrame:
    if by_topic_df is None or by_topic_df.empty:
        return pd.DataFrame(columns=list((by_topic_df.columns if by_topic_df is not None else [])))
    if journey_topic_map is None or journey_topic_map.empty:
        return pd.DataFrame(columns=list(by_topic_df.columns))

    df = by_topic_df.copy()
    mapping = journey_topic_map.copy()
    df["nps_topic"] = df.get("nps_topic", "").astype(str).str.strip()
    mapping["source_nps_topic"] = mapping["source_nps_topic"].astype(str).str.strip()
    merged = df.merge(mapping, left_on="nps_topic", right_on="source_nps_topic", how="inner")
    if merged.empty:
        return pd.DataFrame(columns=list(by_topic_df.columns))

    merged["journey_label"] = merged["journey_label"].astype(str).str.strip()
    merged["responses"] = pd.to_numeric(merged.get("responses"), errors="coerce").fillna(0.0)
    merged["focus_count"] = pd.to_numeric(merged.get("focus_count"), errors="coerce").fillna(0.0)
    merged["incidents"] = pd.to_numeric(merged.get("incidents"), errors="coerce").fillna(0.0)
    merged["nps_mean"] = pd.to_numeric(merged.get("nps_mean"), errors="coerce")
    merged["nps_weighted_sum"] = merged["nps_mean"].fillna(0.0) * merged["responses"]

    time_col = "week" if "week" in merged.columns else ("date" if "date" in merged.columns else "")
    if not time_col:
        return pd.DataFrame(columns=list(by_topic_df.columns))

    grouped = (
        merged.groupby([time_col, "journey_label"], dropna=False, observed=True)
        .agg(
            responses=("responses", "sum"),
            focus_count=("focus_count", "sum"),
            incidents=("incidents", "sum"),
            nps_weighted_sum=("nps_weighted_sum", "sum"),
        )
        .reset_index()
    )
    grouped["nps_mean"] = grouped["nps_weighted_sum"] / grouped["responses"].replace({0: np.nan})
    grouped["focus_rate"] = grouped["focus_count"] / grouped["responses"].replace({0: np.nan})
    grouped["nps_topic"] = grouped["journey_label"].astype(str)
    keep_cols = [
        time_col,
        "nps_topic",
        "responses",
        "focus_count",
        "nps_mean",
        "focus_rate",
        "incidents",
    ]
    return grouped[keep_cols].sort_values([time_col, "nps_topic"]).reset_index(drop=True)


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
    journey_catalog_df: Optional[pd.DataFrame] = None,
    journey_links_df: Optional[pd.DataFrame] = None,
    executive_journey_catalog: Optional[list[dict[str, object]]] = None,
) -> pd.DataFrame:
    """Return presentable evidence chains backed by exact Helix↔VoC links."""

    if links_df is None or links_df.empty:
        return _empty_chain_df()

    enriched = _prepare_enriched_links(
        links_df,
        nps_focus_df,
        helix_df,
        touchpoint_source=touchpoint_source,
    )
    if enriched.empty:
        return _empty_chain_df()
    enriched["source_nps_topic"] = enriched["nps_topic"].astype(str).fillna("").str.strip()
    is_broken_mode = (
        str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
        == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS
    )
    is_executive_mode = (
        str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
        == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS
    )
    if is_broken_mode:
        local_links = journey_links_df.copy() if journey_links_df is not None else None
        if local_links is None or local_links.empty:
            _, local_links = build_broken_journey_catalog(links_df, nps_focus_df, helix_df)
        if local_links is None or local_links.empty:
            return _empty_chain_df()
        journey_cols = [
            "incident_id",
            "nps_id",
            "journey_id",
            "journey_label",
            "touchpoint",
            "palanca",
            "subpalanca",
            "journey_route",
            "journey_expected_evidence",
            "journey_cx_readout",
            "journey_impact_label",
            "journey_confidence_label",
        ]
        journey_map = local_links[journey_cols].drop_duplicates(["incident_id", "nps_id"])
        enriched = enriched.drop(
            columns=[c for c in ["touchpoint"] if c in enriched.columns]
        ).merge(
            journey_map,
            on=["incident_id", "nps_id"],
            how="inner",
            suffixes=("", "_journey"),
        )
        if enriched.empty:
            return _empty_chain_df()
        enriched["journey_touchpoint"] = enriched["touchpoint"].astype(str).fillna("").str.strip()
        enriched["journey_palanca"] = enriched["palanca"].astype(str).fillna("").str.strip()
        enriched["journey_subpalanca"] = enriched["subpalanca"].astype(str).fillna("").str.strip()
        enriched["journey_title"] = enriched["journey_label"].astype(str).fillna("").str.strip()
        enriched["touchpoint"] = enriched["journey_touchpoint"]
        enriched["palanca"] = enriched["journey_palanca"]
        enriched["subpalanca"] = enriched["journey_subpalanca"]
        enriched["nps_topic"] = enriched["journey_title"]
    if is_executive_mode:
        active_executive_catalog = executive_journey_catalog or _default_executive_journey_catalog()
        journey_matches = [
            _executive_journey_match(
                nps_topic=topic,
                touchpoint=tp,
                palanca=pal,
                subpalanca=sub,
                incident_topic=inc_topic,
                incident_summary=inc_summary,
                comment_txt=comment,
                catalog=active_executive_catalog,
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
    source_mode = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
    group_col = (
        "journey_id"
        if (is_executive_mode or is_broken_mode)
        else _grouping_key_for_source(source_mode)
    )
    for topic, grp in enriched.groupby(group_col, dropna=False, observed=True):
        grp = grp.copy()
        linked_pairs = int(len(grp[["incident_id", "nps_id"]].drop_duplicates()))
        if linked_pairs < int(min_links_per_topic):
            continue

        if is_executive_mode or is_broken_mode:
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
            topic_label = _group_label_for_source(grp, touchpoint_source=source_mode) or str(topic)
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
            touchpoint = _touchpoint_for_source(grp, touchpoint_source=source_mode)
            helix_source_service_n2 = (
                str(grp["helix_source_service_n2"].mode(dropna=True).iloc[0])
                if "helix_source_service_n2" in grp.columns
                and not grp["helix_source_service_n2"].mode(dropna=True).empty
                else ""
            )
            journey_route = ""
            journey_expected_evidence = ""
            journey_cx_readout = ""
            journey_impact_label = ""
            journey_confidence_label = ""
        if is_executive_mode or is_broken_mode:
            helix_source_service_n2 = (
                str(grp["helix_source_service_n2"].mode(dropna=True).iloc[0])
                if "helix_source_service_n2" in grp.columns
                and not grp["helix_source_service_n2"].mode(dropna=True).empty
                else ""
            )

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
        comment_records = [
            {
                "comment_id": str(r.get("nps_id", "") or "").strip(),
                "date": _format_nps_date(r.get("nps_date")),
                "nps": _format_nps_score(r.get("nps_score")),
                "group": _derive_nps_group(r.get("nps_group"), r.get("nps_score")),
                "palanca": str(r.get("palanca", "") or "").strip(),
                "subpalanca": str(r.get("subpalanca", "") or "").strip(),
                "comment": " ".join(
                    str(r.get("comment_norm") or r.get("comment_txt") or "").split()
                ),
            }
            for _, r in comment_ranked.iterrows()
            if str(r.get("comment_norm") or r.get("comment_txt") or "").strip()
        ]
        if not incident_records or not comment_records:
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
        comment_sample_count = len(comment_records)
        incident_sample_label = (
            ", ".join(incident_ids[:3])
            if incident_ids
            else f"{incident_sample_count} incidencias Helix"
        )
        if len(incident_ids) > 3:
            incident_sample_label = f"{incident_sample_label} y {len(incident_ids) - 3} más"
        comment_sample_label = " | ".join(comment_examples[:2])
        source_topics = _source_topics_for_group(grp)
        anchor_topic = source_topics[0] if source_topics else ""
        story = _chain_story_for_source(
            touchpoint_source=source_mode,
            topic_label=topic_label,
            touchpoint=touchpoint,
            palanca=palanca,
            subpalanca=subpalanca,
            helix_source_service_n2=helix_source_service_n2,
            journey_route=journey_route,
            journey_cx_readout=journey_cx_readout,
            journey_expected_evidence=journey_expected_evidence,
            journey_impact_label=journey_impact_label,
            journey_confidence_label=journey_confidence_label,
            incident_sample_count=incident_sample_count,
            incident_sample_label=incident_sample_label,
            comment_sample_count=comment_sample_count,
            comment_sample_label=comment_sample_label,
            anchor_topic=anchor_topic,
        )
        rows.append(
            {
                "nps_topic": topic_label,
                "anchor_topic": anchor_topic,
                "source_topics": source_topics,
                "touchpoint": touchpoint,
                "palanca": palanca,
                "subpalanca": subpalanca,
                "helix_source_service_n2": helix_source_service_n2,
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
                "comment_records": comment_records,
                "chain_story": story,
                "delta_focus_rate_pp": delta_focus_rate_pp,
                "incident_rate_per_100_responses": incident_rate_per_100_responses,
                "incidents": incidents_total,
                "responses": responses_total,
                "action_lane": action_lane,
                "owner_role": owner_role,
                "eta_weeks": eta_weeks,
                "presentation_mode": (
                    str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN)
                    if (is_executive_mode or is_broken_mode)
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
