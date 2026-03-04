from __future__ import annotations

from hashlib import sha1
from typing import Optional, Union, List

import pandas as pd

from nps_lens.ingest.base import IngestResult, ValidationIssue, require_columns, standardize_columns


HELIX_REQUIRED = [
    "BBVA_SourceServiceCompany",
    "BBVA_SourceServiceN1",
    "BBVA_SourceServiceN2",
]


def dataset_id_for(path: str, service_origin: str, service_origin_n1: str) -> str:
    h = sha1(f"{path}|{service_origin}|{service_origin_n1}|helix".encode("utf-8")).hexdigest()[:10]
    return f"helix_incidents:{service_origin}:{service_origin_n1}:{h}"


def _split_csvish(value: object) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def _detect_fecha_column(df: pd.DataFrame) -> Optional[str]:
    """Best-effort date column detection for Helix incident exports.

    We normalize to a canonical `Fecha` used by storage partitioning.
    """
    candidates = [
        "Fecha",
        "Fecha apertura",
        "Fecha Apertura",
        "Fecha creación",
        "Fecha creacion",
        "CreatedDate",
        "Created Date",
        "Open Date",
        "Date",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    # fallback: first column that contains 'fecha' or 'date'
    for c in df.columns:
        lc = str(c).lower()
        if "fecha" in lc or "date" in lc:
            return c
    return None


def read_helix_incidents_excel(
    path: str,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    sheet_name: Optional[Union[str, int]] = None,
) -> IngestResult:
    """Read + filter Helix incidents Excel by selected context.

    Contract (strict filtering):
      - Always filter by:
          service_origin == BBVA_SourceServiceCompany
          service_origin_n1 == BBVA_SourceServiceN1
      - Only if the selected context has service_origin_n2 tokens (non-empty),
        then ALSO filter by strict equality (token-set) with BBVA_SourceServiceN2.

    If after filtering there are no rows, return empty df (ingestion is not performed).
    """

    sn: Union[str, int] = 0 if not sheet_name else sheet_name
    df = pd.read_excel(path, sheet_name=sn, engine="openpyxl")
    if isinstance(df, dict):
        df = list(df.values())[0]

    # Canonicalize / robust column names (tolerate minor variants)
    df = standardize_columns(
        df,
        mapping={
            "BBVA_SourceServiceCompany": "BBVA_SourceServiceCompany",
            "BBVA Source Service Company": "BBVA_SourceServiceCompany",
            "SourceServiceCompany": "BBVA_SourceServiceCompany",
            "BBVA_SourceServiceN1": "BBVA_SourceServiceN1",
            "BBVA Source Service N1": "BBVA_SourceServiceN1",
            "SourceServiceN1": "BBVA_SourceServiceN1",
            "BBVA_SourceServiceN2": "BBVA_SourceServiceN2",
            "BBVA Source Service N2": "BBVA_SourceServiceN2",
            "SourceServiceN2": "BBVA_SourceServiceN2",
        },
    )

    issues: List[ValidationIssue] = []
    issues.extend(require_columns(df, HELIX_REQUIRED))

    if any(i.level == "ERROR" for i in issues):
        return IngestResult(df=df, issues=issues, dataset_id=dataset_id_for(path, service_origin, service_origin_n1))

    # Normalize N2 column to stable CSV-ish string
    d = df.copy()
    d["BBVA_SourceServiceCompany"] = d["BBVA_SourceServiceCompany"].astype(str)
    d["BBVA_SourceServiceN1"] = d["BBVA_SourceServiceN1"].astype(str)
    d["BBVA_SourceServiceN2"] = d["BBVA_SourceServiceN2"].apply(lambda v: ", ".join(_split_csvish(v)))

    # Mandatory filters
    before = len(d)
    d = d.loc[d["BBVA_SourceServiceCompany"].astype(str) == str(service_origin)]
    dropped = before - len(d)
    if dropped:
        issues.append(
            ValidationIssue(
                level="INFO",
                message=f"Filtradas {dropped} filas fuera de BBVA_SourceServiceCompany={service_origin}.",
            )
        )

    before = len(d)
    d = d.loc[d["BBVA_SourceServiceN1"].astype(str) == str(service_origin_n1)]
    dropped = before - len(d)
    if dropped:
        issues.append(
            ValidationIssue(
                level="INFO",
                message=f"Filtradas {dropped} filas fuera de BBVA_SourceServiceN1={service_origin_n1}.",
            )
        )

    # Optional N2 filter ONLY when selected context has tokens
    sel_n2 = [v.strip() for v in (service_origin_n2 or "").split(",") if v.strip()]
    if sel_n2:
        sel = set(sel_n2)

        def _row_equals_selected(v: object) -> bool:
            """Strict match for N2.

            The Excel can contain empty values or comma-separated tokens.
            When the user selects N2 in the context, we ingest ONLY rows
            whose token-set equals the selected token-set.
            """
            toks = {p.strip() for p in str(v or "").split(",") if p.strip()}
            return toks == sel

        before = len(d)
        d = d.loc[d["BBVA_SourceServiceN2"].apply(_row_equals_selected)]
        dropped = before - len(d)
        if dropped:
            issues.append(
                ValidationIssue(
                    level="INFO",
                    message=(
                        f"Filtradas {dropped} filas fuera de BBVA_SourceServiceN2 == {{{', '.join(sorted(sel))}}}."
                    ),
                )
            )

    # If empty after filtering, signal to caller (no persistence)
    if d.empty:
        issues.append(
            ValidationIssue(
                level="WARN",
                message=(
                    "No hay registros para el contexto seleccionado. "
                    "La ingesta se omite para evitar mezclar contextos."
                ),
            )
        )
        return IngestResult(df=d, issues=issues, dataset_id=dataset_id_for(path, service_origin, service_origin_n1))

    # Attach selected context columns for downstream joins
    d["service_origin"] = str(service_origin)
    d["service_origin_n1"] = str(service_origin_n1)
    d["service_origin_n2_selected"] = ", ".join(sel_n2)

    # Canonical Fecha (best-effort)
    fecha_col = _detect_fecha_column(d)
    if fecha_col is not None:
        d["Fecha"] = pd.to_datetime(d[fecha_col], errors="coerce")
        bad = int(d["Fecha"].isna().sum())
        if bad:
            issues.append(ValidationIssue(level="WARN", message=f"{bad} filas con Fecha inválida (columna '{fecha_col}')"))
    else:
        d["Fecha"] = pd.NaT
        issues.append(
            ValidationIssue(
                level="WARN",
                message="No se detectó columna de fecha. Se guardará Fecha=NaT (sin particionado temporal).",
            )
        )

    return IngestResult(df=d, issues=issues, dataset_id=dataset_id_for(path, service_origin, service_origin_n1))
