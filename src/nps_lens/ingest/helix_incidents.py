from __future__ import annotations

from hashlib import sha1
from typing import List, Optional, Union
from zipfile import BadZipFile, ZipFile

import pandas as pd

from nps_lens.domain.helix import (
    OWNER_SUPPORT_COMPANY,
    SOURCE_SERVICE_COMPANY,
    SOURCE_SERVICE_N1,
    SOURCE_SERVICE_N2,
)
from nps_lens.domain.normalization import equivalence_key
from nps_lens.ingest.base import IngestResult, ValidationIssue, require_columns, standardize_columns
from nps_lens.ingest.helix_dates import (
    coerce_helix_datetime_series,
    looks_like_helix_datetime_column,
)

HELIX_REQUIRED = [OWNER_SUPPORT_COMPANY]


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
        "Submit Date",
        "SubmitDate",
        "Submitted Date",
        "SubmittedDate",
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


def _parse_helix_datetime(series: pd.Series) -> pd.Series:
    return coerce_helix_datetime_series(series)


def _looks_like_datetime_col(col: str) -> bool:
    return looks_like_helix_datetime_column(col)


def _auto_parse_epoch_datetime_columns(
    d: pd.DataFrame, issues: List[ValidationIssue]
) -> pd.DataFrame:
    """Convert epoch-encoded datetime columns to pandas datetime.

    Many Helix exports include multiple timestamp fields stored as Unix epoch
    milliseconds (or seconds). To make downstream slicing and debugging easy,
    we convert *all* date-like columns (by name heuristic) to datetime when they
    look numeric.

    We preserve non-date columns and avoid coercing small numeric fields.
    """

    out = d.copy()
    converted: List[str] = []
    for c in list(out.columns):
        if c == "Fecha":
            continue
        if not _looks_like_datetime_col(str(c)):
            continue

        dt = _parse_helix_datetime(out[c])
        if len(dt) and float(dt.notna().mean()) >= 0.6:
            out[c] = dt
            converted.append(str(c))

    if converted:
        issues.append(
            ValidationIssue(
                level="INFO",
                message=(
                    "Columnas de fecha detectadas y convertidas desde epoch/strings a datetime: "
                    + ", ".join(converted[:20])
                    + (" ..." if len(converted) > 20 else "")
                ),
            )
        )
    return out


def _workbook_has_external_hyperlinks(path: str) -> bool:
    """Inspect the OOXML relationship parts before loading the workbook in memory."""

    try:
        with ZipFile(path, "r") as archive:
            for name in archive.namelist():
                if not name.startswith("xl/worksheets/_rels/") or not name.endswith(".rels"):
                    continue
                if b"/hyperlink" in archive.read(name):
                    return True
    except (BadZipFile, OSError):
        return False
    return False


def read_helix_incidents_excel(
    path: str,
    service_origin: str,
    service_origin_n1: str,
    service_origin_n2: str,
    sheet_name: Optional[Union[str, int]] = None,
) -> IngestResult:
    """Read + filter Helix incidents Excel by selected context.

    Contract: Owner Support Company is the only mandatory ingestion context.
    Helix N1/N2 remain source attributes used later by optional channel attribution.

    If after filtering there are no rows, return empty df (ingestion is not performed).
    """

    # Prefer Helix_Raw / Helix raw sheet as source of truth (not "Issues oficial").
    if sheet_name is None:
        try:
            import openpyxl  # type: ignore

            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
            sheetnames = list(wb.sheetnames)
        except Exception:
            sheetnames = []
        candidates = ["Helix_Raw", "Helix raw", "Helix Raw", "helix_raw", "helix raw"]
        picked = None
        lower_map = {s.lower(): s for s in sheetnames}
        for c in candidates:
            if c.lower() in lower_map:
                picked = lower_map[c.lower()]
                break
        sn: Union[str, int] = picked if picked is not None else 0
    else:
        sn = sheet_name
    df = pd.read_excel(path, sheet_name=sn, engine="openpyxl")
    if isinstance(df, dict):
        df = list(df.values())[0]
    try:
        if not _workbook_has_external_hyperlinks(path):
            raise LookupError("workbook_without_external_hyperlinks")
        import openpyxl  # type: ignore

        wb_links = openpyxl.load_workbook(path, read_only=False, data_only=False)
        ws = wb_links[sn] if isinstance(sn, str) else wb_links.worksheets[int(sn)]
        header_row = next(ws.iter_rows(min_row=1, max_row=1))
        hyperlink_payload: dict[str, list[str]] = {}
        for col_idx, cell in enumerate(header_row, start=1):
            header = str(cell.value or "").strip()
            if not header:
                continue
            values: list[str] = []
            has_hyperlink = False
            for row_idx in range(2, len(df) + 2):
                target = ""
                link = ws.cell(row=row_idx, column=col_idx).hyperlink
                if link is not None:
                    target = str(
                        getattr(link, "target", "") or getattr(link, "location", "") or ""
                    ).strip()
                if target:
                    has_hyperlink = True
                values.append(target)
            if has_hyperlink:
                hyperlink_payload[f"{header}__hyperlink"] = values
        if hyperlink_payload:
            df = pd.concat([df, pd.DataFrame(hyperlink_payload, index=df.index)], axis=1)
    except (Exception, LookupError):
        pass

    # Canonicalize / robust column names (tolerate minor variants)
    df = standardize_columns(
        df,
        mapping={
            OWNER_SUPPORT_COMPANY: OWNER_SUPPORT_COMPANY,
            "OwnerSupportCompany": OWNER_SUPPORT_COMPANY,
            "Owner SupportCompany": OWNER_SUPPORT_COMPANY,
            SOURCE_SERVICE_COMPANY: SOURCE_SERVICE_COMPANY,
            "BBVA Source Service Company": SOURCE_SERVICE_COMPANY,
            "SourceServiceCompany": SOURCE_SERVICE_COMPANY,
            "Servicio Origen - BU/UG": SOURCE_SERVICE_COMPANY,
            SOURCE_SERVICE_N1: SOURCE_SERVICE_N1,
            "BBVA Source Service N1": SOURCE_SERVICE_N1,
            "SourceServiceN1": SOURCE_SERVICE_N1,
            "Servicio Origen - Servicio N1": SOURCE_SERVICE_N1,
            SOURCE_SERVICE_N2: SOURCE_SERVICE_N2,
            "BBVA Source Service N2": SOURCE_SERVICE_N2,
            "SourceServiceN2": SOURCE_SERVICE_N2,
            "Servicio Origen - Servicio N2": SOURCE_SERVICE_N2,
        },
    )

    issues: List[ValidationIssue] = []
    issues.extend(require_columns(df, HELIX_REQUIRED))

    if any(i.level == "ERROR" for i in issues):
        return IngestResult(
            df=df, issues=issues, dataset_id=dataset_id_for(path, service_origin, service_origin_n1)
        )

    if SOURCE_SERVICE_N1 not in df.columns:
        df[SOURCE_SERVICE_N1] = ""
    if SOURCE_SERVICE_N2 not in df.columns:
        df[SOURCE_SERVICE_N2] = ""

    # Normalize N2 column to stable CSV-ish string
    d = df.copy()
    d[OWNER_SUPPORT_COMPANY] = d[OWNER_SUPPORT_COMPANY].astype(str).str.strip()
    d[SOURCE_SERVICE_N1] = d[SOURCE_SERVICE_N1].astype(str).str.strip()
    d[SOURCE_SERVICE_N2] = d[SOURCE_SERVICE_N2].apply(lambda v: ", ".join(_split_csvish(v)))

    # Context filters
    before = len(d)
    selected_company_key = equivalence_key(service_origin)
    d = d.loc[d[OWNER_SUPPORT_COMPANY].map(equivalence_key) == selected_company_key]
    dropped = before - len(d)
    if dropped:
        issues.append(
            ValidationIssue(
                level="INFO",
                message=f"Filtradas {dropped} filas fuera de {OWNER_SUPPORT_COMPANY}={service_origin}.",
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
        return IngestResult(
            df=d, issues=issues, dataset_id=dataset_id_for(path, service_origin, service_origin_n1)
        )

    # Attach selected context columns for downstream joins
    d["service_origin"] = str(service_origin)
    d["service_origin_n1"] = ""
    d["service_origin_n2_selected"] = ""

    # Canonical Fecha (best-effort)
    fecha_col = _detect_fecha_column(d)
    if fecha_col is not None:
        d["Fecha"] = _parse_helix_datetime(d[fecha_col])
        bad = int(d["Fecha"].isna().sum())
        if bad:
            issues.append(
                ValidationIssue(
                    level="WARN", message=f"{bad} filas con Fecha inválida (columna '{fecha_col}')"
                )
            )
    else:
        d["Fecha"] = pd.NaT
        issues.append(
            ValidationIssue(
                level="WARN",
                message="No se detectó columna de fecha. Se guardará Fecha=NaT (sin particionado temporal).",
            )
        )

    # Convert other date-like columns (many come as epoch ms) for easier inspection and consistent slicing
    d = _auto_parse_epoch_datetime_columns(d, issues)

    return IngestResult(
        df=d, issues=issues, dataset_id=dataset_id_for(path, service_origin, service_origin_n1)
    )
