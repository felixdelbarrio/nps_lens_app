from __future__ import annotations

import json
import re
import unicodedata
from hashlib import sha1, sha256
from typing import Optional, Union

import pandas as pd

from nps_lens import PIPELINE_VERSION
from nps_lens.core.store import DatasetContext
from nps_lens.ingest.base import IngestResult, ValidationIssue, require_columns
from nps_lens.ingest.features import add_precomputed_features

PARSER_VERSION = "2026.04.17"

NPS_THERMAL_REQUIRED = [
    "Fecha",
    "NPS",
    "Canal",
    "Palanca",
    "Subpalanca",
]

NPS_THERMAL_OPTIONAL = [
    "ID",
    "NPS Group",
    "Comment",
    "UsuarioDecisión",
    "Browser",
    "Operating System",
    "service_origin",
    "service_origin_n1",
    "service_origin_n2",
]

SCHEMA_DRIFT_COLUMNS = {
    "Browser",
    "Operating System",
}

_HEADER_TOKEN_RE = re.compile(r"[^a-z0-9]+")
_WS_RE = re.compile(r"\s+")
_EMPTY_MARKERS = {"", "nan", "none", "null", "nat"}

_HEADER_ALIASES = {
    "fecha": "Fecha",
    "id": "ID",
    "nps": "NPS",
    "npsgroup": "NPS Group",
    "gruponps": "NPS Group",
    "nps_group": "NPS Group",
    "comment": "Comment",
    "comments": "Comment",
    "comentario": "Comment",
    "texto": "Comment",
    "usuariodecision": "UsuarioDecisión",
    "usuariodecisionfinal": "UsuarioDecisión",
    "decisionusuario": "UsuarioDecisión",
    "usuario decision": "UsuarioDecisión",
    "canal": "Canal",
    "channel": "Canal",
    "palanca": "Palanca",
    "lever": "Palanca",
    "subpalanca": "Subpalanca",
    "sublever": "Subpalanca",
    "browser": "Browser",
    "navegador": "Browser",
    "operatingsystem": "Operating System",
    "operatingsystemname": "Operating System",
    "sistemaoperativo": "Operating System",
    "serviceoriginbuug": "service_origin",
    "serviceoriginbug": "service_origin",
    "serviceorigin": "service_origin",
    "service_origin": "service_origin",
    "serviceoriginn1": "service_origin_n1",
    "service_origin_n1": "service_origin_n1",
    "serviceoriginn2": "service_origin_n2",
    "service_origin_n2": "service_origin_n2",
}


def dataset_id_for(path: str, service_origin: str, service_origin_n1: str) -> str:
    h = sha1(
        f"{path}|{service_origin}|{service_origin_n1}|{PARSER_VERSION}".encode("utf-8")
    ).hexdigest()[:10]
    return f"nps_thermal:{service_origin}:{service_origin_n1}:{h}"


def _normalize_header(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = _HEADER_TOKEN_RE.sub(" ", text)
    return _WS_RE.sub(" ", text).strip()


def _canonical_column_name(column: object) -> str:
    normalized = _normalize_header(column)
    alias_key = normalized.replace(" ", "")
    mapped = _HEADER_ALIASES.get(alias_key) or _HEADER_ALIASES.get(normalized)
    return mapped or str(column).strip()


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _coerce_string(value: object) -> str:
    if _is_missing(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    text = str(value).strip()
    return _WS_RE.sub(" ", text)


def _split_csvish(value: object) -> list[str]:
    text = _coerce_string(value)
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _normalize_comment(value: object) -> str:
    return _coerce_string(value)


def _normalize_nps_group(score: object, group: object) -> str:
    explicit = _coerce_string(group).upper()
    if explicit:
        if "PROM" in explicit:
            return "PROMOTOR"
        if "PAS" in explicit or "NEUT" in explicit:
            return "PASIVO"
        if "DET" in explicit:
            return "DETRACTOR"
        return explicit

    numeric = pd.to_numeric(pd.Series([score]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return ""
    if numeric >= 9:
        return "PROMOTOR"
    if numeric <= 6:
        return "DETRACTOR"
    return "PASIVO"


def _business_key(row: pd.Series) -> str:
    external_id = _coerce_string(row.get("ID"))
    if external_id and external_id.lower() not in _EMPTY_MARKERS:
        return f"id:{external_id}"

    payload = {
        "Fecha": row.get("Fecha").isoformat() if isinstance(row.get("Fecha"), pd.Timestamp) else "",
        "NPS": (
            None
            if pd.isna(row.get("NPS"))
            else float(pd.to_numeric(pd.Series([row.get("NPS")]), errors="coerce").iloc[0])
        ),
        "Comment": _normalize_comment(row.get("Comment")),
        "UsuarioDecisión": _coerce_string(row.get("UsuarioDecisión")),
        "Canal": _coerce_string(row.get("Canal")),
        "Palanca": _coerce_string(row.get("Palanca")),
        "Subpalanca": _coerce_string(row.get("Subpalanca")),
        "Browser": _coerce_string(row.get("Browser")),
        "Operating System": _coerce_string(row.get("Operating System")),
        "service_origin": _coerce_string(row.get("service_origin")),
        "service_origin_n1": _coerce_string(row.get("service_origin_n1")),
        "service_origin_n2": _coerce_string(row.get("service_origin_n2")),
    }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return f"fp:{sha256(encoded.encode('utf-8')).hexdigest()}"


def _record_fingerprint(row: pd.Series, extra_columns: list[str]) -> str:
    payload = {
        "ID": _coerce_string(row.get("ID")),
        "Fecha": row.get("Fecha").isoformat() if isinstance(row.get("Fecha"), pd.Timestamp) else "",
        "NPS": (
            None
            if pd.isna(row.get("NPS"))
            else float(pd.to_numeric(pd.Series([row.get("NPS")]), errors="coerce").iloc[0])
        ),
        "NPS Group": _coerce_string(row.get("NPS Group")),
        "Comment": _normalize_comment(row.get("Comment")),
        "UsuarioDecisión": _coerce_string(row.get("UsuarioDecisión")),
        "Canal": _coerce_string(row.get("Canal")),
        "Palanca": _coerce_string(row.get("Palanca")),
        "Subpalanca": _coerce_string(row.get("Subpalanca")),
        "Browser": _coerce_string(row.get("Browser")),
        "Operating System": _coerce_string(row.get("Operating System")),
        "service_origin": _coerce_string(row.get("service_origin")),
        "service_origin_n1": _coerce_string(row.get("service_origin_n1")),
        "service_origin_n2": _coerce_string(row.get("service_origin_n2")),
        "extra": {column: _coerce_string(row.get(column)) for column in extra_columns},
    }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _infer_context(
    df: pd.DataFrame, column: str, current: Optional[str]
) -> tuple[Optional[str], list[ValidationIssue]]:
    issues: list[ValidationIssue] = []
    if current is not None:
        return current, issues
    if column not in df.columns:
        return current, issues

    values = sorted(
        {_coerce_string(value) for value in df[column].tolist() if _coerce_string(value)}
    )
    if len(values) == 1:
        return values[0], issues
    if len(values) > 1:
        issues.append(
            ValidationIssue(
                level="ERROR",
                code="ambiguous_context",
                message=f"Excel contiene múltiples valores en {column}. Debes seleccionar el contexto explícitamente.",
                column=column,
                details={"values": values[:10]},
            )
        )
    return current, issues


def _filter_context(
    df: pd.DataFrame,
    column: str,
    expected: str,
    issues: list[ValidationIssue],
    *,
    normalizer: Optional[callable] = None,
) -> pd.DataFrame:
    if column not in df.columns:
        df[column] = expected
        return df

    norm = normalizer or (lambda value: _coerce_string(value))
    before = len(df)
    mask = df[column].apply(norm) == norm(expected)
    filtered = df.loc[mask].copy()
    dropped = before - len(filtered)
    if dropped:
        issues.append(
            ValidationIssue(
                level="WARN",
                code="rows_filtered_by_context",
                message=f"Filtradas {dropped} filas fuera de {column}={expected or '∅'}.",
                column=column,
                details={"dropped_rows": dropped, "expected": expected},
            )
        )
    return filtered


def read_nps_thermal_excel(
    path: str,
    service_origin: Optional[str] = None,
    service_origin_n1: Optional[str] = None,
    service_origin_n2: Optional[str] = None,
    sheet_name: Optional[Union[str, int]] = None,
) -> IngestResult:
    sheet: Union[str, int] = sheet_name if sheet_name is not None and sheet_name != "" else 0
    df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl", dtype=object)
    if isinstance(df, dict):
        df = list(df.values())[0]

    df = df.rename(columns={column: _canonical_column_name(column) for column in df.columns})

    issues: list[ValidationIssue] = []
    raw_rows = int(len(df))

    known_columns = set(NPS_THERMAL_REQUIRED + NPS_THERMAL_OPTIONAL)
    extra_columns = sorted(
        [
            column
            for column in df.columns
            if column not in known_columns or column in SCHEMA_DRIFT_COLUMNS
        ]
    )
    if extra_columns:
        issues.append(
            ValidationIssue(
                level="WARN",
                code="extra_columns_detected",
                message="Se detectaron columnas adicionales no críticas. La carga continúa y se conservan en trazabilidad.",
                details={"columns": extra_columns},
            )
        )

    issues.extend(require_columns(df, NPS_THERMAL_REQUIRED))

    missing_optional_columns = [
        column for column in NPS_THERMAL_OPTIONAL if column not in df.columns
    ]
    for column in missing_optional_columns:
        if column == "NPS Group":
            issues.append(
                ValidationIssue(
                    level="WARN",
                    code="optional_column_missing",
                    message="Falta NPS Group; se derivará desde NPS.",
                    column=column,
                )
            )
        elif column == "Comment":
            issues.append(
                ValidationIssue(
                    level="WARN",
                    code="optional_column_missing",
                    message="Falta Comment; se rellenará vacío y se perderá capacidad de análisis textual.",
                    column=column,
                )
            )
        elif column == "ID":
            issues.append(
                ValidationIssue(
                    level="WARN",
                    code="optional_column_missing",
                    message="Falta ID; se usará un fingerprint estable para deduplicar.",
                    column=column,
                )
            )
        elif column in {"service_origin", "service_origin_n1", "service_origin_n2"}:
            continue
        else:
            issues.append(
                ValidationIssue(
                    level="WARN",
                    code="optional_column_missing",
                    message=f"Falta {column}; se rellenará vacío.",
                    column=column,
                )
            )

    service_origin, inferred_issues = _infer_context(df, "service_origin", service_origin)
    issues.extend(inferred_issues)
    service_origin_n1, inferred_issues = _infer_context(df, "service_origin_n1", service_origin_n1)
    issues.extend(inferred_issues)

    if service_origin is None or service_origin_n1 is None:
        issues.append(
            ValidationIssue(
                level="ERROR",
                code="missing_context",
                message="Falta contexto: service_origin y/o service_origin_n1. Debes enviarlos en la carga o incluirlos como columnas únicas en el Excel.",
            )
        )

    if any(issue.level == "ERROR" for issue in issues):
        so = service_origin or "unknown"
        sn1 = service_origin_n1 or "unknown"
        return IngestResult(
            df=df,
            issues=issues,
            dataset_id=dataset_id_for(path, so, sn1),
            meta={
                "parser_version": PARSER_VERSION,
                "raw_rows": raw_rows,
                "extra_columns": extra_columns,
                "missing_optional_columns": missing_optional_columns,
            },
        )

    work = df.copy()
    work = _filter_context(work, "service_origin", str(service_origin), issues)
    work = _filter_context(work, "service_origin_n1", str(service_origin_n1), issues)
    work["service_origin"] = str(service_origin)
    work["service_origin_n1"] = str(service_origin_n1)
    if "service_origin_n2" not in work.columns:
        work["service_origin_n2"] = ""
    work["service_origin_n2"] = work["service_origin_n2"].apply(
        lambda value: ", ".join(_split_csvish(value))
    )

    if service_origin_n2 is not None:
        work = _filter_context(
            work,
            "service_origin_n2",
            ", ".join(_split_csvish(service_origin_n2)),
            issues,
            normalizer=DatasetContext._norm_n2,
        )

    for column in NPS_THERMAL_OPTIONAL:
        if column not in work.columns:
            work[column] = ""

    for column in [
        "ID",
        "Comment",
        "UsuarioDecisión",
        "Canal",
        "Palanca",
        "Subpalanca",
        "Browser",
        "Operating System",
        "service_origin",
        "service_origin_n1",
        "service_origin_n2",
    ]:
        work[column] = work[column].apply(
            _normalize_comment if column == "Comment" else _coerce_string
        )

    work["Fecha"] = pd.to_datetime(work["Fecha"], errors="coerce")
    work["NPS"] = pd.to_numeric(work["NPS"], errors="coerce")
    work["NPS Group"] = [
        _normalize_nps_group(score, group)
        for score, group in zip(work["NPS"].tolist(), work["NPS Group"].tolist())
    ]

    invalid_date_rows = int(work["Fecha"].isna().sum())
    if invalid_date_rows:
        issues.append(
            ValidationIssue(
                level="WARN",
                code="invalid_dates_dropped",
                message=f"Se descartaron {invalid_date_rows} filas con Fecha inválida.",
                column="Fecha",
                details={"rows": invalid_date_rows},
            )
        )
    invalid_nps_rows = int(work["NPS"].isna().sum())
    if invalid_nps_rows:
        issues.append(
            ValidationIssue(
                level="WARN",
                code="invalid_nps_dropped",
                message=f"Se descartaron {invalid_nps_rows} filas con NPS inválido.",
                column="NPS",
                details={"rows": invalid_nps_rows},
            )
        )

    work = work.loc[work["Fecha"].notna() & work["NPS"].notna()].copy()
    if work.empty:
        issues.append(
            ValidationIssue(
                level="ERROR",
                code="no_valid_rows",
                message="No quedan filas válidas tras normalizar Fecha y NPS.",
            )
        )
        return IngestResult(
            df=work,
            issues=issues,
            dataset_id=dataset_id_for(path, str(service_origin), str(service_origin_n1)),
            meta={
                "parser_version": PARSER_VERSION,
                "raw_rows": raw_rows,
                "extra_columns": extra_columns,
                "missing_optional_columns": missing_optional_columns,
            },
        )

    work["_source_row_number"] = range(2, len(work) + 2)
    work["_business_key"] = work.apply(_business_key, axis=1)
    work["_record_fingerprint"] = work.apply(
        lambda row: _record_fingerprint(row, extra_columns),
        axis=1,
    )
    duplicate_rows_in_file = int(work.duplicated(subset=["_business_key"], keep="last").sum())
    if duplicate_rows_in_file:
        issues.append(
            ValidationIssue(
                level="WARN",
                code="duplicate_rows_in_file",
                message=f"Se descartaron {duplicate_rows_in_file} filas duplicadas dentro del fichero usando la clave de negocio canónica.",
                details={"rows": duplicate_rows_in_file},
            )
        )
        work = work.drop_duplicates(subset=["_business_key"], keep="last").copy()

    work, added_features = add_precomputed_features(work)
    if added_features:
        issues.append(
            ValidationIssue(
                level="INFO",
                code="precomputed_features",
                message=f"Features precomputadas: {', '.join(added_features)}",
                details={"columns": added_features},
            )
        )

    return IngestResult(
        df=work.reset_index(drop=True),
        issues=issues,
        dataset_id=dataset_id_for(path, str(service_origin), str(service_origin_n1)),
        meta={
            "parser_version": PARSER_VERSION,
            "pipeline_version": PIPELINE_VERSION,
            "raw_rows": raw_rows,
            "normalized_rows": int(len(work)),
            "duplicate_rows_in_file": duplicate_rows_in_file,
            "extra_columns": extra_columns,
            "missing_optional_columns": missing_optional_columns,
        },
    )
