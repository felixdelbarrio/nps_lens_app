from __future__ import annotations

import re
from hashlib import sha1
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from nps_lens import PIPELINE_VERSION
from nps_lens.core.nps_math import classify_nps_scores
from nps_lens.core.store import DatasetContext
from nps_lens.domain.column_aliases import ColumnAliasRegistry
from nps_lens.domain.record_identity import business_keys, hash_rows
from nps_lens.ingest.base import IngestResult, ValidationIssue, require_columns
from nps_lens.ingest.features import add_precomputed_features

PARSER_VERSION = "2026.09.16.column-aliases"

NPS_THERMAL_REQUIRED = [
    "Fecha",
    "NPS",
    "Canal",
]

NPS_THERMAL_OPTIONAL = [
    "Palanca",
    "Subpalanca",
    "ID",
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

_WS_RE = re.compile(r"\s+")
_EMPTY_MARKERS = {"", "nan", "none", "null", "nat"}


def dataset_id_for(path: str, service_origin: str, service_origin_n1: str) -> str:
    h = sha1(
        f"{path}|{service_origin}|{service_origin_n1}|{PARSER_VERSION}".encode("utf-8")
    ).hexdigest()[:10]
    return f"nps_thermal:{service_origin}:{service_origin_n1}:{h}"


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
    return "" if _is_missing(value) else str(value)


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
    column_aliases_path: Optional[Path] = None,
) -> IngestResult:
    sheet: Union[str, int] = sheet_name if sheet_name is not None and sheet_name != "" else 0
    df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl", dtype=object)
    if isinstance(df, dict):
        df = list(df.values())[0]

    issues: list[ValidationIssue] = []
    raw_rows = int(len(df))
    resolution = ColumnAliasRegistry.load(column_aliases_path).resolve(df.columns)
    df = df.rename(columns=resolution.rename)
    applied_column_aliases = [
        {"source": source, "canonical": canonical}
        for source, canonical in resolution.applied_aliases
    ]
    for canonical, sources in resolution.ambiguities:
        issues.append(
            ValidationIssue(
                level="ERROR",
                code="ambiguous_column_alias",
                message=(
                    f"Varias columnas representan '{canonical}': {', '.join(sources)}. "
                    "Conserva solo una o usa el nombre canónico."
                ),
                column=canonical,
                details={"columns": list(sources)},
            )
        )
    if applied_column_aliases:
        issues.append(
            ValidationIssue(
                level="INFO",
                code="column_aliases_applied",
                message="Alias de columnas aplicados: "
                + ", ".join(
                    f"{item['source']} → {item['canonical']}" for item in applied_column_aliases
                ),
                details={"mappings": applied_column_aliases},
            )
        )

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
        if column == "Comment":
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
                "applied_column_aliases": applied_column_aliases,
            },
        )

    work = df.copy()
    work["_source_row_number"] = work.index + 2
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

    for column, source in [
        ("Canal", "source_channel"),
        ("Palanca", "source_lever"),
        ("Subpalanca", "source_sublever"),
    ]:
        work[source] = work[column].map(_normalize_comment)

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
    work["NPS Group"] = classify_nps_scores(work["NPS"])

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
    invalid_nps_rows = int(work["NPS Group"].eq("").sum())
    if invalid_nps_rows:
        issues.append(
            ValidationIssue(
                level="WARN",
                code="invalid_nps_dropped",
                message=f"Se descartaron {invalid_nps_rows} filas con NPS inválido (debe ser un entero de 0 a 10).",
                column="NPS",
                details={"rows": invalid_nps_rows},
            )
        )

    work = work.loc[work["Fecha"].notna() & work["NPS Group"].ne("")].copy()
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
                "applied_column_aliases": applied_column_aliases,
            },
        )

    work["_business_key"] = business_keys(work)
    fingerprint_columns = [
        "ID",
        "Fecha",
        "NPS",
        "NPS Group",
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
        *extra_columns,
    ]
    work["_record_fingerprint"] = hash_rows(work, fingerprint_columns)
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

    from nps_lens.analytics.taxonomy import detect_taxonomy

    detection = detect_taxonomy(work)
    issues.append(
        ValidationIssue(
            level="INFO",
            code="taxonomy_detected",
            message="Taxonomía detectada: " + detection["state"],
            details=detection,
        )
    )
    return IngestResult(
        df=work.reset_index(drop=True),
        issues=issues,
        dataset_id=dataset_id_for(path, str(service_origin), str(service_origin_n1)),
        meta={
            "parser_version": PARSER_VERSION,
            "pipeline_version": PIPELINE_VERSION,
            "taxonomy": detection,
            "raw_rows": raw_rows,
            "normalized_rows": int(len(work)),
            "duplicate_rows_in_file": duplicate_rows_in_file,
            "extra_columns": extra_columns,
            "missing_optional_columns": missing_optional_columns,
            "applied_column_aliases": applied_column_aliases,
        },
    )
