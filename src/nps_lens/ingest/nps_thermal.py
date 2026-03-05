from __future__ import annotations

from hashlib import sha1
from typing import Optional, Union

import pandas as pd

from nps_lens.core.store import DatasetContext
from nps_lens.ingest.base import IngestResult, ValidationIssue, require_columns, standardize_columns
from nps_lens.ingest.features import add_precomputed_features

NPS_THERMAL_REQUIRED = [
    "Fecha",
    "ID",
    "NPS Group",
    "NPS",
    "Comment",
    "UsuarioDecisión",
    "Canal",
    "Palanca",
    "Subpalanca",
]


def dataset_id_for(path: str, service_origin: str, service_origin_n1: str) -> str:
    h = sha1(f"{path}|{service_origin}|{service_origin_n1}".encode("utf-8")).hexdigest()[:10]
    return f"nps_thermal:{service_origin}:{service_origin_n1}:{h}"


def _split_csvish(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def read_nps_thermal_excel(
    path: str,
    service_origin: Optional[str] = None,
    service_origin_n1: Optional[str] = None,
    service_origin_n2: Optional[str] = None,
    sheet_name: Optional[Union[str, int]] = None,
) -> IngestResult:
    """Read + normalize NPS térmico Excel.

    Context contract for this app:
      - Dataset is *stored* per (service_origin, service_origin_n1)
      - Column `service_origin_n2` is optional and may contain comma-separated values.

    The Excel may:
      1) already contain the context columns (service_origin_buug/service_origin_n1/service_origin_n2)
      2) not contain them, in which case the caller must provide service_origin/service_origin_n1
    """

    sn: Union[str, int] = sheet_name if sheet_name else 0
    df = pd.read_excel(path, sheet_name=sn, engine="openpyxl")
    if isinstance(df, dict):
        df = list(df.values())[0]

    # Canonicalize column names (robust to minor variants)
    df = standardize_columns(
        df,
        mapping={
            "NPS Group": "NPS Group",
            "UsuarioDecisión": "UsuarioDecisión",
            # context columns (note the upstream typo: service_origin_buug)
            "service_origin_buug": "service_origin",
            "service_origin_bug": "service_origin",
            "service_origin": "service_origin",
            "service_origin_n1": "service_origin_n1",
            "service_origin_n2": "service_origin_n2",
        },
    )

    issues: list[ValidationIssue] = []
    issues.extend(require_columns(df, NPS_THERMAL_REQUIRED))

    # Resolve context (either from args or infer from columns)
    if "service_origin" in df.columns and service_origin is None:
        vals = [
            v for v in df["service_origin"].astype(str).dropna().unique().tolist() if str(v).strip()
        ]
        if len(vals) == 1:
            service_origin = str(vals[0])
        elif len(vals) > 1:
            issues.append(
                ValidationIssue(
                    level="ERROR",
                    message=(
                        "Excel contiene múltiples service_origin. "
                        "Selecciona uno en la UI o filtra antes de importar."
                    ),
                )
            )
    if "service_origin_n1" in df.columns and service_origin_n1 is None:
        vals = [
            v
            for v in df["service_origin_n1"].astype(str).dropna().unique().tolist()
            if str(v).strip()
        ]
        if len(vals) == 1:
            service_origin_n1 = str(vals[0])
        elif len(vals) > 1:
            issues.append(
                ValidationIssue(
                    level="ERROR",
                    message=(
                        "Excel contiene múltiples service_origin_n1. "
                        "Selecciona uno en la UI o filtra antes de importar."
                    ),
                )
            )

    if service_origin is None or service_origin_n1 is None:
        issues.append(
            ValidationIssue(
                level="ERROR",
                message="Falta contexto: service_origin y/o service_origin_n1 (no inferibles del Excel).",
            )
        )

    if any(i.level == "ERROR" for i in issues):
        # best-effort dataset_id; keep deterministic even when missing ctx
        so = service_origin or "unknown"
        sn1 = service_origin_n1 or "unknown"
        return IngestResult(df=df, issues=issues, dataset_id=dataset_id_for(path, so, sn1))

    # Filter to selected context if the Excel has those columns
    df_out = df.copy()
    if "service_origin" in df_out.columns:
        before = len(df_out)
        df_out = df_out.loc[df_out["service_origin"].astype(str) == str(service_origin)]
        dropped = before - len(df_out)
        if dropped:
            issues.append(
                ValidationIssue(
                    level="WARN",
                    message=f"Filtradas {dropped} filas fuera de service_origin={service_origin}.",
                )
            )
    else:
        df_out["service_origin"] = str(service_origin)

    if "service_origin_n1" in df_out.columns:
        before = len(df_out)
        df_out = df_out.loc[df_out["service_origin_n1"].astype(str) == str(service_origin_n1)]
        dropped = before - len(df_out)
        if dropped:
            issues.append(
                ValidationIssue(
                    level="WARN",
                    message=f"Filtradas {dropped} filas fuera de service_origin_n1={service_origin_n1}.",
                )
            )
    else:
        df_out["service_origin_n1"] = str(service_origin_n1)

    # Ensure service_origin_n2 exists (optional)
    if "service_origin_n2" not in df_out.columns:
        df_out["service_origin_n2"] = ""

    # normalize types
    df_out["Fecha"] = pd.to_datetime(df_out["Fecha"], errors="coerce")
    bad_dates = int(df_out["Fecha"].isna().sum())
    if bad_dates:
        issues.append(
            ValidationIssue(level="WARN", message=f"{bad_dates} filas con Fecha inválida")
        )

    df_out["NPS"] = pd.to_numeric(df_out["NPS"], errors="coerce")
    bad_nps = int(df_out["NPS"].isna().sum())
    if bad_nps:
        issues.append(ValidationIssue(level="WARN", message=f"{bad_nps} filas con NPS inválido"))

    df_out["ID"] = df_out["ID"].astype(str)

    # Normalize service_origin_n2 formatting (stable, no stray spaces)
    df_out["service_origin_n2"] = df_out["service_origin_n2"].apply(
        lambda v: ", ".join(_split_csvish(v))
    )

    # Optional filter by service_origin_n2 (strict token-set equality)
    if service_origin_n2 is not None:
        want = ", ".join(_split_csvish(service_origin_n2))
        want_key = DatasetContext._norm_n2(want)
        have_key = df_out["service_origin_n2"].apply(DatasetContext._norm_n2)
        before = len(df_out)
        df_out = df_out.loc[have_key == want_key]
        dropped = before - len(df_out)
        if dropped:
            issues.append(
                ValidationIssue(
                    level="WARN",
                    message=f"Filtradas {dropped} filas fuera de service_origin_n2={want or '∅'}.",
                )
            )

    # basic de-dup
    before = len(df_out)
    df_out = df_out.drop_duplicates(subset=["ID"], keep="last")
    dropped = before - len(df_out)
    if dropped:
        issues.append(ValidationIssue(level="WARN", message=f"Eliminados {dropped} IDs duplicados"))

    # Precompute stable derived columns used across the app (performance + determinism)
    df_out, added = add_precomputed_features(df_out)
    if added:
        issues.append(
            ValidationIssue(level="INFO", message=f"Features precomputadas: {', '.join(added)}")
        )

    return IngestResult(
        df=df_out,
        issues=issues,
        dataset_id=dataset_id_for(path, str(service_origin), str(service_origin_n1)),
    )
