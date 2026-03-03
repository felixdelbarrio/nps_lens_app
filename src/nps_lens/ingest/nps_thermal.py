from __future__ import annotations

from datetime import datetime
from hashlib import sha1
from typing import List, Optional

import pandas as pd

from nps_lens.ingest.base import IngestResult, ValidationIssue, require_columns, standardize_columns


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


def dataset_id_for(path: str, geo: str, channel: str) -> str:
    h = sha1(f"{path}|{geo}|{channel}".encode("utf-8")).hexdigest()[:10]
    return f"nps_thermal:{geo}:{channel}:{h}"


def read_nps_thermal_excel(
    path: str,
    geo: str,
    channel: str,
    sheet_name: str = "Hoja1",
) -> IngestResult:
    df = pd.read_excel(path, sheet_name=sheet_name)
    df = standardize_columns(
        df,
        mapping={
            "NPS Group": "NPS Group",
            "UsuarioDecisión": "UsuarioDecisión",
        },
    )
    issues: List[ValidationIssue] = []
    issues.extend(require_columns(df, NPS_THERMAL_REQUIRED))

    if any(i.level == "ERROR" for i in issues):
        return IngestResult(df=df, issues=issues, dataset_id=dataset_id_for(path, geo, channel))

    # normalize types
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    bad_dates = df["Fecha"].isna().sum()
    if bad_dates:
        issues.append(ValidationIssue(level="WARN", message=f"{bad_dates} rows with invalid Fecha"))

    df["NPS"] = pd.to_numeric(df["NPS"], errors="coerce")
    bad_nps = df["NPS"].isna().sum()
    if bad_nps:
        issues.append(ValidationIssue(level="WARN", message=f"{bad_nps} rows with invalid NPS"))

    df["ID"] = df["ID"].astype(str)

    # add canonical dimensions
    df["geo"] = geo
    df["channel"] = channel

    # basic de-dup
    before = len(df)
    df = df.drop_duplicates(subset=["ID"], keep="last")
    dropped = before - len(df)
    if dropped:
        issues.append(ValidationIssue(level="WARN", message=f"Dropped {dropped} duplicate IDs"))

    return IngestResult(df=df, issues=issues, dataset_id=dataset_id_for(path, geo, channel))
