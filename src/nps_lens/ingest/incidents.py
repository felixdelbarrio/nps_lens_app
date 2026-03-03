from __future__ import annotations

from hashlib import sha1
from typing import List

import pandas as pd

from nps_lens.ingest.base import IngestResult, ValidationIssue, require_columns


INCIDENTS_REQUIRED = [
    "opened_at",
    "closed_at",
    "incident_id",
    "category",
    "severity",
    "system",
    "description",
    "channel",
    "geo",
]


def dataset_id_for(path: str) -> str:
    h = sha1(path.encode("utf-8")).hexdigest()[:10]
    return f"incidents:{h}"


def read_incidents_csv(path: str) -> IngestResult:
    df = pd.read_csv(path)
    issues: List[ValidationIssue] = []
    issues.extend(require_columns(df, INCIDENTS_REQUIRED))
    if any(i.level == "ERROR" for i in issues):
        return IngestResult(df=df, issues=issues, dataset_id=dataset_id_for(path))

    df["opened_at"] = pd.to_datetime(df["opened_at"], errors="coerce")
    df["closed_at"] = pd.to_datetime(df["closed_at"], errors="coerce")
    if df["opened_at"].isna().any():
        issues.append(ValidationIssue(level="WARN", message="Some opened_at values are invalid"))
    return IngestResult(df=df, issues=issues, dataset_id=dataset_id_for(path))
