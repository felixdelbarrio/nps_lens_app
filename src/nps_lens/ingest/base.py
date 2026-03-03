from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import pandas as pd


@dataclass(frozen=True)
class ValidationIssue:
    level: str  # ERROR|WARN
    message: str
    column: Optional[str] = None


@dataclass(frozen=True)
class IngestResult:
    df: pd.DataFrame
    issues: list[ValidationIssue]
    dataset_id: str


def require_columns(df: pd.DataFrame, required: Sequence[str]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    missing = [c for c in required if c not in df.columns]
    for c in missing:
        issues.append(ValidationIssue(level="ERROR", message="Missing required column", column=c))
    return issues


def standardize_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    ren = {}
    for col in out.columns:
        key = col.strip()
        if key in mapping:
            ren[col] = mapping[key]
    return out.rename(columns=ren)
