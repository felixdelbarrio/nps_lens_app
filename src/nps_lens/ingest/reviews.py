from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1

import pandas as pd

from nps_lens.ingest.base import IngestResult, ValidationIssue, require_columns

REVIEWS_REQUIRED = ["store", "date", "rating", "text", "app_version", "geo"]


def dataset_id_for(source: str) -> str:
    h = sha1(source.encode("utf-8")).hexdigest()[:10]
    return f"reviews:{h}"


def read_reviews_csv(path: str) -> IngestResult:
    df = pd.read_csv(path)
    issues: list[ValidationIssue] = []
    issues.extend(require_columns(df, REVIEWS_REQUIRED))
    if any(i.level == "ERROR" for i in issues):
        return IngestResult(df=df, issues=issues, dataset_id=dataset_id_for(path))

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    if df["rating"].isna().any():
        issues.append(ValidationIssue(level="WARN", message="Some ratings are invalid"))
    return IngestResult(df=df, issues=issues, dataset_id=dataset_id_for(path))


# --- Connectors (stubs) ---
@dataclass(frozen=True)
class ReviewsConnectorConfig:
    store: str
    geo: str
    channel: str


class ReviewsConnector:
    """Stub connector.

    In entornos corporativos, el acceso a App Store / Google Play suele requerir
    credenciales, IP allowlists y/o conectores internos. Este stub define el contrato.
    """

    def __init__(self, config: ReviewsConnectorConfig) -> None:
        self.config = config

    def fetch(self, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError(
            "Connector stub: implement fetch() with your corporate connector / API."
        )
