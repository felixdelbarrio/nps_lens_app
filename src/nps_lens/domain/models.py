from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nps_lens.ingest.base import ValidationIssue


@dataclass(frozen=True)
class UploadContext:
    service_origin: str
    service_origin_n1: str
    service_origin_n2: str = ""


@dataclass(frozen=True)
class UploadAttempt:
    upload_id: str
    filename: str
    file_hash: str
    uploaded_at: str
    parser_version: str
    context: UploadContext
    status: str
    total_rows: int
    normalized_rows: int
    inserted_rows: int
    updated_rows: int
    duplicate_in_file_rows: int
    duplicate_historical_rows: int
    extra_columns: list[str] = field(default_factory=list)
    missing_optional_columns: list[str] = field(default_factory=list)
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.level == "WARN")

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.level == "ERROR")


@dataclass(frozen=True)
class SummarySnapshot:
    total_records: int
    date_range: dict[str, str | None]
    overall_nps: float | None
    promoter_rate: float | None
    detractor_rate: float | None
    uploads: int
    duplicates_prevented: int
    top_drivers: dict[str, list[dict[str, Any]]]
    latest_uploads: list[dict[str, Any]]
