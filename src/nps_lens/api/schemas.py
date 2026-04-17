from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class IssueResponse(BaseModel):
    level: str
    message: str
    column: str | None = None
    code: str = ""
    details: dict[str, Any] = Field(default_factory=dict)


class UploadResponse(BaseModel):
    upload_id: str
    filename: str
    file_hash: str
    uploaded_at: str
    parser_version: str
    status: str
    service_origin: str
    service_origin_n1: str
    service_origin_n2: str
    total_rows: int
    normalized_rows: int
    inserted_rows: int
    updated_rows: int
    duplicate_in_file_rows: int
    duplicate_historical_rows: int
    extra_columns: list[str] = Field(default_factory=list)
    missing_optional_columns: list[str] = Field(default_factory=list)
    issues: list[IssueResponse] = Field(default_factory=list)


class SummaryResponse(BaseModel):
    total_records: int
    date_range: dict[str, str | None]
    overall_nps: float | None = None
    promoter_rate: float | None = None
    detractor_rate: float | None = None
    uploads: int
    duplicates_prevented: int
    top_drivers: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    latest_uploads: list[dict[str, Any]] = Field(default_factory=list)


class ContextOptionsResponse(BaseModel):
    default_service_origin: str
    default_service_origin_n1: str
    service_origins: list[str]
    service_origin_n1_map: dict[str, list[str]]
