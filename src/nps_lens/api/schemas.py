from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class IssueResponse(BaseModel):
    level: str
    message: str
    column: Optional[str] = None
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
    date_range: dict[str, Optional[str]]
    overall_nps: Optional[float] = None
    promoter_rate: Optional[float] = None
    detractor_rate: Optional[float] = None
    uploads: int
    duplicates_prevented: int
    top_drivers: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    latest_uploads: list[dict[str, Any]] = Field(default_factory=list)


class CausalMethodOption(BaseModel):
    value: str
    label: str
    summary: str
    flow: str


class ContextOptionsResponse(BaseModel):
    default_service_origin: str
    default_service_origin_n1: str
    default_service_origin_n2: str = ""
    service_origins: list[str]
    service_origin_n1_map: dict[str, list[str]]
    service_origin_n2_values: list[str] = Field(default_factory=list)
    service_origin_n2_map: dict[str, dict[str, list[str]]] = Field(default_factory=dict)
    service_origin_n2_options: list[str] = Field(default_factory=list)
    available_years: list[str] = Field(default_factory=list)
    available_months_by_year: dict[str, list[str]] = Field(default_factory=dict)
    nps_groups: list[str] = Field(default_factory=list)
    causal_method_options: list[CausalMethodOption] = Field(default_factory=list)
    preferences: dict[str, Any] = Field(default_factory=dict)
    nps_dataset: dict[str, Any] = Field(default_factory=dict)
    helix_dataset: dict[str, Any] = Field(default_factory=dict)


class PreferencesResponse(BaseModel):
    service_origin: str
    service_origin_n1: str
    service_origin_n2: str = ""
    pop_year: str = "Todos"
    pop_month: str = "Todos"
    nps_group_choice: str = "Todos"
    theme_mode: str = "light"
    downloads_path: str = ""
    touchpoint_source: str = "domain_touchpoint"
    min_similarity: float = 0.25
    max_days_apart: int = 10
    min_n_opportunities: int = 200
    min_n_cross_comparisons: int = 30


class PreferencesUpdateRequest(PreferencesResponse):
    pass


class ServiceOriginHierarchyRequest(BaseModel):
    service_origins: list[str] = Field(default_factory=list)
    service_origin_n1_map: dict[str, list[str]] = Field(default_factory=dict)
    service_origin_n2_map: dict[str, dict[str, list[str]]] = Field(default_factory=dict)


class HelixUploadResponse(BaseModel):
    upload_id: str
    filename: str
    uploaded_at: str
    status: str
    row_count: int
    column_count: int
    sheet_name: str = ""
    issues: list[IssueResponse] = Field(default_factory=list)
    dataset: dict[str, Any] = Field(default_factory=dict)


class DatasetTableResponse(BaseModel):
    dataset_kind: str
    total_rows: int
    offset: int
    limit: int
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    has_more: bool = False


class DashboardResponse(BaseModel):
    context_label: str = ""
    context_pills: list[str] = Field(default_factory=list)
    kpis: dict[str, Any] = Field(default_factory=dict)
    overview: dict[str, Any] = Field(default_factory=dict)
    comparison: dict[str, Any] = Field(default_factory=dict)
    cohorts: dict[str, Any] = Field(default_factory=dict)
    gaps: dict[str, Any] = Field(default_factory=dict)
    opportunities: dict[str, Any] = Field(default_factory=dict)
    controls: dict[str, Any] = Field(default_factory=dict)
    report_markdown: str = ""
    empty_state: str = ""


class LinkingResponse(BaseModel):
    available: bool = False
    context_pills: list[str] = Field(default_factory=list)
    focus_group: str = ""
    focus_label: str = ""
    empty_state: str = ""
    kpis: dict[str, Any] = Field(default_factory=dict)
    touchpoint_mode: dict[str, Any] = Field(default_factory=dict)
    situation: dict[str, Any] = Field(default_factory=dict)
    journeys: dict[str, Any] = Field(default_factory=dict)
    scenarios: dict[str, Any] = Field(default_factory=dict)
    overview_figure: Optional[dict[str, Any]] = None
    priority_figure: Optional[dict[str, Any]] = None
    risk_recovery_figure: Optional[dict[str, Any]] = None
    heatmap_figure: Optional[dict[str, Any]] = None
    lag_figure: Optional[dict[str, Any]] = None
    ranking_table: list[dict[str, Any]] = Field(default_factory=list)
    evidence_table: list[dict[str, Any]] = Field(default_factory=list)
    journey_routes_table: list[dict[str, Any]] = Field(default_factory=list)
    top_topic: str = ""
