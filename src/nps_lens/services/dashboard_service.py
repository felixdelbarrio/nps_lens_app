from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence, cast

import numpy as np
import pandas as pd

from nps_lens.analytics.drivers import driver_table
from nps_lens.analytics.hotspot_metrics import (
    align_hotspot_evidence_to_axis,
    build_hotspot_evidence,
    build_hotspot_timeline,
    select_best_business_axis_for_hotspots,
)
from nps_lens.analytics.incident_attribution import (
    build_broken_journey_catalog,
    build_causal_topic_map,
    build_incident_attribution_chains,
    load_executive_journey_catalog,
    remap_links_to_causal_entities,
    remap_topic_timeseries_to_causal_entities,
    summarize_attribution_chains,
)
from nps_lens.analytics.incident_rationale import (
    build_incident_nps_rationale,
    summarize_incident_nps_rationale,
)
from nps_lens.analytics.nps_helix_link import (
    build_incident_display_text,
    can_use_daily_resample,
    causal_rank_by_topic,
    daily_aggregates,
    detect_detractor_changepoints_with_bootstrap,
    estimate_best_lag_by_topic,
    estimate_best_lag_days_by_topic,
    incidents_lead_changepoints_flag,
    link_incidents_to_nps_topics,
    weekly_aggregates,
)
from nps_lens.analytics.opportunities import rank_opportunities
from nps_lens.analytics.text_mining import extract_topics
from nps_lens.core.knowledge_cache import (
    load_entries as kc_load_entries,
)
from nps_lens.core.knowledge_cache import (
    score_adjustments as kc_score_adjustments,
)
from nps_lens.core.nps_math import filter_by_nps_group, focus_mask, grouped_focus_rates
from nps_lens.core.store import DatasetContext, HelixIncidentStore
from nps_lens.design.tokens import DesignTokens, cp_level_color, palette
from nps_lens.domain.causal_methods import (
    TOUCHPOINT_SOURCE_BBVA_SOURCE_N2,
    TOUCHPOINT_SOURCE_BROKEN_JOURNEYS,
    TOUCHPOINT_SOURCE_DOMAIN,
    TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS,
    TOUCHPOINT_SOURCE_PALANCA,
    causal_method_options,
    get_causal_method_spec,
    linking_navigation,
)
from nps_lens.domain.models import UploadContext
from nps_lens.ingest.base import ValidationIssue
from nps_lens.ingest.helix_incidents import read_helix_incidents_excel
from nps_lens.reports import BusinessPptResult, generate_business_review_ppt
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.settings import Settings, normalize_downloads_path
from nps_lens.ui.business import (
    default_windows,
    driver_delta_table,
    selected_month_label,
    slice_by_window,
)
from nps_lens.ui.charts import (
    chart_case_incident_heatmap,
    chart_case_lag_days,
    chart_causal_entity_bar,
    chart_cohort_heatmap,
    chart_daily_kpis,
    chart_daily_mix_business,
    chart_daily_volume,
    chart_driver_bar,
    chart_driver_delta,
    chart_incident_priority_matrix,
    chart_incident_risk_recovery,
    chart_nps_trend,
    chart_opportunities_bar,
    chart_topic_bars,
)
from nps_lens.ui.narratives import (
    build_executive_story,
    build_ppt_8slide_script,
    compare_periods,
    executive_summary,
    explain_opportunities,
    explain_topics,
)
from nps_lens.ui.plotly_theme import apply_plotly_theme
from nps_lens.ui.population import MONTH_LABELS_ES, POP_ALL, population_date_window
from nps_lens.ui.theme import Theme, get_theme

_FILENAME_SANITIZER_RE = re.compile(r"[^A-Za-z0-9._-]+")
_MONTH_LABEL_TO_NUMBER = {label: number for number, label in MONTH_LABELS_ES.items()}
_DEFAULT_NPS_GROUPS = [POP_ALL, "Detractores", "Neutros", "Promotores"]
_DEFAULT_DIMENSIONS = ["Palanca", "Subpalanca", "Canal", "UsuarioDecisión"]
_COHORT_ROW_DIMENSIONS = {"Palanca": "Palanca", "Subpalanca": "Subpalanca"}
_COHORT_COLUMN_DIMENSIONS = {
    "Canal": "Canal",
    "Usuario": "UsuarioDecisión",
    "NPSGROUP": "NPS Group",
}


def _unique_string_values(values: Sequence[object]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        unique.append(normalized)
        seen.add(normalized)
    return unique


def _chain_record_ids(value: object, *, field_name: str) -> list[str]:
    if not isinstance(value, list):
        return []
    return _unique_string_values(
        [
            item.get(field_name, "")
            for item in value
            if isinstance(item, dict) and str(item.get(field_name, "")).strip()
        ]
    )


def _series_or_default(frame: pd.DataFrame, column: str, *, default: object = "") -> pd.Series[Any]:
    series = frame.get(column)
    if isinstance(series, pd.Series):
        return series
    return pd.Series([default] * len(frame), index=frame.index)


def _numeric_series(frame: pd.DataFrame, column: str, *, default: float = 0.0) -> pd.Series[Any]:
    return pd.to_numeric(
        _series_or_default(frame, column, default=default), errors="coerce"
    ).fillna(default)


def _annotate_chain_candidates(chain_df: pd.DataFrame) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()

    out = chain_df.copy().reset_index(drop=True)

    def _safe_int_label(value: object) -> int:
        if isinstance(value, (int, np.integer)):
            return int(value)
        if isinstance(value, (float, np.floating)):
            return 0 if np.isnan(float(value)) else int(value)
        if isinstance(value, str):
            try:
                return int(float(value))
            except ValueError:
                return 0
        return 0

    topic = (
        out.get("nps_topic", pd.Series([""] * len(out), index=out.index)).astype(str).str.strip()
    )
    touchpoint = (
        out.get("touchpoint", pd.Series([""] * len(out), index=out.index)).astype(str).str.strip()
    )
    base_keys: list[str] = []
    for _, row in out.iterrows():
        key_payload = {
            "presentation_mode": str(row.get("presentation_mode", "") or "").strip(),
            "nps_topic": str(row.get("nps_topic", "") or "").strip(),
            "touchpoint": str(row.get("touchpoint", "") or "").strip(),
            "palanca": str(row.get("palanca", "") or "").strip(),
            "subpalanca": str(row.get("subpalanca", "") or "").strip(),
            "journey_route": str(row.get("journey_route", "") or "").strip(),
            "linked_pairs": _safe_int_label(row.get("linked_pairs", 0)),
            "linked_incidents": _safe_int_label(row.get("linked_incidents", 0)),
            "linked_comments": _safe_int_label(row.get("linked_comments", 0)),
            "incident_ids": _chain_record_ids(
                row.get("incident_records"), field_name="incident_id"
            ),
            "comment_ids": _chain_record_ids(row.get("comment_records"), field_name="comment_id"),
        }
        base_keys.append(
            hashlib.sha1(
                json.dumps(key_payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
            ).hexdigest()[:12]
        )

    key_counts: dict[str, int] = {}
    chain_keys: list[str] = []
    for base_key in base_keys:
        next_count = key_counts.get(base_key, 0) + 1
        key_counts[base_key] = next_count
        chain_keys.append(base_key if next_count == 1 else f"{base_key}-{next_count}")
    out["chain_key"] = chain_keys
    out["selection_label"] = [
        (
            f"{touchpoint_val or 'Touchpoint sin etiquetar'} | {topic_val or 'Tema sin etiqueta'} | "
            f"{_safe_int_label(inc)} INC | {_safe_int_label(com)} VoC"
        )
        for topic_val, touchpoint_val, inc, com in zip(
            topic.tolist(),
            touchpoint.tolist(),
            out.get("linked_incidents", pd.Series([0] * len(out), index=out.index)).tolist(),
            out.get("linked_comments", pd.Series([0] * len(out), index=out.index)).tolist(),
        )
    ]
    return out


def _select_chain_rows(chain_df: pd.DataFrame, selected_keys: list[str]) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()
    ordered_keys = _unique_string_values(selected_keys)
    if not ordered_keys:
        return chain_df.head(0).copy()

    selected = chain_df[chain_df["chain_key"].astype(str).isin(ordered_keys)].copy()
    if selected.empty:
        return selected

    selected["__order"] = pd.Categorical(
        selected["chain_key"].astype(str),
        categories=ordered_keys,
        ordered=True,
    )
    selected = selected.sort_values("__order").drop(columns="__order").reset_index(drop=True)
    return selected


def _cap_chain_evidence_rows(
    chain_df: pd.DataFrame,
    *,
    max_incident_examples: int = 5,
    max_comment_examples: int = 2,
) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()

    out = chain_df.copy()

    def _normalize_list(value: object) -> list[str]:
        if isinstance(value, list):
            values = value
        elif value in (None, ""):
            values = []
        else:
            values = [value]
        return [str(v).strip() for v in values if str(v).strip()]

    def _cap(values: list[str], limit: int) -> list[str]:
        try:
            max_items = int(limit)
        except Exception:
            return values
        if max_items <= 0:
            return values
        return values[:max_items]

    def _normalize_records(value: object) -> list[dict[str, str]]:
        if isinstance(value, list):
            values = value
        elif value in (None, ""):
            values = []
        else:
            values = [value]
        records: list[dict[str, str]] = []
        for entry in values:
            if not isinstance(entry, dict):
                continue
            records.append({str(k): str(v or "").strip() for k, v in entry.items()})
        return records

    def _cap_records(values: list[dict[str, str]], limit: int) -> list[dict[str, str]]:
        try:
            max_items = int(limit)
        except Exception:
            return values
        if max_items <= 0:
            return values
        return values[:max_items]

    out["incident_examples"] = [
        _cap(_normalize_list(v), max_incident_examples)
        for v in out.get("incident_examples", pd.Series([[]] * len(out), index=out.index)).tolist()
    ]
    out["comment_examples"] = [
        _cap(_normalize_list(v), max_comment_examples)
        for v in out.get("comment_examples", pd.Series([[]] * len(out), index=out.index)).tolist()
    ]
    out["incident_records"] = [
        _cap_records(_normalize_records(v), max_incident_examples)
        for v in out.get("incident_records", pd.Series([[]] * len(out), index=out.index)).tolist()
    ]
    out["comment_records"] = [
        _cap_records(_normalize_records(v), max_comment_examples)
        for v in out.get("comment_records", pd.Series([[]] * len(out), index=out.index)).tolist()
    ]
    return out


class DashboardService:
    def __init__(self, repository: SqliteNpsRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.helix_store = HelixIncidentStore(settings.data_dir / "helix")
        self.logger = logging.getLogger(__name__)

    def resolve_context(
        self,
        *,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
    ) -> UploadContext:
        preferences = self.settings.ui_defaults()
        origin = str(
            service_origin or preferences["service_origin"] or self.settings.default_service_origin
        )
        origin_n1 = str(
            service_origin_n1
            or preferences["service_origin_n1"]
            or self.settings.default_service_origin_n1
        )
        return UploadContext(
            service_origin=origin,
            service_origin_n1=origin_n1,
            service_origin_n2=str(service_origin_n2 or preferences["service_origin_n2"] or ""),
        )

    def context_options(
        self,
        context: UploadContext,
    ) -> dict[str, object]:
        preferences = self.settings.ui_defaults()
        records = self.repository.load_records_df(context)
        years, months_by_year = self._available_periods(records)
        latest_upload = self.repository.list_uploads(limit=1, context=context)
        nps_dataset = {
            "available": not records.empty,
            "rows": int(len(records)),
            "columns": int(len(records.columns)),
            "updated_at": latest_upload[0]["uploaded_at"] if latest_upload else None,
            "status": latest_upload[0]["status"] if latest_upload else "missing",
        }
        helix_dataset = self._helix_dataset_status(context)
        return {
            "default_service_origin": preferences["service_origin"],
            "default_service_origin_n1": preferences["service_origin_n1"],
            "default_service_origin_n2": preferences["service_origin_n2"],
            "service_origins": self.settings.allowed_service_origins,
            "service_origin_n1_map": self.settings.allowed_service_origin_n1,
            "service_origin_n2_values": self.settings.service_origin_n2_values,
            "service_origin_n2_map": self.settings.service_origin_n2_map,
            "service_origin_n2_options": self.settings.service_origin_n2_options(
                context.service_origin,
                context.service_origin_n1,
            ),
            "available_years": years,
            "available_months_by_year": months_by_year,
            "nps_groups": _DEFAULT_NPS_GROUPS,
            "causal_method_options": causal_method_options(),
            "preferences": preferences,
            "nps_dataset": nps_dataset,
            "helix_dataset": helix_dataset,
        }

    def ingest_helix_excel(
        self,
        *,
        filename: str,
        payload: bytes,
        context: UploadContext,
        sheet_name: Optional[str] = None,
    ) -> dict[str, object]:
        upload_id = hashlib.sha256(
            f"{filename}|{datetime.now(timezone.utc).isoformat()}".encode("utf-8")
        ).hexdigest()[:12]
        uploaded_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        stored_path = self._persist_upload_file(
            upload_id=upload_id,
            filename=filename,
            payload=payload,
            folder_name="helix-uploads",
        )
        result = read_helix_incidents_excel(
            str(stored_path),
            service_origin=context.service_origin,
            service_origin_n1=context.service_origin_n1,
            service_origin_n2=context.service_origin_n2,
            sheet_name=sheet_name or None,
        )
        issues = [issue.to_dict() for issue in result.issues]
        has_errors = any(issue.level == "ERROR" for issue in result.issues)
        if not has_errors:
            dataset_context = DatasetContext(
                service_origin=context.service_origin,
                service_origin_n1=context.service_origin_n1,
                service_origin_n2=context.service_origin_n2,
            )
            self.helix_store.save_df(dataset_context, result.df, source=filename)
            self.logger.info(
                "Helix upload processed",
                extra={
                    "upload_id": upload_id,
                    "upload_filename": filename,
                    "row_count": int(len(result.df)),
                },
            )
        else:
            self.logger.warning(
                "Helix upload failed",
                extra={"upload_id": upload_id, "upload_filename": filename},
            )

        return {
            "upload_id": upload_id,
            "filename": Path(filename).name,
            "uploaded_at": uploaded_at,
            "status": "failed" if has_errors else "completed",
            "row_count": int(len(result.df)),
            "column_count": int(len(result.df.columns)),
            "sheet_name": sheet_name or "",
            "issues": issues,
            "dataset": self._helix_dataset_status(context),
        }

    def nps_dashboard(
        self,
        *,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: str = POP_ALL,
        comparison_dimension: str = "Palanca",
        gap_dimension: str = "Palanca",
        opportunity_dimension: str = "Palanca",
        cohort_row: str = "Palanca",
        cohort_col: str = "Canal",
        min_n: int = 200,
        min_n_cross: int = 30,
        theme_mode: str = "light",
    ) -> dict[str, object]:
        theme = get_theme(theme_mode)
        all_records = self.repository.load_records_df(context)
        history_df = filter_by_nps_group(all_records.copy(), nps_group)
        current_df = self._apply_population_filters(history_df.copy(), pop_year, pop_month)
        context_label = selected_month_label(pop_year=pop_year, pop_month=pop_month, df=history_df)

        if current_df.empty:
            return {
                "context_pills": self._context_pills(context, pop_year, pop_month, nps_group),
                "kpis": {
                    "samples": 0,
                    "nps_average": None,
                    "detractor_rate": None,
                    "promoter_rate": None,
                },
                "overview": {},
                "comparison": {},
                "cohorts": {},
                "gaps": {},
                "opportunities": {},
                "report_markdown": "",
                "empty_state": "No hay datos cargados para el contexto y filtros seleccionados.",
            }

        summary = executive_summary(current_df)
        topics_df = self._topics_df(current_df)
        topics_bullets = explain_topics(topics_df, max_items=5)

        comparison_payload: dict[str, object] = {}
        comparison_story = None
        delta_df = pd.DataFrame()
        w_cur, w_base = default_windows(history_df, pop_year=pop_year, pop_month=pop_month)
        if w_cur is not None and w_base is not None:
            cur_window_df = slice_by_window(history_df, w_cur)
            base_window_df = slice_by_window(history_df, w_base)
            comparison_story = compare_periods(cur_window_df, base_window_df)
            delta_df = driver_delta_table(
                cur_window_df,
                base_window_df,
                dimension=comparison_dimension,
                min_n=min_n_cross,
            )
            comparison_payload = {
                "summary": {
                    "label_current": comparison_story.label_current,
                    "label_baseline": comparison_story.label_baseline,
                    "delta_nps": comparison_story.delta_nps,
                    "delta_detr_pp": comparison_story.delta_detr_pp,
                    "n_current": comparison_story.n_current,
                    "n_baseline": comparison_story.n_baseline,
                },
                "dimension": comparison_dimension,
                "figure": self._serialize_figure(chart_driver_delta(delta_df, theme)),
                "table": self._serialize_rows(delta_df.head(30)),
                "has_data": not delta_df.empty,
            }

        gap_stats = pd.DataFrame(
            [stat.__dict__ for stat in driver_table(current_df, gap_dimension)]
        )
        if not gap_stats.empty:
            gap_stats = gap_stats.sort_values("gap_vs_overall", ascending=True)

        opportunities = rank_opportunities(
            current_df,
            dimensions=[opportunity_dimension],
            min_n=min_n,
        )
        opportunities_df = pd.DataFrame([item.__dict__ for item in opportunities])
        if not opportunities_df.empty:
            opportunities_df["label"] = opportunities_df.apply(
                lambda row: f"{row['dimension']}={row['value']}",
                axis=1,
            )
        opportunity_bullets = explain_opportunities(opportunities_df, max_items=5)

        report_markdown = build_executive_story(
            summary,
            comparison=comparison_story,
            top_opportunities=opportunity_bullets,
            top_topics=topics_bullets,
        )

        return {
            "context_label": context_label,
            "context_pills": self._context_pills(context, pop_year, pop_month, nps_group),
            "kpis": {
                "samples": summary.n,
                "nps_average": summary.nps_avg,
                "detractor_rate": summary.detractor_rate,
                "promoter_rate": summary.promoter_rate,
            },
            "overview": {
                "daily_kpis_figure": self._serialize_figure(chart_daily_kpis(current_df, theme)),
                "weekly_trend_figure": self._serialize_figure(
                    chart_nps_trend(current_df, theme, freq="W")
                ),
                "topics_figure": self._serialize_figure(chart_topic_bars(topics_df, theme)),
                "topics_table": self._serialize_rows(topics_df),
                "daily_volume_figure": self._serialize_figure(
                    chart_daily_volume(current_df, theme)
                ),
                "daily_mix_figure": self._serialize_figure(
                    chart_daily_mix_business(current_df, theme)
                ),
                "insight_bullets": topics_bullets,
            },
            "comparison": comparison_payload,
            "cohorts": {
                "row_dimension": cohort_row,
                "column_dimension": cohort_col,
                "figure": self._serialize_figure(
                    chart_cohort_heatmap(
                        current_df,
                        theme,
                        row_dim=_COHORT_ROW_DIMENSIONS.get(cohort_row, "Palanca"),
                        col_dim=_COHORT_COLUMN_DIMENSIONS.get(cohort_col, "Canal"),
                        min_n=min_n_cross,
                    )
                ),
            },
            "gaps": {
                "dimension": gap_dimension,
                "figure": self._serialize_figure(chart_driver_bar(gap_stats, theme)),
                "table": self._serialize_rows(gap_stats.head(30)),
                "has_data": not gap_stats.empty,
            },
            "opportunities": {
                "dimension": opportunity_dimension,
                "figure": self._serialize_figure(chart_opportunities_bar(opportunities_df, theme)),
                "table": self._serialize_rows(opportunities_df.head(25)),
                "bullets": opportunity_bullets,
                "has_data": not opportunities_df.empty,
            },
            "controls": {
                "dimensions": _DEFAULT_DIMENSIONS,
                "cohort_rows": list(_COHORT_ROW_DIMENSIONS.keys()),
                "cohort_columns": list(_COHORT_COLUMN_DIMENSIONS.keys()),
                "min_n": min_n,
                "min_n_cross": min_n_cross,
            },
            "report_markdown": report_markdown,
            "empty_state": "",
        }

    def dataset_rows(
        self,
        *,
        dataset_kind: str,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: str = POP_ALL,
        offset: int = 0,
        limit: int = 100,
    ) -> dict[str, object]:
        if dataset_kind == "helix":
            frame = self._load_helix_df(context)
            frame = self._apply_population_filters(frame, pop_year, pop_month)
        else:
            frame = self.repository.load_records_df(context)
            frame = filter_by_nps_group(frame, nps_group)
            frame = self._apply_population_filters(frame, pop_year, pop_month)

        total_rows = int(len(frame))
        slice_df = frame.iloc[offset : offset + limit].copy()
        return {
            "dataset_kind": dataset_kind,
            "total_rows": total_rows,
            "offset": offset,
            "limit": limit,
            "columns": [str(column) for column in frame.columns.tolist()],
            "rows": self._serialize_rows(slice_df),
            "has_more": offset + len(slice_df) < total_rows,
        }

    def linking_dashboard(
        self,
        *,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: str = POP_ALL,
        min_similarity: float = 0.25,
        max_days_apart: int = 10,
        touchpoint_source: str = "",
        theme_mode: str = "light",
    ) -> dict[str, object]:
        theme = get_theme(theme_mode)
        nps_frame = self.repository.load_records_df(context)
        nps_slice = self._apply_population_filters(
            filter_by_nps_group(nps_frame.copy(), nps_group),
            pop_year,
            pop_month,
        )
        helix_slice = self._apply_population_filters(
            self._load_helix_df(context),
            pop_year,
            pop_month,
        )

        focus_group, focus_label = self._linking_focus_group(nps_group)
        if nps_slice.empty or helix_slice.empty:
            return self._empty_linking_payload(
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=nps_group,
                focus_group=focus_group,
                focus_label=focus_label,
                empty_state=(
                    "No hay suficiente base cruzada para analizar incidencias frente a NPS en el "
                    "contexto actual. Carga Helix y revisa el periodo activo."
                ),
            )

        focus_df = nps_slice.loc[focus_mask(nps_slice, focus_group=focus_group)].copy()
        if focus_df.empty:
            return self._empty_linking_payload(
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=nps_group,
                focus_group=focus_group,
                focus_label=focus_label,
                empty_state=(
                    "El grupo focal seleccionado no tiene suficientes respuestas para construir "
                    "análisis causal con incidencias."
                ),
            )

        active_touchpoint_source = str(
            touchpoint_source
            or self.settings.ui_defaults()["touchpoint_source"]
            or TOUCHPOINT_SOURCE_DOMAIN
        ).strip()
        method_spec = get_causal_method_spec(active_touchpoint_source)
        focus_name = self._focus_name(focus_group)
        core = self._compute_linking_core(
            nps_df=nps_slice,
            helix_df=helix_slice,
            focus_df=focus_df,
            focus_group=focus_group,
            min_similarity=min_similarity,
            max_days_apart=max_days_apart,
        )
        overall_daily = cast(pd.DataFrame, core["overall_daily"])
        overall_weekly = cast(pd.DataFrame, core["overall_weekly"])
        by_topic_weekly = cast(pd.DataFrame, core["by_topic_weekly"])
        by_topic_daily = cast(pd.DataFrame, core["by_topic_daily"])
        links_df = cast(pd.DataFrame, core["links_df"])
        executive_journey_catalog = load_executive_journey_catalog(
            self.settings.knowledge_dir,
            service_origin=context.service_origin,
            service_origin_n1=context.service_origin_n1,
        )
        mode_payload = self._build_touchpoint_mode_payload(
            touchpoint_source=active_touchpoint_source,
            links_df=links_df,
            focus_df=focus_df,
            helix_df=helix_slice,
            by_topic_weekly=by_topic_weekly,
            by_topic_daily=by_topic_daily,
            executive_journey_catalog=executive_journey_catalog,
        )
        broken_journeys_df = mode_payload["broken_journeys_df"]
        broken_journey_links_df = mode_payload["broken_journey_links_df"]
        links_mode_df = mode_payload["links_mode_df"]
        by_topic_weekly_mode = mode_payload["by_topic_weekly_mode"]
        by_topic_daily_mode = mode_payload["by_topic_daily_mode"]
        trend_df = overall_daily if not overall_daily.empty else overall_weekly
        average_focus = float(_numeric_series(trend_df, "focus_rate", default=0.0).mean())
        show_all_groups = str(nps_group or "").strip().lower() == str(POP_ALL).lower()

        rank = causal_rank_by_topic(by_topic_weekly_mode)
        cp_by_topic = detect_detractor_changepoints_with_bootstrap(
            by_topic_weekly_mode,
            pen=6.0,
            n_boot=200,
            block_size=2,
            tol_periods=1,
        )
        lag_by_topic = estimate_best_lag_by_topic(by_topic_weekly_mode, max_lag_weeks=6)
        lead_share = incidents_lead_changepoints_flag(
            by_topic_weekly_mode,
            cp_by_topic,
            window_weeks=4,
        )
        lag_days_mode = (
            estimate_best_lag_days_by_topic(
                by_topic_daily_mode,
                max_lag_days=21,
                min_points=30,
            )
            if can_use_daily_resample(
                overall_daily,
                min_days_with_responses=20,
                min_coverage=0.45,
            )
            else pd.DataFrame()
        )

        kc_entries = kc_load_entries(self.settings.knowledge_dir)
        kc_adj = kc_score_adjustments(
            kc_entries,
            context.service_origin,
            context.service_origin_n1,
            context.service_origin_n2,
        )

        rank2 = pd.DataFrame()
        rank_view = pd.DataFrame()
        top_topic = ""
        if not rank.empty:
            rank2 = (
                rank.merge(cp_by_topic, on="nps_topic", how="left")
                .merge(lag_by_topic, on="nps_topic", how="left")
                .merge(lead_share, on="nps_topic", how="left")
            )
            if not kc_adj.empty:
                rank2 = rank2.merge(kc_adj, on="nps_topic", how="left")
            else:
                rank2["factor"] = 1.0
                rank2["confirmed"] = 0
                rank2["rejected"] = 0
            rank2["factor"] = _numeric_series(rank2, "factor", default=1.0)
            rank2["confirmed"] = _numeric_series(rank2, "confirmed", default=0.0).astype(int)
            rank2["rejected"] = _numeric_series(rank2, "rejected", default=0.0).astype(int)
            rank2["confidence_learned"] = (
                pd.to_numeric(rank2["score"], errors="coerce").fillna(0.0)
                * rank2["factor"].astype(float)
            ).clip(0.0, 1.0)
            rank2 = rank2.sort_values(
                ["confidence_learned", "incidents", "responses"],
                ascending=False,
            ).reset_index(drop=True)
            top_topic = str(rank2.iloc[0]["nps_topic"])

            formatted_rank = rank2.copy()
            formatted_rank["confidence_learned"] = (
                pd.to_numeric(formatted_rank["confidence_learned"], errors="coerce")
                .fillna(0.0)
                .round(3)
            )
            formatted_rank["score"] = (
                pd.to_numeric(formatted_rank["score"], errors="coerce").fillna(0.0).round(3)
            )
            formatted_rank["factor"] = (
                pd.to_numeric(formatted_rank["factor"], errors="coerce").fillna(1.0).round(3)
            )
            formatted_rank["corr"] = _numeric_series(formatted_rank, "corr", default=np.nan).round(
                3
            )
            formatted_rank["max_cp_stability"] = _numeric_series(
                formatted_rank, "max_cp_stability", default=np.nan
            ).round(3)
            formatted_rank["incidents_lead_changepoint_share"] = (
                _numeric_series(
                    formatted_rank,
                    "incidents_lead_changepoint_share",
                    default=np.nan,
                )
                .mul(100.0)
                .round(0)
            )
            formatted_rank["best_lag_weeks"] = _numeric_series(
                formatted_rank, "best_lag_weeks", default=np.nan
            )
            formatted_rank["changepoints"] = formatted_rank.get(
                "changepoints",
                pd.Series([[]] * len(formatted_rank), index=formatted_rank.index),
            ).map(
                lambda value: (
                    "[" + ", ".join([str(item) for item in value]) + "]"
                    if isinstance(value, list) and value
                    else "[]"
                )
            )
            rank_view = formatted_rank[
                [
                    "nps_topic",
                    "confidence_learned",
                    "score",
                    "factor",
                    "confirmed",
                    "rejected",
                    "best_lag_weeks",
                    "corr",
                    "incidents_lead_changepoint_share",
                    "max_cp_level",
                    "max_cp_stability",
                    "changepoints",
                    "incidents",
                ]
            ].rename(
                columns={
                    "nps_topic": "Tópico NPS",
                    "confidence_learned": "Confidence (learned)",
                    "score": "Confidence (raw)",
                    "factor": "Learning factor",
                    "confirmed": "✓ Confirmed",
                    "rejected": "✗ Rejected",
                    "best_lag_weeks": "Lag (semanas)",
                    "corr": "Corr@Lag",
                    "incidents_lead_changepoint_share": "Incidencias→CP (share)",
                    "max_cp_level": "CP Significance",
                    "max_cp_stability": "CP Stability",
                    "changepoints": "Changepoints",
                    "incidents": "Incidencias (asignadas)",
                }
            )

        rationale_rank = rank2 if not rank2.empty else rank
        rationale_df = build_incident_nps_rationale(
            by_topic_weekly_mode,
            focus_group=focus_group,
            rank_df=rationale_rank,
            min_topic_responses=80,
            recovery_factor=0.65,
        )
        rationale_summary = summarize_incident_nps_rationale(rationale_df)
        evidence_df = self._build_linking_evidence_table(
            focus_df,
            helix_slice,
            links_mode_df,
            max_rows=300,
        )
        chain_candidates_df = build_incident_attribution_chains(
            links_mode_df,
            focus_df,
            helix_slice,
            rationale_df=rationale_df,
            top_k=0,
            max_incident_examples=5,
            max_comment_examples=2,
            min_links_per_topic=1,
            touchpoint_source=active_touchpoint_source,
            journey_catalog_df=broken_journeys_df,
            journey_links_df=broken_journey_links_df,
            executive_journey_catalog=executive_journey_catalog,
        )
        chain_candidates_df = _annotate_chain_candidates(chain_candidates_df)
        chain_candidates_summary = summarize_attribution_chains(chain_candidates_df)
        default_chain_keys = _unique_string_values(
            chain_candidates_df.get("chain_key", pd.Series(dtype=str)).astype(str).tolist()
        )[:3]
        chain_cards_df = _cap_chain_evidence_rows(
            (
                _select_chain_rows(chain_candidates_df, default_chain_keys)
                if default_chain_keys
                else chain_candidates_df
            ),
            max_incident_examples=5,
            max_comment_examples=2,
        )
        scenario_cards = self._build_linking_scenario_cards(
            chain_cards_df,
            by_topic_weekly=by_topic_weekly_mode,
            by_topic_daily=by_topic_daily_mode,
            lag_days=lag_days_mode,
            rank_df=rationale_rank,
            theme=theme,
            theme_mode=theme_mode,
            focus_name=focus_name,
            touchpoint_source=active_touchpoint_source,
        )
        entity_summary_df = self._build_entity_summary_df(
            chain_candidates_df,
            touchpoint_source=active_touchpoint_source,
        )
        entity_summary_chart_df = chain_candidates_df.copy()
        if not entity_summary_chart_df.empty:
            entity_summary_chart_df["entity_label"] = (
                _series_or_default(entity_summary_chart_df, "nps_topic").astype(str).str.strip()
            )
        median_lag_weeks = pd.to_numeric(
            pd.Series([rationale_summary.median_lag_weeks]),
            errors="coerce",
        ).iloc[0]
        median_lag_value = float(median_lag_weeks) if pd.notna(median_lag_weeks) else None

        timeline_figure = self._serialize_figure(
            self._build_linking_overview_figure(
                trend_df,
                nps_df=nps_slice,
                focus_label=focus_label,
                focus_name=focus_name,
                show_all_groups=show_all_groups,
                theme=theme,
            )
        )
        situation_notes = [method_spec.situation_note]
        if not show_all_groups and not overall_daily.empty:
            situation_notes.append(
                "La línea principal usa media móvil de 7 días para resaltar tendencia sin perder el detalle diario."
            )

        ranking_rows = self._serialize_rows(rank_view.head(20))
        topic_options = [
            "Todos",
            *_unique_string_values(_series_or_default(rank2, "nps_topic").astype(str).tolist()),
        ]
        evidence_sorted_df = evidence_df.copy()
        if not evidence_sorted_df.empty:
            topic_rank = {
                topic: index
                for index, topic in enumerate(
                    _unique_string_values(
                        _series_or_default(rank2, "nps_topic").astype(str).tolist()
                    )
                )
            }
            evidence_sorted_df["__topic_order"] = evidence_sorted_df.get(
                "nps_topic",
                pd.Series([""] * len(evidence_sorted_df), index=evidence_sorted_df.index),
            ).map(lambda topic: topic_rank.get(str(topic).strip(), len(topic_rank)))
            evidence_sorted_df["similarity"] = pd.to_numeric(
                _series_or_default(evidence_sorted_df, "similarity", default=0.0),
                errors="coerce",
            ).fillna(0.0)
            evidence_sorted_df = evidence_sorted_df.sort_values(
                ["__topic_order", "similarity"],
                ascending=[True, False],
            ).drop(columns="__topic_order")

        deep_dive_rows = self._serialize_rows(evidence_sorted_df)
        return {
            "available": True,
            "context_pills": self._context_pills(context, pop_year, pop_month, nps_group),
            "focus_group": focus_group,
            "focus_label": focus_label,
            "empty_state": "",
            "causal_method": {
                "value": method_spec.value,
                "label": method_spec.label,
                "summary": method_spec.summary,
                "flow": method_spec.flow,
            },
            "navigation": linking_navigation(method_spec),
            "kpis": {
                "responses": int(len(nps_slice)),
                "focus_responses": int(len(focus_df)),
                "incidents": int(len(helix_slice)),
                "linked_pairs": int(len(links_mode_df)),
                "topics_analyzed": int(rationale_summary.topics_analyzed),
                "nps_points_at_risk": float(rationale_summary.nps_points_at_risk),
                "nps_points_recoverable": float(rationale_summary.nps_points_recoverable),
                "top3_incident_share": float(rationale_summary.top3_incident_share),
                "confidence_mean": float(rationale_summary.confidence_mean),
                "average_focus_rate": average_focus,
                "median_lag_weeks": median_lag_value,
            },
            "situation": {
                "title": "Situación del periodo",
                "subtitle": method_spec.situation_subtitle,
                "kpis": [
                    {"label": "Respuestas analizadas", "value": str(int(len(nps_slice)))},
                    {"label": "Incidencias del periodo", "value": str(int(len(helix_slice)))},
                    {
                        "label": "Método causal",
                        "value": method_spec.label,
                        "hint": method_spec.flow,
                    },
                    {"label": f"{focus_label} medio", "value": f"{average_focus*100.0:.1f}%"},
                ],
                "metadata": [
                    {"label": "Flujo causal", "value": method_spec.flow},
                    {"label": "Foco analítico", "value": method_spec.navigation_label},
                ],
                "figure_title": "Timeline causal (diario)",
                "figure": timeline_figure,
                "note": " ".join([note for note in situation_notes if note]),
            },
            "entity_summary": {
                "title": method_spec.navigation_title,
                "subtitle": method_spec.navigation_subtitle,
                "kpis": self._build_entity_summary_kpis(
                    chain_candidates_df,
                    touchpoint_source=active_touchpoint_source,
                ),
                "figure_title": method_spec.chart_title,
                "figure": self._serialize_figure(
                    chart_causal_entity_bar(
                        entity_summary_chart_df,
                        theme=theme,
                        entity_label=method_spec.entity_singular,
                        top_k=(
                            min(10, len(entity_summary_chart_df))
                            if not entity_summary_chart_df.empty
                            else 10
                        ),
                    )
                ),
                "table_title": method_spec.table_title,
                "table": self._serialize_rows(entity_summary_df),
                "empty_state": method_spec.table_empty_message,
            },
            "scenarios": {
                "title": "Análisis de escenarios causales",
                "subtitle": (
                    f"Escenarios priorizados bajo la lectura causal {method_spec.label.lower()}."
                ),
                "banner": {
                    "kicker": "Narrativa causal",
                    "title": (
                        f"{len(chain_candidates_df)} {method_spec.entity_plural.lower()} defendibles para {focus_name}"
                        if not chain_candidates_df.empty
                        else "Sin escenarios defendibles en esta ventana"
                    ),
                    "summary": (
                        f"{method_spec.summary} La política Helix↔VoC está fijada en similitud ≥ "
                        f"{float(min_similarity):.2f}, top-5 por incidencia y ventana de ±{int(max_days_apart)} días."
                    ),
                    "metrics": [
                        {
                            "label": "Método causal",
                            "value": method_spec.label,
                            "hint": "Flujo del método causal: " + method_spec.flow,
                        },
                        {
                            "label": "Incidencias con match",
                            "value": str(chain_candidates_summary["linked_incidents_total"]),
                        },
                        {
                            "label": "Comentarios enlazados",
                            "value": str(chain_candidates_summary["linked_comments_total"]),
                        },
                        {
                            "label": "Links validados",
                            "value": str(chain_candidates_summary["linked_pairs_total"]),
                        },
                    ],
                },
                "pills": [
                    "Solo cadena completa defendible",
                    f"{int(chain_candidates_summary['topics_total'])} focos causales activos",
                    f"{int(chain_candidates_summary['chains_total'])} escenarios priorizados",
                ],
                "cards": scenario_cards,
                "default_chain_keys": default_chain_keys,
            },
            "deep_dive": {
                "title": "NPS deep dive",
                "subtitle": method_spec.deep_dive_subtitle,
                "kpis": [
                    {
                        "label": "NPS en riesgo",
                        "value": f"{float(rationale_summary.nps_points_at_risk):.2f} pts",
                    },
                    {
                        "label": "NPS recuperable",
                        "value": f"{float(rationale_summary.nps_points_recoverable):.2f} pts",
                    },
                    {
                        "label": "Concentración top-3",
                        "value": f"{float(rationale_summary.top3_incident_share)*100.0:.1f}%",
                    },
                    {
                        "label": "Tiempo de reacción",
                        "value": (
                            f"{float(median_lag_value):.1f} semanas"
                            if median_lag_value is not None
                            else "n/d"
                        ),
                    },
                ],
                "topic_filter": {
                    "label": "Tópico",
                    "options": topic_options,
                    "default": top_topic or "Todos",
                },
                "tabs": [
                    {"id": "ranking", "label": "Ranking de hipótesis"},
                    {"id": "evidence", "label": "Evidence wall"},
                    {"id": "analysis", "label": "Data deepdive analysis"},
                ],
                "trending": {
                    "title": "NPS tópicos trending",
                    "figure": self._serialize_figure(
                        self._build_topics_trending_figure(rank2, theme)
                    ),
                    "empty_state": "No hay señal suficiente para construir tópicos trending.",
                },
                "ranking": {
                    "title": "Ranking de hipótesis",
                    "rows": ranking_rows,
                    "empty_state": "No hay suficiente señal para rankear focos causales en el periodo seleccionado.",
                },
                "evidence": {
                    "title": "Evidence wall",
                    "rows": deep_dive_rows,
                    "empty_state": "No hay evidencia validada para el foco seleccionado.",
                },
                "analysis": {
                    "title": "Data deepdive analysis",
                    "rows": deep_dive_rows,
                    "empty_state": "No hay filas de detalle para el foco seleccionado.",
                },
            },
        }

    def generate_ppt_report(
        self,
        *,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: str = POP_ALL,
        min_n: int = 200,
        min_similarity: float = 0.25,
        max_days_apart: int = 10,
        touchpoint_source: str = "",
    ) -> BusinessPptResult:
        history_df = filter_by_nps_group(self.repository.load_records_df(context).copy(), nps_group)
        if history_df.empty:
            raise ValueError("No hay datos NPS para el contexto seleccionado.")

        helix_history = self._load_helix_df(context)
        if helix_history.empty:
            raise ValueError(
                "No hay incidencias Helix cargadas para el contexto actual. La PPT requiere base cruzada."
            )

        current_df = self._apply_population_filters(history_df.copy(), pop_year, pop_month)
        if current_df.empty:
            raise ValueError(
                "El periodo filtrado no tiene respuestas NPS. Ajusta año, mes o grupo antes de generar la PPT."
            )
        helix_current = self._apply_population_filters(helix_history.copy(), pop_year, pop_month)
        if helix_current.empty:
            raise ValueError(
                "El periodo filtrado no tiene incidencias Helix. Ajusta año o mes para generar una PPT alineada con la vista causal."
            )

        focus_group, _ = self._linking_focus_group(nps_group)
        focus_name = self._focus_name(focus_group)
        focus_current = current_df.loc[focus_mask(current_df, focus_group=focus_group)].copy()
        if focus_current.empty:
            raise ValueError(
                "El grupo focal seleccionado no tiene suficientes respuestas para construir el racional causal."
            )

        active_touchpoint_source = str(
            touchpoint_source
            or self.settings.ui_defaults()["touchpoint_source"]
            or TOUCHPOINT_SOURCE_DOMAIN
        ).strip()

        core = self._compute_linking_core(
            nps_df=current_df,
            helix_df=helix_current,
            focus_df=focus_current,
            focus_group=focus_group,
            min_similarity=min_similarity,
            max_days_apart=max_days_apart,
        )
        links_df = cast(pd.DataFrame, core["links_df"])
        if links_df.empty:
            raise ValueError(
                "No se encontraron vínculos defendibles entre Helix y VoC con los umbrales actuales."
            )
        overall_weekly = cast(pd.DataFrame, core["overall_weekly"])
        by_topic_weekly = cast(pd.DataFrame, core["by_topic_weekly"])
        overall_daily = cast(pd.DataFrame, core["overall_daily"])
        by_topic_daily = cast(pd.DataFrame, core["by_topic_daily"])
        overall_daily = self._attach_daily_nps_mean(overall_daily, current_df)

        mode_payload = self._build_touchpoint_mode_payload(
            touchpoint_source=active_touchpoint_source,
            links_df=links_df,
            focus_df=focus_current,
            helix_df=helix_current,
            by_topic_weekly=by_topic_weekly,
            by_topic_daily=by_topic_daily,
            executive_journey_catalog=load_executive_journey_catalog(
                self.settings.knowledge_dir,
                service_origin=context.service_origin,
                service_origin_n1=context.service_origin_n1,
            ),
        )
        broken_journeys_df = mode_payload["broken_journeys_df"]
        broken_journey_links_df = mode_payload["broken_journey_links_df"]
        links_mode_df = mode_payload["links_mode_df"]
        by_topic_weekly_mode = mode_payload["by_topic_weekly_mode"]
        by_topic_daily_mode = mode_payload["by_topic_daily_mode"]

        rank = causal_rank_by_topic(by_topic_weekly_mode)
        changepoints = detect_detractor_changepoints_with_bootstrap(
            by_topic_weekly_mode,
            pen=6.0,
            n_boot=200,
            block_size=2,
            tol_periods=1,
        )
        lag_weeks = estimate_best_lag_by_topic(by_topic_weekly_mode, max_lag_weeks=6)
        lead_share = incidents_lead_changepoints_flag(
            by_topic_weekly_mode,
            changepoints,
            window_weeks=4,
        )
        lag_days = (
            estimate_best_lag_days_by_topic(
                by_topic_daily_mode,
                max_lag_days=21,
                min_points=30,
            )
            if can_use_daily_resample(
                overall_daily,
                min_days_with_responses=20,
                min_coverage=0.45,
            )
            else pd.DataFrame()
        )

        kc_entries = kc_load_entries(self.settings.knowledge_dir)
        kc_adj = kc_score_adjustments(
            kc_entries,
            context.service_origin,
            context.service_origin_n1,
            context.service_origin_n2,
        )
        ranking_df = pd.DataFrame()
        if not rank.empty:
            ranking_df = (
                rank.merge(changepoints, on="nps_topic", how="left")
                .merge(lag_weeks, on="nps_topic", how="left")
                .merge(lead_share, on="nps_topic", how="left")
            )
            if not kc_adj.empty:
                ranking_df = ranking_df.merge(kc_adj, on="nps_topic", how="left")
            else:
                ranking_df["factor"] = 1.0
                ranking_df["confirmed"] = 0
                ranking_df["rejected"] = 0
            ranking_df["factor"] = _numeric_series(ranking_df, "factor", default=1.0)
            ranking_df["confirmed"] = _numeric_series(ranking_df, "confirmed", default=0.0).astype(
                int
            )
            ranking_df["rejected"] = _numeric_series(ranking_df, "rejected", default=0.0).astype(
                int
            )
            ranking_df["confidence_learned"] = (
                pd.to_numeric(ranking_df["score"], errors="coerce").fillna(0.0)
                * ranking_df["factor"].astype(float)
            ).clip(0.0, 1.0)
            ranking_df = ranking_df.sort_values(
                ["confidence_learned", "incidents", "responses"],
                ascending=False,
            ).reset_index(drop=True)

        rationale_rank = ranking_df if not ranking_df.empty else rank
        rationale_df = build_incident_nps_rationale(
            by_topic_weekly_mode,
            focus_group=focus_group,
            rank_df=rationale_rank,
            min_topic_responses=80,
            recovery_factor=0.65,
        )
        if rationale_df.empty:
            raise ValueError(
                "No hay señal suficiente para construir el racional causal con el contexto actual."
            )
        rationale_summary = summarize_incident_nps_rationale(rationale_df)

        executive_journey_catalog = load_executive_journey_catalog(
            self.settings.knowledge_dir,
            service_origin=context.service_origin,
            service_origin_n1=context.service_origin_n1,
        )
        attribution_all_df = build_incident_attribution_chains(
            links_mode_df,
            focus_current,
            helix_current,
            rationale_df=rationale_df,
            top_k=0,
            max_incident_examples=5,
            max_comment_examples=2,
            min_links_per_topic=1,
            touchpoint_source=active_touchpoint_source,
            journey_catalog_df=broken_journeys_df,
            journey_links_df=broken_journey_links_df,
            executive_journey_catalog=executive_journey_catalog,
        )
        entity_summary_kpis = self._build_entity_summary_kpis(
            attribution_all_df,
            touchpoint_source=active_touchpoint_source,
        )
        attribution_summary = summarize_attribution_chains(attribution_all_df)
        attribution_df = self._select_top_chain_rows(attribution_all_df)

        business_story_md = self._build_business_report_md(
            current_df=current_df,
            history_df=history_df,
            pop_year=pop_year,
            pop_month=pop_month,
            min_n=min_n,
        )
        ppt_8slides_md = build_ppt_8slide_script(
            rationale_summary,
            rationale_df,
            attribution_df=attribution_df,
            attribution_summary=attribution_summary,
            touchpoint_source=active_touchpoint_source,
            service_origin=context.service_origin,
            service_origin_n1=context.service_origin_n1,
            focus_name=focus_name,
            period_label=self._period_label(current_df),
            top_k=6,
        )

        incident_evidence_df = build_hotspot_evidence(
            links_mode_df,
            focus_current,
            helix_current,
            system_date=pd.Timestamp.now().date(),
            max_hotspots=10,
            min_validated_similarity=min_similarity,
            max_days_apart=max_days_apart,
        )
        incident_evidence_df, hotspot_focus_note = self._align_evidence_to_best_axis(
            current_df,
            helix_current,
            incident_evidence_df,
        )
        incident_timeline_df = build_hotspot_timeline(
            links_mode_df,
            focus_current,
            helix_current,
            incident_evidence_df=incident_evidence_df,
            max_hotspots=10,
            min_validated_similarity=min_similarity,
            max_days_apart=max_days_apart,
        )

        period_start, period_end = self._period_bounds(current_df)
        overall_series = overall_daily if not overall_daily.empty else overall_weekly

        report = generate_business_review_ppt(
            service_origin=context.service_origin,
            service_origin_n1=context.service_origin_n1,
            service_origin_n2=context.service_origin_n2,
            period_start=period_start,
            period_end=period_end,
            focus_name=focus_name,
            overall_weekly=overall_series,
            rationale_df=rationale_df,
            nps_points_at_risk=float(rationale_summary.nps_points_at_risk),
            nps_points_recoverable=float(rationale_summary.nps_points_recoverable),
            top3_incident_share=float(rationale_summary.top3_incident_share),
            median_lag_weeks=float(rationale_summary.median_lag_weeks),
            story_md=business_story_md,
            script_8slides_md=ppt_8slides_md,
            attribution_df=attribution_df,
            ranking_df=ranking_df,
            by_topic_daily=by_topic_daily_mode,
            lag_days_by_topic=lag_days,
            by_topic_weekly=by_topic_weekly_mode,
            lag_weeks_by_topic=lag_weeks,
            selected_nps_df=current_df,
            comparison_nps_df=history_df,
            incident_evidence_df=incident_evidence_df,
            changepoints_by_topic=changepoints,
            incident_timeline_df=incident_timeline_df,
            hotspot_focus_note=hotspot_focus_note,
            touchpoint_source=active_touchpoint_source,
            entity_summary_df=attribution_all_df,
            entity_summary_kpis=entity_summary_kpis,
            executive_journey_catalog=executive_journey_catalog,
            broken_journeys_df=broken_journeys_df,
        )
        saved_path = self._persist_report_copy(report)
        return BusinessPptResult(
            file_name=report.file_name,
            content=report.content,
            slide_count=report.slide_count,
            saved_path=str(saved_path),
        )

    def _persist_report_copy(self, report: BusinessPptResult) -> Path:
        # Desktop/webview downloads cannot target an arbitrary folder directly, so the API
        # writes the canonical copy server-side into the configured downloads directory.
        preferred_dir = Path(
            normalize_downloads_path(self.settings.ui_defaults()["downloads_path"], create=True)
        )
        fallback_dir = self.settings.data_dir / "reports"
        for target_dir in [preferred_dir, fallback_dir]:
            try:
                target_dir.mkdir(parents=True, exist_ok=True)
                saved_path = target_dir / report.file_name
                saved_path.write_bytes(report.content)
                return saved_path
            except OSError:
                continue
        raise OSError("No se pudo persistir la copia local del reporte generado.")

    def _build_business_report_md(
        self,
        *,
        current_df: pd.DataFrame,
        history_df: pd.DataFrame,
        pop_year: str,
        pop_month: str,
        min_n: int,
    ) -> str:
        summary = executive_summary(current_df)
        topics_df = self._topics_df(current_df)
        topics_bullets = explain_topics(topics_df, max_items=5)
        opportunities_df = pd.DataFrame(
            [
                item.__dict__
                for item in rank_opportunities(current_df, dimensions=["Palanca"], min_n=min_n)
            ]
        )
        opportunity_bullets = explain_opportunities(opportunities_df, max_items=5)
        comparison_story = None
        w_cur, w_base = default_windows(history_df, pop_year=pop_year, pop_month=pop_month)
        if w_cur is not None and w_base is not None:
            comparison_story = compare_periods(
                slice_by_window(history_df, w_cur),
                slice_by_window(history_df, w_base),
            )
        return build_executive_story(
            summary,
            comparison=comparison_story,
            top_opportunities=opportunity_bullets,
            top_topics=topics_bullets,
        )

    @staticmethod
    def _attach_daily_nps_mean(base_df: pd.DataFrame, nps_df: pd.DataFrame) -> pd.DataFrame:
        if base_df.empty or "date" not in base_df.columns:
            return base_df
        source = nps_df.copy()
        if "Fecha" not in source.columns or "NPS" not in source.columns:
            return base_df
        source["Fecha"] = pd.to_datetime(source["Fecha"], errors="coerce")
        source["NPS"] = pd.to_numeric(source["NPS"], errors="coerce")
        source = source.dropna(subset=["Fecha"])
        if source.empty:
            return base_df
        daily_nps = (
            source.assign(date=lambda frame: frame["Fecha"].dt.normalize())
            .groupby("date", as_index=False)
            .agg(nps_mean=("NPS", "mean"))
        )
        output = base_df.copy()
        output["date"] = pd.to_datetime(output["date"], errors="coerce").dt.normalize()
        return output.merge(daily_nps, on="date", how="left")

    @staticmethod
    def _select_top_chain_rows(attribution_df: pd.DataFrame) -> pd.DataFrame:
        if attribution_df.empty:
            return attribution_df
        if "priority" in attribution_df.columns:
            return attribution_df.sort_values("priority", ascending=False).head(3).copy()
        return attribution_df.head(3).copy()

    @staticmethod
    def _period_bounds(frame: pd.DataFrame) -> tuple[date, date]:
        if "Fecha" not in frame.columns:
            raise ValueError("No se pudo resolver la ventana temporal del reporte.")
        dates = pd.to_datetime(frame["Fecha"], errors="coerce").dropna()
        if dates.empty:
            raise ValueError("No se pudo resolver la ventana temporal del reporte.")
        return dates.min().date(), dates.max().date()

    @staticmethod
    def _period_label(frame: pd.DataFrame) -> str:
        start, end = DashboardService._period_bounds(frame)
        return f"{start.isoformat()} -> {end.isoformat()}"

    @staticmethod
    def _focus_name(focus_group: str) -> str:
        if focus_group == "promoter":
            return "promotores"
        if focus_group == "passive":
            return "neutros"
        return "detractores"

    @staticmethod
    def _build_touchpoint_mode_payload(
        *,
        touchpoint_source: str,
        links_df: pd.DataFrame,
        focus_df: pd.DataFrame,
        helix_df: pd.DataFrame,
        by_topic_weekly: pd.DataFrame,
        by_topic_daily: pd.DataFrame,
        executive_journey_catalog: Optional[list[dict[str, object]]] = None,
    ) -> dict[str, pd.DataFrame]:
        broken_journeys_df, broken_journey_links_df = build_broken_journey_catalog(
            links_df,
            focus_df,
            helix_df,
        )
        links_mode_df = links_df.copy()
        by_topic_weekly_mode = by_topic_weekly.copy()
        by_topic_daily_mode = by_topic_daily.copy()
        causal_topic_map_df = build_causal_topic_map(
            links_df,
            focus_df,
            helix_df,
            touchpoint_source=touchpoint_source,
            journey_links_df=broken_journey_links_df,
            executive_journey_catalog=executive_journey_catalog,
        )
        if not causal_topic_map_df.empty:
            links_mode_df = remap_links_to_causal_entities(links_df, causal_topic_map_df)
            by_topic_weekly_mode = remap_topic_timeseries_to_causal_entities(
                by_topic_weekly,
                causal_topic_map_df,
            )
            by_topic_daily_mode = remap_topic_timeseries_to_causal_entities(
                by_topic_daily,
                causal_topic_map_df,
            )

        return {
            "broken_journeys_df": broken_journeys_df,
            "broken_journey_links_df": broken_journey_links_df,
            "causal_topic_map_df": causal_topic_map_df,
            "links_mode_df": links_mode_df,
            "by_topic_weekly_mode": by_topic_weekly_mode,
            "by_topic_daily_mode": by_topic_daily_mode,
        }

    @staticmethod
    def _build_topics_trending_figure(rank_df: pd.DataFrame, theme: Theme) -> object:
        if rank_df is None or rank_df.empty or "confidence_learned" not in rank_df.columns:
            return None

        import plotly.graph_objects as go

        tokens = DesignTokens.default()
        pal = palette(tokens, theme.mode)
        topn = rank_df.head(15).copy()
        if topn.empty:
            return None
        topn["rank"] = np.arange(1, len(topn) + 1)
        topn["topic_label"] = topn.apply(
            lambda row: (
                f"TOP {int(row['rank'])} · {row['nps_topic']}"
                if int(row["rank"]) <= 3
                else str(row["nps_topic"])
            ),
            axis=1,
        )
        topn["topic_label"] = topn["topic_label"].astype(str).str.slice(0, 72)
        topn_plot = topn.iloc[::-1].copy()
        colors: list[str] = []
        for rank in topn_plot["rank"].tolist():
            if int(rank) == 1:
                colors.append(pal["color.primary.bg.alert"])
            elif int(rank) == 2:
                colors.append(pal["color.primary.bg.warning"])
            elif int(rank) == 3:
                colors.append(pal["color.primary.bg.success"])
            else:
                colors.append(
                    pal.get("color.neutral.bg.01", pal.get("color.primary.bg.bar", "#CAD1D8"))
                )

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=topn_plot["confidence_learned"],
                y=topn_plot["topic_label"],
                orientation="h",
                marker=dict(color=colors),
                text=[f"{float(value):.2f}" for value in topn_plot["confidence_learned"].tolist()],
                textposition="outside",
                hovertemplate="Tópico=%{y}<br>confidence learned=%{x:.2f}<extra></extra>",
            )
        )
        fig.update_layout(
            height=440,
            margin=dict(l=10, r=10, t=62, b=10),
            xaxis=dict(range=[0, 1], title="confidence learned"),
            yaxis=dict(title="Tópicos trending"),
        )
        return apply_plotly_theme(fig, theme)

    @staticmethod
    def _build_entity_summary_df(
        chain_df: pd.DataFrame,
        *,
        touchpoint_source: str,
    ) -> pd.DataFrame:
        if chain_df is None or chain_df.empty:
            return pd.DataFrame()

        source = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
        summary_df = chain_df.copy()
        summary_df["entity_label"] = (
            _series_or_default(summary_df, "nps_topic").astype(str).str.strip()
        )
        summary_df["anchor_topic"] = (
            _series_or_default(summary_df, "anchor_topic").astype(str).str.strip()
        )
        summary_df["touchpoint"] = (
            _series_or_default(summary_df, "touchpoint").astype(str).str.strip()
        )
        summary_df["palanca"] = _series_or_default(summary_df, "palanca").astype(str).str.strip()
        summary_df["subpalanca"] = (
            _series_or_default(summary_df, "subpalanca").astype(str).str.strip()
        )
        summary_df["helix_source_service_n2"] = (
            _series_or_default(summary_df, "helix_source_service_n2").astype(str).str.strip()
        )
        summary_df["linked_pairs"] = _numeric_series(
            summary_df, "linked_pairs", default=0.0
        ).astype(int)
        summary_df["linked_incidents"] = _numeric_series(
            summary_df, "linked_incidents", default=0.0
        ).astype(int)
        summary_df["linked_comments"] = _numeric_series(
            summary_df, "linked_comments", default=0.0
        ).astype(int)
        summary_df["avg_nps"] = _numeric_series(summary_df, "avg_nps", default=np.nan).round(2)
        summary_df["confidence"] = _numeric_series(summary_df, "confidence", default=np.nan).round(
            3
        )
        summary_df["nps_points_at_risk"] = _numeric_series(
            summary_df, "nps_points_at_risk", default=0.0
        ).round(2)
        summary_df = summary_df.sort_values(
            ["priority", "linked_pairs", "nps_points_at_risk"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

        if source == TOUCHPOINT_SOURCE_PALANCA:
            return summary_df[
                [
                    "entity_label",
                    "touchpoint",
                    "anchor_topic",
                    "linked_incidents",
                    "linked_comments",
                    "linked_pairs",
                    "avg_nps",
                    "nps_points_at_risk",
                    "confidence",
                    "palanca",
                    "subpalanca",
                ]
            ].rename(
                columns={
                    "entity_label": "Palanca",
                    "touchpoint": "Touchpoint afectado dominante",
                    "anchor_topic": "Tópico NPS ancla",
                    "linked_incidents": "Incidencias",
                    "linked_comments": "Comentarios VoC",
                    "linked_pairs": "Links validados",
                    "avg_nps": "NPS medio",
                    "nps_points_at_risk": "NPS en riesgo (pts)",
                    "confidence": "Confianza",
                    "palanca": "palanca",
                    "subpalanca": "subpalanca",
                }
            )

        if source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2:
            return summary_df[
                [
                    "entity_label",
                    "touchpoint",
                    "palanca",
                    "subpalanca",
                    "anchor_topic",
                    "linked_incidents",
                    "linked_comments",
                    "linked_pairs",
                    "avg_nps",
                    "nps_points_at_risk",
                    "confidence",
                ]
            ].rename(
                columns={
                    "entity_label": "Source Service N2 de Hélix",
                    "touchpoint": "Touchpoint relacionado",
                    "palanca": "Palanca dominante",
                    "subpalanca": "Subpalanca dominante",
                    "anchor_topic": "Tópico NPS ancla",
                    "linked_incidents": "Incidencias",
                    "linked_comments": "Comentarios VoC",
                    "linked_pairs": "Links validados",
                    "avg_nps": "NPS medio",
                    "nps_points_at_risk": "NPS en riesgo (pts)",
                    "confidence": "Confianza",
                }
            )

        if source == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
            return summary_df[
                [
                    "entity_label",
                    "touchpoint",
                    "palanca",
                    "subpalanca",
                    "anchor_topic",
                    "linked_incidents",
                    "linked_comments",
                    "linked_pairs",
                    "avg_nps",
                    "nps_points_at_risk",
                    "confidence",
                ]
            ].rename(
                columns={
                    "entity_label": "Journey roto",
                    "touchpoint": "Touchpoint detectado",
                    "palanca": "Palanca dominante",
                    "subpalanca": "Subpalanca dominante",
                    "anchor_topic": "Tópico NPS ancla",
                    "linked_incidents": "Incidencias",
                    "linked_comments": "Comentarios VoC",
                    "linked_pairs": "Links validados",
                    "avg_nps": "NPS medio",
                    "nps_points_at_risk": "NPS en riesgo (pts)",
                    "confidence": "Confianza",
                }
            )

        if source == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS:
            return summary_df[
                [
                    "entity_label",
                    "touchpoint",
                    "palanca",
                    "subpalanca",
                    "anchor_topic",
                    "linked_incidents",
                    "linked_comments",
                    "linked_pairs",
                    "avg_nps",
                    "nps_points_at_risk",
                    "confidence",
                ]
            ].rename(
                columns={
                    "entity_label": "Journey de detracción",
                    "touchpoint": "Touchpoint del catálogo",
                    "palanca": "Palanca",
                    "subpalanca": "Subpalanca",
                    "anchor_topic": "Tópico NPS ancla",
                    "linked_incidents": "Incidencias",
                    "linked_comments": "Comentarios VoC",
                    "linked_pairs": "Links validados",
                    "avg_nps": "NPS medio",
                    "nps_points_at_risk": "NPS en riesgo (pts)",
                    "confidence": "Confianza",
                }
            )

        return summary_df[
            [
                "entity_label",
                "palanca",
                "anchor_topic",
                "linked_incidents",
                "linked_comments",
                "linked_pairs",
                "avg_nps",
                "nps_points_at_risk",
                "confidence",
            ]
        ].rename(
            columns={
                "entity_label": "Subpalanca",
                "palanca": "Palanca dominante",
                "anchor_topic": "Tópico NPS ancla",
                "linked_incidents": "Incidencias",
                "linked_comments": "Comentarios VoC",
                "linked_pairs": "Links validados",
                "avg_nps": "NPS medio",
                "nps_points_at_risk": "NPS en riesgo (pts)",
                "confidence": "Confianza",
            }
        )

    @staticmethod
    def _build_entity_summary_kpis(
        chain_df: pd.DataFrame,
        *,
        touchpoint_source: str,
    ) -> list[dict[str, str]]:
        if chain_df is None or chain_df.empty:
            return []

        source = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
        entities_total = int(
            chain_df.get("nps_topic", pd.Series(dtype=str)).astype(str).str.strip().ne("").sum()
        )
        touchpoints_total = int(
            chain_df.get("touchpoint", pd.Series(dtype=str))
            .astype(str)
            .str.strip()
            .replace("", np.nan)
            .dropna()
            .nunique()
        )
        incidents_total = int(
            _numeric_series(chain_df, "linked_incidents", default=0.0).fillna(0.0).sum()
        )
        links_total = int(_numeric_series(chain_df, "linked_pairs", default=0.0).fillna(0.0).sum())
        confidence_mean = float(
            _numeric_series(chain_df, "confidence", default=0.0).fillna(0.0).mean()
        )

        if source == TOUCHPOINT_SOURCE_PALANCA:
            return [
                {"label": "Palancas activas", "value": str(entities_total)},
                {"label": "Touchpoints afectados", "value": str(touchpoints_total)},
                {"label": "Links validados", "value": str(links_total)},
            ]
        if source == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
            return [
                {"label": "Journeys rotos", "value": str(entities_total)},
                {"label": "Touchpoints detectados", "value": str(touchpoints_total)},
                {"label": "Links validados", "value": str(links_total)},
            ]
        if source == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS:
            return [
                {"label": "Journeys de detracción", "value": str(entities_total)},
                {"label": "Touchpoints cubiertos", "value": str(touchpoints_total)},
                {"label": "Links validados", "value": str(links_total)},
            ]
        if source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2:
            return [
                {"label": "Source Service N2 activos", "value": str(entities_total)},
                {"label": "Incidencias con match", "value": str(incidents_total)},
                {"label": "Links validados", "value": str(links_total)},
            ]
        return [
            {"label": "Subpalancas activas", "value": str(entities_total)},
            {"label": "Confianza media", "value": f"{confidence_mean:.2f}"},
            {"label": "Links validados", "value": str(links_total)},
        ]

    def _build_linking_scenario_cards(
        self,
        chain_df: pd.DataFrame,
        *,
        by_topic_weekly: pd.DataFrame,
        by_topic_daily: pd.DataFrame,
        lag_days: pd.DataFrame,
        rank_df: pd.DataFrame,
        theme: Theme,
        theme_mode: str,
        focus_name: str,
        touchpoint_source: str,
    ) -> list[dict[str, object]]:
        if chain_df is None or chain_df.empty:
            return []

        source = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
        method_spec = get_causal_method_spec(source)

        def _metric_number(value: object) -> Optional[float]:
            parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
            return float(parsed) if pd.notna(parsed) else None

        cards: list[dict[str, object]] = []
        for index, (_, row) in enumerate(chain_df.reset_index(drop=True).iterrows(), start=1):
            active_df = pd.DataFrame([row]).copy()
            title = str(row.get("nps_topic", "") or "").strip()
            topic = title
            anchor_topic = str(row.get("anchor_topic", "") or title).strip()
            palanca = str(row.get("palanca", "") or "").strip()
            subpalanca = str(row.get("subpalanca", "") or "").strip()
            touchpoint = str(row.get("touchpoint", "") or "").strip()
            source_service_n2 = str(row.get("helix_source_service_n2", "") or "").strip()
            serialized_row = self._serialize_rows(active_df)[0] if not active_df.empty else {}
            if source == TOUCHPOINT_SOURCE_PALANCA:
                flow_steps = [
                    f"({int(float(row.get('linked_incidents', 0) or 0))}) Incidencias Helix",
                    touchpoint or "Touchpoint afectado",
                    title or "Palanca",
                    f"({int(float(row.get('linked_comments', 0) or 0))}) Comentarios VoC",
                    "NPS",
                ]
            elif source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2:
                flow_steps = [
                    f"({int(float(row.get('linked_incidents', 0) or 0))}) Incidencias Helix",
                    source_service_n2 or title or "Source Service N2",
                    f"({int(float(row.get('linked_comments', 0) or 0))}) Comentarios VoC",
                    "NPS",
                ]
            elif source == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
                flow_steps = [
                    f"({int(float(row.get('linked_incidents', 0) or 0))}) Incidencias + comentarios",
                    title or "Journey roto",
                    touchpoint or "Touchpoint detectado",
                    "NPS",
                ]
            elif source == TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS:
                flow_steps = [
                    f"({int(float(row.get('linked_incidents', 0) or 0))}) Incidencias + comentarios",
                    title or "Journey de detracción",
                    " / ".join([value for value in [touchpoint, palanca, subpalanca] if value])
                    or "Touchpoint / Palanca / Subpalanca",
                    "NPS",
                ]
            else:
                flow_steps = [
                    f"({int(float(row.get('linked_incidents', 0) or 0))}) Incidencias Helix",
                    touchpoint or title or "Touchpoint afectado",
                    title or "Subpalanca",
                    f"({int(float(row.get('linked_comments', 0) or 0))}) Comentarios VoC",
                    "NPS",
                ]
            serialized_row.update(
                {
                    "rank": index,
                    "title": title,
                    "statement": str(row.get("chain_story", "") or "").strip(),
                    "flow_steps": flow_steps,
                    "spotlight_metrics": [
                        {"label": method_spec.entity_singular, "value": title or "n/d"},
                        {"label": "Tópico NPS ancla", "value": anchor_topic or "n/d"},
                        {
                            "label": (
                                "Source Service N2"
                                if source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2
                                else "Touchpoint afectado"
                            ),
                            "value": (
                                source_service_n2
                                if source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2
                                else touchpoint or "n/d"
                            ),
                        },
                        {
                            "label": f"Prob. {focus_name}",
                            "value": (
                                f"{float(_metric_number(row.get('detractor_probability')) or 0.0)*100.0:.1f}%"
                                if _metric_number(row.get("detractor_probability")) is not None
                                else "n/d"
                            ),
                        },
                        {
                            "label": "Delta NPS",
                            "value": (
                                f"{float(_metric_number(row.get('nps_delta_expected')) or 0.0):+.1f}"
                                if _metric_number(row.get("nps_delta_expected")) is not None
                                else "n/d"
                            ),
                        },
                        {
                            "label": "Impacto total",
                            "value": f"{float(_metric_number(row.get('total_nps_impact')) or 0.0):.2f} pts",
                        },
                        {
                            "label": "Confianza",
                            "value": f"{float(_metric_number(row.get('confidence')) or 0.0):.2f}",
                        },
                        {
                            "label": "Links validados",
                            "value": str(int(float(row.get("linked_pairs", 0) or 0))),
                        },
                        {
                            "label": "Prioridad",
                            "value": f"{float(_metric_number(row.get('priority')) or 0.0):.2f}",
                        },
                        {
                            "label": "NPS en riesgo",
                            "value": f"{float(_metric_number(row.get('nps_points_at_risk')) or 0.0):.2f} pts",
                        },
                        {
                            "label": "NPS recuperable",
                            "value": f"{float(_metric_number(row.get('nps_points_recoverable')) or 0.0):.2f} pts",
                        },
                        {
                            "label": "Owner",
                            "value": str(row.get("owner_role", "") or "").strip() or "n/d",
                        },
                    ],
                    "matrix_figure": self._serialize_figure(
                        chart_incident_priority_matrix(active_df, theme=theme, top_k=1)
                    ),
                    "risk_recovery_figure": self._serialize_figure(
                        chart_incident_risk_recovery(active_df, theme=theme, top_k=1)
                    ),
                    "detail_table": self._serialize_rows(
                        self._build_linking_detail_table(
                            active_df,
                            focus_name=focus_name,
                            touchpoint_source=source,
                        )
                    ),
                    "heatmap_figure": self._serialize_figure(
                        chart_case_incident_heatmap(by_topic_daily, theme, topic=topic)
                    ),
                    "changepoints_figure": self._serialize_figure(
                        self._build_changepoints_lag_figure(
                            by_topic_weekly,
                            rank_df,
                            topic=topic,
                            theme=theme,
                            theme_mode=theme_mode,
                            focus_name=focus_name,
                        )
                    ),
                    "lag_figure": self._serialize_figure(
                        chart_case_lag_days(
                            by_topic_daily,
                            lag_days,
                            theme,
                            topic=topic,
                            focus_name=focus_name,
                        )
                    ),
                }
            )
            cards.append(serialized_row)
        return cards

    @staticmethod
    def _build_linking_detail_table(
        active_df: pd.DataFrame,
        *,
        focus_name: str,
        touchpoint_source: str,
    ) -> pd.DataFrame:
        show_cols = [
            "nps_topic",
            "anchor_topic",
            "touchpoint",
            "priority",
            "confidence",
            "nps_points_at_risk",
            "nps_points_recoverable",
            "detractor_probability",
            "nps_delta_expected",
            "total_nps_impact",
            "causal_score",
            "delta_focus_rate_pp",
            "incident_rate_per_100_responses",
            "incidents",
            "responses",
            "action_lane",
            "owner_role",
            "eta_weeks",
        ]
        source = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
        entity_label = get_causal_method_spec(source).entity_singular
        touchpoint_label = (
            "Source Service N2"
            if source == TOUCHPOINT_SOURCE_BBVA_SOURCE_N2
            else "Touchpoint afectado"
        )
        detail_df = active_df.copy()
        for column in show_cols:
            if column not in detail_df.columns:
                detail_df[column] = (
                    np.nan
                    if column
                    not in {
                        "action_lane",
                        "owner_role",
                        "nps_topic",
                        "anchor_topic",
                        "touchpoint",
                    }
                    else ""
                )
        detail_df["detractor_probability"] = _numeric_series(
            detail_df, "detractor_probability", default=np.nan
        ).round(3)
        detail_df["priority"] = _numeric_series(detail_df, "priority", default=np.nan).round(3)
        detail_df["confidence"] = _numeric_series(detail_df, "confidence", default=np.nan).round(3)
        detail_df["nps_points_at_risk"] = _numeric_series(
            detail_df, "nps_points_at_risk", default=np.nan
        ).round(2)
        detail_df["nps_points_recoverable"] = _numeric_series(
            detail_df, "nps_points_recoverable", default=np.nan
        ).round(2)
        detail_df["nps_delta_expected"] = _numeric_series(
            detail_df, "nps_delta_expected", default=np.nan
        ).round(2)
        detail_df["total_nps_impact"] = _numeric_series(
            detail_df, "total_nps_impact", default=np.nan
        ).round(2)
        detail_df["causal_score"] = _numeric_series(
            detail_df, "causal_score", default=np.nan
        ).round(3)
        detail_df["delta_focus_rate_pp"] = _numeric_series(
            detail_df, "delta_focus_rate_pp", default=np.nan
        ).round(2)
        detail_df["incident_rate_per_100_responses"] = _numeric_series(
            detail_df, "incident_rate_per_100_responses", default=np.nan
        ).round(2)
        detail_df["incidents"] = _numeric_series(detail_df, "incidents", default=np.nan).round(0)
        detail_df["responses"] = _numeric_series(detail_df, "responses", default=np.nan).round(0)
        detail_df["eta_weeks"] = _numeric_series(detail_df, "eta_weeks", default=np.nan).round(1)
        return detail_df[show_cols].rename(
            columns={
                "nps_topic": entity_label,
                "anchor_topic": "Tópico NPS ancla",
                "touchpoint": touchpoint_label,
                "priority": "Prioridad",
                "confidence": "Confianza",
                "nps_points_at_risk": "NPS en riesgo (pts)",
                "nps_points_recoverable": "NPS recuperable (pts)",
                "detractor_probability": f"Prob. {focus_name} con incidencia",
                "nps_delta_expected": "Delta NPS esperado",
                "total_nps_impact": "Impacto total NPS (pts)",
                "causal_score": "Causal score",
                "delta_focus_rate_pp": f"Δ % {focus_name.capitalize()} (pp)",
                "incident_rate_per_100_responses": "Incidencias por 100 respuestas",
                "incidents": "Incidencias",
                "responses": "Respuestas",
                "action_lane": "Lane de acción",
                "owner_role": "Owner (rol)",
                "eta_weeks": "ETA (semanas)",
            }
        )

    @staticmethod
    def _build_changepoints_lag_figure(
        by_topic_weekly: pd.DataFrame,
        rank_df: pd.DataFrame,
        *,
        topic: str,
        theme: Theme,
        theme_mode: str,
        focus_name: str,
    ) -> object:
        topic_key = str(topic or "").strip()
        if not topic_key or by_topic_weekly.empty or rank_df.empty:
            return None

        g = (
            by_topic_weekly[by_topic_weekly["nps_topic"].astype(str).str.strip() == topic_key]
            .sort_values("week")
            .copy()
        )
        lag_row = rank_df[rank_df["nps_topic"].astype(str).str.strip() == topic_key].head(1)
        if g.empty or lag_row.empty:
            return None

        lag_raw = pd.to_numeric(lag_row["best_lag_weeks"], errors="coerce").iloc[0]
        lag_weeks = int(lag_raw) if pd.notna(lag_raw) else 0
        g["week"] = pd.to_datetime(g["week"], errors="coerce")
        g = g.dropna(subset=["week"])
        if g.empty:
            return None
        g["focus_rate"] = _numeric_series(g, "focus_rate", default=0.0)
        g["incidents"] = _numeric_series(g, "incidents", default=0.0)
        g["incidents_shifted"] = g["incidents"].shift(lag_weeks)

        cps = lag_row.get("changepoints", pd.Series([[]])).iloc[0]
        if not isinstance(cps, list):
            cps = [] if pd.isna(cps) else [str(cps)]
        cp_level = str(lag_row.get("max_cp_level", pd.Series([""])).iloc[0] or "")
        cp_color = cp_level_color(DesignTokens.default(), theme_mode, cp_level)
        pal = palette(DesignTokens.default(), theme_mode)

        import plotly.graph_objects as go

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=g["week"],
                y=g["focus_rate"],
                name=f"% {focus_name}",
                mode="lines+markers",
                line=dict(color=pal["color.primary.accent.value-07.default"], width=2),
                marker=dict(color=pal["color.primary.accent.value-07.default"], size=6),
            )
        )
        fig.add_trace(
            go.Bar(
                x=g["week"],
                y=g["incidents_shifted"],
                name=f"# incidencias (shift {lag_weeks}w)",
                yaxis="y2",
                opacity=0.70,
                marker=dict(color=pal["color.primary.accent.value-01.default"]),
            )
        )
        for cp in cps[:8]:
            with contextlib.suppress(Exception):
                fig.add_vline(
                    x=pd.to_datetime(cp),
                    line_width=2,
                    line_dash="dot",
                    line_color=cp_color,
                )
        fig.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=62, b=10),
            yaxis=dict(title=f"% {focus_name}", tickformat=".0%"),
            yaxis2=dict(title="Incidencias (shifted)", overlaying="y", side="right"),
            legend=dict(orientation="h"),
        )
        return apply_plotly_theme(fig, theme)

    @staticmethod
    def _align_evidence_to_best_axis(
        nps_df: pd.DataFrame,
        helix_df: pd.DataFrame,
        evidence_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, str]:
        if evidence_df.empty:
            return evidence_df, ""
        axis_info = select_best_business_axis_for_hotspots(nps_df, helix_df, min_n=200)
        axis = str(axis_info.get("best_axis", "Palanca"))
        red_map = axis_info.get("red_labels", {})
        labels = list(red_map.get(axis, [])) if isinstance(red_map, dict) else []
        aligned = align_hotspot_evidence_to_axis(
            evidence_df,
            axis=axis,
            red_labels=labels,
            max_hotspots=10,
        )
        ratios = axis_info.get("axis_ratios", {})
        palanca_ratio = float(ratios.get("Palanca", 0.0)) if isinstance(ratios, dict) else 0.0
        subpalanca_ratio = float(ratios.get("Subpalanca", 0.0)) if isinstance(ratios, dict) else 0.0
        note = (
            f"Eje seleccionado para el racional: {axis} "
            f"(cobertura Helix en rojos: Palanca {palanca_ratio*100:.1f}% · "
            f"Subpalanca {subpalanca_ratio*100:.1f}%)."
        )
        return (aligned if not aligned.empty else evidence_df), note

    def _apply_population_filters(
        self,
        frame: pd.DataFrame,
        pop_year: str,
        pop_month: str,
    ) -> pd.DataFrame:
        if frame.empty or "Fecha" not in frame.columns:
            return frame

        result = frame.copy()
        result["Fecha"] = pd.to_datetime(result["Fecha"], errors="coerce")
        start, end, month_filter = population_date_window(pop_year, pop_month)
        if start is not None:
            result = result.loc[result["Fecha"] >= pd.Timestamp(start)]
        if end is not None:
            result = result.loc[result["Fecha"] <= pd.Timestamp(end)]
        if month_filter:
            result = result.loc[result["Fecha"].dt.month == int(str(month_filter).strip().zfill(2))]
        return result

    def _available_periods(self, frame: pd.DataFrame) -> tuple[list[str], dict[str, list[str]]]:
        if frame.empty or "Fecha" not in frame.columns:
            return [POP_ALL], {POP_ALL: [POP_ALL]}

        dates = pd.to_datetime(frame["Fecha"], errors="coerce").dropna()
        if dates.empty:
            return [POP_ALL], {POP_ALL: [POP_ALL]}

        years = sorted({str(int(value.year)) for value in dates.tolist()})
        months_all = sorted({str(int(value.month)).zfill(2) for value in dates.tolist()})
        months_by_year: dict[str, list[str]] = {POP_ALL: [POP_ALL] + months_all}
        for year in years:
            months = sorted(
                {
                    str(int(value.month)).zfill(2)
                    for value in dates.tolist()
                    if int(value.year) == int(year)
                }
            )
            months_by_year[year] = [POP_ALL] + months
        return [POP_ALL] + years, months_by_year

    def _helix_dataset_status(self, context: UploadContext) -> dict[str, object]:
        stored = self.helix_store.get(
            DatasetContext(
                service_origin=context.service_origin,
                service_origin_n1=context.service_origin_n1,
                service_origin_n2=context.service_origin_n2,
            )
        )
        if stored is None:
            return {
                "available": False,
                "rows": 0,
                "columns": 0,
                "updated_at": None,
                "status": "missing",
                "source": None,
            }
        try:
            meta = json.loads(stored.meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
        normalized_updated_at = self._normalize_timestamp(meta.get("updated_at_utc"))
        if normalized_updated_at and normalized_updated_at != meta.get("updated_at_utc"):
            meta["updated_at_utc"] = normalized_updated_at
            with contextlib.suppress(Exception):
                stored.meta_path.write_text(
                    json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
                )
        return {
            "available": True,
            "rows": int(meta.get("rows", 0) or 0),
            "columns": int(meta.get("cols", 0) or 0),
            "updated_at": normalized_updated_at,
            "status": "active",
            "source": meta.get("source"),
        }

    def _load_helix_df(self, context: UploadContext) -> pd.DataFrame:
        stored = self.helix_store.get(
            DatasetContext(
                service_origin=context.service_origin,
                service_origin_n1=context.service_origin_n1,
                service_origin_n2=context.service_origin_n2,
            )
        )
        if stored is None:
            return pd.DataFrame()
        return self.helix_store.load_df(stored)

    def _context_pills(
        self,
        context: UploadContext,
        pop_year: str,
        pop_month: str,
        nps_group: str,
    ) -> list[str]:
        month_label = (
            pop_month
            if pop_month in {POP_ALL, ""}
            else MONTH_LABELS_ES.get(_MONTH_LABEL_TO_NUMBER.get(pop_month, pop_month), pop_month)
        )
        return [
            f"Service origin: {context.service_origin}",
            f"N1: {context.service_origin_n1}",
            f"N2: {context.service_origin_n2 or '-'}",
            f"Año: {pop_year}",
            f"Mes: {month_label or POP_ALL}",
            f"Grupo: {nps_group}",
        ]

    def _topics_df(self, frame: pd.DataFrame) -> pd.DataFrame:
        comment_column = "Comment" if "Comment" in frame.columns else ""
        if not comment_column:
            return pd.DataFrame()
        topics = extract_topics(frame[comment_column].astype(str), n_clusters=10)
        return pd.DataFrame([topic.__dict__ for topic in topics])

    def _build_linking_evidence_table(
        self,
        focus_df: pd.DataFrame,
        helix_df: pd.DataFrame,
        links_df: pd.DataFrame,
        *,
        max_rows: int = 25,
    ) -> pd.DataFrame:
        if links_df.empty:
            return pd.DataFrame()

        focus_copy = focus_df.copy()
        focus_copy["nps_id"] = focus_copy.get("ID", focus_copy.index).astype(str)
        comment_column = "Comment" if "Comment" in focus_copy.columns else "Comentario"
        if comment_column not in focus_copy.columns:
            focus_copy[comment_column] = ""
        helix_copy = helix_df.copy()
        helix_copy["incident_id"] = helix_copy.get(
            "Incident Number",
            helix_copy.get("ID de la Incidencia", helix_copy.index),
        ).astype(str)
        helix_copy["incident_summary"] = build_incident_display_text(helix_copy)

        comment_map = focus_copy.set_index("nps_id")[comment_column].astype(str).fillna("")
        incident_map = (
            helix_copy.set_index("incident_id")["incident_summary"].astype(str).fillna("")
        )

        evidence = links_df.copy().sort_values("similarity", ascending=False).head(int(max_rows))
        evidence["detractor_comment"] = (
            evidence["nps_id"].astype(str).map(comment_map).fillna("").str.slice(0, 220)
        )
        evidence["incident_summary"] = (
            evidence["incident_id"].astype(str).map(incident_map).fillna("").str.slice(0, 220)
        )
        return evidence[
            [
                "nps_topic",
                "similarity",
                "incident_id",
                "incident_summary",
                "nps_id",
                "detractor_comment",
            ]
        ].copy()

    def _compute_linking_core(
        self,
        *,
        nps_df: pd.DataFrame,
        helix_df: pd.DataFrame,
        focus_df: pd.DataFrame,
        focus_group: str,
        min_similarity: float,
        max_days_apart: int,
    ) -> dict[str, object]:
        assignments_df, links_df = link_incidents_to_nps_topics(
            focus_df,
            helix_df,
            min_similarity=min_similarity,
            max_days_apart=max_days_apart,
        )
        overall_weekly, by_topic_weekly = weekly_aggregates(
            nps_df,
            helix_df,
            assignments_df,
            focus_group=focus_group,
        )
        overall_daily, by_topic_daily = daily_aggregates(
            nps_df,
            helix_df,
            assignments_df,
            focus_group=focus_group,
        )
        rationale_rank = causal_rank_by_topic(by_topic_weekly)
        rationale_df = build_incident_nps_rationale(
            by_topic_weekly,
            focus_group=focus_group,
            rank_df=rationale_rank,
            min_topic_responses=80,
            recovery_factor=0.65,
        )
        rationale_summary = summarize_incident_nps_rationale(rationale_df)
        lag_days = (
            estimate_best_lag_days_by_topic(
                by_topic_daily,
                max_lag_days=21,
                min_points=30,
            )
            if can_use_daily_resample(
                overall_daily,
                min_days_with_responses=20,
                min_coverage=0.45,
            )
            else pd.DataFrame()
        )
        evidence_df = self._build_linking_evidence_table(
            focus_df,
            helix_df,
            links_df,
            max_rows=300,
        )
        top_topic = ""
        if not rationale_df.empty:
            top_topic = str(rationale_df.iloc[0]["nps_topic"])
        elif not rationale_rank.empty:
            top_topic = str(rationale_rank.iloc[0]["nps_topic"])
        return {
            "links_df": links_df,
            "overall_weekly": overall_weekly,
            "by_topic_weekly": by_topic_weekly,
            "overall_daily": overall_daily,
            "by_topic_daily": by_topic_daily,
            "rationale_rank": rationale_rank,
            "rationale_df": rationale_df,
            "rationale_summary": rationale_summary,
            "lag_days": lag_days,
            "evidence_df": evidence_df,
            "top_topic": top_topic,
        }

    def _empty_linking_payload(
        self,
        *,
        context: UploadContext,
        pop_year: str,
        pop_month: str,
        nps_group: str,
        focus_group: str,
        focus_label: str,
        empty_state: str,
    ) -> dict[str, object]:
        return {
            "available": False,
            "context_pills": self._context_pills(context, pop_year, pop_month, nps_group),
            "focus_group": focus_group,
            "focus_label": focus_label,
            "empty_state": empty_state,
            "kpis": {},
            "causal_method": {},
            "navigation": [],
            "situation": {},
            "entity_summary": {},
            "scenarios": {},
            "deep_dive": {},
        }

    def _build_linking_overview_figure(
        self,
        trend_df: pd.DataFrame,
        *,
        nps_df: pd.DataFrame,
        focus_label: str,
        focus_name: str,
        show_all_groups: bool,
        theme: Theme,
    ) -> object:
        if trend_df.empty:
            return None

        import plotly.graph_objects as go

        x_column = "date" if "date" in trend_df.columns else "week"
        chart_df = trend_df.copy().sort_values(x_column)
        focus_rate_series = (
            chart_df["focus_rate"]
            if "focus_rate" in chart_df.columns
            else pd.Series([0.0] * len(chart_df), index=chart_df.index)
        )
        incidents_series = (
            chart_df["incidents"]
            if "incidents" in chart_df.columns
            else pd.Series([0.0] * len(chart_df), index=chart_df.index)
        )
        chart_df["focus_rate"] = pd.to_numeric(
            focus_rate_series,
            errors="coerce",
        ).fillna(0.0)
        chart_df["incidents"] = pd.to_numeric(
            incidents_series,
            errors="coerce",
        ).fillna(0.0)
        chart_df[x_column] = pd.to_datetime(chart_df[x_column], errors="coerce")
        chart_df = chart_df.dropna(subset=[x_column])
        if chart_df.empty:
            return None

        fig = go.Figure()
        if show_all_groups:
            group_rates = grouped_focus_rates(
                nps_df,
                frequency="D" if x_column == "date" else "W",
            )
            if not group_rates.empty:
                fig.add_trace(
                    go.Scatter(
                        x=group_rates[x_column],
                        y=group_rates["detractor_rate"],
                        name="% detractores",
                        mode="lines+markers",
                        line=dict(color=theme.danger_soft, width=2),
                        marker=dict(color=theme.danger_soft, size=6),
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=group_rates[x_column],
                        y=group_rates["passive_rate"],
                        name="% pasivos",
                        mode="lines+markers",
                        line=dict(color=theme.warning, width=2),
                        marker=dict(color=theme.warning, size=6),
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=group_rates[x_column],
                        y=group_rates["promoter_rate"],
                        name="% promotores",
                        mode="lines+markers",
                        line=dict(color=theme.success, width=2),
                        marker=dict(color=theme.success, size=6),
                    )
                )
        else:
            if x_column == "date":
                chart_df["focus_rate_smooth"] = (
                    chart_df["focus_rate"].rolling(7, min_periods=1).mean()
                )
                fig.add_trace(
                    go.Scatter(
                        x=chart_df[x_column],
                        y=chart_df["focus_rate"],
                        name=f"% {focus_name} (diario)",
                        mode="lines+markers" if len(chart_df) <= 90 else "lines",
                        line=dict(color=theme.danger_soft, width=1.5),
                        marker=dict(color=theme.danger_soft, size=5),
                        opacity=0.45,
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=chart_df[x_column],
                        y=chart_df["focus_rate_smooth"],
                        name=f"% {focus_name} (media 7d)",
                        mode="lines",
                        line=dict(color=theme.danger_soft, width=3),
                    )
                )
            else:
                fig.add_trace(
                    go.Scatter(
                        x=chart_df[x_column],
                        y=chart_df["focus_rate"],
                        mode="lines+markers",
                        name=focus_label,
                        line=dict(color=theme.danger_soft, width=2.5),
                        marker=dict(color=theme.danger_soft, size=6),
                        hovertemplate=f"{focus_label}: %{{y:.1%}}<extra></extra>",
                    )
                )
        fig.add_trace(
            go.Bar(
                x=chart_df[x_column],
                y=chart_df["incidents"],
                name="Incidencias",
                yaxis="y2",
                opacity=0.72,
                marker=dict(color=theme.accent),
                hovertemplate="Incidencias: %{y:.0f}<extra></extra>",
            )
        )
        fig.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=62, b=10),
            paper_bgcolor=theme.chart_paper,
            plot_bgcolor=theme.chart_plot,
            font=dict(color=theme.text),
            legend=dict(orientation="h"),
            yaxis=dict(
                title="Tasa por grupo" if show_all_groups else focus_label,
                tickformat=".0%",
                gridcolor=theme.chart_grid,
            ),
            yaxis2=dict(
                title="Incidencias",
                overlaying="y",
                side="right",
                showgrid=False,
            ),
            xaxis=dict(gridcolor=theme.chart_grid),
        )
        return apply_plotly_theme(fig, theme)

    @staticmethod
    def _linking_focus_group(nps_group: str) -> tuple[str, str]:
        normalized = str(nps_group or "").strip().lower()
        if normalized == "promotores":
            return "promoter", "% promotores"
        if normalized == "neutros":
            return "passive", "% neutros"
        return "detractor", "% detractores"

    @staticmethod
    def _serialize_figure(figure: object) -> Optional[dict[str, object]]:
        if figure is None:
            return None
        to_json = getattr(figure, "to_json", None)
        if not callable(to_json):
            return None
        return cast(dict[str, object], json.loads(to_json()))

    @staticmethod
    def _serialize_rows(frame: pd.DataFrame) -> list[dict[str, object]]:
        if frame.empty:
            return []
        serialized = frame.copy()
        for column in serialized.columns:
            if pd.api.types.is_datetime64_any_dtype(serialized[column]):
                serialized[column] = (
                    pd.to_datetime(serialized[column], errors="coerce")
                    .dt.strftime("%Y-%m-%dT%H:%M:%S")
                    .where(serialized[column].notna(), None)
                )
        serialized = serialized.where(serialized.notna(), None)
        return [
            {str(key): value for key, value in row.items()}
            for row in serialized.to_dict(orient="records")
        ]

    @staticmethod
    def _normalize_timestamp(value: object) -> Optional[str]:
        raw = str(value or "").strip()
        if not raw:
            return None
        candidates = [raw]
        if raw.endswith("+00:00Z"):
            candidates.append(raw.replace("+00:00Z", "Z"))
        if raw.endswith("Z") and "+" in raw:
            candidates.append(raw[:-1])
        for candidate in candidates:
            parsed = pd.to_datetime(candidate, errors="coerce", utc=True)
            if pd.isna(parsed):
                continue
            return cast(pd.Timestamp, parsed).isoformat().replace("+00:00", "Z")
        return None

    def _persist_upload_file(
        self,
        *,
        upload_id: str,
        filename: str,
        payload: bytes,
        folder_name: str,
    ) -> Path:
        uploads_dir = self.settings.data_dir / folder_name
        uploads_dir.mkdir(parents=True, exist_ok=True)
        safe_name = _FILENAME_SANITIZER_RE.sub("_", Path(filename).name)
        path = uploads_dir / f"{upload_id}__{safe_name}"
        path.write_bytes(payload)
        return path


def serialize_issues(issues: list[ValidationIssue]) -> list[dict[str, object]]:
    return [issue.to_dict() for issue in issues]
