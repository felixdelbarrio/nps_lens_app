from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, cast

import pandas as pd

from nps_lens.analytics.incident_rationale import (
    build_incident_nps_rationale,
    summarize_incident_nps_rationale,
)
from nps_lens.analytics.journey import build_routes
from nps_lens.analytics.nps_helix_link import (
    build_incident_display_text,
    can_use_daily_resample,
    causal_rank_by_topic,
    daily_aggregates,
    estimate_best_lag_days_by_topic,
    link_incidents_to_nps_topics,
    weekly_aggregates,
)
from nps_lens.analytics.drivers import driver_table
from nps_lens.analytics.opportunities import rank_opportunities
from nps_lens.analytics.text_mining import extract_topics
from nps_lens.core.nps_math import filter_by_nps_group, focus_mask
from nps_lens.core.store import DatasetContext, HelixIncidentStore
from nps_lens.domain.models import UploadContext
from nps_lens.ingest.base import ValidationIssue
from nps_lens.ingest.helix_incidents import read_helix_incidents_excel
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.settings import Settings
from nps_lens.ui.business import (
    default_windows,
    driver_delta_table,
    selected_month_label,
    slice_by_window,
)
from nps_lens.ui.charts import (
    chart_case_incident_heatmap,
    chart_case_lag_days,
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


class DashboardService:
    def __init__(self, repository: SqliteNpsRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.helix_store = HelixIncidentStore(settings.data_dir / "helix")
        self.theme: Theme = get_theme("light")
        self.logger = logging.getLogger(__name__)

    def resolve_context(
        self,
        *,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
    ) -> UploadContext:
        origin = service_origin or self.settings.default_service_origin
        origin_n1 = service_origin_n1 or self.settings.default_service_origin_n1
        return UploadContext(
            service_origin=origin,
            service_origin_n1=origin_n1,
            service_origin_n2=service_origin_n2 or "",
        )

    def context_options(
        self,
        context: UploadContext,
    ) -> dict[str, object]:
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
            "default_service_origin": self.settings.default_service_origin,
            "default_service_origin_n1": self.settings.default_service_origin_n1,
            "service_origins": self.settings.allowed_service_origins,
            "service_origin_n1_map": self.settings.allowed_service_origin_n1,
            "available_years": years,
            "available_months_by_year": months_by_year,
            "nps_groups": _DEFAULT_NPS_GROUPS,
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
    ) -> dict[str, object]:
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
                "figure": self._serialize_figure(chart_driver_delta(delta_df, self.theme)),
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
                "daily_kpis_figure": self._serialize_figure(
                    chart_daily_kpis(current_df, self.theme)
                ),
                "weekly_trend_figure": self._serialize_figure(
                    chart_nps_trend(current_df, self.theme, freq="W")
                ),
                "topics_figure": self._serialize_figure(chart_topic_bars(topics_df, self.theme)),
                "topics_table": self._serialize_rows(topics_df),
                "daily_volume_figure": self._serialize_figure(
                    chart_daily_volume(current_df, self.theme)
                ),
                "daily_mix_figure": self._serialize_figure(
                    chart_daily_mix_business(current_df, self.theme)
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
                        self.theme,
                        row_dim=_COHORT_ROW_DIMENSIONS.get(cohort_row, "Palanca"),
                        col_dim=_COHORT_COLUMN_DIMENSIONS.get(cohort_col, "Canal"),
                        min_n=min_n_cross,
                    )
                ),
            },
            "gaps": {
                "dimension": gap_dimension,
                "figure": self._serialize_figure(chart_driver_bar(gap_stats, self.theme)),
                "table": self._serialize_rows(gap_stats.head(30)),
                "has_data": not gap_stats.empty,
            },
            "opportunities": {
                "dimension": opportunity_dimension,
                "figure": self._serialize_figure(
                    chart_opportunities_bar(opportunities_df, self.theme)
                ),
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
    ) -> dict[str, object]:
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
            return {
                "available": False,
                "context_pills": self._context_pills(context, pop_year, pop_month, nps_group),
                "focus_group": focus_group,
                "focus_label": focus_label,
                "empty_state": (
                    "No hay suficiente base cruzada para analizar incidencias frente a NPS en el "
                    "contexto actual. Carga Helix y revisa el periodo activo."
                ),
                "kpis": {},
                "overview_figure": None,
                "priority_figure": None,
                "risk_recovery_figure": None,
                "heatmap_figure": None,
                "lag_figure": None,
                "ranking_table": [],
                "evidence_table": [],
                "journey_routes_table": [],
                "top_topic": "",
            }

        focus_df = nps_slice.loc[focus_mask(nps_slice, focus_group=focus_group)].copy()
        if focus_df.empty:
            return {
                "available": False,
                "context_pills": self._context_pills(context, pop_year, pop_month, nps_group),
                "focus_group": focus_group,
                "focus_label": focus_label,
                "empty_state": (
                    "El grupo focal seleccionado no tiene suficientes respuestas para construir "
                    "análisis causal con incidencias."
                ),
                "kpis": {},
                "overview_figure": None,
                "priority_figure": None,
                "risk_recovery_figure": None,
                "heatmap_figure": None,
                "lag_figure": None,
                "ranking_table": [],
                "evidence_table": [],
                "journey_routes_table": [],
                "top_topic": "",
            }

        assignments_df, links_df = link_incidents_to_nps_topics(focus_df, helix_slice)
        overall_weekly, by_topic_weekly = weekly_aggregates(
            nps_slice,
            helix_slice,
            assignments_df,
            focus_group=focus_group,
        )
        overall_daily, by_topic_daily = daily_aggregates(
            nps_slice,
            helix_slice,
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
        routes = build_routes(focus_df, incidents_df=helix_slice)
        routes_df = pd.DataFrame(
            [
                {
                    "route_signature": route.route_signature,
                    "n": route.n,
                    "detractor_rate": route.detractor_rate,
                    "score": route.score,
                    "touchpoint": str(route.evidence.get("lever", "")),
                    "subtouchpoint": str(route.evidence.get("sublever", "")),
                    "topic": str(route.evidence.get("topic", "")),
                }
                for route in routes
            ]
        )

        top_topic = ""
        if not rationale_df.empty:
            top_topic = str(rationale_df.iloc[0]["nps_topic"])
        elif not rationale_rank.empty:
            top_topic = str(rationale_rank.iloc[0]["nps_topic"])

        return {
            "available": True,
            "context_pills": self._context_pills(context, pop_year, pop_month, nps_group),
            "focus_group": focus_group,
            "focus_label": focus_label,
            "empty_state": "",
            "kpis": {
                "responses": int(len(nps_slice)),
                "focus_responses": int(len(focus_df)),
                "incidents": int(len(helix_slice)),
                "linked_pairs": int(len(links_df)),
                "topics_analyzed": rationale_summary.topics_analyzed,
                "nps_points_at_risk": rationale_summary.nps_points_at_risk,
                "nps_points_recoverable": rationale_summary.nps_points_recoverable,
                "top3_incident_share": rationale_summary.top3_incident_share,
                "confidence_mean": rationale_summary.confidence_mean,
            },
            "overview_figure": self._serialize_figure(
                self._build_linking_overview_figure(
                    overall_daily if not overall_daily.empty else overall_weekly,
                    focus_label=focus_label,
                )
            ),
            "priority_figure": self._serialize_figure(
                chart_incident_priority_matrix(rationale_df, self.theme)
            ),
            "risk_recovery_figure": self._serialize_figure(
                chart_incident_risk_recovery(rationale_df, self.theme)
            ),
            "heatmap_figure": self._serialize_figure(
                chart_case_incident_heatmap(by_topic_daily, self.theme, topic=top_topic)
            ),
            "lag_figure": self._serialize_figure(
                chart_case_lag_days(
                    by_topic_daily,
                    lag_days,
                    self.theme,
                    topic=top_topic,
                    focus_name=focus_label,
                )
            ),
            "ranking_table": self._serialize_rows(rationale_df.head(20)),
            "evidence_table": self._serialize_rows(
                self._build_linking_evidence_table(focus_df, helix_slice, links_df)
            ),
            "journey_routes_table": self._serialize_rows(routes_df.head(20)),
            "top_topic": top_topic,
        }

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
        return {
            "available": True,
            "rows": int(meta.get("rows", 0) or 0),
            "columns": int(meta.get("cols", 0) or 0),
            "updated_at": meta.get("updated_at_utc"),
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

        evidence = links_df.copy().sort_values("similarity", ascending=False).head(25)
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

    def _build_linking_overview_figure(
        self,
        trend_df: pd.DataFrame,
        *,
        focus_label: str,
    ) -> object:
        if trend_df.empty:
            return None

        import plotly.graph_objects as go

        x_column = "date" if "date" in trend_df.columns else "week"
        chart_df = trend_df.copy().sort_values(x_column)
        chart_df["focus_rate"] = pd.to_numeric(
            chart_df.get("focus_rate"),
            errors="coerce",
        ).fillna(0.0)
        chart_df["incidents"] = pd.to_numeric(
            chart_df.get("incidents"),
            errors="coerce",
        ).fillna(0.0)
        chart_df[x_column] = pd.to_datetime(chart_df[x_column], errors="coerce")
        chart_df = chart_df.dropna(subset=[x_column])
        if chart_df.empty:
            return None

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=chart_df[x_column],
                y=chart_df["focus_rate"],
                mode="lines+markers",
                name=focus_label,
                line=dict(color=self.theme.danger_soft, width=2.5),
                marker=dict(color=self.theme.danger_soft, size=6),
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
                marker=dict(color=self.theme.accent),
                hovertemplate="Incidencias: %{y:.0f}<extra></extra>",
            )
        )
        fig.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=62, b=10),
            paper_bgcolor=self.theme.chart_paper,
            plot_bgcolor=self.theme.chart_plot,
            font=dict(color=self.theme.text),
            legend=dict(orientation="h"),
            yaxis=dict(title=focus_label, tickformat=".0%", gridcolor=self.theme.chart_grid),
            yaxis2=dict(
                title="Incidencias",
                overlaying="y",
                side="right",
                showgrid=False,
            ),
            xaxis=dict(gridcolor=self.theme.chart_grid),
        )
        return apply_plotly_theme(fig, self.theme)

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
