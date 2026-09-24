from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import re
from collections import OrderedDict
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Optional, Sequence, cast

import numpy as np
import pandas as pd

from nps_lens.analytics.channel_topic_scope import (
    restrict_to_topics,
    topics_observed_in_channel,
)
from nps_lens.analytics.drivers import compute_nps_from_scores, driver_table
from nps_lens.analytics.helix_operational_metrics import (
    HelixOperationalBenchmark,
    build_helix_operational_benchmark,
    enrich_chain_with_operational_metrics,
    enrich_rationale_with_operational_metrics,
)
from nps_lens.analytics.hotspot_metrics import (
    align_hotspot_evidence_to_axis,
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
from nps_lens.analytics.incident_rationale import build_incident_nps_rationale
from nps_lens.analytics.linking_diagnostics import linking_diagnostics
from nps_lens.analytics.linking_policy import (
    LINK_MAX_VISIBLE_COMMENTS,
    LINK_MAX_VISIBLE_INCIDENTS,
)
from nps_lens.analytics.nps_helix_link import (
    annotate_incident_link_quality,
    build_incident_display_text,
    daily_aggregates,
    link_incidents_to_nps_topics,
    nps_matchable_mask,
    weekly_aggregates,
)
from nps_lens.analytics.text_mining import summarize_taxonomy
from nps_lens.core.nps_math import (
    daily_metrics,
    filter_by_nps_group,
    focus_mask,
    grouped_focus_rates,
)
from nps_lens.core.store import DatasetContext, HelixIncidentStore
from nps_lens.design.tokens import DesignTokens
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
from nps_lens.domain.helix import SOURCE_SERVICE_N1, SOURCE_SERVICE_N2
from nps_lens.domain.helix_links import (
    build_helix_incident_url_lookup,
    enrich_helix_incident_links,
    resolve_helix_incident_url,
)
from nps_lens.domain.models import UploadContext
from nps_lens.domain.normalization import equivalence_key
from nps_lens.domain.publication_scope import build_publication_scope
from nps_lens.domain.record_identity import analytical_response_ids
from nps_lens.ingest.base import ValidationIssue
from nps_lens.ingest.helix_dates import incident_occurrence_dates
from nps_lens.ingest.helix_incidents import read_helix_incidents_excel
from nps_lens.platform.downloads import persist_download
from nps_lens.platform.publication import (
    DATA_PAGE_SIZE,
    PUBLICATION_SCHEMA_VERSION,
    PublicationArtifact,
    build_publication_archive,
    build_static_data_snapshot,
)
from nps_lens.reports import BusinessPptResult, generate_business_review_ppt
from nps_lens.reports.content_selectors import select_causal_scenarios
from nps_lens.reports.executive_newsletter import build_executive_newsletter
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.analytics import (
    build_period_kpis,
    daily_nps_explanation,
    format_metric,
    format_percentage,
)
from nps_lens.services.taxonomy_service import TaxonomyService
from nps_lens.settings import Settings, normalize_downloads_path
from nps_lens.ui.business import (
    PeriodWindow,
    default_windows,
    selected_month_label,
    slice_by_window,
)
from nps_lens.ui.charts import (
    chart_causal_entity_bar,
    chart_cohort_heatmap,
    chart_daily_kpis,
    chart_daily_mix_business,
    chart_daily_volume,
    chart_daily_volume_mix_business,
    chart_driver_bar,
    chart_period_aggregates,
    chart_topic_bars,
)
from nps_lens.ui.narratives import explain_topics
from nps_lens.ui.plotly_theme import apply_plotly_theme
from nps_lens.ui.population import MONTH_LABELS_ES, POP_ALL, population_date_window
from nps_lens.ui.theme import Theme, get_theme

_FILENAME_SANITIZER_RE = re.compile(r"[^A-Za-z0-9._-]+")
_MONTH_LABEL_TO_NUMBER = {label: number for number, label in MONTH_LABELS_ES.items()}
_DEFAULT_NPS_GROUPS = [POP_ALL, "Detractores", "Neutros", "Promotores"]
_DEFAULT_SCORE_CHANNELS = [POP_ALL]
_PREFERRED_SCORE_CHANNEL = "Web"
_PREFERRED_NPS_GROUP = "Detractores"
_DEFAULT_DIMENSIONS = ["Palanca", "Subpalanca"]
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


def _flatten_string_values(value: object) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return _unique_string_values(list(value))
    normalized = str(value or "").strip()
    return [normalized] if normalized else []


def _filter_frame_by_topic_values(
    frame: Optional[pd.DataFrame],
    topics: Sequence[object],
    *,
    column: str = "nps_topic",
) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    if frame.empty:
        return frame.copy()

    normalized_topics = set(_unique_string_values(topics))
    if not normalized_topics or column not in frame.columns:
        return frame.head(0).copy()

    mask = frame[column].astype(str).str.strip().isin(normalized_topics)
    return frame.loc[mask].copy().reset_index(drop=True)


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


def _text_series(frame: pd.DataFrame, column: str, *, default: str = "") -> pd.Series[Any]:
    return _series_or_default(frame, column, default=default).astype(str).fillna("").str.strip()


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

    def _normalize_records(value: object) -> list[dict[str, object]]:
        if isinstance(value, list):
            values = value
        elif value in (None, ""):
            values = []
        else:
            values = [value]
        records: list[dict[str, object]] = []
        for entry in values:
            if not isinstance(entry, dict):
                continue
            records.append(dict(entry))
        return records

    def _cap_records(values: list[dict[str, object]], limit: int) -> list[dict[str, object]]:
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


def _affected_topics_for_method(
    chain_df: Optional[pd.DataFrame],
    topic_map_df: Optional[pd.DataFrame],
) -> list[str]:
    topics: list[str] = []

    if chain_df is not None and not chain_df.empty:
        if "source_topics" in chain_df.columns:
            for value in chain_df["source_topics"].tolist():
                topics.extend(_flatten_string_values(value))
        if "anchor_topic" in chain_df.columns:
            topics.extend(chain_df["anchor_topic"].astype(str).tolist())

    if (
        not topics
        and topic_map_df is not None
        and not topic_map_df.empty
        and "source_nps_topic" in topic_map_df.columns
    ):
        topics.extend(topic_map_df["source_nps_topic"].astype(str).tolist())

    return _unique_string_values(topics)


class DashboardService:
    def __init__(self, repository: SqliteNpsRepository, settings: Settings) -> None:
        self.repository = repository
        self.settings = settings
        self.taxonomy = TaxonomyService(repository, settings.equivalences_path)
        self.helix_store = HelixIncidentStore(settings.data_dir / "helix")
        self._migrate_helix_owner_context()
        self.logger = logging.getLogger(__name__)
        # The analytical routes allocate large pandas/sklearn intermediates.  A single
        # bounded lock/cache prevents identical requests (and exports) from calculating
        # the same dataset concurrently while keeping memory use predictable.
        self._analytics_lock = RLock()
        self._frame_cache: OrderedDict[tuple[object, ...], pd.DataFrame] = OrderedDict()
        self._result_cache: OrderedDict[tuple[object, ...], dict[str, object]] = OrderedDict()
        self._frame_cache_limit = 4
        self._result_cache_limit = 4

    def _migrate_helix_owner_context(self) -> None:
        contexts = self.helix_store.list_contexts()
        owners = sorted(
            {
                context.service_origin
                for context in contexts
                if context.service_origin_n1 or context.service_origin_n2
            }
        )
        for owner in owners:
            owner_contexts = [context for context in contexts if context.service_origin == owner]
            frames = [
                self.helix_store.load_df(stored)
                for context in owner_contexts
                if (stored := self.helix_store.get(context)) is not None
            ]
            if not frames:
                continue
            combined = pd.concat(frames, ignore_index=True, sort=False)
            identity_columns = [
                column
                for column in ("Record ID", "Incident Number", "ID")
                if column in combined.columns
            ]
            combined = (
                combined.drop_duplicates(identity_columns, keep="last")
                if identity_columns
                else combined.drop_duplicates(keep="last")
            )
            canonical = DatasetContext(owner, "", "")
            self.helix_store.save_df(canonical, combined.reset_index(drop=True), "migración")
            for context in owner_contexts:
                if context != canonical:
                    self.helix_store.delete(context)

    @staticmethod
    def _path_revision(path: Path) -> tuple[int, int]:
        try:
            stat = path.stat()
        except OSError:
            return (0, 0)
        return (int(stat.st_mtime_ns), int(stat.st_size))

    @staticmethod
    def _context_key(context: UploadContext) -> tuple[str, str, str]:
        return (context.service_origin, "", "")

    def _data_revision(self, context: UploadContext) -> tuple[object, ...]:
        dataset_context = DatasetContext(*self._context_key(context))
        stored = self.helix_store.get(dataset_context)
        helix_revision = self._path_revision(stored.path) if stored else (0, 0)
        database_revision = (
            self._path_revision(self.repository.db_path),
            self._path_revision(Path(f"{self.repository.db_path}-wal")),
        )
        knowledge_revision = self._path_revision(
            self.settings.knowledge_dir / "knowledge_cache.jsonl"
        )
        return (
            *database_revision,
            helix_revision,
            knowledge_revision,
            self._path_revision(self.settings.equivalences_path),
            self.taxonomy.lens_override,
        )

    @staticmethod
    def _remember_bounded(
        cache: OrderedDict[tuple[object, ...], Any],
        key: tuple[object, ...],
        value: Any,
        limit: int,
    ) -> Any:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > limit:
            cache.popitem(last=False)
        return value

    def clear_caches(self) -> None:
        with self._analytics_lock:
            self._frame_cache.clear()
            self._result_cache.clear()
            self.taxonomy.clear_source_cache()

    def _cached_result(
        self,
        key: tuple[object, ...],
        builder: Callable[[], dict[str, object]],
    ) -> dict[str, object]:
        with self._analytics_lock:
            cached = self._result_cache.get(key)
            if cached is not None:
                self._result_cache.move_to_end(key)
                return cached
            result = builder()
            return cast(
                dict[str, object],
                self._remember_bounded(
                    self._result_cache,
                    key,
                    result,
                    self._result_cache_limit,
                ),
            )

    def _load_nps_df(self, context: UploadContext) -> pd.DataFrame:
        key = ("nps-frame", *self._context_key(context), self._data_revision(context))
        with self._analytics_lock:
            cached = self._frame_cache.get(key)
            if cached is not None:
                self._frame_cache.move_to_end(key)
                return cached
            frame = self.taxonomy.resolve(context)
            frame["match_status"] = nps_matchable_mask(frame).map(
                {True: "matchable", False: "non_matchable"}
            )
            return cast(
                pd.DataFrame,
                self._remember_bounded(
                    self._frame_cache,
                    key,
                    frame,
                    self._frame_cache_limit,
                ),
            )

    def _safe_helix_operational_benchmark(
        self,
        helix_df: pd.DataFrame,
        *,
        context: str,
    ) -> HelixOperationalBenchmark:
        try:
            return build_helix_operational_benchmark(helix_df)
        except Exception as exc:
            self.logger.warning(
                "No se pudo construir el benchmark operativo Helix en %s; se usará payload parcial: %s",
                context,
                exc,
            )
            return HelixOperationalBenchmark({}, {}, None)

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
        return UploadContext(
            service_origin=origin,
            service_origin_n1="",
            service_origin_n2="",
        )

    def context_options(
        self,
        context: UploadContext,
    ) -> dict[str, object]:
        preferences = self.settings.ui_defaults()
        restored = self.taxonomy.state(context).get("restored")
        profile = self.repository.records_profile(context)
        if restored:
            frame = self.taxonomy.source(context)
            preferences.update(restored.get("analysis_config", {}))
            dates = pd.to_datetime(frame["Fecha"], errors="coerce").dropna()
            profile = {
                "rows": len(frame),
                "columns": len(frame.columns),
                "periods": sorted(set(zip(dates.dt.strftime("%Y"), dates.dt.strftime("%m")))),
                "score_channels": self.taxonomy.resolve(context, frame)["Canal"]
                .drop_duplicates()
                .tolist(),
            }

        periods = cast(list[tuple[str, str]], profile["periods"])
        concrete_years = sorted({year for year, _month in periods})
        all_months = sorted({month for _year, month in periods})
        years = [POP_ALL, *concrete_years]
        months_by_year = {POP_ALL: [POP_ALL, *all_months]}
        for year in concrete_years:
            months_by_year[year] = [
                POP_ALL,
                *sorted(month for period_year, month in periods if period_year == year),
            ]
        stored_helix = self.helix_store.get(DatasetContext(*self._context_key(context)))
        helix_periods = self.helix_store.available_periods(stored_helix) if stored_helix else []
        causal_default_year, causal_default_month = self._latest_common_period_values(
            periods, helix_periods
        )
        score_channels = [POP_ALL, *cast(list[str], profile["score_channels"])]
        latest_upload = self.repository.list_uploads(limit=1, context=context)
        nps_rows = cast(int, profile["rows"])
        nps_columns = cast(int, profile["columns"])
        nps_dataset = {
            "available": bool(nps_rows),
            "rows": nps_rows,
            "columns": nps_columns if nps_rows else 0,
            "updated_at": latest_upload[0]["uploaded_at"] if latest_upload else None,
            "status": latest_upload[0]["status"] if latest_upload else "missing",
        }
        helix_dataset = self._helix_dataset_status(context)
        channels_by_owner = self.repository.channels_by_owner(self.settings.allowed_service_origins)
        return {
            "default_service_origin": preferences["service_origin"],
            "default_service_origin_n1": "",
            "default_service_origin_n2": "",
            "service_origins": self.settings.allowed_service_origins,
            "service_origin_n1_map": channels_by_owner,
            "service_origin_n2_map": self.settings.service_origin_n2_map,
            "service_origin_n2_options": self.settings.service_origin_n2_options(
                context.service_origin,
                context.service_origin_n1,
            ),
            "available_years": years,
            "available_months_by_year": months_by_year,
            "causal_default_year": causal_default_year,
            "causal_default_month": causal_default_month,
            "nps_groups": _DEFAULT_NPS_GROUPS,
            "score_channels": score_channels,
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
                service_origin_n1="",
                service_origin_n2="",
            )
            snapshots_dir = self.settings.data_dir / "helix" / "uploads"
            snapshots_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path = snapshots_dir / f"{upload_id}.jsonl"
            result.df.to_json(
                snapshot_path,
                orient="records",
                lines=True,
                force_ascii=False,
                date_format="iso",
            )
            history = self._helix_upload_history()
            history.append(
                {
                    "upload_id": upload_id,
                    "filename": Path(filename).name,
                    "uploaded_at": uploaded_at,
                    "service_origin": context.service_origin,
                    "row_count": int(len(result.df)),
                    "column_count": int(len(result.df.columns)),
                    "sheet_name": sheet_name or "",
                    "snapshot": str(snapshot_path),
                }
            )
            self._write_helix_upload_history(history)
            self._rebuild_helix_dataset(dataset_context)
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

    def _helix_history_path(self) -> Path:
        return self.settings.data_dir / "helix" / "uploads.json"

    def _helix_upload_history(self) -> list[dict[str, object]]:
        try:
            payload = json.loads(self._helix_history_path().read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return []
        return (
            [dict(item) for item in payload if isinstance(item, dict)]
            if isinstance(payload, list)
            else []
        )

    def _write_helix_upload_history(self, history: list[dict[str, object]]) -> None:
        path = self._helix_history_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")

    def list_helix_uploads(self, owner_support_company: str) -> list[dict[str, object]]:
        return sorted(
            [
                item
                for item in self._helix_upload_history()
                if str(item.get("service_origin", "")) == owner_support_company
            ],
            key=lambda item: str(item.get("uploaded_at", "")),
            reverse=True,
        )

    def _rebuild_helix_dataset(self, context: DatasetContext) -> None:
        frames: list[pd.DataFrame] = []
        for item in self.list_helix_uploads(context.service_origin):
            snapshot = Path(str(item.get("snapshot", "")))
            if snapshot.is_file():
                with contextlib.suppress(ValueError):
                    frames.append(pd.read_json(snapshot, orient="records", lines=True, dtype=False))
        if not frames:
            self.helix_store.delete(context)
            return
        combined = pd.concat(frames, ignore_index=True, sort=False)
        identity_columns = [
            column
            for column in ("Record ID", "Incident Number", "ID")
            if column in combined.columns
        ]
        combined = (
            combined.drop_duplicates(identity_columns, keep="last")
            if identity_columns
            else combined.drop_duplicates(keep="last")
        )
        self.helix_store.save_df(context, combined.reset_index(drop=True), source="histórico Helix")

    def delete_helix_upload(self, upload_id: str, owner_support_company: str) -> dict[str, int]:
        history = self._helix_upload_history()
        target = next(
            (
                item
                for item in history
                if str(item.get("upload_id", "")) == upload_id
                and str(item.get("service_origin", "")) == owner_support_company
            ),
            None,
        )
        if target is None:
            raise ValueError(
                "La ingesta de incidencias no existe para el Owner Support Company activo."
            )
        Path(str(target.get("snapshot", ""))).unlink(missing_ok=True)
        self._write_helix_upload_history([item for item in history if item is not target])
        self._rebuild_helix_dataset(DatasetContext(owner_support_company, "", ""))
        self.clear_caches()
        return {
            "removed_uploads": 1,
            "removed_records": int(str(target.get("row_count", 0) or 0)),
        }

    def delete_owner_helix_data(self, owner_support_company: str) -> dict[str, int]:
        history = self._helix_upload_history()
        targets = [
            item for item in history if str(item.get("service_origin", "")) == owner_support_company
        ]
        for item in targets:
            Path(str(item.get("snapshot", ""))).unlink(missing_ok=True)
        self._write_helix_upload_history([item for item in history if item not in targets])
        self.helix_store.delete(DatasetContext(owner_support_company, "", ""))
        self.clear_caches()
        return {
            "removed_uploads": len(targets),
            "removed_records": sum(int(str(item.get("row_count", 0) or 0)) for item in targets),
        }

    def nps_dashboard(
        self,
        *,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        gap_dimension: str = "Palanca",
        cohort_row: str = "Palanca",
        cohort_col: str = "Canal",
        min_n: int = 200,
        min_n_cross: int = 30,
        theme_mode: str = "light",
    ) -> dict[str, object]:
        key = (
            "nps-dashboard",
            *self._context_key(context),
            self._data_revision(context),
            pop_year,
            pop_month,
            nps_group,
            score_channel,
            gap_dimension,
            cohort_row,
            cohort_col,
            min_n,
            min_n_cross,
            theme_mode,
        )
        return self._cached_result(
            key,
            lambda: self._build_nps_dashboard(
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=nps_group,
                score_channel=score_channel,
                gap_dimension=gap_dimension,
                cohort_row=cohort_row,
                cohort_col=cohort_col,
                min_n=min_n,
                min_n_cross=min_n_cross,
                theme_mode=theme_mode,
            ),
        )

    def _build_nps_dashboard(
        self,
        *,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        gap_dimension: str = "Palanca",
        cohort_row: str = "Palanca",
        cohort_col: str = "Canal",
        min_n: int = 200,
        min_n_cross: int = 30,
        theme_mode: str = "light",
    ) -> dict[str, object]:
        theme = get_theme(theme_mode)
        all_records = self._load_nps_df(context)
        resolved_channel = self._resolve_score_channel(all_records, score_channel)
        resolved_group = self._resolve_nps_group(all_records, nps_group)
        scope_history_df = all_records
        scope_current_df = self._apply_population_filters(scope_history_df, pop_year, pop_month)
        channel_history_df = self._apply_score_channel_filter(scope_history_df, resolved_channel)
        analysis_history_df = filter_by_nps_group(channel_history_df, resolved_group)
        analysis_current_df = self._apply_population_filters(
            analysis_history_df,
            pop_year,
            pop_month,
        )
        context_label = selected_month_label(
            pop_year=pop_year,
            pop_month=pop_month,
            df=scope_history_df,
        )
        scope_kpis = build_period_kpis(
            history_df=scope_history_df,
            current_df=scope_current_df,
            pop_year=pop_year,
            pop_month=pop_month,
            context_label=context_label,
        )

        if scope_current_df.empty:
            return {
                "context_pills": self._context_pills(
                    context,
                    pop_year,
                    pop_month,
                    resolved_group,
                    resolved_channel,
                ),
                "kpis": {
                    "samples": 0,
                    "nps_average": None,
                    "classic_nps": None,
                    "detractor_rate": None,
                    "neutral_rate": None,
                    "promoter_rate": None,
                    "comments": 0,
                },
                "scope": scope_kpis,
                "overview": {},
                "cohorts": {},
                "gaps": {},
                "empty_state": "No hay datos cargados para el contexto y filtros seleccionados.",
            }

        period_scope = cast(dict[str, Any], scope_kpis.get("period", {}))
        period_scope_kpis = cast(dict[str, Any], period_scope.get("kpis", {}))
        period_aggregates = cast(list[dict[str, object]], scope_kpis.get("period_aggregates", []))
        scope_daily_metrics = daily_metrics(scope_current_df, days=60)
        topics_df = self._topics_df(analysis_current_df)
        if not topics_df.empty:
            topics_df = topics_df.sort_values(
                ["n", "cluster_id"], ascending=[False, True]
            ).reset_index(drop=True)
        topics_bullets = explain_topics(topics_df, max_items=5)

        topic_keys = topics_observed_in_channel(
            scope_current_df,
            gap_dimension,
            resolved_channel,
        )
        gap_current_df = restrict_to_topics(scope_current_df, gap_dimension, topic_keys)
        _, gap_base_window = default_windows(
            scope_history_df,
            pop_year=pop_year,
            pop_month=pop_month,
        )
        gap_base_df = (
            slice_by_window(scope_history_df, gap_base_window)
            if gap_base_window is not None
            else scope_history_df.iloc[:0]
        )

        nps_explanation_bullets = daily_nps_explanation(period_scope)

        return {
            "context_label": context_label,
            "context_pills": self._context_pills(
                context,
                pop_year,
                pop_month,
                resolved_group,
                resolved_channel,
            ),
            "kpis": period_scope_kpis,
            "scope": scope_kpis,
            "overview": {
                "daily_kpis_figure": self._serialize_figure(
                    chart_daily_kpis(scope_current_df, theme, metrics=scope_daily_metrics)
                ),
                "period_aggregates_figure": self._serialize_figure(
                    chart_period_aggregates(period_aggregates, theme)
                ),
                "topics_figure": self._serialize_figure(chart_topic_bars(topics_df, theme)),
                "topics_table": self._serialize_rows(topics_df),
                "daily_volume_figure": self._serialize_figure(
                    chart_daily_volume(scope_current_df, theme, metrics=scope_daily_metrics)
                ),
                "daily_volume_mix_figure": self._serialize_figure(
                    chart_daily_volume_mix_business(
                        scope_current_df,
                        theme,
                        metrics=scope_daily_metrics,
                    )
                ),
                "daily_mix_figure": self._serialize_figure(
                    chart_daily_mix_business(scope_current_df, theme, metrics=scope_daily_metrics)
                ),
                "daily_explanation_bullets": nps_explanation_bullets,
                "insight_bullets": topics_bullets,
            },
            "cohorts": {
                "row_dimension": cohort_row,
                "column_dimension": cohort_col,
                "figure": self._serialize_figure(
                    chart_cohort_heatmap(
                        scope_current_df,
                        theme,
                        row_dim=_COHORT_ROW_DIMENSIONS.get(cohort_row, "Palanca"),
                        col_dim=_COHORT_COLUMN_DIMENSIONS.get(cohort_col, "Canal"),
                        min_n=min_n_cross,
                    )
                ),
            },
            "gaps": self._build_gap_payload(
                gap_current_df,
                gap_base_df,
                gap_dimension,
                theme,
                base_window=gap_base_window,
            ),
            "controls": {
                "dimensions": _DEFAULT_DIMENSIONS,
                "cohort_rows": list(_COHORT_ROW_DIMENSIONS.keys()),
                "cohort_columns": list(_COHORT_COLUMN_DIMENSIONS.keys()),
                "min_n": min_n,
                "min_n_cross": min_n_cross,
            },
            "empty_state": "",
        }

    def _build_gap_payload(
        self,
        current_df: pd.DataFrame,
        base_df: pd.DataFrame,
        dimension: str,
        theme: Theme,
        *,
        base_window: PeriodWindow | None = None,
    ) -> dict[str, object]:
        base_value = compute_nps_from_scores(base_df["NPS"]) if not base_df.empty else float("nan")
        base_nps = float(base_value) if np.isfinite(base_value) else None
        base_label = selected_month_label(df=base_df).replace(
            "periodo seleccionado", "sin histórico"
        )
        base_date_values = (
            base_df["Fecha"] if "Fecha" in base_df.columns else pd.Series(dtype="datetime64[ns]")
        )
        base_dates = pd.to_datetime(base_date_values, errors="coerce").dropna()
        base_range: dict[str, Optional[str]] = (
            {
                "start": base_dates.min().date().isoformat(),
                "end": (
                    base_window.end.isoformat()
                    if base_window is not None
                    else base_dates.max().date().isoformat()
                ),
            }
            if not base_dates.empty
            else {"start": None, "end": None}
        )
        stats = pd.DataFrame(
            [item.__dict__ for item in driver_table(current_df, dimension, base_nps=base_nps)]
            if base_nps is not None
            else []
        )
        if not stats.empty:
            stats = stats.sort_values(["gap_vs_base", "n"], ascending=[True, False]).reset_index(
                drop=True
            )
        return {
            "dimension": dimension,
            "base_nps": base_nps,
            "base_label": base_label,
            "base_range": base_range,
            "gap_column_label": f"Brecha vs Base [{base_label}]",
            "title": "Brechas NPS",
            "subtitle": (
                "El canal selecciona tópicos; sus NPS y la base usan todas las opiniones."
            ),
            "figure": self._serialize_figure(chart_driver_bar(stats, theme, base_label=base_label)),
            "table": self._serialize_rows(stats.head(30)),
            "has_data": not stats.empty,
        }

    def _build_comments_snapshot(
        self,
        *,
        history_df: pd.DataFrame,
        pop_year: str,
        pop_month: str,
        theme_mode: str = "light",
    ) -> dict[str, object]:
        """Materialize only the three comment views used by the static WebApp."""
        theme = get_theme(theme_mode)
        channels = self._available_score_channels(history_df)
        groups = [
            group
            for group in _DEFAULT_NPS_GROUPS
            if group == POP_ALL or not filter_by_nps_group(history_df, group).empty
        ]
        dimensions = [
            dimension for dimension in _DEFAULT_DIMENSIONS if dimension in history_df.columns
        ]
        if "Palanca" not in dimensions:
            dimensions.insert(0, "Palanca")

        topics: dict[str, dict[str, object]] = {}
        gaps: dict[str, dict[str, object]] = {}
        metric_current = self._apply_population_filters(history_df, pop_year, pop_month)
        _, gap_base_window = default_windows(
            history_df,
            pop_year=pop_year,
            pop_month=pop_month,
        )
        metric_base = (
            slice_by_window(history_df, gap_base_window)
            if gap_base_window is not None
            else history_df.iloc[:0]
        )
        for channel in channels:
            channel_history = self._apply_score_channel_filter(history_df, channel)
            topics[channel] = {}
            gaps[channel] = {}
            for dimension in dimensions:
                topic_keys = topics_observed_in_channel(
                    metric_current,
                    dimension,
                    channel,
                )
                gaps[channel][dimension] = self._build_gap_payload(
                    restrict_to_topics(metric_current, dimension, topic_keys),
                    metric_base,
                    dimension,
                    theme,
                    base_window=gap_base_window,
                )
            for group in groups:
                analysis_history = filter_by_nps_group(channel_history, group)
                current = self._apply_population_filters(analysis_history, pop_year, pop_month)
                topics_df = self._topics_df(current)
                if not topics_df.empty:
                    topics_df = topics_df.sort_values(
                        ["n", "cluster_id"], ascending=[False, True]
                    ).reset_index(drop=True)
                topics[channel][group] = {
                    "figure": self._serialize_figure(chart_topic_bars(topics_df, theme)),
                    "rows": self._serialize_rows(topics_df),
                    "insights": explain_topics(topics_df, max_items=5),
                }
        return {
            "controls": {
                "channels": channels,
                "groups": groups,
                "dimensions": dimensions,
                "defaults": {
                    "channel": _PREFERRED_SCORE_CHANNEL,
                    "group": _PREFERRED_NPS_GROUP,
                    "dimension": "Palanca",
                },
            },
            "topics": topics,
            "gaps": gaps,
        }

    def dataset_rows(
        self,
        *,
        dataset_kind: str,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> dict[str, object]:
        if dataset_kind == "helix":
            # Helix is a contextual historical dataset. NPS period/channel controls must not
            # hide its rows; the causal engine applies its own explicit temporal window.
            frame = annotate_incident_link_quality(
                self._enrich_helix_links(self._load_helix_df(context))
            )
        else:
            frame = self._load_nps_df(context)
            resolved_channel = self._resolve_score_channel(frame, score_channel)
            resolved_group = self._resolve_nps_group(frame, nps_group)
            frame = self._apply_score_channel_filter(frame, resolved_channel)
            frame = filter_by_nps_group(frame, resolved_group)
            frame = self._apply_population_filters(frame, pop_year, pop_month)

        total_rows = int(len(frame))
        slice_df = frame.iloc[offset : offset + limit].copy()
        visible_columns = [
            str(column)
            for column in frame.columns.tolist()
            if not str(column).strip().lower().endswith(("__href", "__hyperlink"))
        ]
        return {
            "dataset_kind": dataset_kind,
            "total_rows": total_rows,
            "offset": offset,
            "limit": limit,
            "columns": visible_columns,
            "rows": self._serialize_rows(slice_df),
            "has_more": offset + len(slice_df) < total_rows,
        }

    @staticmethod
    def _causal_helix_window(
        helix_df: pd.DataFrame,
        nps_df: pd.DataFrame,
        *,
        max_days_apart: int,
    ) -> pd.DataFrame:
        """Apply the one causal time policy without pretending Helix is an NPS dataset."""

        if helix_df.empty or nps_df.empty or "Fecha" not in nps_df.columns:
            return helix_df.iloc[0:0].copy()
        nps_dates = pd.to_datetime(nps_df["Fecha"], errors="coerce").dropna().dt.normalize()
        if nps_dates.empty:
            return helix_df.iloc[0:0].copy()
        incident_dates = incident_occurrence_dates(helix_df)[0].dt.normalize()
        delta = pd.Timedelta(days=max(0, int(max_days_apart)))
        return helix_df.loc[
            incident_dates.between(nps_dates.min() - delta, nps_dates.max() + delta)
        ].copy()

    def _causal_analysis_bundle(
        self,
        *,
        context: UploadContext,
        pop_year: str,
        pop_month: str,
        score_channel: str,
        min_similarity: float,
        max_days_apart: int,
        touchpoint_source: str,
    ) -> dict[str, object]:
        """Build the canonical causal dataset consumed by both UI and PowerPoint."""

        active_source = str(
            touchpoint_source
            or self.settings.ui_defaults()["touchpoint_source"]
            or TOUCHPOINT_SOURCE_DOMAIN
        ).strip()
        key = (
            "causal-analysis",
            *self._context_key(context),
            self._data_revision(context),
            pop_year,
            pop_month,
            score_channel,
            min_similarity,
            max_days_apart,
            active_source,
            self._helix_base_url(),
        )

        def _build() -> dict[str, object]:
            nps_frame = self._load_nps_df(context)
            requested_channel = self._resolve_score_channel(nps_frame, score_channel)
            assignments = self.settings.service_origin_n2_map.get(context.service_origin, {}).get(
                requested_channel, []
            )
            resolved_channel = requested_channel if assignments else POP_ALL
            nps_slice = self._apply_population_filters(
                self._apply_score_channel_filter(nps_frame, resolved_channel),
                pop_year,
                pop_month,
            )
            focus_group, focus_label = self._linking_focus_group(POP_ALL)
            focus_df = nps_slice.loc[focus_mask(nps_slice, focus_group=focus_group)].copy()
            helix_total = self._load_helix_df(context)
            helix_history = self._load_helix_df(
                context,
                score_channel=requested_channel if assignments else None,
            )
            helix_window = self._causal_helix_window(
                helix_history,
                nps_slice,
                max_days_apart=max_days_apart,
            )
            helix_annotated = annotate_incident_link_quality(helix_window)
            helix_slice = helix_annotated.loc[helix_annotated["Causal Match Eligible"]].copy()
            base: dict[str, object] = {
                "ready": False,
                "resolved_channel": resolved_channel,
                "focus_group": focus_group,
                "focus_label": focus_label,
                "nps_slice": nps_slice,
                "focus_df": focus_df,
                "helix_slice": helix_slice,
                "helix_window_rows": int(len(helix_window)),
                "helix_excluded_quality": int(len(helix_window) - len(helix_slice)),
                "touchpoint_source": active_source,
            }
            diagnostic_inputs: dict[str, Any] = dict(
                nps=nps_slice,
                focus=focus_df,
                helix=helix_total,
                scoped=helix_history,
                period=helix_window,
                eligible=helix_slice,
                requested_scope=list(assignments),
            )
            base["diagnostics"] = linking_diagnostics(**diagnostic_inputs, links=pd.DataFrame())
            if nps_slice.empty or focus_df.empty or helix_slice.empty:
                return base

            operational_benchmark = self._safe_helix_operational_benchmark(
                helix_slice,
                context="causal_analysis",
            )
            core = self._compute_linking_core(
                nps_df=nps_slice,
                helix_df=helix_slice,
                focus_df=focus_df,
                focus_group=focus_group,
                min_similarity=min_similarity,
                max_days_apart=max_days_apart,
            )
            links_df = cast(pd.DataFrame, core["links_df"])
            base["diagnostics"] = linking_diagnostics(**diagnostic_inputs, links=links_df)
            by_topic_weekly = cast(pd.DataFrame, core["by_topic_weekly"])
            executive_journey_catalog = load_executive_journey_catalog(
                self.settings.knowledge_dir,
                service_origin=context.service_origin,
                service_origin_n1=context.service_origin_n1,
            )
            mode_payload = self._build_touchpoint_mode_payload(
                touchpoint_source=active_source,
                links_df=links_df,
                focus_df=focus_df,
                helix_df=helix_slice,
                by_topic_weekly=by_topic_weekly,
                executive_journey_catalog=executive_journey_catalog,
            )
            links_mode_df = mode_payload["links_mode_df"]
            rationale_df = self._build_rationale_df(
                by_topic_weekly=mode_payload["by_topic_weekly_mode"],
                focus_group=focus_group,
                links_df=links_mode_df,
                operational_benchmark=operational_benchmark,
            )
            chains = build_incident_attribution_chains(
                links_mode_df,
                focus_df,
                helix_slice,
                rationale_df=rationale_df,
                top_k=0,
                max_incident_examples=0,
                max_comment_examples=0,
                min_links_per_topic=1,
                touchpoint_source=active_source,
                journey_catalog_df=mode_payload["broken_journeys_df"],
                journey_links_df=mode_payload["broken_journey_links_df"],
                executive_journey_catalog=executive_journey_catalog,
            )
            chains = enrich_chain_with_operational_metrics(
                chains,
                benchmark=operational_benchmark,
            )
            chains = _annotate_chain_candidates(chains)
            chains = self._inject_incident_record_urls(chains, helix_df=helix_slice)
            base.update(
                {
                    "ready": True,
                    "core": core,
                    "mode_payload": mode_payload,
                    "rationale_df": rationale_df,
                    "chains": chains,
                    "executive_journey_catalog": executive_journey_catalog,
                }
            )
            return base

        return self._cached_result(key, _build)

    def linking_dashboard(
        self,
        *,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        min_similarity: float = 0.15,
        max_days_apart: int = 90,
        touchpoint_source: str = "",
        theme_mode: str = "light",
    ) -> dict[str, object]:
        touchpoint_source = str(
            touchpoint_source
            or self.settings.ui_defaults()["touchpoint_source"]
            or TOUCHPOINT_SOURCE_DOMAIN
        ).strip()
        key = (
            "linking-dashboard",
            *self._context_key(context),
            self._data_revision(context),
            pop_year,
            pop_month,
            POP_ALL,
            str(score_channel or POP_ALL),
            min_similarity,
            max_days_apart,
            touchpoint_source,
            theme_mode,
            self._helix_base_url(),
        )
        return self._cached_result(
            key,
            lambda: self._build_linking_dashboard(
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=nps_group,
                score_channel=score_channel,
                min_similarity=min_similarity,
                max_days_apart=max_days_apart,
                touchpoint_source=touchpoint_source,
                theme_mode=theme_mode,
            ),
        )

    def _build_linking_dashboard(
        self,
        *,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        min_similarity: float = 0.15,
        max_days_apart: int = 90,
        touchpoint_source: str = "",
        theme_mode: str = "light",
    ) -> dict[str, object]:
        theme = get_theme(theme_mode)
        del nps_group
        resolved_group = POP_ALL
        analysis = self._causal_analysis_bundle(
            context=context,
            pop_year=pop_year,
            pop_month=pop_month,
            score_channel=str(score_channel or POP_ALL),
            min_similarity=min_similarity,
            max_days_apart=max_days_apart,
            touchpoint_source=touchpoint_source,
        )
        resolved_channel = str(analysis["resolved_channel"])
        focus_group = str(analysis["focus_group"])
        focus_label = str(analysis["focus_label"])
        if not bool(analysis["ready"]):
            payload = self._empty_linking_payload(
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=resolved_group,
                score_channel=resolved_channel,
                focus_group=focus_group,
                focus_label=focus_label,
                empty_state=(
                    "No hay suficiente base cruzada para analizar incidencias frente a NPS en el "
                    "contexto actual. Carga Helix y revisa el periodo activo."
                ),
            )
            payload["diagnostics"] = analysis["diagnostics"]
            return payload
        nps_slice = cast(pd.DataFrame, analysis["nps_slice"])
        focus_df = cast(pd.DataFrame, analysis["focus_df"])
        helix_slice = cast(pd.DataFrame, analysis["helix_slice"])
        active_touchpoint_source = str(analysis["touchpoint_source"])
        method_spec = get_causal_method_spec(active_touchpoint_source)
        focus_name = self._focus_name(focus_group)
        core = cast(dict[str, object], analysis["core"])
        overall_daily = cast(pd.DataFrame, core["overall_daily"])
        overall_weekly = cast(pd.DataFrame, core["overall_weekly"])
        by_topic_weekly = cast(pd.DataFrame, core["by_topic_weekly"])
        links_df = cast(pd.DataFrame, core["links_df"])
        mode_payload = cast(dict[str, object], analysis["mode_payload"])
        causal_topic_map_df = cast(pd.DataFrame, mode_payload["causal_topic_map_df"])
        links_mode_df = cast(pd.DataFrame, mode_payload["links_mode_df"])
        trend_df = overall_daily if not overall_daily.empty else overall_weekly
        average_focus = float(_numeric_series(trend_df, "focus_rate", default=0.0).mean())
        show_all_groups = str(resolved_group or "").strip().lower() == str(POP_ALL).lower()

        evidence_df = self._build_linking_evidence_table(
            focus_df,
            helix_slice,
            links_df,
            max_rows=300,
        )
        evidence_df = self._attach_incident_link_column(
            evidence_df,
            helix_df=helix_slice,
            incident_column="incident_id",
        )
        chain_candidates_df = cast(pd.DataFrame, analysis["chains"])
        chain_candidates_summary = summarize_attribution_chains(chain_candidates_df)
        ordered_chain_candidates = select_causal_scenarios(
            chain_candidates_df,
            max_rows=len(chain_candidates_df),
        )
        chain_cards_df = _cap_chain_evidence_rows(
            ordered_chain_candidates,
            max_incident_examples=LINK_MAX_VISIBLE_INCIDENTS,
            max_comment_examples=LINK_MAX_VISIBLE_COMMENTS,
        )
        scenario_cards = self._build_linking_scenario_cards(chain_cards_df)
        entity_summary_df = self._build_entity_summary_df(
            chain_candidates_df,
            touchpoint_source=active_touchpoint_source,
        )
        entity_summary_chart_df = chain_candidates_df.copy()
        if not entity_summary_chart_df.empty:
            entity_summary_chart_df["entity_label"] = (
                _series_or_default(entity_summary_chart_df, "nps_topic").astype(str).str.strip()
            )
        affected_topics = _affected_topics_for_method(chain_candidates_df, causal_topic_map_df)[:10]
        filtered_evidence_df = _filter_frame_by_topic_values(
            evidence_df,
            affected_topics,
        )
        timeline_figure = self._serialize_figure(
            self._build_linking_overview_figure(
                trend_df,
                nps_df=nps_slice,
                focus_label=focus_label,
                focus_name=focus_name,
                show_all_groups=show_all_groups,
                theme=theme,
                include_incidents=False,
            )
        )
        situation_notes = [method_spec.situation_note]
        excluded_quality = int(cast(Any, analysis.get("helix_excluded_quality", 0) or 0))
        if excluded_quality:
            situation_notes.append(
                f"Se excluyeron {excluded_quality} incidencias sin narrativa operativa válida; "
                "siguen visibles y auditables en Datos > Helix."
            )
        if not show_all_groups and not overall_daily.empty:
            situation_notes.append(
                "La línea principal usa media móvil de 7 días para resaltar tendencia sin perder el detalle diario."
            )

        evidence_sorted_df = filtered_evidence_df.copy()
        if not evidence_sorted_df.empty:
            topic_rank = {topic: index for index, topic in enumerate(affected_topics)}
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

        metrics_source = _filter_frame_by_topic_values(by_topic_weekly, affected_topics)
        if not evidence_sorted_df.empty and not metrics_source.empty:
            topic_metrics = (
                metrics_source.groupby("nps_topic", observed=True)
                .agg(Respuestas=("responses", "sum"), focus_count=("focus_count", "sum"))
                .reset_index()
            )
            topic_metrics["Tasa foco"] = topic_metrics["focus_count"] / topic_metrics[
                "Respuestas"
            ].replace({0: np.nan})
            evidence_sorted_df = evidence_sorted_df.merge(
                topic_metrics.drop(columns="focus_count"), on="nps_topic", how="left"
            )
        # Preserve evidence for every affected topic instead of allowing the
        # first topic to consume the whole static-snapshot row budget.
        evidence_visible_df = (
            evidence_sorted_df.groupby("nps_topic", sort=False, group_keys=False).head(10)
            if "nps_topic" in evidence_sorted_df.columns
            else evidence_sorted_df.head(100)
        )
        evidence_visible_df = evidence_visible_df.reindex(
            columns=[
                "nps_topic",
                "incident_id",
                "incident_id__href",
                "incident_summary",
                "detractor_comment",
                "Tasa foco",
                "similarity",
            ]
        )
        evidence_visible_df["Tasa foco"] = evidence_visible_df["Tasa foco"].map(format_percentage)
        evidence_visible_df = evidence_visible_df.rename(
            columns={
                "nps_topic": "NPS Topic",
                "incident_id": "Incident ID",
                "incident_id__href": "Incident ID__href",
                "incident_summary": "Incident Summary",
                "detractor_comment": "Detractor Comment",
                "Tasa foco": "Tasa Foco",
                "similarity": "Similarity",
            }
        )
        evidence_rows = self._serialize_rows(evidence_visible_df)
        topic_views = {}
        if (
            active_touchpoint_source == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS
            and not entity_summary_chart_df.empty
        ):
            for topic, group in entity_summary_chart_df.groupby("anchor_topic", sort=True):
                topic_figure = self._serialize_figure(
                    chart_causal_entity_bar(
                        group,
                        theme=theme,
                        entity_label=method_spec.entity_singular,
                        top_k=10,
                    )
                )
                if topic_figure:
                    # Reuse the common theme instead of repeating it for each snapshot view.
                    cast(dict[str, object], topic_figure["layout"]).pop("template", None)
                    topic_views[str(topic)] = topic_figure
        return {
            "available": True,
            "diagnostics": analysis["diagnostics"],
            "context_pills": self._context_pills(
                context,
                pop_year,
                pop_month,
                resolved_group,
                resolved_channel,
            ),
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
                "incidents_in_time_window": int(
                    cast(Any, analysis.get("helix_window_rows", len(helix_slice)))
                ),
                "incidents_excluded_quality": excluded_quality,
                "linked_pairs": int(len(links_mode_df)),
                "topics_analyzed": len(affected_topics),
                "average_focus_rate": average_focus,
            },
            "situation": {
                "narrative": {
                    "kicker": "Evidencia observada",
                    "title": (
                        f"{len(scenario_cards)} {method_spec.entity_plural.lower()} con vínculos para {focus_name}"
                        if scenario_cards
                        else "Sin vínculos semánticos en esta ventana"
                    ),
                    "summary": (
                        f"{method_spec.summary} La política Helix↔VoC está fijada en similitud ≥ "
                        f"{float(min_similarity):.2f}, top-5 por incidencia y ventana de ±{int(max_days_apart)} días."
                    ),
                    "metrics": self._build_situation_narrative_metrics(
                        method_label=method_spec.label,
                        method_flow=method_spec.flow,
                        responses_total=len(nps_slice),
                        comments_total=chain_candidates_summary["linked_comments_total"],
                        incidents_total=len(helix_slice),
                        linked_incidents_total=chain_candidates_summary["linked_incidents_total"],
                        linked_pairs_total=chain_candidates_summary["linked_pairs_total"],
                        focus_label=focus_label,
                        average_focus_rate=average_focus,
                    ),
                },
                "metadata": [
                    {"label": "Recorrido analizado", "value": method_spec.flow},
                    {"label": "Foco analítico", "value": method_spec.navigation_label},
                ],
                "figure": timeline_figure,
                "note": " ".join([note for note in situation_notes if note]),
                "evidence": {
                    "title": "Evidencias",
                    "subtitle": "Comentarios e incidencias vinculados para los 10 tópicos afectados con mayor evidencia.",
                    "rows": evidence_rows,
                    "empty_state": "No hay vínculos semánticos para los tópicos afectados.",
                },
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
                "topic_figures": topic_views,
                "empty_state": method_spec.table_empty_message,
            },
            "scenarios": {
                "title": "Evidencia por tópico",
                "subtitle": f"Orden: vínculos, incidencias, comentarios y similitud ({method_spec.label.lower()}).",
                "cards": scenario_cards,
            },
        }

    def generate_ppt_report(
        self,
        *,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        min_similarity: float = 0.15,
        max_days_apart: int = 90,
        touchpoint_source: str = "",
        report_dimension_analysis: str = "",
    ) -> BusinessPptResult:
        scope_history_df = self._load_nps_df(context)
        if scope_history_df.empty:
            raise ValueError("No hay datos NPS para el contexto seleccionado.")
        topic_channel = self._resolve_score_channel(
            scope_history_df,
            score_channel or _PREFERRED_SCORE_CHANNEL,
        )
        resolved_group = self._resolve_nps_group(scope_history_df, nps_group or POP_ALL)
        descriptive_current_df = self._apply_population_filters(
            scope_history_df,
            pop_year,
            pop_month,
        )
        if descriptive_current_df.empty:
            raise ValueError(
                "El periodo filtrado no tiene respuestas NPS. Ajusta año o mes antes de generar la PPT."
            )

        focus_group, _ = self._linking_focus_group(resolved_group)
        focus_name = self._focus_name(focus_group)
        active_touchpoint_source = str(
            touchpoint_source
            or self.settings.ui_defaults()["touchpoint_source"]
            or TOUCHPOINT_SOURCE_DOMAIN
        ).strip()
        resolved_report_dimension_analysis = str(
            report_dimension_analysis
            or self.settings.ui_defaults().get("report_dimension_analysis", "palanca")
            or "palanca"
        ).strip()

        context_label = selected_month_label(
            pop_year=pop_year,
            pop_month=pop_month,
            df=scope_history_df,
        )
        period_kpis = build_period_kpis(
            history_df=scope_history_df,
            current_df=descriptive_current_df,
            pop_year=pop_year,
            pop_month=pop_month,
            context_label=context_label,
        )

        period_start, period_end = self._period_bounds(descriptive_current_df)
        attribution_df = pd.DataFrame()
        attribution_all_df = pd.DataFrame()
        entity_summary_kpis: list[dict[str, str]] = []
        broken_journeys_df = pd.DataFrame()
        include_causal_section = False

        try:
            causal = self._causal_analysis_bundle(
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                score_channel=topic_channel,
                min_similarity=min_similarity,
                max_days_apart=max_days_apart,
                touchpoint_source=active_touchpoint_source,
            )
            if bool(causal["ready"]):
                focus_name = self._focus_name(str(causal["focus_group"]))
                attribution_all_df = cast(pd.DataFrame, causal["chains"])
                attribution_df = select_causal_scenarios(
                    attribution_all_df,
                    max_rows=len(attribution_all_df),
                )
                mode_payload = cast(dict[str, object], causal["mode_payload"])
                broken_journeys_df = cast(pd.DataFrame, mode_payload["broken_journeys_df"])
                entity_summary_kpis = self._build_entity_summary_kpis(
                    attribution_all_df,
                    touchpoint_source=active_touchpoint_source,
                )
                include_causal_section = not attribution_df.empty
        except Exception as exc:
            include_causal_section = False
            attribution_df = pd.DataFrame()
            attribution_all_df = pd.DataFrame()
            entity_summary_kpis = []
            broken_journeys_df = pd.DataFrame()
            self.logger.warning(
                "No se pudo construir el bloque causal Helix para la PPT; se generará fallback: %s",
                exc,
            )

        report = generate_business_review_ppt(
            service_origin=context.service_origin,
            service_origin_n1=context.service_origin_n1,
            service_origin_n2=context.service_origin_n2,
            period_start=period_start,
            period_end=period_end,
            focus_name=focus_name,
            topic_channel=topic_channel,
            attribution_df=attribution_df,
            selected_nps_df=descriptive_current_df,
            comparison_nps_df=scope_history_df,
            touchpoint_source=active_touchpoint_source,
            entity_summary_df=attribution_all_df,
            entity_summary_kpis=entity_summary_kpis,
            broken_journeys_df=broken_journeys_df,
            report_dimension_analysis=resolved_report_dimension_analysis,
            period_kpis=period_kpis,
            include_causal_section=include_causal_section,
        )
        saved_path = self._persist_artifact(report.content, report.file_name)
        return BusinessPptResult(
            file_name=report.file_name,
            content=report.content,
            slide_count=report.slide_count,
            saved_path=str(saved_path),
            compact_file_name=report.compact_file_name,
            compact_content=report.compact_content,
        )

    def _persist_artifact(self, content: bytes, file_name: str) -> Path:
        preferred_dir = Path(
            normalize_downloads_path(self.settings.ui_defaults()["downloads_path"], create=True)
        )
        return persist_download(content, file_name, preferred_dir)

    def generate_publication(
        self,
        *,
        context: UploadContext,
        pop_year: str = POP_ALL,
        pop_month: str = POP_ALL,
        nps_group: Optional[str] = None,
        min_n: int = 200,
        min_similarity: float = 0.15,
        max_days_apart: int = 90,
        touchpoint_source: str = "",
        report_dimension_analysis: str = "",
    ) -> PublicationArtifact:
        with self._analytics_lock, self.taxonomy.snapshot_lens(context):
            active_touchpoint_source = touchpoint_source or str(
                self.settings.ui_defaults()["touchpoint_source"]
            )
            scope = build_publication_scope(
                owner_support_company=context.service_origin,
                year=pop_year,
                month=pop_month,
                causal_method=active_touchpoint_source,
            )
            history_df = self._load_nps_df(context)
            publish_channel = self._resolve_score_channel(history_df, _PREFERRED_SCORE_CHANNEL)
            publish_group = self._resolve_nps_group(history_df, nps_group)
            causal_channel = self._resolve_score_channel(history_df, _PREFERRED_SCORE_CHANNEL)
            causal_group = self._resolve_nps_group(history_df, POP_ALL)
            dashboard = self.nps_dashboard(
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=publish_group,
                score_channel=publish_channel,
                min_n=min_n,
            )
            comments = self._build_comments_snapshot(
                history_df=history_df,
                pop_year=pop_year,
                pop_month=pop_month,
            )
            dashboard_overview = cast(dict[str, object], dashboard.get("overview", {}))
            dashboard["overview"] = {
                key: value
                for key, value in dashboard_overview.items()
                if key not in {"topics_figure", "topics_table", "insight_bullets"}
            }
            dashboard.pop("gaps", None)
            linking = self.linking_dashboard(
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=causal_group,
                score_channel=causal_channel,
                min_similarity=min_similarity,
                max_days_apart=max_days_apart,
                touchpoint_source=active_touchpoint_source,
            )
            report = self.generate_ppt_report(
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=causal_group,
                score_channel=causal_channel,
                min_similarity=min_similarity,
                max_days_apart=max_days_apart,
                touchpoint_source=active_touchpoint_source,
                report_dimension_analysis=report_dimension_analysis,
            )
            row_limit = DATA_PAGE_SIZE
            nps_data = self.dataset_rows(
                dataset_kind="nps",
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=publish_group,
                score_channel=publish_channel,
                limit=row_limit,
            )
            helix_data = self.dataset_rows(
                dataset_kind="helix",
                context=context,
                pop_year=pop_year,
                pop_month=pop_month,
                score_channel=POP_ALL,
                limit=row_limit,
            )
            data_snapshot = build_static_data_snapshot(nps_data, helix_data)
            static_datasets = data_snapshot["datasets"]
            if not isinstance(static_datasets, dict):
                raise ValueError("No se pudo materializar el snapshot estático de publicación.")
            static_data = {
                kind: {
                    "columns": dataset["columns"],
                    "total_rows": dataset["total_rows"],
                    "deferred": True,
                }
                for kind, dataset in static_datasets.items()
                if isinstance(dataset, dict)
            }
            generated_at = datetime.now(timezone.utc).isoformat()
            newsletter_current = self._apply_population_filters(history_df, pop_year, pop_month)
            newsletter_start, newsletter_end = self._period_bounds(newsletter_current)
            newsletter = build_executive_newsletter(
                current_df=newsletter_current,
                period_kpis=cast(dict[str, object], dashboard.get("scope", {})),
                linking=linking,
                topic_channel=publish_channel,
                period_start=newsletter_start,
                period_end=newsletter_end,
            )
            publication: dict[str, object] = {
                "schema_version": PUBLICATION_SCHEMA_VERSION,
                "generated_at": generated_at,
                "brand": {
                    "name": "BBVA Banca de Empresas e Instituciones",
                    "design_system": "BBVA Experience",
                    "design_tokens": DesignTokens.default().colors_light,
                },
                "scope": scope,
                "filters": {
                    "service_origin": context.service_origin,
                    "year": pop_year,
                    "month": pop_month,
                    "nps_group": publish_group,
                    "score_channel": publish_channel,
                    "min_n": min_n,
                    "min_similarity": min_similarity,
                    "max_days_apart": max_days_apart,
                    "touchpoint_source": active_touchpoint_source,
                    "causal_nps_group": causal_group,
                    "causal_channel": causal_channel,
                },
                "static_views": {
                    "default": {"score_channel": publish_channel, "nps_group": publish_group},
                    "immutable": True,
                },
                "newsletter": newsletter,
                "screens": {
                    "dashboard": dashboard,
                    "comments": comments,
                    "linking": linking,
                    "data": static_data,
                },
                "snapshots": {
                    "data": data_snapshot,
                },
                "manifest": {
                    "generated_at": generated_at,
                    "scope": scope,
                    "privacy": "No incluye configuración administrativa ni telemetría.",
                    "report": report.file_name,
                    "report_without_evolution": report.compact_file_name,
                },
            }
            date_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            artifact = build_publication_archive(
                publication,
                report_name=report.file_name,
                report_content=report.content,
                compact_report_name=report.compact_file_name,
                compact_report_content=report.compact_content,
                file_name=f"nps-lens-publicacion-{date_stamp}.zip",
            )
            saved_path = self._persist_artifact(artifact.content, artifact.file_name)
            return replace(artifact, saved_path=str(saved_path))

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
    def _build_situation_narrative_metrics(
        *,
        method_label: str,
        method_flow: str,
        responses_total: int,
        comments_total: int,
        incidents_total: int,
        linked_incidents_total: int,
        linked_pairs_total: int,
        focus_label: str,
        average_focus_rate: float,
    ) -> list[dict[str, str]]:
        return [
            {
                "label": "Método de agrupación",
                "value": method_label,
                "hint": "Recorrido de la evidencia: " + method_flow,
            },
            {
                "label": "Respuestas analizadas",
                "value": str(int(responses_total)),
            },
            {
                "label": "Comentarios enlazados",
                "value": str(int(comments_total)),
            },
            {
                "label": "Incidencias del periodo",
                "value": str(int(incidents_total)),
            },
            {
                "label": "Incidencias con match",
                "value": str(int(linked_incidents_total)),
            },
            {
                "label": "Vínculos semánticos",
                "value": str(int(linked_pairs_total)),
            },
            {
                "label": f"{focus_label} medio",
                "value": format_percentage(average_focus_rate),
            },
        ]

    @staticmethod
    def _build_touchpoint_mode_payload(
        *,
        touchpoint_source: str,
        links_df: pd.DataFrame,
        focus_df: pd.DataFrame,
        helix_df: pd.DataFrame,
        by_topic_weekly: pd.DataFrame,
        executive_journey_catalog: Optional[list[dict[str, object]]] = None,
    ) -> dict[str, pd.DataFrame]:
        broken_journeys_df, broken_journey_links_df = pd.DataFrame(), pd.DataFrame()
        if touchpoint_source == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
            broken_journeys_df, broken_journey_links_df = build_broken_journey_catalog(
                links_df,
                focus_df,
                helix_df,
            )
        links_mode_df = links_df.copy()
        by_topic_weekly_mode = by_topic_weekly.copy()
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

        return {
            "broken_journeys_df": broken_journeys_df,
            "broken_journey_links_df": broken_journey_links_df,
            "causal_topic_map_df": causal_topic_map_df,
            "links_mode_df": links_mode_df,
            "by_topic_weekly_mode": by_topic_weekly_mode,
        }

    def _helix_base_url(self) -> str:
        return str(self.settings.ui_defaults().get("helix_base_url", "") or "").strip()

    def _enrich_helix_links(self, frame: pd.DataFrame) -> pd.DataFrame:
        return enrich_helix_incident_links(frame, base_url=self._helix_base_url())

    def _build_helix_incident_url_lookup(self, helix_df: pd.DataFrame) -> dict[str, str]:
        return build_helix_incident_url_lookup(helix_df, base_url=self._helix_base_url())

    def _attach_incident_link_column(
        self,
        frame: pd.DataFrame,
        *,
        helix_df: pd.DataFrame,
        incident_column: str = "incident_id",
    ) -> pd.DataFrame:
        if frame is None or frame.empty or incident_column not in frame.columns:
            return pd.DataFrame(columns=list(frame.columns) if frame is not None else [])
        lookup = self._build_helix_incident_url_lookup(helix_df)
        if not lookup:
            return frame.copy()
        out = frame.copy()
        out[f"{incident_column}__href"] = _text_series(out, incident_column).map(
            lambda value: lookup.get(str(value).strip(), "")
        )
        return out

    def _inject_incident_record_urls(
        self,
        chain_df: pd.DataFrame,
        *,
        helix_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if chain_df is None or chain_df.empty:
            return pd.DataFrame(columns=list(chain_df.columns) if chain_df is not None else [])
        lookup = self._build_helix_incident_url_lookup(helix_df)
        if not lookup:
            return chain_df.copy()

        out = chain_df.copy()
        incident_records = out.get("incident_records", pd.Series([[]] * len(out), index=out.index))
        normalized_records: list[list[dict[str, object]]] = []
        for value in incident_records.tolist():
            records: list[dict[str, object]] = []
            source_records = value if isinstance(value, list) else []
            for entry in source_records:
                if not isinstance(entry, dict):
                    continue
                incident_id = str(entry.get("incident_id", "") or "").strip()
                current_url = str(
                    entry.get("url", "")
                    or entry.get("incident_id__href", "")
                    or entry.get("incident_id__hyperlink", "")
                    or ""
                ).strip()
                resolved_url = resolve_helix_incident_url(
                    incident_id,
                    lookup,
                    current_url=current_url,
                    base_url=self._helix_base_url(),
                )
                records.append(
                    dict(entry) | {"url": resolved_url, "incident_id__href": resolved_url}
                )
            normalized_records.append(records)
        out["incident_records"] = normalized_records
        return out

    def _build_rationale_df(
        self,
        *,
        by_topic_weekly: pd.DataFrame,
        focus_group: str,
        links_df: pd.DataFrame,
        operational_benchmark: HelixOperationalBenchmark,
    ) -> pd.DataFrame:
        rationale_df = build_incident_nps_rationale(
            by_topic_weekly,
            focus_group=focus_group,
            min_topic_responses=80,
        )
        return enrich_rationale_with_operational_metrics(
            rationale_df,
            links_df=links_df,
            benchmark=operational_benchmark,
        )

    @staticmethod
    def _build_entity_summary_df(
        chain_df: pd.DataFrame,
        *,
        touchpoint_source: str,
    ) -> pd.DataFrame:
        if chain_df is None or chain_df.empty:
            return pd.DataFrame()

        source = str(touchpoint_source or TOUCHPOINT_SOURCE_DOMAIN).strip()
        entity_names = {
            TOUCHPOINT_SOURCE_PALANCA: "Palanca",
            TOUCHPOINT_SOURCE_BBVA_SOURCE_N2: "Source Service N2 de Hélix",
            TOUCHPOINT_SOURCE_BROKEN_JOURNEYS: "Journey observado",
            TOUCHPOINT_SOURCE_EXECUTIVE_JOURNEYS: "Journey",
        }
        entity_name = entity_names.get(source, "Subpalanca")
        summary = chain_df.copy()
        for column in ("linked_pairs", "linked_incidents", "linked_comments"):
            summary[column] = _numeric_series(summary, column, default=0).astype(int)
        for column in (
            "avg_nps",
            "avg_similarity",
        ):
            summary[column] = _numeric_series(summary, column, default=np.nan).round(3)
        summary = summary.sort_values(
            ["linked_pairs", "linked_incidents", "linked_comments", "avg_similarity", "nps_topic"],
            ascending=[False, False, False, False, True],
        )
        columns = [
            "nps_topic",
            "anchor_topic",
            "touchpoint",
            "linked_incidents",
            "linked_comments",
            "linked_pairs",
            "avg_similarity",
            "avg_nps",
        ]
        if source == TOUCHPOINT_SOURCE_BROKEN_JOURNEYS:
            columns = [column for column in columns if column not in {"nps_topic", "touchpoint"}]
        summary["avg_similarity"] = summary["avg_similarity"].map(format_percentage)
        return summary[columns].rename(
            columns={
                "nps_topic": entity_name,
                "anchor_topic": "Tópico NPS ancla",
                "touchpoint": "Touchpoint relacionado",
                "linked_incidents": "Incidencias relacionadas",
                "linked_comments": "Comentarios relacionados",
                "linked_pairs": "Vínculos semánticos",
                "avg_similarity": "Confianza",
                "avg_nps": "Nota media (0–10)",
            }
        )

    @staticmethod
    def _build_entity_summary_kpis(
        chain_df: pd.DataFrame, *, touchpoint_source: str
    ) -> list[dict[str, str]]:
        del touchpoint_source
        if chain_df is None or chain_df.empty:
            return []
        entities = (
            _series_or_default(chain_df, "nps_topic").astype(str).str.strip().replace("", np.nan)
        )
        return [
            {"label": "Tópicos observados", "value": str(int(entities.nunique()))},
            {
                "label": "Incidencias relacionadas",
                "value": str(int(_numeric_series(chain_df, "linked_incidents").sum())),
            },
            {
                "label": "Vínculos semánticos",
                "value": str(int(_numeric_series(chain_df, "linked_pairs").sum())),
            },
        ]

    def _build_linking_scenario_cards(self, chain_df: pd.DataFrame) -> list[dict[str, object]]:
        if chain_df is None or chain_df.empty:
            return []
        cards: list[dict[str, object]] = []
        for index, (_, row) in enumerate(chain_df.reset_index(drop=True).iterrows(), start=1):
            title = str(row.get("nps_topic", "") or "").strip()
            card = self._serialize_rows(pd.DataFrame([row]))[0]
            card.update(
                {
                    "identity_rows": [
                        {
                            "label": "Tópico NPS ancla",
                            "value": str(card.get("anchor_topic") or "n/d"),
                        },
                        {
                            "label": "Organizaciones responsables observadas",
                            "value": str(card.get("support_organizations") or "n/d"),
                        },
                        {
                            "label": "Duración media histórica de resolución (semanas)",
                            "value": format_metric(row.get("historical_resolution_weeks")),
                        },
                    ],
                    "rank": index,
                    "title": title,
                    "statement": (
                        f"Se observan {int(row.get('linked_pairs', 0) or 0)} vínculos semánticos entre "
                        f"{int(row.get('linked_incidents', 0) or 0)} incidencias y "
                        f"{int(row.get('linked_comments', 0) or 0)} comentarios."
                    ),
                    "spotlight_metrics": [
                        {
                            "label": "Nota media del tópico",
                            "value": format_metric(row.get("avg_nps")),
                        },
                        {
                            "label": "Vínculos semánticos",
                            "value": str(int(row.get("linked_pairs", 0) or 0)),
                        },
                        {
                            "label": "Incidencias relacionadas",
                            "value": str(int(row.get("linked_incidents", 0) or 0)),
                        },
                        {
                            "label": "Confianza",
                            "value": format_percentage(row.get("avg_similarity")),
                        },
                    ],
                    "flow_steps": [
                        "Incidencias Helix",
                        "Vínculos semánticos",
                        title or "Tópico NPS",
                        "Comentarios VoC",
                    ],
                }
            )
            cards.append(card)
        return cards

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
            f"(cobertura Helix en rojos: Palanca {format_percentage(palanca_ratio)} · "
            f"Subpalanca {format_percentage(subpalanca_ratio)})."
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
            end_boundary = pd.Timestamp(end) + pd.Timedelta(days=1)
            result = result.loc[result["Fecha"] < end_boundary]
        if month_filter:
            result = result.loc[result["Fecha"].dt.month == int(str(month_filter).strip().zfill(2))]
        return result

    def _apply_score_channel_filter(
        self,
        frame: pd.DataFrame,
        score_channel: str,
    ) -> pd.DataFrame:
        if frame.empty or "Canal" not in frame.columns:
            return frame
        channel = str(score_channel or POP_ALL).strip()
        if channel.casefold() in {POP_ALL.casefold(), "all"}:
            return frame
        mask = (
            frame["Canal"].fillna("").astype(str).str.strip().str.casefold().eq(channel.casefold())
        )
        return frame.loc[mask]

    def _resolve_score_channel(
        self,
        frame: pd.DataFrame,
        score_channel: Optional[str],
    ) -> str:
        available = self._available_score_channels(frame)
        requested = str(score_channel or "").strip()
        if requested:
            for option in available:
                if option.casefold() == requested.casefold():
                    return option
            return POP_ALL
        for option in available:
            if option.casefold() == _PREFERRED_SCORE_CHANNEL.casefold():
                return option
        return POP_ALL

    def _resolve_nps_group(self, frame: pd.DataFrame, nps_group: Optional[str]) -> str:
        available = _DEFAULT_NPS_GROUPS
        requested = str(nps_group or "").strip()
        if requested:
            for option in available:
                if option.casefold() == requested.casefold():
                    return option
            return POP_ALL
        if "NPS Group" not in frame.columns:
            return POP_ALL
        groups = set(frame["NPS Group"].astype("string").fillna("").str.strip().str.casefold())
        if _PREFERRED_NPS_GROUP.casefold() in groups or any(
            value.startswith("detr") for value in groups
        ):
            return _PREFERRED_NPS_GROUP
        return POP_ALL

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

    @staticmethod
    def _latest_common_period(
        nps_frame: pd.DataFrame, helix_frame: pd.DataFrame
    ) -> tuple[str, str]:
        """Return the latest year/month with records in both NPS and Helix.

        The causal view needs temporal coverage on both sides. Falling back to
        ``Todos`` is safer than selecting the latest NPS-only month, which can
        make every causal mode look empty when Helix has an older cutoff.
        """
        if nps_frame.empty or helix_frame.empty:
            return POP_ALL, POP_ALL
        if "Fecha" not in nps_frame.columns or "Fecha" not in helix_frame.columns:
            return POP_ALL, POP_ALL

        nps_dates = pd.to_datetime(nps_frame["Fecha"], errors="coerce").dropna()
        helix_dates = pd.to_datetime(helix_frame["Fecha"], errors="coerce").dropna()
        if nps_dates.empty or helix_dates.empty:
            return POP_ALL, POP_ALL

        nps_periods = {(int(value.year), int(value.month)) for value in nps_dates.tolist()}
        helix_periods = {(int(value.year), int(value.month)) for value in helix_dates.tolist()}
        common = nps_periods & helix_periods
        if not common:
            return POP_ALL, POP_ALL

        year, month = max(common)
        return str(year), str(month).zfill(2)

    @staticmethod
    def _latest_common_period_values(
        nps_periods: Sequence[tuple[str, str]],
        helix_periods: Sequence[tuple[str, str]],
    ) -> tuple[str, str]:
        if not nps_periods or not helix_periods:
            return POP_ALL, POP_ALL
        nps_values = {(int(year), int(month)) for year, month in nps_periods}
        helix_values = {(int(year), int(month)) for year, month in helix_periods}
        common = nps_values & helix_values
        if not common:
            return POP_ALL, POP_ALL
        year, month = max(common)
        return str(year), str(month).zfill(2)

    def _available_score_channels(self, frame: pd.DataFrame) -> list[str]:
        if frame.empty or "Canal" not in frame.columns:
            return _DEFAULT_SCORE_CHANNELS.copy()
        values = _unique_string_values(frame["Canal"].tolist())
        return [POP_ALL] + values if values else _DEFAULT_SCORE_CHANNELS.copy()

    def _helix_dataset_status(self, context: UploadContext) -> dict[str, object]:
        stored = self.helix_store.get(DatasetContext(*self._context_key(context)))
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

    def _load_helix_df(
        self,
        context: UploadContext,
        *,
        score_channel: Optional[str] = None,
    ) -> pd.DataFrame:
        restored = self.taxonomy.state(context).get("restored")
        if restored and restored.get("helix") is not None:
            from io import StringIO

            return pd.read_json(StringIO(json.dumps(restored["helix"])), orient="table")
        stored = self.helix_store.get(DatasetContext(*self._context_key(context)))
        if stored is None:
            return pd.DataFrame()
        key = (
            "helix-frame",
            *self._context_key(context),
            str(score_channel or ""),
            self._path_revision(stored.path),
            self.taxonomy.registry(context).signature("helix"),
            tuple(
                self.settings.service_origin_n2_map.get(context.service_origin, {}).get(
                    str(score_channel or ""), []
                )
            ),
        )
        with self._analytics_lock:
            cached = self._frame_cache.get(key)
            if cached is not None:
                self._frame_cache.move_to_end(key)
                return cached
            frame = self.taxonomy.registry(context).apply("helix", self.helix_store.load_df(stored))
            assignments = self.settings.service_origin_n2_map.get(context.service_origin, {}).get(
                str(score_channel or ""), []
            )
            assignment_keys = {
                equivalence_key(value) for value in assignments if equivalence_key(value)
            }
            if assignment_keys:
                n1 = frame.get(SOURCE_SERVICE_N1, pd.Series("", index=frame.index)).map(
                    equivalence_key
                )
                n2_matches: pd.Series[bool] = (
                    frame.get(SOURCE_SERVICE_N2, pd.Series("", index=frame.index))
                    .astype(str)
                    .map(
                        lambda value: any(
                            equivalence_key(token) in assignment_keys
                            for token in value.split(",")
                            if equivalence_key(token)
                        )
                    )
                )
                frame = frame.loc[n1.isin(assignment_keys) | n2_matches].copy()
            return cast(
                pd.DataFrame,
                self._remember_bounded(
                    self._frame_cache,
                    key,
                    frame,
                    self._frame_cache_limit,
                ),
            )

    def _context_pills(
        self,
        context: UploadContext,
        pop_year: str,
        pop_month: str,
        nps_group: str,
        score_channel: str = POP_ALL,
    ) -> list[str]:
        month_label = (
            pop_month
            if pop_month in {POP_ALL, ""}
            else MONTH_LABELS_ES.get(_MONTH_LABEL_TO_NUMBER.get(pop_month, pop_month), pop_month)
        )
        return [
            f"Owner Support Company: {context.service_origin}",
            f"Año: {pop_year}",
            f"Mes: {month_label or POP_ALL}",
            f"Canal: {score_channel or POP_ALL}",
            f"Grupo: {nps_group}",
        ]

    def _topics_df(self, frame: pd.DataFrame) -> pd.DataFrame:
        comment_column = "Comment" if "Comment" in frame.columns else ""
        if not comment_column:
            return pd.DataFrame()
        topics = summarize_taxonomy(frame)
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
        focus_copy["nps_id"] = analytical_response_ids(focus_copy)
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
        overall_daily, _ = daily_aggregates(
            nps_df,
            helix_df,
            assignments_df,
            focus_group=focus_group,
        )
        return {
            "links_df": links_df,
            "overall_weekly": overall_weekly,
            "by_topic_weekly": by_topic_weekly,
            "overall_daily": overall_daily,
        }

    def _empty_linking_payload(
        self,
        *,
        context: UploadContext,
        pop_year: str,
        pop_month: str,
        nps_group: str,
        score_channel: str,
        focus_group: str,
        focus_label: str,
        empty_state: str,
    ) -> dict[str, object]:
        return {
            "available": False,
            "context_pills": self._context_pills(
                context,
                pop_year,
                pop_month,
                nps_group,
                score_channel,
            ),
            "focus_group": focus_group,
            "focus_label": focus_label,
            "empty_state": empty_state,
            "kpis": {},
            "causal_method": {},
            "navigation": [],
            "situation": {},
            "entity_summary": {},
            "scenarios": {},
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
        include_incidents: bool = True,
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
        layout_kwargs: dict[str, object] = {
            "height": 380,
            "margin": dict(l=10, r=10, t=62, b=10),
            "paper_bgcolor": theme.chart_paper,
            "plot_bgcolor": theme.chart_plot,
            "font": dict(color=theme.text),
            "legend": dict(orientation="h"),
            "yaxis": dict(
                title="Tasa por grupo" if show_all_groups else focus_label,
                tickformat=".0%",
                gridcolor=theme.chart_grid,
            ),
            "xaxis": dict(gridcolor=theme.chart_grid),
        }
        if include_incidents:
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
            layout_kwargs["yaxis2"] = dict(
                title="Incidencias",
                overlaying="y",
                side="right",
                showgrid=False,
            )
        fig.update_layout(
            **layout_kwargs,
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
        """Serialize a DataFrame into strict-JSON-safe records.

        Pandas keeps ``NaN`` values in numeric columns when ``None`` is assigned
        unless the column is first converted to ``object``. Starlette/FastAPI
        rejects non-finite floats (NaN/inf) during strict JSON encoding, so each
        scalar is normalized explicitly before returning it to the API layer.
        """
        if frame.empty:
            return []

        serialized = frame.copy()
        for column in serialized.columns:
            if pd.api.types.is_datetime64_any_dtype(serialized[column]):
                serialized[column] = pd.to_datetime(
                    serialized[column], errors="coerce"
                ).dt.strftime("%Y-%m-%dT%H:%M:%S")

        serialized = serialized.astype(object)
        serialized = serialized.where(pd.notna(serialized), None)

        def _json_safe_scalar(value: object) -> object:
            if value is None or value is pd.NA or value is pd.NaT:
                return None
            if isinstance(value, (float, np.floating)):
                return float(value) if np.isfinite(value) else None
            if isinstance(value, np.integer):
                return int(value)
            if isinstance(value, np.bool_):
                return bool(value)
            if isinstance(value, (pd.Timestamp, datetime, date)):
                return value.isoformat()
            return value

        return [
            {str(key): _json_safe_scalar(value) for key, value in row.items()}
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
