from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, cast
from urllib.parse import quote

import pandas as pd
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from nps_lens.analytics.taxonomy import TaxonomyConfig
from nps_lens.api.schemas import (
    ContextOptionsResponse,
    DashboardResponse,
    DatasetTableResponse,
    EquivalenceRegistryRequest,
    HelixUploadResponse,
    LinkingResponse,
    PreferencesResponse,
    PreferencesUpdateRequest,
    ServiceOriginHierarchyRequest,
    SummaryResponse,
    TaxonomyGenerateRequest,
    TaxonomySettingsRequest,
    UploadResponse,
)
from nps_lens.core.store import DatasetContext
from nps_lens.core.telemetry import RequestTimer, TelemetryCollector
from nps_lens.domain.models import UploadContext
from nps_lens.domain.normalization import CATEGORICAL_DIMENSIONS, EquivalenceRegistry
from nps_lens.platform.downloads import persist_download
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.dashboard_service import DashboardService
from nps_lens.services.nps_service import NpsService
from nps_lens.settings import (
    Settings,
    load_runtime_dotenv,
    normalize_downloads_path,
    normalize_helix_base_url,
    persist_service_origin_hierarchy,
    persist_ui_prefs,
)


def _resolve_context(
    settings: Settings,
    service_origin: Optional[str],
    service_origin_n1: Optional[str],
    service_origin_n2: Optional[str],
) -> UploadContext:
    preferences = settings.ui_defaults()
    return UploadContext(
        service_origin=str(
            service_origin or preferences["service_origin"] or settings.default_service_origin
        ),
        service_origin_n1=str(
            service_origin_n1
            or preferences["service_origin_n1"]
            or settings.default_service_origin_n1
        ),
        service_origin_n2=str(service_origin_n2 or preferences["service_origin_n2"] or ""),
    )


def _optional_context(
    service_origin: Optional[str],
    service_origin_n1: Optional[str],
    service_origin_n2: Optional[str],
) -> Optional[UploadContext]:
    if not service_origin or not service_origin_n1:
        return None
    return UploadContext(
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        service_origin_n2=service_origin_n2 or "",
    )


def _authenticated_email(request: Request) -> str:
    value = request.headers.get("X-Goog-Authenticated-User-Email", "").strip()
    if ":" in value:
        value = value.rsplit(":", 1)[-1]
    return value.casefold()


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    if settings is None:
        load_runtime_dotenv()
    app_settings = settings or Settings.from_env()
    repository = SqliteNpsRepository(app_settings.database_path)
    service = NpsService(repository=repository, settings=app_settings)
    dashboard_service = DashboardService(repository=repository, settings=app_settings)

    app = FastAPI(title="NPS Lens API", version="2.0.0")
    app.state.settings = app_settings
    app.state.repository = repository
    app.state.service = service
    app.state.dashboard_service = dashboard_service
    app.state.telemetry = TelemetryCollector()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def enforce_domain_access(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        current = cast(Settings, request.app.state.settings)
        if current.auth_mode == "gcp_iap" and request.url.path.startswith("/api/"):
            email = _authenticated_email(request)
            if not email or not email.endswith("@" + current.allowed_email_domain):
                return Response(
                    content='{"detail":"Acceso restringido al dominio BBVA."}',
                    media_type="application/json",
                    status_code=403,
                )
            request.state.user_email = email
        return await call_next(request)

    @app.middleware("http")
    async def collect_telemetry(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        status = 500
        response_bytes = 0
        error_type = ""
        with RequestTimer() as timer:
            try:
                response = await call_next(request)
                status = int(response.status_code)
                response_bytes = int(response.headers.get("content-length", "0") or 0)
                return response
            except Exception as exc:
                error_type = type(exc).__name__
                raise
            finally:
                duration_ms, cpu_ms = timer.elapsed()
                route_object = request.scope.get("route")
                # Never retain user-controlled paths from unmatched requests.
                route = str(getattr(route_object, "path", "<unmatched>"))
                request.app.state.telemetry.record(
                    method=request.method,
                    route=route,
                    status=status,
                    duration_ms=duration_ms,
                    cpu_ms=cpu_ms,
                    response_bytes=response_bytes,
                    error_type=error_type,
                )

    def get_service(request: Request) -> NpsService:
        return cast(NpsService, request.app.state.service)

    def get_dashboard_service(request: Request) -> DashboardService:
        return cast(DashboardService, request.app.state.dashboard_service)

    def require_admin(request: Request) -> None:
        current = cast(Settings, request.app.state.settings)
        if current.auth_mode != "gcp_iap":
            return
        if _authenticated_email(request) not in current.admin_emails:
            raise HTTPException(
                status_code=403, detail="Esta operación requiere rol administrador."
            )

    def access_payload(request: Request) -> dict[str, object]:
        current = cast(Settings, request.app.state.settings)
        email = _authenticated_email(request)
        is_admin = current.auth_mode != "gcp_iap" or email in current.admin_emails
        return {
            "email": email,
            "role": "admin" if is_admin else "viewer",
            "is_admin": is_admin,
            "allowed_domain": current.allowed_email_domain,
        }

    def refresh_settings(request: Request) -> Settings:
        reloaded = Settings.from_env()
        request.app.state.settings = reloaded
        request.app.state.service.settings = reloaded
        request.app.state.dashboard_service.clear_caches()
        request.app.state.dashboard_service.settings = reloaded
        request.app.state.dashboard_service.helix_store = (
            request.app.state.dashboard_service.helix_store.__class__(reloaded.data_dir / "helix")
        )
        return reloaded

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/access")
    def access(request: Request) -> dict[str, object]:
        return access_payload(request)

    @app.get("/api/telemetry")
    def telemetry(request: Request) -> dict[str, object]:
        require_admin(request)
        collector = cast(TelemetryCollector, request.app.state.telemetry)
        return collector.snapshot()

    @app.get("/api/telemetry/export")
    def export_telemetry(request: Request) -> Response:
        require_admin(request)
        payload = request.app.state.telemetry.to_json_bytes()
        current_settings = cast(Settings, request.app.state.settings)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        file_name = f"nps-lens-telemetria-{stamp}.json"
        try:
            saved_path = persist_download(
                payload,
                file_name,
                Path(
                    normalize_downloads_path(
                        current_settings.ui_defaults()["downloads_path"], create=True
                    )
                ),
            )
        except OSError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return Response(
            content=payload,
            media_type="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{file_name}"',
                "X-NPS-LENS-SAVED-PATH": str(saved_path),
                "Cache-Control": "no-store",
            },
        )

    @app.get("/api/config", response_model=ContextOptionsResponse)
    def config(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        payload = dashboard_layer.context_options(
            _resolve_context(
                cast(Settings, request.app.state.settings),
                service_origin,
                service_origin_n1,
                service_origin_n2,
            )
        )
        payload["access"] = access_payload(request)
        return payload

    @app.get("/api/uploads", response_model=list[UploadResponse])
    def list_uploads(service_layer: NpsService = Depends(get_service)) -> list[dict[str, object]]:
        return service_layer.list_uploads()

    @app.get("/api/summary", response_model=SummaryResponse)
    def summary(
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        service_layer: NpsService = Depends(get_service),
    ) -> dict[str, object]:
        return service_layer.summary(
            _optional_context(service_origin, service_origin_n1, service_origin_n2)
        )

    @app.post("/api/reprocess", response_model=SummaryResponse)
    def reprocess(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        service_layer: NpsService = Depends(get_service),
    ) -> dict[str, object]:
        require_admin(request)
        return service_layer.summary(
            _optional_context(service_origin, service_origin_n1, service_origin_n2)
        )

    @app.post("/api/uploads/nps", response_model=UploadResponse)
    async def upload_nps(
        request: Request,
        file: UploadFile = File(...),
        service_origin: str = Form(...),
        service_origin_n1: str = Form(...),
        service_origin_n2: str = Form(""),
        sheet_name: str = Form(""),
        service_layer: NpsService = Depends(get_service),
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        require_admin(request)
        filename = file.filename or "upload.xlsx"
        suffix = Path(filename).suffix.lower()
        if suffix not in {".xlsx", ".xlsm", ".xls"}:
            raise HTTPException(status_code=400, detail="Solo se admiten ficheros Excel.")

        payload = await file.read()
        if not payload:
            raise HTTPException(status_code=400, detail="El fichero está vacío.")

        result = service_layer.ingest_excel(
            filename=filename,
            payload=payload,
            context=UploadContext(
                service_origin=service_origin,
                service_origin_n1=service_origin_n1,
                service_origin_n2=service_origin_n2,
            ),
            sheet_name=sheet_name,
        )
        if result["status"] == "completed" and dashboard_layer.taxonomy.state(
            UploadContext(service_origin, service_origin_n1, service_origin_n2)
        ).get("restored"):
            dashboard_layer.taxonomy.resume_local(
                UploadContext(service_origin, service_origin_n1, service_origin_n2)
            )
        dashboard_layer.clear_caches()
        return result

    @app.post("/api/uploads/helix", response_model=HelixUploadResponse)
    async def upload_helix(
        request: Request,
        file: UploadFile = File(...),
        service_origin: str = Form(...),
        service_origin_n1: str = Form(...),
        service_origin_n2: str = Form(""),
        sheet_name: str = Form(""),
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        require_admin(request)
        filename = file.filename or "helix.xlsx"
        suffix = Path(filename).suffix.lower()
        if suffix not in {".xlsx", ".xlsm", ".xls"}:
            raise HTTPException(status_code=400, detail="Solo se admiten ficheros Excel.")

        payload = await file.read()
        if not payload:
            raise HTTPException(status_code=400, detail="El fichero está vacío.")

        result = dashboard_layer.ingest_helix_excel(
            filename=filename,
            payload=payload,
            context=UploadContext(
                service_origin=service_origin,
                service_origin_n1=service_origin_n1,
                service_origin_n2=service_origin_n2,
            ),
            sheet_name=sheet_name,
        )
        dashboard_layer.clear_caches()
        return result

    @app.get("/api/dashboard/context", response_model=ContextOptionsResponse)
    def dashboard_context(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        payload = dashboard_layer.context_options(
            _resolve_context(
                cast(Settings, request.app.state.settings),
                service_origin,
                service_origin_n1,
                service_origin_n2,
            )
        )
        payload["access"] = access_payload(request)
        return payload

    @app.get("/api/preferences", response_model=PreferencesResponse)
    def preferences(request: Request) -> dict[str, object]:
        current_settings = cast(Settings, request.app.state.settings)
        return current_settings.ui_defaults()

    @app.put("/api/preferences", response_model=PreferencesResponse)
    def update_preferences(
        payload: PreferencesUpdateRequest,
        request: Request,
    ) -> dict[str, object]:
        require_admin(request)
        current_settings = cast(Settings, request.app.state.settings)
        next_values = payload.model_dump()
        try:
            next_values["downloads_path"] = normalize_downloads_path(
                next_values.get("downloads_path")
            )
            next_values["helix_base_url"] = normalize_helix_base_url(
                next_values.get("helix_base_url")
            )
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        persist_ui_prefs(current_settings.dotenv_path, next_values)
        return refresh_settings(request).ui_defaults()

    @app.put("/api/settings/service-origins", response_model=ContextOptionsResponse)
    def update_service_origins(
        payload: ServiceOriginHierarchyRequest,
        request: Request,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        require_admin(request)
        service_origins = [value.strip() for value in payload.service_origins if value.strip()]
        if not service_origins:
            raise HTTPException(
                status_code=400,
                detail="Debe existir al menos un Service Origin BUUG.",
            )

        service_origin_n1_map: dict[str, list[str]] = {}
        service_origin_n2_map: dict[str, dict[str, list[str]]] = {}
        for origin in service_origins:
            n1_values = [
                value.strip()
                for value in payload.service_origin_n1_map.get(origin, [])
                if value.strip()
            ]
            if not n1_values:
                raise HTTPException(
                    status_code=400,
                    detail=f"El origen '{origin}' debe incluir al menos un N1.",
                )
            service_origin_n1_map[origin] = list(dict.fromkeys(n1_values))
            origin_n2_map = payload.service_origin_n2_map.get(origin, {})
            service_origin_n2_map[origin] = {
                n1: list(
                    dict.fromkeys(
                        [value.strip() for value in origin_n2_map.get(n1, []) if value.strip()]
                    )
                )
                for n1 in service_origin_n1_map[origin]
            }

        current_settings = cast(Settings, request.app.state.settings)
        current_preferences = current_settings.ui_defaults()
        default_service_origin = str(current_preferences["service_origin"])
        if default_service_origin not in service_origins:
            default_service_origin = service_origins[0]
        default_service_origin_n1 = str(current_preferences["service_origin_n1"])
        if default_service_origin_n1 not in service_origin_n1_map.get(default_service_origin, []):
            default_service_origin_n1 = service_origin_n1_map[default_service_origin][0]

        persist_service_origin_hierarchy(
            current_settings.dotenv_path,
            service_origins=service_origins,
            service_origin_n1_map=service_origin_n1_map,
            service_origin_n2_map=service_origin_n2_map,
            default_service_origin=default_service_origin,
            default_service_origin_n1=default_service_origin_n1,
        )
        reloaded = refresh_settings(request)
        request.app.state.dashboard_service = DashboardService(
            repository=cast(SqliteNpsRepository, request.app.state.repository),
            settings=reloaded,
        )
        updated_dashboard_layer = request.app.state.dashboard_service
        return updated_dashboard_layer.context_options(updated_dashboard_layer.resolve_context())

    def taxonomy_context(request: Request) -> UploadContext:
        return _resolve_context(
            cast(Settings, request.app.state.settings),
            request.query_params.get("service_origin"),
            request.query_params.get("service_origin_n1"),
            request.query_params.get("service_origin_n2"),
        )

    @app.get("/api/settings/equivalences")
    def equivalences(
        request: Request, dashboard_layer: DashboardService = Depends(get_dashboard_service)
    ) -> dict[str, object]:
        require_admin(request)
        registry = EquivalenceRegistry.load(dashboard_layer.settings.equivalences_path)
        context = taxonomy_context(request)
        nps = dashboard_layer.taxonomy.source(context)
        stored = dashboard_layer.helix_store.get(
            DatasetContext(
                context.service_origin, context.service_origin_n1, context.service_origin_n2
            )
        )
        helix = dashboard_layer.helix_store.load_df(stored) if stored else pd.DataFrame()
        statistics = {}
        for scope in sorted(CATEGORICAL_DIMENSIONS):
            domain, column = scope.split(".", 1)
            frame = nps if domain == "nps" else helix
            source_column = (
                {
                    "Canal": "source_channel",
                    "Palanca": "source_lever",
                    "Subpalanca": "source_sublever",
                }.get(column, column)
                if domain == "nps"
                else column
            )
            values = (
                frame[source_column] if source_column in frame else pd.Series([], dtype="string")
            )
            counts = values.astype("string").fillna("").value_counts()
            groups = registry.to_dict()["dimensions"].get(scope, [])
            statistics[scope] = {
                "groups": [
                    {
                        "canonical": group["canonical"],
                        "affected": int(
                            counts.reindex([group["canonical"], *group["aliases"]], fill_value=0)
                            .groupby(level=0)
                            .first()
                            .sum()
                        ),
                    }
                    for group in groups
                ],
                "suggestions": registry.collision_report(scope, counts.index.tolist())[:50],
            }
        return {
            **registry.to_dict(),
            "available_dimensions": sorted(CATEGORICAL_DIMENSIONS),
            "statistics": statistics,
        }

    @app.put("/api/settings/equivalences")
    def update_equivalences(
        payload: EquivalenceRegistryRequest,
        request: Request,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        require_admin(request)
        try:
            registry = EquivalenceRegistry.from_dict(payload.model_dump())
            registry.save(dashboard_layer.settings.equivalences_path)
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        dashboard_layer.clear_caches()
        return equivalences(request, dashboard_layer)

    @app.get("/api/taxonomy")
    def taxonomy_studio(
        request: Request, dashboard_layer: DashboardService = Depends(get_dashboard_service)
    ) -> dict[str, Any]:
        return dashboard_layer.taxonomy.studio(taxonomy_context(request))

    @app.get("/api/taxonomy/explore")
    def taxonomy_explore(
        request: Request,
        mode: str,
        offset: int = 0,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, Any]:
        try:
            return dashboard_layer.taxonomy.explore(taxonomy_context(request), mode, max(offset, 0))
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/taxonomy/compare")
    def taxonomy_compare(
        request: Request,
        left: str,
        right: str,
        offset: int = 0,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, Any]:
        try:
            return dashboard_layer.taxonomy.compare(
                taxonomy_context(request), left, right, max(offset, 0)
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/taxonomy/generate")
    def taxonomy_generate(
        payload: TaxonomyGenerateRequest,
        request: Request,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, Any]:
        require_admin(request)
        try:
            with dashboard_layer._analytics_lock:
                result = dashboard_layer.taxonomy.generate(
                    taxonomy_context(request),
                    payload.mode,
                    TaxonomyConfig(**payload.config),
                    payload.regenerate,
                )
                dashboard_layer.clear_caches()
                return result
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.put("/api/taxonomy/settings")
    def taxonomy_settings(
        payload: TaxonomySettingsRequest,
        request: Request,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, Any]:
        require_admin(request)
        try:
            result = dashboard_layer.taxonomy.configure(
                taxonomy_context(request), payload.model_dump(exclude_none=True)
            )
            dashboard_layer.clear_caches()
            return result
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/taxonomy/snapshot")
    def taxonomy_snapshot(
        request: Request, dashboard_layer: DashboardService = Depends(get_dashboard_service)
    ) -> Response:
        require_admin(request)
        context = taxonomy_context(request)
        try:
            snapshot = dashboard_layer.taxonomy.snapshot(
                context,
                dashboard_layer._load_helix_df(context),
                dashboard_layer.settings.ui_defaults(),
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        content = json.dumps(snapshot, ensure_ascii=False).encode("utf-8")
        if len(content) > 30 * 1024 * 1024:
            raise HTTPException(
                413, "El snapshot supera 30 MB; selecciona una política con menos lentes."
            )
        return Response(
            content,
            media_type="application/json",
            headers={"Content-Disposition": 'attachment; filename="nps-lens-taxonomy.json"'},
        )

    @app.post("/api/taxonomy/restore")
    def taxonomy_restore(
        request: Request,
        file: UploadFile = File(...),
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, Any]:
        require_admin(request)
        try:
            content = file.file.read(30 * 1024 * 1024 + 1)
            if len(content) > 30 * 1024 * 1024:
                raise ValueError("El snapshot supera 30 MB.")
            snapshot = json.loads(content)
            context = taxonomy_context(request)
            dashboard_layer.taxonomy.restore(context, snapshot)
            dashboard_layer.clear_caches()
            return dashboard_layer.taxonomy.studio(context)
        except (ValueError, KeyError, TypeError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.post("/api/taxonomy/resume")
    def taxonomy_resume(
        request: Request, dashboard_layer: DashboardService = Depends(get_dashboard_service)
    ) -> dict[str, Any]:
        require_admin(request)
        context = taxonomy_context(request)
        dashboard_layer.taxonomy.resume_local(context)
        dashboard_layer.clear_caches()
        return dashboard_layer.taxonomy.studio(context)

    @app.get("/api/dashboard/nps", response_model=DashboardResponse)
    def dashboard_nps(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        pop_year: str = "Todos",
        pop_month: str = "Todos",
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        gap_dimension: str = "Palanca",
        cohort_row: str = "Palanca",
        cohort_col: str = "Canal",
        min_n: int = 200,
        min_n_cross: int = 30,
        theme_mode: str = "light",
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        return dashboard_layer.nps_dashboard(
            context=_resolve_context(
                cast(Settings, request.app.state.settings),
                service_origin,
                service_origin_n1,
                service_origin_n2,
            ),
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
        )

    @app.get("/api/dashboard/linking", response_model=LinkingResponse)
    def dashboard_linking(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        pop_year: str = "Todos",
        pop_month: str = "Todos",
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        min_similarity: float = 0.15,
        max_days_apart: int = 90,
        touchpoint_source: str = "",
        theme_mode: str = "light",
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        return dashboard_layer.linking_dashboard(
            context=_resolve_context(
                cast(Settings, request.app.state.settings),
                service_origin,
                service_origin_n1,
                service_origin_n2,
            ),
            pop_year=pop_year,
            pop_month=pop_month,
            nps_group=nps_group,
            score_channel=score_channel,
            min_similarity=min_similarity,
            max_days_apart=max_days_apart,
            touchpoint_source=touchpoint_source,
            theme_mode=theme_mode,
        )

    @app.get("/api/dashboard/report/pptx")
    def dashboard_report(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        pop_year: str = "Todos",
        pop_month: str = "Todos",
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        min_n: int = 200,
        min_similarity: float = 0.15,
        max_days_apart: int = 90,
        touchpoint_source: str = "",
        report_dimension_analysis: str = "",
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> Response:
        try:
            report = dashboard_layer.generate_ppt_report(
                context=_resolve_context(
                    cast(Settings, request.app.state.settings),
                    service_origin,
                    service_origin_n1,
                    service_origin_n2,
                ),
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=nps_group,
                score_channel=score_channel,
                min_n=min_n,
                min_similarity=min_similarity,
                max_days_apart=max_days_apart,
                touchpoint_source=touchpoint_source,
                report_dimension_analysis=report_dimension_analysis,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except OSError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        headers = {
            "Content-Disposition": (
                f'attachment; filename="{report.file_name}"; '
                f"filename*=UTF-8''{quote(report.file_name)}"
            ),
            "X-NPS-LENS-SAVED-PATH": report.saved_path,
        }
        return Response(
            content=report.content,
            media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
            headers=headers,
        )

    @app.get("/api/dashboard/publication.zip")
    def dashboard_publication(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        pop_year: str = "Todos",
        pop_month: str = "Todos",
        nps_group: Optional[str] = None,
        min_n: int = 200,
        min_similarity: float = 0.15,
        max_days_apart: int = 90,
        touchpoint_source: str = "",
        report_dimension_analysis: str = "",
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> Response:
        require_admin(request)
        try:
            artifact = dashboard_layer.generate_publication(
                context=_resolve_context(
                    cast(Settings, request.app.state.settings),
                    service_origin,
                    service_origin_n1,
                    service_origin_n2,
                ),
                pop_year=pop_year,
                pop_month=pop_month,
                nps_group=nps_group,
                min_n=min_n,
                min_similarity=min_similarity,
                max_days_apart=max_days_apart,
                touchpoint_source=touchpoint_source,
                report_dimension_analysis=report_dimension_analysis,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except OSError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return Response(
            content=artifact.content,
            media_type="application/zip",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="{artifact.file_name}"; '
                    f"filename*=UTF-8''{quote(artifact.file_name)}"
                ),
                "X-NPS-LENS-PUBLICATION-BYTES": str(artifact.size_bytes),
                "X-NPS-LENS-SAVED-PATH": artifact.saved_path,
            },
        )

    @app.get("/api/dashboard/data/{dataset_kind}", response_model=DatasetTableResponse)
    def dashboard_table(
        request: Request,
        dataset_kind: str,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        pop_year: str = "Todos",
        pop_month: str = "Todos",
        nps_group: Optional[str] = None,
        score_channel: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        kind = dataset_kind.strip().lower()
        if kind not in {"nps", "helix"}:
            raise HTTPException(status_code=404, detail="Dataset no soportado.")
        return dashboard_layer.dataset_rows(
            dataset_kind=kind,
            context=_resolve_context(
                cast(Settings, request.app.state.settings),
                service_origin,
                service_origin_n1,
                service_origin_n2,
            ),
            pop_year=pop_year,
            pop_month=pop_month,
            nps_group=nps_group,
            score_channel=score_channel,
            offset=max(offset, 0),
            limit=max(min(limit, 500), 1),
        )

    _configure_static_frontend(app, app_settings)
    return app


def _configure_static_frontend(app: FastAPI, settings: Settings) -> None:
    dist_dir = settings.frontend_dist_dir
    if not dist_dir.exists():
        return

    assets_dir = dist_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="frontend-assets")

    index_path = dist_dir / "index.html"
    if not index_path.exists():
        return

    @app.get("/", include_in_schema=False)
    def serve_frontend_index() -> FileResponse:
        return FileResponse(index_path)

    @app.get("/{full_path:path}", include_in_schema=False)
    def serve_frontend_spa(full_path: str) -> FileResponse:
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404)
        candidate = dist_dir / full_path
        if candidate.exists() and candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(index_path)
