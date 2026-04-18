from __future__ import annotations

from pathlib import Path
from typing import Optional, cast
from urllib.parse import quote

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from nps_lens.api.schemas import (
    ContextOptionsResponse,
    DashboardResponse,
    DatasetTableResponse,
    HelixUploadResponse,
    LinkingResponse,
    PreferencesResponse,
    PreferencesUpdateRequest,
    ServiceOriginHierarchyRequest,
    SummaryResponse,
    UploadResponse,
)
from nps_lens.domain.models import UploadContext
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.dashboard_service import DashboardService
from nps_lens.services.nps_service import NpsService
from nps_lens.settings import Settings, persist_service_origin_hierarchy, persist_ui_prefs


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


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    app_settings = settings or Settings.from_env()
    repository = SqliteNpsRepository(app_settings.database_path)
    service = NpsService(repository=repository, settings=app_settings)
    dashboard_service = DashboardService(repository=repository, settings=app_settings)

    app = FastAPI(title="NPS Lens API", version="2.0.0")
    app.state.settings = app_settings
    app.state.repository = repository
    app.state.service = service
    app.state.dashboard_service = dashboard_service

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def get_service(request: Request) -> NpsService:
        return cast(NpsService, request.app.state.service)

    def get_dashboard_service(request: Request) -> DashboardService:
        return cast(DashboardService, request.app.state.dashboard_service)

    def refresh_settings(request: Request) -> Settings:
        reloaded = Settings.from_env()
        request.app.state.settings = reloaded
        request.app.state.service.settings = reloaded
        request.app.state.dashboard_service.settings = reloaded
        request.app.state.dashboard_service.helix_store = request.app.state.dashboard_service.helix_store.__class__(
            reloaded.data_dir / "helix"
        )
        return reloaded

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/config", response_model=ContextOptionsResponse)
    def config(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        return dashboard_layer.context_options(
            _resolve_context(
                cast(Settings, request.app.state.settings),
                service_origin,
                service_origin_n1,
                service_origin_n2,
            )
        )

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
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        service_layer: NpsService = Depends(get_service),
    ) -> dict[str, object]:
        return service_layer.summary(
            _optional_context(service_origin, service_origin_n1, service_origin_n2)
        )

    @app.post("/api/uploads/nps", response_model=UploadResponse)
    async def upload_nps(
        file: UploadFile = File(...),
        service_origin: str = Form(...),
        service_origin_n1: str = Form(...),
        service_origin_n2: str = Form(""),
        sheet_name: str = Form(""),
        service_layer: NpsService = Depends(get_service),
    ) -> dict[str, object]:
        filename = file.filename or "upload.xlsx"
        suffix = Path(filename).suffix.lower()
        if suffix not in {".xlsx", ".xlsm", ".xls"}:
            raise HTTPException(status_code=400, detail="Solo se admiten ficheros Excel.")

        payload = await file.read()
        if not payload:
            raise HTTPException(status_code=400, detail="El fichero está vacío.")

        return service_layer.ingest_excel(
            filename=filename,
            payload=payload,
            context=UploadContext(
                service_origin=service_origin,
                service_origin_n1=service_origin_n1,
                service_origin_n2=service_origin_n2,
            ),
            sheet_name=sheet_name,
        )

    @app.post("/api/uploads/helix", response_model=HelixUploadResponse)
    async def upload_helix(
        file: UploadFile = File(...),
        service_origin: str = Form(...),
        service_origin_n1: str = Form(...),
        service_origin_n2: str = Form(""),
        sheet_name: str = Form(""),
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        filename = file.filename or "helix.xlsx"
        suffix = Path(filename).suffix.lower()
        if suffix not in {".xlsx", ".xlsm", ".xls"}:
            raise HTTPException(status_code=400, detail="Solo se admiten ficheros Excel.")

        payload = await file.read()
        if not payload:
            raise HTTPException(status_code=400, detail="El fichero está vacío.")

        return dashboard_layer.ingest_helix_excel(
            filename=filename,
            payload=payload,
            context=UploadContext(
                service_origin=service_origin,
                service_origin_n1=service_origin_n1,
                service_origin_n2=service_origin_n2,
            ),
            sheet_name=sheet_name,
        )

    @app.get("/api/dashboard/context", response_model=ContextOptionsResponse)
    def dashboard_context(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
        return dashboard_layer.context_options(
            _resolve_context(
                cast(Settings, request.app.state.settings),
                service_origin,
                service_origin_n1,
                service_origin_n2,
            )
        )

    @app.get("/api/preferences", response_model=PreferencesResponse)
    def preferences(request: Request) -> dict[str, object]:
        current_settings = cast(Settings, request.app.state.settings)
        return current_settings.ui_defaults()

    @app.put("/api/preferences", response_model=PreferencesResponse)
    def update_preferences(
        payload: PreferencesUpdateRequest,
        request: Request,
    ) -> dict[str, object]:
        current_settings = cast(Settings, request.app.state.settings)
        persist_ui_prefs(current_settings.dotenv_path, payload.model_dump())
        return refresh_settings(request).ui_defaults()

    @app.put("/api/settings/service-origins", response_model=ContextOptionsResponse)
    def update_service_origins(
        payload: ServiceOriginHierarchyRequest,
        request: Request,
        dashboard_layer: DashboardService = Depends(get_dashboard_service),
    ) -> dict[str, object]:
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
                n1: list(dict.fromkeys([value.strip() for value in origin_n2_map.get(n1, []) if value.strip()]))
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

    @app.get("/api/dashboard/nps", response_model=DashboardResponse)
    def dashboard_nps(
        request: Request,
        service_origin: Optional[str] = None,
        service_origin_n1: Optional[str] = None,
        service_origin_n2: Optional[str] = None,
        pop_year: str = "Todos",
        pop_month: str = "Todos",
        nps_group: str = "Todos",
        comparison_dimension: str = "Palanca",
        gap_dimension: str = "Palanca",
        opportunity_dimension: str = "Palanca",
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
            comparison_dimension=comparison_dimension,
            gap_dimension=gap_dimension,
            opportunity_dimension=opportunity_dimension,
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
        nps_group: str = "Todos",
        min_similarity: float = 0.25,
        max_days_apart: int = 10,
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
            min_similarity=min_similarity,
            max_days_apart=max_days_apart,
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
        nps_group: str = "Todos",
        min_n: int = 200,
        min_similarity: float = 0.25,
        max_days_apart: int = 10,
        touchpoint_source: str = "",
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
                min_n=min_n,
                min_similarity=min_similarity,
                max_days_apart=max_days_apart,
                touchpoint_source=touchpoint_source,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        headers = {
            "Content-Disposition": (
                f"attachment; filename=\"{report.file_name}\"; "
                f"filename*=UTF-8''{quote(report.file_name)}"
            )
        }
        return Response(
            content=report.content,
            media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
            headers=headers,
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
        nps_group: str = "Todos",
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
