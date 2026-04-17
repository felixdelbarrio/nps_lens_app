from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from nps_lens.api.schemas import ContextOptionsResponse, SummaryResponse, UploadResponse
from nps_lens.domain.models import UploadContext
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.nps_service import NpsService
from nps_lens.settings import Settings


def _optional_context(
    service_origin: Optional[str],
    service_origin_n1: Optional[str],
    service_origin_n2: Optional[str],
) -> UploadContext | None:
    if not service_origin or not service_origin_n1:
        return None
    return UploadContext(
        service_origin=service_origin,
        service_origin_n1=service_origin_n1,
        service_origin_n2=service_origin_n2 or "",
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or Settings.from_env()
    repository = SqliteNpsRepository(app_settings.database_path)
    service = NpsService(repository=repository, settings=app_settings)

    app = FastAPI(title="NPS Lens API", version="2.0.0")
    app.state.settings = app_settings
    app.state.repository = repository
    app.state.service = service

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def get_service(request: Request) -> NpsService:
        return request.app.state.service

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/config", response_model=ContextOptionsResponse)
    def config(service_layer: NpsService = Depends(get_service)) -> dict[str, object]:
        return service_layer.context_options()

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
