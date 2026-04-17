from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
import uvicorn
from dotenv import load_dotenv
from rich import print as rprint

from nps_lens.logging import setup_logging
from nps_lens.repositories.sqlite_repository import SqliteNpsRepository
from nps_lens.services.nps_service import NpsService
from nps_lens.settings import Settings
from nps_lens.domain.models import UploadContext

app = typer.Typer(add_completion=False)


def _service() -> NpsService:
    load_dotenv()
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    return NpsService(SqliteNpsRepository(settings.database_path), settings)


@app.command()
def serve(
    host: Optional[str] = typer.Option(None, help="API host"),
    port: Optional[int] = typer.Option(None, help="API port"),
) -> None:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    uvicorn.run(
        "nps_lens.api.app:create_app",
        factory=True,
        host=host or settings.api_host,
        port=port or settings.api_port,
    )


@app.command()
def ingest(
    excel_path: Path = typer.Argument(..., exists=True),
    service_origin: Optional[str] = typer.Option(None),
    service_origin_n1: Optional[str] = typer.Option(None),
    service_origin_n2: str = typer.Option(""),
) -> None:
    service = _service()
    context = UploadContext(
        service_origin=service_origin or service.settings.default_service_origin,
        service_origin_n1=service_origin_n1 or service.settings.default_service_origin_n1,
        service_origin_n2=service_origin_n2,
    )
    result = service.ingest_excel(
        filename=excel_path.name,
        payload=excel_path.read_bytes(),
        context=context,
    )
    rprint(result)


@app.command()
def summary(
    service_origin: Optional[str] = typer.Option(None),
    service_origin_n1: Optional[str] = typer.Option(None),
    service_origin_n2: str = typer.Option(""),
) -> None:
    service = _service()
    context = None
    if service_origin and service_origin_n1:
        context = UploadContext(
            service_origin=service_origin,
            service_origin_n1=service_origin_n1,
            service_origin_n2=service_origin_n2,
        )
    rprint(json.dumps(service.summary(context), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    app()
