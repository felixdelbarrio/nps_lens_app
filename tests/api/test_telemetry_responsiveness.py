"""Slow Excel processing must leave the API event loop available."""

import asyncio
from threading import Event

import httpx
import pytest

from nps_lens.api.app import create_app
from nps_lens.settings import Settings


@pytest.mark.parametrize("kind", ["nps", "helix"])
def test_health_remains_available_during_excel_import(tmp_path, monkeypatch, kind):
    settings = Settings(
        data_dir=tmp_path,
        database_path=tmp_path / "nps.sqlite3",
        frontend_dist_dir=tmp_path / "dist",
        frontend_public_dir=tmp_path / "public",
        api_host="127.0.0.1",
        api_port=8000,
        default_service_origin="Bank",
        default_service_origin_n1="",
        allowed_service_origins=["Bank"],
        allowed_service_origin_n1={"Bank": []},
        log_level="INFO",
    )
    app = create_app(settings)
    started, release, completed = Event(), Event(), Event()

    def ingest(**_):
        started.set()
        release.wait(timeout=3)
        completed.set()
        raise ValueError("synthetic import failure")

    target, method = (
        (app.state.service, "ingest_excel")
        if kind == "nps"
        else (app.state.dashboard_service, "ingest_helix_excel")
    )
    monkeypatch.setattr(target, method, ingest)

    async def exercise():
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            upload = asyncio.create_task(
                client.post(
                    f"/api/uploads/{kind}",
                    data={"service_origin": "Bank"},
                    files={"file": ("synthetic.xlsx", b"synthetic")},
                )
            )
            try:
                assert await asyncio.to_thread(started.wait, 2)
                response = await asyncio.wait_for(client.get("/api/health"), timeout=1)
                assert response.status_code == 200
                assert not completed.is_set(), "Import blocked the API event loop"
            finally:
                release.set()
                await upload

    asyncio.run(exercise())
