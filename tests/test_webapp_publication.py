from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from nps_lens.platform.webapp_preview import build_preview, extract_report, load_publication


def test_webapp_preview_loads_publication_and_embedded_report(tmp_path: Path) -> None:
    archive_path = tmp_path / "nps-lens-publicacion-20260827-100000.zip"
    publication = {
        "schema_version": "1.0",
        "generated_at": "2026-08-27T08:00:00Z",
        "filters": {"service_origin": "BBVA México", "month": "03"},
        "screens": {"dashboard": {}, "linking": {}, "data": {}},
        "manifest": {"report": "informe-exclusivo.pptx"},
    }
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("publication.json", json.dumps(publication))
        archive.writestr("informe-exclusivo.pptx", b"PPTX")

    payload = load_publication(archive_path)
    output = tmp_path / "preview"
    report_url = extract_report(archive_path, output, payload)
    target = build_preview(Path("webapp/apps-script"), output, payload, report_url=report_url)

    html = target.read_text(encoding="utf-8")
    assert report_url == "informe-exclusivo.pptx"
    assert (output / report_url).read_bytes() == b"PPTX"
    assert '"reportUrl":"informe-exclusivo.pptx"' in html
    assert "Validar y cargar" in html
    assert "Descargar diagnóstico JSON" in html


def test_apps_script_uses_archive_import_without_manual_drive_ids() -> None:
    publication_source = Path("webapp/apps-script/10_Publication.gs").read_text(encoding="utf-8")
    app_source = Path("webapp/apps-script/App.html").read_text(encoding="utf-8")

    assert "function importPublicationArchive(form)" in publication_source
    assert "setPublishedEditionFiles" not in publication_source
    assert "edition-file" not in app_source
    assert "report-file" not in app_source


def test_apps_script_converts_report_and_supports_admin_operations() -> None:
    root = Path("webapp/apps-script")
    publication = (root / "10_Publication.gs").read_text(encoding="utf-8")
    activity = (root / "20_Activity.gs").read_text(encoding="utf-8")
    administration = (root / "30_Administration.gs").read_text(encoding="utf-8")
    newsletter = (root / "40_Newsletter.gs").read_text(encoding="utf-8")
    manifest = json.loads((root / "appsscript.json").read_text(encoding="utf-8"))

    assert "application/vnd.google-apps.presentation" in publication
    assert "SlidesApp.openById" in publication
    assert "function getActivityReport(request)" in activity
    assert "user_email" in activity
    assert "Session.getEffectiveUser" in administration
    assert "_assertAdmin_(viewer)" in administration
    assert "function testNewsletter()" in newsletter
    assert "saveNewsletterRecipient" in newsletter
    assert "filter(item => item.active)" in newsletter
    assert manifest["dependencies"]["enabledAdvancedServices"][0]["serviceId"] == "drive"
