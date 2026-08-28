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
    assert 'data-access-role="admin"' in html
    assert 'data-app-version="local"' in html
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
    config = (root / "00_Config.gs").read_text(encoding="utf-8")
    administration = (root / "30_Administration.gs").read_text(encoding="utf-8")
    newsletter = (root / "40_Newsletter.gs").read_text(encoding="utf-8")
    manifest = json.loads((root / "appsscript.json").read_text(encoding="utf-8"))

    assert "application/vnd.google-apps.presentation" in publication
    assert "SlidesApp.openById" in publication
    assert "function getActivityReport(request)" in activity
    assert "user_email" in activity
    assert "initialAdmin" in config
    assert "_assertAdmin_(viewer)" in administration
    assert "function diagnoseNpsLensAccess()" in administration
    assert "ScriptApp.requireAllScopes" in administration
    assert "configuredAdmins || viewer.email" in administration
    assert "SpreadsheetApp.create('NPS Lens · Administración')" in administration
    assert "console.log(JSON.stringify(result))" in administration
    assert "function testNewsletter()" in newsletter
    assert "saveNewsletterRecipient" in newsletter
    assert "filter(item => item.active)" in newsletter
    assert "newsletterFrom: 'nps-lens.group@bbva.com'" in config
    assert "newsletterSenderName: 'NPS Lens'" in config
    assert "Gmail.Users.Settings.SendAs.list('me')" in newsletter
    assert "Gmail.Users.Messages.send({raw}, 'me')" in newsletter
    assert "MailApp.sendEmail" not in newsletter
    assert [
        service["serviceId"] for service in manifest["dependencies"]["enabledAdvancedServices"]
    ] == ["drive", "gmail"]
    assert "https://www.googleapis.com/auth/gmail.send" in manifest["oauthScopes"]


def test_telemetry_driven_optimizations_avoid_redundant_drive_and_sheet_reads() -> None:
    root = Path("webapp/apps-script")
    config = (root / "00_Config.gs").read_text(encoding="utf-8")
    publication = (root / "10_Publication.gs").read_text(encoding="utf-8")
    activity = (root / "20_Activity.gs").read_text(encoding="utf-8")
    administration = (root / "30_Administration.gs").read_text(encoding="utf-8")
    newsletter = (root / "40_Newsletter.gs").read_text(encoding="utf-8")
    app = (root / "App.html").read_text(encoding="utf-8")

    assert "version: '2.4.0'" in config
    assert "function getReportUrl()" not in publication
    assert "https://docs.google.com/presentation/d/" in publication
    assert "_publishedEdition_" not in administration
    assert "'v' + NPS_LENS.version" in activity
    assert "event.occurredAt" in activity
    assert "serverP95Ms" in activity and "renderP95Ms" in activity
    assert "slowCalls" in activity
    assert "performanceEvents" in activity
    assert "function exportActivityReport" not in activity
    assert "_publishedEdition_" not in newsletter
    assert "let activityCache" in app
    assert "downloadActivityReport" in app
    assert "exportActivityReport" not in app
    assert "if(viewer.reportUrl)" in app
