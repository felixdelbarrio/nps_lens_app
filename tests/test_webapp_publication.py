from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from nps_lens.platform.webapp_preview import build_preview, extract_report, load_publication


def test_webapp_preview_loads_publication_and_embedded_report(tmp_path: Path) -> None:
    archive_path = tmp_path / "nps-lens-publicacion-20260827-100000.zip"
    publication = {
        "schema_version": "5.0",
        "generated_at": "2026-08-27T08:00:00Z",
        "filters": {"service_origin": "BBVA México", "month": "03"},
        "screens": {"dashboard": {}, "linking": {}, "data": {}},
        "manifest": {"report": "informe-ejecutivo.pptx"},
    }
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("publication.json", json.dumps(publication))
        archive.writestr("informe-ejecutivo.pptx", b"PPTX")

    payload = load_publication(archive_path)
    output = tmp_path / "preview"
    report_url = extract_report(archive_path, output, payload)
    target = build_preview(Path("webapp/apps-script"), output, payload, report_url=report_url)

    html = target.read_text(encoding="utf-8")
    assert report_url == "informe-ejecutivo.pptx"
    assert (output / report_url).read_bytes() == b"PPTX"
    assert '"reportUrl":"informe-ejecutivo.pptx"' in html
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
    assert "'owner_support_company'" in publication_source
    assert "'buug'" not in publication_source


def test_apps_script_converts_report_and_supports_admin_operations() -> None:
    root = Path("webapp/apps-script")
    publication = (root / "10_Publication.gs").read_text(encoding="utf-8")
    activity = (root / "20_Activity.gs").read_text(encoding="utf-8")
    config = (root / "00_Config.gs").read_text(encoding="utf-8")
    administration = (root / "30_Administration.gs").read_text(encoding="utf-8")
    newsletter = (root / "40_Newsletter.gs").read_text(encoding="utf-8")
    manifest = json.loads((root / "appsscript.json").read_text(encoding="utf-8"))

    assert "application/vnd.google-apps.presentation" in publication
    assert "SlidesApp" not in publication
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
    assert "https://www.googleapis.com/auth/presentations" not in manifest["oauthScopes"]


def test_telemetry_driven_optimizations_avoid_redundant_drive_and_sheet_reads() -> None:
    root = Path("webapp/apps-script")
    config = (root / "00_Config.gs").read_text(encoding="utf-8")
    publication = (root / "10_Publication.gs").read_text(encoding="utf-8")
    activity = (root / "20_Activity.gs").read_text(encoding="utf-8")
    webapp = (root / "60_WebApp.gs").read_text(encoding="utf-8")
    newsletter = (root / "40_Newsletter.gs").read_text(encoding="utf-8")
    app = (root / "App.html").read_text(encoding="utf-8")

    assert "version: '3.0.2'" in config
    assert "function getReportUrl()" not in publication
    assert "https://docs.google.com/presentation/d/" in publication
    assert "_publishedEdition_" not in publication
    assert "_loadSnapshot_" in publication
    assert "Utilities.gzip" in publication
    assert "Utilities.newBlob(bytes, 'application/gzip', 'snapshot.json.gz')" in publication
    assert "Utilities.ungzip(Utilities.newBlob(bytes))" not in publication
    assert "function _datasetSnapshot_" not in publication
    assert "edition.snapshots.data" in publication
    assert "_validateArchive_" in publication
    assert "'v' + NPS_LENS.version" in activity
    assert "event.occurredAt" in activity
    assert "serverP95Ms" in activity and "renderP95Ms" in activity
    assert "slowCalls" in activity
    assert "performanceEvents" in activity
    assert "row[3] === 'server_call' || row[3] === 'snapshot_load'" in activity
    assert "function exportActivityReport" not in activity
    assert "function getNewsletterSettings" not in newsletter
    assert "function getNewsletterWorkspace" in newsletter
    assert "_reportUrl_(publication.scopeKey, showEvolutionNps)" in newsletter
    assert "_presentationEntryUrl_" not in publication
    assert "function _compactSlidesProperty_(scopeKey)" in publication
    assert "report_without_evolution" in publication
    assert "showEvolutionNps ? publication.slidesFileId : compactId" in publication
    assert "reports.length !== 2" in publication
    assert "_configuredPublicationFolder_" not in newsletter
    assert "let activityCache" in app
    assert "downloadActivityReport" in app
    assert "exportActivityReport" not in app
    assert "if(viewer.reportUrl)" in app
    assert "viewer.shellDeferred = Boolean(publication)" in webapp
    assert "function getPublishedShell(" in webapp
    assert "event.parameter.presentation === '1'" not in webapp
    assert (
        "viewer.reportUrl = publication ? _reportUrl_(publication.scopeKey,viewer.evolutionNpsVisible) : ''"
        in webapp
    )
    assert "viewer.evolutionNpsVisible = _evolutionNpsVisible_()" in webapp
    assert "_encodedSnapshot_" in webapp
    assert "DecompressionStream" in app
    assert "snapshot_load" in app
    assert "viewer.publicationCatalog = _publicationCatalog_()" in webapp
    assert "delete edition.snapshots" in publication
    assert "NPS_LENS_SHELL_" in publication
    assert "function getPublishedDataset(" in webapp
    assert "offset, limit" not in webapp
    assert "source.deferred" in app
    assert "rpc('getPublicationCatalog')" not in app
    assert "rpc('getAdministration')" not in app
    assert "Promise.all([rpc('getNewsletter" not in app
    assert "saveEvolutionNpsSettings" in app
    assert "importPublicationArchive','ok'" in app
    assert "beginPublicationUpload" in publication
    assert "appendPublicationUploadChunk" in publication
    assert "commitPublicationUpload" in publication
    assert "bytesToBase64" in app
    assert "importPublicationArchive(form)" not in app
    assert "['publication','Publicación']" in app
    assert "function getPublicationSettings" in publication
    assert "_cacheKey_" in publication and "_cacheKey_" in activity and "_cacheKey_" in newsletter
    assert app.strip().endswith("</script>")
    assert "plotly-cartesian-2.35.2.min.js" in (root / "Index.html").read_text(encoding="utf-8")


def test_publication_import_preserves_global_evolution_visibility() -> None:
    publication = Path("webapp/apps-script/10_Publication.gs").read_text(encoding="utf-8")
    start = publication.index("function importPublicationArchive")
    end = publication.index("function _reportUrl_", start)
    import_body = publication[start:end]

    assert "evolutionNpsVisibleProperty" not in import_body
    assert "NPS_LENS_EVOLUTION_NPS_VISIBLE" not in import_body


def test_newsletter_locks_the_published_causal_method() -> None:
    root = Path("webapp/apps-script")
    webapp = (root / "60_WebApp.gs").read_text(encoding="utf-8")
    newsletter = (root / "40_Newsletter.gs").read_text(encoding="utf-8")
    app = (root / "App.html").read_text(encoding="utf-8")

    assert "?source=newsletter&scope=" in newsletter
    assert "viewer.causalMethodLocked" in webapp
    assert "viewer.causalMethodLocked?'disabled':''" in app


def test_newsletter_and_webapp_are_responsive_on_mobile() -> None:
    project_root = Path(__file__).resolve().parents[1]
    root = project_root / "webapp" / "apps-script"
    index = (root / "Index.html").read_text(encoding="utf-8")
    design = (root / "DesignSystem.html").read_text(encoding="utf-8")
    newsletter = (root / "40_Newsletter.gs").read_text(encoding="utf-8")
    archive_builder = (project_root / "src" / "nps_lens" / "platform" / "publication.py").read_text(
        encoding="utf-8"
    )

    assert (
        'name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"' in index
    )
    assert "html,body{width:100%;max-width:100%;overflow-x:hidden}" in design
    assert "@media(max-width:600px)" in design
    assert ".table-wrap table{width:max-content;min-width:100%}" in design
    assert 'class="email-card"' in newsletter
    assert 'class="quote-table"' in newsletter
    assert "@media only screen and (max-width:600px)" in newsletter
    assert 'class="email-card"' in archive_builder
    assert 'class="quote-table"' in archive_builder
    assert "@media only screen and (max-width:600px)" in archive_builder


def test_newsletter_is_short_and_uses_the_evolution_toggle_for_its_headline() -> None:
    project_root = Path(__file__).resolve().parents[1]
    newsletter = (project_root / "webapp" / "apps-script" / "40_Newsletter.gs").read_text(
        encoding="utf-8"
    )
    publication_renderer = (
        project_root / "src" / "nps_lens" / "platform" / "publication.py"
    ).read_text(encoding="utf-8")

    assert "const headline = showEvolutionNps ?" in newsletter
    assert (
        "_newsletterHtml_(insight, reportUrl, publication.scopeKey, showEvolutionNps)" in newsletter
    )
    assert (
        "_newsletterPlain_(insight, reportUrl, publication.scopeKey, showEvolutionNps)"
        in newsletter
    )
    for removed_heading in (
        "Lo que debes saber",
        "Principal foco ·",
        "De la voz del cliente a la operación",
    ):
        assert removed_heading not in newsletter
        assert removed_heading not in publication_renderer
    assert "La voz del cliente" in newsletter
    assert "Señales a vigilar" in newsletter
    assert newsletter.index("Ver análisis completo</a>") < newsletter.index("Explorar NPS Lens</a>")
    assert "background:#001391;color:#fff" in newsletter
    assert "background:#004481;color:#fff" in newsletter


def test_presentation_links_point_directly_to_google_slides() -> None:
    root = Path(__file__).resolve().parents[1] / "webapp" / "apps-script"
    publication = (root / "10_Publication.gs").read_text(encoding="utf-8")
    administration = (root / "30_Administration.gs").read_text(encoding="utf-8")
    newsletter = (root / "40_Newsletter.gs").read_text(encoding="utf-8")
    webapp = (root / "60_WebApp.gs").read_text(encoding="utf-8")

    combined = publication + administration + newsletter + webapp
    assert "_presentationEntryUrl_" not in combined
    assert "presentation=1" not in combined
    assert "window.top.location.replace" not in combined
    assert "https://docs.google.com/presentation/d/" in publication
    assert "const reportUrl = _reportUrl_(publication.scopeKey, showEvolutionNps);" in newsletter


def test_admin_cleanup_trashes_every_publication_artifact_and_invalidates_caches() -> None:
    setup = Path("webapp/apps-script/90_Setup.gs").read_text(encoding="utf-8")

    assert "function clearNpsLensCachesAndReports()" in setup
    assert "_assertAdmin_(viewer)" in setup
    assert "item.snapshotFileId" in setup
    assert "item.pptxFileId" in setup
    assert "item.slidesFileId" in setup
    assert "_compactPptxProperty_" in setup
    assert "_compactSlidesProperty_" in setup
    assert "setTrashed(true)" in setup
    assert "cacheEpochProperty" in setup
