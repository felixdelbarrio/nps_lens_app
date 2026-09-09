from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_apps_script_webapp_preserves_local_navigation_and_causal_detail() -> None:
    local = (ROOT / "frontend" / "src" / "App.tsx").read_text(encoding="utf-8")
    web = (ROOT / "webapp" / "apps-script" / "App.html").read_text(encoding="utf-8")
    shared_views = [
        "Evolución NPS",
        "Comentarios",
        "Causalidad",
        "Agregados por periodo",
        "NPS clásico vs detractores",
        "Brechas NPS",
        "Comparativas cruzadas",
        "Qué dicen los clientes",
    ]
    for label in shared_views:
        assert label.casefold() in local.casefold()
        assert label.casefold() in web.casefold()
    summary_block = local[local.index("const SUMMARY_TABS") : local.index("const NPS_TABS")]
    comments_block = local[local.index("const NPS_TABS") : local.index("const DATA_TABS")]
    assert summary_block.index('{ id: "volume-mix"') < summary_block.index('{ id: "cohorts"')
    assert comments_block.index('{ id: "topics"') < comments_block.index('{ id: "gaps"')
    assert '{ id: "comparison"' not in comments_block
    assert 'id: "cohorts"' not in comments_block
    assert "opportunities" not in comments_block
    assert web.index("['topics','Qué dicen los clientes']") < web.index("['gaps','Brechas NPS']")
    assert "['comparison'" not in web
    assert "['opportunities'" not in web
    assert "Oportunidades priorizadas" not in web
    assert "comments.gaps?.[state.scoreChannel]?.[state.gapDimension]" in web
    assert "comments.gaps?.[state.scoreChannel]?.[state.npsGroup]" not in web
    assert "view==='gaps'?'':`<label class=\"field\">Grupo score" in web
    assert "['volume-mix','Cómo y cuándo lo dicen'],['cohorts','Comparativas cruzadas']" in web
    assert "readonlyField('Canal','Web')" in web
    for causal_detail in ("Evidencia Helix", "Voz del cliente"):
        assert causal_detail in web
    for removed_detail in ("Ficha cuantitativa", "Heat map", "Changepoints + lag", "Lag en días"):
        assert removed_detail not in web
    for navigation in ("Anterior", "Ver siguiente", "scenarioIndex", "data-scenario-step"):
        assert navigation in web
    assert "rows(cards).map" not in web


def test_public_webapp_has_static_comment_filters_and_admin_is_explicit() -> None:
    web = (ROOT / "webapp" / "apps-script" / "App.html").read_text(encoding="utf-8")
    assert "data-filter" not in web
    assert "comments-channel" in web
    assert "comments-group" in web
    assert "comments-dimension" in web
    assert "activity-days" in web  # El único selector pertenece a la administración.
    assert "Newsletter" in web
    assert "Telemetría" in web
    assert "if(viewer.isAdmin)" in "".join(web.split())


def test_public_webapp_renders_every_local_rationale_from_the_snapshot() -> None:
    web = (ROOT / "webapp" / "apps-script" / "App.html").read_text(encoding="utf-8")

    for payload_field in (
        "historical.note",
        "daily_explanation_bullets",
        "topic.insights",
        "narrative.metrics",
        "situation.metadata",
        "situation.note",
        "entity.kpis",
        "situation.evidence",
    ):
        assert payload_field in web

    assert "data-scenario-detail" in web
    assert "data-deep-detail" not in web
    assert "data-evidence-view" in web
    assert "Asociaciones temporales observadas" not in web


def test_admin_navigation_and_activity_are_centralized() -> None:
    app = (ROOT / "webapp" / "apps-script" / "App.html").read_text(encoding="utf-8")
    index = (ROOT / "webapp" / "apps-script" / "Index.html").read_text(encoding="utf-8")
    design = (ROOT / "webapp" / "apps-script" / "DesignSystem.html").read_text(encoding="utf-8")

    assert 'data-admin-route="settings"' in index
    assert 'data-admin-route="newsletter"' in index
    assert 'data-admin-route="import"' in index
    assert "Adopción" in app
    assert "['evolution','Evolución NPS']" in app
    assert "contentTabs(settingsViews,view,'settings','Secciones de configuración')" in app
    assert 'class="settings-page"' in app
    assert 'class="settings-tabs"' not in app
    assert "activity-metrics" in app
    assert "saveEvolutionNpsSettings" in app
    assert "Probar newsletter" in app
    assert "recordActivityEvents" in app
    assert "recordTelemetry" not in app
    assert ".workspace-action" in design
    assert ".settings-page{display:flex;flex-direction:column;gap:24px;min-width:0;width:100%}" in design
    assert ".settings-tabs{" not in design
    assert ".activity-metrics{grid-column:1/-1}" in design


def test_initial_administrator_is_resolved_without_runtime_identity_inference() -> None:
    config = (ROOT / "webapp" / "apps-script" / "00_Config.gs").read_text(encoding="utf-8")
    webapp = (ROOT / "webapp" / "apps-script" / "60_WebApp.gs").read_text(encoding="utf-8")
    index = (ROOT / "webapp" / "apps-script" / "Index.html").read_text(encoding="utf-8")

    assert "Session.getActiveUser" in config
    assert "initialAdmin: 'felix.delbarrio@bbva.com'" in config
    assert "configuredAdmin || initialAdmin" in config
    assert "Session.getEffectiveUser" not in config
    assert "adminSource" in config
    assert "template.adminBodyClass" in webapp
    assert "template.accessRole" in webapp
    assert "template.appVersion" in webapp
    assert 'data-access-role="<?= accessRole ?>"' in index
    assert 'data-app-version="<?= appVersion ?>"' in index
