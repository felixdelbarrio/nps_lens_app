from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_apps_script_webapp_preserves_local_navigation_and_causal_detail() -> None:
    local = (ROOT / "frontend" / "src" / "App.tsx").read_text(encoding="utf-8")
    web = (ROOT / "webapp" / "apps-script" / "App.html").read_text(encoding="utf-8")
    shared_views = [
        "Sumario del periodo",
        "Analítica NPS",
        "Incidencias ↔ NPS",
        "Agregados por periodo",
        "NPS clásico vs detractores",
        "Oportunidades priorizadas",
        "Comparativas cruzadas",
        "Qué dicen los clientes",
        "Cambios respecto al histórico",
    ]
    for label in shared_views:
        assert label.casefold() in local.casefold()
        assert label.casefold() in web.casefold()
    for causal_detail in (
        "Evidencia Helix",
        "Voz del cliente",
        "Matriz visual",
        "Ficha cuantitativa",
        "Heat map",
        "Changepoints + lag",
        "Lag en días",
    ):
        assert causal_detail in web
    for navigation in ("Anterior", "Ver siguiente", "scenarioIndex", "data-scenario-step"):
        assert navigation in web
    assert "rows(cards).map" not in web


def test_public_webapp_has_no_filter_controls_and_admin_is_explicit() -> None:
    web = (ROOT / "webapp" / "apps-script" / "App.html").read_text(encoding="utf-8")
    assert "data-filter" not in web
    assert "activity-days" in web  # El único selector pertenece a la administración.
    assert "Newsletter" in web
    assert "Telemetría" in web
    assert "if(viewer.isAdmin)" in "".join(web.split())


def test_admin_navigation_and_activity_are_centralized() -> None:
    app = (ROOT / "webapp" / "apps-script" / "App.html").read_text(encoding="utf-8")
    index = (ROOT / "webapp" / "apps-script" / "Index.html").read_text(encoding="utf-8")
    design = (ROOT / "webapp" / "apps-script" / "DesignSystem.html").read_text(encoding="utf-8")

    assert 'data-admin-route="settings"' in index
    assert 'data-admin-route="newsletter"' in index
    assert 'data-admin-route="import"' in index
    assert "Adopción" in app
    assert "Probar newsletter" in app
    assert "recordActivityEvents" in app
    assert "recordTelemetry" not in app
    assert ".workspace-action" in design


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
