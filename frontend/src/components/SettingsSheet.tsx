import { useEffect } from "react";

import type { ServiceOriginHierarchyPayload } from "../api";
import type { ThemeMode } from "../theme";
import { Icon } from "./Icon";
import { NavigationTabs } from "./NavigationTabs";
import { ServiceOriginMaintenance } from "./ServiceOriginMaintenance";
import { EquivalenceMaintenance } from "./EquivalenceMaintenance";
import { SnapshotSettings } from "./TaxonomyStudio";
import type { TaxonomyContext } from "../api";
import { TelemetryPanel } from "./TelemetryPanel";
import { ColumnAliasMaintenance } from "./ColumnAliasMaintenance";

export type SettingsTab = "appearance" | "ingestion" | "column-aliases" | "advanced" | "maintenance" | "equivalences" | "telemetry" | "snapshots";

type SettingsSheetProps = {
  taxonomyContext?: TaxonomyContext;
  onTaxonomyChange?: () => Promise<void>;
  open: boolean;
  activeTab: SettingsTab;
  onTabChange: (value: SettingsTab) => void;
  onClose: () => void;
  themeMode: ThemeMode;
  setThemeMode: (value: ThemeMode) => void;
  downloadsPath: string;
  setDownloadsPath: (value: string) => void;
  helixBaseUrl: string;
  setHelixBaseUrl: (value: string) => void;
  reportDimensionAnalysis: "palanca" | "subpalanca";
  setReportDimensionAnalysis: (value: "palanca" | "subpalanca") => void;
  minSimilarity: number;
  setMinSimilarity: (value: number) => void;
  maxDaysApart: number;
  setMaxDaysApart: (value: number) => void;
  minNCross: number;
  setMinNCross: (value: number) => void;
  serviceOrigins: string[];
  serviceOriginN1Map: Record<string, string[]>;
  serviceOriginN2Map: Record<string, Record<string, string[]>>;
  hierarchySaving: boolean;
  onSaveHierarchy: (payload: ServiceOriginHierarchyPayload) => Promise<void>;
  onReprocess: () => Promise<void>;
  reprocessPending: boolean;
  actionsDisabled?: boolean;
};

const SETTINGS_TABS = [
  { id: "appearance", label: "Configuración" },
  { id: "ingestion", label: "Reglas de ingesta" },
  { id: "column-aliases", label: "Alias de columnas NPS" },
  { id: "advanced", label: "Ajustes avanzados" },
  { id: "equivalences", label: "Equivalencias" },
  { id: "snapshots", label: "Snapshots" },
  { id: "telemetry", label: "Telemetría" },
  { id: "maintenance", label: "Mantenimiento del Canal" }
] as const;

export function SettingsSheet({
  taxonomyContext = {},
  onTaxonomyChange = async () => {},
  open,
  activeTab,
  onTabChange,
  onClose,
  themeMode,
  setThemeMode,
  downloadsPath,
  setDownloadsPath,
  helixBaseUrl,
  setHelixBaseUrl,
  reportDimensionAnalysis,
  setReportDimensionAnalysis,
  minSimilarity,
  setMinSimilarity,
  maxDaysApart,
  setMaxDaysApart,
  minNCross,
  setMinNCross,
  serviceOrigins,
  serviceOriginN1Map,
  serviceOriginN2Map,
  hierarchySaving,
  onSaveHierarchy,
  onReprocess,
  reprocessPending,
  actionsDisabled = false
}: SettingsSheetProps) {
  useEffect(() => {
    if (!open) {
      return undefined;
    }
    function handleEscape(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, [onClose, open]);

  if (!open) {
    return null;
  }

  return (
    <div className="sheet-backdrop" onClick={onClose} role="presentation">
      <aside
        aria-labelledby="settings-sheet-title"
        aria-modal="true"
        className="settings-sheet"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
      >
        <header className="settings-sheet-header">
          <div className="settings-sheet-title-wrap">
            <p className="eyebrow">Configuración global</p>
            <h2 id="settings-sheet-title">Preferencias del producto</h2>
            <p className="secondary-copy">
              Ajusta ambientación, descargas, causalidad y mantenimiento del catálogo de servicio.
            </p>
          </div>
          <button aria-label="Cerrar configuración" className="icon-button" onClick={onClose} type="button">
            <Icon label="Cerrar configuración" name="settings" />
          </button>
        </header>

        <NavigationTabs
          compact
          items={SETTINGS_TABS.map((tab) => ({ ...tab }))}
          onChange={(value) => onTabChange(value as SettingsTab)}
          value={activeTab}
          disabled={actionsDisabled}
        />

        {activeTab === "appearance" ? (
          <section className="settings-group">
            <div className="section-heading">
              <div>
                <h3>Configuración</h3>
                <p className="secondary-copy">
                  La ambientación y la ruta de descarga se persisten en la configuración operativa del producto.
                </p>
              </div>
            </div>
            <div className="settings-section-stack">
              <article className="settings-subsection">
                <div className="settings-subsection-copy">
                  <h4>Apariencia</h4>
                  <p className="secondary-copy">
                    El ambient light/dark se apoya solo en tokens BBVA y se conserva entre sesiones.
                  </p>
                </div>
                <div className="field-grid">
                  <label className="field-span-2">
                    <span>Ambient</span>
                    <select
                      onChange={(event) => setThemeMode(event.target.value as ThemeMode)}
                      value={themeMode}
                    >
                      <option value="light">Light</option>
                      <option value="dark">Dark</option>
                    </select>
                  </label>
                </div>
              </article>

              <article className="settings-subsection">
                <div className="settings-subsection-copy">
                  <h4>Descargas</h4>
                  <p className="secondary-copy">
                    Todas las exportaciones generan una copia server-side en esta ruta validada.
                  </p>
                </div>
                <div className="field-grid">
                  <label className="field-span-2">
                    <span>Ruta de descarga</span>
                    <input
                      onChange={(event) => setDownloadsPath(event.target.value)}
                      placeholder="~/Downloads"
                      value={downloadsPath}
                    />
                    <small className="field-hint">
                      Si introduces una ruta relativa, se resolverá sobre tu directorio de usuario.
                    </small>
                  </label>
                </div>
              </article>

              <article className="settings-subsection">
                <div className="settings-subsection-copy">
                  <h4>Reporte</h4>
                  <p className="secondary-copy">
                    Define la dimensión que se mostrará en las slides analíticas del PPTX.
                  </p>
                </div>
                <div className="choice-grid" role="group" aria-label="Dimensión de análisis">
                  {(["palanca", "subpalanca"] as const).map((value) => (
                    <button
                      className={`choice-chip${reportDimensionAnalysis === value ? " is-selected" : ""}`}
                      disabled={actionsDisabled}
                      key={value}
                      onClick={() => setReportDimensionAnalysis(value)}
                      type="button"
                    >
                      {value === "palanca" ? "Palanca" : "Subpalanca"}
                    </button>
                  ))}
                </div>
              </article>

              <article className="settings-subsection">
                <div className="settings-subsection-copy">
                  <h4>Ruta base de Helix</h4>
                  <p className="secondary-copy">
                    Se usa para abrir incidencias desde tablas, escenarios causales y evidencia enlazada.
                  </p>
                </div>
                <div className="field-grid">
                  <label className="field-span-2">
                    <span>Ruta base de Helix</span>
                    <input
                      onChange={(event) => setHelixBaseUrl(event.target.value)}
                      placeholder="https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/"
                      value={helixBaseUrl}
                    />
                    <small className="field-hint">
                      La aplicación añadirá automáticamente el `Record ID` de la incidencia al final de esta ruta.
                    </small>
                  </label>
                </div>
              </article>
            </div>
          </section>
        ) : null}

        {activeTab === "ingestion" ? (
          <section className="settings-group">
            <div className="section-heading"><div>
              <h3>Reglas de ingesta</h3>
              <p className="secondary-copy">
                La clasificación se calcula automáticamente desde la puntuación NPS.
                Las etiquetas del archivo importado no intervienen en los resultados.
              </p>
            </div></div>
            <article className="settings-subsection">
              <div className="settings-subsection-copy">
                <h4>Clasificación automática NPS</h4>
                <p className="secondary-copy">Regla fija para todas las respuestas con una nota entera de 0 a 10.</p>
              </div>
              <ul>
                <li>≤6: detractor</li>
                <li>7–8: neutro</li>
                <li>≥9: promotor</li>
              </ul>
              <p className="field-hint">
                Las filas sin una puntuación válida se descartan y se detallan en el resultado de la carga.
                La regla también se aplica al histórico y a las nuevas publicaciones.
              </p>
            </article>
          </section>
        ) : null}

        {activeTab === "column-aliases" ? (
          <section className="settings-group">
            <div className="section-heading"><div>
              <h3>Alias de columnas NPS</h3>
              <p className="secondary-copy">
                Adapta nombres de cabeceras de nuevos formatos sin alterar sus datos.
              </p>
            </div></div>
            <ColumnAliasMaintenance
              context={taxonomyContext}
              disabled={actionsDisabled}
              key={`${taxonomyContext.service_origin || ""}:${taxonomyContext.service_origin_n1 || ""}`}
            />
          </section>
        ) : null}

        {activeTab === "advanced" ? (
          <section className="settings-group">
            <div className="section-heading">
              <div>
                <h3>Análisis causal</h3>
                <p className="secondary-copy">
                  El método causal ya se selecciona en los filtros de Causalidad; aquí se mantienen únicamente los umbrales operativos.
                </p>
              </div>
            </div>
            <div className="settings-section-stack">
              <article className="settings-subsection">
                <div className="settings-subsection-copy">
                  <h4>Parámetros de causalidad</h4>
                  <p className="secondary-copy">
                    Ajusta el matching causal y la ventana temporal que gobiernan el cruce Helix ↔ VoC.
                  </p>
                </div>
                <div className="field-grid">
                  <label>
                    <span>Similitud en la causalidad</span>
                    <input
                      max={1}
                      min={0.05}
                      onChange={(event) => setMinSimilarity(Number(event.target.value))}
                      step={0.05}
                      type="number"
                      value={minSimilarity}
                    />
                  </label>
                  <label>
                    <span>Ventana de días</span>
                    <input
                      max={30}
                      min={1}
                      onChange={(event) => setMaxDaysApart(Number(event.target.value))}
                      type="number"
                      value={maxDaysApart}
                    />
                  </label>
                </div>
              </article>

              <article className="settings-subsection">
                <div className="settings-subsection-copy">
                  <h4>Umbrales avanzados</h4>
                  <p className="secondary-copy">
                    Ajusta el volumen mínimo exigido a las comparativas cruzadas.
                  </p>
                </div>
                <div className="field-grid">
                  <label>
                    <span>Mínimo N para comparativas cruzadas</span>
                    <input
                      min={10}
                      onChange={(event) => setMinNCross(Number(event.target.value))}
                      step={10}
                      type="number"
                      value={minNCross}
                    />
                  </label>
                </div>
                <div className="settings-subsection-actions">
                  <button
                    className="secondary-button"
                    data-testid="reprocess-button"
                    onClick={() => void onReprocess()}
                    disabled={actionsDisabled || reprocessPending}
                    type="button"
                  >
                    {reprocessPending ? "Reprocesando..." : "Reprocesar agregados"}
                  </button>
                </div>
              </article>
            </div>
          </section>
        ) : null}

        {activeTab === "maintenance" ? (
          <section className="settings-group">
            <div className="section-heading">
              <div>
                <h3>Mantenimiento del Canal</h3>
                <p className="secondary-copy">
                  Gestiona compañías y la asignación opcional de N1/N2 de Helix a cada Canal.
                </p>
              </div>
            </div>
            <ServiceOriginMaintenance
              onSave={onSaveHierarchy}
              serviceOriginN1Map={serviceOriginN1Map}
              serviceOriginN2Map={serviceOriginN2Map}
              serviceOrigins={serviceOrigins}
              saving={hierarchySaving}
            />
          </section>
        ) : null}

        {activeTab === "equivalences" ? (
          <section className="settings-group">
            <div className="section-heading"><div>
              <h3>Unificar conceptos</h3>
              <p className="secondary-copy">
                Decide qué nombre verá el cliente y agrupa debajo todas las formas equivalentes de escribirlo.
              </p>
            </div></div>
            <EquivalenceMaintenance disabled={actionsDisabled} context={taxonomyContext} onChange={onTaxonomyChange} />
          </section>
        ) : null}

        {activeTab === "snapshots" ? <SnapshotSettings context={taxonomyContext} onChange={onTaxonomyChange} disabled={actionsDisabled} /> : null}

        {activeTab === "telemetry" ? (
          <section className="settings-group">
            <div className="section-heading"><div>
              <h3>Telemetría</h3>
              <p className="secondary-copy">Exporta un diagnóstico reproducible y seguro para mejorar el código.</p>
            </div></div>
            <TelemetryPanel disabled={actionsDisabled} />
          </section>
        ) : null}
      </aside>
    </div>
  );
}
