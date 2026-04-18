import { useEffect } from "react";

import type { ServiceOriginHierarchyPayload } from "../api";
import type { ThemeMode } from "../theme";
import { Icon } from "./Icon";
import { NavigationTabs } from "./NavigationTabs";
import { ServiceOriginMaintenance } from "./ServiceOriginMaintenance";

export type SettingsTab = "appearance" | "advanced" | "maintenance";

type SettingsSheetProps = {
  open: boolean;
  activeTab: SettingsTab;
  onTabChange: (value: SettingsTab) => void;
  onClose: () => void;
  themeMode: ThemeMode;
  setThemeMode: (value: ThemeMode) => void;
  minSimilarity: number;
  setMinSimilarity: (value: number) => void;
  maxDaysApart: number;
  setMaxDaysApart: (value: number) => void;
  minN: number;
  setMinN: (value: number) => void;
  minNCross: number;
  setMinNCross: (value: number) => void;
  serviceOrigins: string[];
  serviceOriginN1Map: Record<string, string[]>;
  serviceOriginN2Map: Record<string, Record<string, string[]>>;
  hierarchySaving: boolean;
  onSaveHierarchy: (payload: ServiceOriginHierarchyPayload) => Promise<void>;
};

const SETTINGS_TABS = [
  { id: "appearance", label: "Apariencia y visualización" },
  { id: "advanced", label: "Ajustes avanzados" },
  { id: "maintenance", label: "Mantenimiento Service Origin" }
] as const;

export function SettingsSheet({
  open,
  activeTab,
  onTabChange,
  onClose,
  themeMode,
  setThemeMode,
  minSimilarity,
  setMinSimilarity,
  maxDaysApart,
  setMaxDaysApart,
  minN,
  setMinN,
  minNCross,
  setMinNCross,
  serviceOrigins,
  serviceOriginN1Map,
  serviceOriginN2Map,
  hierarchySaving,
  onSaveHierarchy
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
              Ajusta ambientación, umbrales analíticos y mantenimiento del catálogo de servicio.
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
        />

        {activeTab === "appearance" ? (
          <section className="settings-group">
            <div className="section-heading">
              <div>
                <h3>Apariencia y visualización</h3>
                <p className="secondary-copy">
                  El ambient light/dark se aplica sobre tokens BBVA y se conserva entre sesiones.
                </p>
              </div>
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
          </section>
        ) : null}

        {activeTab === "advanced" ? (
          <section className="settings-group">
            <div className="section-heading">
              <div>
                <h3>Umbrales avanzados</h3>
                <p className="secondary-copy">
                  Se restauran los cuatro parámetros operativos del flujo causal y de priorización.
                </p>
              </div>
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
              <label>
                <span>Mínimo N para oportunidades</span>
                <input
                  min={50}
                  onChange={(event) => setMinN(Number(event.target.value))}
                  step={10}
                  type="number"
                  value={minN}
                />
              </label>
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
          </section>
        ) : null}

        {activeTab === "maintenance" ? (
          <section className="settings-group">
            <div className="section-heading">
              <div>
                <h3>Mantenimiento Service Origin</h3>
                <p className="secondary-copy">
                  Gestiona jerarquías BUUG → N1 → N2 y persístelas en la configuración del producto.
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
      </aside>
    </div>
  );
}
