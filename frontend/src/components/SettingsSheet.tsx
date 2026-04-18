import { useEffect } from "react";

import { Icon } from "./Icon";
import { NavigationTabs } from "./NavigationTabs";

type SettingsTab = "preferences" | "appearance" | "advanced";

type SettingsSheetProps = {
  open: boolean;
  activeTab: SettingsTab;
  onTabChange: (value: SettingsTab) => void;
  onClose: () => void;
  serviceOrigin: string;
  setServiceOrigin: (value: string) => void;
  serviceOriginN1: string;
  setServiceOriginN1: (value: string) => void;
  serviceOriginN2: string;
  setServiceOriginN2: (value: string) => void;
  popYear: string;
  setPopYear: (value: string) => void;
  popMonth: string;
  setPopMonth: (value: string) => void;
  npsGroup: string;
  setNpsGroup: (value: string) => void;
  minN: number;
  setMinN: (value: number) => void;
  minNCross: number;
  setMinNCross: (value: number) => void;
  serviceOrigins: string[];
  n1Options: string[];
  availableYears: string[];
  monthOptions: string[];
  npsGroups: string[];
};

const SETTINGS_TABS = [
  { id: "preferences", label: "Preferencias" },
  { id: "appearance", label: "Apariencia / visualización" },
  { id: "advanced", label: "Ajustes avanzados" }
] as const;

export function SettingsSheet({
  open,
  activeTab,
  onTabChange,
  onClose,
  serviceOrigin,
  setServiceOrigin,
  serviceOriginN1,
  setServiceOriginN1,
  serviceOriginN2,
  setServiceOriginN2,
  popYear,
  setPopYear,
  popMonth,
  setPopMonth,
  npsGroup,
  setNpsGroup,
  minN,
  setMinN,
  minNCross,
  setMinNCross,
  serviceOrigins,
  n1Options,
  availableYears,
  monthOptions,
  npsGroups
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
              Ajusta contexto, visualización y umbrales sin sacar la configuración al primer nivel.
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

        {activeTab === "preferences" ? (
          <section className="settings-group">
            <div className="section-heading">
              <div>
                <h3>Preferencias operativas</h3>
                <p className="secondary-copy">Contexto de servicio persistente para carga y lectura analítica.</p>
              </div>
            </div>
            <div className="field-grid">
              <label>
                <span>Service origin</span>
                <select onChange={(event) => setServiceOrigin(event.target.value)} value={serviceOrigin}>
                  {serviceOrigins.map((origin) => (
                    <option key={origin} value={origin}>
                      {origin}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>Service origin N1</span>
                <select onChange={(event) => setServiceOriginN1(event.target.value)} value={serviceOriginN1}>
                  {n1Options.map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field-span-2">
                <span>Service origin N2</span>
                <input
                  onChange={(event) => setServiceOriginN2(event.target.value)}
                  placeholder="Opcional"
                  value={serviceOriginN2}
                />
              </label>
            </div>
          </section>
        ) : null}

        {activeTab === "appearance" ? (
          <section className="settings-group">
            <div className="section-heading">
              <div>
                <h3>Visualización analítica</h3>
                <p className="secondary-copy">Controla el recorte temporal y el segmento que se presenta en insights.</p>
              </div>
            </div>
            <div className="field-grid">
              <label>
                <span>Año</span>
                <select onChange={(event) => setPopYear(event.target.value)} value={popYear}>
                  {availableYears.map((year) => (
                    <option key={year} value={year}>
                      {year}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>Mes</span>
                <select onChange={(event) => setPopMonth(event.target.value)} value={popMonth}>
                  {monthOptions.map((month) => (
                    <option key={month} value={month}>
                      {month}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field-span-2">
                <span>Grupo NPS</span>
                <select onChange={(event) => setNpsGroup(event.target.value)} value={npsGroup}>
                  {npsGroups.map((group) => (
                    <option key={group} value={group}>
                      {group}
                    </option>
                  ))}
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
                <p className="secondary-copy">Parámetros de robustez para comparativas y priorización.</p>
              </div>
            </div>
            <div className="field-grid">
              <label>
                <span>Min N oportunidades</span>
                <input min={10} onChange={(event) => setMinN(Number(event.target.value))} type="number" value={minN} />
              </label>
              <label>
                <span>Min N comparativas</span>
                <input
                  min={10}
                  onChange={(event) => setMinNCross(Number(event.target.value))}
                  type="number"
                  value={minNCross}
                />
              </label>
            </div>
          </section>
        ) : null}
      </aside>
    </div>
  );
}
