import { useEffect, useMemo, useState } from "react";

import type { ServiceOriginHierarchyPayload } from "../api";

type ServiceOriginMaintenanceProps = {
  serviceOrigins: string[];
  serviceOriginN1Map: Record<string, string[]>;
  serviceOriginN2Map: Record<string, Record<string, string[]>>;
  onSave: (payload: ServiceOriginHierarchyPayload) => Promise<void> | void;
  saving: boolean;
};

type ServiceOriginDraftSeed = {
  serviceOrigins: string[];
  serviceOriginN1Map: Record<string, string[]>;
  serviceOriginN2Map: Record<string, Record<string, string[]>>;
};

function dedupe(values: string[]) {
  return Array.from(new Set(values.map((value) => value.trim()).filter(Boolean)));
}

function buildDraft({
  serviceOrigins,
  serviceOriginN1Map,
  serviceOriginN2Map
}: ServiceOriginDraftSeed): ServiceOriginHierarchyPayload {
  const normalizedOrigins = dedupe(serviceOrigins);
  const normalizedN1Map = Object.fromEntries(
    normalizedOrigins.map((origin) => [origin, dedupe(serviceOriginN1Map[origin] || [])])
  );
  const normalizedN2Map = Object.fromEntries(
    normalizedOrigins.map((origin) => [
      origin,
      Object.fromEntries(
        normalizedN1Map[origin].map((n1) => [n1, dedupe(serviceOriginN2Map[origin]?.[n1] || [])])
      )
    ])
  );
  return {
    service_origins: normalizedOrigins,
    service_origin_n1_map: normalizedN1Map,
    service_origin_n2_map: normalizedN2Map
  };
}

export function ServiceOriginMaintenance({
  serviceOrigins,
  serviceOriginN1Map,
  serviceOriginN2Map,
  onSave,
  saving
}: ServiceOriginMaintenanceProps) {
  const sourceFingerprint = useMemo(
    () =>
      JSON.stringify({
        serviceOrigins,
        serviceOriginN1Map,
        serviceOriginN2Map
      }),
    [serviceOriginN1Map, serviceOriginN2Map, serviceOrigins]
  );
  const [draft, setDraft] = useState(() =>
    buildDraft({
      serviceOrigins,
      serviceOriginN1Map,
      serviceOriginN2Map
    })
  );
  const [selectedOrigin, setSelectedOrigin] = useState(serviceOrigins[0] || "");
  const [selectedN1, setSelectedN1] = useState(serviceOriginN1Map[serviceOrigins[0] || ""]?.[0] || "");
  const [newOrigin, setNewOrigin] = useState("");
  const [newN1, setNewN1] = useState("");
  const [newN2, setNewN2] = useState("");

  useEffect(() => {
    const nextDraft = buildDraft({
      serviceOrigins,
      serviceOriginN1Map,
      serviceOriginN2Map
    });
    setDraft(nextDraft);
    const nextOrigin = nextDraft.service_origins[0] || "";
    setSelectedOrigin((current) =>
      current && nextDraft.service_origins.includes(current) ? current : nextOrigin
    );
    const nextN1 = nextDraft.service_origin_n1_map[nextOrigin]?.[0] || "";
    setSelectedN1((current) =>
      current && nextDraft.service_origin_n1_map[selectedOrigin || nextOrigin]?.includes(current)
        ? current
        : nextN1
    );
  }, [selectedOrigin, serviceOriginN1Map, serviceOriginN2Map, serviceOrigins, sourceFingerprint]);

  const originOptions = draft.service_origins;
  const n1Options = draft.service_origin_n1_map[selectedOrigin] || [];
  const n2Options = draft.service_origin_n2_map[selectedOrigin]?.[selectedN1] || [];

  function handleAddOrigin() {
    const candidate = newOrigin.trim();
    if (!candidate) {
      return;
    }
    setDraft((current) => {
      if (current.service_origins.includes(candidate)) {
        return current;
      }
      return {
        service_origins: [...current.service_origins, candidate],
        service_origin_n1_map: {
          ...current.service_origin_n1_map,
          [candidate]: []
        },
        service_origin_n2_map: {
          ...current.service_origin_n2_map,
          [candidate]: {}
        }
      };
    });
    setSelectedOrigin(candidate);
    setSelectedN1("");
    setNewOrigin("");
  }

  function handleRemoveOrigin(origin: string) {
    setDraft((current) => {
      const nextOrigins = current.service_origins.filter((value) => value !== origin);
      const nextN1Map = Object.fromEntries(
        Object.entries(current.service_origin_n1_map).filter(([key]) => key !== origin)
      );
      const nextN2Map = Object.fromEntries(
        Object.entries(current.service_origin_n2_map).filter(([key]) => key !== origin)
      );
      return {
        service_origins: nextOrigins,
        service_origin_n1_map: nextN1Map,
        service_origin_n2_map: nextN2Map
      };
    });
    if (selectedOrigin === origin) {
      setSelectedOrigin(originOptions.find((value) => value !== origin) || "");
      setSelectedN1("");
    }
  }

  function handleAddN1() {
    const candidate = newN1.trim();
    if (!candidate || !selectedOrigin) {
      return;
    }
    setDraft((current) => {
      const nextValues = dedupe([...(current.service_origin_n1_map[selectedOrigin] || []), candidate]);
      return {
        ...current,
        service_origin_n1_map: {
          ...current.service_origin_n1_map,
          [selectedOrigin]: nextValues
        },
        service_origin_n2_map: {
          ...current.service_origin_n2_map,
          [selectedOrigin]: {
            ...(current.service_origin_n2_map[selectedOrigin] || {}),
            [candidate]: current.service_origin_n2_map[selectedOrigin]?.[candidate] || []
          }
        }
      };
    });
    setSelectedN1(candidate);
    setNewN1("");
  }

  function handleRemoveN1(origin: string, n1: string) {
    setDraft((current) => {
      const nextN1Values = (current.service_origin_n1_map[origin] || []).filter((value) => value !== n1);
      const nextOriginN2Map = Object.fromEntries(
        Object.entries(current.service_origin_n2_map[origin] || {}).filter(([key]) => key !== n1)
      );
      return {
        ...current,
        service_origin_n1_map: {
          ...current.service_origin_n1_map,
          [origin]: nextN1Values
        },
        service_origin_n2_map: {
          ...current.service_origin_n2_map,
          [origin]: nextOriginN2Map
        }
      };
    });
    if (selectedN1 === n1) {
      setSelectedN1(n1Options.find((value) => value !== n1) || "");
    }
  }

  function handleAddN2() {
    const candidate = newN2.trim();
    if (!candidate || !selectedOrigin || !selectedN1) {
      return;
    }
    setDraft((current) => {
      const nextValues = dedupe([
        ...(current.service_origin_n2_map[selectedOrigin]?.[selectedN1] || []),
        candidate
      ]);
      return {
        ...current,
        service_origin_n2_map: {
          ...current.service_origin_n2_map,
          [selectedOrigin]: {
            ...(current.service_origin_n2_map[selectedOrigin] || {}),
            [selectedN1]: nextValues
          }
        }
      };
    });
    setNewN2("");
  }

  function handleRemoveN2(origin: string, n1: string, n2: string) {
    setDraft((current) => ({
      ...current,
      service_origin_n2_map: {
        ...current.service_origin_n2_map,
        [origin]: {
          ...(current.service_origin_n2_map[origin] || {}),
          [n1]: (current.service_origin_n2_map[origin]?.[n1] || []).filter(
            (value) => value !== n2
          )
        }
      }
    }));
  }

  async function handleSave() {
    await onSave(draft);
  }

  const totalN1 = originOptions.reduce(
    (sum, origin) => sum + (draft.service_origin_n1_map[origin]?.length || 0),
    0
  );
  const totalN2 = originOptions.reduce(
    (sum, origin) =>
      sum +
      Object.values(draft.service_origin_n2_map[origin] || {}).reduce(
        (innerSum, values) => innerSum + values.length,
        0
      ),
    0
  );

  return (
    <section className="maintenance-shell">
      <div className="metric-grid maintenance-metrics">
        <article className="metric-card">
          <span>BUUG activas</span>
          <strong>{originOptions.length}</strong>
        </article>
        <article className="metric-card">
          <span>N1 configurados</span>
          <strong>{totalN1}</strong>
        </article>
        <article className="metric-card">
          <span>N2 disponibles</span>
          <strong>{totalN2}</strong>
        </article>
      </div>

      <div className="maintenance-layout">
        <section className="surface-card maintenance-column">
          <div className="section-heading">
            <div>
              <h3>BUUG</h3>
              <p className="secondary-copy">
                Define las unidades disponibles para el contexto de servicio.
              </p>
            </div>
          </div>
          <div className="chip-stack">
            {originOptions.map((origin) => (
              <div className="editable-chip" key={origin}>
                <button
                  className={`choice-chip${selectedOrigin === origin ? " is-selected" : ""}`}
                  onClick={() => {
                    setSelectedOrigin(origin);
                    setSelectedN1(draft.service_origin_n1_map[origin]?.[0] || "");
                  }}
                  type="button"
                >
                  {origin}
                </button>
                <button
                  aria-label={`Eliminar ${origin}`}
                  className="chip-remove"
                  onClick={() => handleRemoveOrigin(origin)}
                  type="button"
                >
                  ×
                </button>
              </div>
            ))}
          </div>
          <div className="inline-editor">
            <input
              onChange={(event) => setNewOrigin(event.target.value)}
              placeholder="Añadir BUUG"
              value={newOrigin}
            />
            <button className="secondary-button" onClick={handleAddOrigin} type="button">
              Añadir
            </button>
          </div>
        </section>

        <section className="surface-card maintenance-column">
          <div className="section-heading">
            <div>
              <h3>N1</h3>
              <p className="secondary-copy">
                {selectedOrigin
                  ? `Catálogo operativo asociado a ${selectedOrigin}.`
                  : "Selecciona una BUUG para editar su catálogo N1."}
              </p>
            </div>
          </div>
          <div className="chip-stack">
            {n1Options.map((n1) => (
              <div className="editable-chip" key={n1}>
                <button
                  className={`choice-chip${selectedN1 === n1 ? " is-selected" : ""}`}
                  onClick={() => setSelectedN1(n1)}
                  type="button"
                >
                  {n1}
                </button>
                <button
                  aria-label={`Eliminar ${n1}`}
                  className="chip-remove"
                  onClick={() => handleRemoveN1(selectedOrigin, n1)}
                  type="button"
                >
                  ×
                </button>
              </div>
            ))}
          </div>
          <div className="inline-editor">
            <input
              disabled={!selectedOrigin}
              onChange={(event) => setNewN1(event.target.value)}
              placeholder="Añadir N1"
              value={newN1}
            />
            <button className="secondary-button" onClick={handleAddN1} type="button">
              Añadir
            </button>
          </div>
        </section>
      </div>

      <section className="surface-card maintenance-column">
        <div className="section-heading section-heading-inline">
          <div>
            <h3>N2 por combinación</h3>
            <p className="secondary-copy">
              {selectedOrigin && selectedN1
                ? `Define los N2 disponibles para ${selectedOrigin} · ${selectedN1}.`
                : "Selecciona BUUG y N1 para editar el catálogo N2."}
            </p>
          </div>
          <button className="primary-button" onClick={() => void handleSave()} type="button">
            {saving ? "Guardando..." : "Guardar jerarquía"}
          </button>
        </div>

        <div className="chip-stack">
          {n2Options.map((n2) => (
            <div className="editable-chip" key={n2}>
              <span className="choice-chip is-static">{n2}</span>
              <button
                aria-label={`Eliminar ${n2}`}
                className="chip-remove"
                onClick={() => handleRemoveN2(selectedOrigin, selectedN1, n2)}
                type="button"
              >
                ×
              </button>
            </div>
          ))}
        </div>
        <div className="inline-editor">
          <input
            disabled={!selectedOrigin || !selectedN1}
            onChange={(event) => setNewN2(event.target.value)}
            placeholder="Añadir N2"
            value={newN2}
          />
          <button className="secondary-button" onClick={handleAddN2} type="button">
            Añadir
          </button>
        </div>
      </section>
    </section>
  );
}
