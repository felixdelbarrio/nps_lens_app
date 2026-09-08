import { useEffect, useMemo, useState } from "react";

import type { LinkingPayload, PlotlyFigureSpec } from "../api";
import { formatDisplayValue } from "../utils/numberFormat";
import { NavigationTabs } from "./NavigationTabs";
import { PlotFigure } from "./PlotFigure";
import { RecordTable } from "./RecordTable";

type LinkingWorkspaceProps = {
  linking: LinkingPayload;
  tab: string;
  onTabChange: (value: string) => void;
};

const SCENARIO_DETAIL_TABS = [
  { id: "helix", label: "Evidencia Helix" },
  { id: "voc", label: "Voz del cliente" }
];

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asRows(value: unknown): Array<Record<string, unknown>> {
  return Array.isArray(value)
    ? value.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
    : [];
}

function asString(value: unknown, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function asFigure(value: unknown): PlotlyFigureSpec | null {
  return value && typeof value === "object" && "data" in (value as Record<string, unknown>)
    ? (value as PlotlyFigureSpec)
    : null;
}

function displayValue(value: unknown, label?: string) {
  const formatted = formatDisplayValue(value, label);
  return formatted || asString(value, "—");
}

function normalizeComparableValue(value: unknown) {
  return asString(value).toLocaleLowerCase();
}

function linkedCountHeading(count: number, singular: string, plural: string) {
  return `${count} ${count === 1 ? singular : plural}`;
}

function incidentHref(record: Record<string, unknown>) {
  return asString(record.url ?? record.incident_id__href ?? record.incident_id__hyperlink);
}

function renderHelixCards(records: Array<Record<string, unknown>>) {
  if (!records.length) {
    return <p className="empty-state">Sin evidencia Helix visible.</p>;
  }
  return (
    <div className="evidence-card-grid">
      {records.map((record, index) => {
        const incidentId = asString(record.incident_id, "INC");
        const href = incidentHref(record);
        return (
          <article className="evidence-card" key={`helix-record-${index}`}>
            {href ? (
              <a className="evidence-pill evidence-pill-link" href={href} rel="noreferrer" target="_blank">
                {incidentId}
              </a>
            ) : (
              <span className="evidence-pill">{incidentId}</span>
            )}
            <p>{asString(record.summary)}</p>
          </article>
        );
      })}
    </div>
  );
}

function renderVocCards(records: Array<Record<string, unknown>>) {
  if (!records.length) {
    return <p className="empty-state">Sin evidencia VoC visible.</p>;
  }
  return (
    <div className="evidence-card-grid">
      {records.map((record, index) => (
        <article className="evidence-card" key={`voc-record-${index}`}>
          <div className="evidence-pill-row">
            <span className="evidence-pill">ID: {asString(record.comment_id, "-")}</span>
            <span className="evidence-pill">Fecha: {displayValue(record.date, "Fecha")}</span>
            <span className="evidence-pill">Score: {displayValue(record.nps, "Score")}</span>
            <span className="evidence-pill">Grupo: {asString(record.group, "-")}</span>
          </div>
          <p>{asString(record.comment)}</p>
        </article>
      ))}
    </div>
  );
}

function buildScenarioIdentityRows(
  activeCard: Record<string, unknown>,
  methodLabel: string,
  methodValue: string
): Array<{ label: string; value: string }> {
  const rows: Array<{ label: string; value: string }> = [];
  const seenValues = new Set<string>();
  const appendRow = (label: string, value: unknown) => {
    const text = asString(value);
    const comparable = normalizeComparableValue(text);
    if (!text || text === "n/d" || text === "—" || seenValues.has(comparable)) {
      return;
    }
    rows.push({ label, value: text });
    seenValues.add(comparable);
  };

  const title = asString(activeCard.title, "n/d");
  const anchorTopic = asString(activeCard.anchor_topic);
  const touchpoint = asString(activeCard.touchpoint);
  const sourceServiceN2 = asString(activeCard.helix_source_service_n2);
  const palanca = asString(activeCard.palanca);
  const subpalanca = asString(activeCard.subpalanca);

  appendRow(methodLabel || "Tópico observado", title);
  if (anchorTopic && normalizeComparableValue(anchorTopic) !== normalizeComparableValue(title)) {
    appendRow("Tópico NPS ancla", anchorTopic);
  }

  if (methodValue === "bbva_source_service_n2") {
    if (sourceServiceN2 && normalizeComparableValue(sourceServiceN2) !== normalizeComparableValue(title)) {
      appendRow("Source Service N2", sourceServiceN2);
    }
  } else {
    appendRow(methodValue === "broken_journeys" ? "Touchpoint detectado" : "Touchpoint afectado", touchpoint);
    if (methodValue !== "palanca_touchpoint") {
      appendRow("Palanca", palanca);
    }
    if (methodValue !== "domain_touchpoint") {
      appendRow("Subpalanca", subpalanca);
    }
    if (
      sourceServiceN2 &&
      methodValue === "executive_journeys" &&
      normalizeComparableValue(sourceServiceN2) !== normalizeComparableValue(touchpoint)
    ) {
      appendRow("Source Service N2", sourceServiceN2);
    }
  }

  appendRow("Organizaciones responsables observadas", asString(activeCard.support_organizations, "n/d"));
  appendRow(
    "Duración media histórica de resolución (semanas)",
    displayValue(activeCard.historical_resolution_weeks, "duración histórica")
  );
  return rows;
}

function buildScenarioMetricRows(
  spotlightMetrics: Array<Record<string, unknown>>
): Array<Record<string, unknown>> {
  const hiddenLabels = new Set([
    "Palanca",
    "Subpalanca",
    "Touchpoint afectado",
    "Touchpoint detectado",
    "Source Service N2",
    "Tópico NPS ancla",
  ]);
  return spotlightMetrics.filter((metric) => !hiddenLabels.has(asString(metric.label)));
}

export function LinkingWorkspace({ linking, tab, onTabChange }: LinkingWorkspaceProps) {
  const method = asRecord(linking.causal_method);
  const situation = asRecord(linking.situation);
  const narrative = asRecord(situation.narrative);
  const entitySummary = asRecord(linking.entity_summary);
  const scenarios = asRecord(linking.scenarios);
  const scenarioCards = asRows(scenarios.cards);
  const narrativeMetrics = asRows(narrative.metrics);
  const navigationItems = useMemo(() => {
    const items = asRows(linking.navigation).map((item) => ({
      id: asString(item.id),
      label: asString(item.label)
    }));
    return items.length
      ? items
      : [
          { id: "situation", label: "Situación del periodo" },
          { id: "entity-summary", label: "Resumen de evidencia" },
          { id: "scenarios", label: "Evidencia por escenario" }
        ];
  }, [linking.navigation]);
  const situationMetadata = asRows(situation.metadata);
  const entitySummaryKpis = asRows(entitySummary.kpis);
  const entitySummaryRows = asRows(entitySummary.table);
  const situationEvidence = asRecord(situation.evidence);
  const [activeChainIndex, setActiveChainIndex] = useState(0);
  const [scenarioDetailTab, setScenarioDetailTab] = useState("helix");
  const [scenarioEvidenceView, setScenarioEvidenceView] = useState<"table" | "cards">("table");

  useEffect(() => {
    if (!navigationItems.some((item) => item.id === tab) && navigationItems[0]) {
      onTabChange(navigationItems[0].id);
    }
  }, [navigationItems, onTabChange, tab]);

  useEffect(() => {
    setActiveChainIndex(0);
  }, [scenarioCards.length, linking.focus_group, method.value]);

  const activeCard = scenarioCards[activeChainIndex] || null;
  const activeHelixRecords = asRows(activeCard?.incident_records);
  const activeVocRecords = asRows(activeCard?.comment_records);
  const spotlightMetrics = asRows(activeCard?.spotlight_metrics);
  const scenarioMetricRows = useMemo(
    () => buildScenarioMetricRows(spotlightMetrics),
    [spotlightMetrics]
  );
  const scenarioIdentityRows = useMemo(
    () => buildScenarioIdentityRows(activeCard || {}, asString(method.label), asString(method.value)),
    [activeCard, method.label, method.value]
  );

  const evidenceHelixTable = activeHelixRecords.map((record) => ({
    ID: asString(record.incident_id),
    ID__href: incidentHref(record),
    "Evidencia Helix": asString(record.summary)
  }));
  const evidenceVocTable = activeVocRecords.map((record) => ({
    ID: asString(record.comment_id),
    Fecha: asString(record.date),
    Score: record.nps,
    Grupo: asString(record.group),
    Palanca: asString(record.palanca),
    Subpalanca: asString(record.subpalanca),
    Comentario: asString(record.comment)
  }));

  return (
    <section className="surface-card stack-panel linking-workspace">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Causalidad</p>
          <h2>Evidencia NPS ↔ Helix</h2>
          <p className="secondary-copy">
            {asString(method.summary, "Base cruzada entre incidencias y Voz del Cliente.")}
          </p>
        </div>
      </div>

      <NavigationTabs compact items={navigationItems} onChange={onTabChange} value={tab} />

      {tab === "situation" ? (
        <div className="linking-stack">
          <section className="hero-banner hero-banner-wow">
            <p className="eyebrow">{asString(narrative.kicker, "Evidencia observada")}</p>
            <h3>{asString(narrative.title, "Sin vínculos semánticos en esta ventana")}</h3>
            <p className="secondary-copy">{asString(narrative.summary)}</p>
            <div className="hero-metrics hero-metrics-wow">
              {narrativeMetrics.map((metric, index) => {
                const label = asString(metric.label);
                const isLeadMetric = label === "Método de agrupación";
                return (
                  <article
                    className={`hero-metric-card${isLeadMetric ? " hero-metric-card-lead" : ""}`}
                    key={`narrative-metric-${index}`}
                  >
                    <span>{label}</span>
                    <strong>{displayValue(metric.value, label)}</strong>
                    {asString(metric.hint) ? <p>{asString(metric.hint)}</p> : null}
                  </article>
                );
              })}
            </div>
          </section>

          {situationMetadata.length ? (
            <div className="context-pill-row narrative-pill-row">
              {situationMetadata.map((item, index) => (
                <span className="context-pill" key={`situation-meta-${index}`}>
                  <strong>{asString(item.label)}:</strong> {asString(item.value)}
                </span>
              ))}
            </div>
          ) : null}

          {asString(situation.note) ? (
            <article className="note-card">
              <p className="secondary-copy">{asString(situation.note)}</p>
            </article>
          ) : null}

          <section className="linking-panel">
            <div className="section-heading"><div>
              <h3>{asString(situationEvidence.title, "Evidencias")}</h3>
              <p className="secondary-copy">{asString(situationEvidence.subtitle)}</p>
            </div></div>
            <RecordTable
              emptyMessage={asString(situationEvidence.empty_state, "No hay evidencias disponibles.")}
              rows={asRows(situationEvidence.rows)}
            />
          </section>
        </div>
      ) : null}

      {tab === "entity-summary" ? (
        <div className="linking-stack">
          <div className="section-heading">
            <div>
              <h3>{asString(entitySummary.title)}</h3>
              <p className="secondary-copy">{asString(entitySummary.subtitle)}</p>
            </div>
          </div>

          <div className="metric-grid metric-grid-3">
            {entitySummaryKpis.map((metric, index) => (
              <article className="metric-card" key={`entity-summary-metric-${index}`}>
                <span>{asString(metric.label)}</span>
                <strong>{displayValue(metric.value, asString(metric.label))}</strong>
              </article>
            ))}
          </div>

          <section className="linking-panel">
            <div className="section-heading">
              <div>
                <h3>{asString(entitySummary.figure_title)}</h3>
              </div>
            </div>
            <PlotFigure
              emptyMessage={asString(entitySummary.empty_state, "No hay resumen de evidencia disponible.")}
              figure={asFigure(entitySummary.figure)}
              testId="linking-entity-summary-figure"
            />
          </section>

          <section className="linking-panel">
            <div className="section-heading">
              <div>
                <h3>{asString(entitySummary.table_title)}</h3>
              </div>
            </div>
            <RecordTable
              emptyMessage={asString(entitySummary.empty_state, "No hay detalle de evidencia disponible.")}
              rows={entitySummaryRows}
            />
          </section>
        </div>
      ) : null}

      {tab === "scenarios" ? (
        <div className="linking-stack">
          <div className="section-heading">
            <div>
              <h3>{asString(scenarios.title, "Evidencia por escenario")}</h3>
              <p className="secondary-copy">{asString(scenarios.subtitle)}</p>
            </div>
          </div>

          {!activeCard ? (
            <p className="empty-state">
              No se encontraron vínculos semánticos entre casos Helix y comentarios VoC en esta ventana.
            </p>
          ) : (
            <>
              <div className="scenario-nav">
                <button
                  className="secondary-button"
                  disabled={scenarioCards.length <= 1}
                  onClick={() =>
                    setActiveChainIndex((current) =>
                      scenarioCards.length ? (current - 1 + scenarioCards.length) % scenarioCards.length : 0
                    )
                  }
                  type="button"
                >
                  Anterior
                </button>
                <div className="scenario-nav-meta">
                  <strong>{`Escenario ${activeChainIndex + 1} de ${scenarioCards.length}`}</strong>
                  <span>{asString(activeCard.selection_label, asString(activeCard.title))}</span>
                </div>
                <button
                  className="secondary-button"
                  disabled={scenarioCards.length <= 1}
                  onClick={() =>
                    setActiveChainIndex((current) =>
                      scenarioCards.length ? (current + 1) % scenarioCards.length : 0
                    )
                  }
                  type="button"
                >
                  Ver siguiente
                </button>
              </div>

              <section className="spotlight-card spotlight-card-wow">
                <div className="spotlight-head">
                  <div className="spotlight-copy">
                    <p className="eyebrow">Evidencia observada</p>
                    <h3>{asString(activeCard.title, asString(activeCard.nps_topic))}</h3>
                    <p>{asString(activeCard.statement, asString(activeCard.chain_story))}</p>
                  </div>
                  <div className="spotlight-rank">{`#${asString(activeCard.rank, String(activeChainIndex + 1))}`}</div>
                </div>

                <div className="spotlight-flow">
                  {((activeCard.flow_steps as unknown[]) || []).map((step, index) => (
                    <span className="spotlight-step" key={`flow-step-${index}`}>
                      {asString(step)}
                    </span>
                  ))}
                </div>

                <div className="scenario-overview-grid">
                  <article className="scenario-fact-sheet">
                    <h4>Ficha descriptiva</h4>
                    <dl className="scenario-fact-list">
                      {scenarioIdentityRows.map((item) => (
                        <div className="scenario-fact-row" key={item.label}>
                          <dt>{item.label}</dt>
                          <dd>{item.value}</dd>
                        </div>
                      ))}
                    </dl>
                  </article>

                  <div className="scenario-evidence-stack">
                    <article className="scenario-fact-sheet">
                      <h4>{linkedCountHeading(activeHelixRecords.length, "incidencia enlazada", "incidencias enlazadas")}</h4>
                      <div className="evidence-pill-row">
                        {activeHelixRecords.length ? (
                          activeHelixRecords.slice(0, 6).map((record, index) => {
                            const incidentId = asString(record.incident_id, `INC-${index + 1}`);
                            const href = incidentHref(record);
                            return href ? (
                              <a
                                className="evidence-pill evidence-pill-link"
                                href={href}
                                key={`${incidentId}-${index}`}
                                rel="noreferrer"
                                target="_blank"
                              >
                                {incidentId}
                              </a>
                            ) : (
                              <span className="evidence-pill" key={`${incidentId}-${index}`}>
                                {incidentId}
                              </span>
                            );
                          })
                        ) : (
                          <span className="secondary-copy">Sin incidencias visibles para este escenario.</span>
                        )}
                      </div>
                    </article>

                    <article className="scenario-fact-sheet">
                      <h4>{linkedCountHeading(activeVocRecords.length, "comentario enlazado", "comentarios enlazados")}</h4>
                      <div className="evidence-pill-row">
                        {activeVocRecords.length ? (
                          activeVocRecords.slice(0, 6).map((record, index) => (
                            <span
                              className="evidence-pill"
                              key={`${asString(record.comment_id, `VOC-${index + 1}`)}-${index}`}
                            >
                              {asString(record.comment_id, `VOC-${index + 1}`)}
                            </span>
                          ))
                        ) : (
                          <span className="secondary-copy">Sin comentarios visibles para este escenario.</span>
                        )}
                      </div>
                    </article>
                  </div>
                </div>

                <div className="spotlight-metrics spotlight-metrics-compact">
                  {scenarioMetricRows.map((metric, index) => (
                    <article className="spotlight-metric" key={`spotlight-metric-${index}`}>
                      <span>{asString(metric.label)}</span>
                      <strong>{displayValue(metric.value, asString(metric.label))}</strong>
                    </article>
                  ))}
                </div>
              </section>

              <div className="evidence-toolbar">
                <div className="evidence-view-toggle" role="tablist">
                  <button
                    className={scenarioEvidenceView === "table" ? "is-active" : ""}
                    onClick={() => setScenarioEvidenceView("table")}
                    type="button"
                  >
                    Tabla
                  </button>
                  <button
                    className={scenarioEvidenceView === "cards" ? "is-active" : ""}
                    onClick={() => setScenarioEvidenceView("cards")}
                    type="button"
                  >
                    Cards
                  </button>
                </div>
                <div className="evidence-toolbar-note">
                  <strong>{activeHelixRecords.length}</strong> evidencias Helix visibles ·{" "}
                  <strong>{activeVocRecords.length}</strong> comentarios VoC visibles
                </div>
              </div>

              <NavigationTabs
                compact
                items={SCENARIO_DETAIL_TABS.map((item) => ({
                  ...item,
                  label:
                    item.id === "helix"
                      ? `Evidencia Helix (${activeHelixRecords.length})`
                      : item.id === "voc"
                        ? `Voz del cliente (${activeVocRecords.length})`
                        : item.label
                }))}
                onChange={setScenarioDetailTab}
                value={scenarioDetailTab}
              />

              {scenarioDetailTab === "helix" ? (
                scenarioEvidenceView === "table" ? (
                  <RecordTable emptyMessage="Sin evidencia Helix visible." rows={evidenceHelixTable} />
                ) : (
                  renderHelixCards(activeHelixRecords)
                )
              ) : null}

              {scenarioDetailTab === "voc" ? (
                scenarioEvidenceView === "table" ? (
                  <RecordTable emptyMessage="Sin evidencia VoC visible." rows={evidenceVocTable} />
                ) : (
                  renderVocCards(activeVocRecords)
                )
              ) : null}

            </>
          )}
        </div>
      ) : null}

    </section>
  );
}
