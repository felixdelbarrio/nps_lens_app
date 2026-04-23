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
  { id: "voc", label: "Voz del cliente" },
  { id: "matrix", label: "Matriz visual" },
  { id: "detail", label: "Ficha cuantitativa" },
  { id: "heat", label: "Heat map" },
  { id: "cp", label: "Changepoints + lag" },
  { id: "lag", label: "Lag en días" }
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

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map((item) => asString(item)).filter(Boolean) : [];
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

function getTopicName(row: Record<string, unknown>) {
  return asString(row["Tópico NPS"] ?? row.nps_topic ?? row.topic ?? row.label);
}

function normalizeTopicLabel(value: unknown) {
  return String(value ?? "")
    .replace(/^TOP\s+\d+\s+·\s+/i, "")
    .trim();
}

function filterArrayValue<T>(value: T, indexes: number[]): T {
  return Array.isArray(value) ? (indexes.map((index) => value[index]) as T) : value;
}

function buildTopicsTrendingFigure(
  baseFigure: PlotlyFigureSpec | null,
  selectedTopic: string
): PlotlyFigureSpec | null {
  if (!baseFigure?.data?.length) {
    return null;
  }
  if (selectedTopic === "Todos") {
    return baseFigure;
  }

  const [firstTrace, ...restTraces] = baseFigure.data;
  if (!firstTrace || typeof firstTrace !== "object" || firstTrace === null) {
    return baseFigure;
  }

  const traceRecord = firstTrace as Record<string, unknown>;
  const yValues = Array.isArray(traceRecord.y) ? traceRecord.y : [];
  const matchingIndexes = yValues
    .map((label, index) => ({ label: normalizeTopicLabel(label), index }))
    .filter((item) => item.label === selectedTopic)
    .map((item) => item.index);

  if (!matchingIndexes.length) {
    return null;
  }

  const filteredTrace = Object.fromEntries(
    Object.entries(traceRecord).map(([key, value]) => [key, filterArrayValue(value, matchingIndexes)])
  );

  return {
    ...baseFigure,
    data: [filteredTrace, ...restTraces]
  };
}

function renderHelixCards(records: Array<Record<string, unknown>>) {
  if (!records.length) {
    return <p className="empty-state">Sin evidencia Helix visible.</p>;
  }
  return (
    <div className="evidence-card-grid">
      {records.map((record, index) => {
        const incidentId = asString(record.incident_id, "INC");
        const href = asString(record.url);
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
            <span className="evidence-pill">NPS: {displayValue(record.nps, "NPS")}</span>
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
  methodLabel: string
): Array<{ label: string; value: string }> {
  const touchpointValue =
    asString(activeCard.helix_source_service_n2) || asString(activeCard.touchpoint) || "n/d";

  return [
    { label: methodLabel || "Escenario causal", value: asString(activeCard.title, "n/d") },
    { label: "Tópico NPS ancla", value: asString(activeCard.anchor_topic, "n/d") },
    {
      label: asString(activeCard.helix_source_service_n2) ? "Source Service N2" : "Touchpoint afectado",
      value: touchpointValue
    },
    { label: "Palanca", value: asString(activeCard.palanca, "n/d") },
    { label: "Subpalanca", value: asString(activeCard.subpalanca, "n/d") },
    { label: "Owner", value: asString(activeCard.owner_role, "n/d") },
    { label: "Lane de acción", value: asString(activeCard.action_lane, "n/d") },
    { label: "ETA", value: displayValue(activeCard.eta_weeks, "ETA (semanas)") }
  ].filter((item) => item.value && item.value !== "n/d");
}

function buildScenarioMetricRows(
  spotlightMetrics: Array<Record<string, unknown>>
): Array<Record<string, unknown>> {
  const hiddenLabels = new Set([
    "Palanca",
    "Subpalanca",
    "Touchpoint afectado",
    "Source Service N2",
    "Tópico NPS ancla",
    "Owner"
  ]);
  return spotlightMetrics.filter((metric) => !hiddenLabels.has(asString(metric.label)));
}

export function LinkingWorkspace({ linking, tab, onTabChange }: LinkingWorkspaceProps) {
  const method = asRecord(linking.causal_method);
  const situation = asRecord(linking.situation);
  const entitySummary = asRecord(linking.entity_summary);
  const scenarios = asRecord(linking.scenarios);
  const deepDive = asRecord(linking.deep_dive);
  const banner = asRecord(scenarios.banner);
  const scenarioCards = asRows(scenarios.cards);
  const bannerMetrics = asRows(banner.metrics);
  const navigationItems = useMemo(() => {
    const items = asRows(linking.navigation).map((item) => ({
      id: asString(item.id),
      label: asString(item.label)
    }));
    return items.length
      ? items
      : [
          { id: "situation", label: "Situación del periodo" },
          { id: "entity-summary", label: "Resumen causal" },
          { id: "scenarios", label: "Análisis de escenarios causales" },
          { id: "nps-deep-dive", label: "Análisis de Tópicos de NPS afectados" }
        ];
  }, [linking.navigation]);
  const situationKpis = asRows(situation.kpis);
  const situationMetadata = asRows(situation.metadata);
  const entitySummaryKpis = asRows(entitySummary.kpis);
  const entitySummaryRows = asRows(entitySummary.table);
  const deepDiveTabs = asRows(deepDive.tabs).map((item) => ({
    id: asString(item.id),
    label: asString(item.label)
  }));
  const deepDiveTopicFilterSpec = asRecord(deepDive.topic_filter);
  const deepDiveTopicOptions = asStringArray(deepDiveTopicFilterSpec.options);
  const [activeChainIndex, setActiveChainIndex] = useState(0);
  const [scenarioDetailTab, setScenarioDetailTab] = useState("helix");
  const [scenarioEvidenceView, setScenarioEvidenceView] = useState<"table" | "cards">("table");
  const [deepDiveTopicFilter, setDeepDiveTopicFilter] = useState("Todos");
  const [deepDiveTab, setDeepDiveTab] = useState("ranking");

  useEffect(() => {
    if (!navigationItems.some((item) => item.id === tab) && navigationItems[0]) {
      onTabChange(navigationItems[0].id);
    }
  }, [navigationItems, onTabChange, tab]);

  useEffect(() => {
    setActiveChainIndex(0);
  }, [scenarioCards.length, linking.focus_group, method.value]);

  useEffect(() => {
    if (!deepDiveTabs.length) {
      return;
    }
    if (!deepDiveTabs.some((item) => item.id === deepDiveTab)) {
      setDeepDiveTab(deepDiveTabs[0].id);
    }
  }, [deepDiveTab, deepDiveTabs]);

  useEffect(() => {
    if (tab === "nps-deep-dive") {
      setDeepDiveTopicFilter("Todos");
    }
  }, [linking.focus_group, method.value, tab]);

  useEffect(() => {
    if (!deepDiveTopicOptions.length) {
      setDeepDiveTopicFilter("Todos");
      return;
    }
    if (!deepDiveTopicOptions.includes(deepDiveTopicFilter)) {
      setDeepDiveTopicFilter("Todos");
    }
  }, [deepDiveTopicFilter, deepDiveTopicOptions]);

  const activeCard = scenarioCards[activeChainIndex] || null;
  const activeHelixRecords = asRows(activeCard?.incident_records);
  const activeVocRecords = asRows(activeCard?.comment_records);
  const detailTable = asRows(activeCard?.detail_table);
  const spotlightMetrics = asRows(activeCard?.spotlight_metrics);
  const scenarioMetricRows = useMemo(
    () => buildScenarioMetricRows(spotlightMetrics),
    [spotlightMetrics]
  );
  const scenarioIdentityRows = useMemo(
    () => buildScenarioIdentityRows(activeCard || {}, asString(method.label)),
    [activeCard, method.label]
  );
  const rankingRows = asRows(asRecord(deepDive.ranking).rows);
  const evidenceRows = asRows(asRecord(deepDive.evidence).rows);
  const baseTrendingFigure = asFigure(asRecord(deepDive.trending).figure);
  const filteredTrendingFigure = useMemo(
    () => buildTopicsTrendingFigure(baseTrendingFigure, deepDiveTopicFilter),
    [baseTrendingFigure, deepDiveTopicFilter]
  );
  const filteredRankingRows = useMemo(
    () =>
      deepDiveTopicFilter === "Todos"
        ? rankingRows
        : rankingRows.filter((row) => getTopicName(row) === deepDiveTopicFilter),
    [deepDiveTopicFilter, rankingRows]
  );
  const filteredEvidenceRows = useMemo(
    () =>
      deepDiveTopicFilter === "Todos"
        ? evidenceRows
        : evidenceRows.filter((row) => getTopicName(row) === deepDiveTopicFilter),
    [deepDiveTopicFilter, evidenceRows]
  );

  const evidenceHelixTable = activeHelixRecords.map((record) => ({
    ID: asString(record.incident_id),
    ID__href: asString(record.url),
    "Evidencia Helix": asString(record.summary)
  }));
  const evidenceVocTable = activeVocRecords.map((record) => ({
    ID: asString(record.comment_id),
    Fecha: asString(record.date),
    NPS: record.nps,
    Grupo: asString(record.group),
    Palanca: asString(record.palanca),
    Subpalanca: asString(record.subpalanca),
    Comentario: asString(record.comment)
  }));

  return (
    <section className="surface-card stack-panel linking-workspace">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Incidencias ↔ NPS</p>
          <h2>Lectura causal operativa</h2>
          <p className="secondary-copy">
            {asString(method.summary, "Base cruzada entre incidencias y Voz del Cliente.")}
          </p>
        </div>
      </div>

      <NavigationTabs compact items={navigationItems} onChange={onTabChange} value={tab} />

      {tab === "situation" ? (
        <div className="linking-stack">
          <div className="section-heading">
            <div>
              <h3>{asString(situation.title, "Situación del periodo")}</h3>
              <p className="secondary-copy">{asString(situation.subtitle)}</p>
            </div>
          </div>

          <div className="metric-grid">
            {situationKpis.map((metric, index) => (
              <article className="metric-card" key={`situation-metric-${index}`}>
                <span>{asString(metric.label)}</span>
                <strong>{displayValue(metric.value, asString(metric.label))}</strong>
                {asString(metric.hint) ? <p>{asString(metric.hint)}</p> : null}
              </article>
            ))}
          </div>

          {situationMetadata.length ? (
            <div className="context-pill-row">
              {situationMetadata.map((item, index) => (
                <span className="context-pill" key={`situation-meta-${index}`}>
                  <strong>{asString(item.label)}:</strong> {asString(item.value)}
                </span>
              ))}
            </div>
          ) : null}

          <section className="linking-panel">
            <div className="section-heading">
              <div>
                <h3>{asString(situation.figure_title, "Timeline causal (diario)")}</h3>
              </div>
            </div>
            <PlotFigure
              emptyMessage="No hay suficiente base cruzada para construir el timeline causal."
              figure={asFigure(situation.figure)}
              testId="linking-overview-figure"
            />
            {asString(situation.note) ? <p className="secondary-copy">{asString(situation.note)}</p> : null}
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
              emptyMessage={asString(entitySummary.empty_state, "No hay resumen causal disponible.")}
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
              emptyMessage={asString(entitySummary.empty_state, "No hay detalle causal disponible.")}
              rows={entitySummaryRows}
            />
          </section>
        </div>
      ) : null}

      {tab === "scenarios" ? (
        <div className="linking-stack">
          <div className="section-heading">
            <div>
              <h3>{asString(scenarios.title, "Análisis de escenarios causales")}</h3>
              <p className="secondary-copy">{asString(scenarios.subtitle)}</p>
            </div>
          </div>

          <section className="hero-banner">
            <p className="eyebrow">{asString(banner.kicker, "Narrativa causal")}</p>
            <h3>{asString(banner.title, "Sin escenarios defendibles en esta ventana")}</h3>
            <p className="secondary-copy">{asString(banner.summary)}</p>
            <div className="hero-metrics">
              {bannerMetrics.map((metric, index) => (
                <article className="hero-metric-card" key={`banner-metric-${index}`}>
                  <span>{asString(metric.label)}</span>
                  <strong>{displayValue(metric.value, asString(metric.label))}</strong>
                  {asString(metric.hint) ? <p>{asString(metric.hint)}</p> : null}
                </article>
              ))}
            </div>
          </section>

          {!activeCard ? (
            <p className="empty-state">
              Hay impacto estadístico, pero no se encontraron escenarios defendibles con link explícito entre Helix y VoC para mostrar.
            </p>
          ) : (
            <>
              <div className="section-heading">
                <div>
                  <h3>Escenario activo</h3>
                </div>
              </div>

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
                    <p className="eyebrow">Escenario causal priorizado</p>
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
                    <h4>Ficha priorizada</h4>
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
                      <h4>Incidencias enlazadas</h4>
                      <div className="evidence-pill-row">
                        {activeHelixRecords.length ? (
                          activeHelixRecords.slice(0, 6).map((record, index) => {
                            const incidentId = asString(record.incident_id, `INC-${index + 1}`);
                            const href = asString(record.url);
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
                      <h4>Comentarios enlazados</h4>
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

              {scenarioDetailTab === "matrix" ? (
                <div className="figure-split">
                  <section className="linking-panel">
                    <PlotFigure
                      emptyMessage="No hay suficientes focos para construir la matriz de prioridad."
                      figure={asFigure(activeCard.matrix_figure)}
                    />
                  </section>
                  <section className="linking-panel">
                    <PlotFigure
                      emptyMessage="No hay suficientes señales para comparar riesgo y recuperación."
                      figure={asFigure(activeCard.risk_recovery_figure)}
                    />
                  </section>
                </div>
              ) : null}

              {scenarioDetailTab === "detail" ? (
                <RecordTable
                  emptyMessage="No hay ficha cuantitativa disponible para el escenario activo."
                  rows={detailTable}
                />
              ) : null}

              {scenarioDetailTab === "heat" ? (
                <section className="linking-panel">
                  <PlotFigure
                    emptyMessage="No hay datos suficientes para el heat map del caso activo."
                    figure={asFigure(activeCard.heatmap_figure)}
                  />
                </section>
              ) : null}

              {scenarioDetailTab === "cp" ? (
                <section className="linking-panel">
                  <PlotFigure
                    emptyMessage="No hay datos suficientes para changepoints y lag del caso activo."
                    figure={asFigure(activeCard.changepoints_figure)}
                  />
                </section>
              ) : null}

              {scenarioDetailTab === "lag" ? (
                <section className="linking-panel">
                  <PlotFigure
                    emptyMessage="No hay lag diario disponible para el caso activo."
                    figure={asFigure(activeCard.lag_figure)}
                  />
                </section>
              ) : null}
            </>
          )}
        </div>
      ) : null}

      {tab === "nps-deep-dive" ? (
        <div className="linking-stack">
          <div className="section-heading">
            <div>
              <h3>{asString(deepDive.title, "Análisis de Tópicos de NPS afectados")}</h3>
              <p className="secondary-copy">{asString(deepDive.subtitle)}</p>
            </div>
          </div>

          <div className="metric-grid">
            {asRows(deepDive.kpis).map((metric, index) => (
              <article className="metric-card" key={`deep-dive-metric-${index}`}>
                <span>{asString(metric.label)}</span>
                <strong>{displayValue(metric.value, asString(metric.label))}</strong>
              </article>
            ))}
          </div>

          <div className="inline-actions">
            <label className="inline-field">
              <span>{asString(deepDiveTopicFilterSpec.label, "Tópico")}</span>
              <select
                onChange={(event) => setDeepDiveTopicFilter(event.target.value)}
                value={deepDiveTopicFilter}
              >
                {deepDiveTopicOptions.map((topic) => (
                  <option key={topic} value={topic}>
                    {topic}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <section className="linking-panel">
            <div className="section-heading">
              <div>
                <h3>{asString(asRecord(deepDive.trending).title, "NPS tópicos trending")}</h3>
              </div>
            </div>
            <PlotFigure
              emptyMessage={asString(
                asRecord(deepDive.trending).empty_state,
                "No hay señal suficiente para construir tópicos trending."
              )}
              figure={filteredTrendingFigure ?? baseTrendingFigure}
              testId="linking-topics-trending"
            />
          </section>

          <NavigationTabs compact items={deepDiveTabs} onChange={setDeepDiveTab} value={deepDiveTab} />

          {deepDiveTab === "ranking" ? (
            <section className="linking-panel">
              <div className="section-heading">
                <div>
                  <h3>{asString(asRecord(deepDive.ranking).title, "Ranking de hipótesis")}</h3>
                </div>
              </div>
              <RecordTable
                emptyMessage={asString(
                  asRecord(deepDive.ranking).empty_state,
                  "No hay suficiente señal para rankear el foco seleccionado."
                )}
                rows={filteredRankingRows}
              />
            </section>
          ) : null}

          {deepDiveTab === "evidence" ? (
            <section className="linking-panel">
              <div className="section-heading">
                <div>
                  <h3>{asString(asRecord(deepDive.evidence).title, "Evidence wall")}</h3>
                </div>
              </div>
              <RecordTable
                emptyMessage={asString(
                  asRecord(deepDive.evidence).empty_state,
                  "No hay evidencia validada para el foco seleccionado."
                )}
                rows={filteredEvidenceRows.slice(0, 50)}
              />
            </section>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
