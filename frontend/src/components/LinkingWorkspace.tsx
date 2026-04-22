import { useEffect, useMemo, useState } from "react";

import type { LinkingPayload, PlotlyFigureSpec } from "../api";
import { formatNumber, formatPercent } from "../utils/numberFormat";
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

function asNumber(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed =
    typeof value === "string" ? Number.parseFloat(value.trim().replace(",", ".")) : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function asFigure(value: unknown): PlotlyFigureSpec | null {
  return value && typeof value === "object" && "data" in (value as Record<string, unknown>)
    ? (value as PlotlyFigureSpec)
    : null;
}

function formatMetricValue(value: unknown, digits = 2) {
  const numeric = asNumber(value);
  if (numeric === null) {
    return "—";
  }
  return formatNumber(Number(numeric.toFixed(digits)));
}

function formatPercentValue(value: unknown) {
  return formatPercent(value);
}

function formatSignedMetricValue(value: unknown, digits = 1) {
  const numeric = asNumber(value);
  if (numeric === null) {
    return "—";
  }
  return formatNumber(Number(numeric.toFixed(digits)), { signed: true });
}

function renderHelixCards(records: Array<Record<string, unknown>>) {
  if (!records.length) {
    return <p className="empty-state">Sin evidencia Helix visible.</p>;
  }
  return (
    <div className="evidence-card-grid">
      {records.map((record, index) => (
        <article className="evidence-card" key={`helix-record-${index}`}>
          <span className="evidence-pill">{asString(record.incident_id, "INC")}</span>
          <p>{asString(record.summary)}</p>
        </article>
      ))}
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
            <span className="evidence-pill">Fecha: {asString(record.date, "-")}</span>
            <span className="evidence-pill">NPS: {asString(record.nps, "-")}</span>
            <span className="evidence-pill">Grupo: {asString(record.group, "-")}</span>
          </div>
          <p>{asString(record.comment)}</p>
        </article>
      ))}
    </div>
  );
}

export function LinkingWorkspace({ linking, tab, onTabChange }: LinkingWorkspaceProps) {
  const method = asRecord(linking.touchpoint_mode);
  const situation = asRecord(linking.situation);
  const journeys = asRecord(linking.journeys);
  const scenarios = asRecord(linking.scenarios);
  const scenarioCards = asRows(scenarios.cards);
  const banner = asRecord(scenarios.banner);
  const bannerMetrics = asRows(banner.metrics);
  const contextPills = linking.context_pills || [];
  const [activeChainIndex, setActiveChainIndex] = useState(0);
  const [scenarioDetailTab, setScenarioDetailTab] = useState("helix");
  const [scenarioEvidenceView, setScenarioEvidenceView] = useState<"cards" | "table">("cards");
  const [deepDiveTopicFilter, setDeepDiveTopicFilter] = useState("Todos");
  const [deepDiveSimilarityOrder, setDeepDiveSimilarityOrder] = useState<"desc" | "asc">("desc");

  useEffect(() => {
    setActiveChainIndex(0);
  }, [scenarioCards.length, linking.focus_group, method.value]);

  const activeCard = scenarioCards[activeChainIndex] || null;
  const activeHelixRecords = asRows(activeCard?.incident_records);
  const activeVocRecords = asRows(activeCard?.comment_records);
  const detailTable = asRows(activeCard?.detail_table);
  const evidenceRows = linking.evidence_table || [];

  const deepDiveTopicOptions = useMemo(() => {
    const topics = new Set<string>();
    evidenceRows.forEach((row) => {
      const topic = asString(row.nps_topic);
      if (topic) {
        topics.add(topic);
      }
    });
    return ["Todos", ...Array.from(topics).sort((left, right) => left.localeCompare(right, "es"))];
  }, [evidenceRows]);

  useEffect(() => {
    if (!deepDiveTopicOptions.includes(deepDiveTopicFilter)) {
      setDeepDiveTopicFilter("Todos");
    }
  }, [deepDiveTopicFilter, deepDiveTopicOptions]);

  const deepDiveRows = useMemo(() => {
    const rows = [...evidenceRows];
    const filtered =
      deepDiveTopicFilter === "Todos"
        ? rows
        : rows.filter((row) => asString(row.nps_topic) === deepDiveTopicFilter);
    filtered.sort((left, right) => {
      const leftValue = asNumber(left.similarity);
      const rightValue = asNumber(right.similarity);
      if (leftValue === null && rightValue === null) {
        return 0;
      }
      if (leftValue === null) {
        return 1;
      }
      if (rightValue === null) {
        return -1;
      }
      return deepDiveSimilarityOrder === "asc" ? leftValue - rightValue : rightValue - leftValue;
    });
    return filtered;
  }, [deepDiveSimilarityOrder, deepDiveTopicFilter, evidenceRows]);

  const evidenceHelixTable = activeHelixRecords.map((record) => ({
    ID: asString(record.incident_id),
    "Evidencia Helix": asString(record.summary)
  }));
  const evidenceVocTable = activeVocRecords.map((record) => ({
    ID: asString(record.comment_id),
    Fecha: asString(record.date),
    NPS: asString(record.nps),
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
          <p className="secondary-copy">{asString(method.summary, "Base cruzada entre incidencias y Voz del Cliente.")}</p>
        </div>
      </div>

      {contextPills.length ? (
        <div className="context-pill-row">
          {contextPills.map((pill) => (
            <span className="context-pill" key={pill}>
              {pill}
            </span>
          ))}
        </div>
      ) : null}

      <NavigationTabs
        compact
        items={[
          { id: "situation", label: "Situación del periodo" },
          { id: "journeys", label: "Journeys rotos" },
          { id: "scenarios", label: "Análisis de escenarios causales" },
          { id: "deep-dive", label: "Data deep dive analysis" }
        ]}
        onChange={onTabChange}
        value={tab}
      />

      {tab === "situation" ? (
        <div className="linking-stack">
          <div className="metric-grid metric-grid-3">
            <article className="metric-card">
              <span>Respuestas analizadas</span>
              <strong>{formatNumber(linking.kpis.responses || 0, { fallback: "0" })}</strong>
            </article>
            <article className="metric-card">
              <span>Incidencias del periodo</span>
              <strong>{formatNumber(linking.kpis.incidents || 0, { fallback: "0" })}</strong>
            </article>
            <article className="metric-card">
              <span>{`${linking.focus_label} medio`}</span>
              <strong>{formatPercentValue(situation.average_focus_rate ?? linking.kpis.average_focus_rate)}</strong>
            </article>
          </div>

          <section className="linking-panel">
            <div className="section-heading">
              <div>
                <h3>Timeline causal (diario)</h3>
              </div>
            </div>
            <PlotFigure
              emptyMessage="No hay suficiente base cruzada para construir el timeline causal."
              figure={asFigure(situation.timeline_figure ?? linking.overview_figure)}
              testId="linking-overview-figure"
            />
            {asString(situation.timeline_note) ? (
              <p className="secondary-copy">{asString(situation.timeline_note)}</p>
            ) : null}
          </section>

          <div className="section-heading">
            <div>
              <h3>Mapa causal priorizado</h3>
              <p className="secondary-copy">
                Síntesis del riesgo NPS, los tópicos trending y la evidencia validada del periodo.
              </p>
            </div>
          </div>
          <div className="metric-grid">
            <article className="metric-card">
              <span>NPS en riesgo</span>
              <strong>{`${formatMetricValue(situation.nps_points_at_risk, 2)} pts`}</strong>
            </article>
            <article className="metric-card">
              <span>NPS recuperable</span>
              <strong>{`${formatMetricValue(situation.nps_points_recoverable, 2)} pts`}</strong>
            </article>
            <article className="metric-card">
              <span>Concentración top-3</span>
              <strong>{formatPercentValue(situation.top3_incident_share)}</strong>
            </article>
            <article className="metric-card">
              <span>Tiempo de reacción</span>
              <strong>
                {situation.median_lag_weeks === null || situation.median_lag_weeks === undefined
                  ? "n/d"
                  : `${formatMetricValue(situation.median_lag_weeks, 1)} semanas`}
              </strong>
            </article>
          </div>

          <section className="linking-panel">
            <div className="section-heading">
              <div>
                <h3>Tópicos trending</h3>
              </div>
            </div>
            <PlotFigure
              emptyMessage="No hay señal suficiente para construir tópicos trending."
              figure={asFigure(situation.topics_trending_figure)}
              testId="linking-topics-trending"
            />
          </section>

          <section className="linking-panel">
            <div className="section-heading">
              <div>
                <h3>Ranking de hipótesis</h3>
              </div>
            </div>
            <RecordTable
              emptyMessage="No hay suficiente señal para rankear tópicos en el periodo seleccionado."
              rows={asRows(situation.ranking_table ?? linking.ranking_table)}
            />
          </section>

          <section className="linking-panel">
            <div className="section-heading">
              <div>
                <h3>Evidence wall</h3>
              </div>
            </div>
            <RecordTable
              emptyMessage="No hay links validados para el tópico líder del periodo."
              rows={asRows(situation.evidence_wall)}
            />
          </section>
        </div>
      ) : null}

      {tab === "journeys" ? (
        <div className="linking-stack">
          <div className="section-heading">
            <div>
              <h3>Journeys rotos identificados</h3>
              <p className="secondary-copy">
                Detección automática de touchpoints rotos a partir de embeddings ligeros, keywords y clustering semántico sobre links Helix↔VoC.
              </p>
            </div>
          </div>
          <div className="metric-grid metric-grid-3">
            <article className="metric-card">
              <span>Journeys detectados</span>
              <strong>{formatNumber(journeys.journeys_detected || 0, { fallback: "0" })}</strong>
            </article>
            <article className="metric-card">
              <span>Links validados</span>
              <strong>{formatNumber(journeys.linked_pairs || 0, { fallback: "0" })}</strong>
            </article>
            <article className="metric-card">
              <span>Cohesión media</span>
              <strong>{formatMetricValue(journeys.semantic_cohesion_mean, 2)}</strong>
            </article>
          </div>
          <section className="linking-panel">
            <PlotFigure
              emptyMessage="No he identificado journeys rotos defendibles en esta ventana."
              figure={asFigure(journeys.figure)}
              testId="linking-journeys-figure"
            />
          </section>
          <section className="linking-panel">
            <RecordTable
              emptyMessage="No he identificado journeys rotos defendibles en esta ventana."
              rows={asRows(journeys.table)}
            />
          </section>
        </div>
      ) : null}

      {tab === "scenarios" ? (
        <div className="linking-stack">
          <section className="hero-banner">
            <p className="eyebrow">{asString(banner.kicker, "Narrativa causal")}</p>
            <h3>{asString(banner.title, "Sin cadenas defendibles en esta ventana")}</h3>
            <p className="secondary-copy">{asString(banner.summary)}</p>
            <div className="hero-metrics">
              {bannerMetrics.map((metric, index) => (
                <article className="hero-metric-card" key={`banner-metric-${index}`}>
                  <span>{asString(metric.label)}</span>
                  <strong>{asString(metric.value)}</strong>
                  {asString(metric.hint) ? <p>{asString(metric.hint)}</p> : null}
                </article>
              ))}
            </div>
          </section>

          <div className="context-pill-row">
            {((scenarios.pills as unknown[]) || []).map((pill, index) => (
              <span className="context-pill" key={`scenario-pill-${index}`}>
                {asString(pill)}
              </span>
            ))}
          </div>

          {!activeCard ? (
            <p className="empty-state">
              Hay impacto estadístico, pero no se encontraron cadenas defendibles con link explícito entre Helix y VoC para mostrar en comité.
            </p>
          ) : (
            <>
              <div className="section-heading">
                <div>
                  <h3>Cadena activa</h3>
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
                  <strong>{`Cadena ${activeChainIndex + 1} de ${scenarioCards.length}`}</strong>
                  <span>{asString(activeCard.selection_label, asString(activeCard.nps_topic))}</span>
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

              <section className="spotlight-card">
                <div className="spotlight-head">
                  <div>
                    <p className="eyebrow">Cadena causal priorizada</p>
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
                <div className="spotlight-metrics">
                  <article className="spotlight-metric">
                    <span>Probabilidad foco</span>
                    <strong>{formatPercentValue(activeCard.detractor_probability)}</strong>
                  </article>
                  <article className="spotlight-metric">
                    <span>Delta NPS</span>
                    <strong>{formatSignedMetricValue(activeCard.nps_delta_expected, 1)}</strong>
                  </article>
                  <article className="spotlight-metric">
                    <span>Impacto total</span>
                    <strong>{`${formatMetricValue(activeCard.total_nps_impact, 2)} pts`}</strong>
                  </article>
                  <article className="spotlight-metric">
                    <span>Confianza</span>
                    <strong>{formatMetricValue(activeCard.confidence, 2)}</strong>
                  </article>
                  <article className="spotlight-metric">
                    <span>Links validados</span>
                    <strong>{asString(activeCard.linked_pairs, "0")}</strong>
                  </article>
                  <article className="spotlight-metric">
                    <span>Prioridad</span>
                    <strong>{formatMetricValue(activeCard.priority, 2)}</strong>
                  </article>
                  <article className="spotlight-metric">
                    <span>NPS en riesgo</span>
                    <strong>{`${formatMetricValue(activeCard.nps_points_at_risk, 2)} pts`}</strong>
                  </article>
                  <article className="spotlight-metric">
                    <span>NPS recuperable</span>
                    <strong>{`${formatMetricValue(activeCard.nps_points_recoverable, 2)} pts`}</strong>
                  </article>
                  <article className="spotlight-metric">
                    <span>Owner</span>
                    <strong>{asString(activeCard.owner_role, "n/d")}</strong>
                  </article>
                </div>
              </section>

              <div className="evidence-toolbar">
                <div className="evidence-view-toggle" role="tablist">
                  <button
                    className={scenarioEvidenceView === "cards" ? "is-active" : ""}
                    onClick={() => setScenarioEvidenceView("cards")}
                    type="button"
                  >
                    Cards
                  </button>
                  <button
                    className={scenarioEvidenceView === "table" ? "is-active" : ""}
                    onClick={() => setScenarioEvidenceView("table")}
                    type="button"
                  >
                    Tabla
                  </button>
                </div>
                <div className="evidence-toolbar-note">
                  <strong>{activeHelixRecords.length}</strong> evidencias Helix visibles · <strong>{activeVocRecords.length}</strong> comentarios VoC visibles
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
                scenarioEvidenceView === "cards" ? (
                  renderHelixCards(activeHelixRecords)
                ) : (
                  <RecordTable emptyMessage="Sin evidencia Helix visible." rows={evidenceHelixTable} />
                )
              ) : null}

              {scenarioDetailTab === "voc" ? (
                scenarioEvidenceView === "cards" ? (
                  renderVocCards(activeVocRecords)
                ) : (
                  <RecordTable emptyMessage="Sin evidencia VoC visible." rows={evidenceVocTable} />
                )
              ) : null}

              {scenarioDetailTab === "matrix" ? (
                <div className="figure-split">
                  <section className="linking-panel">
                    <PlotFigure
                      emptyMessage="No hay suficientes tópicos para construir la matriz de prioridad."
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
                  emptyMessage="No hay ficha cuantitativa disponible para la cadena activa."
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

      {tab === "deep-dive" ? (
        <div className="linking-stack">
          <div className="inline-actions">
            <label className="inline-field">
              <span>Tópico</span>
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
            <label className="inline-field">
              <span>Orden similarity</span>
              <select
                onChange={(event) => setDeepDiveSimilarityOrder(event.target.value as "desc" | "asc")}
                value={deepDiveSimilarityOrder}
              >
                <option value="desc">Mayor a menor</option>
                <option value="asc">Menor a mayor</option>
              </select>
            </label>
          </div>
          <div className="table-meta">
            <span>Filas visibles: {formatNumber(deepDiveRows.length, { fallback: "0" })}</span>
          </div>
          <RecordTable
            emptyMessage="No hay evidencia para el filtro de tópico seleccionado."
            rows={deepDiveRows}
          />
        </div>
      ) : null}
    </section>
  );
}
