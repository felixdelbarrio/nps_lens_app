import { startTransition, useEffect, useMemo, useState } from "react";
import useSWR from "swr";

import {
  fetchConfig,
  fetchDashboard,
  fetchDatasetTable,
  fetchLinkingDashboard,
  fetchUploads,
  reprocessSummary,
  uploadHelixFile,
  uploadNpsFile
} from "./api";
import type {
  DashboardPayload,
  DatasetStatus,
  HelixUploadResult,
  LinkingPayload,
  UploadResult
} from "./api";
import { DatasetUploadCard } from "./components/DatasetUploadCard";
import { IssueList } from "./components/IssueList";
import { NavigationTabs } from "./components/NavigationTabs";
import { PlotFigure } from "./components/PlotFigure";
import { UploadsTable } from "./components/UploadsTable";

const MAIN_SECTIONS = [
  { id: "nps", label: "📊 NPS Térmico" },
  { id: "linking", label: "🔗 Incidencias ↔ NPS" },
  { id: "data", label: "🧾 Datos" }
];

const NPS_TABS = [
  { id: "summary", label: "Sumario del periodo" },
  { id: "comparison", label: "Cambios respecto al histórico" },
  { id: "cohorts", label: "Comparativas cruzadas" },
  { id: "gaps", label: "Dónde el NPS se separa" },
  { id: "opportunities", label: "Oportunidades priorizadas" }
];

const OVERVIEW_TABS = [
  { id: "daily", label: "NPS clásico vs detractores" },
  { id: "weekly", label: "Media semanal" },
  { id: "topics", label: "Qué dicen los clientes" },
  { id: "volume", label: "Cuándo lo dicen" },
  { id: "mix", label: "Cómo lo dicen" }
];

const DATA_TABS = [
  { id: "nps", label: "NPS" },
  { id: "helix", label: "Helix" }
];

const LINKING_TABS = [
  { id: "situation", label: "Situación del periodo" },
  { id: "journeys", label: "Journeys de detracción" },
  { id: "scenarios", label: "Escenarios causales" }
];

const SAMPLE_SIZES = [50, 100, 200, 500, 1000];

function formatPercent(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return "—";
  }
  return `${(value * 100).toFixed(1)}%`;
}

function formatNumber(value: number | null | undefined, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return value.toFixed(digits);
}

function ReportModal({
  open,
  report,
  onClose
}: {
  open: boolean;
  report: string;
  onClose: () => void;
}) {
  if (!open) {
    return null;
  }

  return (
    <div className="modal-backdrop" role="presentation">
      <div aria-modal="true" className="modal-card" role="dialog">
        <div className="panel-heading panel-heading-inline">
          <div>
            <p className="eyebrow">Reporte</p>
            <h2>Reporte ejecutivo</h2>
          </div>
          <button className="secondary-button" onClick={onClose} type="button">
            Cerrar
          </button>
        </div>
        <pre className="report-markdown">{report || "No hay reporte disponible para este contexto."}</pre>
      </div>
    </div>
  );
}

export function App() {
  const [serviceOrigin, setServiceOrigin] = useState("");
  const [serviceOriginN1, setServiceOriginN1] = useState("");
  const [serviceOriginN2, setServiceOriginN2] = useState("");
  const [popYear, setPopYear] = useState("Todos");
  const [popMonth, setPopMonth] = useState("Todos");
  const [npsGroup, setNpsGroup] = useState("Todos");
  const [comparisonDimension, setComparisonDimension] = useState("Palanca");
  const [gapDimension, setGapDimension] = useState("Palanca");
  const [opportunityDimension, setOpportunityDimension] = useState("Palanca");
  const [cohortRow, setCohortRow] = useState("Palanca");
  const [cohortCol, setCohortCol] = useState("Canal");
  const [minN, setMinN] = useState(200);
  const [minNCross, setMinNCross] = useState(30);
  const [mainSection, setMainSection] = useState("nps");
  const [npsTab, setNpsTab] = useState("summary");
  const [overviewTab, setOverviewTab] = useState("daily");
  const [linkingTab, setLinkingTab] = useState("situation");
  const [dataTab, setDataTab] = useState<"nps" | "helix">("nps");
  const [historyFilter, setHistoryFilter] = useState("");
  const [activeUploadId, setActiveUploadId] = useState<string | null>(null);
  const [reportOpen, setReportOpen] = useState(false);
  const [tableLimit, setTableLimit] = useState(200);
  const [tableOffset, setTableOffset] = useState(0);
  const [statusCopy, setStatusCopy] = useState("Cargando contexto del producto...");
  const [error, setError] = useState<string | null>(null);
  const [isMutating, setIsMutating] = useState(false);
  const [latestNpsUpload, setLatestNpsUpload] = useState<UploadResult | null>(null);
  const [latestHelixUpload, setLatestHelixUpload] = useState<HelixUploadResult | null>(null);

  const configKey = serviceOrigin || serviceOriginN1 || serviceOriginN2
    ? ["dashboard-context", serviceOrigin, serviceOriginN1, serviceOriginN2]
    : ["dashboard-context-initial"];

  const { data: config, error: configError, isLoading: configLoading, mutate: mutateConfig } =
    useSWR(configKey, () =>
      fetchConfig({
        service_origin: serviceOrigin || undefined,
        service_origin_n1: serviceOriginN1 || undefined,
        service_origin_n2: serviceOriginN2 || undefined
      })
    );

  useEffect(() => {
    if (!config) {
      return;
    }
    setServiceOrigin((current) => current || config.default_service_origin);
    setServiceOriginN1((current) => current || config.default_service_origin_n1);
    if (!config.available_years.includes(popYear)) {
      setPopYear(config.available_years[0] || "Todos");
    }
  }, [config, popYear]);

  const monthOptions = useMemo(() => {
    if (!config) {
      return ["Todos"];
    }
    return config.available_months_by_year[popYear] || config.available_months_by_year.Todos || ["Todos"];
  }, [config, popYear]);

  useEffect(() => {
    if (!monthOptions.includes(popMonth)) {
      setPopMonth(monthOptions[0] || "Todos");
    }
  }, [monthOptions, popMonth]);

  const dashboardQuery = useMemo(
    () => ({
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2,
      pop_year: popYear,
      pop_month: popMonth,
      nps_group: npsGroup,
      comparison_dimension: comparisonDimension,
      gap_dimension: gapDimension,
      opportunity_dimension: opportunityDimension,
      cohort_row: cohortRow,
      cohort_col: cohortCol,
      min_n: minN,
      min_n_cross: minNCross
    }),
    [
      cohortCol,
      cohortRow,
      comparisonDimension,
      gapDimension,
      minN,
      minNCross,
      npsGroup,
      opportunityDimension,
      popMonth,
      popYear,
      serviceOrigin,
      serviceOriginN1,
      serviceOriginN2
    ]
  );

  const dashboardKey =
    serviceOrigin && serviceOriginN1 ? ["dashboard", ...Object.values(dashboardQuery)] : null;

  const {
    data: dashboard,
    error: dashboardError,
    isLoading: dashboardLoading,
    mutate: mutateDashboard
  } = useSWR<DashboardPayload>(dashboardKey, () => fetchDashboard(dashboardQuery));

  const linkingKey =
    mainSection === "linking" && serviceOrigin && serviceOriginN1
      ? ["linking", serviceOrigin, serviceOriginN1, serviceOriginN2, popYear, popMonth, npsGroup]
      : null;
  const {
    data: linking,
    error: linkingError,
    isLoading: linkingLoading,
    mutate: mutateLinking
  } = useSWR<LinkingPayload>(linkingKey, () =>
    fetchLinkingDashboard({
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2,
      pop_year: popYear,
      pop_month: popMonth,
      nps_group: npsGroup
    })
  );

  const uploadsKey =
    serviceOrigin && serviceOriginN1 ? ["uploads", serviceOrigin, serviceOriginN1, serviceOriginN2] : null;
  const {
    data: uploads = [],
    error: uploadsError,
    isLoading: uploadsLoading,
    mutate: mutateUploads
  } = useSWR<UploadResult[]>(uploadsKey, () =>
    fetchUploads({
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2
    })
  );

  const activeDatasetKind = mainSection === "linking" ? "helix" : dataTab;
  const datasetKey =
    serviceOrigin && serviceOriginN1
      ? [
          "dataset",
          activeDatasetKind,
          serviceOrigin,
          serviceOriginN1,
          serviceOriginN2,
          popYear,
          popMonth,
          npsGroup,
          tableOffset,
          tableLimit
        ]
      : null;
  const {
    data: datasetTable,
    error: datasetError,
    isLoading: datasetLoading,
    mutate: mutateDataset
  } = useSWR(datasetKey, () =>
    fetchDatasetTable(activeDatasetKind, {
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2,
      pop_year: popYear,
      pop_month: popMonth,
      nps_group: npsGroup,
      offset: tableOffset,
      limit: tableLimit
    })
  );

  useEffect(() => {
    if (!uploads.length) {
      setActiveUploadId(null);
      return;
    }
    setActiveUploadId((current) => current || uploads[0]?.upload_id || null);
  }, [uploads]);

  useEffect(() => {
    const currentError = configError || dashboardError || uploadsError || datasetError || linkingError;
    if (currentError) {
      setError(currentError.message);
      setStatusCopy("La interfaz no pudo sincronizar el contexto operativo.");
      return;
    }
    setError(null);
    if (isMutating) {
      setStatusCopy("Importando y rehidratando el histórico persistente...");
      return;
    }
    if (configLoading || dashboardLoading || uploadsLoading || linkingLoading) {
      setStatusCopy("Cargando contexto, histórico y vistas analíticas...");
      return;
    }
    setStatusCopy("Contexto, histórico y analítica alineados con el dataset persistido.");
  }, [
    configError,
    configLoading,
    dashboardError,
    dashboardLoading,
    datasetError,
    isMutating,
    linkingError,
    linkingLoading,
    uploadsError,
    uploadsLoading
  ]);

  const selectedUpload = uploads.find((upload) => upload.upload_id === activeUploadId) || latestNpsUpload;
  const n1Options = config?.service_origin_n1_map[serviceOrigin] || [];

  useEffect(() => {
    if (!n1Options.length) {
      return;
    }
    if (!n1Options.includes(serviceOriginN1)) {
      setServiceOriginN1(n1Options[0]);
    }
  }, [n1Options, serviceOriginN1]);

  async function handleNpsUpload(payload: { file: File; sheetName: string }) {
    setIsMutating(true);
    setError(null);
    try {
      const result = await uploadNpsFile({
        ...payload,
        serviceOrigin,
        serviceOriginN1,
        serviceOriginN2
      });
      setLatestNpsUpload(result);
      await Promise.all([
        mutateConfig(),
        mutateUploads(),
        mutateDashboard(),
        mutateDataset(),
        mutateLinking()
      ]);
      startTransition(() => {
        setActiveUploadId(result.upload_id);
        setMainSection("nps");
      });
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsMutating(false);
    }
  }

  async function handleHelixUpload(payload: { file: File; sheetName: string }) {
    setIsMutating(true);
    setError(null);
    try {
      const result = await uploadHelixFile({
        ...payload,
        serviceOrigin,
        serviceOriginN1,
        serviceOriginN2
      });
      setLatestHelixUpload(result);
      await Promise.all([mutateConfig(), mutateDataset(), mutateLinking()]);
      startTransition(() => {
        setMainSection("linking");
        setDataTab("helix");
        setTableOffset(0);
      });
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsMutating(false);
    }
  }

  async function handleReprocess() {
    setIsMutating(true);
    setError(null);
    try {
      await reprocessSummary({
        service_origin: serviceOrigin,
        service_origin_n1: serviceOriginN1,
        service_origin_n2: serviceOriginN2
      });
      await Promise.all([
        mutateConfig(),
        mutateUploads(),
        mutateDashboard(),
        mutateDataset(),
        mutateLinking()
      ]);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsMutating(false);
    }
  }

  const contextPills = dashboard?.context_pills || [];
  const npsDatasetStatus: DatasetStatus =
    config?.nps_dataset || {
      available: false,
      rows: 0,
      columns: 0,
      updated_at: null,
      status: "missing"
    };
  const helixDatasetStatus: DatasetStatus =
    config?.helix_dataset || {
      available: false,
      rows: 0,
      columns: 0,
      updated_at: null,
      status: "missing"
    };

  function renderOverviewTab() {
    if (dashboard?.empty_state) {
      return <p className="empty-state">{dashboard.empty_state}</p>;
    }

    if (overviewTab === "daily") {
      return (
        <section className="panel">
          <p className="panel-copy">Lectura diaria: NPS clásico y porcentaje de detractores dentro del periodo activo.</p>
          <PlotFigure
            emptyMessage="No hay suficientes datos para construir la vista diaria."
            figure={dashboard?.overview.daily_kpis_figure}
            testId="daily-kpis-figure"
          />
        </section>
      );
    }

    if (overviewTab === "weekly") {
      return (
        <section className="panel stack-panel">
          <PlotFigure
            emptyMessage="No hay suficientes datos para construir una tendencia."
            figure={dashboard?.overview.weekly_trend_figure}
            testId="weekly-trend-figure"
          />
          <article className="note-card">
            <p className="eyebrow">Informe de negocio</p>
            <pre className="report-markdown inline-report">{dashboard?.report_markdown || "Sin informe disponible."}</pre>
          </article>
        </section>
      );
    }

    if (overviewTab === "topics") {
      return (
        <section className="panel stack-panel">
          <PlotFigure
            emptyMessage="No hay texto suficiente para extraer temas."
            figure={dashboard?.overview.topics_figure}
            testId="topics-figure"
          />
          <div className="table-shell">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Cluster</th>
                  <th>n</th>
                  <th>Términos</th>
                  <th>Ejemplos</th>
                </tr>
              </thead>
              <tbody>
                {(dashboard?.overview.topics_table || []).map((row, index) => (
                  <tr key={`topic-${index}`}>
                    <td>{String(row.cluster_id ?? "")}</td>
                    <td>{String(row.n ?? "")}</td>
                    <td>{Array.isArray(row.top_terms) ? row.top_terms.join(", ") : String(row.top_terms ?? "")}</td>
                    <td>{Array.isArray(row.examples) ? row.examples.join(" · ") : String(row.examples ?? "")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      );
    }

    if (overviewTab === "volume") {
      return (
        <section className="panel">
          <PlotFigure
            emptyMessage="No hay suficientes datos para construir la vista de volumen diario."
            figure={dashboard?.overview.daily_volume_figure}
            testId="daily-volume-figure"
          />
        </section>
      );
    }

    return (
      <section className="panel stack-panel">
        <article className="note-card">
          <p className="panel-copy">
            Cómo leerlo: más rojo empeora NPS, más verde lo mejora. Usa el volumen para no sobre-interpretar días con pocas respuestas.
          </p>
        </article>
        <PlotFigure
          emptyMessage="No hay suficientes datos para construir la mezcla diaria."
          figure={dashboard?.overview.daily_mix_figure}
          testId="daily-mix-figure"
        />
      </section>
    );
  }

  function renderNpsSection() {
    return (
      <>
        <section className="panel">
          <div className="panel-heading panel-heading-inline">
            <div>
              <p className="eyebrow">Resumen ejecutivo</p>
              <h2>{dashboard?.context_label || "Periodo seleccionado"}</h2>
            </div>
            <button
              className="secondary-button"
              data-testid="reprocess-button"
              onClick={() => {
                void handleReprocess();
              }}
              type="button"
            >
              {isMutating ? "Reprocesando..." : "Reprocesar agregados"}
            </button>
          </div>
          <div className="kpi-grid">
            <article className="kpi-card">
              <span>Muestras</span>
              <strong>{dashboard?.kpis.samples?.toLocaleString("es-ES") || "0"}</strong>
            </article>
            <article className="kpi-card">
              <span>NPS medio (0-10)</span>
              <strong>{formatNumber(dashboard?.kpis.nps_average, 2)}</strong>
            </article>
            <article className="kpi-card">
              <span>Detractores (≤6)</span>
              <strong>{formatPercent(dashboard?.kpis.detractor_rate)}</strong>
            </article>
            <article className="kpi-card">
              <span>Promotores (≥9)</span>
              <strong>{formatPercent(dashboard?.kpis.promoter_rate)}</strong>
            </article>
          </div>
        </section>

        <NavigationTabs items={NPS_TABS} onChange={setNpsTab} value={npsTab} />

        {npsTab === "summary" ? (
          <>
            <NavigationTabs compact items={OVERVIEW_TABS} onChange={setOverviewTab} value={overviewTab} />
            {renderOverviewTab()}
          </>
        ) : null}

        {npsTab === "comparison" ? (
          <section className="panel stack-panel">
            <div className="split-head">
              <div>
                <p className="eyebrow">Comparativa</p>
                <h2>{dashboard?.comparison.summary?.label_current || "Sin base comparativa"}</h2>
              </div>
              <label className="inline-field">
                <span>Dimensión</span>
                <select onChange={(event) => setComparisonDimension(event.target.value)} value={comparisonDimension}>
                  {(dashboard?.controls.dimensions || []).map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <div className="delta-strip">
              <span>Δ NPS: {formatNumber(dashboard?.comparison.summary?.delta_nps, 2)}</span>
              <span>Δ detractores: {formatNumber(dashboard?.comparison.summary?.delta_detr_pp, 1)} pp</span>
            </div>
            <PlotFigure
              emptyMessage="No hay suficiente histórico para comparar el periodo actual con la base."
              figure={dashboard?.comparison.figure}
              testId="comparison-figure"
            />
            <div className="table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Valor</th>
                    <th>Δ NPS</th>
                    <th>NPS actual</th>
                    <th>NPS base</th>
                    <th>n actual</th>
                    <th>n base</th>
                  </tr>
                </thead>
                <tbody>
                  {(dashboard?.comparison.table || []).map((row, index) => (
                    <tr key={`cmp-${index}`}>
                      <td>{String(row.value ?? "")}</td>
                      <td>{String(row.delta_nps ?? "")}</td>
                      <td>{String(row.nps_current ?? "")}</td>
                      <td>{String(row.nps_baseline ?? "")}</td>
                      <td>{String(row.n_current ?? "")}</td>
                      <td>{String(row.n_baseline ?? "")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ) : null}

        {npsTab === "cohorts" ? (
          <section className="panel stack-panel">
            <div className="split-head">
              <div>
                <p className="eyebrow">Cohortes</p>
                <h2>Bolsas de fricción</h2>
              </div>
              <div className="inline-actions">
                <label className="inline-field">
                  <span>Filas</span>
                  <select onChange={(event) => setCohortRow(event.target.value)} value={cohortRow}>
                    {(dashboard?.controls.cohort_rows || []).map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="inline-field">
                  <span>Columnas</span>
                  <select onChange={(event) => setCohortCol(event.target.value)} value={cohortCol}>
                    {(dashboard?.controls.cohort_columns || []).map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
            </div>
            <PlotFigure
              emptyMessage="No hay suficiente información para construir la matriz de cohortes."
              figure={dashboard?.cohorts.figure}
              testId="cohort-figure"
            />
          </section>
        ) : null}

        {npsTab === "gaps" ? (
          <section className="panel stack-panel">
            <div className="split-head">
              <div>
                <p className="eyebrow">Brechas</p>
                <h2>Dónde el NPS se separa del global</h2>
              </div>
              <label className="inline-field">
                <span>Dimensión</span>
                <select onChange={(event) => setGapDimension(event.target.value)} value={gapDimension}>
                  {(dashboard?.controls.dimensions || []).map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <PlotFigure
              emptyMessage="No hay datos suficientes para calcular gaps."
              figure={dashboard?.gaps.figure}
              testId="gaps-figure"
            />
            <div className="table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Valor</th>
                    <th>n</th>
                    <th>NPS</th>
                    <th>Gap</th>
                  </tr>
                </thead>
                <tbody>
                  {(dashboard?.gaps.table || []).map((row, index) => (
                    <tr key={`gap-${index}`}>
                      <td>{String(row.value ?? "")}</td>
                      <td>{String(row.n ?? "")}</td>
                      <td>{String(row.nps ?? "")}</td>
                      <td>{String(row.gap_vs_overall ?? "")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ) : null}

        {npsTab === "opportunities" ? (
          <section className="panel stack-panel">
            <div className="split-head">
              <div>
                <p className="eyebrow">Priorización</p>
                <h2>Oportunidades priorizadas</h2>
              </div>
              <label className="inline-field">
                <span>Dimensión</span>
                <select
                  onChange={(event) => setOpportunityDimension(event.target.value)}
                  value={opportunityDimension}
                >
                  {(dashboard?.controls.dimensions || []).map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <PlotFigure
              emptyMessage="No se detectaron oportunidades con el umbral actual."
              figure={dashboard?.opportunities.figure}
              testId="opportunities-figure"
            />
            <article className="note-card">
              <ul className="plain-list">
                {(dashboard?.opportunities.bullets || []).map((bullet) => (
                  <li key={bullet}>{bullet}</li>
                ))}
              </ul>
            </article>
            <div className="table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Etiqueta</th>
                    <th>n</th>
                    <th>NPS actual</th>
                    <th>Uplift</th>
                    <th>Confianza</th>
                  </tr>
                </thead>
                <tbody>
                  {(dashboard?.opportunities.table || []).map((row, index) => (
                    <tr key={`opp-${index}`}>
                      <td>{String(row.label ?? `${row.dimension}=${row.value}`)}</td>
                      <td>{String(row.n ?? "")}</td>
                      <td>{String(row.current_nps ?? "")}</td>
                      <td>{String(row.potential_uplift ?? "")}</td>
                      <td>{String(row.confidence ?? "")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ) : null}
      </>
    );
  }

  function renderLinkingSection() {
    if (!linking?.available) {
      return (
        <section className="panel stack-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Incidencias ↔ NPS</p>
              <h2>Base cruzada y readiness operativo</h2>
            </div>
          </div>

          <div className="kpi-grid">
            <article className="kpi-card">
              <span>Dataset NPS</span>
              <strong>{npsDatasetStatus.available ? npsDatasetStatus.rows.toLocaleString("es-ES") : "—"}</strong>
            </article>
            <article className="kpi-card">
              <span>Dataset Helix</span>
              <strong>{helixDatasetStatus.available ? helixDatasetStatus.rows.toLocaleString("es-ES") : "—"}</strong>
            </article>
            <article className="kpi-card">
              <span>Última actualización NPS</span>
              <strong>{npsDatasetStatus.updated_at ? new Date(npsDatasetStatus.updated_at).toLocaleDateString("es-ES") : "—"}</strong>
            </article>
            <article className="kpi-card">
              <span>Última actualización Helix</span>
              <strong>{helixDatasetStatus.updated_at ? new Date(helixDatasetStatus.updated_at).toLocaleDateString("es-ES") : "—"}</strong>
            </article>
          </div>

          <article className="note-card">
            <p className="panel-copy">
              {linking?.empty_state ||
                "El dataset Helix aún no está cargado para este contexto. La vista causal se activará cuando exista base cruzada suficiente."}
            </p>
          </article>
        </section>
      );
    }

    return (
      <section className="panel stack-panel">
        <div className="panel-heading">
          <div>
            <p className="eyebrow">Incidencias ↔ NPS</p>
            <h2>Lectura causal operativa</h2>
          </div>
        </div>

        <div className="kpi-grid">
          <article className="kpi-card">
            <span>Respuestas analizadas</span>
            <strong>{Number(linking.kpis.responses || 0).toLocaleString("es-ES")}</strong>
          </article>
          <article className="kpi-card">
            <span>Incidencias del periodo</span>
            <strong>{Number(linking.kpis.incidents || 0).toLocaleString("es-ES")}</strong>
          </article>
          <article className="kpi-card">
            <span>NPS en riesgo</span>
            <strong>{formatNumber(linking.kpis.nps_points_at_risk, 2)}</strong>
          </article>
          <article className="kpi-card">
            <span>NPS recuperable</span>
            <strong>{formatNumber(linking.kpis.nps_points_recoverable, 2)}</strong>
          </article>
        </div>

        <NavigationTabs compact items={LINKING_TABS} onChange={setLinkingTab} value={linkingTab} />

        {linkingTab === "situation" ? (
          <>
            <PlotFigure
              emptyMessage="No hay suficiente base cruzada para construir el timeline causal."
              figure={linking.overview_figure}
              testId="linking-overview-figure"
            />
            <div className="table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Tópico</th>
                    <th>Similarity</th>
                    <th>Incidencia</th>
                    <th>Evidencia Helix</th>
                    <th>Comentario detractor</th>
                  </tr>
                </thead>
                <tbody>
                  {linking.evidence_table.map((row, index) => (
                    <tr key={`evidence-${index}`}>
                      <td>{String(row.nps_topic ?? "")}</td>
                      <td>{String(row.similarity ?? "")}</td>
                      <td>{String(row.incident_id ?? "")}</td>
                      <td>{String(row.incident_summary ?? "")}</td>
                      <td>{String(row.detractor_comment ?? "")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : null}

        {linkingTab === "journeys" ? (
          <div className="table-shell">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Route signature</th>
                  <th>n</th>
                  <th>% detractor</th>
                  <th>Score</th>
                  <th>Touchpoint</th>
                  <th>Subtouchpoint</th>
                  <th>Topic</th>
                </tr>
              </thead>
              <tbody>
                {linking.journey_routes_table.map((row, index) => (
                  <tr key={`route-${index}`}>
                    <td>{String(row.route_signature ?? "")}</td>
                    <td>{String(row.n ?? "")}</td>
                    <td>{String(row.detractor_rate ?? "")}</td>
                    <td>{String(row.score ?? "")}</td>
                    <td>{String(row.touchpoint ?? "")}</td>
                    <td>{String(row.subtouchpoint ?? "")}</td>
                    <td>{String(row.topic ?? "")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}

        {linkingTab === "scenarios" ? (
          <>
            <PlotFigure
              emptyMessage="No hay suficientes tópicos para construir la matriz de prioridad."
              figure={linking.priority_figure}
              testId="linking-priority-figure"
            />
            <PlotFigure
              emptyMessage="No hay suficientes señales para comparar riesgo y recuperación."
              figure={linking.risk_recovery_figure}
              testId="linking-risk-recovery-figure"
            />
            <PlotFigure
              emptyMessage="No hay heatmap diario para el tópico líder."
              figure={linking.heatmap_figure}
              testId="linking-heatmap-figure"
            />
            <PlotFigure
              emptyMessage="No hay lag diario defendible para el tópico líder."
              figure={linking.lag_figure}
              testId="linking-lag-figure"
            />
            <div className="table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Tópico</th>
                    <th>Incidencias</th>
                    <th>Respuestas</th>
                    <th>Prioridad</th>
                    <th>Confianza</th>
                    <th>Impacto total NPS</th>
                  </tr>
                </thead>
                <tbody>
                  {linking.ranking_table.map((row, index) => (
                    <tr key={`rank-${index}`}>
                      <td>{String(row.nps_topic ?? "")}</td>
                      <td>{String(row.incidents ?? "")}</td>
                      <td>{String(row.responses ?? "")}</td>
                      <td>{String(row.priority ?? "")}</td>
                      <td>{String(row.confidence ?? "")}</td>
                      <td>{String(row.total_nps_impact ?? "")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : null}
      </section>
    );
  }

  function renderDataSection() {
    return (
      <section className="panel stack-panel">
        <div className="split-head">
          <div>
            <p className="eyebrow">Datos</p>
            <h2>Exploración tabular</h2>
          </div>
          <div className="inline-actions">
            <label className="inline-field">
              <span>Muestra</span>
              <select
                onChange={(event) => {
                  setTableLimit(Number(event.target.value));
                  setTableOffset(0);
                }}
                value={tableLimit}
              >
                {SAMPLE_SIZES.map((size) => (
                  <option key={size} value={size}>
                    {size}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>

        <NavigationTabs compact items={DATA_TABS} onChange={(value) => {
          setDataTab(value as "nps" | "helix");
          setTableOffset(0);
        }} value={dataTab} />

        <div className="table-meta">
          <span>Filas: {datasetTable?.total_rows?.toLocaleString("es-ES") || "0"}</span>
          <span>Columnas: {datasetTable?.columns.length || 0}</span>
        </div>

        <div className="table-shell">
          <table className="data-table" data-testid="data-table">
            <thead>
              <tr>
                {(datasetTable?.columns || []).map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(datasetTable?.rows || []).map((row, index) => (
                <tr key={`row-${index}`}>
                  {(datasetTable?.columns || []).map((column) => (
                    <td key={`${index}-${column}`}>{String(row[column] ?? "")}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="pager">
          <button
            className="secondary-button"
            disabled={tableOffset === 0}
            onClick={() => setTableOffset((current) => Math.max(0, current - tableLimit))}
            type="button"
          >
            Anterior
          </button>
          <span>
            {datasetTable?.offset || 0}-{(datasetTable?.offset || 0) + (datasetTable?.rows.length || 0)} / {datasetTable?.total_rows || 0}
          </span>
          <button
            className="secondary-button"
            disabled={!datasetTable?.has_more}
            onClick={() => setTableOffset((current) => current + tableLimit)}
            type="button"
          >
            Siguiente
          </button>
        </div>
      </section>
    );
  }

  return (
    <>
      <main className="app-shell">
        <aside className="sidebar">
          <section className="panel sidebar-panel">
            <div className="panel-heading">
              <div>
                <p className="eyebrow">Contexto</p>
                <h2>Filtros globales</h2>
              </div>
            </div>
            <div className="field-grid">
              <label>
                <span>Service origin</span>
                <select onChange={(event) => setServiceOrigin(event.target.value)} value={serviceOrigin}>
                  {(config?.service_origins || []).map((origin) => (
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
              <label>
                <span>Service origin N2</span>
                <input
                  onChange={(event) => setServiceOriginN2(event.target.value)}
                  placeholder="Opcional"
                  value={serviceOriginN2}
                />
              </label>
              <label>
                <span>Año</span>
                <select onChange={(event) => setPopYear(event.target.value)} value={popYear}>
                  {(config?.available_years || ["Todos"]).map((year) => (
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
              <label>
                <span>Grupo</span>
                <select onChange={(event) => setNpsGroup(event.target.value)} value={npsGroup}>
                  {(config?.nps_groups || ["Todos"]).map((group) => (
                    <option key={group} value={group}>
                      {group}
                    </option>
                  ))}
                </select>
              </label>
            </div>
          </section>

          <DatasetUploadCard
            ctaLabel="Importar / actualizar NPS"
            datasetStatus={npsDatasetStatus}
            description="Importa el Excel NPS térmico dentro del contexto seleccionado. La carga es acumulativa, tolera drift de esquema y protege el histórico persistente."
            eyebrow="Carga NPS"
            feedback={latestNpsUpload}
            onSubmit={handleNpsUpload}
            testId="upload-input"
            title="Dataset NPS"
            uploading={isMutating}
          />

          <DatasetUploadCard
            ctaLabel="Importar / actualizar Helix"
            datasetStatus={helixDatasetStatus}
            description="Importa el extracto Helix sin acoplar parsing ni validaciones al frontend. El dataset queda persistido por contexto para explotación posterior."
            eyebrow="Carga Helix"
            feedback={latestHelixUpload}
            onSubmit={handleHelixUpload}
            testId="helix-upload-input"
            title="Dataset Helix"
            uploading={isMutating}
          />

          <section className="panel sidebar-panel">
            <div className="panel-heading">
              <div>
                <p className="eyebrow">Parámetros</p>
                <h2>Umbrales analíticos</h2>
              </div>
            </div>
            <div className="field-grid">
              <label>
                <span>Min N oportunidades</span>
                <input
                  min={10}
                  onChange={(event) => setMinN(Number(event.target.value))}
                  type="number"
                  value={minN}
                />
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
        </aside>

        <section className="workspace">
          <header className="hero panel">
            <div className="hero-copy">
              <p className="eyebrow">NPS Lens</p>
              <h1>Analisis del NPS Térmico y causalidad con incidencias de clientes.</h1>
              <p className="hero-subtitle">
                Migración desacoplada a React preservando contexto operativo, densidad informativa y reglas de negocio del flujo original en Streamlit.
              </p>
            </div>
            <div className="hero-side">
              <span className="status-pill">{isMutating ? "SINCRONIZANDO" : "OPERATIVO"}</span>
              <p data-testid="status-copy">{statusCopy}</p>
              <div className="pill-row">
                {contextPills.map((pill) => (
                  <span className="pill" key={pill}>
                    {pill}
                  </span>
                ))}
              </div>
              <button className="secondary-button" onClick={() => setReportOpen(true)} type="button">
                Reporte
              </button>
            </div>
          </header>

          {error ? (
            <section className="error-banner" data-testid="error-banner">
              <strong>Fallo operativo</strong>
              <p>{error}</p>
            </section>
          ) : null}

          <NavigationTabs items={MAIN_SECTIONS} onChange={(value) => startTransition(() => setMainSection(value))} value={mainSection} />

          {mainSection === "nps" ? renderNpsSection() : null}
          {mainSection === "linking" ? renderLinkingSection() : null}
          {mainSection === "data" ? renderDataSection() : null}

          <section className="traceability-grid">
            <UploadsTable
              activeUploadId={activeUploadId}
              filter={historyFilter}
              onFilterChange={setHistoryFilter}
              onSelectUpload={setActiveUploadId}
              uploads={uploads}
            />

            <aside className="panel">
              <div className="panel-heading">
                <div>
                  <p className="eyebrow">Validación</p>
                  <h2>Issues de ingesta</h2>
                </div>
              </div>
              {!selectedUpload ? (
                <p className="empty-state">
                  Selecciona una carga para inspeccionar warnings, errores y schema drift.
                </p>
              ) : (
                <>
                  <div className="inline-feedback-header">
                    <strong data-testid="selected-upload-name">{selectedUpload.filename}</strong>
                    <span>{selectedUpload.status}</span>
                  </div>
                  <IssueList
                    emptyMessage="La carga no generó avisos ni errores."
                    issues={selectedUpload.issues}
                    testId="selected-issues-list"
                  />
                </>
              )}
            </aside>
          </section>

          {(dashboardLoading || datasetLoading) && !dashboard ? (
            <section className="panel">
              <p className="empty-state">Preparando la vista operativa...</p>
            </section>
          ) : null}
        </section>
      </main>

      <ReportModal onClose={() => setReportOpen(false)} open={reportOpen} report={dashboard?.report_markdown || ""} />
    </>
  );
}

export default App;
