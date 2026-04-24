import { startTransition, useEffect, useMemo, useRef, useState, type ChangeEvent } from "react";
import useSWR from "swr";

import {
  downloadExecutiveReport,
  fetchConfig,
  fetchDashboard,
  fetchDatasetTable,
  fetchLinkingDashboard,
  fetchUploads,
  persistPreferences,
  reprocessSummary,
  updateServiceOrigins,
  uploadHelixFile,
  uploadNpsFile
} from "./api";
import type {
  DashboardPayload,
  DatasetStatus,
  HelixUploadResult,
  LinkingPayload,
  PlotlyFigureSpec,
  PreferencesPayload,
  ServiceOriginHierarchyPayload,
  UploadSelectionPayload,
  UploadResult
} from "./api";
import { DatasetUploadCard } from "./components/DatasetUploadCard";
import { IssueList } from "./components/IssueList";
import { LinkingWorkspace } from "./components/LinkingWorkspace";
import { NavigationTabs } from "./components/NavigationTabs";
import { PlotFigure } from "./components/PlotFigure";
import { PrimaryNav } from "./components/PrimaryNav";
import { RecordTable } from "./components/RecordTable";
import { SettingsSheet } from "./components/SettingsSheet";
import { UploadsTable } from "./components/UploadsTable";
import { Icon } from "./components/Icon";
import {
  applyDocumentTheme,
  normalizeThemeMode,
  persistThemeMode,
  readStoredThemeMode,
  type ThemeMode
} from "./theme";
import { formatNumber, formatPercent } from "./utils/numberFormat";

const MAIN_AREAS = [
  {
    id: "insights",
    label: "Insights",
    description: "Seguimiento analítico y causal",
    icon: "home" as const
  },
  {
    id: "ingest",
    label: "Ingesta",
    description: "Nuevas cargas e histórico",
    icon: "upload" as const
  },
  {
    id: "data",
    label: "Datos",
    description: "Exploración tabular",
    icon: "database" as const
  }
];

const INSIGHT_TABS = [
  { id: "nps", label: "NPS térmico" },
  { id: "linking", label: "Incidencias ↔ NPS" }
];

const INGEST_TABS = [
  { id: "new", label: "Nueva carga" },
  { id: "history", label: "Histórico" },
  { id: "traceability", label: "Detalle de ejecución" }
];

const NPS_TABS = [
  { id: "summary", label: "Sumario del periodo" },
  { id: "comparison", label: "Cambios respecto al histórico" },
  { id: "cohorts", label: "Comparativas cruzadas" },
  { id: "gaps", label: "Dónde el NPS se separa" },
  { id: "opportunities", label: "Oportunidades priorizadas" }
];

const OVERVIEW_TABS = [
  { id: "promoters-vs-detractors", label: "Evolución promotores vs detractores" },
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

const SAMPLE_SIZES = [50, 100, 200, 500, 1000];
const MONTH_LABELS_ES: Record<string, string> = {
  "01": "Enero",
  "02": "Febrero",
  "03": "Marzo",
  "04": "Abril",
  "05": "Mayo",
  "06": "Junio",
  "07": "Julio",
  "08": "Agosto",
  "09": "Septiembre",
  "10": "Octubre",
  "11": "Noviembre",
  "12": "Diciembre"
};

function formatDateLabel(value: string | null | undefined, locale = "es-ES") {
  if (!value) {
    return "—";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "—";
  }
  return parsed.toLocaleDateString(locale);
}

function parseServiceOriginN2(value: string) {
  return Array.from(new Set(value.split(",").map((token) => token.trim()).filter(Boolean)));
}

function serializeServiceOriginN2(values: string[]) {
  return parseServiceOriginN2(values.join(", ")).join(", ");
}

function getLatestAvailableYear(years: string[]) {
  const concreteYears = years.filter((year) => year !== "Todos");
  return concreteYears[concreteYears.length - 1] || "Todos";
}

function getLatestAvailableMonth(months: string[]) {
  const concreteMonths = months.filter((month) => month !== "Todos");
  return concreteMonths[concreteMonths.length - 1] || "Todos";
}

function formatMonthOptionLabel(month: string) {
  return MONTH_LABELS_ES[month] || month;
}

function triggerBlobDownload(blob: Blob, fileName: string) {
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = objectUrl;
  anchor.download = fileName;
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 0);
}

export function App() {
  const [serviceOrigin, setServiceOrigin] = useState("");
  const [serviceOriginN1, setServiceOriginN1] = useState("");
  const [serviceOriginN2, setServiceOriginN2] = useState("");
  const [popYear, setPopYear] = useState("Todos");
  const [popMonth, setPopMonth] = useState("Todos");
  const [npsGroup, setNpsGroup] = useState("Todos");
  const [themeMode, setThemeMode] = useState<ThemeMode>(() => readStoredThemeMode());
  const [downloadsPath, setDownloadsPath] = useState("");
  const [helixBaseUrl, setHelixBaseUrl] = useState("");
  const [touchpointSource, setTouchpointSource] = useState("executive_journeys");
  const [comparisonDimension, setComparisonDimension] = useState("Palanca");
  const [gapDimension, setGapDimension] = useState("Palanca");
  const [opportunityDimension, setOpportunityDimension] = useState("Palanca");
  const [cohortRow, setCohortRow] = useState("Palanca");
  const [cohortCol, setCohortCol] = useState("Canal");
  const [minN, setMinN] = useState(200);
  const [minNCross, setMinNCross] = useState(30);
  const [minSimilarity, setMinSimilarity] = useState(0.25);
  const [maxDaysApart, setMaxDaysApart] = useState(10);
  const [mainArea, setMainArea] = useState("insights");
  const [insightTab, setInsightTab] = useState("nps");
  const [npsTab, setNpsTab] = useState("summary");
  const [overviewTab, setOverviewTab] = useState("promoters-vs-detractors");
  const [linkingTab, setLinkingTab] = useState("situation");
  const [ingestTab, setIngestTab] = useState("new");
  const [dataTab, setDataTab] = useState<"nps" | "helix">("nps");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [settingsTab, setSettingsTab] = useState<"appearance" | "advanced" | "maintenance">(
    "appearance"
  );
  const [historyFilter, setHistoryFilter] = useState("");
  const [activeUploadId, setActiveUploadId] = useState<string | null>(null);
  const [tableLimit, setTableLimit] = useState(200);
  const [tableOffset, setTableOffset] = useState(0);
  const [statusCopy, setStatusCopy] = useState("Cargando contexto del producto...");
  const [error, setError] = useState<string | null>(null);
  const [isMutating, setIsMutating] = useState(false);
  const [isGeneratingReport, setIsGeneratingReport] = useState(false);
  const [isSavingHierarchy, setIsSavingHierarchy] = useState(false);
  const [latestNpsUpload, setLatestNpsUpload] = useState<UploadResult | null>(null);
  const [latestHelixUpload, setLatestHelixUpload] = useState<HelixUploadResult | null>(null);
  const didHydrate = useRef(false);

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
    if (!config || didHydrate.current) {
      return;
    }
    didHydrate.current = true;
    const latestYear = getLatestAvailableYear(config.available_years || []);
    const latestMonth = getLatestAvailableMonth(
      config.available_months_by_year[latestYear] || config.available_months_by_year.Todos || []
    );
    setServiceOrigin(config.default_service_origin);
    setServiceOriginN1(config.default_service_origin_n1);
    setServiceOriginN2(config.default_service_origin_n2 || "");
    setPopYear(latestYear);
    setPopMonth(latestMonth);
    setNpsGroup(config.preferences.nps_group_choice || "Todos");
    setThemeMode(normalizeThemeMode(config.preferences.theme_mode));
    setDownloadsPath(config.preferences.downloads_path || "");
    setHelixBaseUrl(config.preferences.helix_base_url || "");
    setTouchpointSource(config.preferences.touchpoint_source || "executive_journeys");
    setMinSimilarity(config.preferences.min_similarity ?? 0.25);
    setMaxDaysApart(config.preferences.max_days_apart ?? 10);
    setMinN(config.preferences.min_n_opportunities ?? 200);
    setMinNCross(config.preferences.min_n_cross_comparisons ?? 30);
  }, [config]);

  const monthOptions = useMemo(() => {
    if (!config) {
      return ["Todos"];
    }
    return config.available_months_by_year[popYear] || config.available_months_by_year.Todos || ["Todos"];
  }, [config, popYear]);

  useEffect(() => {
    const latestMonth = getLatestAvailableMonth(monthOptions);
    if (!monthOptions.includes(popMonth) || (popMonth === "Todos" && latestMonth !== "Todos")) {
      setPopMonth(latestMonth);
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
      min_n_cross: minNCross,
      theme_mode: themeMode
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
      serviceOriginN2,
      themeMode
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
    mainArea === "insights" && serviceOrigin && serviceOriginN1
      ? [
          "linking",
          serviceOrigin,
          serviceOriginN1,
          serviceOriginN2,
          popYear,
          popMonth,
          npsGroup,
          minSimilarity,
          maxDaysApart,
          touchpointSource,
          themeMode
        ]
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
      nps_group: npsGroup,
      min_similarity: minSimilarity,
      max_days_apart: maxDaysApart,
      touchpoint_source: touchpointSource,
      theme_mode: themeMode
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

  const datasetKey =
    mainArea === "data" && serviceOrigin && serviceOriginN1
      ? [
          "dataset",
          dataTab,
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
    fetchDatasetTable(dataTab, {
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
    if (isGeneratingReport) {
      setStatusCopy("Generando la presentación ejecutiva en PowerPoint...");
      return;
    }
    if (isSavingHierarchy) {
      setStatusCopy("Persistiendo la jerarquía de Service Origin...");
      return;
    }
    if (isMutating) {
      setStatusCopy("Importando y rehidratando el histórico persistente...");
      return;
    }
    if (configLoading || dashboardLoading || uploadsLoading || linkingLoading || datasetLoading) {
      setStatusCopy("Cargando contexto, histórico e insights...");
      return;
    }
    setStatusCopy("Producto sincronizado con histórico persistente y reglas de negocio desacopladas.");
  }, [
    configError,
    configLoading,
    dashboardError,
    dashboardLoading,
    datasetError,
    datasetLoading,
    isGeneratingReport,
    isMutating,
    isSavingHierarchy,
    linkingError,
    linkingLoading,
    uploadsError,
    uploadsLoading
  ]);

  const n1Options = config?.service_origin_n1_map[serviceOrigin] || [];
  const n2Options = config?.service_origin_n2_map[serviceOrigin]?.[serviceOriginN1] || [];
  const hasConfiguredN2 = n2Options.length > 0;
  const causalMethodOptions = config?.causal_method_options || [];
  const selectedN2Values = useMemo(() => parseServiceOriginN2(serviceOriginN2), [serviceOriginN2]);
  useEffect(() => {
    if (!n1Options.length) {
      return;
    }
    if (!n1Options.includes(serviceOriginN1)) {
      setServiceOriginN1(n1Options[0]);
    }
  }, [n1Options, serviceOriginN1]);

  useEffect(() => {
    if (!config) {
      return;
    }
    if (!config.available_years.includes(popYear)) {
      setPopYear(getLatestAvailableYear(config.available_years));
    }
  }, [config, popYear]);

  useEffect(() => {
    if (!n2Options.length) {
      if (serviceOriginN2) {
        setServiceOriginN2("");
      }
      return;
    }
    const nextSelectedValues = selectedN2Values.filter((value) => n2Options.includes(value));
    if (nextSelectedValues.length !== selectedN2Values.length) {
      setServiceOriginN2(serializeServiceOriginN2(nextSelectedValues));
    }
  }, [n2Options, selectedN2Values, serviceOriginN2]);

  useEffect(() => {
    if (!causalMethodOptions.length) {
      return;
    }
    if (!causalMethodOptions.some((option) => option.value === touchpointSource)) {
      setTouchpointSource(
        causalMethodOptions.find((option) => option.value === "executive_journeys")?.value ||
          causalMethodOptions[0]?.value ||
          "executive_journeys"
      );
    }
  }, [causalMethodOptions, touchpointSource]);

  useEffect(() => {
    applyDocumentTheme(themeMode);
    persistThemeMode(themeMode);
  }, [themeMode]);

  const preferencesPayload = useMemo<PreferencesPayload>(
    () => ({
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2,
      pop_year: popYear,
      pop_month: popMonth,
      nps_group_choice: npsGroup,
      theme_mode: themeMode,
      downloads_path: downloadsPath,
      helix_base_url: helixBaseUrl,
      touchpoint_source: touchpointSource,
      min_similarity: minSimilarity,
      max_days_apart: maxDaysApart,
      min_n_opportunities: minN,
      min_n_cross_comparisons: minNCross
    }),
    [
      maxDaysApart,
      minN,
      minNCross,
      minSimilarity,
      downloadsPath,
      helixBaseUrl,
      npsGroup,
      popMonth,
      popYear,
      serviceOrigin,
      serviceOriginN1,
      serviceOriginN2,
      themeMode,
      touchpointSource
    ]
  );

  useEffect(() => {
    if (!didHydrate.current || !serviceOrigin || !serviceOriginN1) {
      return undefined;
    }
    const timeoutId = window.setTimeout(() => {
      void persistPreferences(preferencesPayload).catch((caughtError) => {
        setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
      });
    }, 300);
    return () => window.clearTimeout(timeoutId);
  }, [preferencesPayload, serviceOrigin, serviceOriginN1]);

  async function handleNpsUpload(payload: UploadSelectionPayload) {
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
      await Promise.all([mutateConfig(), mutateUploads(), mutateDashboard(), mutateDataset(), mutateLinking()]);
      startTransition(() => {
        setMainArea("ingest");
        setIngestTab("traceability");
        setActiveUploadId(result.upload_id);
      });
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsMutating(false);
    }
  }

  async function handleHelixUpload(payload: UploadSelectionPayload) {
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
        setMainArea("ingest");
        setIngestTab("new");
        setDataTab("helix");
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
      await Promise.all([mutateConfig(), mutateUploads(), mutateDashboard(), mutateDataset(), mutateLinking()]);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsMutating(false);
    }
  }

  async function handleSaveHierarchy(payload: ServiceOriginHierarchyPayload) {
    setIsSavingHierarchy(true);
    setError(null);
    try {
      const nextConfig = await updateServiceOrigins(payload);
      await mutateConfig(nextConfig, { revalidate: false });
      await Promise.all([mutateDashboard(), mutateLinking(), mutateDataset()]);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsSavingHierarchy(false);
    }
  }

  async function handleDownloadReport() {
    setIsGeneratingReport(true);
    setError(null);
    try {
      const report = await downloadExecutiveReport({
        service_origin: serviceOrigin,
        service_origin_n1: serviceOriginN1,
        service_origin_n2: serviceOriginN2,
        pop_year: popYear,
        pop_month: popMonth,
        nps_group: npsGroup,
        min_n: minN,
        min_similarity: minSimilarity,
        max_days_apart: maxDaysApart,
        touchpoint_source: touchpointSource
      });
      triggerBlobDownload(report.blob, report.fileName);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsGeneratingReport(false);
    }
  }

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
  const selectedUpload = uploads.find((upload) => upload.upload_id === activeUploadId) || latestNpsUpload;

  function handleServiceOriginN2Select(event: ChangeEvent<HTMLSelectElement>) {
    const nextValues = Array.from(event.target.selectedOptions, (option) => option.value);
    setServiceOriginN2(serializeServiceOriginN2(nextValues));
  }

  function renderServiceContainer() {
    return (
      <section className="surface-card context-strip-card sidebar-service-card">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Service Container</p>
            <h2>Service Origin</h2>
            <p className="secondary-copy">
              Contexto activo para Insights, Ingesta y Datos
            </p>
          </div>
        </div>
        <div className="field-grid single-column">
          <label>
            <span>BUUG</span>
            <select onChange={(event) => setServiceOrigin(event.target.value)} value={serviceOrigin}>
              {(config?.service_origins || []).map((origin) => (
                <option key={origin} value={origin}>
                  {origin}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>N1</span>
            <select onChange={(event) => setServiceOriginN1(event.target.value)} value={serviceOriginN1}>
              {n1Options.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
          {hasConfiguredN2 ? (
            <label className="field-span-2">
              <span>N2</span>
              <select
                className="multi-select-control"
                multiple
                onChange={handleServiceOriginN2Select}
                value={selectedN2Values}
              >
                {n2Options.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
              <span className="field-hint">Pulsa Ctrl/Cmd para seleccionar varios N2.</span>
            </label>
          ) : null}
        </div>
      </section>
    );
  }

  function renderFiltersContainer() {
    const showCausalMethodFilter = mainArea === "insights" && insightTab === "linking";

    return (
      <section className="surface-card context-strip-card">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Filters</p>
            <h2>FILTROS</h2>
            <p className="secondary-copy">
              Transversales para Insights, Reportes y Datos
            </p>
          </div>
        </div>
        <div className={`field-grid filters-inline-grid${showCausalMethodFilter ? " has-causal-method" : ""}`}>
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
                  {formatMonthOptionLabel(month)}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>Grupo NPS</span>
            <select onChange={(event) => setNpsGroup(event.target.value)} value={npsGroup}>
              {(config?.nps_groups || ["Todos"]).map((group) => (
                <option key={group} value={group}>
                  {group}
                </option>
              ))}
            </select>
          </label>
          {showCausalMethodFilter ? (
            <label>
              <span>Método causal</span>
              <select
                onChange={(event) => setTouchpointSource(event.target.value)}
                value={touchpointSource}
              >
                {causalMethodOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          ) : null}
        </div>
      </section>
    );
  }

  function renderOverviewTab() {
    if (dashboard?.empty_state) {
      return <p className="empty-state">{dashboard.empty_state}</p>;
    }

    if (overviewTab === "promoters-vs-detractors") {
      const situation = (linking?.situation || {}) as Record<string, unknown>;
      const situationFigure = (situation.figure as PlotlyFigureSpec | null | undefined) || null;
      const situationNote = typeof situation.note === "string" ? situation.note : "";

      return (
        <section className="surface-card stack-panel">
          <p className="secondary-copy">
            Lectura diaria de la evolución del mix NPS sin superponer volumen de incidencias.
          </p>
          <PlotFigure
            emptyMessage={
              linking?.empty_state ||
              "No hay suficiente base cruzada para construir la evolución diaria de promotores vs detractores."
            }
            figure={situationFigure}
            testId="promoters-vs-detractors-figure"
          />
          {situationNote ? <p className="secondary-copy">{situationNote}</p> : null}
        </section>
      );
    }

    if (overviewTab === "daily") {
      return (
        <section className="surface-card">
          <p className="secondary-copy">Lectura diaria del NPS y del peso relativo de detractores en el periodo activo.</p>
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
        <section className="surface-card">
          <PlotFigure
            emptyMessage="No hay suficientes datos para construir una tendencia."
            figure={dashboard?.overview.weekly_trend_figure}
            testId="weekly-trend-figure"
          />
        </section>
      );
    }

    if (overviewTab === "topics") {
      const topicRows = (dashboard?.overview.topics_table || []).map((row) => ({
        Cluster: row.cluster_id ?? "",
        n: row.n ?? "",
        "Términos": Array.isArray(row.top_terms) ? row.top_terms.join(", ") : row.top_terms ?? "",
        Ejemplos: Array.isArray(row.examples) ? row.examples.join(" · ") : row.examples ?? ""
      }));

      return (
        <section className="surface-card stack-panel">
          <PlotFigure
            emptyMessage="No hay texto suficiente para extraer temas."
            figure={dashboard?.overview.topics_figure}
            testId="topics-figure"
          />
          <RecordTable emptyMessage="No hay temas disponibles." rows={topicRows} />
        </section>
      );
    }

    if (overviewTab === "volume") {
      return (
        <section className="surface-card">
          <PlotFigure
            emptyMessage="No hay suficientes datos para construir la vista de volumen diario."
            figure={dashboard?.overview.daily_volume_figure}
            testId="daily-volume-figure"
          />
        </section>
      );
    }

    return (
      <section className="surface-card stack-panel">
        <article className="note-card">
          <p className="secondary-copy">
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
        <section className="surface-card stack-panel">
          <div className="section-heading section-heading-inline">
            <div>
              <p className="eyebrow">ÁMBITO DE ANÁLISIS</p>
              <h2>{dashboard?.context_label || "Periodo seleccionado"}</h2>
              <p className="secondary-copy">
                Permite centrarse exclusivamente en NPS Térmico como aplicar causalidad al mismo analizandolo junto a incidencias de cliente
              </p>
            </div>
          </div>

          <div className="metric-grid">
            <article className="metric-card">
              <span>Muestras</span>
              <strong>{formatNumber(dashboard?.kpis.samples, { fallback: "0" })}</strong>
            </article>
            <article className="metric-card">
              <span>NPS medio (0-10)</span>
              <strong>{formatNumber(dashboard?.kpis.nps_average)}</strong>
            </article>
            <article className="metric-card">
              <span>Detractores (≤6)</span>
              <strong>{formatPercent(dashboard?.kpis.detractor_rate)}</strong>
            </article>
            <article className="metric-card">
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
          <section className="surface-card stack-panel">
            {(() => {
              const comparisonRows = (dashboard?.comparison.table || []).map((row) => ({
                Valor: row.value ?? "",
                "Δ NPS": row.delta_nps ?? "",
                "NPS actual": row.nps_current ?? "",
                "NPS base": row.nps_baseline ?? "",
                "n actual": row.n_current ?? "",
                "n base": row.n_baseline ?? ""
              }));

              return (
                <>
            <div className="section-heading section-heading-inline">
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
              <span>Δ NPS: {formatNumber(dashboard?.comparison.summary?.delta_nps, { signed: true })}</span>
              <span>
                Δ detractores: {formatNumber(dashboard?.comparison.summary?.delta_detr_pp, { signed: true })} pp
              </span>
              <span>Base actual: {formatNumber(dashboard?.comparison.summary?.n_current, { fallback: "0" })}</span>
              <span>Base histórica: {formatNumber(dashboard?.comparison.summary?.n_baseline, { fallback: "0" })}</span>
            </div>
            <PlotFigure
              emptyMessage="No hay suficiente histórico para comparar el periodo actual con la base."
              figure={dashboard?.comparison.figure}
              testId="comparison-figure"
            />
            <RecordTable emptyMessage="No hay base comparativa disponible." rows={comparisonRows} />
                </>
              );
            })()}
          </section>
        ) : null}

        {npsTab === "cohorts" ? (
          <section className="surface-card stack-panel">
            <div className="section-heading section-heading-inline">
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
          <section className="surface-card stack-panel">
            {(() => {
              const gapRows = (dashboard?.gaps.table || []).map((row) => ({
                Valor: row.value ?? "",
                n: row.n ?? "",
                NPS: row.nps ?? "",
                Gap: row.gap_vs_overall ?? ""
              }));

              return (
                <>
            <div className="section-heading section-heading-inline">
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
            <RecordTable emptyMessage="No hay gaps disponibles." rows={gapRows} />
                </>
              );
            })()}
          </section>
        ) : null}

        {npsTab === "opportunities" ? (
          <section className="surface-card stack-panel">
            {(() => {
              const opportunityRows = (dashboard?.opportunities.table || []).map((row) => ({
                Etiqueta: row.label ?? `${row.dimension}=${row.value}`,
                n: row.n ?? "",
                "NPS actual": row.current_nps ?? "",
                Uplift: row.potential_uplift ?? "",
                Confianza: row.confidence ?? ""
              }));

              return (
                <>
            <div className="section-heading section-heading-inline">
              <div>
                <p className="eyebrow">Priorización</p>
                <h2>Oportunidades priorizadas</h2>
              </div>
              <label className="inline-field">
                <span>Dimensión</span>
                <select onChange={(event) => setOpportunityDimension(event.target.value)} value={opportunityDimension}>
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
            <RecordTable emptyMessage="No hay oportunidades disponibles." rows={opportunityRows} />
                </>
              );
            })()}
          </section>
        ) : null}
      </>
    );
  }

  function renderLinkingSection() {
    if (!linking?.available) {
      return (
        <section className="surface-card stack-panel">
          <div className="section-heading">
            <div>
              <p className="eyebrow">Incidencias ↔ NPS</p>
              <h2>Base cruzada y readiness operativo</h2>
            </div>
          </div>
          <div className="metric-grid">
            <article className="metric-card">
              <span>Dataset NPS</span>
              <strong>{npsDatasetStatus.available ? formatNumber(npsDatasetStatus.rows) : "—"}</strong>
            </article>
            <article className="metric-card">
              <span>Dataset Helix</span>
              <strong>{helixDatasetStatus.available ? formatNumber(helixDatasetStatus.rows) : "—"}</strong>
            </article>
            <article className="metric-card">
              <span>Última actualización NPS</span>
              <strong>{formatDateLabel(npsDatasetStatus.updated_at)}</strong>
            </article>
            <article className="metric-card">
              <span>Última actualización Helix</span>
              <strong>{formatDateLabel(helixDatasetStatus.updated_at)}</strong>
            </article>
          </div>
          <article className="note-card">
            <p className="secondary-copy">
              {linking?.empty_state ||
                "El dataset Helix aún no está cargado para este contexto. La vista causal se activará cuando exista base cruzada suficiente."}
            </p>
          </article>
        </section>
      );
    }

    return (
      <LinkingWorkspace linking={linking} onTabChange={setLinkingTab} tab={linkingTab} />
    );
  }

  function renderInsightsArea() {
    return (
      <section className="workspace-stack">
        {insightTab === "nps" ? renderNpsSection() : renderLinkingSection()}
      </section>
    );
  }

  function renderIngestArea() {
    const selectedDuplicateCount = selectedUpload
      ? selectedUpload.duplicate_in_file_rows + selectedUpload.duplicate_historical_rows
      : 0;

    return (
      <section className="workspace-stack">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Dominio</p>
            <h2>Ingesta y trazabilidad</h2>
            <p className="secondary-copy">
              Nueva carga, histórico y detalle de ejecución conviven en el mismo flujo operativo.
            </p>
          </div>
        </div>

        <div className="metric-grid">
          <article className="metric-card">
            <span>Dataset NPS</span>
            <strong>{npsDatasetStatus.available ? formatNumber(npsDatasetStatus.rows) : "—"}</strong>
          </article>
          <article className="metric-card">
            <span>Dataset Helix</span>
            <strong>{helixDatasetStatus.available ? formatNumber(helixDatasetStatus.rows) : "—"}</strong>
          </article>
          <article className="metric-card">
            <span>Cargas registradas</span>
            <strong>{formatNumber(uploads.length, { fallback: "0" })}</strong>
          </article>
          <article className="metric-card">
            <span>Último detalle seleccionado</span>
            <strong>{selectedUpload ? formatNumber(selectedDuplicateCount) : "—"}</strong>
          </article>
        </div>

        <NavigationTabs items={INGEST_TABS} onChange={setIngestTab} value={ingestTab} />

        {ingestTab === "new" ? (
          <section className="ingest-grid">
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
              description="Importa el extracto Helix y deja el dataset persistido por contexto para explotación causal posterior."
              eyebrow="Carga Helix"
              feedback={latestHelixUpload}
              onSubmit={handleHelixUpload}
              testId="helix-upload-input"
              title="Dataset Helix"
              uploading={isMutating}
            />
          </section>
        ) : null}

        {ingestTab === "history" ? (
          <UploadsTable
            activeUploadId={activeUploadId}
            filter={historyFilter}
            onFilterChange={setHistoryFilter}
            onSelectUpload={(uploadId) => {
              setActiveUploadId(uploadId);
              setIngestTab("traceability");
            }}
            uploads={uploads}
          />
        ) : null}

        {ingestTab === "traceability" ? (
          <div className="traceability-layout">
            <section className="surface-card">
              <div className="section-heading">
                <div>
                  <p className="eyebrow">Histórico</p>
                  <h2>Selecciona una ejecución</h2>
                </div>
              </div>
              <UploadsTable
                activeUploadId={activeUploadId}
                filter={historyFilter}
                onFilterChange={setHistoryFilter}
                onSelectUpload={setActiveUploadId}
                uploads={uploads}
              />
            </section>

            <aside className="surface-card traceability-aside">
              <div className="section-heading">
                <div>
                  <p className="eyebrow">Detalle de ejecución</p>
                  <h2>Trazabilidad e issues</h2>
                </div>
              </div>
              {!selectedUpload ? (
                <p className="empty-state">Selecciona una carga para inspeccionar warnings, errores y schema drift.</p>
              ) : (
                <>
                  <dl className="detail-list">
                    <div>
                      <dt>Fichero</dt>
                      <dd data-testid="selected-upload-name">{selectedUpload.filename}</dd>
                    </div>
                    <div>
                      <dt>Estado</dt>
                      <dd>{selectedUpload.status}</dd>
                    </div>
                    <div>
                      <dt>Insertados</dt>
                      <dd>{formatNumber(selectedUpload.inserted_rows)}</dd>
                    </div>
                    <div>
                      <dt>Actualizados</dt>
                      <dd>{formatNumber(selectedUpload.updated_rows)}</dd>
                    </div>
                    <div>
                      <dt>Duplicados prevenidos</dt>
                      <dd>{formatNumber(selectedDuplicateCount)}</dd>
                    </div>
                    <div>
                      <dt>Timestamp</dt>
                      <dd>{new Date(selectedUpload.uploaded_at).toLocaleString("es-ES")}</dd>
                    </div>
                  </dl>
                  <IssueList
                    emptyMessage="La carga no generó avisos ni errores."
                    issues={selectedUpload.issues}
                    testId="selected-issues-list"
                  />
                </>
              )}
            </aside>
          </div>
        ) : null}
      </section>
    );
  }

  function renderDataArea() {
    return (
      <section className="workspace-stack">
        <div className="section-heading section-heading-inline">
          <div>
            <p className="eyebrow">Datos</p>
            <h2>Exploración tabular</h2>
            <p className="secondary-copy">Vista tabular paginada para inspección directa de datasets persistidos.</p>
          </div>
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

        <NavigationTabs
          compact
          items={DATA_TABS}
          onChange={(value) => {
            setDataTab(value as "nps" | "helix");
            setTableOffset(0);
          }}
          value={dataTab}
        />

        <section className="surface-card stack-panel">
          <div className="table-meta">
            <span>Filas: {formatNumber(datasetTable?.total_rows, { fallback: "0" })}</span>
            <span>Columnas: {datasetTable?.columns.length || 0}</span>
          </div>

          <RecordTable
            columns={datasetTable?.columns || []}
            emptyMessage="No hay filas disponibles para este dataset."
            rows={datasetTable?.rows || []}
            testId="data-table"
          />

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
      </section>
    );
  }

  return (
    <>
      <main className="app-shell">
        <aside className="app-sidebar">
          <div className="brand-card">
            <p className="eyebrow">BBVA</p>
            <h1>NPS Lens</h1>
            <p className="secondary-copy">
              Análisis del NPS Térmico y causalidad con incidencias de clientes.
            </p>
          </div>

          {renderServiceContainer()}

          <PrimaryNav
            items={MAIN_AREAS}
            onChange={(value) => startTransition(() => setMainArea(value))}
            value={mainArea}
          />
        </aside>

        <section className="workspace">
          <header className="topbar">
            <div className="topbar-actions topbar-actions-floating">
              <button
                aria-label="Generar reporte en PowerPoint"
                className="icon-button topbar-icon-button"
                onClick={() => void handleDownloadReport()}
                type="button"
              >
                <Icon name="presentation" />
              </button>
              <button
                aria-label="Abrir configuración global"
                className="icon-button topbar-icon-button"
                onClick={() => setSettingsOpen(true)}
                type="button"
              >
                <Icon name="settings" />
              </button>
            </div>
            <div className="topbar-copy">
              <p className="eyebrow">NPS Lens</p>
              <h2>Orquestación operativa</h2>
              <p data-testid="status-copy">{statusCopy}</p>
            </div>
            <div className="topbar-actions">
              <span className={`status-chip${isMutating || isGeneratingReport ? " is-busy" : ""}`}>
                {isMutating || isGeneratingReport ? "Sincronizando" : "Operativo"}
              </span>
            </div>
          </header>

          {error ? (
            <section className="error-banner" data-testid="error-banner">
              <strong>Fallo operativo</strong>
              <p>{error}</p>
            </section>
          ) : null}

          {mainArea === "insights" ? (
            <div className="insight-nav-strip">
              <NavigationTabs compact items={INSIGHT_TABS} onChange={setInsightTab} value={insightTab} />
            </div>
          ) : null}

          {mainArea !== "ingest" ? renderFiltersContainer() : null}

          {mainArea === "insights" ? renderInsightsArea() : null}
          {mainArea === "ingest" ? renderIngestArea() : null}
          {mainArea === "data" ? renderDataArea() : null}

          {(dashboardLoading || datasetLoading) && !dashboard ? (
            <section className="surface-card">
              <p className="empty-state">Preparando la vista operativa...</p>
            </section>
          ) : null}
        </section>
      </main>

      <SettingsSheet
        activeTab={settingsTab}
        downloadsPath={downloadsPath}
        helixBaseUrl={helixBaseUrl}
        hierarchySaving={isSavingHierarchy}
        onReprocess={handleReprocess}
        minN={minN}
        minNCross={minNCross}
        minSimilarity={minSimilarity}
        maxDaysApart={maxDaysApart}
        onClose={() => setSettingsOpen(false)}
        onSaveHierarchy={handleSaveHierarchy}
        onTabChange={setSettingsTab}
        open={settingsOpen}
        serviceOriginN1Map={config?.service_origin_n1_map || {}}
        serviceOriginN2Map={config?.service_origin_n2_map || {}}
        serviceOrigins={config?.service_origins || []}
        reprocessPending={isMutating}
        setDownloadsPath={setDownloadsPath}
        setHelixBaseUrl={setHelixBaseUrl}
        setMinN={setMinN}
        setMinNCross={setMinNCross}
        setMinSimilarity={setMinSimilarity}
        setMaxDaysApart={setMaxDaysApart}
        setThemeMode={setThemeMode}
        themeMode={themeMode}
      />
    </>
  );
}

export default App;
