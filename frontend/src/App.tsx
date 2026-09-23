import { startTransition, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import useSWR from "swr";

import {
  downloadExecutiveReport,
  downloadWebPublication,
  deleteHelixUpload,
  deleteNpsUpload,
  deleteOwnerData,
  fetchConfig,
  fetchDashboard,
  fetchDatasetTable,
  fetchLinkingDashboard,
  fetchHelixUploads,
  fetchUploads,
  persistPreferences,
  replaceNpsUpload,
  reprocessSummary,
  updateServiceOrigins,
  uploadHelixFile,
  uploadNpsFile
} from "./api";
import type {
  DashboardPayload,
  DatasetStatus,
  HelixUploadHistoryItem,
  HelixUploadResult,
  KpiDelta,
  LinkingPayload,
  PreferencesPayload,
  ServiceOriginHierarchyPayload,
  ScopeKpiBlock,
  UploadSelectionPayload,
  UploadResult
} from "./api";
import { TaxonomyIngestNotice, TaxonomyStudio } from "./components/TaxonomyStudio";
import { DatasetUploadCard } from "./components/DatasetUploadCard";
import { IssueList } from "./components/IssueList";
import { LinkingWorkspace } from "./components/LinkingWorkspace";
import { NavigationTabs } from "./components/NavigationTabs";
import { PlotFigure } from "./components/PlotFigure";
import { PrimaryNav } from "./components/PrimaryNav";
import { RecordTable } from "./components/RecordTable";
import { SettingsSheet } from "./components/SettingsSheet";
import type { SettingsTab } from "./components/SettingsSheet";
import { UploadsTable } from "./components/UploadsTable";
import { HelixUploadsTable } from "./components/HelixUploadsTable";
import { Icon } from "./components/Icon";
import {
  applyDocumentTheme,
  normalizeThemeMode,
  persistThemeMode,
  readStoredThemeMode,
  type ThemeMode
} from "./theme";
import {
  formatDelta,
  formatMetric,
  formatNumber,
  formatPercentage,
  formatVolume
} from "./utils/numberFormat";
import { toBusinessCopy } from "./utils/businessCopy";

const MAIN_AREAS = [
  { id: "taxonomy", label: "Taxonomy Studio", description: "Lentes, cobertura y comparación", icon: "database" as const },
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
  { id: "summary", label: "Evolución NPS" },
  { id: "nps-analysis", label: "Comentarios" },
  { id: "linking", label: "Causalidad" }
];

const INGEST_TABS = [
  { id: "new", label: "Nueva carga" },
  { id: "history", label: "Histórico" },
  { id: "traceability", label: "Detalle de ejecución" },
  { id: "publication", label: "Publicación Web" }
];

const SUMMARY_TABS = [
  { id: "period-aggregates", label: "Agregados por periodo" },
  { id: "daily", label: "NPS clásico vs detractores" },
  { id: "volume-mix", label: "Cómo y cuándo lo dicen" },
  { id: "cohorts", label: "Comparativas cruzadas" }
];

const NPS_TABS = [
  { id: "topics", label: "Qué dicen los clientes" },
  { id: "gaps", label: "Brechas NPS" }
];

const DATA_TABS = [
  { id: "nps", label: "NPS" },
  { id: "helix", label: "Helix" }
];

const SAMPLE_SIZES = [50, 100, 200, 500, 1000];
const LINKING_NPS_GROUP = "Todos";
type OperationalState = "operativo" | "sincronizando" | "generando";

function renderStrongMarkdown(text: string): ReactNode[] {
  const parts: ReactNode[] = [];
  const pattern = /\*\*(.+?)\*\*/g;
  let cursor = 0;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(text)) !== null) {
    if (match.index > cursor) {
      parts.push(text.slice(cursor, match.index));
    }
    parts.push(<strong key={`${match.index}-${match[1]}`}>{match[1]}</strong>);
    cursor = match.index + match[0].length;
  }
  if (cursor < text.length) {
    parts.push(text.slice(cursor));
  }
  return parts.length ? parts : [text];
}

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

function getLatestAvailableYear(years: string[]) {
  const concreteYears = years.filter((year) => year !== "Todos");
  return concreteYears[concreteYears.length - 1] || "Todos";
}

function getLatestAvailableMonth(months: string[]) {
  const concreteMonths = months.filter((month) => month !== "Todos");
  return concreteMonths[concreteMonths.length - 1] || "Todos";
}

function chooseDefaultOption(options: string[], preferred: string, persisted?: string) {
  const available = options.length ? options : ["Todos"];
  const persistedValue = (persisted || "").trim();
  if (persistedValue && persistedValue !== "Todos") {
    const matchedPersisted = available.find(
      (option) => option.toLocaleLowerCase() === persistedValue.toLocaleLowerCase()
    );
    if (matchedPersisted) {
      return matchedPersisted;
    }
  }
  const matchedPreferred = available.find(
    (option) => option.toLocaleLowerCase() === preferred.toLocaleLowerCase()
  );
  return matchedPreferred || (available.includes("Todos") ? "Todos" : available[0] || "Todos");
}

function formatMonthOptionLabel(month: string) {
  return MONTH_LABELS_ES[month] || month;
}

function compactPeriodLabel(label: string) {
  return label.replace(/\b\d{2}(\d{2})\b/g, "$1");
}

type KpiPayload = DashboardPayload["kpis"];
type KpiKind = "metric" | "percentage" | "volume";

function formatKpiValue(
  kpis: KpiPayload | undefined,
  display: Record<string, string> | undefined,
  key: keyof KpiPayload,
  kind: KpiKind
) {
  const displayValue = display?.[String(key)];
  if (displayValue) {
    return displayValue;
  }
  const rawValue = kpis?.[key];
  if (kind === "percentage") {
    return formatPercentage(rawValue);
  }
  if (kind === "volume") {
    return formatVolume(rawValue);
  }
  return formatMetric(rawValue);
}

export function App() {
  const [serviceOrigin, setServiceOrigin] = useState("");
  const serviceOriginN1 = "";
  const serviceOriginN2 = "";
  const [popYear, setPopYear] = useState("Todos");
  const [popMonth, setPopMonth] = useState("Todos");
  const [npsGroup, setNpsGroup] = useState("Detractores");
  const [scoreChannel, setScoreChannel] = useState("Web");
  const [themeMode, setThemeMode] = useState<ThemeMode>(() => readStoredThemeMode());
  const [downloadsPath, setDownloadsPath] = useState("");
  const [helixBaseUrl, setHelixBaseUrl] = useState("");
  const [reportDimensionAnalysis, setReportDimensionAnalysis] = useState<"palanca" | "subpalanca">("palanca");
  const [touchpointSource, setTouchpointSource] = useState("broken_journeys");
  const [gapDimension, setGapDimension] = useState("Palanca");
  const [cohortRow, setCohortRow] = useState("Palanca");
  const [cohortCol, setCohortCol] = useState("Canal");
  const [minN, setMinN] = useState(200);
  const [minNCross, setMinNCross] = useState(30);
  const [minSimilarity, setMinSimilarity] = useState(0.15);
  const [maxDaysApart, setMaxDaysApart] = useState(90);
  const [mainArea, setMainArea] = useState("insights");
  const [insightTab, setInsightTab] = useState("summary");
  const [summaryTab, setSummaryTab] = useState("period-aggregates");
  const [npsTab, setNpsTab] = useState("topics");
  const [linkingTab, setLinkingTab] = useState("situation");
  const [ingestTab, setIngestTab] = useState("new");
  const [dataTab, setDataTab] = useState<"nps" | "helix">("nps");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [settingsTab, setSettingsTab] = useState<SettingsTab>("appearance");
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
  const initialContextKey = useRef("");

  const selectedContextKey = serviceOrigin;
  const configKey =
    !serviceOrigin || selectedContextKey === initialContextKey.current
      ? ["dashboard-context-initial"]
      : ["dashboard-context", serviceOrigin];

  const {
    data: config,
    error: configError,
    isLoading: configLoading,
    isValidating: configValidating,
    mutate: mutateConfig
  } = useSWR(
    configKey,
    () =>
      fetchConfig({
        service_origin: serviceOrigin || undefined,
        service_origin_n1: serviceOriginN1 || undefined,
        service_origin_n2: serviceOriginN2 || undefined
      }),
    { keepPreviousData: true, revalidateOnFocus: false }
  );

  useEffect(() => {
    if (!config || didHydrate.current) {
      return;
    }
    didHydrate.current = true;
    initialContextKey.current = config.default_service_origin;
    const latestYear = getLatestAvailableYear(config.available_years || []);
    const latestMonth = getLatestAvailableMonth(
      config.available_months_by_year[latestYear] || config.available_months_by_year.Todos || []
    );
    setServiceOrigin(config.default_service_origin);
    setPopYear(latestYear);
    setPopMonth(latestMonth);
    setScoreChannel(
      chooseDefaultOption(config.score_channels || ["Todos"], "Web", config.preferences.score_channel)
    );
    setNpsGroup(
      chooseDefaultOption(config.nps_groups || ["Todos"], "Detractores", config.preferences.nps_group_choice)
    );
    setThemeMode(normalizeThemeMode(config.preferences.theme_mode));
    setDownloadsPath(config.preferences.downloads_path || "");
    setHelixBaseUrl(config.preferences.helix_base_url || "");
    setReportDimensionAnalysis(config.preferences.report_dimension_analysis || "palanca");
    setTouchpointSource(config.preferences.touchpoint_source || "broken_journeys");
    setMinSimilarity(config.preferences.min_similarity ?? 0.15);
    setMaxDaysApart(config.preferences.max_days_apart ?? 90);
    setMinN(config.preferences.min_n_nps_gaps ?? 200);
    setMinNCross(config.preferences.min_n_cross_comparisons ?? 30);
  }, [config]);

  const monthOptions = useMemo(() => {
    if (!config) {
      return ["Todos"];
    }
    return config.available_months_by_year[popYear] || config.available_months_by_year.Todos || ["Todos"];
  }, [config, popYear]);

  useEffect(() => {
    if (!monthOptions.includes(popMonth)) {
      setPopMonth(monthOptions.includes("Todos") ? "Todos" : getLatestAvailableMonth(monthOptions));
    }
  }, [monthOptions, popMonth]);

  useEffect(() => {
    if (!config) {
      return;
    }
    const options = config.score_channels || ["Todos"];
    if (!options.some((option) => option === scoreChannel)) {
      setScoreChannel(chooseDefaultOption(options, "Web", config.preferences.score_channel));
    }
  }, [config, scoreChannel]);

  useEffect(() => {
    if (!config) {
      return;
    }
    const options = config.nps_groups || ["Todos"];
    if (!options.some((option) => option === npsGroup)) {
      setNpsGroup(chooseDefaultOption(options, "Detractores", config.preferences.nps_group_choice));
    }
  }, [config, npsGroup]);

  const dashboardQuery = useMemo(
    () => ({
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2,
      pop_year: popYear,
      pop_month: popMonth,
      nps_group: npsGroup,
      score_channel: scoreChannel,
      gap_dimension: gapDimension,
      cohort_row: cohortRow,
      cohort_col: cohortCol,
      min_n: minN,
      min_n_cross: minNCross,
      theme_mode: themeMode
    }),
    [
      cohortCol,
      cohortRow,
      gapDimension,
      minN,
      minNCross,
      npsGroup,
      popMonth,
      popYear,
      scoreChannel,
      serviceOrigin,
      serviceOriginN1,
      serviceOriginN2,
      themeMode
    ]
  );

  const dashboardKey =
    serviceOrigin ? ["dashboard", ...Object.values(dashboardQuery)] : null;
  const {
    data: dashboard,
    error: dashboardError,
    isLoading: dashboardLoading,
    isValidating: dashboardValidating,
    mutate: mutateDashboard
  } = useSWR<DashboardPayload>(dashboardKey, () => fetchDashboard(dashboardQuery), {
    keepPreviousData: true,
    revalidateOnFocus: false
  });

  const linkingKey =
    mainArea === "insights" && insightTab === "linking" && serviceOrigin
      ? [
          "linking",
          serviceOrigin,
          serviceOriginN1,
          serviceOriginN2,
          popYear,
          popMonth,
          scoreChannel,
          LINKING_NPS_GROUP,
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
    isValidating: linkingValidating,
    mutate: mutateLinking
  } = useSWR<LinkingPayload>(linkingKey, () =>
    fetchLinkingDashboard({
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2,
      pop_year: popYear,
      pop_month: popMonth,
      nps_group: LINKING_NPS_GROUP,
      score_channel: scoreChannel,
      min_similarity: minSimilarity,
      max_days_apart: maxDaysApart,
      touchpoint_source: touchpointSource,
      theme_mode: themeMode
    }),
    { keepPreviousData: true, revalidateOnFocus: false }
  );

  const uploadsKey =
    serviceOrigin ? ["uploads", serviceOrigin] : null;
  const {
    data: uploads = [],
    error: uploadsError,
    isLoading: uploadsLoading,
    isValidating: uploadsValidating,
    mutate: mutateUploads
  } = useSWR<UploadResult[]>(uploadsKey, () =>
    fetchUploads({
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2
    }),
    { keepPreviousData: true, revalidateOnFocus: false }
  );

  const { data: helixUploads = [], mutate: mutateHelixUploads } = useSWR<HelixUploadHistoryItem[]>(
    serviceOrigin ? ["helix-uploads", serviceOrigin] : null,
    () => fetchHelixUploads(serviceOrigin),
    { keepPreviousData: true, revalidateOnFocus: false }
  );

  const datasetKey =
    mainArea === "data" && serviceOrigin
      ? [
          "dataset",
          dataTab,
          serviceOrigin,
          serviceOriginN1,
          serviceOriginN2,
          popYear,
          popMonth,
          scoreChannel,
          npsGroup,
          tableOffset,
          tableLimit
        ]
      : null;
  const {
    data: datasetTable,
    error: datasetError,
    isLoading: datasetLoading,
    isValidating: datasetValidating,
    mutate: mutateDataset
  } = useSWR(datasetKey, () =>
    fetchDatasetTable(dataTab, {
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2,
      pop_year: popYear,
      pop_month: popMonth,
      nps_group: npsGroup,
      score_channel: scoreChannel,
      offset: tableOffset,
      limit: tableLimit
    }),
    { keepPreviousData: true, revalidateOnFocus: false }
  );

  const isSynchronizing =
    isMutating ||
    isSavingHierarchy ||
    configLoading ||
    dashboardLoading ||
    uploadsLoading ||
    linkingLoading ||
    datasetLoading ||
    configValidating ||
    dashboardValidating ||
    uploadsValidating ||
    linkingValidating ||
    datasetValidating;
  const operationalState: OperationalState = isGeneratingReport
    ? "generando"
    : isSynchronizing
      ? "sincronizando"
      : "operativo";
  const actionsDisabled = operationalState !== "operativo";
  const stableError =
    (configError && !configLoading && !configValidating) ||
    (dashboardError && !dashboardLoading && !dashboardValidating) ||
    (uploadsError && !uploadsLoading && !uploadsValidating) ||
    (datasetError && !datasetLoading && !datasetValidating) ||
    (linkingError && !linkingLoading && !linkingValidating) ||
    null;

  useEffect(() => {
    if (!uploads.length) {
      setActiveUploadId(null);
      return;
    }
    setActiveUploadId((current) => current || uploads[0]?.upload_id || null);
  }, [uploads]);

  useEffect(() => {
    if (stableError) {
      setError(stableError.message);
      setStatusCopy("La interfaz no pudo sincronizar el contexto operativo.");
      return;
    }
    setError(null);
    if (operationalState === "generando") {
      setStatusCopy("Generando la presentación ejecutiva en PowerPoint...");
      return;
    }
    if (isSavingHierarchy) {
      setStatusCopy("Persistiendo la configuración de canales...");
      return;
    }
    if (isMutating) {
      setStatusCopy("Importando y rehidratando el histórico persistente...");
      return;
    }
    if (operationalState === "sincronizando") {
      setStatusCopy("Cargando contexto, histórico e insights...");
      return;
    }
    setStatusCopy("Producto sincronizado con histórico persistente y reglas de negocio desacopladas.");
  }, [
    isGeneratingReport,
    isMutating,
    isSavingHierarchy,
    operationalState,
    stableError
  ]);

  const isAdmin = config?.access?.is_admin ?? true;
  const causalMethodOptions = config?.causal_method_options || [];

  useEffect(() => {
    if (!config) {
      return;
    }
    if (!config.available_years.includes(popYear)) {
      setPopYear(getLatestAvailableYear(config.available_years));
    }
  }, [config, popYear]);

  useEffect(() => {
    if (!causalMethodOptions.length) {
      return;
    }
    if (!causalMethodOptions.some((option) => option.value === touchpointSource)) {
      setTouchpointSource(
        causalMethodOptions.find((option) => option.value === config?.preferences.touchpoint_source)?.value ||
        causalMethodOptions.find((option) => option.value === "broken_journeys")?.value ||
          causalMethodOptions[0]?.value ||
          "broken_journeys"
      );
    }
  }, [causalMethodOptions, touchpointSource, config?.preferences.touchpoint_source]);

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
      score_channel: scoreChannel,
      theme_mode: themeMode,
      downloads_path: downloadsPath,
      helix_base_url: helixBaseUrl,
      report_dimension_analysis: reportDimensionAnalysis,
      touchpoint_source: touchpointSource,
      min_similarity: minSimilarity,
      max_days_apart: maxDaysApart,
      min_n_nps_gaps: minN,
      min_n_cross_comparisons: minNCross
    }),
    [
      maxDaysApart,
      minN,
      minNCross,
      minSimilarity,
      downloadsPath,
      helixBaseUrl,
      reportDimensionAnalysis,
      npsGroup,
      popMonth,
      popYear,
      scoreChannel,
      serviceOrigin,
      serviceOriginN1,
      serviceOriginN2,
      themeMode,
      touchpointSource
    ]
  );

  useEffect(() => {
    if (!didHydrate.current || !serviceOrigin) {
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

  async function handleReplaceNpsUpload(upload: UploadResult) {
    const confirmed = globalThis.confirm(
      `Se eliminarán los registros exclusivos de la carga anterior de “${toBusinessCopy(upload.filename)}” y se reingestará el fichero conservado con las reglas actuales. ¿Quieres continuar?`
    );
    if (!confirmed) {
      return;
    }
    setIsMutating(true);
    setError(null);
    try {
      const result = await replaceNpsUpload(upload.upload_id);
      setLatestNpsUpload(result);
      await Promise.all([mutateConfig(), mutateUploads(), mutateDashboard(), mutateDataset(), mutateLinking()]);
      setActiveUploadId(result.upload_id);
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
      await Promise.all([mutateConfig(), mutateDataset(), mutateLinking(), mutateHelixUploads()]);
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

  async function handleDeleteNpsUpload(upload: UploadResult) {
    if (!globalThis.confirm(`¿Borrar definitivamente la ingesta NPS “${toBusinessCopy(upload.filename)}”?`)) {
      return;
    }
    setIsMutating(true);
    try {
      await deleteNpsUpload(upload.upload_id, serviceOrigin);
      await Promise.all([mutateConfig(), mutateUploads(), mutateDashboard(), mutateDataset(), mutateLinking()]);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsMutating(false);
    }
  }

  async function handleDeleteHelixUpload(upload: HelixUploadHistoryItem) {
    if (!globalThis.confirm(`¿Borrar definitivamente la ingesta de incidencias “${toBusinessCopy(upload.filename)}”?`)) {
      return;
    }
    setIsMutating(true);
    try {
      await deleteHelixUpload(upload.upload_id, serviceOrigin);
      await Promise.all([mutateConfig(), mutateHelixUploads(), mutateDataset(), mutateLinking()]);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsMutating(false);
    }
  }

  async function handleDeleteOwnerData() {
    if (!globalThis.confirm(`¿Borrar COMPLETAMENTE todos los datos NPS e incidencias de ${serviceOrigin}? Esta acción no se puede deshacer.`)) {
      return;
    }
    setIsMutating(true);
    try {
      await deleteOwnerData(serviceOrigin);
      setLatestNpsUpload(null);
      setLatestHelixUpload(null);
      await Promise.all([mutateConfig(), mutateUploads(), mutateHelixUploads(), mutateDashboard(), mutateDataset(), mutateLinking()]);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsMutating(false);
    }
  }

  const taxonomyContext = { service_origin: serviceOrigin, service_origin_n1: serviceOriginN1, service_origin_n2: serviceOriginN2 };
  async function refreshTaxonomy() {
    await Promise.all([mutateConfig(), mutateDashboard(), mutateDataset(), mutateLinking()]);
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
      const savedPath = await downloadExecutiveReport({
        service_origin: serviceOrigin,
        service_origin_n1: serviceOriginN1,
        service_origin_n2: serviceOriginN2,
        pop_year: popYear,
        pop_month: popMonth,
        nps_group: LINKING_NPS_GROUP,
        score_channel: scoreChannel,
        min_similarity: minSimilarity,
        max_days_apart: maxDaysApart,
        touchpoint_source: touchpointSource,
        report_dimension_analysis: reportDimensionAnalysis
      });
      setStatusCopy(savedPath ? `Informe guardado en ${savedPath}` : "Informe descargado correctamente.");
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Error desconocido");
    } finally {
      setIsGeneratingReport(false);
    }
  }

  function exportQuery() {
    return {
      service_origin: serviceOrigin,
      service_origin_n1: serviceOriginN1,
      service_origin_n2: serviceOriginN2,
      pop_year: popYear,
      pop_month: popMonth,
      nps_group: npsGroup,
      min_n: minN,
      min_similarity: minSimilarity,
      max_days_apart: maxDaysApart,
      touchpoint_source: touchpointSource,
      report_dimension_analysis: reportDimensionAnalysis
    };
  }

  async function handleDownloadPublication() {
    setIsGeneratingReport(true);
    setError(null);
    try {
      const savedPath = await downloadWebPublication(exportQuery());
      setStatusCopy(savedPath ? `Publicación Web guardada en ${savedPath}` : "Publicación Web descargada correctamente.");
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

  function renderServiceContainer() {
    return (
      <section className="surface-card context-strip-card sidebar-service-card">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Contexto de compañía</p>
            <h2>Owner Support Company</h2>
            <p className="secondary-copy">
              Contexto activo para Insights, Ingesta y Datos
            </p>
          </div>
        </div>
        <div className="field-grid single-column">
          <label>
            <span>Owner Support Company</span>
            <select
              disabled={actionsDisabled}
              onChange={(event) => setServiceOrigin(event.target.value)}
              value={serviceOrigin}
            >
              {(config?.service_origins || []).map((origin) => (
                <option key={origin} value={origin}>
                  {origin}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>
    );
  }

  function renderPeriodContainer() {
    return (
      <section className="surface-card context-strip-card sidebar-service-card">
        <div className="section-heading">
          <div>
            <p className="eyebrow">PERIOD CONTAINER</p>
            <h2>Periodo</h2>
            <p className="secondary-copy">Ventana transversal para aplicación y reportes</p>
          </div>
        </div>
        <div className="field-grid single-column">
          <label>
            <span>Año</span>
            <select
              data-testid="period-year-select"
              disabled={actionsDisabled}
              onChange={(event) => setPopYear(event.target.value)}
              value={popYear}
            >
              {(config?.available_years || ["Todos"]).map((year) => (
                <option key={year} value={year}>
                  {year}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>Mes</span>
            <select
              data-testid="period-month-select"
              disabled={actionsDisabled}
              onChange={(event) => setPopMonth(event.target.value)}
              value={popMonth}
            >
              {monthOptions.map((month) => (
                <option key={month} value={month}>
                  {formatMonthOptionLabel(month)}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>
    );
  }

  function renderAnalysisFiltersContainer(
    showCausalMethodFilter: boolean,
    showScoreGroup: boolean = true
  ) {
    const gridClass = `field-grid filters-inline-grid${showCausalMethodFilter ? " has-causal-method fixed-causal-filters" : ""}`;

    return (
      <section className="surface-card context-strip-card" data-testid="analysis-filters">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Filters</p>
            <h2>FILTROS</h2>
            <p className="secondary-copy">
              Sincronizados para Comentarios, Causalidad y reportes ejecutivos
            </p>
          </div>
        </div>
        <div className={gridClass}>
          <label>
            <span>Canal</span>
            <select
              data-testid="score-channel-select"
              disabled={actionsDisabled}
              onChange={(event) => setScoreChannel(event.target.value)}
              value={scoreChannel}
            >
              {(config?.score_channels || ["Todos"]).map((channel) => (
                <option key={channel} value={channel}>
                  {channel}
                </option>
              ))}
            </select>
          </label>
          {!showCausalMethodFilter && showScoreGroup ? (
            <label>
              <span>Grupo Score</span>
              <select
                data-testid="score-group-select"
                disabled={actionsDisabled}
                onChange={(event) => setNpsGroup(event.target.value)}
                value={npsGroup}
              >
                {(config?.nps_groups || ["Todos"]).map((group) => (
                  <option key={group} value={group}>
                    {group}
                  </option>
                ))}
              </select>
            </label>
          ) : null}
          {showCausalMethodFilter ? (
            <label>
              <span>Método de agrupación</span>
              <select
                disabled={actionsDisabled}
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

  function renderTopicsPanel() {
    if (dashboard?.empty_state) {
      return <p className="empty-state">{dashboard.empty_state}</p>;
    }

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

  function renderCohortsPanel() {
    return (
      <section className="surface-card stack-panel">
            <div className="section-heading section-heading-inline">
              <div>
                <p className="eyebrow">Cohortes</p>
                <h2>Bolsas de fricción</h2>
              </div>
              <div className="inline-actions">
                <label className="inline-field">
                  <span>Filas</span>
                  <select
                    disabled={actionsDisabled}
                    onChange={(event) => setCohortRow(event.target.value)}
                    value={cohortRow}
                  >
                    {(dashboard?.controls.cohort_rows || []).map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="inline-field">
                  <span>Columnas</span>
                  <select
                    disabled={actionsDisabled}
                    onChange={(event) => setCohortCol(event.target.value)}
                    value={cohortCol}
                  >
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
    );
  }

  function renderGapsPanel() {
    const gapColumnLabel = dashboard?.gaps.gap_column_label || "Brecha vs Base";
    const periodLabel = compactPeriodLabel(dashboard?.context_label || "Periodo");
    const gapRows = (dashboard?.gaps.table || []).map((row) => ({
      Valor: row.value ?? "",
      "Peso en la muestra": row.sample_share == null ? "" : formatPercentage(Number(row.sample_share)),
      [`Total opiniones [${periodLabel}]`]: row.n ?? "",
      [`Opiniones detractoras [${periodLabel}]`]: row.detractors ?? "",
      [`% promotor [${periodLabel}]`]: row.promoter_rate == null ? "" : formatPercentage(Number(row.promoter_rate)),
      [`% detractor [${periodLabel}]`]: row.detractor_rate == null ? "" : formatPercentage(Number(row.detractor_rate)),
      [`NPS Clásico [${periodLabel}]`]: row.nps ?? "",
      [gapColumnLabel]: row.gap_vs_base ?? ""
    }));
    const gapTitle = dashboard?.gaps.title || "Brechas NPS";
    const gapSubtitle =
      dashboard?.gaps.subtitle ||
      "Las barras comparan cada segmento con el NPS clásico acumulado anterior al periodo activo.";

    return (
      <section className="surface-card stack-panel">
            <div className="section-heading section-heading-inline">
              <div>
                <p className="eyebrow">Brechas</p>
                <h2>{gapTitle}</h2>
                <p>{gapSubtitle}</p>
                <p className="metric-note">
                  NPS clásico base: {formatNumber(dashboard?.gaps.base_nps)}
                  {dashboard?.gaps.base_range?.start && dashboard?.gaps.base_range?.end
                    ? ` (${formatDateLabel(dashboard.gaps.base_range.start)} - ${formatDateLabel(dashboard.gaps.base_range.end)})`
                    : ""}
                </p>
              </div>
              <label className="inline-field">
                <span>Dimensión</span>
                <select
                  disabled={actionsDisabled}
                  onChange={(event) => setGapDimension(event.target.value)}
                  value={gapDimension}
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
              emptyMessage="No hay datos suficientes para calcular brechas."
              figure={dashboard?.gaps.figure}
              testId="gaps-figure"
            />
            <RecordTable emptyMessage="No hay brechas disponibles." rows={gapRows} />
      </section>
    );
  }

  function renderScopeMetricCard(
    label: string,
    value: string,
    delta?: KpiDelta,
    deltaKind: KpiKind = "metric"
  ) {
    const deltaText = delta?.display || (delta ? formatDelta(delta.value, deltaKind) : "");
    const deltaClass =
      delta?.favorable === true ? "is-positive" : delta?.favorable === false ? "is-negative" : "";
    return (
      <article className="metric-card metric-card-with-delta">
        <span>{label}</span>
        <strong>{value}</strong>
        {delta ? (
          <small className={`metric-delta ${deltaClass}`.trim()}>
            {deltaText || "sin histórico"}
          </small>
        ) : null}
      </article>
    );
  }

  function renderKpiGrid(
    block: ScopeKpiBlock | undefined,
    fallbackKpis: KpiPayload | undefined,
    withDeltas?: boolean
  ) {
    const kpis = block?.kpis || fallbackKpis;
    const display = block?.display;
    const shouldShowDeltas = withDeltas ?? Boolean(block?.show_deltas);
    const deltas = shouldShowDeltas ? block?.deltas : undefined;
    return (
      <div className="metric-grid metric-grid-5">
        {renderScopeMetricCard(
          "Comentarios",
          formatKpiValue(kpis, display, "comments", "volume"),
          deltas?.comments,
          "volume"
        )}
        {renderScopeMetricCard(
          "Score Medio",
          formatKpiValue(kpis, display, "nps_average", "metric"),
          deltas?.nps_average,
          "metric"
        )}
        {renderScopeMetricCard(
          "NPS Clásico",
          formatKpiValue(kpis, display, "classic_nps", "metric"),
          deltas?.classic_nps,
          "metric"
        )}
        {renderScopeMetricCard(
          "Detractores",
          formatKpiValue(kpis, display, "detractor_rate", "percentage"),
          deltas?.detractor_rate,
          "percentage"
        )}
        {renderScopeMetricCard(
          "Promotores",
          formatKpiValue(kpis, display, "promoter_rate", "percentage"),
          deltas?.promoter_rate,
          "percentage"
        )}
      </div>
    );
  }

  function renderSummaryTabContent() {
    if (dashboard?.empty_state) {
      return <p className="empty-state">{dashboard.empty_state}</p>;
    }

    if (summaryTab === "daily") {
      return (
        <section className="surface-card stack-panel">
          <PlotFigure
            emptyMessage="No hay suficientes datos para construir la vista diaria."
            figure={dashboard?.overview.daily_kpis_figure}
            testId="daily-kpis-figure"
          />
          <PlotFigure
            emptyMessage="No hay suficientes datos para construir la distribución diaria por grupo."
            figure={dashboard?.overview.daily_mix_figure}
            testId="daily-mix-figure"
          />
          <article className="note-card">
            <ul className="plain-list">
              {(dashboard?.overview.daily_explanation_bullets || []).map((bullet) => (
                <li key={bullet}>{renderStrongMarkdown(bullet)}</li>
              ))}
            </ul>
          </article>
        </section>
      );
    }

    if (summaryTab === "period-aggregates") {
      return (
        <section className="surface-card stack-panel">
          {dashboard?.scope?.historical ? (
            <>
              <div className="section-heading section-heading-inline scope-period-heading">
                <div>
                  <h3>{dashboard.scope.historical.label}</h3>
                  {dashboard.scope.historical.note ? (
                    <p className="secondary-copy">{dashboard.scope.historical.note}</p>
                  ) : null}
                </div>
              </div>
              {renderKpiGrid(dashboard.scope.historical, dashboard?.kpis, false)}
            </>
          ) : null}

          <div className="section-heading section-heading-inline scope-period-heading">
            <div>
              <h3>{dashboard?.scope?.period?.label || dashboard?.context_label || "Periodo seleccionado"}</h3>
              {dashboard?.scope?.period?.note ? (
                <p className="secondary-copy">{dashboard.scope.period.note}</p>
              ) : null}
            </div>
          </div>
          {renderKpiGrid(dashboard?.scope?.period, dashboard?.kpis)}

          <PlotFigure
            emptyMessage="No hay suficientes datos para construir los agregados por periodo."
            figure={dashboard?.overview.period_aggregates_figure}
            testId="period-aggregates-figure"
          />
        </section>
      );
    }

    if (summaryTab === "volume-mix") {
      return (
        <section className="surface-card">
          <PlotFigure
            emptyMessage="No hay suficientes datos para construir la vista diaria."
            figure={dashboard?.overview.daily_volume_mix_figure}
            testId="daily-volume-mix-figure"
          />
        </section>
      );
    }

    return renderCohortsPanel();
  }

  function renderSummarySection() {
    return (
      <>
        <section className="surface-card stack-panel">
          <div className="section-heading section-heading-inline">
            <div>
              <p className="eyebrow">ÁMBITO DE ANÁLISIS</p>
              <h2>{dashboard?.scope?.cumulative?.label || `Datos acumulados hasta ${dashboard?.context_label || "periodo seleccionado"}`}</h2>
              <p className="secondary-copy">
                {dashboard?.scope?.cumulative?.note || "KPIs agregados para el periodo disponible."}
              </p>
            </div>
          </div>

          {renderKpiGrid(dashboard?.scope?.cumulative, dashboard?.kpis, false)}
        </section>

        <NavigationTabs
          compact
          disabled={actionsDisabled}
          items={SUMMARY_TABS}
          onChange={setSummaryTab}
          value={summaryTab}
        />
        {renderSummaryTabContent()}
      </>
    );
  }

  function renderNpsSection() {
    const content =
      npsTab === "topics"
        ? renderTopicsPanel()
        : renderGapsPanel();
    return (
      <>
        <NavigationTabs
          compact
          disabled={actionsDisabled}
          items={NPS_TABS}
          onChange={setNpsTab}
          value={npsTab}
        />
        {content}
      </>
    );
  }

  function renderLinkingSection() {
    if (!linking?.available) {
      return (
        <section className="surface-card stack-panel">
          <div className="section-heading">
            <div>
              <p className="eyebrow">Causalidad</p>
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
        {insightTab === "summary" ? renderSummarySection() : null}
        {insightTab === "nps-analysis" ? (
          <>
            {renderAnalysisFiltersContainer(false, npsTab !== "gaps")}
            {renderNpsSection()}
          </>
        ) : null}
        {insightTab === "linking" ? (
          <>
            {renderAnalysisFiltersContainer(true)}
            {renderLinkingSection()}
          </>
        ) : null}
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

        <NavigationTabs
          disabled={actionsDisabled}
          items={INGEST_TABS}
          onChange={setIngestTab}
          value={ingestTab}
        />

        {ingestTab === "new" ? (
          <section className="ingest-grid">
            <DatasetUploadCard
              ctaLabel="Importar / actualizar NPS"
              datasetStatus={npsDatasetStatus}
              description="Importa el Excel NPS dentro del contexto seleccionado. La carga es acumulativa, tolera drift de esquema y protege el histórico persistente."
              disabled={actionsDisabled && !isMutating}
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
              disabled={actionsDisabled && !isMutating}
              eyebrow="Carga Helix"
              feedback={latestHelixUpload}
              onSubmit={handleHelixUpload}
              testId="helix-upload-input"
              title="Dataset Helix"
              uploading={isMutating}
            />
            <section className="panel sidebar-panel">
              <div className="panel-heading"><div><p className="eyebrow">Zona de borrado</p><h2>Datos de la compañía</h2></div></div>
              <p className="panel-copy">Elimina todo el histórico NPS e incidencias del Owner Support Company activo.</p>
              <button className="secondary-button danger-button" disabled={actionsDisabled} onClick={() => void handleDeleteOwnerData()} type="button">
                Borrar completamente {serviceOrigin}
              </button>
            </section>
          </section>
        ) : null}

        {ingestTab === "history" ? (
          <div className="settings-section-stack">
            <UploadsTable
              activeUploadId={activeUploadId}
              filter={historyFilter}
              onFilterChange={setHistoryFilter}
              onSelectUpload={(uploadId) => {
                setActiveUploadId(uploadId);
                setIngestTab("traceability");
              }}
              onDeleteUpload={(upload) => void handleDeleteNpsUpload(upload)}
              uploads={uploads}
            />
            <HelixUploadsTable
              onDeleteUpload={(upload) => void handleDeleteHelixUpload(upload)}
              uploads={helixUploads}
            />
          </div>
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
                onDeleteUpload={(upload) => void handleDeleteNpsUpload(upload)}
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
                      <dd data-testid="selected-upload-name">{toBusinessCopy(selectedUpload.filename)}</dd>
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
                  {selectedUpload.status === "duplicate_upload" ? (
                    <div className="replacement-action">
                      <p>
                        Si el fichero contiene correcciones, puedes sustituir la carga anterior y
                        procesarlo de nuevo con las reglas actuales.
                      </p>
                      <button
                        className="secondary-button"
                        data-testid="replace-duplicate-upload"
                        disabled={actionsDisabled || isMutating}
                        onClick={() => void handleReplaceNpsUpload(selectedUpload)}
                        type="button"
                      >
                        Reemplazar carga anterior
                      </button>
                    </div>
                  ) : null}
                </>
              )}
            </aside>
          </div>
        ) : null}

        {ingestTab === "publication" ? (
          <section className="surface-card stack-panel publication-panel">
            <div className="section-heading section-heading-inline">
              <div>
                <p className="eyebrow">Canal Web</p>
                <h2>Preparar la edición para la WebApp</h2>
                <p className="secondary-copy">
                  Genera una edición estática con los filtros actuales, todas las pantallas de análisis
                  y la presentación ejecutiva que utilizará la newsletter.
                </p>
              </div>
              <button
                className="primary-button"
                data-testid="publication-download-button"
                disabled={actionsDisabled}
                onClick={() => void handleDownloadPublication()}
                type="button"
              >
                {isGeneratingReport ? "Preparando edición…" : "Generar y descargar edición"}
              </button>
            </div>
            <div className="metric-grid metric-grid-3">
              <article className="metric-card">
                <span>Ámbito publicado</span>
                <strong>{dashboard?.context_label || "Filtros actuales"}</strong>
              </article>
              <article className="metric-card">
                <span>Tamaño máximo</span>
                <strong>30 MB</strong>
              </article>
              <article className="metric-card">
                <span>Presentación newsletter</span>
                <strong>Incluida</strong>
              </article>
            </div>
            <article className="note-card">
              <p className="secondary-copy">
                La edición se comprime y ajusta automáticamente al límite de publicación. No incluye
                configuración administrativa ni telemetría.
              </p>
            </article>
          </section>
        ) : null}
      </section>
    );
  }

  function renderDataArea() {
    return (
      <section className="workspace-stack">
        {renderAnalysisFiltersContainer(false)}
        <div className="section-heading section-heading-inline">
          <div>
            <p className="eyebrow">Datos</p>
            <h2>Exploración tabular</h2>
            <p className="secondary-copy">Vista tabular paginada para inspección directa de datasets persistidos.</p>
          </div>
          <label className="inline-field">
            <span>Muestra</span>
            <select
              disabled={actionsDisabled}
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
          disabled={actionsDisabled}
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
              disabled={actionsDisabled || tableOffset === 0}
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
              disabled={actionsDisabled || !datasetTable?.has_more}
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
            <img className="brand-logo" src="/assets/brand/bbva-bei.png" alt="BBVA Banca de Empresas e Instituciones" />
            <h1>NPS Lens</h1>
            <p className="secondary-copy">
              Banca de Empresas e Instituciones · NPS e incidencias relacionadas.
            </p>
          </div>

          {renderServiceContainer()}
          {renderPeriodContainer()}

          <PrimaryNav
            disabled={actionsDisabled}
            items={MAIN_AREAS.filter((item) => isAdmin || item.id !== "ingest")}
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
                data-testid="generate-report-button"
                disabled={actionsDisabled}
                onClick={() => void handleDownloadReport()}
                type="button"
              >
                <Icon name="presentation" />
              </button>
              {isAdmin ? (
                <button
                  aria-label="Abrir configuración global"
                  className="icon-button topbar-icon-button"
                  disabled={actionsDisabled}
                  onClick={() => setSettingsOpen(true)}
                  type="button"
                >
                  <Icon name="settings" />
                </button>
              ) : null}
            </div>
            <div className="topbar-copy">
              <p className="eyebrow">BBVA · Banca de Empresas e Instituciones</p>
              <h2>Orquestación operativa</h2>
              <p data-testid="status-copy">{statusCopy}</p>
            </div>
            <div className="topbar-actions">
              <span
                aria-busy={operationalState !== "operativo"}
                className={`status-chip${operationalState !== "operativo" ? " is-busy" : ""}`}
                data-testid="operational-state"
              >
                {operationalState.toUpperCase()}
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
              <NavigationTabs
                compact
                disabled={actionsDisabled}
                items={INSIGHT_TABS}
                onChange={setInsightTab}
                value={insightTab}
              />
            </div>
          ) : null}

          {mainArea === "insights" ? renderInsightsArea() : null}
          {mainArea === "taxonomy" ? <TaxonomyStudio key={JSON.stringify(taxonomyContext)} context={taxonomyContext} onChange={refreshTaxonomy} disabled={actionsDisabled} /> : null}
          {mainArea === "ingest" ? <TaxonomyIngestNotice context={taxonomyContext} revision={latestNpsUpload?.upload_id || ""} onOpen={() => setMainArea("taxonomy")} /> : null}
          {mainArea === "ingest" ? renderIngestArea() : null}
          {mainArea === "data" ? renderDataArea() : null}

          {(dashboardLoading || datasetLoading) && !dashboard ? (
            <section className="surface-card">
              <p className="empty-state">Preparando la vista operativa...</p>
            </section>
          ) : null}
        </section>
      </main>

      {isAdmin ? (
        <SettingsSheet
          taxonomyContext={taxonomyContext}
          onTaxonomyChange={refreshTaxonomy}
          actionsDisabled={actionsDisabled}
          activeTab={settingsTab}
          downloadsPath={downloadsPath}
          helixBaseUrl={helixBaseUrl}
          hierarchySaving={isSavingHierarchy}
          reportDimensionAnalysis={reportDimensionAnalysis}
          onReprocess={handleReprocess}
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
          setReportDimensionAnalysis={setReportDimensionAnalysis}
          setMinNCross={setMinNCross}
          setMinSimilarity={setMinSimilarity}
          setMaxDaysApart={setMaxDaysApart}
          setThemeMode={setThemeMode}
          themeMode={themeMode}
        />
      ) : null}
    </>
  );
}

export default App;
