export type Issue = {
  level: string;
  message: string;
  column?: string | null;
  code?: string;
  details?: Record<string, unknown>;
};

export type UploadResult = {
  upload_id: string;
  filename: string;
  file_hash: string;
  uploaded_at: string;
  parser_version: string;
  status: string;
  service_origin: string;
  service_origin_n1: string;
  service_origin_n2: string;
  total_rows: number;
  normalized_rows: number;
  inserted_rows: number;
  updated_rows: number;
  duplicate_in_file_rows: number;
  duplicate_historical_rows: number;
  extra_columns: string[];
  missing_optional_columns: string[];
  issues: Issue[];
};

export type DatasetStatus = {
  available: boolean;
  rows: number;
  columns: number;
  updated_at: string | null;
  status: string;
  source?: string | null;
};

export type DashboardConfig = {
  default_service_origin: string;
  default_service_origin_n1: string;
  default_service_origin_n2: string;
  service_origins: string[];
  service_origin_n1_map: Record<string, string[]>;
  service_origin_n2_values: string[];
  service_origin_n2_map: Record<string, Record<string, string[]>>;
  service_origin_n2_options: string[];
  available_years: string[];
  available_months_by_year: Record<string, string[]>;
  causal_default_year: string;
  causal_default_month: string;
  nps_groups: string[];
  score_channels: string[];
  causal_method_options: CausalMethodOption[];
  preferences: PreferencesPayload;
  nps_dataset: DatasetStatus;
  helix_dataset: DatasetStatus;
  access?: { email: string; role: "admin" | "viewer"; is_admin: boolean; allowed_domain: string };
};

export type CausalMethodOption = {
  value: string;
  label: string;
  summary: string;
  flow: string;
};

export type PlotlyFigureSpec = {
  data?: unknown[];
  layout?: Record<string, unknown>;
  frames?: unknown[];
  config?: Record<string, unknown>;
};

export type KpiDelta = {
  value: number | null;
  direction: "up" | "down" | "flat";
  favorable: boolean | null;
  display?: string;
};

export type DashboardKpis = {
  samples: number;
  nps_average: number | null;
  classic_nps?: number | null;
  detractor_rate: number | null;
  neutral_rate: number | null;
  promoter_rate: number | null;
  comments?: number;
};

export type ScopeKpiBlock = {
  label: string;
  period_type?: string;
  base_label?: string;
  actual_label?: string;
  note?: string;
  kpis: DashboardKpis;
  base_kpis?: DashboardKpis;
  deltas?: Record<string, KpiDelta>;
  show_deltas?: boolean;
  display?: Record<string, string>;
  base_display?: Record<string, string>;
  temporal?: ScopeKpiBlock;
};

export type PeriodAggregate = {
  label: string;
  start_date?: string | null;
  end_date?: string | null;
  samples: number;
  comments: number;
  nps_average: number | null;
  classic_nps: number | null;
  detractor_rate: number | null;
  promoter_rate: number | null;
  display?: Record<string, string>;
};

export type DashboardPayload = {
  context_label: string;
  context_pills: string[];
  kpis: DashboardKpis;
  overview: {
    daily_kpis_figure?: PlotlyFigureSpec | null;
    period_aggregates_figure?: PlotlyFigureSpec | null;
    topics_figure?: PlotlyFigureSpec | null;
    topics_table?: Array<Record<string, unknown>>;
    daily_volume_figure?: PlotlyFigureSpec | null;
    daily_volume_mix_figure?: PlotlyFigureSpec | null;
    daily_mix_figure?: PlotlyFigureSpec | null;
    daily_explanation_bullets?: string[];
    insight_bullets?: string[];
  };
  scope?: {
    historical?: ScopeKpiBlock;
    cumulative?: ScopeKpiBlock;
    period?: ScopeKpiBlock;
    temporal?: ScopeKpiBlock;
    period_aggregates?: PeriodAggregate[];
  };
  comparison: {
    summary?: {
      label_current: string;
      label_baseline: string;
      delta_nps: number;
      delta_detr_pp: number;
      n_current: number;
      n_baseline: number;
    };
    dimension?: string;
    figure?: PlotlyFigureSpec | null;
    table?: Array<Record<string, unknown>>;
    has_data?: boolean;
  };
  cohorts: {
    row_dimension?: string;
    column_dimension?: string;
    figure?: PlotlyFigureSpec | null;
  };
  gaps: {
    dimension?: string;
    overall_nps?: number;
    title?: string;
    subtitle?: string;
    figure?: PlotlyFigureSpec | null;
    table?: Array<Record<string, unknown>>;
    has_data?: boolean;
  };
  opportunities: {
    dimension?: string;
    figure?: PlotlyFigureSpec | null;
    table?: Array<Record<string, unknown>>;
    bullets?: string[];
    has_data?: boolean;
  };
  controls: {
    dimensions: string[];
    cohort_rows: string[];
    cohort_columns: string[];
    min_n: number;
    min_n_cross: number;
  };
  empty_state: string;
};

export type LinkingPayload = {
  available: boolean;
  context_pills: string[];
  focus_group: string;
  focus_label: string;
  empty_state: string;
  causal_method?: Record<string, unknown>;
  navigation?: Array<Record<string, unknown>>;
  kpis: Record<string, number | null>;
  situation?: Record<string, unknown>;
  entity_summary?: Record<string, unknown>;
  scenarios?: Record<string, unknown>;
  deep_dive?: Record<string, unknown>;
};

export type DatasetTable = {
  dataset_kind: string;
  total_rows: number;
  offset: number;
  limit: number;
  columns: string[];
  rows: Array<Record<string, unknown>>;
  has_more: boolean;
};

export type HelixUploadResult = {
  upload_id: string;
  filename: string;
  uploaded_at: string;
  status: string;
  row_count: number;
  column_count: number;
  sheet_name: string;
  issues: Issue[];
  dataset: DatasetStatus;
};

export type UploadSelectionPayload = {
  file?: File;
  desktopFilePath?: string;
  desktopFileName?: string;
};

export type DesktopFileSelection = {
  path: string;
  name: string;
};

type DesktopBridgeApi = {
  pick_excel_file?: () => Promise<DesktopFileSelection | null>;
  upload_nps_file?: (
    filePath: string,
    serviceOrigin: string,
    serviceOriginN1: string,
    serviceOriginN2: string
  ) => Promise<UploadResult>;
  upload_helix_file?: (
    filePath: string,
    serviceOrigin: string,
    serviceOriginN1: string,
    serviceOriginN2: string
  ) => Promise<HelixUploadResult>;
  reveal_file?: (filePath: string) => Promise<boolean>;
};

declare global {
  interface Window {
    pywebview?: {
      api?: DesktopBridgeApi;
    };
  }
}

export type DashboardQuery = {
  service_origin: string;
  service_origin_n1: string;
  service_origin_n2: string;
  pop_year: string;
  pop_month: string;
  nps_group: string;
  score_channel: string;
  comparison_dimension: string;
  gap_dimension: string;
  opportunity_dimension: string;
  cohort_row: string;
  cohort_col: string;
  min_n: number;
  min_n_cross: number;
  theme_mode: string;
};

export type PreferencesPayload = {
  service_origin: string;
  service_origin_n1: string;
  service_origin_n2: string;
  pop_year: string;
  pop_month: string;
  nps_group_choice: string;
  score_channel: string;
  theme_mode: "light" | "dark";
  downloads_path: string;
  helix_base_url: string;
  report_dimension_analysis: "palanca" | "subpalanca";
  touchpoint_source: string;
  min_similarity: number;
  max_days_apart: number;
  min_n_opportunities: number;
  min_n_cross_comparisons: number;
};

export type ServiceOriginHierarchyPayload = {
  service_origins: string[];
  service_origin_n1_map: Record<string, string[]>;
  service_origin_n2_map: Record<string, Record<string, string[]>>;
};

export type ReprocessSummary = {
  total_records: number;
  uploads: number;
  duplicates_prevented: number;
};

export type EquivalenceRegistryPayload = {
  schema_version: string;
  dimensions: Record<string, Array<{ canonical: string; aliases: string[] }>>;
  updated_records?: number;
};

export type TelemetryPayload = {
  schema_version: string;
  generated_at: string;
  summary: Record<string, unknown>;
  routes: Array<Record<string, string | number>>;
  events: Array<Record<string, string | number>>;
};

export type ExportQuery = {
  service_origin: string;
  service_origin_n1: string;
  service_origin_n2: string;
  pop_year: string;
  pop_month: string;
  nps_group: string;
  score_channel: string;
  min_n: number;
  min_similarity: number;
  max_days_apart: number;
  touchpoint_source: string;
  report_dimension_analysis: string;
};

function buildUrl(pathname: string, params?: Record<string, string | number | undefined>) {
  const url = new URL(pathname, window.location.origin);
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value === undefined || value === "") {
      return;
    }
    url.searchParams.set(key, String(value));
  });
  return url;
}

async function parseResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const text = await response.text();
    let detail = "";
    try {
      const payload = JSON.parse(text) as { detail?: string };
      detail = payload.detail || "";
    } catch {
      detail = "";
    }
    throw new Error(detail || text || `Request failed with ${response.status}`);
  }
  return (await response.json()) as T;
}

function parseContentDispositionFilename(
  headerValue: string | null,
  fallback = "reporte-ejecutivo.pptx"
): string {
  if (!headerValue) {
    return fallback;
  }
  const encodedMatch = headerValue.match(/filename\*\=UTF-8''([^;]+)/i);
  if (encodedMatch?.[1]) {
    return decodeURIComponent(encodedMatch[1]);
  }
  const plainMatch = headerValue.match(/filename=\"?([^\";]+)\"?/i);
  if (plainMatch?.[1]) {
    return plainMatch[1];
  }
  return fallback;
}

function getDesktopBridge(): DesktopBridgeApi | undefined {
  return window.pywebview?.api;
}

export function canUseDesktopFileBridge(): boolean {
  const bridge = getDesktopBridge();
  return Boolean(bridge?.pick_excel_file);
}

export async function pickDesktopExcelFile(): Promise<DesktopFileSelection | null> {
  const bridge = getDesktopBridge();
  if (!bridge?.pick_excel_file) {
    return null;
  }
  return bridge.pick_excel_file();
}

export async function fetchConfig(params: {
  service_origin?: string;
  service_origin_n1?: string;
  service_origin_n2?: string;
}): Promise<DashboardConfig> {
  return parseResponse<DashboardConfig>(await fetch(buildUrl("/api/dashboard/context", params)));
}

export async function fetchUploads(params: {
  service_origin?: string;
  service_origin_n1?: string;
  service_origin_n2?: string;
}): Promise<UploadResult[]> {
  return parseResponse<UploadResult[]>(await fetch(buildUrl("/api/uploads", params)));
}

export async function fetchDashboard(params: DashboardQuery): Promise<DashboardPayload> {
  return parseResponse<DashboardPayload>(await fetch(buildUrl("/api/dashboard/nps", params)));
}

export async function fetchLinkingDashboard(params: {
  service_origin: string;
  service_origin_n1: string;
  service_origin_n2: string;
  pop_year: string;
  pop_month: string;
  nps_group: string;
  score_channel: string;
  min_similarity: number;
  max_days_apart: number;
  touchpoint_source: string;
  theme_mode: string;
}): Promise<LinkingPayload> {
  return parseResponse<LinkingPayload>(await fetch(buildUrl("/api/dashboard/linking", params)));
}

export async function fetchDatasetTable(
  datasetKind: "nps" | "helix",
  params: {
    service_origin: string;
    service_origin_n1: string;
    service_origin_n2: string;
    pop_year: string;
    pop_month: string;
    nps_group: string;
    score_channel: string;
    offset: number;
    limit: number;
  }
): Promise<DatasetTable> {
  return parseResponse<DatasetTable>(
    await fetch(buildUrl(`/api/dashboard/data/${datasetKind}`, params))
  );
}

export async function uploadNpsFile(payload: {
  file?: File;
  desktopFilePath?: string;
  serviceOrigin: string;
  serviceOriginN1: string;
  serviceOriginN2: string;
}): Promise<UploadResult> {
  const bridge = getDesktopBridge();
  if (payload.desktopFilePath && bridge?.upload_nps_file) {
    return bridge.upload_nps_file(
      payload.desktopFilePath,
      payload.serviceOrigin,
      payload.serviceOriginN1,
      payload.serviceOriginN2
    );
  }
  if (!payload.file) {
    throw new Error("Selecciona un fichero antes de importar.");
  }
  const formData = new FormData();
  formData.set("file", payload.file);
  formData.set("service_origin", payload.serviceOrigin);
  formData.set("service_origin_n1", payload.serviceOriginN1);
  formData.set("service_origin_n2", payload.serviceOriginN2);
  return parseResponse<UploadResult>(
    await fetch("/api/uploads/nps", {
      method: "POST",
      body: formData
    })
  );
}

export async function uploadHelixFile(payload: {
  file?: File;
  desktopFilePath?: string;
  serviceOrigin: string;
  serviceOriginN1: string;
  serviceOriginN2: string;
}): Promise<HelixUploadResult> {
  const bridge = getDesktopBridge();
  if (payload.desktopFilePath && bridge?.upload_helix_file) {
    return bridge.upload_helix_file(
      payload.desktopFilePath,
      payload.serviceOrigin,
      payload.serviceOriginN1,
      payload.serviceOriginN2
    );
  }
  if (!payload.file) {
    throw new Error("Selecciona un fichero antes de importar.");
  }
  const formData = new FormData();
  formData.set("file", payload.file);
  formData.set("service_origin", payload.serviceOrigin);
  formData.set("service_origin_n1", payload.serviceOriginN1);
  formData.set("service_origin_n2", payload.serviceOriginN2);
  return parseResponse<HelixUploadResult>(
    await fetch("/api/uploads/helix", {
      method: "POST",
      body: formData
    })
  );
}

export async function reprocessSummary(params: {
  service_origin: string;
  service_origin_n1: string;
  service_origin_n2: string;
}): Promise<ReprocessSummary> {
  return parseResponse<ReprocessSummary>(
    await fetch(buildUrl("/api/reprocess", params), {
      method: "POST"
    })
  );
}

export async function persistPreferences(payload: PreferencesPayload): Promise<PreferencesPayload> {
  return parseResponse<PreferencesPayload>(
    await fetch("/api/preferences", {
      method: "PUT",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    })
  );
}

export async function updateServiceOrigins(
  payload: ServiceOriginHierarchyPayload
): Promise<DashboardConfig> {
  return parseResponse<DashboardConfig>(
    await fetch("/api/settings/service-origins", {
      method: "PUT",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    })
  );
}

export async function fetchEquivalences(): Promise<EquivalenceRegistryPayload> {
  return parseResponse<EquivalenceRegistryPayload>(await fetch("/api/settings/equivalences"));
}

export async function updateEquivalences(
  payload: EquivalenceRegistryPayload
): Promise<EquivalenceRegistryPayload> {
  return parseResponse<EquivalenceRegistryPayload>(
    await fetch("/api/settings/equivalences", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    })
  );
}

export async function fetchTelemetry(): Promise<TelemetryPayload> {
  return parseResponse<TelemetryPayload>(await fetch("/api/telemetry"));
}

async function downloadArtifact(
  pathname: string,
  params: Record<string, string | number | undefined>,
  fallbackName: string
): Promise<{ blob: Blob | null; fileName: string; savedPath: string }> {
  const response = await fetch(buildUrl(pathname, params));
  if (!response.ok) {
    await parseResponse(response);
  }
  const savedPath = response.headers.get("x-nps-lens-saved-path") || "";
  if (savedPath && getDesktopBridge()?.reveal_file) {
    await response.body?.cancel();
    return {
      blob: null,
      fileName: parseContentDispositionFilename(
        response.headers.get("content-disposition"),
        fallbackName
      ),
      savedPath
    };
  }
  return {
    blob: await response.blob(),
    fileName:
      parseContentDispositionFilename(response.headers.get("content-disposition"), fallbackName),
    savedPath: ""
  };
}

export async function completeArtifactDownload(artifact: {
  blob: Blob | null;
  fileName: string;
  savedPath: string;
}): Promise<string> {
  if (artifact.savedPath) {
    await getDesktopBridge()?.reveal_file?.(artifact.savedPath);
    return artifact.savedPath;
  }
  if (!artifact.blob) {
    throw new Error("La descarga no devolvió ningún fichero.");
  }
  const objectUrl = URL.createObjectURL(artifact.blob);
  const anchor = document.createElement("a");
  anchor.href = objectUrl;
  anchor.download = artifact.fileName;
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 0);
  return "";
}

export function downloadWebPublication(params: ExportQuery) {
  return downloadArtifact("/api/dashboard/publication.zip", params, "nps-lens-publicacion.zip");
}

export function downloadTelemetry() {
  return downloadArtifact("/api/telemetry/export", {}, "nps-lens-telemetria.json");
}

export function downloadExecutiveReport(params: {
  service_origin: string;
  service_origin_n1: string;
  service_origin_n2: string;
  pop_year: string;
  pop_month: string;
  nps_group: string;
  score_channel: string;
  min_n: number;
  min_similarity: number;
  max_days_apart: number;
  touchpoint_source: string;
  report_dimension_analysis: string;
}) {
  return downloadArtifact("/api/dashboard/report/pptx", params, "reporte-ejecutivo.pptx");
}
