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

export type Summary = {
  total_records: number;
  date_range: {
    min: string | null;
    max: string | null;
  };
  overall_nps: number | null;
  promoter_rate: number | null;
  detractor_rate: number | null;
  uploads: number;
  duplicates_prevented: number;
  top_drivers: Record<string, Array<Record<string, unknown>>>;
  latest_uploads: UploadResult[];
};

export type Config = {
  default_service_origin: string;
  default_service_origin_n1: string;
  service_origins: string[];
  service_origin_n1_map: Record<string, string[]>;
};

async function parseResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Request failed with ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function fetchConfig(): Promise<Config> {
  return parseResponse<Config>(await fetch("/api/config"));
}

export async function fetchUploads(): Promise<UploadResult[]> {
  return parseResponse<UploadResult[]>(await fetch("/api/uploads"));
}

export async function fetchSummary(params?: {
  service_origin?: string;
  service_origin_n1?: string;
  service_origin_n2?: string;
}): Promise<Summary> {
  const url = new URL("/api/summary", window.location.origin);
  if (params?.service_origin) {
    url.searchParams.set("service_origin", params.service_origin);
  }
  if (params?.service_origin_n1) {
    url.searchParams.set("service_origin_n1", params.service_origin_n1);
  }
  if (params?.service_origin_n2) {
    url.searchParams.set("service_origin_n2", params.service_origin_n2);
  }
  return parseResponse<Summary>(await fetch(url));
}

export async function reprocessSummary(params?: {
  service_origin?: string;
  service_origin_n1?: string;
  service_origin_n2?: string;
}): Promise<Summary> {
  const url = new URL("/api/reprocess", window.location.origin);
  if (params?.service_origin) {
    url.searchParams.set("service_origin", params.service_origin);
  }
  if (params?.service_origin_n1) {
    url.searchParams.set("service_origin_n1", params.service_origin_n1);
  }
  if (params?.service_origin_n2) {
    url.searchParams.set("service_origin_n2", params.service_origin_n2);
  }
  return parseResponse<Summary>(await fetch(url, { method: "POST" }));
}

export async function uploadFile(payload: {
  file: File;
  serviceOrigin: string;
  serviceOriginN1: string;
  serviceOriginN2: string;
}): Promise<UploadResult> {
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
