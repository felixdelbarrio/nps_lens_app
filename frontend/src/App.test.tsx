import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";

const contextPayload = {
  default_service_origin: "BBVA México",
  default_service_origin_n1: "Senda",
  service_origins: ["BBVA México"],
  service_origin_n1_map: { "BBVA México": ["Senda"] },
  available_years: ["Todos", "2026"],
  available_months_by_year: { Todos: ["Todos", "03"], "2026": ["Todos", "03"] },
  nps_groups: ["Todos", "Detractores", "Neutros", "Promotores"],
  nps_dataset: {
    available: true,
    rows: 26618,
    columns: 17,
    updated_at: "2026-04-17T12:00:00Z",
    status: "completed"
  },
  helix_dataset: {
    available: false,
    rows: 0,
    columns: 0,
    updated_at: null,
    status: "missing",
    source: null
  }
};

const uploadPayload = {
  upload_id: "u-1",
  filename: "NPS Térmico Senda - 03Marzo.xlsx",
  file_hash: "hash",
  uploaded_at: "2026-04-17T12:00:00Z",
  parser_version: "2026.04.17",
  status: "completed",
  service_origin: "BBVA México",
  service_origin_n1: "Senda",
  service_origin_n2: "",
  total_rows: 26618,
  normalized_rows: 26618,
  inserted_rows: 26618,
  updated_rows: 0,
  duplicate_in_file_rows: 0,
  duplicate_historical_rows: 0,
  extra_columns: ["Browser", "Operating System"],
  missing_optional_columns: [],
  issues: [
    {
      level: "WARN",
      code: "extra_columns_detected",
      message: "Se detectaron columnas adicionales no críticas."
    }
  ]
};

const dashboardPayload = {
  context_label: "Marzo 2026",
  context_pills: [
    "Service origin: BBVA México",
    "N1: Senda",
    "N2: -",
    "Año: Todos",
    "Mes: Todos",
    "Grupo: Todos"
  ],
  kpis: {
    samples: 26618,
    nps_average: 4.2,
    detractor_rate: 0.345,
    promoter_rate: 0.22
  },
  overview: {
    daily_kpis_figure: null,
    weekly_trend_figure: null,
    topics_figure: null,
    topics_table: [],
    daily_volume_figure: null,
    daily_mix_figure: null,
    insight_bullets: []
  },
  comparison: { has_data: false, table: [] },
  cohorts: {},
  gaps: { has_data: false, table: [] },
  opportunities: { has_data: false, table: [], bullets: [] },
  controls: {
    dimensions: ["Palanca", "Subpalanca", "Canal", "UsuarioDecisión"],
    cohort_rows: ["Palanca", "Subpalanca"],
    cohort_columns: ["Canal", "Usuario", "NPSGROUP"],
    min_n: 200,
    min_n_cross: 30
  },
  report_markdown: "# Informe de negocio",
  empty_state: ""
};

const tablePayload = {
  dataset_kind: "nps",
  total_rows: 2,
  offset: 0,
  limit: 200,
  columns: ["ID", "Fecha", "NPS", "Palanca"],
  rows: [
    { ID: "1", Fecha: "2026-03-01T10:00:00", NPS: 2, Palanca: "Acceso" },
    { ID: "2", Fecha: "2026-03-02T10:00:00", NPS: 10, Palanca: "Atención" }
  ],
  has_more: false
};

describe("App", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/dashboard/context")) {
          return new Response(JSON.stringify(contextPayload));
        }
        if (url.includes("/api/uploads") && init?.method !== "POST") {
          return new Response(JSON.stringify([uploadPayload]));
        }
        if (url.includes("/api/dashboard/nps")) {
          return new Response(JSON.stringify(dashboardPayload));
        }
        if (url.includes("/api/dashboard/data/")) {
          return new Response(JSON.stringify(tablePayload));
        }
        throw new Error(`Unhandled fetch ${url}`);
      })
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders restored navigation, traceability and upload issues", async () => {
    const user = userEvent.setup();
    render(<App />);

    await waitFor(() =>
      expect(
        screen.getByText(/Contexto, histórico y analítica alineados/i)
      ).toBeInTheDocument()
    );

    expect(screen.getByRole("heading", { name: /Analisis del NPS Térmico/i })).toBeInTheDocument();
    expect(screen.getByText("Histórico de cargas")).toBeInTheDocument();
    expect(screen.getByText("Cambios respecto al histórico")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Ver issues" }));

    expect(screen.getByTestId("selected-upload-name")).toHaveTextContent(
      "NPS Térmico Senda - 03Marzo.xlsx"
    );
    expect(screen.getByTestId("issues-list")).toHaveTextContent("extra_columns_detected");

    await user.click(screen.getByRole("tab", { name: /🧾 Datos/i }));
    expect(screen.getByTestId("data-table")).toHaveTextContent("Acceso");
  });
});
