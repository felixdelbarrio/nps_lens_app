import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";

const configPayload = {
  default_service_origin: "BBVA México",
  default_service_origin_n1: "Senda",
  service_origins: ["BBVA México"],
  service_origin_n1_map: { "BBVA México": ["Senda"] }
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

const summaryPayload = {
  total_records: 26618,
  date_range: {
    min: "2026-03-01T00:00:26",
    max: "2026-03-31T23:59:59"
  },
  overall_nps: -12.5,
  promoter_rate: 0.22,
  detractor_rate: 0.345,
  uploads: 1,
  duplicates_prevented: 0,
  top_drivers: {
    Palanca: [{ value: "Sin Comentarios", n: 1000, nps: -20.5 }],
    Subpalanca: [{ value: "Sin Comentarios", n: 1000, nps: -20.5 }],
    Canal: [{ value: "Otros", n: 1000, nps: -15.2 }]
  },
  latest_uploads: [uploadPayload]
};

describe("App", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.includes("/api/config")) {
          return new Response(JSON.stringify(configPayload));
        }
        if (url.includes("/api/uploads") && init?.method !== "POST") {
          return new Response(JSON.stringify([uploadPayload]));
        }
        if (url.includes("/api/reprocess")) {
          return new Response(JSON.stringify(summaryPayload));
        }
        if (url.includes("/api/summary")) {
          return new Response(JSON.stringify(summaryPayload));
        }
        throw new Error(`Unhandled fetch ${url}`);
      })
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders accumulated summary and shows selected upload issues", async () => {
    const user = userEvent.setup();
    render(<App />);

    await waitFor(() =>
      expect(screen.getByText("Dashboard alineado con el histórico persistente.")).toBeInTheDocument()
    );

    expect(screen.getByText("Histórico de cargas")).toBeInTheDocument();
    expect(screen.getByText("NPS Térmico Senda - 03Marzo.xlsx")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Ver issues" }));

    expect(screen.getByTestId("selected-upload-name")).toHaveTextContent(
      "NPS Térmico Senda - 03Marzo.xlsx"
    );
    expect(screen.getByTestId("issues-list")).toHaveTextContent("extra_columns_detected");
    expect(screen.getByText("26.618")).toBeInTheDocument();
  });
});
