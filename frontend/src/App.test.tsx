import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";

const contextPayload = {
  default_service_origin: "BBVA México",
  default_service_origin_n2: "",
  default_service_origin_n1: "ENTERPRISE WEB",
  service_origins: ["BBVA México"],
  service_origin_n1_map: { "BBVA México": ["ENTERPRISE WEB"] },
  service_origin_n2_values: [],
  service_origin_n2_map: { "BBVA México": { "ENTERPRISE WEB": [] } },
  service_origin_n2_options: [],
  available_years: ["Todos", "2026"],
  available_months_by_year: { Todos: ["Todos", "03"], "2026": ["Todos", "03"] },
  nps_groups: ["Todos", "Detractores", "Neutros", "Promotores"],
  causal_method_options: [
    {
      value: "domain_touchpoint",
      label: "Por Subpalanca",
      summary: "La lectura causal fija el touchpoint desde Subpalanca.",
      flow: "Incidencias -> Touchpoint afectado -> Subpalanca -> Comentario -> NPS"
    },
    {
      value: "executive_journeys",
      label: "Journeys de detracción",
      summary: "La lectura causal se reorganiza en journeys de comité.",
      flow: "Incidencias + comentario + tópico NPS -> Journey ejecutivo -> NPS"
    }
  ],
  preferences: {
    service_origin: "BBVA México",
    service_origin_n1: "ENTERPRISE WEB",
    service_origin_n2: "",
    pop_year: "Todos",
    pop_month: "Todos",
    nps_group_choice: "Todos",
    theme_mode: "light",
    downloads_path: "/Users/test/Downloads",
    touchpoint_source: "domain_touchpoint",
    min_similarity: 0.25,
    max_days_apart: 10,
    min_n_opportunities: 200,
    min_n_cross_comparisons: 30
  },
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
  service_origin_n1: "ENTERPRISE WEB",
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
    "N1: ENTERPRISE WEB",
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

const linkingPayloadAvailable = {
  available: true,
  context_pills: [
    "Service origin: BBVA México",
    "N1: ENTERPRISE WEB",
    "N2: -",
    "Año: Todos",
    "Mes: Todos",
    "Grupo: Todos"
  ],
  focus_group: "detractor",
  focus_label: "% detractores",
  empty_state: "",
  touchpoint_mode: {
    value: "executive_journeys",
    label: "Journeys de detracción",
    summary: "La lectura causal se reorganiza en journeys de comité.",
    flow: "Incidencias + comentario + tópico NPS -> Journey ejecutivo -> NPS"
  },
  kpis: {
    responses: 26618,
    incidents: 233,
    average_focus_rate: 0.1524
  },
  situation: {
    average_focus_rate: 0.1524,
    ranking_table: [
      {
        "Tópico NPS": "Pagos/ Transferencias > Faltan detalles de movimientos",
        "Confidence (learned)": 0.133
      }
    ],
    evidence_wall: [
      {
        similarity: 0.214,
        "Comentario detractor": "Estamos a primero de mes y el edo de cuenta...",
        "Incidencia (descripción)": "ACOTAMIENTO IRD INC000104258819",
        incident_id: "INC000104258819"
      }
    ]
  },
  journeys: {
    journeys_detected: 6,
    linked_pairs: 34,
    semantic_cohesion_mean: 0.91,
    table: [
      {
        "Journey roto": "Uso / Edo de Cuenta",
        "Links validados": 18
      }
    ]
  },
  scenarios: {
    banner: {
      kicker: "Narrativa causal",
      title: "2 cadenas defendibles para detractores",
      summary: "La política Helix↔VoC está fijada en similitud ≥ 0.20.",
      metrics: [
        { label: "Método causal", value: "Journeys de detracción" },
        { label: "Incidencias con match", value: "20" },
        { label: "Comentarios enlazados", value: "22" },
        { label: "Links validados", value: "30" }
      ]
    },
    pills: ["Solo cadena completa defendible", "2 tópicos linkados", "2 cadenas causales"],
    cards: [
      {
        chain_key: "chain-1",
        rank: 1,
        title: "Operativa crítica fallida",
        statement: "Operativa -> transferencias / pagos / firma -> error funcional o timeout.",
        selection_label: "Transferencias / pagos / firma | Operativa crítica fallida | 12 INC | 12 VoC",
        linked_incidents: 12,
        linked_comments: 12,
        linked_pairs: 16,
        detractor_probability: 0.6,
        nps_delta_expected: -0.0,
        total_nps_impact: 0.0,
        confidence: 0.16,
        priority: 0.62,
        nps_points_at_risk: 0.0,
        nps_points_recoverable: 0.0,
        owner_role: "VoC + Analitica",
        flow_steps: [
          "(12) Incidencias Helix",
          "Transferencias / pagos / firma",
          "Operativa / Error funcional / timeout",
          "(12) Comentarios VoC",
          "Riesgo NPS"
        ],
        incident_records: [
          {
            incident_id: "INC000104355468",
            summary: "ACOTAMIENTO IRD El usuario Mario Alberto Santillan Medina..."
          }
        ],
        comment_records: [
          {
            comment_id: "1",
            date: "2026-03-26",
            nps: "0",
            group: "DETRACTOR",
            palanca: "Transferencias",
            subpalanca: "Pagos / firma",
            comment: "AL DESCARGAR EL ESTADO DE CUENTA ME DIRECCIONA..."
          }
        ],
        detail_table: [
          {
            "Tópico NPS": "Operativa crítica fallida",
            Prioridad: 0.62
          }
        ]
      }
    ]
  },
  overview_figure: null,
  priority_figure: null,
  risk_recovery_figure: null,
  heatmap_figure: null,
  lag_figure: null,
  ranking_table: [],
  evidence_table: [
    {
      nps_topic: "Pagos/ Transferencias > Faltan detalles de movimientos",
      similarity: 0.339,
      incident_id: "INC000104231684",
      incident_summary: "ACOTAMIENTO IRD...",
      detractor_comment: "mal no funciona los pagos al sua ni impuestos cdmx"
    }
  ],
  journey_routes_table: [],
  top_topic: "Pagos/ Transferencias > Faltan detalles de movimientos"
};

describe("App", () => {
  const createObjectUrl = vi.fn(() => "blob:report");
  const revokeObjectUrl = vi.fn();
  const anchorClick = vi.fn();
  let currentLinkingPayload: Record<string, unknown>;

  beforeEach(() => {
    currentLinkingPayload = {
      available: false,
      context_pills: [],
      focus_group: "Todos",
      focus_label: "Sin foco",
      empty_state: "Sin base cruzada",
      kpis: {},
      ranking_table: [],
      evidence_table: [],
      journey_routes_table: [],
      top_topic: ""
    };
    vi.stubGlobal(
      "URL",
      Object.assign(URL, {
        createObjectURL: createObjectUrl,
        revokeObjectURL: revokeObjectUrl
      })
    );
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(anchorClick);
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
        if (url.includes("/api/dashboard/linking")) {
          return new Response(JSON.stringify(currentLinkingPayload));
        }
        if (url.includes("/api/dashboard/data/")) {
          return new Response(JSON.stringify(tablePayload));
        }
        if (url.includes("/api/preferences") && init?.method === "PUT") {
          return new Response(init.body as BodyInit, {
            headers: { "Content-Type": "application/json" }
          });
        }
        if (url.includes("/api/dashboard/report/pptx")) {
          return new Response("pptx-content", {
            headers: {
              "Content-Disposition": 'attachment; filename="reporte-ejecutivo.pptx"'
            }
          });
        }
        throw new Error(`Unhandled fetch ${url}`);
      })
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders restored navigation, filters, traceability and uploads", async () => {
    const user = userEvent.setup();
    render(<App />);

    await waitFor(() =>
      expect(
        screen.getByText(/Producto sincronizado con histórico persistente/i)
      ).toBeInTheDocument()
    );

    expect(screen.getByRole("heading", { name: /Analisis del NPS Térmico/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Ingesta/i })).toBeInTheDocument();
    expect(screen.getByText("Cambios respecto al histórico")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Service Origin/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Recorte analítico/i })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Ingesta/i }));
    expect(screen.queryByRole("heading", { name: /Recorte analítico/i })).not.toBeInTheDocument();
    await user.click(screen.getByRole("tab", { name: "Histórico" }));
    expect(screen.getByText("Histórico de cargas")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Ver issues" }));

    expect(screen.getByTestId("selected-upload-name")).toHaveTextContent(
      "NPS Térmico Senda - 03Marzo.xlsx"
    );
    expect(screen.getByTestId("selected-issues-list")).toHaveTextContent(
      "extra_columns_detected"
    );

    await user.click(screen.getByRole("button", { name: /Datos/i }));
    expect(screen.getByTestId("data-table")).toHaveTextContent("Acceso");
  });

  it("downloads the restored executive report from the top bar", async () => {
    const user = userEvent.setup();
    const fetchMock = globalThis.fetch as ReturnType<typeof vi.fn>;

    render(<App />);

    await waitFor(() =>
      expect(
        screen.getByText(/Producto sincronizado con histórico persistente/i)
      ).toBeInTheDocument()
    );

    await user.click(screen.getByRole("button", { name: /^Reporte$/i }));

    await waitFor(() =>
      expect(
        fetchMock.mock.calls.some(([input]) =>
          String(input).includes("/api/dashboard/report/pptx")
        )
      ).toBe(true)
    );
    expect(createObjectUrl).toHaveBeenCalled();
    expect(anchorClick).toHaveBeenCalled();
  });

  it("renders the restored linking workspace with situation, journeys and causal chains", async () => {
    const user = userEvent.setup();
    currentLinkingPayload = linkingPayloadAvailable;

    render(<App />);

    await waitFor(() =>
      expect(
        screen.getByText(/Producto sincronizado con histórico persistente/i)
      ).toBeInTheDocument()
    );

    await user.click(screen.getByRole("tab", { name: "Incidencias ↔ NPS" }));
    expect(screen.getByRole("heading", { name: "Timeline causal (diario)" })).toBeInTheDocument();
    expect(screen.getByText("Ranking de hipótesis")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Journeys rotos" }));
    expect(screen.getByText("Journeys rotos identificados")).toBeInTheDocument();
    expect(screen.getByText("Uso / Edo de Cuenta")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Análisis de escenarios causales" }));
    expect(screen.getByText("2 cadenas defendibles para detractores")).toBeInTheDocument();
    expect(screen.getByText("Operativa crítica fallida")).toBeInTheDocument();
    expect(screen.getByText(/VoC \+ Analitica/i)).toBeInTheDocument();
  });
});
