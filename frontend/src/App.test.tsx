import { render, screen, waitFor } from "@testing-library/react";
import { within } from "@testing-library/react";
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
  causal_method: {
    value: "executive_journeys",
    label: "Journeys de detracción",
    summary: "La lectura causal se reorganiza en journeys de comité.",
    flow: "Incidencias + comentario + tópico NPS -> Journey ejecutivo -> NPS"
  },
  navigation: [
    { id: "situation", label: "Situación del periodo" },
    { id: "entity-summary", label: "Journeys de detracción" },
    { id: "scenarios", label: "Análisis de escenarios causales" },
    { id: "nps-deep-dive", label: "NPS deep dive" }
  ],
  kpis: {
    responses: 26618,
    incidents: 233,
    linked_pairs: 34,
    average_focus_rate: 0.1524
  },
  situation: {
    title: "Situación del periodo",
    subtitle: "Cruce diario entre incidencias y NPS con lectura causal organizada por journeys de detracción.",
    kpis: [
      { label: "Respuestas analizadas", value: "26618" },
      { label: "Incidencias del periodo", value: "233" },
      {
        label: "Método causal",
        value: "Journeys de detracción",
        hint: "Incidencias + comentario + tópico NPS -> Journey ejecutivo -> NPS"
      },
      { label: "% detractores medio", value: "15.2%" }
    ],
    metadata: [
      {
        label: "Flujo causal",
        value: "Incidencias + comentario + tópico NPS -> Journey ejecutivo -> NPS"
      },
      { label: "Foco analítico", value: "Journeys de detracción" }
    ],
    figure_title: "Timeline causal (diario)",
    figure: null,
    note: "El método causal activo transforma la evidencia en journeys ejecutivos con foco de comité."
  },
  entity_summary: {
    title: "Journeys de detracción",
    subtitle: "Cada fila resume un journey ejecutivo del catálogo y la evidencia que sostiene su impacto en NPS.",
    kpis: [
      { label: "Journeys de detracción", value: "6" },
      { label: "Touchpoints cubiertos", value: "4" },
      { label: "Links validados", value: "34" }
    ],
    figure_title: "Journeys de detracción con mayor evidencia validada",
    figure: null,
    table_title: "Detalle de journeys de detracción",
    empty_state: "No hay journeys de detracción defendibles con evidencia suficiente en esta ventana.",
    table: [
      {
        "Journey de detracción": "Uso / Edo de Cuenta",
        "Touchpoint del catálogo": "Consulta",
        "Links validados": 18
      }
    ]
  },
  deep_dive: {
    title: "NPS deep dive",
    subtitle: "Profundización sobre los tópicos NPS explicados por los journeys de detracción activos.",
    kpis: [
      { label: "NPS en riesgo", value: "3.90 pts" },
      { label: "NPS recuperable", value: "2.40 pts" },
      { label: "Concentración top-3", value: "74.0%" },
      { label: "Tiempo de reacción", value: "1.2 semanas" }
    ],
    topic_filter: {
      label: "Tópico",
      options: [
        "Todos",
        "Pagos/ Transferencias > Faltan detalles de movimientos",
        "Operativa crítica fallida"
      ],
      default: "Pagos/ Transferencias > Faltan detalles de movimientos"
    },
    tabs: [
      { id: "ranking", label: "Ranking de hipótesis" },
      { id: "evidence", label: "Evidence wall" },
      { id: "analysis", label: "Data deepdive analysis" }
    ],
    trending: {
      title: "NPS tópicos trending",
      figure: null,
      empty_state: "No hay señal suficiente para construir tópicos trending."
    },
    ranking: {
      title: "Ranking de hipótesis",
      rows: [
        {
          "Tópico NPS": "Pagos/ Transferencias > Faltan detalles de movimientos",
          "Confidence (learned)": 0.133
        },
        {
          "Tópico NPS": "Operativa crítica fallida",
          "Confidence (learned)": 0.111
        }
      ],
      empty_state: ""
    },
    evidence: {
      title: "Evidence wall",
      rows: [
        {
          nps_topic: "Pagos/ Transferencias > Faltan detalles de movimientos",
          similarity: 0.339,
          incident_id: "INC000104231684",
          incident_summary: "ACOTAMIENTO IRD...",
          detractor_comment: "mal no funciona los pagos al sua ni impuestos cdmx"
        },
        {
          nps_topic: "Operativa crítica fallida",
          similarity: 0.121,
          incident_id: "INC000104355468",
          incident_summary: "ACOTAMIENTO IRD El usuario Mario Alberto Santillan Medina...",
          detractor_comment: "AL DESCARGAR EL ESTADO DE CUENTA ME DIRECCIONA..."
        }
      ],
      empty_state: ""
    },
    analysis: {
      title: "Data deepdive analysis",
      rows: [
        {
          nps_topic: "Pagos/ Transferencias > Faltan detalles de movimientos",
          similarity: 0.339,
          incident_id: "INC000104231684",
          incident_summary: "ACOTAMIENTO IRD...",
          detractor_comment: "mal no funciona los pagos al sua ni impuestos cdmx"
        },
        {
          nps_topic: "Operativa crítica fallida",
          similarity: 0.121,
          incident_id: "INC000104355468",
          incident_summary: "ACOTAMIENTO IRD El usuario Mario Alberto Santillan Medina...",
          detractor_comment: "AL DESCARGAR EL ESTADO DE CUENTA ME DIRECCIONA..."
        }
      ],
      empty_state: ""
    }
  },
  scenarios: {
    title: "Análisis de escenarios causales",
    subtitle: "Escenarios priorizados bajo la lectura causal journeys de detracción.",
    banner: {
      kicker: "Narrativa causal",
      title: "2 journeys de detracción defendibles para detractores",
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
          "(12) Incidencias + comentarios",
          "Uso / Edo de Cuenta",
          "Consulta / Operativa / Error funcional",
          "Riesgo NPS"
        ],
        spotlight_metrics: [
          { label: "Journey de detracción", value: "Uso / Edo de Cuenta" },
          { label: "Tópico NPS ancla", value: "Operativa crítica fallida" },
          { label: "Touchpoint afectado", value: "Consulta" },
          { label: "Prob. detractores", value: "60.0%" },
          { label: "Delta NPS", value: "-0.0" },
          { label: "Impacto total", value: "0.00 pts" },
          { label: "Confianza", value: "0.16" },
          { label: "Links validados", value: "16" },
          { label: "Prioridad", value: "0.62" },
          { label: "NPS en riesgo", value: "0.00 pts" },
          { label: "NPS recuperable", value: "0.00 pts" },
          { label: "Owner", value: "VoC + Analitica" }
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
        ],
        matrix_figure: null,
        risk_recovery_figure: null,
        heatmap_figure: null,
        changepoints_figure: null,
        lag_figure: null
      }
    ]
  }
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
    expect(screen.getByRole("combobox", { name: "Año" })).toHaveValue("2026");
    expect(screen.getByRole("combobox", { name: "Mes" })).toHaveValue("03");
    expect(
      screen.getByRole("combobox", { name: "Mes" }).querySelector('option[value="03"]')
    ).toHaveTextContent("Marzo");

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

  it("renders the restored linking workspace with method-driven summary, deep dive and scenarios", async () => {
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
    expect(screen.queryByText("Ranking de hipótesis")).not.toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Journeys de detracción" }));
    expect(screen.getByText("Detalle de journeys de detracción")).toBeInTheDocument();
    expect(screen.getByText("Uso / Edo de Cuenta")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "NPS deep dive" }));
    expect(screen.getAllByText("Ranking de hipótesis").length).toBeGreaterThan(0);
    expect(screen.getByText("NPS tópicos trending")).toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "Tópico" }), "Operativa crítica fallida");
    const rankingTable = screen.getByRole("table");
    expect(within(rankingTable).getByText("Operativa crítica fallida")).toBeInTheDocument();
    expect(
      within(rankingTable).queryByText("Pagos/ Transferencias > Faltan detalles de movimientos")
    ).not.toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Evidence wall" }));
    const evidenceTable = screen.getByRole("table");
    expect(within(evidenceTable).getByText("Operativa crítica fallida")).toBeInTheDocument();
    expect(
      within(evidenceTable).queryByText("Pagos/ Transferencias > Faltan detalles de movimientos")
    ).not.toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Data deepdive analysis" }));
    const analysisTable = screen.getByRole("table");
    expect(within(analysisTable).getByText("Operativa crítica fallida")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Análisis de escenarios causales" }));
    expect(screen.getByText("2 journeys de detracción defendibles para detractores")).toBeInTheDocument();
    expect(screen.getAllByText("Operativa crítica fallida").length).toBeGreaterThan(0);
    expect(screen.getByText(/VoC \+ Analitica/i)).toBeInTheDocument();
  });
});
