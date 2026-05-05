import { render, screen, waitFor } from "@testing-library/react";
import { within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
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
  score_channels: ["Todos", "Web", "App"],
  causal_method_options: [
    {
      value: "palanca_touchpoint",
      label: "Por Palanca",
      summary: "La lectura causal organiza la atribución por palanca.",
      flow: "Incidencias -> Touchpoint afectado -> Palanca -> Comentario -> NPS"
    },
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
    score_channel: "Todos",
    theme_mode: "light",
    downloads_path: "/Users/test/Downloads",
    helix_base_url: "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/",
    report_dimension_analysis: "palanca",
    touchpoint_source: "palanca_touchpoint",
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
    neutral_rate: 0.435,
    promoter_rate: 0.22
  },
  overview: {
    daily_kpis_figure: null,
    weekly_trend_figure: null,
    topics_figure: null,
    topics_table: [],
    daily_volume_figure: null,
    daily_volume_mix_figure: null,
    daily_mix_figure: null,
    daily_explanation_bullets: [
      "El periodo arranca con NPS clásico **-12.5** y termina en **4.0**."
    ],
    insight_bullets: []
  },
  scope: {
    cumulative: {
      label: "Datos acumulados hasta Marzo 2026",
      note: "KPIs calculados solo con Service Container y Period Container.",
      kpis: {
        samples: 50000,
        nps_average: 4.5,
        detractor_rate: 0.32,
        neutral_rate: 0.42,
        promoter_rate: 0.26
      }
    },
    period: {
      label: "Marzo 2026",
      kpis: {
        samples: 26618,
        nps_average: 4.2,
        detractor_rate: 0.345,
        neutral_rate: 0.435,
        promoter_rate: 0.22
      },
      deltas: {
        nps_average: { value: -0.3, direction: "down", favorable: false },
        detractor_rate: { value: 0.025, direction: "up", favorable: false },
        neutral_rate: { value: 0.015, direction: "up", favorable: true },
        promoter_rate: { value: -0.04, direction: "down", favorable: false }
      }
    }
  },
  comparison: { has_data: false, table: [] },
  cohorts: {},
  gaps: { has_data: false, table: [] },
  opportunities: {
    has_data: false,
    table: [],
    bullets: ["Si mejoramos **Palanca=Acceso**, el modelo estima un **potencial de +24.0 puntos**."]
  },
  controls: {
    dimensions: ["Palanca", "Subpalanca", "Canal", "UsuarioDecisión"],
    cohort_rows: ["Palanca", "Subpalanca"],
    cohort_columns: ["Canal", "Usuario", "NPSGROUP"],
    min_n: 200,
    min_n_cross: 30
  },
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
    { id: "nps-deep-dive", label: "Análisis de Tópicos de NPS afectados" }
  ],
  kpis: {
    responses: 26618,
    incidents: 233,
    linked_pairs: 34,
    average_focus_rate: 0.1524
  },
  situation: {
    narrative: {
      kicker: "Narrativa causal",
      title: "2 journeys de detracción defendibles para detractores",
      summary: "La política Helix↔VoC está fijada en similitud ≥ 0.20.",
      metrics: [
        {
          label: "Método causal",
          value: "Journeys de detracción",
          hint: "Incidencias + comentario + tópico NPS -> Journey ejecutivo -> NPS"
        },
        { label: "Respuestas analizadas", value: "26618" },
        { label: "Comentarios enlazados", value: "22" },
        { label: "Incidencias del periodo", value: "233" },
        { label: "Incidencias con match", value: "20" },
        { label: "Links validados", value: "30" },
        { label: "% detractores medio", value: "15.2%" }
      ]
    },
    metadata: [
      {
        label: "Flujo causal",
        value: "Incidencias + comentario + tópico NPS -> Journey ejecutivo -> NPS"
      },
      { label: "Foco analítico", value: "Journeys de detracción" }
    ],
    figure: null,
    note: "El método causal activo transforma la evidencia en journeys ejecutivos con foco de comité."
  },
  entity_summary: {
    title: "Journeys de detracción",
    subtitle: "Cada escenario resume un journey ejecutivo del catálogo y la evidencia que sostiene su impacto en NPS.",
    kpis: [
      { label: "Journeys de detracción", value: "6" },
      { label: "Touchpoints cubiertos", value: "4" },
      { label: "Links validados", value: "34" }
    ],
    figure_title: "Evidencia validada por journey",
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
    title: "Análisis de Tópicos de NPS afectados",
    subtitle: "Profundización sobre los tópicos NPS explicados por los journeys de detracción activos.",
    kpis: [
      { label: "Score en riesgo", value: "3.90 pts" },
      { label: "Score recuperable", value: "2.40 pts" },
      { label: "Concentración top-3", value: "74.0%" },
      { label: "Tiempo de reacción", value: "1.2 semanas" }
    ],
    topic_filter: {
      label: "Tópico NPS afectado",
      options: [
        { value: "Todos", label: "Todos (2 tópicos afectados)" },
        {
          value: "Pagos/ Transferencias > Faltan detalles de movimientos",
          label: "Pagos/ Transferencias > Faltan detalles de movimientos"
        },
        {
          value: "Consulta > Estado de cuenta / comprobantes",
          label: "Consulta > Estado de cuenta / comprobantes"
        }
      ],
      default: "Todos",
      hint: "2 tópicos afectados por journeys de detracción."
    },
    tabs: [
      { id: "ranking", label: "Ranking de hipótesis" },
      { id: "evidence", label: "Evidence wall" }
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
          "Tópico NPS": "Consulta > Estado de cuenta / comprobantes",
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
          incident_id__href:
            "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/IDGH5CDNHIEUEAT3VXLMT3VXLM0OU4",
          incident_summary: "ACOTAMIENTO IRD...",
          detractor_comment: "mal no funciona los pagos al sua ni impuestos cdmx"
        },
        {
          nps_topic: "Consulta > Estado de cuenta / comprobantes",
          similarity: 0.121,
          incident_id: "INC000104355468",
          incident_id__href:
            "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/IDGH5CDNHIEUEAT3VXLMT3VXLM0OU5",
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
          "Riesgo Score"
        ],
        spotlight_metrics: [
          { label: "Journey de detracción", value: "Uso / Edo de Cuenta" },
          {
            label: "Tópico NPS ancla",
            value: "Pagos/ Transferencias > Faltan detalles de movimientos"
          },
          { label: "Touchpoint afectado", value: "Consulta" },
          { label: "Prob. detractores", value: "60.0%" },
          { label: "Delta Score", value: "-0.0" },
          { label: "Impacto total", value: "0.00 pts" },
          { label: "Confianza", value: "0.16" },
          { label: "Links validados", value: "16" },
          { label: "Prioridad", value: "0.62" },
          { label: "Score en riesgo", value: "0.00 pts" },
          { label: "Score recuperable", value: "0.00 pts" },
          { label: "Owner (rol)", value: "VoC + Analitica" }
        ],
        incident_records: [
          {
            incident_id: "INC000104355468",
            summary: "ACOTAMIENTO IRD El usuario Mario Alberto Santillan Medina...",
            incident_id__href:
              "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/IDGH5CDNHIEUEAT3VXLMT3VXLM0OU5"
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
      },
      {
        chain_key: "chain-2",
        rank: 2,
        title: "Fricción en consulta de saldos",
        statement: "Consulta -> saldo / disponibilidad -> retraso de actualización.",
        selection_label: "Consulta de saldos | Fricción en consulta de saldos | 8 INC | 10 VoC",
        linked_incidents: 8,
        linked_comments: 10,
        linked_pairs: 14,
        detractor_probability: 0.52,
        nps_delta_expected: -0.0,
        total_nps_impact: 0.0,
        confidence: 0.14,
        priority: 0.51,
        nps_points_at_risk: 0.0,
        nps_points_recoverable: 0.0,
        owner_role: "Canal Digital",
        flow_steps: [
          "(8) Incidencias + comentarios",
          "Consulta de saldos",
          "Consulta / Disponibilidad / Saldos",
          "Riesgo Score"
        ],
        spotlight_metrics: [
          { label: "Journey de detracción", value: "Consulta de saldos" },
          { label: "Tópico NPS ancla", value: "Consulta > Estado de cuenta / comprobantes" },
          { label: "Touchpoint afectado", value: "Consulta" },
          { label: "Prob. detractores", value: "52.0%" },
          { label: "Delta Score", value: "-0.0" },
          { label: "Impacto total", value: "0.00 pts" },
          { label: "Confianza", value: "0.14" },
          { label: "Links validados", value: "14" },
          { label: "Prioridad", value: "0.51" },
          { label: "Score en riesgo", value: "0.00 pts" },
          { label: "Score recuperable", value: "0.00 pts" },
          { label: "Owner (rol)", value: "Canal Digital" }
        ],
        incident_records: [
          {
            incident_id: "INC000104231684",
            summary: "ACOTAMIENTO IRD...",
            url: "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/IDGH5CDNHIEUEAT3VXLMT3VXLM0OU4"
          }
        ],
        comment_records: [
          {
            comment_id: "2",
            date: "2026-03-25",
            nps: "2",
            group: "DETRACTOR",
            palanca: "Consulta",
            subpalanca: "Saldos",
            comment: "Los saldos tardan mucho en reflejarse."
          }
        ],
        detail_table: [
          {
            "Tópico NPS": "Fricción en consulta de saldos",
            Prioridad: 0.51
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

  function renderApp() {
    return render(
      <SWRConfig value={{ provider: () => new Map(), dedupingInterval: 0 }}>
        <App />
      </SWRConfig>
    );
  }

  it("renders restored navigation, filters, traceability and uploads", async () => {
    const user = userEvent.setup();
    renderApp();

    await waitFor(() =>
      expect(
        screen.getByText(/Producto sincronizado con histórico persistente/i)
      ).toBeInTheDocument()
    );

    expect(screen.getByRole("heading", { name: /NPS Lens/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Ingesta/i })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Media semanal" })).toBeInTheDocument();
    expect(
      screen.queryByRole("tab", { name: "Evolución promotores vs detractores" })
    ).not.toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Sumario del Periodo" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Analítica NPS Térmico" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Insights operativos" })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Service Origin/i })).toBeInTheDocument();
    expect(screen.getByText("PERIOD CONTAINER")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "FILTROS" })).not.toBeInTheDocument();
    expect(screen.queryByRole("listbox", { name: "N2" })).not.toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByRole("combobox", { name: "Año" })).toHaveValue("2026");
      expect(screen.getByRole("combobox", { name: "Mes" })).toHaveValue("03");
    });
    expect(
      screen.getByRole("combobox", { name: "Mes" }).querySelector('option[value="03"]')
    ).toHaveTextContent("Marzo");
    expect(screen.getAllByText("Score medio (0-10)").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Neutros (7-8)").length).toBeGreaterThan(0);
    await user.click(screen.getByRole("tab", { name: "Oportunidades priorizadas" }));
    const opportunityNote = screen.getByText(/Si mejoramos/i).closest("li");
    expect(opportunityNote).not.toBeNull();
    expect(opportunityNote).not.toHaveTextContent("**");
    expect(within(opportunityNote as HTMLElement).getByText("Palanca=Acceso").tagName).toBe("STRONG");

    await user.click(screen.getByRole("tab", { name: "Analítica NPS Térmico" }));
    expect(screen.getByRole("heading", { name: "FILTROS" })).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "Canal" })).toHaveValue("Web");
    expect(screen.getByRole("combobox", { name: "Grupo Score" })).toHaveValue("Detractores");
    expect(screen.getByRole("tab", { name: "Qué dicen los clientes" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Cambios respecto al histórico" })).toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "Canal" }), "App");
    await waitFor(() =>
      expect(screen.getByTestId("operational-state")).toHaveTextContent("OPERATIVO")
    );
    await user.click(screen.getByRole("tab", { name: "Incidencias ↔ NPS" }));
    expect(screen.getByRole("combobox", { name: "Canal" })).toHaveValue("Web");
    expect(screen.queryByRole("combobox", { name: "Grupo Score" })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Ingesta/i }));
    expect(screen.queryByRole("heading", { name: "FILTROS" })).not.toBeInTheDocument();
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

    renderApp();

    await waitFor(() =>
      expect(
        screen.getByText(/Producto sincronizado con histórico persistente/i)
      ).toBeInTheDocument()
    );

    await user.click(screen.getByRole("button", { name: /Generar reporte en PowerPoint/i }));

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

  it("shows the operational state while loading and unlocks actions when stable", async () => {
    renderApp();

    expect(screen.getByTestId("operational-state")).toHaveTextContent("SINCRONIZANDO");
    expect(screen.getByTestId("generate-report-button")).toBeDisabled();

    await waitFor(() =>
      expect(screen.getByTestId("operational-state")).toHaveTextContent("OPERATIVO")
    );
    expect(screen.getByTestId("generate-report-button")).toBeEnabled();
  });

  it("renders the restored linking workspace with method-driven summary, deep dive and scenarios", async () => {
    const user = userEvent.setup();
    currentLinkingPayload = linkingPayloadAvailable;

    renderApp();

    await waitFor(() =>
      expect(
        screen.getByText(/Producto sincronizado con histórico persistente/i)
      ).toBeInTheDocument()
    );

    await user.click(screen.getByRole("tab", { name: "Incidencias ↔ NPS" }));
    await waitFor(() =>
      expect(screen.getByText("2 journeys de detracción defendibles para detractores")).toBeInTheDocument()
    );
    expect(screen.getByRole("combobox", { name: "Método causal" })).toHaveValue("palanca_touchpoint");
    expect(screen.getByText("Respuestas analizadas")).toBeInTheDocument();
    const linkedCommentsMetric = screen.getByText("Comentarios enlazados");
    const incidentsMetric = screen.getByText("Incidencias del periodo");
    const linksMetric = screen.getByText("Links validados");
    const focusMetric = screen.getByText("% detractores medio");
    expect(
      Boolean(linkedCommentsMetric.compareDocumentPosition(incidentsMetric) & Node.DOCUMENT_POSITION_FOLLOWING)
    ).toBe(true);
    expect(
      Boolean(linksMetric.compareDocumentPosition(focusMetric) & Node.DOCUMENT_POSITION_FOLLOWING)
    ).toBe(true);
    expect(screen.getByText(/Flujo causal:/i)).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Timeline causal (diario)" })).not.toBeInTheDocument();
    expect(
      screen.queryByText("No hay suficiente base cruzada para construir el timeline causal.")
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Ranking de hipótesis")).not.toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Journeys de detracción" }));
    expect(screen.getByText("Detalle de journeys de detracción")).toBeInTheDocument();
    expect(screen.getByText("Uso / Edo de Cuenta")).toBeInTheDocument();

    await user.click(
      screen.getByRole("tab", { name: "Análisis de Tópicos de NPS afectados" })
    );
    expect(screen.getAllByText("Ranking de hipótesis").length).toBeGreaterThan(0);
    expect(screen.getByText("NPS tópicos trending")).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: /Tópico NPS afectado/i })).toHaveValue("Todos");
    await user.selectOptions(
      screen.getByRole("combobox", { name: /Tópico NPS afectado/i }),
      "Consulta > Estado de cuenta / comprobantes"
    );
    const rankingTable = screen.getByRole("table");
    expect(within(rankingTable).getByText("Consulta > Estado de cuenta / comprobantes")).toBeInTheDocument();
    expect(
      within(rankingTable).queryByText("Pagos/ Transferencias > Faltan detalles de movimientos")
    ).not.toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Evidence wall" }));
    const evidenceTable = screen.getByRole("table");
    expect(
      within(evidenceTable).getByText("Consulta > Estado de cuenta / comprobantes")
    ).toBeInTheDocument();
    expect(
      within(evidenceTable).queryByText("Pagos/ Transferencias > Faltan detalles de movimientos")
    ).not.toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: "INC000104355468" })).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          href: "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/IDGH5CDNHIEUEAT3VXLMT3VXLM0OU5"
        })
      ])
    );

    await user.click(screen.getByRole("tab", { name: "Análisis de escenarios causales" }));
    expect(
      screen.queryByText("2 journeys de detracción defendibles para detractores")
    ).not.toBeInTheDocument();
    expect(screen.getAllByText("Operativa crítica fallida").length).toBeGreaterThan(0);
    expect(screen.getByText(/VoC \+ Analitica/i)).toBeInTheDocument();
    expect(screen.queryByText("Escenario activo")).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "1 incidencia enlazada" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "1 comentario enlazado" })).toBeInTheDocument();
    expect(screen.queryByText(/Los IDs abren la incidencia original en Helix/i)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Tabla" })).toHaveClass("is-active");
    expect(screen.getAllByRole("link", { name: "INC000104355468" })).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          href: "https://itsmhelixbbva-smartit.onbmc.com/smartit/app/#/incidentPV/IDGH5CDNHIEUEAT3VXLMT3VXLM0OU5"
        })
      ])
    );
  });
});
