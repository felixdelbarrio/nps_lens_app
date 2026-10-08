import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { LinkingPayload } from "../api";
import { LinkingWorkspace } from "./LinkingWorkspace";

vi.mock("./PlotFigure", () => ({ PlotFigure: ({ figure }: { figure: unknown }) => <output data-testid="figure">{JSON.stringify(figure)}</output> }));

const payload = {
  causal_method: { value: "broken_journeys" },
  situation: {
    narrative: { metrics: [{ label: "Respuestas analizadas", value: "20" }] },
    evidence: { rows: [
      { "NPS Topic": "Pagos", "Incident ID": "INC1", "Incident ID__href": "https://example.com/INC1", "Incident Summary": "Error pagos" },
      { "NPS Topic": "Acceso", "Incident ID": "INC2", "Incident Summary": "Error acceso" }
    ] }
  },
  entity_summary: {
    kpis: [{ label: "Tópicos observados", value: "2" }],
    figure: { data: [{ x: [1, 2] }] },
    topic_figures: { Pagos: { data: [{ x: [1] }] }, Acceso: { data: [{ x: [2] }] } },
    table: [{ "Tópico NPS ancla": "Pagos", "Vínculos semánticos": 1 }, { "Tópico NPS ancla": "Acceso", "Vínculos semánticos": 2 }]
  },
  scenarios: { cards: [{
    title: "Escenario", identity_rows: [
      { label: "Tópico NPS ancla", value: "Pagos" },
      { label: "Organizaciones de las incidencias enlazadas", value: "Equipo" }
    ],
    incident_records: [{ incident_id: "INC1", summary: "Error <script>alert(1)</script>", summary_segments: [
      { text: "Error", bold: true }, { text: " <script>alert(1)</script>", bold: false }
    ] }]
  }] }
} as unknown as LinkingPayload;

describe("Causal topic filters", () => {
  it("filters only evidence and keeps the summary unlinked", async () => {
    const user = userEvent.setup();
    render(<LinkingWorkspace linking={payload} tab="situation" onTabChange={() => {}} />);
    expect(screen.getByLabelText("NPS topic")).toHaveValue("");
    await user.selectOptions(screen.getByLabelText("NPS topic"), "Pagos");
    expect(screen.queryByText("INC2")).not.toBeInTheDocument();
    expect(screen.getByText("20")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "INC1" })).toHaveAttribute("href", "https://example.com/INC1");
    expect(screen.getByText("Error pagos").closest("a")).toBeNull();
    await user.selectOptions(screen.getByLabelText("NPS topic"), "");
    expect(screen.getByText("INC2")).toBeInTheDocument();
  });

  it("selects the precomputed chart and matching rows without changing KPIs", async () => {
    const user = userEvent.setup();
    render(<LinkingWorkspace linking={payload} tab="entity-summary" onTabChange={() => {}} />);
    await user.selectOptions(screen.getByLabelText("NPS topic"), "Pagos");
    expect(screen.getByTestId("figure")).toHaveTextContent('{"data":[{"x":[1]}],"layout":{}}');
    expect(within(screen.getByRole("table")).queryByText("Acceso")).not.toBeInTheDocument();
    expect(screen.getByText("Tópicos observados").parentElement).toHaveTextContent("2");
  });

  it("renders the canonical identity and safe precomputed emphasis", () => {
    const { container } = render(<LinkingWorkspace linking={payload} tab="scenarios" onTabChange={() => {}} />);
    expect(container.querySelectorAll("dt")).toHaveLength(2);
    expect(screen.getByText("Organizaciones de las incidencias enlazadas")).toBeInTheDocument();
    expect(screen.getByText("Equipo")).toBeInTheDocument();
    expect(screen.getByText("Error").tagName).toBe("STRONG");
    expect(container.querySelector("script")).toBeNull();
    expect(screen.queryByRole("columnheader", { name: /segments/ })).not.toBeInTheDocument();
  });
});

it("shows full link totals instead of the capped evidence sample", () => {
  const cards = payload.scenarios?.cards as Array<Record<string, unknown>>;
  const linking = { ...payload, scenarios: { cards: [{ ...cards[0], linked_incidents: 375, linked_comments: 118 }] } };
  render(<LinkingWorkspace linking={linking} tab="scenarios" onTabChange={() => {}} />);
  expect(screen.getByRole("heading", { name: "375 incidencias enlazadas" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "118 comentarios enlazados" })).toBeInTheDocument();
});

it("uses server confidence labels without changing the existing table payload", () => {
  const table = [{ "Tópico NPS ancla": "Pagos", "Similitud textual": "90%" }];
  const linking = {
    ...payload,
    entity_summary: {
      ...payload.entity_summary,
      table,
      column_labels: { "Similitud textual": "SIMILITUD SEMÁNTICA" }
    }
  };
  render(<LinkingWorkspace linking={linking} tab="entity-summary" onTabChange={() => {}} />);
  expect(screen.getByRole("columnheader", { name: "SIMILITUD SEMÁNTICA" })).toBeInTheDocument();
  expect(screen.getByText("90,0%")).toBeInTheDocument();
  expect(table[0]["Similitud textual"]).toBe("90%");
});

it("shows the complete server score distribution instead of IDs or sample counts", () => {
  const cards = payload.scenarios?.cards as Array<Record<string, unknown>>;
  const linking = { ...payload, scenarios: { cards: [{
    ...cards[0], linked_comments: 4,
    score_distribution: [
      { score: 0, count: 3, label: "Score 0 / 3 Comentarios" },
      { score: 1, count: 1, label: "Score 1 / 1 Comentario" }
    ],
    comment_records: [{ comment_id: "private-id", nps: "0", comment: "No puedo acceder" }],
    spotlight_metrics: [{label:"NOTA MEDIA DE COMENTARIOS ENLAZADOS",value:"0,25"}]
  }] } };
  render(<LinkingWorkspace linking={linking} tab="scenarios" onTabChange={() => {}} />);
  const overview = screen.getByRole("heading", {name:"4 comentarios enlazados"}).closest("article")!;
  expect(within(overview).getByText("Score 0 / 3 Comentarios")).toBeInTheDocument();
  expect(within(overview).getByText("Score 1 / 1 Comentario")).toBeInTheDocument();
  expect(within(overview).queryByText("private-id")).not.toBeInTheDocument();
  expect(screen.getByText("0,25")).toBeInTheDocument();
});

it("uses the published evidence column order with semantic confidence last", () => {
  const columns = ["Detractor Comment", "Incident ID", "Incident Summary", "NPS Topic", "Confianza semántica"];
  const linking = {
    ...payload,
    situation: {
      evidence: {
        columns,
        rows: [{ "Confianza semántica": "90%", "Detractor Comment": "No puedo acceder", "Incident ID": "INC1", "Incident Summary": "Error", "NPS Topic": "Acceso > Token" }]
      }
    }
  } as unknown as LinkingPayload;
  render(<LinkingWorkspace linking={linking} tab="situation" onTabChange={() => {}} />);
  expect(screen.getAllByRole("columnheader").map(header => header.textContent)).toEqual(columns);
  expect(screen.getByText("90,0%")).toBeInTheDocument();
  expect(screen.queryByText("Tasa Foco")).not.toBeInTheDocument();
  expect(screen.queryByText("Similitud textual")).not.toBeInTheDocument();
});

it("respects the server sequence and resets navigation when the scope changes with the same number of cases", async () => {
  const user = userEvent.setup();
  const first = {
    ...payload,
    context_pills: ["Argentina", "Agosto"],
    scenarios: { cards: [
      { scenario_id: "a", title: "Operativa: primero" },
      { scenario_id: "b", title: "Continuidad: segundo" }
    ] }
  } as unknown as LinkingPayload;
  const { rerender } = render(<LinkingWorkspace linking={first} tab="scenarios" onTabChange={() => {}} />);
  expect(screen.getByRole("heading", { name: "Operativa: primero" })).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Ver siguiente" }));
  expect(screen.getByRole("heading", { name: "Continuidad: segundo" })).toBeInTheDocument();
  const second = {
    ...first,
    context_pills: ["México", "Septiembre"],
    scenarios: { cards: [
      { scenario_id: "c", title: "Acceso: primero del nuevo ámbito" },
      { scenario_id: "d", title: "Información: segundo del nuevo ámbito" }
    ] }
  } as unknown as LinkingPayload;
  rerender(<LinkingWorkspace linking={second} tab="scenarios" onTabChange={() => {}} />);
  expect(screen.getByRole("heading", { name: "Acceso: primero del nuevo ámbito" })).toBeInTheDocument();
  expect(screen.getByText("Escenario 1 de 2")).toBeInTheDocument();
});
