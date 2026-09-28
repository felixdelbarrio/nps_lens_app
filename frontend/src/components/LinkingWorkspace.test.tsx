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
      { label: "Organizaciones responsables observadas", value: "Equipo" },
      { label: "Duración media histórica de resolución (semanas)", value: "1,20" }
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
    expect(container.querySelectorAll("dt")).toHaveLength(3);
    expect(screen.getByText("1,20")).toBeInTheDocument();
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
