import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { TaxonomyStudio, SnapshotSettings } from "./TaxonomyStudio";

const context = { service_origin: "Bank", service_origin_n1: "Web" };
function status() {
  return { active: "SOURCE", requested_active: "SOURCE", policy: "ACTIVE_ONLY", restored: false, discovered_catalog_available: false, discovery_local_available: true, detection: { state: "PARTIAL", rows: 96, missing: 16, usable_comments: 96 }, taxonomies: [
    { mode: "SOURCE", available: true, levers: 2, sublevers: 4, coverage: 0.83 },
    { mode: "COMPLETED", available: false }, { mode: "DISCOVERED", available: false }
  ] };
}
afterEach(() => vi.unstubAllGlobals());
it("edits completed taxonomy, imports discovery, explores and selects", async () => {
  const state = status();
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/discovery/instructions")) return new Response(JSON.stringify({ version: "test", designer: "Designer instructions", classifier: "Classifier instructions" }));
    if (url.includes("/discovery/designer/export")) return new Response(JSON.stringify({stage: "designer", saved_path: "/Downloads/designer.zip"}));
    if (url.includes("/discovery/classifier/import")) { state.discovered_catalog_available = true; Object.assign(state.taxonomies[2], {available:true,coverage:1,levers:2,sublevers:4}); return new Response(JSON.stringify({stage:"complete",progress:{total:96,received:96,pending:0}})); }
    if (url.includes("/discovery/progress")) return new Response(JSON.stringify({total:96,received:0,pending:96,multiple:0,designer:{total:96,received:0,pending:96,levers:0,sublevers:0}}));
    if (url.includes("/discovery")) return new Response(JSON.stringify({ helix_classifier_url:"https://chatgpt.com/g/helix", designer_url: "https://chatgpt.com/g/designer", classifier_url: "https://chatgpt.com/g/classifier", session: "connected" }));
    if (url.includes("/manual")) { if (init?.method === "PUT") Object.assign(state.taxonomies[1], { available: true, coverage: 1, levers: 1, sublevers: 1 }); return new Response(JSON.stringify({revision:"",exists:false,templates:["NONE","SOURCE"],affected_comments:0, taxonomy: [{ lever: "Atención", sublevers: ["Resolución"] }] })); }
    if (url.includes("/helix")) return new Response(JSON.stringify({ready:false,taxonomies:{SOURCE:{received:0,pending:1}},pending:1}));
    if (url.includes("/settings/equivalences")) return new Response(JSON.stringify({dimensions:{"nps.Palanca":[]},available_dimensions:["nps.Palanca"]}));
    if (url.includes("/settings")) { Object.assign(state,JSON.parse(String(init?.body))); state.requested_active = state.active; return new Response("{}"); }
    if (url.includes("/explore") || url.includes("/compare")) return new Response(JSON.stringify({ rows: [], total: 0, note: "Comparación reproducible" }));
    return new Response(JSON.stringify(state));
  });
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  render(<SWRConfig value={{ provider: () => new Map() }}><TaxonomyStudio context={context} onChange={async () => {}} /></SWRConfig>);
  await user.click(await screen.findByRole("button", { name: "Crear Manual" }));
  await user.click(await screen.findByRole("button", { name: "Guardar Manual" }));
  expect(await screen.findByText("Explorar Taxonomía Manual")).toBeInTheDocument();
  expect(screen.queryByText("Normalización · tabla de equivalencias")).not.toBeInTheDocument();
  await user.click(screen.getByRole("tab", {name:"Análisis con LLM"}));
  await user.click(screen.getByRole("button", { name: "Exportar comentarios para crear taxonomía" }));
  expect(await screen.findByText(/ZIP guardado en/)).toBeInTheDocument();
  await user.upload(screen.getByLabelText("Importar ZIP de comentarios clasificados"), new File(["{}"], "result.zip", { type: "application/zip" }));
  await user.click(await screen.findByText("Explorar Descubierta por LLM"));
  expect(await screen.findByText("Comparación reproducible")).toBeInTheDocument();
  expect(screen.queryByRole("button", {name:"Usar como lente"})).not.toBeInTheDocument();
  await user.selectOptions(screen.getByLabelText("Marco de clasificación"), "DISCOVERED");
  await waitFor(() => expect(state.active).toBe("DISCOVERED"));
  expect(fetcher.mock.calls.filter(([url]) => url.includes("/generate"))).toHaveLength(0);
});
it("persists snapshot policy using the global framework", async () => {
  const state = status();
  const fetcher = vi.fn(async (_url: string, init?: RequestInit) => {
    if (init?.method === "PUT") Object.assign(state, JSON.parse(String(init.body)));
    return new Response(JSON.stringify(state));
  });
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  render(<SWRConfig value={{ provider: () => new Map() }}><SnapshotSettings context={context} onChange={async () => {}} /></SWRConfig>);
  await user.selectOptions(await screen.findByLabelText("Qué guardar"), "ALL_AVAILABLE");
  await waitFor(() => expect(state.policy).toBe("ALL_AVAILABLE"));
  expect(screen.getByRole("link", { name: "Guardar snapshot local" })).toHaveAttribute("href", expect.stringContaining("/api/taxonomy/snapshot"));
});

it("places the global framework before both tabs and disables classification without a catalog", async () => {
  const state = status();
  Object.assign(state.taxonomies[0], { selectable: false });
  vi.stubGlobal("fetch", vi.fn(async (url: string) => {
    if (url.includes("/instructions")) return Response.json({ version: "test", designer: "Reglas", classifier: "Reglas", helix: "Reglas" });
    if (url.includes("/progress")) return Response.json({ total: 96, received: 0, pending: 96, multiple: 0, designer: {total: 96, received: 0, pending: 96} });
    if (url.includes("/discovery")) return Response.json({ designer_url: "", classifier_url: "", helix_classifier_url: "" });
    if (url.includes("/helix")) return Response.json({total: 1, received: 0, pending: 1});
    return Response.json(state);
  }));
  render(<SWRConfig value={{ provider: () => new Map() }}><TaxonomyStudio context={context} onChange={async () => {}} /></SWRConfig>);
  const selector = await screen.findByLabelText("Marco de clasificación");
  expect(selector).toBeDisabled();
  expect(selector.querySelector('option[value="SOURCE"]')).toBeNull();
  expect(selector.compareDocumentPosition(screen.getByRole("tab", { name: "Análisis estático" })) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  await userEvent.setup().click(screen.getByRole("tab", { name: "Análisis con LLM" }));
  expect(await screen.findByRole("button", { name: "Descargar todos los ZIP de comentarios pendientes" })).toBeDisabled();
  expect(screen.getByLabelText("Importar ZIP de comentarios clasificados")).toBeDisabled();
  expect(screen.getByRole("button", { name: "Descargar todos los ZIP de incidencias pendientes" })).toBeDisabled();
  expect(screen.getByLabelText("Importar ZIP de incidencias clasificadas")).toBeDisabled();
  expect(screen.getByRole("button", { name: "Exportar comentarios para crear taxonomía" })).toBeEnabled();
});
