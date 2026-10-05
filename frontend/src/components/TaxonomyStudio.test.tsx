import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { TaxonomyStudio, SnapshotSettings } from "./TaxonomyStudio";

const context = { service_origin: "Bank", service_origin_n1: "Web" };
function status() {
  return { active: "SOURCE", requested_active: "SOURCE", active_fingerprint: "source-fingerprint", policy: "ACTIVE_ONLY", restored: false, discovered_catalog_available: false, discovery_local_available: true, detection: { state: "PARTIAL", rows: 96, missing: 16, usable_comments: 96 }, taxonomies: [
    { mode: "SOURCE", available: true, levers: 2, sublevers: 4, coverage: 0.83 },
    { mode: "COMPLETED", available: false }, { mode: "DISCOVERED", available: false }
  ] };
}
afterEach(() => vi.unstubAllGlobals());
it("edits completed taxonomy, imports discovery, explores and selects", async () => {
  const state = status();
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/discovery/instructions")) return new Response(JSON.stringify({ versions: {designer: "test", classifier: "test", helix: "test", normalizer: "test"}, designer: "Designer instructions", classifier: "Classifier instructions" }));
    if (url.includes("/discovery/designer/export")) return new Response(JSON.stringify({stage: "designer", saved_path: "/Downloads/designer.zip"}));
    if (url.includes("/discovery/classifier/import")) { state.discovered_catalog_available = true; Object.assign(state.taxonomies[2], {available:true,coverage:1,levers:2,sublevers:4}); return new Response(JSON.stringify({stage:"complete",progress:{total:96,received:96,pending:0}})); }
    if (url.includes("/discovery/progress")) return new Response(JSON.stringify({total:96,received:0,pending:96,multiple:0,designer:{total:96,received:0,pending:96,levers:0,sublevers:0}}));
    if (url.includes("/discovery")) return new Response(JSON.stringify({ helix_classifier_url:"https://chatgpt.com/g/helix", designer_url: "https://chatgpt.com/g/designer", classifier_url: "https://chatgpt.com/g/classifier", session: "connected" }));
    if (url.includes("/manual")) { if (init?.method === "PUT") Object.assign(state.taxonomies[1], { available: true, coverage: 1, levers: 1, sublevers: 1 }); return new Response(JSON.stringify({revision:"",exists:false,templates:["NONE","SOURCE"],affected_comments:0, taxonomy: [{ lever: "Atención", sublevers: ["Resolución"] }] })); }
    if (url.includes("/helix/engine")) return Response.json({selected_engine:"rules",ready:false,active:"SOURCE",received:0,total:1});
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
  const classification = screen.getByRole("heading", { name: "Clasifica incidencias" });
  const linking = screen.getByRole("heading", { name: "Vinculación Helix ↔ VoC" });
  expect(classification.compareDocumentPosition(linking) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  expect(screen.getByRole("option", {name:"LLM semántico"})).toBeDisabled();
  expect(screen.queryByLabelText("Importar ZIP de vínculos evaluados")).not.toBeInTheDocument();
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
    if (url.includes("/instructions")) return Response.json({ versions: {designer: "test", classifier: "test", helix: "test", normalizer: "test"}, designer: "Reglas", classifier: "Reglas", helix: "Reglas" });
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

it("previews and accepts a discovered proposal without changing the framework", async () => {
  const state = { ...status(), proposed_discovered_fingerprint: "discovered-abcdef", proposed_discovered_taxonomy: {taxonomy:[{lever:"Atención",sublevers:[{name:"Resolución",criterion:"Problema resuelto"}]}]}, designer_review:{reason:"La evidencia respalda las categorías",quotes:["Resolvieron mi problema"]} };
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/instructions")) return Response.json({ versions: { designer: "1", classifier: "1", helix: "1", normalizer: "1" }, designer: "Reglas", classifier: "Reglas", helix: "Reglas" });
    if (url.includes("/settings") && init?.method === "PUT") {
      expect(JSON.parse(String(init.body))).toEqual({accept_proposal:true});
      Object.assign(state.taxonomies[2], {available:true});
      return Response.json({});
    }
    if (url.includes("/discovery")) return Response.json({ designer_url: "", classifier_url: "", helix_classifier_url: "" });
    return Response.json(state);
  });
  vi.stubGlobal("fetch", fetcher);
  render(<SWRConfig value={{ provider: () => new Map() }}><TaxonomyStudio context={context} onChange={async () => {}} /></SWRConfig>);
  await userEvent.setup().click(await screen.findByRole("tab", { name: "Análisis con LLM" }));
  expect(await screen.findByText("source-f")).toBeInTheDocument();
  const proposal = screen.getByRole("region", {name:"Propuesta DISCOVERED"});
  expect(proposal).toHaveTextContent("Atención");
  expect(proposal).toHaveTextContent("Resolución");
  expect(proposal).toHaveTextContent("Problema resuelto");
  expect(proposal).toHaveTextContent("Resolvieron mi problema");
  expect(proposal.closest("article")).toHaveTextContent("Crear Taxonomía");
  await userEvent.setup().click(screen.getByRole("button", { name: "Aceptar taxonomía DISCOVERED" }));
  await waitFor(() => expect(fetcher).toHaveBeenCalledWith(expect.stringContaining("/settings?"), expect.objectContaining({ method: "PUT" })));
  expect(screen.getByLabelText("Marco de clasificación")).toHaveValue("SOURCE");
});

it("passes the changing analysis scope to classification while keeping designer on the dataset", async () => {
  const fetcher = vi.fn(async (url: string) => {
    if (url.includes("/instructions")) return Response.json({ versions: {designer:"test",classifier:"test",helix:"test",normalizer:"test"}, designer:"Reglas",classifier:"Reglas",helix:"Reglas" });
    if (url.includes("/progress")) return Response.json({ total:3,received:1,pending:2,multiple:0,analysis_horizon:{comment_start:"2026-06-12",comment_end:"2026-09-20",helix_start:"2026-03-14",max_days_apart:90},designer:{total:4,received:0,pending:4,levers:0,sublevers:0} });
    if (url.includes("/discovery")) return Response.json({designer_url:"",classifier_url:"",helix_classifier_url:""});
    if (url.includes("/helix/engine")) return Response.json({selected_engine:"rules",ready:false,active:"SOURCE",total:2,received:0});
    if (url.includes("/helix")) return Response.json({total:2,received:0,pending:2,classified:0,unassigned:0,coverage:0,multiple:0,categories:[],taxonomies:{SOURCE:{received:0,pending:2}}});
    return Response.json(status());
  });
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  const scope = {...context,pop_year:"2026",pop_month:"09",score_channel:"Web",nps_group:"Detractores",max_days_apart:"90"};
  const {rerender} = render(<SWRConfig value={{provider:()=>new Map()}}><TaxonomyStudio context={context} classificationContext={scope} onChange={async()=>{}} /></SWRConfig>);
  await user.click(await screen.findByRole("tab",{name:"Análisis con LLM"}));
  expect(await screen.findByText(/Ámbito analítico: 2026-06-12/)).toHaveTextContent("Helix desde 2026-03-14 por ventana de 90 días");
  await waitFor(()=>expect(fetcher.mock.calls.some(([url])=>url.includes("/helix?") && new URL(url,"http://localhost").searchParams.get("pop_month") === "09")).toBe(true));
  expect(fetcher.mock.calls.some(([url])=>url.includes("/discovery/progress?") && !new URL(url,"http://localhost").searchParams.has("pop_month"))).toBe(true);
  rerender(<SWRConfig><TaxonomyStudio context={context} classificationContext={{...scope,pop_month:"10"}} onChange={async()=>{}} /></SWRConfig>);
  await waitFor(()=>expect(fetcher.mock.calls.some(([url])=>url.includes("/helix?") && new URL(url,"http://localhost").searchParams.get("pop_month") === "10")).toBe(true));
});
