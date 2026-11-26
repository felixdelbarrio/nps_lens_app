import { render, screen, waitFor, within } from "@testing-library/react";
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
    if (url.includes("/explore") || url.includes("/compare")) return new Response(JSON.stringify({ rows: [], total: 0, comparable: 0, groups: 0, note: "Comparación reproducible" }));
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
  const commentsControl = screen.getByLabelText("Clasificación de comentarios");
  expect(commentsControl.closest("article")).toHaveTextContent("Clasifica comentarios");
  expect(screen.getByRole("heading", { name: "Clasifica incidencias" })).toBeInTheDocument();
  expect(screen.queryByRole("heading", { name: "Vinculación Helix ↔ VoC" })).not.toBeInTheDocument();
  expect(screen.queryByRole("switch", {name:"Vinculación con LLM"})).not.toBeInTheDocument();
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
    if (url.includes("/progress")) return Response.json({total:96,received:96,pending:0,multiple:0,designer:{total:96,received:96,pending:0,levers:1,sublevers:1}});
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
  const details = proposal.querySelector("details")!;
  const project = proposal.closest("article")!;
  const follows = (before: Element, after: Element) => expect(before.compareDocumentPosition(after) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  await waitFor(() => expect(project.querySelector(".exchange-progress")).toBeInTheDocument());
  follows(project.querySelector(".exchange-progress")!, screen.getByLabelText("URL · Crear Taxonomía"));
  follows(screen.getByLabelText("URL · Crear Taxonomía"), screen.getByRole("button", {name:"Copiar instrucciones de Crear Taxonomía"}));
  follows(screen.getByRole("button", {name:"Copiar instrucciones de Crear Taxonomía"}), screen.getByRole("button", {name:"Exportar comentarios para crear taxonomía"}));
  follows(project.querySelector(".exchange-actions")!, proposal);
  follows(details, screen.getByRole("link", {name:"Descargar taxonomía en Excel"}));
  expect(details).not.toHaveAttribute("open");
  expect(screen.getByRole("link", {name:"Descargar taxonomía en Excel"})).toHaveAttribute("href", expect.stringContaining("mode=DISCOVERED&proposal=true"));
  expect(screen.getByRole("link", {name:"Descargar taxonomía en Excel"})).toHaveAttribute("href", expect.stringContaining("service_origin=Bank&service_origin_n1=Web"));
  expect(screen.getByRole("button", {name:"Aceptar taxonomía DISCOVERED"})).toBeVisible();
  await userEvent.setup().click(screen.getByText("Ver taxonomía", {exact:true}));
  expect(details).toHaveAttribute("open");
  expect(proposal).toHaveTextContent("Atención");
  expect(proposal).toHaveTextContent("Resolución");
  expect(proposal).toHaveTextContent("Problema resuelto");
  expect(proposal).toHaveTextContent("Resolvieron mi problema");
  expect(proposal.closest("article")).toHaveTextContent("Crear Taxonomía");
  await userEvent.setup().click(screen.getByText("Ver taxonomía", {exact:true}));
  expect(details).not.toHaveAttribute("open");
  await userEvent.setup().click(screen.getByRole("button", { name: "Aceptar taxonomía DISCOVERED" }));
  await waitFor(() => expect(fetcher).toHaveBeenCalledWith(expect.stringContaining("/settings?"), expect.objectContaining({ method: "PUT" })));
  expect(screen.getByLabelText("Marco de clasificación")).toHaveValue("SOURCE");
});

it("exports pending comments across the company by default and keeps the analytical scope optional", async () => {
  let allTotal = 3;
  let classified = 3;
  const params = (url: string) => new URL(url, "http://localhost").searchParams;
  const fetcher = vi.fn(async (url: string, _init?: RequestInit) => {
    if (url.includes("/instructions")) return Response.json({ versions: {designer:"test",classifier:"test",helix:"test",semantic:"test"}, designer:"Reglas",classifier:"Reglas",helix:"Reglas",semantic:"Reglas" });
    if (url.includes("/discovery/progress")) {
      const period = params(url).get("pop_month") === "09";
      const total = period ? 3 : allTotal;
      const received = period ? 3 : classified;
      return Response.json({ total,received,pending:total-received,multiple:0,mode:"SOURCE",taxonomy_fingerprint:"source-fingerprint",analysis_horizon:{comment_start:"2026-06-12",comment_end:period ? "2026-09-20" : "2026-10-05",helix_start:"2026-06-03",max_days_apart:90},designer:{total:allTotal,received:0,pending:allTotal,levers:0,sublevers:0} });
    }
    if (url.includes("/discovery/classifier/export")) return Response.json({saved_paths:["/Downloads/comentarios-000001.zip"],saved_directory:"/Downloads",batches:1});
    if (url.includes("/discovery/classifier/import")) { classified = allTotal; return Response.json({progress:{total:allTotal,received:classified,pending:0}}); }
    if (url.includes("/discovery")) return Response.json({designer_url:"",classifier_url:"",helix_classifier_url:"",semantic_url:""});
    if (url.includes("/comments/engine") || url.includes("/helix/engine")) return Response.json({selected_engine:"rules",ready:true,base_available:true,active:"SOURCE",total:3,received:3});
    if (url.includes("/helix")) return Response.json({total:2,received:0,pending:2,classified:0,unassigned:0,coverage:0,multiple:0,categories:[],taxonomies:{SOURCE:{received:0,pending:2}}});
    if (url.includes("/manual")) return Response.json({taxonomy:[],templates:["NONE"],exists:false});
    return Response.json(status());
  });
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  const scope = {...context,pop_year:"2026",pop_month:"09",score_channel:"Web",nps_group:"Detractores",max_days_apart:"90"};
  const cache = new Map();
  const provider = () => cache;
  const {rerender} = render(<SWRConfig value={{provider}}><TaxonomyStudio context={context} classificationContext={scope} onChange={async()=>{}} /></SWRConfig>);
  await user.click(await screen.findByRole("tab",{name:"Análisis con LLM"}));
  const selector = await screen.findByLabelText("Ámbito de clasificación de comentarios");
  const download = () => screen.getByRole("button",{name:"Descargar todos los ZIP de comentarios pendientes"});
  const classifier = within(selector.closest("article")!);
  expect(selector).toHaveValue("all");
  expect(await classifier.findByText("0 pendientes")).toBeInTheDocument();
  expect(download()).toBeDisabled();
  await waitFor(()=>expect(fetcher.mock.calls.some(([url])=>url.includes("/discovery/progress?") && params(url).get("pop_year") === "Todos" && params(url).get("pop_month") === "Todos")).toBe(true));
  expect(fetcher.mock.calls.some(([url])=>url.includes("/discovery/progress?") && !params(url).has("pop_month"))).toBe(true);
  await waitFor(()=>expect(fetcher.mock.calls.some(([url])=>url.includes("/helix?") && params(url).get("pop_month") === "09")).toBe(true));
  await waitFor(()=>expect(fetcher.mock.calls.some(([url])=>url.includes("/comments/engine?") && params(url).get("pop_month") === "09")).toBe(true));

  // An additional ingestion occurs while the classifier is unmounted. The same SWR
  // cache still contains the previous completed progress; remount must revalidate it.
  await user.click(screen.getByRole("tab",{name:"Análisis estático"}));
  allTotal = 4;
  await user.click(screen.getByRole("tab",{name:"Análisis con LLM"}));
  const currentClassifier = within((await screen.findByLabelText("Ámbito de clasificación de comentarios")).closest("article")!);
  expect(await currentClassifier.findByText("1 pendientes")).toBeInTheDocument();
  await waitFor(()=>expect(download()).toBeEnabled());
  await user.click(download());
  await waitFor(()=>expect(fetcher.mock.calls.some(([url, init])=>url.includes("/classifier/export?") && params(url).get("pop_month") === "Todos" && params(url).get("pop_year") === "Todos" && init?.method === "POST")).toBe(true));
  await waitFor(()=>expect(download()).toBeEnabled());
  await user.selectOptions(screen.getByLabelText("Ámbito de clasificación de comentarios"),"period");
  expect(await currentClassifier.findByText("0 pendientes")).toBeInTheDocument();
  expect(download()).toBeDisabled();
  expect(await currentClassifier.findByText(/Ámbito de clasificación: 2026-06-12 – 2026-09-20/)).toBeInTheDocument();
  await user.selectOptions(screen.getByLabelText("Ámbito de clasificación de comentarios"),"all");
  await waitFor(()=>expect(download()).toBeEnabled());
  await user.upload(screen.getByLabelText("Importar ZIP de comentarios clasificados"),new File(["zip"],"response.zip",{type:"application/zip"}));
  expect(await currentClassifier.findByText("0 pendientes")).toBeInTheDocument();
  await waitFor(()=>expect(download()).toBeDisabled());
  // A manual refresh also handles data added outside this mounted view.
  await waitFor(()=>expect(screen.getByRole("button",{name:"Actualizar pendientes"})).toBeEnabled());
  allTotal = 5;
  await user.click(screen.getByRole("button",{name:"Actualizar pendientes"}));
  await waitFor(()=>expect(download()).toBeEnabled());
  expect(await currentClassifier.findByText("1 pendientes")).toBeInTheDocument();
  rerender(<SWRConfig value={{provider}}><TaxonomyStudio context={context} classificationContext={{...scope,pop_month:"10"}} onChange={async()=>{}} /></SWRConfig>);
  await waitFor(()=>expect(fetcher.mock.calls.some(([url])=>url.includes("/helix?") && params(url).get("pop_month") === "10")).toBe(true));
});

it("places semantic criteria after manual taxonomy and refreshes the selected catalog", async () => {
  const semantic = {SOURCE:{total:4,received:0,pending:4,levers:2,sublevers:4,taxonomy_fingerprint:"original"},COMPLETED:{total:2,received:0,pending:2,levers:1,sublevers:2,taxonomy_fingerprint:"manual"}};
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/semantic/export")) return Response.json({saved_path:"/Downloads/criteria.zip"});
    if (url.includes("/semantic/import")) { Object.assign(semantic.COMPLETED,{received:2,pending:0,taxonomy_fingerprint:"enriched"}); return Response.json({imported:true}); }
    if (url.includes("/progress")) return Response.json({semantic});
    if (url.includes("/instructions")) return Response.json({versions:{semantic:"v1"},semantic:"Conserva todas las categorías"});
    if (url.includes("/discovery")) return Response.json({semantic_url:"https://chatgpt.com/g/g-p-6ac743802efc81a4a705074f9fd2644b"});
    if (url.includes("/manual")) return Response.json({taxonomy:[],templates:["NONE"],exists:false});
    return Response.json(status());
  });
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  render(<SWRConfig value={{provider:()=>new Map()}}><TaxonomyStudio context={context} onChange={async()=>{}} /></SWRConfig>);
  const heading = await screen.findByRole("heading",{name:"Crear similitud semántica"});
  expect(screen.getByRole("heading",{name:"Taxonomía Manual"}).compareDocumentPosition(heading) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  expect(heading.compareDocumentPosition(screen.getByText("Comparar taxonomías")) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  await user.selectOptions(screen.getByLabelText("Taxonomía para crear criterios"),"COMPLETED");
  await user.click(screen.getByRole("button",{name:"Exportar comentarios y taxonomía para crear criterios"}));
  expect(await screen.findByText("ZIP guardado en /Downloads/criteria.zip")).toBeInTheDocument();
  expect(fetcher).toHaveBeenCalledWith(expect.stringContaining("mode=COMPLETED"),expect.objectContaining({method:"POST"}));
  await user.upload(screen.getByLabelText("Importar ZIP de criterios semánticos"),new File(["zip"],"response.zip",{type:"application/zip"}));
  expect(await screen.findByText("enriched")).toBeInTheDocument();
  expect(screen.getByText("0 pendientes")).toBeInTheDocument();
});
