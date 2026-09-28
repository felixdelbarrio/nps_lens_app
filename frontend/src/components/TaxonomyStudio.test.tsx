import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { TaxonomyStudio, SnapshotSettings } from "./TaxonomyStudio";

const context = { service_origin: "Bank", service_origin_n1: "Web" };
function status() {
  return { active: "SOURCE", requested_active: "SOURCE", default: "SOURCE", policy: "ACTIVE_ONLY", restored: false, discovered_catalog_available: false, discovery_local_available: true, detection: { state: "PARTIAL", rows: 96, missing: 16, usable_comments: 96 }, taxonomies: [
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
    if (url.includes("/discovery/classifier/import")) { Object.assign(state.taxonomies[2], {available:true,coverage:1,levers:2,sublevers:4}); return new Response(JSON.stringify({stage:"complete"})); }
    if (url.includes("/discovery")) return new Response(JSON.stringify({ helix_classifier_url:"https://chatgpt.com/g/helix", designer_url: "https://chatgpt.com/g/designer", classifier_url: "https://chatgpt.com/g/classifier", session: "connected" }));
    if (url.includes("/manual")) { if (init?.method === "PUT") Object.assign(state.taxonomies[1], { available: true, coverage: 1, levers: 1, sublevers: 1 }); return new Response(JSON.stringify({ taxonomy: [{ lever: "Atención", sublevers: ["Resolución"] }] })); }
    if (url.includes("/helix")) return new Response(JSON.stringify({ready:false,taxonomies:{SOURCE:{received:0,pending:1}},pending:1}));
    if (url.includes("/settings/equivalences")) return new Response(JSON.stringify({dimensions:{"nps.Palanca":[]},available_dimensions:["nps.Palanca"]}));
    if (url.includes("/settings")) { state.active = JSON.parse(String(init?.body)).active; state.requested_active = state.active; return new Response("{}"); }
    if (url.includes("/explore") || url.includes("/compare")) return new Response(JSON.stringify({ rows: [], total: 0, note: "Comparación reproducible" }));
    return new Response(JSON.stringify(state));
  });
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  render(<SWRConfig value={{ provider: () => new Map() }}><TaxonomyStudio context={context} onChange={async () => {}} /></SWRConfig>);
  await user.click(await screen.findByRole("button", { name: "Crear / modificar Manual" }));
  await user.click(await screen.findByRole("button", { name: "Guardar Manual" }));
  expect(await screen.findByRole("button", { name: "Explorar Manual" })).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Exportar comentarios para crear taxonomía" }));
  expect(await screen.findByText(/ZIP guardado en/)).toBeInTheDocument();
  await user.upload(screen.getByLabelText("Importar ZIP de comentarios clasificados"), new File(["{}"], "result.zip", { type: "application/zip" }));
  await user.click(await screen.findByRole("button", { name: "Explorar Descubierta por LLM" }));
  expect(await screen.findByText("Comparación reproducible")).toBeInTheDocument();
  expect(screen.queryByRole("button", {name:"Usar como lente"})).not.toBeInTheDocument();
  await user.selectOptions(screen.getByLabelText("Lente activa"), "DISCOVERED");
  await waitFor(() => expect(state.active).toBe("DISCOVERED"));
  expect(fetcher.mock.calls.filter(([url]) => url.includes("/generate"))).toHaveLength(0);
});
it("persists snapshot policy and default lens", async () => {
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
  await user.selectOptions(screen.getByLabelText("Taxonomía activa por defecto"), "SOURCE");
  await waitFor(() => expect(state.default).toBe("SOURCE"));
  expect(screen.getByRole("link", { name: "Guardar snapshot local" })).toHaveAttribute("href", expect.stringContaining("/api/taxonomy/snapshot"));
});
