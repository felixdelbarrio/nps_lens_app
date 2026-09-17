import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { TaxonomyStudio, SnapshotSettings } from "./TaxonomyStudio";

const context = { service_origin: "Bank", service_origin_n1: "Web" };
function status() {
  return { active: "NORMALIZED", requested_active: "NORMALIZED", default: "NORMALIZED", policy: "ACTIVE_ONLY", restored: false, discovery_local_available: true, detection: { state: "PARTIAL", rows: 96, missing: 16, usable_comments: 96 }, taxonomies: [
    { mode: "SOURCE", available: true, levers: 2, sublevers: 4, coverage: 0.83 },
    { mode: "NORMALIZED", available: true, levers: 2, sublevers: 4, coverage: 0.83 },
    { mode: "COMPLETED", available: false }, { mode: "DISCOVERED", available: false }
  ] };
}
afterEach(() => vi.unstubAllGlobals());
it("generates both lenses, explores, compares and selects without regenerating", async () => {
  const state = status();
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/discovery")) return new Response(JSON.stringify({ method: "chatgpt_browser", designer_url: "https://chatgpt.com/g/designer", classifier_url: "https://chatgpt.com/g/classifier", session: "connected" }));
    if (url.includes("/generate")) { const body = JSON.parse(String(init?.body)); const item = state.taxonomies.find(t => t.mode === body.mode)!; Object.assign(item, { available: true, coverage: 1, levers: 2, sublevers: 4 }); return new Response(JSON.stringify({ cache_hit: false })); }
    if (url.includes("/settings")) { state.active = JSON.parse(String(init?.body)).active; state.requested_active = state.active; return new Response("{}"); }
    if (url.includes("/explore") || url.includes("/compare")) return new Response(JSON.stringify({ rows: [], total: 0, note: "Comparación reproducible" }));
    return new Response(JSON.stringify(state));
  });
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  render(<SWRConfig value={{ provider: () => new Map() }}><TaxonomyStudio context={context} onChange={async () => {}} /></SWRConfig>);
  await user.click(await screen.findByRole("button", { name: "Completar" }));
  expect(await screen.findByRole("button", { name: "Explorar Completada" })).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Descubrir" }));
  await user.click(await screen.findByRole("button", { name: "Explorar Descubierta" }));
  expect(await screen.findByText("Comparación reproducible")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Comparar taxonomías" }));
  await user.click(screen.getAllByRole("button", { name: "Usar como lente" })[3]);
  await waitFor(() => expect(state.active).toBe("DISCOVERED"));
  expect(fetcher.mock.calls.filter(([url]) => url.includes("/generate"))).toHaveLength(2);
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
