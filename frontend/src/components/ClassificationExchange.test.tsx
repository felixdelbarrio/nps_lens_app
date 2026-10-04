import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { TaxonomyProject } from "./TaxonomyProject";
import { HelixClassifier } from "./HelixClassifier";

const context = { service_origin: "Bank" };
afterEach(() => vi.unstubAllGlobals());

it.each([
  ["classifier", 1], ["classifier", 0],
  ["helix", 1], ["helix", 0],
] as const)("%s preserves imported progress with pending=%s", async (role, pending) => {
  let imported = false;
  const calls: string[] = [];
  const fetcher = vi.fn(async (url: string) => {
    if (url.includes("/instructions")) return Response.json({ versions: {designer:"3",classifier:"3",helix:"3",normalizer:"3"}, [role]: "Reglas" });
    const counts = () => ({ total: 2, received: imported ? 2 - pending : 0, pending: imported ? pending : 2 });
    if (url.includes("/import")) {
      imported = true; calls.push("import");
      return Response.json(role === "helix" ? counts() : { progress: counts() });
    }
    if (url.includes("/export")) {
      calls.push("export");
      return Response.json({ saved_paths: ["/Downloads/series/1_2_comentarios.zip", "/Downloads/series/2_2_comentarios.zip"], saved_directory: "/Downloads/series", batches: 2 });
    }
    calls.push("refresh");
    return Response.json({ ...counts(), multiple: 0, classified: 0, unassigned: 0, coverage: 0, links: 0, categories: [], designer: counts() });
  });
  vi.stubGlobal("fetch", fetcher);
  const onChange = vi.fn(async () => {});
  render(<SWRConfig value={{ provider: () => new Map(), dedupingInterval: 0 }}>
    {role === "classifier" ? <TaxonomyProject role="classifier" context={context} url="" disabled={false} canExport onChange={onChange} /> : <HelixClassifier context={context} mode="SOURCE" url="" disabled={false} onChange={onChange} />}
  </SWRConfig>);
  await screen.findByRole("progressbar");
  await userEvent.setup().click(screen.getByRole("button", { name: role === "classifier" ? "Descargar todos los ZIP de comentarios pendientes" : "Descargar todos los ZIP de incidencias pendientes" }));
  expect(await screen.findByText("1_2_comentarios.zip")).toBeInTheDocument();
  expect(screen.getByText("2_2_comentarios.zip")).toBeInTheDocument();
  await userEvent.setup().upload(screen.getByLabelText(role === "classifier" ? "Importar ZIP de comentarios clasificados" : "Importar ZIP de incidencias clasificadas"), new File(["ZIP"], "result.zip", { type: "application/zip" }));
  await waitFor(() => expect(onChange).toHaveBeenCalled());
  expect(calls.filter(c => c === "export")).toHaveLength(1);
  expect(calls.indexOf("export")).toBeLessThan(calls.indexOf("import"));
  expect(await screen.findByText(pending ? /Continúa con los ZIP que ya has descargado/ : /Clasificación completa. No quedan pendientes/)).toBeInTheDocument();
  expect(screen.getByText("2_2_comentarios.zip")).toBeInTheDocument();
  await waitFor(() => expect(screen.getByLabelText(role === "classifier" ? "Importar ZIP de comentarios clasificados" : "Importar ZIP de incidencias clasificadas")).toBeEnabled());
  expect(screen.getByRole("progressbar")).toHaveAttribute("value", String(2 - pending));
  expect(screen.getByRole("button", { name: role === "classifier" ? "Descargar todos los ZIP de comentarios pendientes" : "Descargar todos los ZIP de incidencias pendientes" })).toHaveProperty("disabled", !pending);
});

it("refreshes locally resolved empty comments without showing a nonexistent ZIP", async () => {
  let exported = false;
  vi.stubGlobal("fetch", vi.fn(async (url: string) => {
    if (url.includes("/instructions")) return Response.json({ versions: {designer:"3",classifier:"3",helix:"3",normalizer:"3"}, classifier: "Reglas" });
    if (url.includes("/export")) { exported = true; return Response.json({ saved_paths: [], saved_directory: null, stage: "complete", batches: 0 }); }
    return Response.json({ total: 2, received: exported ? 2 : 0, pending: exported ? 0 : 2, multiple: 0 });
  }));
  render(<SWRConfig value={{ provider: () => new Map() }}><TaxonomyProject role="classifier" context={context} url="" disabled={false} canExport onChange={async () => {}} /></SWRConfig>);
  await screen.findByRole("progressbar");
  await userEvent.setup().click(screen.getByRole("button", { name: "Descargar todos los ZIP de comentarios pendientes" }));
  expect(await screen.findByText("Clasificación completa. No quedan pendientes.")).toBeInTheDocument();
  await waitFor(() => expect(screen.getByRole("progressbar")).toHaveAttribute("value", "2"));
  expect(screen.queryByText(/guardado en null/)).not.toBeInTheDocument();
});
