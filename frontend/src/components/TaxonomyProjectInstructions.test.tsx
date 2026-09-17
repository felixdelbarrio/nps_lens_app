import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";

import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";

const context = { service_origin: "Bank", service_origin_n1: "Web" };
const instructions = { version: "v-test", designer: "CREA · JSON exacto\nreglas", classifier: "CLASIFICA · todos los IDs\nreglas" };
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

function setup() {
  const fetcher = vi.fn(async () => new Response(JSON.stringify(instructions)));
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  render(<SWRConfig value={{ provider: () => new Map() }}>
    <TaxonomyProjectInstructions role="designer" context={context} />
    <TaxonomyProjectInstructions role="classifier" context={context} />
  </SWRConfig>);
  return { user, fetcher };
}

it("copies each exact server template and shares one read-only request", async () => {
  const { user, fetcher } = setup();
  const clipboard = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
  const designer = screen.getByRole("button", { name: "Copiar instrucciones de Crea Taxonomía" });
  await waitFor(() => expect(designer).toBeEnabled());
  await user.click(designer);
  expect(clipboard).toHaveBeenLastCalledWith(instructions.designer);
  await user.click(screen.getByRole("button", { name: "Copiar instrucciones de Clasifica taxonomía" }));
  expect(clipboard).toHaveBeenLastCalledWith(instructions.classifier);
  expect(screen.getAllByText(/Instrucciones copiadas/)).toHaveLength(2);
  expect(fetcher).toHaveBeenCalledTimes(1);
  expect(fetcher).toHaveBeenCalledWith(expect.stringContaining("/discovery/instructions?"), undefined);
});

it("reveals selectable text when clipboard access is denied", async () => {
  const { user } = setup();
  vi.spyOn(navigator.clipboard, "writeText").mockRejectedValue(new Error("NotAllowedError"));
  const button = screen.getByRole("button", { name: "Copiar instrucciones de Crea Taxonomía" });
  await waitFor(() => expect(button).toBeEnabled());
  await user.click(button);
  expect(await screen.findByText(/cópialo manualmente/)).toBeInTheDocument();
  const text = screen.getByRole("textbox", { name: "Instrucciones de Crea Taxonomía" });
  expect(text).toHaveValue(instructions.designer);
  expect(text.closest("details")).toHaveAttribute("open");
  expect(text).toHaveAttribute("readonly");
});

it("reports loading errors instead of copying an empty template", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response("{}", { status: 503 })));
  render(<SWRConfig value={{ provider: () => new Map(), shouldRetryOnError: false }}>
    <TaxonomyProjectInstructions role="designer" context={context} />
  </SWRConfig>);
  expect(await screen.findByRole("alert")).toHaveTextContent("No se pudieron cargar");
  expect(screen.getByRole("button", { name: /Copiar instrucciones/ })).toBeDisabled();
});
