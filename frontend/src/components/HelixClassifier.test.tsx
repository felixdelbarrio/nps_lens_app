import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { HelixClassifier } from "./HelixClassifier";

afterEach(() => vi.unstubAllGlobals());
it("blocks LLM until import completes and activates the selected method", async () => {
  let ready = false;
  let engine = "rules";
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/instructions")) return new Response(JSON.stringify({version:"1",helix:"Reglas Helix"}));
    if (url.includes("/engine")) {
      if (init?.method === "PUT") engine = new URL(url, "http://localhost").searchParams.get("engine")!;
      return new Response(JSON.stringify({engine}));
    }
    if (url.includes("/import")) ready = true;
    return new Response(JSON.stringify({ready, received:ready ? 2 : 0,pending:ready ? 0 : 2,engine}));
  });
  vi.stubGlobal("fetch", fetcher);
  const onChange = vi.fn(async () => {});
  const user = userEvent.setup();
  render(<SWRConfig value={{provider: () => new Map()}}><HelixClassifier context={{service_origin:"Bank"}} modes={["SOURCE"]} active="SOURCE" url="https://chatgpt.com/g/test" disabled={false} onChange={onChange} /></SWRConfig>);
  const toggle = screen.getByRole("switch");
  expect(toggle).toBeDisabled();
  await user.selectOptions(screen.getByLabelText("Método causal para Helix"), "broken_journeys");
  await user.upload(screen.getByLabelText("Importar ZIP Helix de respuesta"), new File(["ZIP"], "response.zip", {type:"application/zip"}));
  await waitFor(() => expect(toggle).toBeEnabled());
  await user.click(toggle);
  await waitFor(() => expect(toggle).toBeChecked());
  expect(onChange).toHaveBeenLastCalledWith("broken_journeys");
  expect(fetcher.mock.calls.some(([url, init]) => url.includes("method=broken_journeys") && url.includes("engine=llm") && init?.method === "PUT")).toBe(true);
});

it("allows returning to rules after the active corpus becomes stale", async () => {
  let engine = "llm";
  vi.stubGlobal("fetch", vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/instructions")) return new Response(JSON.stringify({version:"1",helix:"Reglas Helix"}));
    if (url.includes("/engine")) {
      if (init?.method === "PUT") engine = "rules";
      return new Response(JSON.stringify({engine}));
    }
    return new Response(JSON.stringify({detail:"Taxonomía desactualizada"}),{status:409});
  }));
  const user = userEvent.setup();
  render(<SWRConfig value={{provider: () => new Map()}}><HelixClassifier context={{service_origin:"Bank"}} modes={["SOURCE"]} active="SOURCE" url="https://chatgpt.com/g/test" disabled={false} onChange={async () => {}} /></SWRConfig>);
  const toggle = screen.getByRole("switch");
  await waitFor(() => expect(toggle).toBeChecked());
  expect(toggle).toBeEnabled();
  await user.click(toggle);
  await waitFor(() => expect(toggle).not.toBeChecked());
});
