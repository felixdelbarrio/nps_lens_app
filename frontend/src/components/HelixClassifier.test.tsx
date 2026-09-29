import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { HelixClassifier } from "./HelixClassifier";
import { CausalEngineControl } from "./CausalEngineControl";
afterEach(() => vi.unstubAllGlobals());
it("imports independent taxonomy assignments without method selectors and autosaves URL", async () => {
  const fetcher = vi.fn(async (url: string) => {
    if (url.includes("/instructions")) return new Response(JSON.stringify({version:"2",helix:"Reglas Helix"}));
    return new Response(JSON.stringify({pending:2,taxonomies:{SOURCE:{received:0,pending:1},COMPLETED:{received:0,pending:1}}}));
  });
  vi.stubGlobal("fetch", fetcher);
  const onChange = vi.fn(async () => {});
  const user = userEvent.setup();
  render(<SWRConfig value={{provider: () => new Map()}}><HelixClassifier context={{service_origin:"Bank"}} mode="SOURCE" url="https://chatgpt.com/g/test" disabled={false} onChange={onChange} /></SWRConfig>);
  expect(screen.queryByRole("switch")).not.toBeInTheDocument();
  expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
  const url = screen.getByLabelText("URL · Helix Classifier");
  await user.clear(url); await user.type(url,"https://chatgpt.com/g/updated"); await user.tab();
  await waitFor(() => expect(fetcher).toHaveBeenCalledWith(expect.stringContaining("/discovery"), expect.objectContaining({method:"PUT",body:JSON.stringify({helix_classifier_url:"https://chatgpt.com/g/updated"})})));
  await user.upload(screen.getByLabelText("Importar ZIP Helix de respuesta"), new File(["ZIP"], "response.zip", {type:"application/zip"}));
  await waitFor(() => expect(onChange).toHaveBeenCalled());
});
it.each([false,true])("allows activation only when ready=%s", async ready => {
  let engine = "rules";
  vi.stubGlobal("fetch", vi.fn(async (_url: string, init?: RequestInit) => {
    if (init?.method === "PUT") engine = "llm";
    return new Response(JSON.stringify({engine,ready,active:"SOURCE"}));
  }));
  const user = userEvent.setup();
  render(<SWRConfig value={{provider: () => new Map()}}><CausalEngineControl context={{service_origin:"Bank"}} disabled={false} onChange={async () => {}} /></SWRConfig>);
  await screen.findByText(/Lente: Original/);
  const toggle = screen.getByRole("switch");
  if (ready) {expect(toggle).toBeEnabled();await user.click(toggle);await waitFor(()=>expect(toggle).toBeChecked());}
  else expect(toggle).toBeDisabled();
});
it("allows returning to rules after classifications become stale", async () => {
  let engine = "llm";
  vi.stubGlobal("fetch", vi.fn(async (_url: string, init?: RequestInit) => {
    if (init?.method === "PUT") engine = "rules";
    return new Response(JSON.stringify({engine,ready:false,reason:"Taxonomía desactualizada",active:"SOURCE"}));
  }));
  const user = userEvent.setup();
  render(<SWRConfig value={{provider: () => new Map()}}><CausalEngineControl context={{service_origin:"Bank"}} disabled={false} onChange={async () => {}} /></SWRConfig>);
  const toggle = screen.getByRole("switch");
  await waitFor(() => expect(toggle).toBeChecked());
  expect(toggle).toBeEnabled();
  await user.click(toggle);
  await waitFor(() => expect(toggle).not.toBeChecked());
});
