import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { HelixClassifier } from "./HelixClassifier";
import { ClassificationEngineControl } from "./ClassificationEngineControl";
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
  const url = screen.getByLabelText("URL · Clasifica incidencias");
  await user.clear(url); await user.type(url,"https://chatgpt.com/g/updated"); await user.tab();
  await waitFor(() => expect(fetcher).toHaveBeenCalledWith(expect.stringContaining("/discovery"), expect.objectContaining({method:"PUT",body:JSON.stringify({helix_classifier_url:"https://chatgpt.com/g/updated"})})));
  await user.upload(screen.getByLabelText("Importar ZIP de incidencias clasificadas"), new File(["ZIP"], "response.zip", {type:"application/zip"}));
  await waitFor(() => expect(onChange).toHaveBeenCalled());
});
it.each([false,true])("allows activation only when ready=%s", async ready => {
  let engine = "rules";
  vi.stubGlobal("fetch", vi.fn(async (_url: string, init?: RequestInit) => {
    if (init?.method === "PUT") engine = "llm";
    return new Response(JSON.stringify({selected_engine:engine,ready,active:"SOURCE"}));
  }));
  const user = userEvent.setup();
  render(<SWRConfig value={{provider: () => new Map()}}><ClassificationEngineControl kind="helix" context={{service_origin:"Bank"}} disabled={false} onChange={async () => {}} /></SWRConfig>);
  await screen.findByText(/Lente: Taxonomía Original/);
  const toggle = screen.getByRole("switch");
  if (ready) {expect(toggle).toBeEnabled();await user.click(toggle);await waitFor(()=>expect(toggle).toBeChecked());}
  else expect(toggle).toBeDisabled();
});
it("blocks unavailable LLM activation without showing a pending rules analysis", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({selected_engine:"rules",ready:false,reason:"Ámbito pendiente",active:"SOURCE",received:1,total:2}))));
  render(<SWRConfig value={{provider: () => new Map()}}><ClassificationEngineControl kind="helix" context={{service_origin:"Bank"}} disabled={false} onChange={async () => {}} /></SWRConfig>);
  await screen.findByText(/Lente: Taxonomía Original/);
  expect(screen.queryByText(/Ámbito pendiente/)).not.toBeInTheDocument();
  expect(screen.getByRole("switch")).toBeDisabled();
  expect(screen.getByRole("switch")).not.toBeChecked();
});
it("keeps unavailable LLM checked and permits switching back to rules", async () => {
  let selected_engine = "llm";
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (init?.method === "PUT") selected_engine = new URL(url, "http://localhost").searchParams.get("engine")!;
    return new Response(JSON.stringify({selected_engine,ready:false,reason:"Ámbito pendiente",active:"SOURCE",received:1,total:2}));
  });
  vi.stubGlobal("fetch", fetcher);
  const onChange = vi.fn(async () => {});
  render(<SWRConfig value={{provider: () => new Map()}}><ClassificationEngineControl kind="helix" context={{service_origin:"Bank"}} disabled={false} onChange={onChange} /></SWRConfig>);
  await screen.findByText(/Ámbito pendiente/);
  const toggle = screen.getByRole("switch", {name:"Usar vinculación semántica LLM"});
  expect(toggle).toBeChecked();
  expect(toggle).toBeEnabled();
  await userEvent.click(toggle);
  await waitFor(() => expect(toggle).not.toBeChecked());
  expect(selected_engine).toBe("rules");
  expect(screen.queryByText(/Ámbito pendiente/)).not.toBeInTheDocument();
  expect(onChange).toHaveBeenCalledOnce();
});
