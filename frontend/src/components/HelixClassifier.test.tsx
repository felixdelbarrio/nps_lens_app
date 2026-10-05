import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { HelixClassifier } from "./HelixClassifier";
import { ClassificationEngineControl } from "./ClassificationEngineControl";
afterEach(() => vi.unstubAllGlobals());
it("imports independent taxonomy assignments without method selectors and autosaves URL", async () => {
  const fetcher = vi.fn(async (url: string) => {
    if (url.includes("/instructions")) return new Response(JSON.stringify({versions:{designer:"1",classifier:"1",helix:"2",normalizer:"1"},helix:"Reglas Helix"}));
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
  vi.stubGlobal("fetch", vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/instructions")) return Response.json({versions:{helix:"2"},helix:"Reglas Helix"});
    if (init?.method === "PUT") engine = new URL(url, "http://localhost").searchParams.get("engine")!;
    return new Response(JSON.stringify({selected_engine:engine,ready,active:"SOURCE"}));
  }));
  const user = userEvent.setup();
  render(<SWRConfig value={{provider: () => new Map()}}><ClassificationEngineControl kind="helix" context={{service_origin:"Bank"}} disabled={false} onChange={async () => {}} /></SWRConfig>);
  await screen.findByText(/Marco: Taxonomía Original/);
  const selector = screen.getByLabelText("Método de vinculación");
  expect(screen.queryByLabelText("Importar ZIP de vínculos evaluados")).not.toBeInTheDocument();
  if (ready) {
    expect(selector).toBeEnabled();
    await user.selectOptions(selector, "llm");
    await waitFor(()=>expect(selector).toHaveValue("llm"));
    expect(screen.getByLabelText("Importar ZIP de vínculos evaluados")).toBeInTheDocument();
    await user.selectOptions(selector, "rules");
    await waitFor(()=>expect(selector).toHaveValue("rules"));
    expect(screen.queryByLabelText("Importar ZIP de vínculos evaluados")).not.toBeInTheDocument();
  } else expect(screen.getByRole("option", {name:"LLM semántico"})).toBeDisabled();
});
it("blocks unavailable LLM activation and explains pending classifications", async () => {
  vi.stubGlobal("fetch", vi.fn(async (url: string) => url.includes("/instructions") ? Response.json({versions:{helix:"2"},helix:"Reglas Helix"}) : new Response(JSON.stringify({selected_engine:"rules",ready:false,reason:"Ámbito pendiente",active:"SOURCE",received:1,total:2}))));
  render(<SWRConfig value={{provider: () => new Map()}}><ClassificationEngineControl kind="helix" context={{service_origin:"Bank"}} disabled={false} onChange={async () => {}} /></SWRConfig>);
  await screen.findByText(/Marco: Taxonomía Original/);
  expect(screen.getByText(/Ámbito pendiente/)).toBeInTheDocument();
  expect(screen.getByRole("option", {name:"LLM semántico"})).toBeDisabled();
  expect(screen.getByLabelText("Método de vinculación")).toHaveValue("rules");
});
it("keeps unavailable LLM selected and permits switching back to rules", async () => {
  let selected_engine = "llm";
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/instructions")) return Response.json({versions:{helix:"2"},helix:"Reglas Helix"});
    if (init?.method === "PUT") selected_engine = new URL(url, "http://localhost").searchParams.get("engine")!;
    return new Response(JSON.stringify({selected_engine,ready:false,reason:"Ámbito pendiente",active:"SOURCE",received:1,total:2}));
  });
  vi.stubGlobal("fetch", fetcher);
  const onChange = vi.fn(async () => {});
  render(<SWRConfig value={{provider: () => new Map()}}><ClassificationEngineControl kind="helix" context={{service_origin:"Bank"}} disabled={false} onChange={onChange} /></SWRConfig>);
  await screen.findByText(/Ámbito pendiente/);
  const selector = screen.getByLabelText("Método de vinculación");
  expect(selector).toHaveValue("llm");
  expect(selector).toBeEnabled();
  expect(screen.queryByLabelText("Importar ZIP de vínculos evaluados")).not.toBeInTheDocument();
  await userEvent.selectOptions(selector, "rules");
  await waitFor(() => expect(selector).toHaveValue("rules"));
  expect(selected_engine).toBe("rules");
  expect(screen.getByText(/Ámbito pendiente/)).toBeInTheDocument();
  expect(onChange).toHaveBeenCalledOnce();
});

it("exports only linking when categories are complete and displays their fingerprint", async () => {
  const fetcher = vi.fn(async (url: string) => {
    if (url.includes("/instructions")) return Response.json({versions:{helix:"2"},helix:"Reglas Helix"});
    if (url.includes("/export")) return Response.json({saved_paths:[],saved_directory:null,batches:0});
    return Response.json({mode:"DISCOVERED",taxonomy_fingerprint:"abcdef123456",pending:0,link_pending:2,total:2,received:2});
  });
  vi.stubGlobal("fetch", fetcher);
  render(<SWRConfig value={{provider: () => new Map()}}><HelixClassifier context={{service_origin:"Bank"}} mode="DISCOVERED" url="" disabled={false} onlyLinking onChange={async () => {}} /></SWRConfig>);
  await screen.findByText("abcdef12");
  const button = screen.getByRole("button",{name:"Exportar linking reutilizando categorías"});
  expect(button).toBeEnabled();
  expect(screen.queryByText("Vínculos NPS")).not.toBeInTheDocument();
  await userEvent.click(button);
  await waitFor(() => expect(fetcher).toHaveBeenCalledWith(expect.stringMatching(/\/helix\/export\?.*only_linking=true/), expect.objectContaining({method:"POST"})));
});

it("requires LLM for discovered comments and never offers rules", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json({active:"DISCOVERED",selected_engine:"llm",base_available:false,ready:false,total:3,received:0,reason:"Clasificación pendiente"})));
  render(<SWRConfig value={{provider: () => new Map()}}><ClassificationEngineControl kind="comments" context={{service_origin:"Bank"}} disabled={false} onChange={async () => {}} /></SWRConfig>);
  await screen.findByText("Clasificación LLM requerida para DISCOVERED.");
  expect(screen.getByLabelText("Clasificación de comentarios")).toHaveValue("llm");
  expect(screen.queryByRole("option", {name:"Clasificación base"})).not.toBeInTheDocument();
});

it("selects available base and LLM comment classifications using the existing endpoint", async () => {
  let selected_engine = "rules";
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (init?.method === "PUT") selected_engine = new URL(url,"http://localhost").searchParams.get("engine")!;
    return Response.json({active:"COMPLETED",selected_engine,base_available:true,ready:true,total:3,received:3});
  });
  vi.stubGlobal("fetch",fetcher);
  render(<SWRConfig value={{provider: () => new Map()}}><ClassificationEngineControl kind="comments" context={{service_origin:"Bank"}} disabled={false} onChange={async () => {}} /></SWRConfig>);
  await screen.findByText(/Marco: Taxonomía Manual/);
  const selector = screen.getByLabelText("Clasificación de comentarios");
  await userEvent.selectOptions(selector,"llm");
  await waitFor(() => expect(selector).toHaveValue("llm"));
  await userEvent.selectOptions(selector,"rules");
  await waitFor(() => expect(selector).toHaveValue("rules"));
  expect(fetcher).toHaveBeenCalledWith(expect.stringContaining("/comments/engine?"), expect.objectContaining({method:"PUT"}));
});
