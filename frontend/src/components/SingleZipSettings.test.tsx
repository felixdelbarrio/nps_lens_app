import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SWRConfig } from "swr";
import { afterEach, expect, it, vi } from "vitest";
import { SingleZipSettings } from "./SingleZipSettings";
import { TaxonomyStudio } from "./TaxonomyStudio";

const defaults = {
  single_zip_enabled: false, classifier_single_zip_url: "", helix_single_zip_url: "",
  designer_url: "", semantic_url: "",
  classifier_url: "https://chatgpt.com/g/original-comments", helix_classifier_url: "https://chatgpt.com/g/original-helix"
};
afterEach(() => vi.unstubAllGlobals());

it("switches the existing exchange panels while settings only persist the toggle", async () => {
  let settings = { ...defaults };
  const fetcher = vi.fn(async (url: string, init?: RequestInit) => {
    if (url.includes("/instructions")) return Response.json({
      versions: { classifier:"old",helix:"old",designer:"old",classifier_single_zip:"new",helix_single_zip:"new" },
      classifier:"Comentarios numerados.",helix:"Incidencias numeradas.",designer:"Taxonomía.",
      classifier_single_zip:"Comentarios por entregas: espera Sí.",helix_single_zip:"Incidencias por entregas: espera Sí."
    });
    if (url.includes("/discovery/progress")) return Response.json({total:2,received:0,pending:2,multiple:0,designer:{total:2,received:0,pending:2}});
    if (url.includes("/discovery")) {
      if (init?.method === "PUT") settings = { ...settings, ...JSON.parse(init.body as string) };
      return Response.json(settings);
    }
    if (url.includes("/engine")) return Response.json({active:"SOURCE",selected_engine:"rules",base_available:true,ready:false,total:2,received:0});
    if (url.includes("/helix")) return Response.json({total:2,received:0,pending:2,link_pending:0,multiple:0,classified:0,unassigned:0,coverage:0,mode:"SOURCE"});
    return Response.json({active:"SOURCE",active_fingerprint:"source",discovery_local_available:true,restored:false,detection:{rows:2},taxonomies:[{mode:"SOURCE",available:true,levers:1,sublevers:1}]});
  });
  vi.stubGlobal("fetch", fetcher);
  const user = userEvent.setup();
  const clipboard = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
  const context = {service_origin:"Bank"};
  render(<SWRConfig value={{provider:()=>new Map()}}><SingleZipSettings context={context} disabled={false} /><TaxonomyStudio context={context} onChange={async()=>{}} /></SWRConfig>);
  const toggle = await screen.findByRole("switch", {name:"Activar ZIP único"});
  await waitFor(()=>expect(toggle).toBeEnabled());
  expect(toggle).not.toBeChecked();
  const configuration = within(toggle.closest("article")!);
  expect(configuration.queryByRole("textbox")).not.toBeInTheDocument();
  expect(configuration.queryByRole("button")).not.toBeInTheDocument();
  await user.click(await screen.findByRole("tab",{name:"Análisis con LLM"}));
  const comments = ()=>screen.getByLabelText("URL · Clasifica comentarios");
  const helix = ()=>screen.getByLabelText("URL · Clasifica incidencias");
  await waitFor(()=>expect(comments()).toHaveValue(defaults.classifier_url));
  expect(helix()).toHaveValue(defaults.helix_classifier_url);
  await user.click(screen.getByRole("button",{name:"Copiar instrucciones de Clasifica comentarios"}));
  expect(clipboard).toHaveBeenLastCalledWith("Comentarios numerados.");
  await user.click(toggle);
  await waitFor(()=>expect(comments()).toHaveValue(""));
  expect(helix()).toHaveValue("");
  expect(screen.getByRole("button",{name:"Descargar ZIP único de comentarios pendientes"})).toBeInTheDocument();
  expect(screen.getByRole("button",{name:"Descargar ZIP único de incidencias pendientes"})).toBeInTheDocument();
  await user.type(comments(),"https://chatgpt.com/g/new-comments");
  await user.tab();
  await waitFor(()=>expect(settings.classifier_single_zip_url).toBe("https://chatgpt.com/g/new-comments"));
  expect(settings.classifier_url).toBe(defaults.classifier_url);
  await user.click(screen.getByRole("button",{name:"Copiar instrucciones de Clasifica comentarios"}));
  expect(clipboard).toHaveBeenLastCalledWith("Comentarios por entregas: espera Sí.");
  await user.click(screen.getByRole("button",{name:"Copiar instrucciones de Clasifica incidencias"}));
  expect(clipboard).toHaveBeenLastCalledWith("Incidencias por entregas: espera Sí.");
  await user.click(toggle);
  await waitFor(()=>expect(comments()).toHaveValue(defaults.classifier_url));
  expect(helix()).toHaveValue(defaults.helix_classifier_url);
  expect(screen.getByRole("button",{name:"Descargar todos los ZIP de comentarios pendientes"})).toBeInTheDocument();
  await user.click(toggle);
  await waitFor(()=>expect(comments()).toHaveValue("https://chatgpt.com/g/new-comments"));
});
