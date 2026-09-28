import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";
import { TAXONOMY_NAMES } from "../utils/taxonomy";

type Status = { ready: boolean; received: number; pending: number; engine: "rules" | "llm" };
const METHODS = { palanca_touchpoint: "Palanca", domain_touchpoint: "Subpalanca", bbva_source_service_n2: "Helix Source Service N2", broken_journeys: "Journey roto", executive_journeys: "Journey de detracción" };
export function HelixClassifier({ context, modes, active, url, disabled, onChange }: { context: TaxonomyContext; modes: TaxonomyMode[]; active: TaxonomyMode; url: string; disabled: boolean; onChange: (method?: string) => Promise<void> }) {
  const [mode, setMode] = useState(active);
  const [method, setMethod] = useState("domain_touchpoint");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const selected = { ...context, mode, method };
  const { data, error, mutate } = useSWR(taxonomyUrl("/helix", selected), () => taxonomyRequest<Status>("/helix", selected), { shouldRetryOnError: false });
  const { data: engineState, mutate: mutateEngine } = useSWR(taxonomyUrl("/helix/engine", context), () => taxonomyRequest<{ engine: "rules" | "llm" }>("/helix/engine", context));
  async function run(action: () => Promise<void>, nextMethod?: string) {
    setBusy(true); setMessage("");
    try { await action(); await Promise.all([mutate(), mutateEngine()]); await onChange(nextMethod); }
    catch (error) { setMessage(error instanceof Error ? error.message : "No se pudo completar la operación."); }
    finally { setBusy(false); }
  }
  const locked = disabled || busy;
  return <article className="settings-subsection"><h3>Helix Classifier</h3>
    <a href={url} target="_blank" rel="noreferrer">Abrir Helix Classifier</a>
    <TaxonomyProjectInstructions role="helix" context={context} />
    <div className="field-grid"><label>Taxonomía para Helix<select value={mode} disabled={locked} onChange={e => setMode(e.target.value as TaxonomyMode)}>{modes.map(value => <option key={value} value={value}>{TAXONOMY_NAMES[value]}</option>)}</select></label>
    <label>Método causal para Helix<select value={method} disabled={locked} onChange={e => setMethod(e.target.value)}>{Object.entries(METHODS).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label></div>
    <p>Exporta las incidencias pendientes junto con la taxonomía y los comentarios de contexto. Importa los lotes de respuesta antes de activar LLM.</p>
    {data ? <p>{data.received} incidencias clasificadas · {data.pending} pendientes</p> : null}
    {error ? <p role="status">{error.message}</p> : null}
    <div className="inline-actions"><button className="primary-button" disabled={locked || !data?.pending} onClick={() => void run(async () => { const result = await taxonomyRequest<{ saved_path: string }>("/helix/export", selected, { method: "POST" }); setMessage(`ZIP guardado en ${result.saved_path}`); })}>Exportar ZIP Helix a Descargas</button>
    <label>Importar ZIP Helix de respuesta<input type="file" accept=".zip,application/zip" disabled={locked} onChange={e => { const file = e.currentTarget.files?.[0]; e.currentTarget.value = ""; if (file) void run(async () => { const body = new FormData(); body.append("file", file); await taxonomyRequest("/helix/import", selected, { method: "POST", body }); setMessage("Respuesta Helix validada e importada."); }); }} /></label></div>
    <label className="switch-field"><input type="checkbox" role="switch" checked={engineState?.engine === "llm"} disabled={locked || (!data?.ready && engineState?.engine !== "llm")} onChange={e => { const engine = e.target.checked ? "llm" : "rules"; void run(async () => { await taxonomyRequest("/helix/engine", { ...selected, engine }, { method: "PUT" }); setMessage(engine === "llm" ? "Causalidad LLM activada con esta taxonomía y método." : "Causalidad por reglas de negocio activada."); }, engine === "llm" ? method : undefined); }} />Usar causalidad mediante LLM</label>
    {!data?.ready ? <p className="field-hint">Se requiere la respuesta completa y vigente para esta taxonomía y método.</p> : null}
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
