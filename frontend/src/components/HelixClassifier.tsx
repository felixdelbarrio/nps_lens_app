import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";
import { ProjectUrlField } from "./ProjectUrlField";
import { TAXONOMY_NAMES } from "../utils/taxonomy";

type Status = { pending: number; taxonomies: Partial<Record<TaxonomyMode, {received:number;pending:number}>> };
export function HelixClassifier({ context, modes, url, disabled, onChange }: { context: TaxonomyContext; modes: TaxonomyMode[]; url: string; disabled: boolean; onChange: () => Promise<void> }) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const { data, error, mutate } = useSWR([taxonomyUrl("/helix", context), modes.join(",")], () => taxonomyRequest<Status>("/helix", context), { shouldRetryOnError: false });
  async function run(action: () => Promise<void>) {
    setBusy(true); setMessage("");
    try { await action(); await mutate(); await onChange(); }
    catch (error) { setMessage(error instanceof Error ? error.message : "No se pudo completar la operación."); }
    finally { setBusy(false); }
  }
  const locked = disabled || busy;
  return <article className="settings-subsection"><h3>Helix Classifier</h3>
    <ProjectUrlField context={context} field="helix_classifier_url" label="URL · Helix Classifier" url={url} disabled={locked} />
    <TaxonomyProjectInstructions role="helix" context={context} />
    <p>Taxonomías incluidas: {modes.map(mode => TAXONOMY_NAMES[mode]).join(", ") || "ninguna"}. La selección se realiza en Taxonomía. El método causal se elige en Causalidad.</p>
    <p>Cada incidencia conserva una pareja Palanca/Subpalanca independiente para cada taxonomía seleccionada.</p>
    {data ? <ul>{Object.entries(data.taxonomies).map(([mode, counts]) => <li key={mode}>{TAXONOMY_NAMES[mode as TaxonomyMode]}: {counts.received} clasificadas · {counts.pending} pendientes</li>)}</ul> : null}
    {error ? <p role="status">{error.message}</p> : null}
    <div className="inline-actions"><button className="primary-button" disabled={locked || !data?.pending} onClick={() => void run(async () => { const result = await taxonomyRequest<{ saved_path: string }>("/helix/export", context, { method: "POST" }); setMessage(`ZIP guardado en ${result.saved_path}`); })}>Exportar ZIP Helix a Descargas</button>
    <label>Importar ZIP Helix de respuesta<input type="file" accept=".zip,application/zip" disabled={locked} onChange={e => { const file = e.currentTarget.files?.[0]; e.currentTarget.value = ""; if (file) void run(async () => { const body = new FormData(); body.append("file", file); await taxonomyRequest("/helix/import", context, { method: "POST", body }); setMessage("Clasificaciones Helix validadas e importadas."); }); }} /></label></div>
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
