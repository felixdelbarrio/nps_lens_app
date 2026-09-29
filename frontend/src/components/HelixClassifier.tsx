import { formatVolume, formatPercentage } from "../utils/numberFormat";
import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";
import { ProjectUrlField } from "./ProjectUrlField";
import { TAXONOMY_NAMES } from "../utils/taxonomy";

type Status = { total:number; received:number; classified:number; unassigned:number; coverage:number; links:number; categories:Array<{lever:string;sublever:string;count:number}>; pending: number; taxonomies: Partial<Record<TaxonomyMode, {received:number;pending:number}>> };
export function HelixClassifier({ context, mode, url, disabled, onChange }: { context: TaxonomyContext; mode: TaxonomyMode; url: string; disabled: boolean; onChange: () => Promise<void> }) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const { data, error, mutate } = useSWR([taxonomyUrl("/helix", context), mode], () => taxonomyRequest<Status>("/helix", context), { shouldRetryOnError: false });
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
    <p>Lente activa: {TAXONOMY_NAMES[mode]}. El método causal se elige en Causalidad. Cada lente conserva sus propias clasificaciones.</p>
    {data ? <><div className="metric-grid">{[["Incidencias",data.total],["Procesadas",data.received],["Con categoría",data.classified],["Sin encaje",data.unassigned],["Pendientes",data.pending],["Vínculos NPS",data.links]].map(([label,value]) => <div className="metric-card" key={label}><span>{label}</span><strong>{formatVolume(value)}</strong></div>)}</div><p>Cobertura temática: {formatPercentage(data.coverage)}. Sin encaje significa procesada sin evidencia suficiente para asignar una categoría.</p>
    {data.categories?.length ? <details><summary>Distribución de incidencias</summary><div className="table-scroll"><table><thead><tr><th>Palanca</th><th>Subpalanca</th><th>Incidencias</th></tr></thead><tbody>{data.categories.map(row => <tr key={`${row.lever}/${row.sublever}`}><td>{row.lever}</td><td>{row.sublever}</td><td>{formatVolume(row.count)}</td></tr>)}</tbody></table></div></details> : null}</> : null}
    {error ? <p role="status">{error.message}</p> : null}
    <div className="inline-actions"><button className="primary-button" disabled={locked || !data?.pending} onClick={() => void run(async () => { const result = await taxonomyRequest<{ saved_path: string }>("/helix/export", context, { method: "POST" }); setMessage(`ZIP guardado en ${result.saved_path}`); })}>Exportar ZIP Helix a Descargas</button>
    <label>Importar ZIP Helix de respuesta<input type="file" accept=".zip,application/zip" disabled={locked} onChange={e => { const file = e.currentTarget.files?.[0]; e.currentTarget.value = ""; if (file) void run(async () => { const body = new FormData(); body.append("file", file); await taxonomyRequest("/helix/import", context, { method: "POST", body }); setMessage("Clasificaciones Helix validadas e importadas."); }); }} /></label></div>
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
