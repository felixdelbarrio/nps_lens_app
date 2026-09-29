import { formatVolume } from "../utils/numberFormat";
import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext } from "../api";
import { ProjectUrlField } from "./ProjectUrlField";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";

export function TaxonomyProject({ role, context, url, disabled, canExport, onChange }: { role: "designer" | "classifier"; context: TaxonomyContext; url: string; disabled: boolean; canExport: boolean; onChange: () => Promise<void> }) {
  const { data: progress, mutate } = useSWR(role === "classifier" ? taxonomyUrl("/discovery/progress", context) : null, () => taxonomyRequest<{total:number;received:number;pending:number}>("/discovery/progress", context));
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const title = role === "designer" ? "Crea Taxonomía" : "Clasifica comentarios";
  async function perform(action: () => Promise<void>) {
    setBusy(true); setMessage("");
    try { await action(); }
    catch (error) { setMessage(error instanceof Error ? error.message : "No se pudo completar la operación."); }
    finally { setBusy(false); }
  }
  const locked = disabled || busy;
  return <article className="settings-subsection">
    <h4>{title}</h4>
    <ProjectUrlField context={context} field={role === "designer" ? "designer_url" : "classifier_url"} label={`URL · ${title}`} url={url} disabled={locked} />
    <TaxonomyProjectInstructions role={role} context={context} />
    <p>{role === "designer" ? "Exporta los comentarios para diseñar la taxonomía. Importa el ZIP con el JSON estricto de categorías devuelto por el proyecto." : "Exporta únicamente comentarios pendientes con la taxonomía descubierta. Importa el ZIP con los JSON estrictos de sus clasificaciones."}</p>
    {role === "classifier" && progress ? <div role="status"><p>{formatVolume(progress.received)} de {formatVolume(progress.total)} comentarios procesados · {formatVolume(progress.pending)} pendientes.</p><progress aria-label="Progreso de clasificación" value={progress.received} max={progress.total || 1} /><p>{!progress.total ? "Importa comentarios NPS desde Ingesta." : !canExport ? "Crea e importa primero la taxonomía." : progress.pending ? "Exporta los pendientes, procesa todos los lotes del ZIP y vuelve a importar la respuesta. Cada importación se acumula; no repite los comentarios ya procesados." : "Clasificación completa. Puedes seleccionar Descubierta por LLM como lente activa."}</p></div> : null}
    <button className="primary-button" disabled={locked || !canExport || (role === "classifier" && progress?.pending === 0)} onClick={() => void perform(async () => { const result = await taxonomyRequest<{saved_path: string}>(`/discovery/${role}/export`, context, {method:"POST"}); setMessage(`ZIP guardado en ${result.saved_path}`); })}>{role === "designer" ? "Exportar comentarios para crear taxonomía" : "Exportar comentarios pendientes"}</button>
    <label>{role === "designer" ? "Importar ZIP de taxonomía" : "Importar ZIP de comentarios clasificados"}<input type="file" accept=".zip,application/zip" disabled={locked} onChange={e => { const file = e.currentTarget.files?.[0]; e.currentTarget.value = ""; if (file) void perform(async () => { const body = new FormData(); body.append("file", file); const result = await taxonomyRequest<{ progress?: {total:number;received:number;pending:number} }>(`/discovery/${role}/import`, context, {method:"POST",body}); setMessage(role === "designer" ? "Taxonomía importada. Ya puedes exportar los comentarios pendientes en Clasifica comentarios." : `Importación validada. ${result.progress?.received} de ${result.progress?.total} comentarios procesados; ${result.progress?.pending} pendientes. Las clasificaciones ya están disponibles en Descubierta por LLM.`); await mutate(); await onChange(); }); }} /></label>
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
