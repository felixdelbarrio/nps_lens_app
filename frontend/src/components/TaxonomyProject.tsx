import { useState } from "react";
import { taxonomyRequest, type TaxonomyContext } from "../api";
import { ProjectUrlField } from "./ProjectUrlField";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";

export function TaxonomyProject({ role, context, url, disabled, canExport, onChange }: { role: "designer" | "classifier"; context: TaxonomyContext; url: string; disabled: boolean; canExport: boolean; onChange: () => Promise<void> }) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const title = role === "designer" ? "Crea Taxonomía" : "Clasifica taxonomía";
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
    <button className="primary-button" disabled={locked || !canExport} onClick={() => void perform(async () => { const result = await taxonomyRequest<{saved_path: string}>(`/discovery/${role}/export`, context, {method:"POST"}); setMessage(`ZIP guardado en ${result.saved_path}`); })}>{role === "designer" ? "Exportar comentarios para crear taxonomía" : "Exportar comentarios pendientes"}</button>
    <label>{role === "designer" ? "Importar ZIP de taxonomía" : "Importar ZIP de comentarios clasificados"}<input type="file" accept=".zip,application/zip" disabled={locked} onChange={e => { const file = e.currentTarget.files?.[0]; e.currentTarget.value = ""; if (file) void perform(async () => { const body = new FormData(); body.append("file", file); const result = await taxonomyRequest<{ stage: string; received?: number; batches?: number }>(`/discovery/${role}/import`, context, {method:"POST",body}); setMessage(role === "designer" ? "Taxonomía importada. Ya puedes exportar los comentarios pendientes en Clasifica taxonomía." : result.stage === "complete" ? "Todos los lotes validados. Clasificaciones disponibles en Descubierta por LLM." : `Lotes recibidos: ${result.received}/${result.batches}. Importa los restantes para completar la clasificación.`); await onChange(); }); }} /></label>
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
