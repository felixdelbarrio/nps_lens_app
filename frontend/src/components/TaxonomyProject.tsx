import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext } from "../api";
import { exportClassification, classificationImportMessage, type ClassificationExport } from "../utils/classificationExchange";
import { PROJECT_NAMES } from "../utils/taxonomy";
import { ExchangeFiles } from "./ExchangeFiles";
import { ProjectUrlField } from "./ProjectUrlField";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";
import { ADDITIONAL_TOPICS, ExchangeProgress, type ExchangeCounts } from "./ExchangeProgress";

type Progress = ExchangeCounts & { multiple:number; designer:ExchangeCounts & {levers:number;sublevers:number} };
type Role = "designer" | "classifier" | "normalizer";
const FIELDS = {designer:"designer_url",classifier:"classifier_url",normalizer:"normalizer_url"} as const;
const IMPORT_LABELS = {designer:"Importar ZIP de taxonomía",classifier:"Importar ZIP de comentarios clasificados",normalizer:"Importar ZIP de conceptos"};
const EXPORT_LABELS = {designer:"Exportar comentarios para crear taxonomía",classifier:"Descargar todos los ZIP de comentarios pendientes",normalizer:"Exportar comentarios para unificar conceptos"};
export function TaxonomyProject({ role, context, url, disabled, canExport, onChange }: { role: Role; context: TaxonomyContext; url: string; disabled: boolean; canExport: boolean; onChange: () => Promise<void> }) {
  const { data: progress, mutate } = useSWR(role !== "normalizer" ? taxonomyUrl("/discovery/progress", context) : null, () => taxonomyRequest<Progress>("/discovery/progress", context), {revalidateOnFocus:false});
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [exported, setExported] = useState<ClassificationExport | null>(null);
  async function perform(action: () => Promise<void>) {
    setBusy(true); setMessage("");
    try { await action(); }
    catch (error) { setMessage(error instanceof Error ? error.message : "No se pudo completar la operación."); }
    finally { setBusy(false); }
  }
  async function exportZips() {
    if (role !== "classifier") {
      const result = await taxonomyRequest<{saved_path: string}>(`/discovery/${role}/export`, context, {method:"POST"});
      setMessage(`ZIP guardado en ${result.saved_path}`);
      return;
    }
    setExported(null);
    try { setExported(await exportClassification(context, "/discovery/classifier/export")); }
    finally { await mutate(); await onChange(); }
  }
  async function importZip(file: File) {
    const body = new FormData();
    body.append("file", file);
    const result = await taxonomyRequest<{progress?: ExchangeCounts}>(`/discovery/${role}/import`, context, {method:"POST", body});
    const success = role === "designer" ? "Taxonomía importada. Ya puedes utilizarla para clasificar comentarios e incidencias." : role === "normalizer" ? "Conceptos actualizados para esta compañía." : "Importación validada. Progreso acumulado actualizado.";
    setMessage(success + (role === "classifier" && result.progress ? ` ${classificationImportMessage(result.progress.pending)}` : ""));
    await mutate();
    await onChange();
  }
  const locked = disabled || busy;
  const counts = role === "designer" ? progress?.designer : progress;
  return <article className="settings-subsection taxonomy-project">
    <div className="section-heading"><div><h3>{PROJECT_NAMES[role]}</h3><p className="secondary-copy">{role === "designer" ? "Descubre dimensiones de experiencia que expliquen las opiniones del canal." : role === "classifier" ? "Clasifica con la lente activa. La categoría principal conserva los recuentos; los temas adicionales aportan contexto." : "Propón nombres principales y alias equivalentes para la compañía seleccionada."}</p></div></div>
    {counts && role !== "normalizer" ? <ExchangeProgress counts={counts} unit="comentarios" metrics={role === "designer" ? [{label:"Palancas",value:progress!.designer.levers},{label:"Subpalancas",value:progress!.designer.sublevers}] : [{...ADDITIONAL_TOPICS,value:progress!.multiple}]} /> : null}
    <ProjectUrlField context={context} field={FIELDS[role]} label={`URL · ${PROJECT_NAMES[role]}`} url={url} disabled={locked} />
    <TaxonomyProjectInstructions role={role} context={context} />
    {role === "classifier" && progress ? <p className="field-hint">{!progress.total ? "Importa comentarios NPS desde Ingesta." : !canExport ? "Crea o selecciona primero una taxonomía." : progress.pending ? "Descarga todos los ZIP pendientes de una vez. Procesa cada archivo numerado en el Proyecto ChatGPT e importa su respuesta." : "Clasificación completa. Activa Usar clasificación LLM en los filtros de Comentarios."}</p> : null}
    <div className="exchange-actions"><button className="primary-button" disabled={locked || !canExport || (role === "classifier" && progress?.pending === 0)} onClick={() => void perform(exportZips)}>{EXPORT_LABELS[role]}</button>
    <label>{IMPORT_LABELS[role]}<input type="file" accept=".zip,application/zip" disabled={locked} onChange={e => { const file = e.currentTarget.files?.[0]; e.currentTarget.value = ""; if (file) void perform(() => importZip(file)); }} /></label></div>
    <ExchangeFiles result={exported} />
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
