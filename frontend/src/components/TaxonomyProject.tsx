import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyStatus } from "../api";
import { exportClassification, classificationImportMessage, type ClassificationExport } from "../utils/classificationExchange";
import { PROJECT_NAMES, taxonomyCounts } from "../utils/taxonomy";
import { TaxonomyDownload } from "./TaxonomyDownload";
import { ExchangeFiles } from "./ExchangeFiles";
import { ProjectUrlField } from "./ProjectUrlField";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";
import { ClassificationEngineControl } from "./ClassificationEngineControl";
import { ADDITIONAL_TOPICS, ExchangeProgress, type ExchangeCounts } from "./ExchangeProgress";

type SemanticProgress = ExchangeCounts & {levers:number;sublevers:number;taxonomy_fingerprint:string;review?:{reason:string;quotes:string[]}};
type Progress = ExchangeCounts & { semantic: Partial<Record<"SOURCE" | "COMPLETED", SemanticProgress>>; analysis_horizon?: {comment_start:string|null;comment_end:string|null;helix_start:string|null;max_days_apart:number}; mode:string; taxonomy_fingerprint:string; multiple:number; designer:ExchangeCounts & {levers:number;sublevers:number} };
type Role = "designer" | "classifier" | "normalizer" | "semantic";
const FIELDS = {semantic:"semantic_url",designer:"designer_url",classifier:"classifier_url",normalizer:"normalizer_url"} as const;
const IMPORT_LABELS = {semantic:"Importar ZIP de criterios semánticos",designer:"Importar ZIP de taxonomía",classifier:"Importar ZIP de comentarios clasificados",normalizer:"Importar ZIP de conceptos"};
const EXPORT_LABELS = {semantic:"Exportar comentarios y taxonomía para crear criterios",designer:"Exportar comentarios para crear taxonomía",classifier:"Descargar todos los ZIP de comentarios pendientes",normalizer:"Exportar comentarios para unificar conceptos"};
export function TaxonomyProject({ role, context, url, disabled, canExport, onChange, proposal, proposalFingerprint, review }: { proposal?: TaxonomyStatus["proposed_discovered_taxonomy"]; proposalFingerprint?: string; review?: TaxonomyStatus["designer_review"]; role: Role; context: TaxonomyContext; url: string; disabled: boolean; canExport: boolean; onChange: () => Promise<void> }) {
  const [semanticMode, setSemanticMode] = useState<"SOURCE" | "COMPLETED">("SOURCE");
  const exchangeContext = role === "semantic" ? { ...context, mode: semanticMode } : context;
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
      const result = await taxonomyRequest<{saved_path: string}>(`/discovery/${role}/export`, exchangeContext, {method:"POST"});
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
    const result = await taxonomyRequest<{progress?: ExchangeCounts}>(`/discovery/${role}/import`, exchangeContext, {method:"POST", body});
    const success = role === "semantic" ? "Criterios semánticos importados. Fingerprint actualizado; clasifica con estos criterios desde Análisis con LLM." : role === "designer" ? "Propuesta de taxonomía importada. Revísala antes de aceptarla." : role === "normalizer" ? "Conceptos actualizados para esta compañía." : "Importación validada. Progreso acumulado actualizado.";
    setMessage(success + (role === "classifier" && result.progress ? ` ${classificationImportMessage(result.progress.pending)}` : ""));
    await mutate();
    await onChange();
  }
  const locked = disabled || busy;
  const semanticProgress = progress?.semantic?.[semanticMode];
  const counts = role === "semantic" ? semanticProgress : role === "designer" ? progress?.designer : progress;
  const hasCategories = role !== "semantic" || !!semanticProgress?.total;
  const proposalCounts = proposal ? taxonomyCounts(proposal) : null;
  return <article className="settings-subsection taxonomy-project">
    <div className="section-heading"><div><h3>{PROJECT_NAMES[role]}</h3><p className="secondary-copy">{role === "semantic" ? "Define criterios y fronteras para las categorías originales o manuales. Conserva sus nombres y aplica los criterios a comentarios e incidencias." : role === "designer" ? "Descubre dimensiones de experiencia que expliquen las opiniones del canal." : role === "classifier" ? "Clasifica con la lente activa. La categoría principal conserva los recuentos; los temas adicionales aportan contexto." : "Propón nombres principales y alias equivalentes para la compañía seleccionada."}</p></div></div>
    {role === "semantic" ? <label>Taxonomía para crear criterios<select value={semanticMode} disabled={locked} onChange={event => setSemanticMode(event.target.value as "SOURCE" | "COMPLETED")}><option value="SOURCE">Taxonomía Original</option><option value="COMPLETED">Taxonomía Manual</option></select></label> : null}
    {counts && role !== "normalizer" ? <ExchangeProgress counts={counts} unit={role === "semantic" ? "categorías" : "comentarios"} metrics={role === "semantic" ? [{label:"Palancas",value:semanticProgress!.levers},{label:"Subpalancas",value:semanticProgress!.sublevers}] : role === "designer" ? [{label:"Palancas",value:progress!.designer.levers},{label:"Subpalancas",value:progress!.designer.sublevers}] : [{...ADDITIONAL_TOPICS,value:progress!.multiple}]} /> : null}
    {role === "semantic" && semanticProgress ? <><p>Fingerprint <code>{semanticProgress.taxonomy_fingerprint.slice(0, 8)}</code></p>{semanticProgress.review ? <p>{semanticProgress.review.reason}</p> : <p className="field-hint">Exporta el catálogo y el corpus; importa el ZIP devuelto por el proyecto para completar los criterios.</p>}</> : null}
    {role === "classifier" ? <ClassificationEngineControl kind="comments" context={context} disabled={locked || !canExport} onChange={onChange} /> : null}
    <ProjectUrlField context={context} field={FIELDS[role]} label={`URL · ${PROJECT_NAMES[role]}`} url={url} disabled={locked} />
    <TaxonomyProjectInstructions role={role} context={context} />
    {role === "classifier" && progress ? <p className="field-hint">{!progress.total ? "Importa comentarios NPS desde Ingesta." : !canExport ? "Crea o selecciona primero una taxonomía." : progress.pending ? "Descarga todos los ZIP pendientes de una vez. Procesa cada archivo numerado en el Proyecto ChatGPT e importa su respuesta." : progress.mode === "DISCOVERED" ? "Clasificación completa. La clasificación LLM está activa." : "Clasificación completa. Selecciona Clasificación LLM en este panel."}</p> : null}
    {role === "classifier" && progress?.analysis_horizon?.comment_start ? <p>Ámbito analítico: {progress.analysis_horizon.comment_start} – {progress.analysis_horizon.comment_end}; Helix desde {progress.analysis_horizon.helix_start} por ventana de {progress.analysis_horizon.max_days_apart} días.</p> : null}
    {role === "classifier" && progress ? <p>Modo activo: {progress.mode} · fingerprint <code>{progress.taxonomy_fingerprint?.slice(0, 8)}</code></p> : null}
    <div className="exchange-actions"><button className="primary-button" disabled={locked || !canExport || !hasCategories || (role === "classifier" && progress?.pending === 0)} onClick={() => void perform(exportZips)}>{EXPORT_LABELS[role]}</button>
    <label>{IMPORT_LABELS[role]}<input type="file" accept=".zip,application/zip" disabled={locked || !hasCategories} onChange={e => { const file = e.currentTarget.files?.[0]; e.currentTarget.value = ""; if (file) void perform(() => importZip(file)); }} /></label></div>
    <ExchangeFiles result={exported} />
    {role === "designer" && proposal ? <section aria-label="Propuesta DISCOVERED">
      <h4>Propuesta DISCOVERED · fingerprint <code>{proposalFingerprint?.slice(0, 8)}</code></h4>
      <p>{proposalCounts!.levers} Palancas · {proposalCounts!.sublevers} Subpalancas</p>
      <details className="taxonomy-details" key={proposalFingerprint}>
        <summary>Ver taxonomía</summary>
        <div className="taxonomy-detail-content">
          {proposal.taxonomy.map(branch => <div key={branch.lever}><h4>{branch.lever}</h4><p className="field-hint">Criterio de Palanca: delimitado por los criterios de sus Subpalancas.</p><ul>{branch.sublevers.map(sub => <li key={sub.name}><strong>{sub.name}</strong><p>Criterio: {sub.criterion}</p></li>)}</ul></div>)}
          {review ? <div><h4>Revisión del diseñador</h4><p>{review.reason}</p>{review.quotes.map((quote, index) => <blockquote key={index}>{quote}</blockquote>)}</div> : null}
        </div>
      </details>
      <div className="inline-actions"><TaxonomyDownload context={context} mode="DISCOVERED" proposal /></div>
      <p>Aceptar la deja disponible; no cambia el Marco de clasificación.</p>
      <div className="inline-actions">{[{label:"Aceptar taxonomía DISCOVERED", change:{accept_proposal:true}}, {label:"Descartar propuesta", change:{discard_proposal:true}}].map(({label, change}) => <button key={label} className="secondary-button" disabled={locked} onClick={() => void perform(async () => { await taxonomyRequest("/settings", context, {method:"PUT", headers:{"Content-Type":"application/json"}, body:JSON.stringify(change)}); await onChange(); setMessage(change.accept_proposal ? "Taxonomía DISCOVERED disponible. El Marco de clasificación no ha cambiado." : "Propuesta descartada."); })}>{label}</button>)}</div>
    </section> : null}
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
