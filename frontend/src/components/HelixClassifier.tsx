import { formatVolume, formatPercentage } from "../utils/numberFormat";
import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";
import { ExchangeFiles } from "./ExchangeFiles";
import { ProjectUrlField } from "./ProjectUrlField";
import { ADDITIONAL_TOPICS, ExchangeProgress } from "./ExchangeProgress";
import { exportClassification, classificationImportMessage, type ClassificationExport } from "../utils/classificationExchange";
import { PROJECT_NAMES, TAXONOMY_NAMES } from "../utils/taxonomy";

type Status = { total:number; received:number; classified:number; unassigned:number; coverage:number; mode:TaxonomyMode; taxonomy_fingerprint:string; link_pending:number; multiple:number; categories:Array<{lever:string;sublever:string;count:number}>; pending: number; taxonomies: Partial<Record<TaxonomyMode, {received:number;pending:number}>> };
export function HelixClassifier({ context, mode, url, disabled, onChange }: { context: TaxonomyContext; mode: TaxonomyMode; url: string; disabled: boolean; onChange: () => Promise<void> }) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [exported, setExported] = useState<ClassificationExport | null>(null);
  const { data, error, mutate } = useSWR([taxonomyUrl("/helix", context), mode], () => taxonomyRequest<Status>("/helix", context), { shouldRetryOnError: false });
  async function run(action: () => Promise<void>) {
    setBusy(true); setMessage("");
    try { await action(); }
    catch (error) { setMessage(error instanceof Error ? error.message : "No se pudo completar la operación."); }
    finally { setBusy(false); }
  }
  async function importZip(file: File) {
    const body = new FormData();
    body.append("file", file);
    const result = await taxonomyRequest<Status>("/helix/import", context, { method: "POST", body });
    const success = "Clasificaciones Helix importadas con formato y citas comprobados.";
    setMessage(`${success} ${classificationImportMessage(result.pending)} ${result.link_pending} incidencias pendientes de evaluar vínculos.`);
    await mutate();
    await onChange();
  }
  const locked = disabled || busy;
  const pending = (data?.pending || 0) + (data?.link_pending || 0);
  const reevaluate = Boolean(data?.received) && pending === 0;
  return <article className="settings-subsection"><h3>{PROJECT_NAMES.helix}</h3>
    <ProjectUrlField context={context} field="helix_classifier_url" label={`URL · ${PROJECT_NAMES.helix}`} url={url} disabled={locked} />
    <TaxonomyProjectInstructions role="helix" context={context} />
    <p>Lente activa: {TAXONOMY_NAMES[mode]} ({data?.mode || mode}) · fingerprint <code>{data?.taxonomy_fingerprint?.slice(0, 8)}</code>. Este intercambio clasifica las incidencias y evalúa sus vínculos con los comentarios. Cada lente conserva sus propios resultados.</p>
    {data ? <><ExchangeProgress counts={data} unit="incidencias" metrics={[{label:"Con categoría",value:data.classified},{label:"Sin encaje",value:data.unassigned},{...ADDITIONAL_TOPICS,value:data.multiple}]} /><p>Cobertura temática: {formatPercentage(data.coverage)}. Sin encaje incluye categorías de reserva y resultados sin categoría. Las citas comprueban procedencia; la afinidad semántica requiere revisión.</p>
    {data.categories?.length ? <details><summary>Distribución de incidencias</summary><div className="table-scroll"><table><thead><tr><th>Palanca</th><th>Subpalanca</th><th>Incidencias</th></tr></thead><tbody>{data.categories.map(row => <tr key={`${row.lever}/${row.sublever}`}><td>{row.lever}</td><td>{row.sublever}</td><td>{formatVolume(row.count)}</td></tr>)}</tbody></table></div></details> : null}</> : null}
    {data ? <p>{data.link_pending} incidencias pendientes de evaluar vínculos. Las categorías existentes se conservan exactamente. El método de vinculación se elige en los filtros de Insights · Evidencia Helix ↔ VoC.</p> : null}
    {error ? <p role="status">{error.message}</p> : null}
    <div className="inline-actions"><button className="primary-button" disabled={locked || !(pending || data?.received)} onClick={() => void run(async () => { setExported(null); setExported(await exportClassification({...context, reevaluate: String(reevaluate)}, "/helix/export")); })}>{reevaluate ? "Reevaluar vínculos conservando categorías" : "Descargar todos los ZIP de incidencias pendientes"}</button>
    <label>Importar ZIP de incidencias clasificadas<input type="file" accept=".zip,application/zip" disabled={locked} onChange={e => { const file = e.currentTarget.files?.[0]; e.currentTarget.value = ""; if (file) void run(() => importZip(file)); }} /></label></div>
    <ExchangeFiles result={exported} />
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
