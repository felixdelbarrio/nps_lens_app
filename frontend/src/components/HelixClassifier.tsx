import { formatVolume, formatPercentage } from "../utils/numberFormat";
import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";
import { ExchangeFiles } from "./ExchangeFiles";
import { ProjectUrlField } from "./ProjectUrlField";
import { ADDITIONAL_TOPICS, ExchangeProgress } from "./ExchangeProgress";
import { exportClassification, classificationImportMessage, type ClassificationExport } from "../utils/classificationExchange";
import { PROJECT_NAMES } from "../utils/taxonomy";
import { TAXONOMY_NAMES } from "../utils/taxonomy";

type Status = { total:number; received:number; classified:number; unassigned:number; coverage:number; mode:TaxonomyMode; taxonomy_fingerprint:string; link_pending:number; multiple:number; categories:Array<{lever:string;sublever:string;count:number}>; pending: number; taxonomies: Partial<Record<TaxonomyMode, {received:number;pending:number}>> };
export function HelixClassifier({ context, mode, url, disabled, onChange, onlyLinking = false }: { onlyLinking?: boolean; context: TaxonomyContext; mode: TaxonomyMode; url: string; disabled: boolean; onChange: () => Promise<void> }) {
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
    setMessage(onlyLinking ? `Vínculos Helix importados. ${result.link_pending} incidencias con linking pendiente.` : `${success} ${classificationImportMessage(result.pending)}`);
    await mutate();
    await onChange();
  }
  const locked = disabled || busy;
  return <article className="settings-subsection"><h3>{onlyLinking ? "Recalcular vínculos Helix ↔ VoC" : PROJECT_NAMES.helix}</h3>
    {!onlyLinking ? <ProjectUrlField context={context} field="helix_classifier_url" label={`URL · ${PROJECT_NAMES.helix}`} url={url} disabled={locked} /> : null}
    <TaxonomyProjectInstructions role="helix" context={context} />
    <p>Lente activa: {TAXONOMY_NAMES[mode]} ({data?.mode || mode}) · fingerprint <code>{data?.taxonomy_fingerprint?.slice(0, 8)}</code>. El método de vinculación se elige en Evidencia Helix ↔ VoC. Cada lente conserva sus propias clasificaciones.</p>
    {data && !onlyLinking ? <><ExchangeProgress counts={data} unit="incidencias" metrics={[{label:"Con categoría",value:data.classified},{label:"Sin encaje",value:data.unassigned},{...ADDITIONAL_TOPICS,value:data.multiple}]} /><p>Cobertura temática: {formatPercentage(data.coverage)}. Sin encaje incluye categorías de reserva y resultados sin categoría. Las citas comprueban procedencia; la afinidad semántica requiere revisión.</p>
    {data.categories?.length ? <details><summary>Distribución de incidencias</summary><div className="table-scroll"><table><thead><tr><th>Palanca</th><th>Subpalanca</th><th>Incidencias</th></tr></thead><tbody>{data.categories.map(row => <tr key={`${row.lever}/${row.sublever}`}><td>{row.lever}</td><td>{row.sublever}</td><td>{formatVolume(row.count)}</td></tr>)}</tbody></table></div></details> : null}</> : null}
    {onlyLinking && data ? <p>{data.link_pending} incidencias con linking pendiente. Se exportan solo las que ya tienen categorías, conservándolas exactamente.</p> : null}
    {error ? <p role="status">{error.message}</p> : null}
    <div className="inline-actions"><button className="primary-button" disabled={locked || !(onlyLinking ? data?.link_pending : data?.pending)} onClick={() => void run(async () => { setExported(null); setExported(await exportClassification(onlyLinking ? {...context, only_linking: "true"} : context, "/helix/export")); })}>{onlyLinking ? "Exportar solo linking reutilizando categorías" : "Descargar todos los ZIP de incidencias pendientes"}</button>
    <label>{onlyLinking ? "Importar ZIP de vínculos evaluados" : "Importar ZIP de incidencias clasificadas"}<input type="file" accept=".zip,application/zip" disabled={locked} onChange={e => { const file = e.currentTarget.files?.[0]; e.currentTarget.value = ""; if (file) void run(() => importZip(file)); }} /></label></div>
    <ExchangeFiles result={exported} />
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
