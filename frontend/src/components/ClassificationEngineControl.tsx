import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";
import { TAXONOMY_NAMES } from "../utils/taxonomy";
import { formatVolume } from "../utils/numberFormat";

export type EngineStatus = {selected_engine:"rules"|"llm";base_available:boolean;ready:boolean;linking_ready:boolean;reason:string;active:TaxonomyMode;received:number;total:number};
export function ClassificationEngineControl({kind, context, disabled, onChange}: {kind:"comments"|"helix";context:TaxonomyContext;disabled:boolean;onChange:()=>Promise<void>}) {
  const path = `/${kind}/engine`;
  const {data, error, mutate} = useSWR(taxonomyUrl(path,context),()=>taxonomyRequest<EngineStatus>(path,context),{revalidateOnFocus:false});
  const [busy,setBusy] = useState(false);
  const [message,setMessage] = useState("");
  async function change(engine: string) {
    setBusy(true);setMessage("");
    try { const status = await taxonomyRequest<EngineStatus>(path,{...context,engine},{method:"PUT"});await mutate(status,{revalidate:false});await onChange(); }
    catch(error) { setMessage(error instanceof Error ? error.message : "No se pudo cambiar el motor."); }
    finally {setBusy(false);}
  }
  const llmRequired = kind === "comments" && data?.active === "DISCOVERED";
  const locked = disabled || busy || !data;
  return <div className="classification-engine">
    {kind === "helix" ? <>
      <span>Método de vinculación</span>
      <label className="switch-field"><span>TF-IDF / reglas</span>
        <input type="checkbox" role="switch" aria-label="Vinculación con LLM" checked={data?.selected_engine === "llm"} disabled={locked || !data?.linking_ready} onChange={e => void change(e.target.checked ? "llm" : "rules")} />
        <span>LLM semántico</span>
      </label>
      <p className="field-hint">TF-IDF compara texto y aplica reglas. LLM utiliza los vínculos evaluados en Clasifica incidencias.</p>
    </> : <label>Clasificación de comentarios<select value={llmRequired ? "llm" : data?.selected_engine || "rules"} disabled={locked || llmRequired} onChange={e => void change(e.target.value)}>
      {!llmRequired && data?.base_available ? <option value="rules">Clasificación base</option> : null}
      <option value="llm" disabled={!data?.ready}>Clasificación LLM</option>
    </select></label>}
    {llmRequired ? <p className="field-hint">Clasificación LLM requerida para DISCOVERED.</p> : null}
    {data ? <p className="field-hint">Marco: {TAXONOMY_NAMES[data.active]}. {formatVolume(data.received)} de {formatVolume(data.total)} {kind === "helix" ? "incidencias elegibles" : "comentarios del horizonte analítico"} procesados. {data.reason}</p> : null}
    {message || error ? <p role="status">{message || error.message}</p> : null}
  </div>;
}
