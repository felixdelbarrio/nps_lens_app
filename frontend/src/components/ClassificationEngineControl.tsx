import { HelixClassifier } from "./HelixClassifier";
import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";
import { TAXONOMY_NAMES } from "../utils/taxonomy";
import { formatVolume } from "../utils/numberFormat";

export type EngineStatus = {selected_engine:"rules"|"llm";base_available:boolean;ready:boolean;reason:string;active:TaxonomyMode;received:number;total:number};
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
  return <div className="classification-engine"><label>{kind === "helix" ? "Método de vinculación" : "Clasificación de comentarios"}<select value={llmRequired ? "llm" : data?.selected_engine || "rules"} disabled={disabled || busy || !data || llmRequired} onChange={e => void change(e.target.value)}>
      {!llmRequired && (kind === "helix" || data?.base_available) ? <option value="rules">{kind === "helix" ? "TF-IDF / reglas" : "Clasificación base"}</option> : null}
      <option value="llm" disabled={!data?.ready}>{kind === "helix" ? "LLM semántico" : "Clasificación LLM"}</option>
    </select></label>
    {llmRequired ? <p className="field-hint">Clasificación LLM requerida para DISCOVERED.</p> : null}
    {kind === "helix" ? <p className="field-hint">TF-IDF = similitud textual + reglas. LLM = validación semántica de candidatos.</p> : null}
    {data ? <p className="field-hint">Marco: {TAXONOMY_NAMES[data.active]}. {formatVolume(data.received)} de {formatVolume(data.total)} {kind === "helix" ? "incidencias elegibles" : "comentarios del horizonte analítico"} procesados. {!data.ready ? data.reason : ""}</p> : null}
    {kind === "helix" && data?.ready && data.selected_engine === "llm" ? <HelixClassifier context={context} mode={data.active} url="" disabled={disabled || busy} onlyLinking onChange={async () => { await mutate(); await onChange(); }} /> : null}
    {message || error ? <p role="status">{message || error.message}</p> : null}
  </div>;
}
