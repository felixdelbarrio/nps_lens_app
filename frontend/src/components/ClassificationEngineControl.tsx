import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";
import { TAXONOMY_NAMES } from "../utils/taxonomy";
import { formatVolume } from "../utils/numberFormat";

export type EngineStatus = {engine:"rules"|"llm";ready:boolean;reason:string;active:TaxonomyMode;received:number;total:number};
export function ClassificationEngineControl({kind, context, disabled, onChange}: {kind:"comments"|"helix";context:TaxonomyContext;disabled:boolean;onChange:()=>Promise<void>}) {
  const path = `/${kind}/engine`;
  const {data, error, mutate} = useSWR(taxonomyUrl(path,context),()=>taxonomyRequest<EngineStatus>(path,context),{revalidateOnFocus:false});
  const [busy,setBusy] = useState(false);
  const [message,setMessage] = useState("");
  async function change(engine: string) {
    setBusy(true);setMessage("");
    try { await taxonomyRequest(path,{...context,engine},{method:"PUT"});await mutate();await onChange(); }
    catch(error) { setMessage(error instanceof Error ? error.message : "No se pudo cambiar el motor."); }
    finally {setBusy(false);}
  }
  return <div className="classification-engine"><label className="switch-field"><input type="checkbox" role="switch" checked={data?.ready && data.engine === "llm"} disabled={disabled || busy || !data?.ready} onChange={e=>void change(e.target.checked ? "llm" : "rules")} />{kind === "helix" ? "Usar causalidad mediante LLM" : "Usar clasificación LLM"}</label>
    {data ? <p className="field-hint">Lente: {TAXONOMY_NAMES[data.active]}. {formatVolume(data.received)} de {formatVolume(data.total)} {kind === "helix" ? "incidencias" : "comentarios"} del ámbito visible procesados. {data.ready ? "" : data.reason}</p> : null}
    {message || error ? <p role="status">{message || error.message}</p> : null}
  </div>;
}
