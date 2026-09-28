import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";
import { TAXONOMY_NAMES } from "../utils/taxonomy";

export function CausalEngineControl({context, disabled, onChange}: {context:TaxonomyContext;disabled:boolean;onChange:()=>Promise<void>}) {
  const {data, error, mutate} = useSWR(taxonomyUrl("/helix/engine",context),()=>taxonomyRequest<{engine:"rules"|"llm";ready:boolean;reason:string;active:TaxonomyMode}>("/helix/engine",context));
  const [busy,setBusy] = useState(false);
  const [message,setMessage] = useState("");
  async function change(engine: string) {
    setBusy(true);setMessage("");
    try { await taxonomyRequest("/helix/engine",{...context,engine},{method:"PUT"});await mutate();await onChange(); }
    catch(error) { setMessage(error instanceof Error ? error.message : "No se pudo cambiar el motor causal."); }
    finally {setBusy(false);}
  }
  return <div><label className="switch-field"><input type="checkbox" role="switch" checked={data?.engine === "llm"} disabled={disabled || busy || (!data?.ready && data?.engine !== "llm")} onChange={e=>void change(e.target.checked ? "llm" : "rules")} />Usar causalidad mediante LLM</label>
    {data ? <p className="field-hint">Lente: {TAXONOMY_NAMES[data.active]}. {data.ready ? "Respuesta Helix disponible para cualquier método causal." : data.reason || "Importa las clasificaciones Helix de esta lente para activar LLM."}</p> : null}
    {message || error ? <p role="status">{message || error.message}</p> : null}
  </div>;
}
