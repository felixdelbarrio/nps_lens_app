import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext } from "../api";

type Branch = { lever: string; sublevers: string[]; previous_lever?: string; previous_sublevers?: string[] };
export function ManualTaxonomyEditor({ context, disabled, onChange }: { context: TaxonomyContext; disabled: boolean; onChange: () => Promise<void> }) {
  const [branches, setBranches] = useState<Branch[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const { data: info, error, mutate } = useSWR(taxonomyUrl("/manual", context), () => taxonomyRequest<{exists:boolean; revision:string; templates:string[]; affected_comments:number}>("/manual", context));
  const [template, setTemplate] = useState("NONE");
  const [draft, setDraft] = useState({ template: "NONE", revision: "" });
  const [confirmed, setConfirmed] = useState(false);
  async function load(selected: string) {
    setBusy(true); setMessage("");
    try {
      const result = await taxonomyRequest<{ taxonomy: Branch[]; revision:string }>("/manual", { ...context, template: selected });
      setDraft({ template: selected, revision: result.revision }); setConfirmed(false);
      setBranches(result.taxonomy.map(branch => ({ ...branch, previous_lever: branch.lever, previous_sublevers: [...branch.sublevers] })));
    } catch (error) { setMessage(error instanceof Error ? error.message : "Error al cargar."); }
    finally { setBusy(false); }
  }
  async function save() {
    setBusy(true); setMessage("");
    try {
      await taxonomyRequest("/manual", context, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ taxonomy: branches, ...draft, confirmed }) });
      setBranches(null); setMessage("Taxonomía Manual guardada. Las categorías eliminadas quedan pendientes de clasificación."); await mutate(); await onChange();
    } catch (error) { setMessage(error instanceof Error ? error.message : "Error al guardar."); }
    finally { setBusy(false); }
  }
  function update(index: number, branch: Branch) { setBranches(current => current?.map((item, i) => i === index ? branch : item) ?? null); }
  const locked = disabled || busy;
  return <article className="settings-subsection"><h4>Catálogo editable</h4>
    <p>Elige una plantilla para crear una nueva taxonomía o modifica la existente. Cada guardado genera una revisión independiente.</p>
    {info?.exists ? <p className="secondary-copy">Revisión Manual: <code title={info.revision}>{info.revision.slice(0, 12)}</code></p> : null}
    {branches === null ? <>
      <label>Plantilla<select value={template} disabled={locked} onChange={event => setTemplate(event.target.value)}>{(info?.templates || ["NONE"]).map(value => <option key={value} value={value}>{value === "NONE" ? "Sin plantilla" : value === "SOURCE" ? "Original" : "Descubierto"}</option>)}</select></label>
      <div className="inline-actions"><button className="secondary-button" disabled={locked || !info} onClick={() => void load(template)}>Crear Manual</button><button className="secondary-button" disabled={locked || !info?.exists} onClick={() => void load("CURRENT")}>Visualizar / modificar Manual</button></div>
    </> : <>
      <div className="manual-taxonomy-branches">{branches.map((branch, i) => <fieldset className="manual-branch" key={i} disabled={locked}><legend>Palanca {i + 1}</legend>
        <label>Nombre de Palanca<input value={branch.lever} onChange={e => update(i, { ...branch, lever: e.target.value })} /></label>
        {branch.sublevers.map((sub, j) => <div className="manual-sublever" key={j}><label>Subpalanca {j + 1}<input value={sub} onChange={e => update(i, { ...branch, sublevers: branch.sublevers.map((value, k) => k === j ? e.target.value : value) })} /></label><button className="secondary-button" onClick={() => update(i, { ...branch, sublevers: branch.sublevers.filter((_, k) => k !== j), previous_sublevers: branch.previous_sublevers?.filter((_, k) => k !== j) })}>Eliminar Subpalanca {j + 1}</button></div>)}
        <div className="inline-actions"><button className="secondary-button" onClick={() => update(i, { ...branch, sublevers: [...branch.sublevers, ""], previous_sublevers: [...(branch.previous_sublevers || []), ""] })}>Añadir Subpalanca</button><button className="secondary-button" onClick={() => setBranches(branches.filter((_, k) => k !== i))}>Eliminar Palanca {i + 1}</button></div>
      </fieldset>)}</div>
      {draft.revision ? <div role="note"><p>Al guardar se sustituirá Manual: se recalcularán las relaciones de {info?.affected_comments || 0} comentarios con la plantilla o las correspondencias editadas. Las categorías eliminadas quedarán pendientes. Los cambios de categorías o criterios requieren reclasificar; guardar el mismo catálogo conserva los criterios y las clasificaciones de comentarios LLM. La lente activa solo cambia desde su selector.</p><label className="checkbox-field"><input type="checkbox" checked={confirmed} onChange={event => setConfirmed(event.target.checked)} />Confirmo la sustitución y el recálculo de relaciones</label></div> : null}
      <div className="inline-actions manual-save-actions"><button className="secondary-button" disabled={locked} onClick={() => setBranches([...branches, { lever: "", sublevers: [""], previous_lever: "", previous_sublevers: [""] }])}>Añadir Palanca</button><button className="primary-button" disabled={locked || !branches.length || Boolean(draft.revision && !confirmed)} onClick={() => void save()}>Guardar Manual</button><button className="secondary-button" disabled={locked} onClick={() => setBranches(null)}>Cancelar</button></div>
    </>}
    {error ? <p role="alert">{error.message}</p> : null}
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
