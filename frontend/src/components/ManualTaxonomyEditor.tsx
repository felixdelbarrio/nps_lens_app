import { useState } from "react";
import { taxonomyRequest, type TaxonomyContext } from "../api";

type Branch = { lever: string; sublevers: string[]; previous_lever?: string; previous_sublevers?: string[] };
export function ManualTaxonomyEditor({ context, disabled, onChange }: { context: TaxonomyContext; disabled: boolean; onChange: () => Promise<void> }) {
  const [branches, setBranches] = useState<Branch[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  async function load() {
    setBusy(true); setMessage("");
    try {
      const result = await taxonomyRequest<{ taxonomy: Branch[] }>("/manual", context);
      setBranches(result.taxonomy.map(branch => ({ ...branch, previous_lever: branch.lever, previous_sublevers: [...branch.sublevers] })));
    } catch (error) { setMessage(error instanceof Error ? error.message : "Error al cargar."); }
    finally { setBusy(false); }
  }
  async function save() {
    setBusy(true); setMessage("");
    try {
      await taxonomyRequest("/manual", context, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ taxonomy: branches }) });
      setBranches(null); setMessage("Taxonomía Manual guardada. Las categorías eliminadas quedan pendientes de clasificación."); await onChange();
    } catch (error) { setMessage(error instanceof Error ? error.message : "Error al guardar."); }
    finally { setBusy(false); }
  }
  function update(index: number, branch: Branch) { setBranches(current => current?.map((item, i) => i === index ? branch : item) ?? null); }
  const locked = disabled || busy;
  return <article className="settings-subsection"><h4>Crear / modificar</h4>
    <p>Parte de Original; si no contiene categorías, utiliza la Descubierta por LLM. Puedes añadir, renombrar y eliminar Palancas y Subpalancas.</p>
    {branches === null ? <button className="secondary-button" disabled={locked} onClick={() => void load()}>Crear / modificar Manual</button> : <>
      {branches.map((branch, i) => <fieldset key={i} disabled={locked}><legend>Palanca {i + 1}</legend>
        <label>Nombre de Palanca<input value={branch.lever} onChange={e => update(i, { ...branch, lever: e.target.value })} /></label>
        {branch.sublevers.map((sub, j) => <div className="inline-actions" key={j}><label>Subpalanca {j + 1}<input value={sub} onChange={e => update(i, { ...branch, sublevers: branch.sublevers.map((value, k) => k === j ? e.target.value : value) })} /></label><button className="secondary-button" onClick={() => update(i, { ...branch, sublevers: branch.sublevers.filter((_, k) => k !== j), previous_sublevers: branch.previous_sublevers?.filter((_, k) => k !== j) })}>Eliminar Subpalanca {j + 1}</button></div>)}
        <div className="inline-actions"><button className="secondary-button" onClick={() => update(i, { ...branch, sublevers: [...branch.sublevers, ""], previous_sublevers: [...(branch.previous_sublevers || []), ""] })}>Añadir Subpalanca</button><button className="secondary-button" onClick={() => setBranches(branches.filter((_, k) => k !== i))}>Eliminar Palanca {i + 1}</button></div>
      </fieldset>)}
      <div className="inline-actions"><button className="secondary-button" disabled={locked} onClick={() => setBranches([...branches, { lever: "", sublevers: [""], previous_lever: "", previous_sublevers: [""] }])}>Añadir Palanca</button><button className="primary-button" disabled={locked || !branches.length} onClick={() => void save()}>Guardar Manual</button><button className="secondary-button" disabled={locked} onClick={() => setBranches(null)}>Cancelar</button></div>
    </>}
    {message ? <p role="status">{message}</p> : null}
  </article>;
}
