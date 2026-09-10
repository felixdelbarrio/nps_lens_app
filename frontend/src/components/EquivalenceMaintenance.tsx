import { useState } from "react";
import useSWR from "swr";

import { fetchEquivalences, updateEquivalences, type EquivalenceRegistryPayload, type TaxonomyContext } from "../api";

export function EquivalenceMaintenance({ disabled = false, context = {}, onChange }: { disabled?: boolean; context?: TaxonomyContext; onChange?: () => Promise<void> }) {
  const { data, error, mutate } = useSWR(["equivalences", new URLSearchParams(context).toString()], () => fetchEquivalences(context));
  const [draft, setDraft] = useState<EquivalenceRegistryPayload | null>(null);
  const [domain, setDomain] = useState("nps");
  const [dimension, setDimension] = useState("nps.Palanca");
  const [message, setMessage] = useState("");
  const [saving, setSaving] = useState(false);
  const registry = draft || data;
  if (error) return <p role="alert">{error.message}</p>;
  if (!registry) return <p>Preparando conceptos…</p>;
  const dimensions = (registry.available_dimensions || Object.keys(registry.dimensions)).filter(key => key.startsWith(domain + "."));
  const groups = registry.dimensions[dimension] || [];
  const stats = registry.statistics?.[dimension];
  function edit(index: number, canonical: string, aliases: string[]) {
    if (!registry) return;
    const next = [...(registry.dimensions[dimension] || [])];
    next[index] = { canonical, aliases };
    setDraft({ ...registry, dimensions: { ...registry.dimensions, [dimension]: next } });
  }
  async function save() {
    if (!registry) return;
    setSaving(true); setMessage("");
    try { const next = await updateEquivalences(registry, context); setDraft(null); await mutate(next, false); await onChange?.(); setMessage("Equivalencias guardadas. Los datos fuente permanecen intactos."); }
    catch (error) { setMessage(error instanceof Error ? error.message : "No se pudieron guardar los conceptos."); }
    finally { setSaving(false); }
  }
  return <div className="settings-section-stack">
    <p>Solo se unifican los alias que guardes. Los comentarios, descripciones y resoluciones permanecen intactos.</p>
    <div className="field-grid"><label>Dominio<select value={domain} disabled={saving} onChange={e => { setDomain(e.target.value); setDimension((registry.available_dimensions || Object.keys(registry.dimensions)).find(key => key.startsWith(e.target.value + ".")) || ""); }}><option value="nps">NPS</option><option value="helix">Helix</option></select></label>
    <label>Dimensión<select value={dimension} disabled={saving} onChange={e => setDimension(e.target.value)}>{dimensions.map(key => <option key={key} value={key}>{key.split(".")[1]}</option>)}</select></label></div>
    <h4>{dimension}</h4>
    {groups.map((group, index) => <article className="settings-subsection" key={index}>
      <label>Nombre principal<input disabled={disabled || saving} value={group.canonical} onChange={e => edit(index, e.target.value, group.aliases)} /></label>
      <p>{stats?.groups.find(row => row.canonical === group.canonical)?.affected ?? 0} registros afectados en el corpus</p>
      <div className="alias-chips">{group.aliases.map((alias, ai) => <span className="alias-chip" key={ai}>{alias}<button aria-label={`Eliminar ${alias}`} disabled={disabled || saving} onClick={() => edit(index, group.canonical, group.aliases.filter((_, i) => ai !== i))}>×</button></span>)}</div>
      <input aria-label="Añadir alias" placeholder="Escribe un alias y pulsa Intro" disabled={disabled || saving} onKeyDown={e => { if (e.key !== "Enter") return; e.preventDefault(); const value = e.currentTarget.value; if (value.trim()) edit(index, group.canonical, [...new Set([...group.aliases, value])]); e.currentTarget.value = ""; }} />
      <button className="secondary-button" disabled={disabled || saving} onClick={() => setDraft({ ...registry, dimensions: { ...registry.dimensions, [dimension]: groups.filter((_, i) => i !== index) } })}>Eliminar grupo</button>
    </article>)}
    <button className="secondary-button" disabled={disabled || saving} onClick={() => edit(groups.length, "Nuevo concepto", [])}>Añadir concepto</button>
    <article className="settings-subsection"><h4>Posibles variantes triviales</h4><p>Coincidencias por espacios, mayúsculas, acentos o separadores. Revísalas antes de añadirlas.</p>{stats?.suggestions.length ? stats.suggestions.map((suggestion, index) => <p key={index}>{suggestion.variants.join(" · ")} <button disabled={disabled || saving} onClick={() => edit(groups.length, suggestion.variants[0], suggestion.variants.slice(1))}>Añadir a borrador</button></p>) : <p>No se detectan variantes pendientes.</p>}</article>
    <button className="primary-button" disabled={disabled || saving} onClick={() => void save()}>{saving ? "Guardando…" : "Guardar equivalencias"}</button>
    {message ? <p role="status">{message}</p> : null}
  </div>;
}
