import { useEffect, useState } from "react";

import { fetchEquivalences, updateEquivalences, type EquivalenceRegistryPayload } from "../api";

const DIMENSION_LABELS: Record<string, string> = { Palanca: "Palancas de experiencia", Subpalanca: "Motivos detallados" };

export function EquivalenceMaintenance({ disabled = false }: { disabled?: boolean }) {
  const [registry, setRegistry] = useState<EquivalenceRegistryPayload | null>(null);
  const [message, setMessage] = useState("Preparando conceptos…");
  useEffect(() => {
    let active = true;
    void fetchEquivalences()
      .then((value) => {
        if (!active) return;
        setRegistry(value);
        setMessage("");
      })
      .catch((error: Error) => {
        if (active) setMessage(error.message);
      });
    return () => { active = false; };
  }, []);
  function updateGroup(dimension: string, groupIndex: number, transform: (group: { canonical: string; aliases: string[] }) => { canonical: string; aliases: string[] }) {
    if (!registry) return;
    setRegistry({ ...registry, dimensions: { ...registry.dimensions, [dimension]: registry.dimensions[dimension].map((group, index) => index === groupIndex ? transform(group) : group) } });
  }
  function addConcept(dimension: string) {
    if (!registry) return;
    setRegistry({ ...registry, dimensions: { ...registry.dimensions, [dimension]: [...registry.dimensions[dimension], { canonical: "Nuevo concepto", aliases: [] }] } });
  }
  async function save() {
    if (!registry) return;
    setMessage("Aplicando los conceptos al histórico…");
    try { const next = await updateEquivalences(registry); setRegistry(next); setMessage(`${next.updated_records ?? 0} registros revisados. Todas las vistas ya usan la misma denominación.`); }
    catch (error) { setMessage(error instanceof Error ? error.message : "No se pudieron guardar los conceptos."); }
  }
  if (!registry) return <p className="secondary-copy">{message}</p>;
  return <div className="settings-section-stack">
    <article className="equivalence-example"><span className="equivalence-example-before">Pagos y transferencias</span><span aria-hidden="true">→</span><strong>Pagos/transferencias</strong><p>Las variantes se consolidan antes de calcular oportunidades, comparativas y causalidad.</p></article>
    {Object.entries(registry.dimensions).map(([dimension, groups]) => <article className="settings-subsection" key={dimension}>
      <div className="settings-subsection-copy equivalence-heading"><div><p className="eyebrow">{groups.length} conceptos</p><h4>{DIMENSION_LABELS[dimension] || dimension}</h4></div><button className="secondary-button" disabled={disabled} onClick={() => addConcept(dimension)} type="button">Añadir concepto</button></div>
      <div className="equivalence-table" role="table" aria-label={DIMENSION_LABELS[dimension] || dimension}>
        <div className="equivalence-table-header" role="row"><span>Nombre que verá el cliente</span><span>También puede aparecer como…</span></div>
        {groups.map((group, groupIndex) => <div className="equivalence-row" role="row" key={`${dimension}-${groupIndex}`}>
          <input aria-label="Nombre principal" disabled={disabled} value={group.canonical} onChange={(event) => updateGroup(dimension, groupIndex, current => ({ ...current, canonical: event.target.value }))} />
          <div className="alias-editor"><div className="alias-chips">{group.aliases.map((alias, aliasIndex) => <span className="alias-chip" key={`${alias}-${aliasIndex}`}>{alias}<button aria-label={`Eliminar ${alias}`} disabled={disabled} onClick={() => updateGroup(dimension, groupIndex, current => ({ ...current, aliases: current.aliases.filter((_, index) => index !== aliasIndex) }))} type="button">×</button></span>)}</div><input aria-label="Añadir otra forma de escribirlo" disabled={disabled} placeholder="Escribe una variante y pulsa Intro" onKeyDown={(event) => { if (event.key !== "Enter") return; event.preventDefault(); const value = event.currentTarget.value.trim(); if (!value) return; updateGroup(dimension, groupIndex, current => ({ ...current, aliases: [...new Set([...current.aliases, value])] })); event.currentTarget.value = ""; }} /></div>
        </div>)}
      </div>
    </article>)}
    <div className="settings-subsection-actions"><button className="primary-button" disabled={disabled} onClick={() => void save()} type="button">Guardar y unificar datos</button>{message ? <span className="field-hint">{message}</span> : null}</div>
  </div>;
}
