import { useState } from "react";
import useSWR from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyDiscoverySettings, type TaxonomyMode, type TaxonomyStatus } from "../api";
import { TAXONOMY_NAMES as NAMES } from "../utils/taxonomy";
import { ManualTaxonomyEditor } from "./ManualTaxonomyEditor";
import { EquivalenceMaintenance } from "./EquivalenceMaintenance";
import { HelixClassifier } from "./HelixClassifier";
import { TaxonomyProject } from "./TaxonomyProject";
const jsonRequest = (method: string, body: unknown) => ({ method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
type Props = { context: TaxonomyContext; onChange: () => Promise<void>; disabled?: boolean };
type Exploration = { rows: Array<Record<string, string | number | string[]>>; total: number; audit?: { multi_parent_sublevers: string[]; generic_labels: string[]; note: string }; note?: string };

export function TaxonomyStudio({ context, onChange, disabled = false }: Props) {
  const { data, error, mutate } = useSWR(taxonomyUrl("", context), () => taxonomyRequest<TaxonomyStatus>("", context));
  const { data: discovery } = useSWR(data?.discovery_local_available ? taxonomyUrl("/discovery", context) : null, () => taxonomyRequest<TaxonomyDiscoverySettings>("/discovery", context));
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [left, setLeft] = useState<TaxonomyMode>("SOURCE");
  const [right, setRight] = useState<TaxonomyMode>("DISCOVERED");
  const [exploration, setExploration] = useState<Exploration | null>(null);
  const [view, setView] = useState<{ mode?: TaxonomyMode; compare?: boolean; offset: number }>({ offset: 0 });
  async function refresh() { await mutate(); await onChange(); setExploration(null); }
  async function action(run: () => Promise<unknown>, reload = true) {
    setBusy(true); setMessage("");
    try { await run(); if (reload) await refresh(); }
    catch (error) { setMessage(error instanceof Error ? error.message : "No se pudo completar la operación."); }
    finally { setBusy(false); }
  }
  async function explore(next: typeof view) {
    setView(next);
    await action(async () => setExploration(await taxonomyRequest<Exploration>(next.compare ? "/compare" : "/explore", { ...context, ...(next.compare ? { left, right } : { mode: next.mode || "SOURCE" }), offset: String(next.offset) })), false);
  }
  if (error) return <p role="alert">{error.message}</p>;
  if (!data) return <p>Preparando taxonomías…</p>;
  const locked = disabled || busy;
  const helixModes = data.helix_modes || [data.active];
  const available = data.taxonomies.filter(item => item.available && item.selectable !== false);
  return <section className="surface-card taxonomy-studio">
    <div className="panel-heading"><div><p className="eyebrow">Análisis local · mismo corpus</p><h2>Taxonomy Studio</h2><p>{data.detection.rows.toLocaleString("es")} respuestas</p></div></div>
    <h3>Taxonomía</h3>
    <label>Lente activa<select value={data.active} disabled={locked} onChange={e => void action(() => taxonomyRequest("/settings", context, jsonRequest("PUT", {active:e.target.value})))}>{data.taxonomies.map(item => <option key={item.mode} value={item.mode} disabled={!item.available || item.selectable === false}>{NAMES[item.mode]}</option>)}</select></label>
    <p className="secondary-copy">Las equivalencias se aplican automáticamente a Original y Manual al usarlas como lente. Descubierta por LLM conserva las categorías del modelo.</p>
    {data.requested_active !== data.active ? <p role="status">La lente anterior no está disponible. Se utiliza Original.</p> : null}
    {data.taxonomies.map(item => <article className="settings-subsection" key={item.mode}>
      <h3>{NAMES[item.mode]}</h3>
      {item.available && item.selectable !== false ? <p>{item.levers} Palancas · {item.sublevers} Subpalancas · {((item.coverage || 0) * 100).toFixed(1)}% cobertura</p> : <p>{item.mode === "SOURCE" ? "El fichero no contiene una taxonomía original." : item.mode === "DISCOVERED" && data.discovered_catalog_available ? "Categorías importadas. Pendiente de clasificar comentarios." : item.stale ? "Los datos han cambiado; revisa esta taxonomía." : "Aún no creada."}</p>}
      <button className="secondary-button" disabled={locked || !item.available || item.selectable === false} onClick={() => void explore({mode:item.mode,offset:0})}>Explorar {NAMES[item.mode]}</button>
      {data.discovery_local_available && !data.restored ? <label className="checkbox-field"><input type="checkbox" checked={helixModes.includes(item.mode)} disabled={locked || !item.available || item.selectable === false || (helixModes.length === 1 && helixModes.includes(item.mode))} onChange={e => { const modes = e.target.checked ? [...helixModes,item.mode] : helixModes.filter(mode => mode !== item.mode); void action(() => taxonomyRequest("/settings",context,jsonRequest("PUT",{helix_modes:modes}))); }} />Incluir {NAMES[item.mode]} en la clasificación Helix</label> : null}
      {item.mode === "COMPLETED" ? <>
        <details><summary>Normalización · tabla de equivalencias</summary><EquivalenceMaintenance taxonomyOnly disabled={locked || data.restored} context={context} onChange={refresh} /></details>
        {data.discovery_local_available && !data.restored ? <ManualTaxonomyEditor context={context} disabled={locked} onChange={refresh} /> : null}
      </> : null}
      {item.mode === "DISCOVERED" && data.discovery_local_available && discovery ? <div className="field-grid taxonomy-projects">
        <TaxonomyProject role="designer" context={context} url={discovery.designer_url} disabled={locked || data.restored} canExport={data.detection.rows > 0} onChange={refresh} />
        <TaxonomyProject role="classifier" context={context} url={discovery.classifier_url} disabled={locked || data.restored} canExport={Boolean(data.discovered_catalog_available)} onChange={refresh} />
      </div> : null}
    </article>)}
    {data.discovery_local_available && discovery && !data.restored ? <HelixClassifier context={context} modes={helixModes} url={discovery.helix_classifier_url} disabled={locked} onChange={refresh} /> : null}
    <details><summary>Comparar taxonomías</summary><div className="field-grid">
      <label>Taxonomía de partida<select value={left} onChange={e => setLeft(e.target.value as TaxonomyMode)}>{available.map(item => <option key={item.mode} value={item.mode}>{NAMES[item.mode]}</option>)}</select></label>
      <label>Comparar con<select value={right} onChange={e => setRight(e.target.value as TaxonomyMode)}>{data.taxonomies.map(item => <option key={item.mode} value={item.mode} disabled={!item.available || item.selectable === false}>{NAMES[item.mode]}</option>)}</select></label>
    </div><button className="secondary-button" disabled={locked || !available.some(item => item.mode === right) || !available.some(item => item.mode === left)} onClick={() => void explore({compare:true,offset:0})}>Ver comparación</button></details>
    {busy ? <p role="status">Procesando…</p> : null}{message ? <p role="status">{message}</p> : null}
    {exploration ? <div className="taxonomy-exploration">
      <h3>{view.compare ? `${NAMES[left]} → ${NAMES[right]}` : NAMES[view.mode || "SOURCE"]}</h3>
      <p>{exploration.note || exploration.audit?.note}</p>
      {exploration.audit ? <p>Subpalancas bajo varias Palancas: {exploration.audit.multi_parent_sublevers.join(", ") || "ninguna"}. Categorías genéricas: {exploration.audit.generic_labels.join(", ") || "ninguna"}.</p> : null}
      <div className="table-scroll"><table><thead><tr>{(view.compare ? ["Origen", "Destino", "Volumen", "Share"] : ["Palanca", "Subpalanca", "Volumen", "Share", "NPS", "Promotores / Neutros / Detractores", "Ejemplos"]).map(label => <th key={label}>{label}</th>)}</tr></thead><tbody>{exploration.rows.map((row, i) => <tr key={i}>{view.compare ? <><td>{row.from_lever} &gt; {row.from_sublever}</td><td>{row.to_lever} &gt; {row.to_sublever}</td><td>{row.volume}</td><td>{(Number(row.share) * 100).toFixed(1)}%</td></> : <><td>{row.Palanca}</td><td>{row.Subpalanca}</td><td>{row.volume}</td><td>{(Number(row.share) * 100).toFixed(1)}%</td><td>{Number(row.nps).toFixed(1)}</td><td>{row.promoters} / {row.neutrals} / {row.detractors}</td><td>{Array.isArray(row.examples) ? row.examples.map((text, index) => <p key={index}>{text}</p>) : null}</td></>}</tr>)}</tbody></table></div>
      <div className="inline-actions"><button disabled={locked || view.offset === 0} onClick={() => void explore({ ...view, offset: Math.max(0, view.offset - 100) })}>Anterior</button><span>{view.offset + 1}–{view.offset + exploration.rows.length} de {exploration.total}</span><button disabled={locked || view.offset + 100 >= exploration.total} onClick={() => void explore({ ...view, offset: view.offset + 100 })}>Siguiente</button></div>
    </div> : null}
  </section>;
}

export function TaxonomyIngestNotice({ context, revision, onOpen }: { context: TaxonomyContext; revision: string; onOpen: () => void }) {
  const { data } = useSWR([taxonomyUrl("", context), revision], () => taxonomyRequest<TaxonomyStatus>("", context));
  if (!data?.detection.rows) return null;
  const { state, missing } = data.detection;
  return <article className="settings-subsection"><h3>Taxonomía del dataset</h3><p>{state === "MISSING" ? "El fichero contiene comentarios NPS pero no incluye Palanca/Subpalanca. Puedes analizarlo igualmente." : state === "PARTIAL" ? `${missing.toLocaleString("es")} respuestas no tienen clasificación completa.` : state === "NO_TEXT" ? "No hay comentarios utilizables para completar o descubrir temas." : "Taxonomía detectada."}</p><button className="secondary-button" onClick={onOpen}>{state === "MISSING" ? "Descubrir taxonomía" : "Abrir Taxonomy Studio"}</button></article>;
}

export function SnapshotSettings({ context, onChange, disabled = false }: Props) {
  const { data, mutate } = useSWR(taxonomyUrl("", context), () => taxonomyRequest<TaxonomyStatus>("", context));
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  async function perform(run: () => Promise<unknown>) {
    setBusy(true); setMessage("");
    try { await run(); await mutate(); await onChange(); }
    catch (error) { setMessage(error instanceof Error ? error.message : "Error al guardar snapshot."); }
    finally { setBusy(false); }
  }
  if (!data) return <p>Preparando snapshots…</p>;
  return <section className="settings-group"><h3>Snapshots</h3><p>Conserva el corpus, las asignaciones, las equivalencias y Helix para abrir el mismo análisis sin recalcular modelos.</p>
    <label>Taxonomía activa por defecto<select value={data.default} disabled={disabled || busy} onChange={e => void perform(() => taxonomyRequest("/settings", context, jsonRequest("PUT", { default: e.target.value })))}>{data.taxonomies.filter(t => t.available).map(t => <option key={t.mode} value={t.mode}>{NAMES[t.mode]}</option>)}</select></label>
    <label>Qué guardar<select value={data.policy} disabled={disabled || busy} onChange={e => void perform(() => taxonomyRequest("/settings", context, jsonRequest("PUT", { policy: e.target.value })))}><option value="ACTIVE_ONLY">Solo activa</option><option value="SOURCE_AND_ACTIVE">Original y activa</option><option value="ALL_AVAILABLE">Todas las disponibles</option></select></label>
    <a className="secondary-button" href={taxonomyUrl("/snapshot", context)} download>Guardar snapshot local</a>
    <label>Restaurar snapshot<input type="file" accept=".json" disabled={disabled || busy} onChange={e => { const file = e.target.files?.[0]; if (!file) return; const form = new FormData(); form.append("file", file); void perform(() => taxonomyRequest("/restore", context, { method: "POST", body: form })); e.target.value = ""; }} /></label>
    {data.restored ? <><p>Snapshot histórico activo. Sus datos y asignaciones permanecen congelados.</p><button className="secondary-button" disabled={disabled || busy} onClick={() => void perform(() => taxonomyRequest("/resume", context, { method: "POST" }))}>Volver al dataset local</button></> : null}
    {message ? <p role="alert">{message}</p> : null}
  </section>;
}
