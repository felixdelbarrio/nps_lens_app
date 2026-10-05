import { formatVolume, formatPercentage, formatMetric } from "../utils/numberFormat";
import { useState } from "react";
import useSWR, { useSWRConfig } from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyDiscoverySettings, type TaxonomyMode, type TaxonomyStatus } from "../api";
import { TAXONOMY_NAMES as NAMES } from "../utils/taxonomy";
import { ManualTaxonomyEditor } from "./ManualTaxonomyEditor";
import { NavigationTabs } from "./NavigationTabs";
import { ClassificationEngineControl } from "./ClassificationEngineControl";
import { HelixClassifier } from "./HelixClassifier";
import { TaxonomyProject } from "./TaxonomyProject";
const jsonRequest = (method: string, body: unknown) => ({ method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
type Props = { classificationContext?: TaxonomyContext; context: TaxonomyContext; onChange: () => Promise<void>; disabled?: boolean };
type Exploration = { rows: Array<Record<string, string | number | string[]>>; total: number; audit?: { multi_parent_sublevers: string[]; generic_labels: string[]; note: string }; note?: string };

export function TaxonomyStudio({ context, classificationContext = context, onChange, disabled = false }: Props) {
  const { data, error, mutate } = useSWR(taxonomyUrl("", context), () => taxonomyRequest<TaxonomyStatus>("", context));
  const { data: discovery } = useSWR(data?.discovery_local_available ? taxonomyUrl("/discovery", context) : null, () => taxonomyRequest<TaxonomyDiscoverySettings>("/discovery", context));
  const [tab, setTab] = useState("static");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [left, setLeft] = useState<TaxonomyMode>("SOURCE");
  const [right, setRight] = useState<TaxonomyMode>("DISCOVERED");
  const { mutate: revalidate } = useSWRConfig();
  async function refresh() {
    await mutate();
    await revalidate(key => {
      const url = Array.isArray(key) ? key[0] : key;
      return typeof url === "string" && /^\/api\/taxonomy\/(manual|comments\/engine|helix(?:\/engine)?|discovery\/progress|explore|compare)(\?|$)/.test(url);
    });
    await onChange();
  }
  async function action(run: () => Promise<unknown>, reload = true) {
    setBusy(true); setMessage("");
    try { await run(); if (reload) await refresh(); }
    catch (error) { setMessage(error instanceof Error ? error.message : "No se pudo completar la operación."); }
    finally { setBusy(false); }
  }
  if (error) return <p role="alert">{error.message}</p>;
  if (!data) return <p>Preparando taxonomías…</p>;
  const locked = disabled || busy;
  const available = data.taxonomies.filter(item => item.available && item.selectable !== false);
  const activeMode = data.active;
  const hasFramework = available.some(item => item.mode === activeMode);
  const exchangeKey = `${taxonomyUrl("", classificationContext)}:${activeMode}`;
  return <section className="surface-card taxonomy-studio">
    <div className="panel-heading"><div><p className="eyebrow">Análisis local · mismo corpus</p><h2>Taxonomy Studio</h2><p>{formatVolume(data.detection.rows)} respuestas</p></div></div>
      <section className="taxonomy-lens"><p className="eyebrow">Marco de clasificación</p><p>Se aplica a Insights, comentarios, incidencias y presentaciones. La selección se guarda por compañía; cada taxonomía conserva sus resultados LLM.</p><label>Marco de clasificación<select value={hasFramework ? activeMode : ""} disabled={locked || !available.length} onChange={e => void action(() => taxonomyRequest("/settings",context,jsonRequest("PUT",{active:e.target.value})))}><option value="" disabled>No hay una taxonomía seleccionada</option>{available.map(item => <option key={item.mode} value={item.mode}>{NAMES[item.mode]}</option>)}</select></label></section>
    <NavigationTabs items={[{id:"static",label:"Análisis estático"},{id:"llm",label:"Análisis con LLM"}]} value={tab} onChange={setTab} />
    {tab === "static" ? <div className="settings-section-stack">
      {data.taxonomies.filter(item => item.mode !== "DISCOVERED").map(item => <article className="settings-subsection" key={item.mode}>
        <h3>{NAMES[item.mode]}</h3>
        {item.available && item.selectable !== false ? <><p>{item.levers} Palancas · {item.sublevers} Subpalancas · {formatPercentage(item.coverage || 0)} cobertura</p><TaxonomyExplorer key={`${item.mode}-${item.created_at || ""}`} context={context} mode={item.mode} /></> : <p>{item.stale ? "Los datos han cambiado; revisa esta taxonomía." : "Aún no disponible."}</p>}
        {item.mode === "COMPLETED" && data.discovery_local_available && !data.restored ? <ManualTaxonomyEditor context={context} disabled={locked} onChange={refresh} /> : null}
      </article>)}
    </div> : <div className="settings-section-stack">
      <article className="settings-subsection">
        <h3>Lente activa</h3>
        <p>{NAMES[activeMode]} · fingerprint <code>{data.active_fingerprint.slice(0, 8)}</code></p>
      </article>
      {data.discovery_local_available && discovery && !data.restored ? <>
        <TaxonomyProject role="designer" proposal={data.proposed_discovered_taxonomy} proposalFingerprint={data.proposed_discovered_fingerprint} review={data.designer_review} context={context} url={discovery.designer_url} disabled={locked} canExport={data.detection.rows > 0} onChange={refresh} />
        <TaxonomyProject key={`classifier:${exchangeKey}`} role="classifier" context={classificationContext} url={discovery.classifier_url} disabled={locked || !hasFramework} canExport={hasFramework} onChange={refresh} />
        <p className="eyebrow">1. Clasificar incidencias</p>
        <HelixClassifier key={`helix:${exchangeKey}`} context={classificationContext} mode={activeMode} url={discovery.helix_classifier_url} disabled={locked || !hasFramework} onChange={refresh} />
        <article className="settings-subsection"><p className="eyebrow">2. Vincular</p><h3>Vinculación Helix ↔ VoC</h3><ClassificationEngineControl key={`linking:${exchangeKey}`} kind="helix" context={classificationContext} disabled={locked || !hasFramework} onChange={refresh} /></article>
      </> : <p>Los intercambios LLM están disponibles en el dataset local.</p>}
      {available.some(item => item.mode === "DISCOVERED") ? <TaxonomyExplorer context={context} mode="DISCOVERED" /> : null}
    </div>}
    <details><summary>Comparar taxonomías</summary><div className="field-grid">
      <label>Taxonomía de partida<select value={left} onChange={e => setLeft(e.target.value as TaxonomyMode)}>{available.map(item => <option key={item.mode} value={item.mode}>{NAMES[item.mode]}</option>)}</select></label>
      <label>Comparar con<select value={right} onChange={e => setRight(e.target.value as TaxonomyMode)}>{data.taxonomies.map(item => <option key={item.mode} value={item.mode} disabled={!item.available || item.selectable === false}>{NAMES[item.mode]}</option>)}</select></label>
    </div>{available.some(item => item.mode === right) && available.some(item => item.mode === left) ? <TaxonomyExplorer key={`${left}-${right}`} context={context} left={left} right={right} /> : null}</details>
    {busy ? <p role="status">Procesando…</p> : null}{message ? <p role="status">{message}</p> : null}
  </section>;
}

function TaxonomyExplorer({ context, mode, left, right }: { context: TaxonomyContext; mode?: TaxonomyMode; left?: TaxonomyMode; right?: TaxonomyMode }) {
  const [open, setOpen] = useState(false);
  const [offset, setOffset] = useState(0);
  const compare = !mode;
  const path = compare ? "/compare" : "/explore";
  const params = { ...context, ...(mode ? { mode } : { left: left!, right: right! }), offset: String(offset) };
  const { data: exploration, error, isLoading } = useSWR(open ? taxonomyUrl(path, params) : null, () => taxonomyRequest<Exploration>(path, params));
  return <details onToggle={event => setOpen(event.currentTarget.open)}><summary>{mode ? `Explorar ${NAMES[mode]}` : "Ver comparación"}</summary>
    {isLoading ? <p role="status">Cargando…</p> : null}
    {error ? <p role="alert">{error.message}</p> : null}
    {exploration ? <div className="taxonomy-exploration">
      <p>{exploration.note || exploration.audit?.note}</p>
      {exploration.audit ? <p>Subpalancas bajo varias Palancas: {exploration.audit.multi_parent_sublevers.join(", ") || "ninguna"}. Categorías genéricas: {exploration.audit.generic_labels.join(", ") || "ninguna"}.</p> : null}
      {exploration.rows.length ? <><div className="table-scroll"><table><thead><tr>{(compare ? ["Origen", "Destino", "Volumen", "Share"] : ["Palanca", "Subpalanca", "Volumen", "Share", "NPS", "Promotores / Neutros / Detractores", "Ejemplos"]).map(label => <th key={label}>{label}</th>)}</tr></thead><tbody>{exploration.rows.map((row, i) => <tr key={i}>{compare ? <><td>{row.from_lever} &gt; {row.from_sublever}</td><td>{row.to_lever} &gt; {row.to_sublever}</td><td>{row.volume}</td><td>{formatPercentage(row.share)}</td></> : <><td>{row.Palanca}</td><td>{row.Subpalanca}</td><td>{row.volume}</td><td>{formatPercentage(row.share)}</td><td>{formatMetric(row.nps)}</td><td>{row.promoters} / {row.neutrals} / {row.detractors}</td><td>{Array.isArray(row.examples) ? row.examples.map((text, index) => <p key={index}>{text}</p>) : null}</td></>}</tr>)}</tbody></table></div>
      <div className="inline-actions"><button disabled={isLoading || offset === 0} onClick={() => setOffset(Math.max(0, offset - 100))}>Anterior</button><span>{offset + 1}–{offset + exploration.rows.length} de {exploration.total}</span><button disabled={isLoading || offset + 100 >= exploration.total} onClick={() => setOffset(offset + 100)}>Siguiente</button></div></> : null}
    </div> : null}
  </details>;
}

export function TaxonomyIngestNotice({ context, revision, onOpen }: { context: TaxonomyContext; revision: string; onOpen: () => void }) {
  const { data } = useSWR([taxonomyUrl("", context), revision], () => taxonomyRequest<TaxonomyStatus>("", context));
  if (!data?.detection.rows) return null;
  const { state, missing } = data.detection;
  return <article className="settings-subsection"><h3>Taxonomía del dataset</h3><p>{state === "MISSING" ? "El fichero contiene comentarios NPS pero no incluye Palanca/Subpalanca. Puedes analizarlo igualmente." : state === "PARTIAL" ? `${formatVolume(missing)} respuestas no tienen clasificación completa.` : state === "NO_TEXT" ? "No hay comentarios utilizables para completar o descubrir temas." : "Taxonomía detectada."}</p><button className="secondary-button" onClick={onOpen}>{state === "MISSING" ? "Descubrir taxonomía" : "Abrir Taxonomy Studio"}</button></article>;
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
    <label>Qué guardar<select value={data.policy} disabled={disabled || busy} onChange={e => void perform(() => taxonomyRequest("/settings", context, jsonRequest("PUT", { policy: e.target.value })))}><option value="ACTIVE_ONLY">Solo activa</option><option value="SOURCE_AND_ACTIVE">Original y activa</option><option value="ALL_AVAILABLE">Todas las disponibles</option></select></label>
    <a className="secondary-button" href={taxonomyUrl("/snapshot", context)} download>Guardar snapshot local</a>
    <label>Restaurar snapshot<input type="file" accept=".json" disabled={disabled || busy} onChange={e => { const file = e.target.files?.[0]; if (!file) return; const form = new FormData(); form.append("file", file); void perform(() => taxonomyRequest("/restore", context, { method: "POST", body: form })); e.target.value = ""; }} /></label>
    {data.restored ? <><p>Snapshot histórico activo. Sus datos y asignaciones permanecen congelados.</p><button className="secondary-button" disabled={disabled || busy} onClick={() => void perform(() => taxonomyRequest("/resume", context, { method: "POST" }))}>Volver al dataset local</button></> : null}
    {message ? <p role="alert">{message}</p> : null}
  </section>;
}
