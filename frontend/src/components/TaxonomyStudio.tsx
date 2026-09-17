import { useEffect, useState } from "react";
import useSWR from "swr";

import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyDiscoverySettings, type TaxonomyMode, type TaxonomyStatus } from "../api";

const NAMES: Record<TaxonomyMode, string> = { SOURCE: "Origen", NORMALIZED: "Normalizada", COMPLETED: "Completada", DISCOVERED: "Descubierta" };
const jsonRequest = (method: string, body: unknown) => ({ method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
type Props = { context: TaxonomyContext; onChange: () => Promise<void>; disabled?: boolean };
type Exploration = { rows: Array<Record<string, string | number | string[]>>; total: number; audit?: { multi_parent_sublevers: string[]; generic_labels: string[]; note: string }; note?: string };

export function TaxonomyStudio({ context, onChange, disabled = false }: Props) {
  const { data, error, mutate } = useSWR(taxonomyUrl("", context), () => taxonomyRequest<TaxonomyStatus>("", context));
  const { data: discovery, mutate: mutateDiscovery } = useSWR(
    data?.discovery_local_available ? taxonomyUrl("/discovery", context) : null,
    () => taxonomyRequest<TaxonomyDiscoverySettings>("/discovery", context)
  );
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [certaintyThreshold, setCertaintyThreshold] = useState(0.65);
  const [minF1, setMinF1] = useState(0.65);
  const [discoveryMethod, setDiscoveryMethod] = useState<TaxonomyDiscoverySettings["method"]>("disabled");
  const [designerUrl, setDesignerUrl] = useState("");
  const [classifierUrl, setClassifierUrl] = useState("");
  const [left, setLeft] = useState<TaxonomyMode>("NORMALIZED");
  const [right, setRight] = useState<TaxonomyMode>("DISCOVERED");
  const [exploration, setExploration] = useState<Exploration | null>(null);
  const [view, setView] = useState<{ mode?: TaxonomyMode; compare?: boolean; offset: number }>({ offset: 0 });
  useEffect(() => {
    if (!discovery) return;
    setDiscoveryMethod(discovery.method);
    setDesignerUrl(discovery.designer_url);
    setClassifierUrl(discovery.classifier_url);
  }, [discovery]);
  async function action(run: () => Promise<unknown>, refresh = true) {
    setBusy(true); setMessage("");
    try {
      await run();
      if (refresh) { await mutate(); await onChange(); setExploration(null); }
    } catch (caught) { setMessage(caught instanceof Error ? caught.message : "No se pudo completar la operación."); }
    finally { setBusy(false); }
  }
  async function generate(mode: TaxonomyMode, regenerate: boolean) {
    await action(async () => {
      const config = mode === "COMPLETED" ? { certainty_threshold: certaintyThreshold, min_f1: minF1 } : {};
      const result = await taxonomyRequest<{ cache_hit: boolean }>("/generate", context, jsonRequest("POST", { mode, regenerate, config }));
      setMessage(result.cache_hit ? "Resultado reutilizado de la caché local." : "Taxonomía calculada y guardada en local.");
    });
  }
  async function saveDiscovery(nextMethod = discoveryMethod) {
    await action(async () => {
      await taxonomyRequest("/discovery", context, jsonRequest("PUT", { method: nextMethod, designer_url: designerUrl, classifier_url: classifierUrl }));
      await mutateDiscovery();
      setMessage("Configuración de descubrimiento guardada.");
    }, false);
  }
  async function connectDiscovery() {
    await action(async () => {
      await taxonomyRequest("/discovery/connect", context, { method: "POST" });
      await mutateDiscovery();
      setMessage("ChatGPT conectado.");
    }, false);
  }
  async function disconnectDiscovery() {
    await action(async () => {
      await taxonomyRequest("/discovery/disconnect", context, { method: "POST" });
      await mutateDiscovery();
      setMessage("Sesión local de ChatGPT eliminada.");
    }, false);
  }
  async function explore(next: typeof view) {
    setView(next);
    await action(async () => {
      setExploration(await taxonomyRequest<Exploration>(next.compare ? "/compare" : "/explore", { ...context, ...(next.compare ? { left, right } : { mode: next.mode || "NORMALIZED" }), offset: String(next.offset) }));
    }, false);
  }
  if (error) return <p role="alert">{error.message}</p>;
  if (!data) return <p>Preparando taxonomías…</p>;
  const locked = disabled || busy;
  return <section className="surface-card taxonomy-studio">
    <div className="panel-heading"><div><p className="eyebrow">Análisis local · mismo corpus</p><h2>Taxonomy Studio</h2><p>Lente activa: <strong>{NAMES[data.active]}</strong> · {data.detection.rows.toLocaleString("es")} respuestas</p></div></div>
    {data.detection.originals_unavailable ? <p role="status">{data.detection.originals_unavailable} registros históricos no tienen el fichero original disponible. Origen muestra el valor conservado por la versión anterior.</p> : null}
    {data.requested_active !== data.active ? <p role="status">La lente anterior está desactualizada. Se muestra Normalizada hasta regenerarla.</p> : null}
    <p className="secondary-copy">Completar aprende de etiquetas humanas; descubrir crea una alternativa usando únicamente ID y Comment. La nota NPS no se envía ni participa en el proceso.</p>
    <div className="field-grid">
      <label>Umbral de asignación<input type="number" min={0.5} max={1} step={0.05} value={certaintyThreshold} onChange={e => setCertaintyThreshold(Number(e.target.value))} disabled={locked} /></label>
      <label>Macro F1 mínimo<input type="number" min={0} max={1} step={0.05} value={minF1} onChange={e => setMinF1(Number(e.target.value))} disabled={locked} /></label>
    </div>
    {data.discovery_local_available && discovery ? <article className="settings-subsection taxonomy-discovery-settings">
      <h3>Método de descubrimiento</h3>
      <label>Método<select value={discoveryMethod} disabled={locked} onChange={e => { const next = e.target.value as TaxonomyDiscoverySettings["method"]; setDiscoveryMethod(next); void saveDiscovery(next); }}><option value="disabled">No configurado</option><option value="chatgpt_browser">ChatGPT automatizado</option></select></label>
      {discoveryMethod === "chatgpt_browser" ? <>
        <div className="field-grid">
          <label>Designer URL<input value={designerUrl} disabled={locked} onChange={e => setDesignerUrl(e.target.value)} /></label>
          <label>Classifier URL<input value={classifierUrl} disabled={locked} onChange={e => setClassifierUrl(e.target.value)} /></label>
        </div>
        <p role="status">{discovery.session === "connected" ? "Conectado" : discovery.session === "expired" ? "Sesión caducada" : "No conectado"}</p>
        <div className="inline-actions"><button className="secondary-button" disabled={locked} onClick={() => void saveDiscovery()}>Guardar URLs</button><button className="primary-button" disabled={locked || discovery.session === "connected"} onClick={() => void connectDiscovery()}>Conectar con ChatGPT</button><button className="secondary-button" disabled={locked || discovery.session === "not_connected"} onClick={() => void disconnectDiscovery()}>Desconectar</button></div>
        <p className="field-hint">El acceso se realiza directamente en Chromium. NPS Lens no captura usuario, contraseña ni MFA.</p>
      </> : null}
    </article> : null}
    <div className="taxonomy-cards">{data.taxonomies.map(item => <article className="settings-subsection" key={item.mode}>
      <h3>{NAMES[item.mode]}</h3>
      {item.available ? <><p>{item.levers} Palancas · {item.sublevers} Subpalancas</p><p>{((item.coverage || 0) * 100).toFixed(1)}% cobertura{item.macro_f1 != null ? ` · Macro F1 ${item.macro_f1.toFixed(2)}` : ""}</p>{item.mode === "NORMALIZED" ? <p>{item.equivalence_groups} grupos de equivalencias</p> : null}</> : <p>{item.stale ? "Corpus o configuración modificados; requiere regeneración." : "Aún no creada."}</p>}
      <div className="inline-actions">
        {item.available ? <><button className="secondary-button" disabled={locked} onClick={() => void explore({ mode: item.mode, offset: 0 })}>Explorar {NAMES[item.mode]}</button><button className="primary-button" disabled={locked || data.active === item.mode} onClick={() => void action(() => taxonomyRequest("/settings", context, jsonRequest("PUT", { active: item.mode })))}>Usar como lente</button></> : null}
        {(item.mode === "COMPLETED" || item.mode === "DISCOVERED") && !data.restored ? <button className="secondary-button" disabled={locked || !data.detection.usable_comments || (item.mode === "COMPLETED" && data.detection.state === "MISSING") || (item.mode === "DISCOVERED" && discoveryMethod !== "chatgpt_browser")} onClick={() => void generate(item.mode, item.available || Boolean(item.stale))}>{item.available || item.stale ? "Regenerar" : item.mode === "COMPLETED" ? "Completar" : "Descubrir"}</button> : null}
      </div>
    </article>)}</div>
    <div className="field-grid">
      <label>Taxonomía de partida<select value={left} onChange={e => setLeft(e.target.value as TaxonomyMode)}>{data.taxonomies.filter(t => t.available).map(t => <option key={t.mode} value={t.mode}>{NAMES[t.mode]}</option>)}</select></label>
      <label>Comparar con<select value={right} onChange={e => setRight(e.target.value as TaxonomyMode)}><option value="DISCOVERED" disabled={!data.taxonomies.find(t => t.mode === "DISCOVERED")?.available}>Descubierta</option>{data.taxonomies.filter(t => t.available && t.mode !== "DISCOVERED").map(t => <option key={t.mode} value={t.mode}>{NAMES[t.mode]}</option>)}</select></label>
    </div>
    <button className="secondary-button" disabled={locked || !data.taxonomies.some(t => t.mode === right && t.available)} onClick={() => void explore({ compare: true, offset: 0 })}>Comparar taxonomías</button>
    {busy ? <p role="status">Procesando en local…</p> : null}{message ? <p role="status">{message}</p> : null}
    {exploration ? <div className="taxonomy-exploration">
      <h3>{view.compare ? `${NAMES[left]} → ${NAMES[right]}` : NAMES[view.mode || "NORMALIZED"]}</h3>
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
    <label>Qué guardar<select value={data.policy} disabled={disabled || busy} onChange={e => void perform(() => taxonomyRequest("/settings", context, jsonRequest("PUT", { policy: e.target.value })))}><option value="ACTIVE_ONLY">Solo activa</option><option value="SOURCE_AND_ACTIVE">Origen y activa</option><option value="ALL_AVAILABLE">Todas las disponibles</option></select></label>
    <a className="secondary-button" href={taxonomyUrl("/snapshot", context)} download>Guardar snapshot local</a>
    <label>Restaurar snapshot<input type="file" accept=".json" disabled={disabled || busy} onChange={e => { const file = e.target.files?.[0]; if (!file) return; const form = new FormData(); form.append("file", file); void perform(() => taxonomyRequest("/restore", context, { method: "POST", body: form })); e.target.value = ""; }} /></label>
    {data.restored ? <><p>Snapshot histórico activo. Sus datos y asignaciones permanecen congelados.</p><button className="secondary-button" disabled={disabled || busy} onClick={() => void perform(() => taxonomyRequest("/resume", context, { method: "POST" }))}>Volver al dataset local</button></> : null}
    {message ? <p role="alert">{message}</p> : null}
  </section>;
}
