import { useState } from "react";
import useSWR, { useSWRConfig } from "swr";
import { isDiscoverySettingsKey, taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyDiscoverySettings } from "../api";
import { ProjectUrlField } from "./ProjectUrlField";
import { TaxonomyProjectInstructions } from "./TaxonomyProjectInstructions";

export function SingleZipSettings({ context, disabled }: { context: TaxonomyContext; disabled: boolean }) {
  const { data, error, mutate: mutateSettings } = useSWR(taxonomyUrl("/discovery", context), () => taxonomyRequest<TaxonomyDiscoverySettings>("/discovery", context));
  const { mutate } = useSWRConfig();
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState("");
  async function toggle(enabled: boolean) {
    setSaving(true); setMessage("");
    try {
      const result = await mutateSettings(() => taxonomyRequest<TaxonomyDiscoverySettings>("/discovery", context, {
        method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ single_zip_enabled: enabled })
      }), { optimisticData: current => ({ ...current!, single_zip_enabled: enabled }), rollbackOnError: true, revalidate: false });
      await mutate(isDiscoverySettingsKey, result, { revalidate: false });
    } catch (error) { setMessage(error instanceof Error ? error.message : "No se pudo guardar la configuración."); }
    finally { setSaving(false); }
  }
  const locked = disabled || saving;
  return <article className="settings-subsection">
    <div className="settings-subsection-copy">
      <h4>ZIP único con progreso y reanudación</h4>
      <p className="secondary-copy">Sube un ZIP por trabajo a ChatGPT. Importa las entregas parciales y responde Sí para continuar. Conserva el original y las entregas para reanudar.</p>
    </div>
    <label className="checkbox-field"><input type="checkbox" checked={data?.single_zip_enabled ?? false} disabled={locked || !data} onChange={event => void toggle(event.target.checked)} /> Activar ZIP único</label>
    <p className="field-hint">Desactivado: se mantiene la descarga de archivos numerados. Configura dos proyectos con las nuevas instrucciones para usar ZIP único.</p>
    {data ? <div className="settings-section-stack">
      {([{ role: "classifier", field: "classifier_single_zip_url", label: "Comentarios · ZIP único" }, { role: "helix", field: "helix_single_zip_url", label: "Incidencias · ZIP único" }] as const).map(({ role, field, label }) => <div key={role}>
        <ProjectUrlField context={context} field={field} label={`URL · ${label}`} url={data[field]} disabled={locked} />
        <TaxonomyProjectInstructions role={role} context={context} singleZip />
      </div>)}
    </div> : null}
    {error ? <p role="alert">No se pudo cargar la configuración de ZIP único.</p> : null}
    {message ? <p role="alert">{message}</p> : null}
  </article>;
}
