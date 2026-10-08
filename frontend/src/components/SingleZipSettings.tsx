import { useState } from "react";
import useSWR, { useSWRConfig } from "swr";
import { isDiscoverySettingsKey, taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyDiscoverySettings } from "../api";

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
      }), { revalidate: false });
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
    <label className="switch-field"><input type="checkbox" role="switch" aria-label="Activar ZIP único" checked={data?.single_zip_enabled ?? false} disabled={locked || !data} onChange={event => void toggle(event.target.checked)} /><span>Activar ZIP único</span></label>
    <p className="field-hint">Gestiona las URLs, instrucciones y entregas en Clasifica comentarios y Clasifica incidencias. Desactivado: ZIP numerados. Activado: un ZIP con entregas parciales y reanudación.</p>
    {error ? <p role="alert">No se pudo cargar la configuración de ZIP único.</p> : null}
    {message ? <p role="alert">{message}</p> : null}
  </article>;
}
