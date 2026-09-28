import { useEffect, useState } from "react";
import { useSWRConfig } from "swr";
import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyDiscoverySettings } from "../api";

export function ProjectUrlField({ context, field, label, url, disabled }: { context: TaxonomyContext; field: keyof TaxonomyDiscoverySettings; label: string; url: string; disabled: boolean }) {
  const [value, setValue] = useState(url);
  const [message, setMessage] = useState("");
  const [saving, setSaving] = useState(false);
  const { mutate } = useSWRConfig();
  useEffect(() => setValue(url), [url]);
  async function save() {
    if (value === url) return;
    setSaving(true); setMessage("");
    try {
      const result = await taxonomyRequest<TaxonomyDiscoverySettings>("/discovery", context, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ [field]: value }) });
      await mutate(taxonomyUrl("/discovery", context), result, { revalidate: false });
      setMessage("URL guardada.");
    } catch (error) { setMessage(error instanceof Error ? error.message : "No se pudo guardar la URL."); }
    finally { setSaving(false); }
  }
  return <div><label>{label}<input type="url" value={value} disabled={disabled || saving} onChange={e => setValue(e.target.value)} onBlur={() => void save()} onKeyDown={e => { if (e.key === "Enter") e.currentTarget.blur(); }} /></label>
    <a href={url} target="_blank" rel="noreferrer">Abrir {label.replace("URL · ", "")}</a>
    {message ? <p role="status">{message}</p> : null}
  </div>;
}
