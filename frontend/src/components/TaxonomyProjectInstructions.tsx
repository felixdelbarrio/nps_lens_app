import { useState } from "react";
import useSWR from "swr";

import { taxonomyRequest, taxonomyUrl, type TaxonomyContext, type TaxonomyProjectInstructions as Instructions } from "../api";

const TITLES = { designer: "Crea Taxonomía", classifier: "Clasifica taxonomía" };

export function TaxonomyProjectInstructions({ role, context }: { role: "designer" | "classifier"; context: TaxonomyContext }) {
  const { data, error, mutate } = useSWR(
    taxonomyUrl("/discovery/instructions", context),
    () => taxonomyRequest<Instructions>("/discovery/instructions", context),
    { revalidateOnFocus: false, revalidateOnReconnect: false }
  );
  const [expanded, setExpanded] = useState(false);
  const [message, setMessage] = useState("");
  const title = TITLES[role];

  async function copy() {
    if (!data) return;
    try {
      if (!navigator.clipboard?.writeText) throw new Error("Clipboard unavailable");
      await navigator.clipboard.writeText(data[role]);
      setMessage("Instrucciones copiadas. Pégalas en las instrucciones del proyecto.");
    } catch {
      setExpanded(true);
      setMessage("No se pudo acceder al portapapeles. Selecciona el texto y cópialo manualmente.");
    }
  }

  return <div className="taxonomy-project-instructions">
    <button type="button" className="secondary-button" disabled={!data} onClick={() => void copy()}>
      Copiar instrucciones de {title}
    </button>
    {error ? <p role="alert">No se pudieron cargar las instrucciones. <button type="button" onClick={() => void mutate()}>Reintentar</button></p> : null}
    {data ? <details open={expanded} onToggle={event => setExpanded(event.currentTarget.open)}>
      <summary>Ver instrucciones de {title}</summary>
      <p className="field-hint">Versión {data.version} · El batch utiliza estas mismas reglas.</p>
      <textarea aria-label={`Instrucciones de ${title}`} readOnly rows={12} value={data[role]} onFocus={event => event.currentTarget.select()} />
    </details> : null}
    {message ? <p role="status">{message}</p> : null}
  </div>;
}
