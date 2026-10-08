import { useState } from "react";
import { downloadTaxonomy, taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";

export function TaxonomyDownload({ context, mode, proposal = false }: { context: TaxonomyContext; mode: TaxonomyMode; proposal?: boolean }) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  async function download() {
    if (busy) return;
    setBusy(true); setMessage("");
    try {
      const path = await downloadTaxonomy(context, mode, proposal);
      setMessage(path ? `Excel guardado en ${path}` : "Excel descargado.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "No se pudo descargar la taxonomía.");
    } finally { setBusy(false); }
  }
  return <div className="taxonomy-download">
    <a className="secondary-button" aria-disabled={busy} href={taxonomyUrl("/export", { ...context, mode, ...(proposal ? { proposal: "true" } : {}) })} download onClick={event => { event.preventDefault(); void download(); }}>
      {busy ? "Descargando…" : "Descargar taxonomía en Excel"}
    </a>
    {message ? <p role="status">{message}</p> : null}
  </div>;
}
