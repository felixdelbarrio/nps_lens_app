import { taxonomyUrl, type TaxonomyContext, type TaxonomyMode } from "../api";

export function TaxonomyDownload({ context, mode, proposal = false }: { context: TaxonomyContext; mode: TaxonomyMode; proposal?: boolean }) {
  return <a className="secondary-button" href={taxonomyUrl("/export", { ...context, mode, ...(proposal ? { proposal: "true" } : {}) })} download>
    Descargar taxonomía en Excel
  </a>;
}
