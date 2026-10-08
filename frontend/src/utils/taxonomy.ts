import type { TaxonomyMode, TaxonomyStatus } from "../api";
export const TAXONOMY_NAMES: Record<TaxonomyMode, string> = { SOURCE: "Taxonomía Original", COMPLETED: "Taxonomía Manual", DISCOVERED: "Descubierta por LLM" };

export const PROJECT_NAMES = { semantic: "Crear similitud semántica", designer: "Crear Taxonomía", classifier: "Clasifica comentarios", helix: "Clasifica incidencias", normalizer: "Unifica conceptos" };

export function taxonomyCounts(catalog: NonNullable<TaxonomyStatus["proposed_discovered_taxonomy"]>) {
  return { levers: catalog.taxonomy.length, sublevers: catalog.taxonomy.reduce((sum, branch) => sum + branch.sublevers.length, 0) };
}
