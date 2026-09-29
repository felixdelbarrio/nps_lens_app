import type { TaxonomyMode } from "../api";
export const TAXONOMY_NAMES: Record<TaxonomyMode, string> = { SOURCE: "Taxonomía Original", COMPLETED: "Taxonomía Manual", DISCOVERED: "Descubierta por LLM" };

export const PROJECT_NAMES = { designer: "Crear Taxonomía", classifier: "Clasifica comentarios", helix: "Clasifica incidencias", normalizer: "Unifica conceptos" };
