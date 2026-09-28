import type { TaxonomyMode } from "../api";
export const TAXONOMY_NAMES: Record<TaxonomyMode, string> = { SOURCE: "Origen", NORMALIZED: "Origen + Normalizada", COMPLETED: "Completada", COMPLETED_NORMALIZED: "Completada + Normalizada", DISCOVERED: "Descubierta por LLM" };
