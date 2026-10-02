import { taxonomyRequest, type TaxonomyContext } from "../api";

export type ClassificationExport = { saved_paths: string[]; saved_directory: string | null; batches: number };

export function exportClassification(context: TaxonomyContext, endpoint: string) {
  return taxonomyRequest<ClassificationExport>(endpoint, context, { method: "POST" });
}

export function classificationImportMessage(pending: number) {
  return pending ? "Continúa con los ZIP que ya has descargado. El progreso se acumula." : "Clasificación completa. No quedan pendientes.";
}
