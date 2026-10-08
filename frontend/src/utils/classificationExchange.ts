import { taxonomyRequest, type TaxonomyContext } from "../api";

export type ClassificationExport = { saved_paths: string[]; saved_directory: string | null; batches: number; single_zip?: boolean };

export function exportClassification(context: TaxonomyContext, endpoint: string) {
  return taxonomyRequest<ClassificationExport>(endpoint, context, { method: "POST" });
}

export function classificationImportMessage(pending: number, singleZip = false) {
  return pending ? (singleZip ? "Responde Sí en ChatGPT para recibir el siguiente lote e importa cada entrega. El progreso se acumula." : "Continúa con los ZIP que ya has descargado. El progreso se acumula.") : "Clasificación completa. No quedan pendientes.";
}
