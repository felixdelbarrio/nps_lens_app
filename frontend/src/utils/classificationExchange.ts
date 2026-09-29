import { taxonomyRequest, type TaxonomyContext } from "../api";

type ExportResult = { saved_path: string | null };

export async function exportExchange(context: TaxonomyContext, endpoint: string) {
  const result = await taxonomyRequest<ExportResult>(endpoint, context, { method: "POST" });
  return result.saved_path ? `ZIP guardado en ${result.saved_path}` : "Clasificación completa. No quedan comentarios pendientes.";
}

export async function prepareNextZip(context: TaxonomyContext, endpoint: string, pending: number) {
  if (!pending) return "";
  try {
    const result = await taxonomyRequest<ExportResult>(endpoint, context, { method: "POST" });
    return result.saved_path ? ` Siguiente ZIP preparado en ${result.saved_path}` : " Clasificación completa. No quedan pendientes.";
  } catch (error) {
    return ` No se pudo preparar el siguiente ZIP: ${error instanceof Error ? error.message : "Error de exportación."} Puedes volver a intentarlo con el botón de exportación; la importación se conserva.`;
  }
}
