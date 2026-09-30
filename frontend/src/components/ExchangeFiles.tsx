import type { ClassificationExport } from "../utils/classificationExchange";

export function ExchangeFiles({ result }: { result: ClassificationExport | null }) {
  if (!result) return null;
  if (!result.saved_paths.length) return <p role="status">Clasificación completa. No quedan pendientes.</p>;
  return <section aria-label="ZIP preparados">
    <p>{result.saved_paths.length} ZIP preparados. Procesa cada archivo por separado e importa su respuesta.</p>
    <p className="field-hint">Carpeta: {result.saved_directory}</p>
    <ul>{result.saved_paths.map(path => <li key={path}>{path.split(/[\\/]/).pop()}</li>)}</ul>
  </section>;
}
