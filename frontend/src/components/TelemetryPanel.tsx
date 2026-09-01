import { useEffect, useState } from "react";

import {
  completeArtifactDownload,
  downloadTelemetry,
  fetchTelemetry,
  type TelemetryPayload
} from "../api";

export function TelemetryPanel({ disabled = false }: { disabled?: boolean }) {
  const [value, setValue] = useState<TelemetryPayload | null>(null);
  const [error, setError] = useState("");
  const [downloadStatus, setDownloadStatus] = useState("");
  useEffect(() => {
    void fetchTelemetry().then(setValue).catch((caught: Error) => setError(caught.message));
  }, []);
  return (
    <div className="settings-section-stack">
      <article className="settings-subsection">
        <div className="settings-subsection-copy">
          <h4>Diagnóstico de rendimiento</h4>
          <p className="secondary-copy">
            Métricas acotadas de latencia, CPU, memoria y errores. No captura consultas, cuerpos ni opiniones.
          </p>
        </div>
        {value ? (
          <div className="telemetry-grid">
            {Object.entries(value.summary).map(([label, metric]) => (
              <div className="telemetry-metric" key={label}>
                <span>{label}</span>
                <strong>{typeof metric === "object" ? JSON.stringify(metric) : String(metric)}</strong>
              </div>
            ))}
          </div>
        ) : <p className="field-hint">{error || "Calculando telemetría…"}</p>}
      </article>
      <div className="settings-subsection-actions">
        <button
          className="primary-button"
          disabled={disabled}
          onClick={() => {
            setError("");
            setDownloadStatus("");
            void downloadTelemetry()
              .then(async (artifact) => {
                const savedPath = await completeArtifactDownload(artifact);
                setDownloadStatus(savedPath ? `Fichero guardado en ${savedPath}` : "Descarga completada correctamente.");
              })
              .catch((caught: Error) => setError(caught.message));
          }}
          type="button"
        >
          Descargar JSON para CODEX
        </button>
        {downloadStatus ? <p className="field-hint" role="status">{downloadStatus}</p> : null}
      </div>
    </div>
  );
}
