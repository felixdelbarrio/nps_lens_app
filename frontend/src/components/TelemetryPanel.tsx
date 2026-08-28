import { useEffect, useState } from "react";

import {
  canUseDesktopFileBridge,
  downloadTelemetry,
  fetchTelemetry,
  type TelemetryPayload
} from "../api";

function saveBlob(blob: Blob, fileName: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = fileName;
  anchor.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 0);
}

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
              .then(({ blob, fileName, savedPath }) => {
                if (savedPath && canUseDesktopFileBridge()) {
                  setDownloadStatus(`Fichero guardado en ${savedPath}`);
                } else if (blob) {
                  saveBlob(blob, fileName);
                  setDownloadStatus("Descarga preparada correctamente.");
                }
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
