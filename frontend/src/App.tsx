import { startTransition, useEffect, useState } from "react";
import useSWR from "swr";

import {
  fetchConfig,
  fetchSummary,
  fetchUploads,
  reprocessSummary,
  uploadFile
} from "./api";
import type { Config, Summary, UploadResult } from "./api";
import { SummaryBoard } from "./components/SummaryBoard";
import { UploadForm } from "./components/UploadForm";
import { UploadsTable } from "./components/UploadsTable";

function IssuesPanel({ upload }: { upload: UploadResult | null }) {
  return (
    <aside className="panel issues-panel">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Validación</p>
          <h2>Issues de ingesta</h2>
        </div>
      </div>
      {!upload ? (
        <p className="empty-state">Selecciona una carga para inspeccionar avisos, errores y schema drift.</p>
      ) : (
        <>
          <div className="issue-summary">
            <strong data-testid="selected-upload-name">{upload.filename}</strong>
            <span>{upload.status}</span>
          </div>
          <ul className="issue-list" data-testid="issues-list">
            {upload.issues.length ? (
              upload.issues.map((issue) => (
                <li className={`issue-card issue-${issue.level.toLowerCase()}`} key={`${issue.code}-${issue.message}`}>
                  <span>{issue.level}</span>
                  <strong>{issue.code || "issue"}</strong>
                  <p>{issue.message}</p>
                </li>
              ))
            ) : (
              <li className="issue-card issue-info">
                <span>INFO</span>
                <strong>sin_issues</strong>
                <p>La carga no generó avisos ni errores.</p>
              </li>
            )}
          </ul>
        </>
      )}
    </aside>
  );
}

export function App() {
  const [activeUploadId, setActiveUploadId] = useState<string | null>(null);
  const [status, setStatus] = useState("Cargando contratos...");
  const [filter, setFilter] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isMutating, setIsMutating] = useState(false);

  const {
    data: config,
    error: configError,
    isLoading: isConfigLoading
  } = useSWR<Config>("config", fetchConfig);
  const {
    data: uploads = [],
    error: uploadsError,
    isLoading: isUploadsLoading,
    mutate: mutateUploads
  } = useSWR<UploadResult[]>("uploads", fetchUploads);
  const {
    data: summary,
    error: summaryError,
    isLoading: isSummaryLoading,
    mutate: mutateSummary
  } = useSWR<Summary>("summary", () => fetchSummary());

  const isBusy = isMutating || isConfigLoading || isUploadsLoading || isSummaryLoading;

  useEffect(() => {
    const currentError = configError || uploadsError || summaryError;
    if (currentError) {
      setError(currentError.message);
      setStatus("No se pudo cargar el dashboard.");
      return;
    }
    if (isBusy) {
      setStatus("Sincronizando histórico y agregados...");
      return;
    }
    setError(null);
    setStatus("Dashboard alineado con el histórico persistente.");
  }, [configError, isBusy, summaryError, uploadsError]);

  useEffect(() => {
    if (!uploads.length) {
      return;
    }
    setActiveUploadId((current) => current || uploads[0]?.upload_id || null);
  }, [uploads]);

  async function handleUpload(payload: {
    file: File;
    serviceOrigin: string;
    serviceOriginN1: string;
    serviceOriginN2: string;
  }) {
    setIsMutating(true);
    setError(null);
    setStatus(`Procesando ${payload.file.name}...`);
    try {
      const uploadResponse = await uploadFile(payload);
      await Promise.all([mutateUploads(), mutateSummary()]);
      startTransition(() => {
        setActiveUploadId(uploadResponse.upload_id);
      });
      setStatus(`Carga ${uploadResponse.status}. Insertados ${uploadResponse.inserted_rows}.`);
    } catch (caughtError) {
      const message = caughtError instanceof Error ? caughtError.message : "Error desconocido";
      setError(message);
      setStatus("La carga falló.");
    } finally {
      setIsMutating(false);
    }
  }

  async function handleReprocess() {
    setIsMutating(true);
    setError(null);
    setStatus("Recalculando agregados desde el histórico consolidado...");
    try {
      const summaryResponse = await reprocessSummary();
      await mutateSummary(summaryResponse, { revalidate: false });
      setStatus("Agregados recalculados.");
    } catch (caughtError) {
      const message = caughtError instanceof Error ? caughtError.message : "Error desconocido";
      setError(message);
      setStatus("No se pudieron recalcular los agregados.");
    } finally {
      setIsMutating(false);
    }
  }

  const activeUpload = uploads.find((upload) => upload.upload_id === activeUploadId) || null;

  return (
    <main className="shell">
      <section className="hero">
        <div>
          <p className="eyebrow">NPS Lens 2.0</p>
          <h1>Ingesta acumulativa, historial persistente y UI desacoplada</h1>
        </div>
        <div className="hero-status">
          <span className="status-pill">{isBusy ? "busy" : "ready"}</span>
          <p data-testid="status-copy">{status}</p>
        </div>
      </section>

      {error ? (
        <section className="error-banner" data-testid="error-banner">
          <strong>Fallo operativo</strong>
          <p>{error}</p>
        </section>
      ) : null}

      <div className="layout-grid">
        <UploadForm config={config ?? null} uploading={isBusy} onSubmit={handleUpload} />
        <SummaryBoard
          summary={summary ?? null}
          processing={isBusy}
          onRefresh={handleReprocess}
        />
        <UploadsTable
          uploads={uploads}
          filter={filter}
          onFilterChange={setFilter}
          activeUploadId={activeUploadId}
          onSelectUpload={setActiveUploadId}
        />
        <IssuesPanel upload={activeUpload} />
      </div>
    </main>
  );
}

export default App;
