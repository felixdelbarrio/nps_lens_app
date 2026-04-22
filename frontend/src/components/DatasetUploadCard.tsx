import { FormEvent, useEffect, useRef, useState } from "react";

import {
  canUseDesktopFileBridge,
  pickDesktopExcelFile,
  type DatasetStatus,
  type HelixUploadResult,
  type Issue,
  type UploadResult,
  type UploadSelectionPayload
} from "../api";
import { formatNumber } from "../utils/numberFormat";
import { IssueList } from "./IssueList";

type UploadFeedback = Pick<UploadResult, "status" | "filename" | "issues"> | HelixUploadResult | null;

type DatasetUploadCardProps = {
  eyebrow: string;
  title: string;
  description: string;
  ctaLabel: string;
  datasetStatus: DatasetStatus;
  uploading: boolean;
  onSubmit: (payload: UploadSelectionPayload) => Promise<void>;
  feedback: UploadFeedback;
  testId: string;
};

export function DatasetUploadCard({
  eyebrow,
  title,
  description,
  ctaLabel,
  datasetStatus,
  uploading,
  onSubmit,
  feedback,
  testId
}: DatasetUploadCardProps) {
  const [file, setFile] = useState<File | null>(null);
  const [desktopPickerAvailable, setDesktopPickerAvailable] = useState(() =>
    canUseDesktopFileBridge()
  );
  const [desktopFile, setDesktopFile] = useState<{ path: string; name: string } | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    function syncDesktopBridgeAvailability() {
      setDesktopPickerAvailable(canUseDesktopFileBridge());
    }

    syncDesktopBridgeAvailability();
    window.addEventListener("pywebviewready", syncDesktopBridgeAvailability);
    return () => window.removeEventListener("pywebviewready", syncDesktopBridgeAvailability);
  }, []);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (desktopPickerAvailable && desktopFile) {
      await onSubmit({
        desktopFilePath: desktopFile.path,
        desktopFileName: desktopFile.name
      });
      setDesktopFile(null);
      setFile(null);
      if (inputRef.current) {
        inputRef.current.value = "";
      }
      return;
    }
    if (!file) {
      return;
    }
    await onSubmit({ file });
    setFile(null);
    setDesktopFile(null);
    if (inputRef.current) {
      inputRef.current.value = "";
    }
  }

  async function handlePickDesktopFile() {
    const selectedFile = await pickDesktopExcelFile();
    if (!selectedFile) {
      return;
    }
    setDesktopFile(selectedFile);
    setFile(null);
    if (inputRef.current) {
      inputRef.current.value = "";
    }
  }

  const feedbackIssues: Issue[] = feedback?.issues || [];
  const hasSelectedFile = Boolean(file || desktopFile);

  return (
    <form className="panel sidebar-panel" onSubmit={handleSubmit}>
      <div className="panel-heading">
        <div>
          <p className="eyebrow">{eyebrow}</p>
          <h2>{title}</h2>
        </div>
      </div>

      <p className="panel-copy">{description}</p>

      <div className="dataset-status-row">
        <span className={`dataset-status-badge dataset-status-${datasetStatus.status}`}>
          {datasetStatus.available ? "Dataset activo" : "Sin dataset"}
        </span>
        <span className="dataset-status-copy">
          {datasetStatus.available
            ? `${formatNumber(datasetStatus.rows, { fallback: "0" })} filas`
            : "Pendiente de importar"}
        </span>
      </div>

      <div className="field-grid single-column">
        <label>
          <span>Fichero</span>
          {desktopPickerAvailable ? (
            <>
              <div className="inline-actions">
                <button
                  className="secondary-button"
                  onClick={() => void handlePickDesktopFile()}
                  type="button"
                >
                  Seleccionar fichero
                </button>
              </div>
              <input
                data-testid={testId}
                placeholder="Ningún fichero seleccionado"
                readOnly
                type="text"
                value={desktopFile?.name || ""}
              />
              <span className="field-hint">
                El selector nativo de escritorio evita el fallo del navegador embebido al elegir Excel.
              </span>
            </>
          ) : (
            <input
              ref={inputRef}
              accept=".xlsx,.xlsm,.xls"
              data-testid={testId}
              onChange={(event) => {
                setFile(event.target.files?.[0] ?? null);
                setDesktopFile(null);
              }}
              type="file"
            />
          )}
        </label>
      </div>

      <button className="primary-button" disabled={uploading || !hasSelectedFile} type="submit">
        {uploading ? "Importando..." : ctaLabel}
      </button>

      {feedback ? (
        <div className="inline-feedback">
          <div className="inline-feedback-header">
            <strong>{feedback.filename}</strong>
            <span>{feedback.status}</span>
          </div>
          <IssueList
            emptyMessage="La última carga no generó avisos ni errores."
            issues={feedbackIssues}
          />
        </div>
      ) : null}
    </form>
  );
}
