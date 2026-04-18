import { FormEvent, useRef, useState } from "react";

import type { DatasetStatus, HelixUploadResult, Issue, UploadResult } from "../api";
import { IssueList } from "./IssueList";

type UploadFeedback = Pick<UploadResult, "status" | "filename" | "issues"> | HelixUploadResult | null;

type DatasetUploadCardProps = {
  eyebrow: string;
  title: string;
  description: string;
  ctaLabel: string;
  datasetStatus: DatasetStatus;
  uploading: boolean;
  onSubmit: (payload: { file: File; sheetName: string }) => Promise<void>;
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
  const [sheetName, setSheetName] = useState("");
  const inputRef = useRef<HTMLInputElement | null>(null);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!file) {
      return;
    }
    await onSubmit({ file, sheetName });
    setFile(null);
    setSheetName("");
    if (inputRef.current) {
      inputRef.current.value = "";
    }
  }

  const feedbackIssues: Issue[] = feedback?.issues || [];

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
          {datasetStatus.available ? `${datasetStatus.rows.toLocaleString("es-ES")} filas` : "Pendiente de importar"}
        </span>
      </div>

      <div className="field-grid single-column">
        <label>
          <span>Fichero</span>
          <input
            ref={inputRef}
            accept=".xlsx,.xlsm,.xls"
            data-testid={testId}
            onChange={(event) => setFile(event.target.files?.[0] ?? null)}
            type="file"
          />
        </label>
        <label>
          <span>Sheet</span>
          <input
            onChange={(event) => setSheetName(event.target.value)}
            placeholder="Opcional"
            value={sheetName}
          />
        </label>
      </div>

      <button className="primary-button" disabled={uploading || !file} type="submit">
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
