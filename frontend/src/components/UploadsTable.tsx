import { useDeferredValue } from "react";

import type { UploadResult } from "../api";

type UploadsTableProps = {
  uploads: UploadResult[];
  filter: string;
  onFilterChange: (value: string) => void;
  activeUploadId: string | null;
  onSelectUpload: (uploadId: string) => void;
};

export function UploadsTable({
  uploads,
  filter,
  onFilterChange,
  activeUploadId,
  onSelectUpload
}: UploadsTableProps) {
  const deferredFilter = useDeferredValue(filter);
  const normalizedFilter = deferredFilter.trim().toLowerCase();
  const visibleUploads = uploads.filter((upload) => {
    if (!normalizedFilter) {
      return true;
    }
    return [upload.filename, upload.status, upload.service_origin, upload.service_origin_n1]
      .join(" ")
      .toLowerCase()
      .includes(normalizedFilter);
  });

  return (
    <section className="panel uploads-panel">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Trazabilidad</p>
          <h2>Histórico de cargas</h2>
        </div>
        <input
          data-testid="history-filter-input"
          className="history-filter"
          placeholder="Filtrar histórico"
          value={filter}
          onChange={(event) => onFilterChange(event.target.value)}
        />
      </div>

      <div className="table-shell">
        <table data-testid="uploads-table">
          <thead>
            <tr>
              <th>Fichero</th>
              <th>Estado</th>
              <th>Insertados</th>
              <th>Actualizados</th>
              <th>Duplicados</th>
              <th>Timestamp</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {visibleUploads.map((upload) => {
              const duplicateCount =
                upload.duplicate_in_file_rows + upload.duplicate_historical_rows;
              return (
                <tr
                  className={upload.upload_id === activeUploadId ? "is-active" : ""}
                  key={upload.upload_id}
                >
                  <td>
                    <strong>{upload.filename}</strong>
                    <span>{upload.service_origin_n1}</span>
                  </td>
                  <td>{upload.status}</td>
                  <td>{upload.inserted_rows.toLocaleString("es-ES")}</td>
                  <td>{upload.updated_rows.toLocaleString("es-ES")}</td>
                  <td>{duplicateCount.toLocaleString("es-ES")}</td>
                  <td>{new Date(upload.uploaded_at).toLocaleString("es-ES")}</td>
                  <td>
                    <button
                      className="ghost-button"
                      data-testid={`show-issues-${upload.upload_id}`}
                      onClick={() => onSelectUpload(upload.upload_id)}
                      type="button"
                    >
                      Ver issues
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
