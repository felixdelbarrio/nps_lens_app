import type { HelixUploadHistoryItem } from "../api";
import { toBusinessCopy } from "../utils/businessCopy";
import { formatNumber } from "../utils/numberFormat";

type HelixUploadsTableProps = {
  uploads: HelixUploadHistoryItem[];
  onDeleteUpload: (upload: HelixUploadHistoryItem) => void;
};

export function HelixUploadsTable({ uploads, onDeleteUpload }: HelixUploadsTableProps) {
  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Incidencias</p>
          <h2>Histórico de ingestas</h2>
        </div>
      </div>
      <div className="table-shell">
        <table className="data-table">
          <thead>
            <tr><th>Fichero</th><th>Filas</th><th>Timestamp</th><th /></tr>
          </thead>
          <tbody>
            {uploads.map((upload) => (
              <tr key={upload.upload_id}>
                <td><strong>{toBusinessCopy(upload.filename)}</strong></td>
                <td>{formatNumber(upload.row_count, { fallback: "0" })}</td>
                <td>{new Date(upload.uploaded_at).toLocaleString("es-ES")}</td>
                <td>
                  <button
                    className="secondary-button danger-button"
                    onClick={() => onDeleteUpload(upload)}
                    type="button"
                  >
                    Borrar ingesta
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
