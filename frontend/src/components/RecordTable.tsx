import { formatDisplayValue } from "../utils/numberFormat";

type RecordTableProps = {
  rows: Array<Record<string, unknown>>;
  emptyMessage: string;
  testId?: string;
  columns?: string[];
};

export function RecordTable({ rows, emptyMessage, testId, columns: providedColumns }: RecordTableProps) {
  if (!rows.length) {
    return <p className="empty-state">{emptyMessage}</p>;
  }

  const columns = providedColumns?.length ? providedColumns : Object.keys(rows[0] || {});

  return (
    <div className="table-shell">
      <table className="data-table" data-testid={testId}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={`record-row-${rowIndex}`}>
              {columns.map((column) => (
                <td key={`${rowIndex}-${column}`}>{formatDisplayValue(row[column], column)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
