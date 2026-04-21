type RecordTableProps = {
  rows: Array<Record<string, unknown>>;
  emptyMessage: string;
  testId?: string;
};

function formatCellValue(value: unknown) {
  if (value === null || value === undefined) {
    return "";
  }
  if (Array.isArray(value)) {
    return value
      .map((item) => (typeof item === "object" && item !== null ? JSON.stringify(item) : String(item)))
      .join(", ");
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export function RecordTable({ rows, emptyMessage, testId }: RecordTableProps) {
  if (!rows.length) {
    return <p className="empty-state">{emptyMessage}</p>;
  }

  const columns = Object.keys(rows[0] || {});

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
                <td key={`${rowIndex}-${column}`}>{formatCellValue(row[column])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
