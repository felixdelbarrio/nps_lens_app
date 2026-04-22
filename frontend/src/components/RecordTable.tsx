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

  const columns = (providedColumns?.length ? providedColumns : Object.keys(rows[0] || {})).filter(
    (column) => !column.endsWith("__href") && !column.endsWith("__hyperlink")
  );

  function renderCell(row: Record<string, unknown>, column: string, rowIndex: number) {
    const hrefCandidate = row[`${column}__href`] ?? row[`${column}__hyperlink`];
    const href = typeof hrefCandidate === "string" ? hrefCandidate.trim() : "";
    const label = formatDisplayValue(row[column], column);
    if (!href || !label) {
      return label;
    }
    return (
      <a
        className="table-link"
        href={href}
        key={`${rowIndex}-${column}-link`}
        rel="noreferrer"
        target="_blank"
      >
        {label}
      </a>
    );
  }

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
                <td key={`${rowIndex}-${column}`}>{renderCell(row, column, rowIndex)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
