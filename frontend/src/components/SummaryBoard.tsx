import type { Summary } from "../api";

type SummaryBoardProps = {
  summary: Summary | null;
  processing: boolean;
  onRefresh: () => Promise<void>;
};

function formatPercent(value: number | null) {
  if (value === null) {
    return "n/d";
  }
  return `${(value * 100).toFixed(1)}%`;
}

function formatNps(value: number | null) {
  if (value === null) {
    return "n/d";
  }
  return value.toFixed(1);
}

export function SummaryBoard({ summary, processing, onRefresh }: SummaryBoardProps) {
  const cards = [
    {
      label: "Histórico consolidado",
      value: summary?.total_records?.toLocaleString("es-ES") || "0"
    },
    {
      label: "NPS acumulado",
      value: formatNps(summary?.overall_nps ?? null)
    },
    {
      label: "Promotores",
      value: formatPercent(summary?.promoter_rate ?? null)
    },
    {
      label: "Detractores",
      value: formatPercent(summary?.detractor_rate ?? null)
    }
  ];

  return (
    <section className="panel summary-panel">
      <div className="panel-heading summary-header">
        <div>
          <p className="eyebrow">Resultado acumulado</p>
          <h2>Resumen operativo</h2>
        </div>
        <button
          className="ghost-button"
          data-testid="reprocess-button"
          disabled={processing}
          onClick={() => void onRefresh()}
          type="button"
        >
          {processing ? "Actualizando..." : "Reprocesar agregados"}
        </button>
      </div>

      <div className="summary-cards">
        {cards.map((card) => (
          <article className="summary-card" key={card.label}>
            <span>{card.label}</span>
            <strong>{card.value}</strong>
          </article>
        ))}
      </div>

      <div className="driver-grid">
        {Object.entries(summary?.top_drivers || {}).map(([dimension, items]) => (
          <article className="driver-card" key={dimension}>
            <h3>{dimension}</h3>
            <ul>
              {items.map((item) => (
                <li key={`${dimension}-${String(item.value)}`}>
                  <div>
                    <strong>{String(item.value)}</strong>
                    <span>{Number(item.n || 0).toLocaleString("es-ES")} respuestas</span>
                  </div>
                  <em>{Number(item.nps || 0).toFixed(1)}</em>
                </li>
              ))}
            </ul>
          </article>
        ))}
      </div>
    </section>
  );
}
