import { formatPercentage, formatVolume } from "../utils/numberFormat";

export type ExchangeCounts = { total: number; received: number; pending: number };
export function ExchangeProgress({ counts, unit, metrics = [] }: { counts: ExchangeCounts; unit: string; metrics?: Array<{ label: string; value: number }> }) {
  return <section className="exchange-progress" aria-label={`Progreso de ${unit}`}>
    <div className="exchange-progress-heading"><p><strong>{formatVolume(counts.received)}</strong> de {formatVolume(counts.total)} {unit} procesados</p><span>{formatPercentage(counts.total ? counts.received / counts.total : 0)}</span></div>
    <progress aria-label={`Progreso de ${unit}`} value={counts.received} max={counts.total || 1} />
    <p className="field-hint">{formatVolume(counts.pending)} pendientes</p>
    {metrics.length ? <div className="metric-grid">{metrics.map(metric => <div className="metric-card" key={metric.label}><span>{metric.label}</span><strong>{formatVolume(metric.value)}</strong></div>)}</div> : null}
  </section>;
}
