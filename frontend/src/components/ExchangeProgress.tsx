import { formatPercentage, formatVolume } from "../utils/numberFormat";

export const ADDITIONAL_TOPICS = { label: "Con más de una categoría", description: "Registros con una categoría principal y al menos una adicional. Cada registro cuenta una sola vez en los totales y en el NPS." };

export type ExchangeCounts = { total: number; received: number; pending: number };
export function ExchangeProgress({ counts, unit, metrics = [] }: { counts: ExchangeCounts; unit: string; metrics?: Array<{ label: string; value: number; description?: string }> }) {
  return <section className="exchange-progress" aria-label={`Progreso de ${unit}`}>
    <div className="exchange-progress-heading"><p><strong>{formatVolume(counts.received)}</strong> de {formatVolume(counts.total)} {unit} procesados</p><span>{formatPercentage(counts.total ? counts.received / counts.total : 0)}</span></div>
    <progress aria-label={`Progreso de ${unit}`} value={counts.received} max={counts.total || 1} />
    <p className="field-hint">{formatVolume(counts.pending)} pendientes</p>
    {metrics.length ? <div className="metric-grid">{metrics.map(metric => <div className="metric-card" key={metric.label}><span>{metric.label}</span><strong>{formatVolume(metric.value)}</strong>{metric.description ? <p className="field-hint">{metric.description}</p> : null}</div>)}</div> : null}
  </section>;
}
