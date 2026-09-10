type Segment = { text: string; bold: boolean };

export function EvidenceText({ text, segments }: { text: string; segments: unknown }) {
  if (!Array.isArray(segments) || !segments.length) return <>{text}</>;
  return <>{(segments as Segment[]).map((segment, index) => segment.bold
    ? <strong key={index}>{segment.text}</strong>
    : <span key={index}>{segment.text}</span>)}</>;
}
