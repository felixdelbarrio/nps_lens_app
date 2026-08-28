const LEGACY_NPS_NAME = /NPS[\s_-]+T[eé]rmico/giu;

export function toBusinessCopy(value: string) {
  return value.normalize("NFC").replace(LEGACY_NPS_NAME, "NPS");
}
