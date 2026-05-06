const LOCALE = "es-ES";

const NUMBER_FORMATTER = new Intl.NumberFormat(LOCALE, {
  maximumFractionDigits: 2
});

const SIGNED_NUMBER_FORMATTER = new Intl.NumberFormat(LOCALE, {
  maximumFractionDigits: 2,
  signDisplay: "exceptZero"
});

const PERCENT_FORMATTER = new Intl.NumberFormat(LOCALE, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
  style: "percent"
});
const FIXED_TWO_DECIMAL_FORMATTER = new Intl.NumberFormat(LOCALE, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});

const PERCENT_COLUMN_PATTERN =
  /%|percent|percentage|porcentaje|share|ratio|rate|prob(?:ability|\.)?/i;
const NON_PERCENT_COLUMN_PATTERN =
  /\bpp\b|por 100|per 100|score|confidence|confianza|similaridad|cohesi|corr|lag|eta|pts?\b/i;
const SIGNED_COLUMN_PATTERN = /\bgap\b|brecha|diferencia|difference/i;
const PLAIN_NUMERIC_PATTERN = /^[+-]?\d+(?:[.,]\d+)?$/;
const PERCENT_VALUE_PATTERN = /^([+-]?\d+(?:[.,]\d+)?)\s*%$/;
const NUMERIC_WITH_UNIT_PATTERN =
  /^([+-]?\d+(?:[.,]\d+)?)(\s*(?:pp|pts?|puntos|semanas?|d[ií]as?|links?|incidencias?|comentarios(?:\s+voc)?|respuestas|validados?))$/i;
const PERCENT_TOKENS = [
  "percent",
  "percentage",
  "porcentaje",
  "share",
  "ratio",
  "rate",
  "focus",
  "prob",
  "probability",
  "detractor",
  "promoter",
  "promotores",
  "detractores",
  "neutros",
  "passive"
];
const NON_PERCENT_TOKENS = [
  "pp",
  "por 100",
  "per 100",
  "score",
  "confidence",
  "confianza",
  "similaridad",
  "similitud",
  "cohesion",
  "cohesión",
  "corr",
  "lag",
  "eta",
  "pts",
  "puntos",
  "nps",
  "uplift",
  "gap",
  "brecha",
  "impacto",
  "impact",
  "delta"
];

function normalizeSpacing(value: string) {
  return value.replace(/\u00a0/g, " ");
}

function normalizeColumnName(columnName: string) {
  return columnName
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function coerceFiniteNumber(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!PLAIN_NUMERIC_PATTERN.test(trimmed)) {
    return null;
  }
  const parsed = Number.parseFloat(trimmed.replace(",", "."));
  return Number.isFinite(parsed) ? parsed : null;
}

export function formatNumber(value: unknown, { signed = false, fallback = "—" } = {}) {
  const numeric = coerceFiniteNumber(value);
  if (numeric === null) {
    return fallback;
  }
  return (signed ? SIGNED_NUMBER_FORMATTER : NUMBER_FORMATTER).format(numeric);
}

export function formatPercent(value: unknown, { fallback = "—" } = {}) {
  const numeric = coerceFiniteNumber(value);
  if (numeric === null) {
    return fallback;
  }
  return normalizeSpacing(PERCENT_FORMATTER.format(numeric));
}

function isPercentColumn(columnName?: string) {
  if (!columnName) {
    return false;
  }
  const normalizedColumnName = normalizeColumnName(columnName);
  if (
    NON_PERCENT_COLUMN_PATTERN.test(normalizedColumnName) ||
    NON_PERCENT_TOKENS.some((token) => normalizedColumnName.includes(token))
  ) {
    return false;
  }
  return (
    PERCENT_COLUMN_PATTERN.test(normalizedColumnName) ||
    PERCENT_TOKENS.some((token) => normalizedColumnName.includes(token))
  );
}

function formatPercentString(value: string) {
  const match = value.trim().match(PERCENT_VALUE_PATTERN);
  if (!match) {
    return null;
  }
  const numeric = coerceFiniteNumber(match[1]);
  if (numeric === null) {
    return null;
  }
  return formatPercent(numeric / 100);
}

function shouldDisplaySigned(columnName?: string) {
  if (!columnName) {
    return false;
  }
  const normalizedColumnName = normalizeColumnName(columnName);
  return SIGNED_COLUMN_PATTERN.test(normalizedColumnName);
}

function formatNumericUnitString(value: string, columnName?: string) {
  const match = value.trim().match(NUMERIC_WITH_UNIT_PATTERN);
  if (!match) {
    return null;
  }
  const numeric = coerceFiniteNumber(match[1]);
  if (numeric === null) {
    return null;
  }
  const suffix = match[2].replace(/\s+/g, " ").trim();
  const formatted = shouldDisplaySigned(columnName) || match[1].trim().startsWith("+")
    ? SIGNED_NUMBER_FORMATTER.format(numeric)
    : NUMBER_FORMATTER.format(numeric);
  return `${formatted} ${suffix}`.trim();
}

export function formatDisplayValue(value: unknown, columnName?: string): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (Array.isArray(value)) {
    return value.map((item) => formatDisplayValue(item, columnName)).join(", ");
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  if (typeof value === "string") {
    const formattedPercent = formatPercentString(value);
    if (formattedPercent) {
      return formattedPercent;
    }
    const formattedUnit = formatNumericUnitString(value, columnName);
    if (formattedUnit) {
      return formattedUnit;
    }
  }
  if (isPercentColumn(columnName)) {
    return formatPercent(value, { fallback: String(value) });
  }
  const numeric = coerceFiniteNumber(value);
  if (numeric !== null) {
    if (shouldDisplaySigned(columnName) || String(value).trim().startsWith("+")) {
      return formatNumber(numeric, { signed: true });
    }
    if (typeof value === "string" && value.includes(".") && !String(value).includes("%")) {
      return FIXED_TWO_DECIMAL_FORMATTER.format(numeric);
    }
    return formatNumber(numeric);
  }
  return String(value);
}
