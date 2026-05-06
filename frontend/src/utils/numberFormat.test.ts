import { describe, expect, it } from "vitest";

import { formatDisplayValue, formatNumber, formatPercent } from "./numberFormat";

describe("numberFormat", () => {
  it("limits generic numbers to two decimals", () => {
    expect(formatNumber(58.34521981031492)).toBe("58,35");
    expect(formatNumber(-38.33605220228385)).toBe("-38,34");
  });

  it("renders percentages with fixed two-decimal locale format", () => {
    expect(formatPercent(0.345)).toBe("34,50%");
    expect(formatPercent(0.6)).toBe("60,00%");
  });

  it("detects percent-like columns centrally", () => {
    expect(formatDisplayValue(0.5795254182580882, "detractor_probability")).toBe("57,95%");
    expect(formatDisplayValue(0.1524, "average_focus_rate")).toBe("15,24%");
    expect(formatDisplayValue(0.22, "% promotores")).toBe("22,00%");
  });

  it("does not mistake non-percent metrics for percentages", () => {
    expect(formatDisplayValue(29.95659158491985, "potential_uplift")).toBe("29,96");
    expect(formatDisplayValue(-97.79486970379659, "gap_vs_overall")).toBe("-97,79");
    expect(formatDisplayValue(0.6779040931692755, "confidence")).toBe("0,68");
    expect(formatDisplayValue(12.3456, "delta_focus_rate_pp")).toBe("12,35");
  });

  it("normalizes incoming strings that already contain a percent sign", () => {
    expect(formatDisplayValue("34.5%", "any_column")).toBe("34,50%");
    expect(formatDisplayValue("34,5 %", "any_column")).toBe("34,50%");
  });
});
