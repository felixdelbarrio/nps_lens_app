import { describe, expect, it } from "vitest";

import { formatDisplayValue, formatNumber, formatPercent } from "./numberFormat";

describe("numberFormat", () => {
  it("limits generic numbers to one business decimal", () => {
    expect(formatNumber(58.34521981031492)).toBe("58,3");
    expect(formatNumber(-38.33605220228385)).toBe("-38,3");
  });

  it("renders percentages with one business decimal", () => {
    expect(formatPercent(0.345)).toBe("34,5%");
    expect(formatPercent(0.6)).toBe("60,0%");
  });

  it("detects percent-like columns centrally", () => {
    expect(formatDisplayValue(0.5795254182580882, "focus_rate_high_incidence")).toBe("58,0%");
    expect(formatDisplayValue(0.1524, "average_focus_rate")).toBe("15,2%");
    expect(formatDisplayValue(0.22, "% promotores")).toBe("22,0%");
  });

  it("does not mistake non-percent metrics for percentages", () => {
    expect(formatDisplayValue(28, "Nº detractores")).toBe("28");
    expect(formatDisplayValue(29.95659158491985, "nps")).toBe("30");
    expect(formatDisplayValue(-97.79486970379659, "gap_vs_overall")).toBe("-97,8");
    expect(formatDisplayValue(0.6779040931692755, "confidence")).toBe("0,7");
    expect(formatDisplayValue(12.3456, "focus_rate_difference_pp")).toBe("12,3");
  });

  it("normalizes incoming strings that already contain a percent sign", () => {
    expect(formatDisplayValue("34.5%", "any_column")).toBe("34,5%");
    expect(formatDisplayValue("34,5 %", "any_column")).toBe("34,5%");
  });
});
