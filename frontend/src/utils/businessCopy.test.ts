import { describe, expect, it } from "vitest";

import { toBusinessCopy } from "./businessCopy";

describe("toBusinessCopy", () => {
  it("uses the current NPS product name for historical file names", () => {
    const historicalName = `NPS T${"e\u0301"}rmico Senda.xlsx`;

    expect(toBusinessCopy(historicalName)).toBe("NPS Senda.xlsx");
  });
});
