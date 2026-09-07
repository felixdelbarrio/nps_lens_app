import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { expect, it, vi } from "vitest";

import { SettingsSheet, type SettingsTab } from "./SettingsSheet";

vi.mock("../api", () => ({
  fetchEquivalences: vi.fn().mockResolvedValue({
    schema_version: "2.0",
    dimensions: { "nps.Palanca": [], "nps.Subpalanca": [] }
  }),
  updateEquivalences: vi.fn()
}));

function SettingsHarness() {
  const [tab, setTab] = useState<SettingsTab>("appearance");
  return <SettingsSheet
    open activeTab={tab} onTabChange={setTab} onClose={vi.fn()}
    themeMode="light" setThemeMode={vi.fn()}
    downloadsPath="" setDownloadsPath={vi.fn()}
    helixBaseUrl="" setHelixBaseUrl={vi.fn()}
    reportDimensionAnalysis="palanca" setReportDimensionAnalysis={vi.fn()}
    minSimilarity={0.5} setMinSimilarity={vi.fn()}
    maxDaysApart={7} setMaxDaysApart={vi.fn()}
    minNCross={30} setMinNCross={vi.fn()}
    serviceOrigins={[]} serviceOriginN1Map={{}} serviceOriginN2Map={{}}
    hierarchySaving={false} onSaveHierarchy={vi.fn()}
    onReprocess={vi.fn()} reprocessPending={false}
  />;
}

it("shows fixed score rules separately from editable equivalences", async () => {
  const user = userEvent.setup();
  render(<SettingsHarness />);
  await user.click(screen.getByRole("tab", { name: "Reglas de ingesta" }));
  expect(screen.getByRole("heading", { name: "Clasificación automática NPS" })).toBeInTheDocument();
  expect(screen.getByText("≤6: detractor")).toBeInTheDocument();
  expect(screen.getByText("7–8: neutro")).toBeInTheDocument();
  expect(screen.getByText("≥9: promotor")).toBeInTheDocument();
  expect(screen.queryByRole("spinbutton")).not.toBeInTheDocument();
  await user.click(screen.getByRole("tab", { name: "Equivalencias" }));
  expect(await screen.findByRole("heading", { name: "nps.Palanca" })).toBeInTheDocument();
  expect(screen.getByRole("option", { name: "Subpalanca" })).toBeInTheDocument();
  expect(screen.queryByText("Clasificación NPS")).not.toBeInTheDocument();
});
