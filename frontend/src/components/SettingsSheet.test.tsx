import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { expect, it, vi } from "vitest";

import { SettingsSheet, type SettingsTab } from "./SettingsSheet";
import { updateNpsColumnAliases } from "../api";

vi.mock("../api", () => ({
  fetchEquivalences: vi.fn().mockResolvedValue({
    schema_version: "2.0",
    dimensions: { "nps.Palanca": [], "nps.Subpalanca": [] }
  }),
  updateEquivalences: vi.fn(),
  fetchNpsColumnAliases: vi.fn().mockResolvedValue({
    schema_version: "1.0",
    fields: [
      { canonical: "Fecha", required: true, aliases: ["Date"] },
      { canonical: "Comment", required: false, aliases: ["Text"] }
    ]
  }),
  updateNpsColumnAliases: vi.fn().mockImplementation(async (payload) => payload)
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

it("edits NPS column aliases while keeping required status read-only", async () => {
  const user = userEvent.setup();
  render(<SettingsHarness />);

  await user.click(screen.getByRole("tab", { name: "Alias de columnas NPS" }));
  expect(await screen.findByRole("heading", { name: "Fecha" })).toBeInTheDocument();
  expect(screen.getByText("Obligatorio")).toBeInTheDocument();
  expect(screen.getByText("Opcional")).toBeInTheDocument();
  expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();

  const input = screen.getByRole("textbox", { name: "Añadir alias para Fecha" });
  await user.type(input, "Survey Date{Enter}");
  await user.click(screen.getByRole("button", { name: "Guardar alias de columnas" }));

  expect(updateNpsColumnAliases).toHaveBeenCalledWith(
    expect.objectContaining({
      fields: expect.arrayContaining([
        expect.objectContaining({ canonical: "Fecha", aliases: ["Date", "Survey Date"] })
      ])
    })
  );
  expect(await screen.findByText("Alias de columnas NPS guardados.")).toBeInTheDocument();
});
