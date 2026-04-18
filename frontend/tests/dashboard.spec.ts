import path from "node:path";
import fs from "node:fs";
import { fileURLToPath } from "node:url";

import { expect, test } from "@playwright/test";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
function fixtureExcel(name: string) {
  const fixturesDir = path.resolve(__dirname, "../../tests/fixtures/excel");
  const expected = name.normalize("NFD");
  const match = fs
    .readdirSync(fixturesDir)
    .find((entry) => entry.normalize("NFD") === expected);

  if (!match) {
    throw new Error(`Fixture not found: ${name}`);
  }

  return path.join(fixturesDir, match);
}

const marchFixture = fixtureExcel("NPS Térmico Senda - 03Marzo.xlsx");
const marchFixtureSuffix = /03Marzo\.xlsx/;

test("uploads a schema-drift file and shows cumulative results", async ({ page }) => {
  test.setTimeout(180000);

  await page.goto("/");
  await expect(
    page.getByRole("heading", {
      name: /Analisis del NPS Térmico y causalidad con incidencias de clientes/i
    })
  ).toBeVisible();

  await page.getByRole("button", { name: /Ingesta/i }).click();
  await page.getByTestId("upload-input").setInputFiles(marchFixture);
  await page.getByRole("button", { name: "Importar / actualizar NPS" }).click();

  await expect(page.getByTestId("uploads-table")).toContainText(marchFixtureSuffix, {
    timeout: 180000
  });
  await page.getByRole("button", { name: "Ver issues" }).click();
  await expect(page.getByTestId("selected-upload-name")).toContainText(marchFixtureSuffix);
  await expect(page.getByTestId("selected-issues-list")).toContainText("extra_columns_detected");

  await page.getByRole("button", { name: /Insights/i }).click();
  await page.getByTestId("reprocess-button").click();
  await expect(page.getByTestId("reprocess-button")).toHaveText("Reprocesar agregados");

  await page.getByRole("button", { name: /Datos/i }).click();
  await expect(page.getByTestId("data-table")).toContainText("Browser");
  await expect(page.getByTestId("error-banner")).toHaveCount(0);
});
