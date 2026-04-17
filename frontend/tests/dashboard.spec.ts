import path from "node:path";
import { fileURLToPath } from "node:url";

import { expect, test } from "@playwright/test";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const marchFixture = path.resolve(
  __dirname,
  "../../tests/fixtures/excel/NPS Térmico Senda - 03Marzo.xlsx"
);

test("uploads a schema-drift file and shows cumulative results", async ({ page }) => {
  test.setTimeout(180000);

  await page.goto("/");
  await expect(page.getByRole("heading", { name: /Ingesta acumulativa/i })).toBeVisible();

  await page.getByTestId("upload-input").setInputFiles(marchFixture);
  await page.getByRole("button", { name: "Subir y consolidar" }).click();

  await expect(page.getByTestId("uploads-table")).toContainText("NPS Térmico Senda - 03Marzo.xlsx", {
    timeout: 180000
  });
  await expect(page.getByTestId("selected-upload-name")).toContainText(
    "NPS Térmico Senda - 03Marzo.xlsx"
  );
  await expect(page.getByTestId("issues-list")).toContainText("extra_columns_detected");

  await page.getByTestId("reprocess-button").click();
  await expect(page.getByTestId("reprocess-button")).toHaveText("Reprocesar agregados");
  await expect(page.getByTestId("error-banner")).toHaveCount(0);
});
