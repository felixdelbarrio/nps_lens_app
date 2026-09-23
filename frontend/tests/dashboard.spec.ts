import path from "node:path";
import fs from "node:fs";
import { execFileSync } from "node:child_process";
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

function responseZip(input: string, output: string, stage: "designer" | "classifier") {
  const script = `
import json, sys, zipfile
source, target, stage = sys.argv[1:]
with zipfile.ZipFile(source) as archive:
    manifest = json.loads(archive.read("manifest.json"))
    comments = {name: json.loads(archive.read(name)) for name in archive.namelist() if name.startswith("comments/")}
taxonomy = {"taxonomy": [
    {"lever": "Atención", "sublevers": ["Resolución"]},
    {"lever": "Sin clasificación temática", "sublevers": ["Información insuficiente", "Tema no cubierto"]},
]}
with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, separators=(",", ":")))
    if stage == "designer":
        archive.writestr("taxonomy.json", json.dumps(taxonomy, ensure_ascii=False, separators=(",", ":")))
    else:
        for name, payload in comments.items():
            result = {"classifications": [{"id": row["id"], "primary_classification": {"lever": "Atención", "sublever": "Resolución"}} for row in payload["comments"]]}
            archive.writestr(name.replace("comments/", "results/"), json.dumps(result, ensure_ascii=False, separators=(",", ":")))
`;
  execFileSync(path.resolve(__dirname, "../../.venv/bin/python"), ["-c", script, input, output, stage]);
}

async function exportedPath(page: import("@playwright/test").Page) {
  const message = await page.getByText(/ZIP guardado en .*\.zip/).textContent();
  const match = message?.match(/ZIP guardado en (.+?\.zip)\./);
  if (!match) throw new Error(`No se encontró la ruta del ZIP en: ${message}`);
  return match[1];
}

test("uploads a schema-drift file and shows cumulative results", async ({ page }) => {
  test.setTimeout(240000);

  await page.goto("/");
  await expect(
    page.getByRole("heading", {
      name: /NPS Lens/i
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
  await page.getByRole("button", { name: /Abrir configuración global/i }).click();
  await page.getByRole("tab", { name: "Ajustes avanzados" }).click();
  await page.getByTestId("reprocess-button").click();
  await expect(page.getByTestId("reprocess-button")).toHaveText("Reprocesar agregados");
  await page.getByRole("button", { name: /Cerrar configuración/i }).click();

  await page.getByRole("button", { name: /Datos/i }).click();
  await expect(page.getByTestId("data-table")).toContainText("Browser");
  await expect(page.getByTestId("error-banner")).toHaveCount(0);

  await page.getByRole("button", { name: /Taxonomy Studio/i }).click();
  await expect(page.getByRole("combobox", { name: "Método", exact: true })).toHaveValue(
    "chatgpt_zip"
  );
  await page.getByRole("button", { name: "Exportar ZIP a Descargas" }).click();
  const designerInput = await exportedPath(page);
  const designerOutput = path.join(path.dirname(designerInput), "designer-response.zip");
  responseZip(designerInput, designerOutput, "designer");
  await page.getByLabel("Importar ZIP de respuesta").setInputFiles(designerOutput);
  const classifierInput = await exportedPath(page);
  const classifierOutput = path.join(path.dirname(classifierInput), "classifier-response.zip");
  responseZip(classifierInput, classifierOutput, "classifier");
  await page.getByLabel("Importar ZIP de respuesta").setInputFiles(classifierOutput);
  await expect(page.getByText(/Todos los lotes validados/)).toBeVisible();
  await expect(page.getByRole("button", { name: "Explorar Descubierta" })).toBeVisible();
  await expect(page.getByTestId("error-banner")).toHaveCount(0);
});
