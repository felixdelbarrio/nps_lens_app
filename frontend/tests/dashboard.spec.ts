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
    if stage == "classifier":
        assert manifest["schema_version"] == "nps-lens-comments/5"
        categories = json.loads(archive.read("taxonomy.json"))["categories"]
        primary = next(key for key, pair in categories.items() if pair["lever"] == "Atención" and pair["sublever"] == "Resolución")
taxonomy = {"taxonomy": [
    {"lever": "Atención", "sublevers": ["Resolución"]},
    {"lever": "Sin clasificación temática", "sublevers": ["Información insuficiente", "Tema no cubierto"]},
]}
for branch in taxonomy["taxonomy"]:
    branch["sublevers"] = [{"name": sub, "criterion": "Usar solo para el significado explícito de " + sub} for sub in branch["sublevers"]]
with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
    archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False))
    if stage == "designer":
        taxonomy["review"] = {"quotes": [row["Comment"] for value in comments.values() for row in value["comments"] if row["Comment"].strip()][:1], "reason": "Fronteras contrastadas con la narrativa del corpus."}
        archive.writestr("taxonomy.json", json.dumps(taxonomy, ensure_ascii=False))
    else:
        for name, payload in comments.items():
            result = {"classifications": [{"id": row["id"], "primary": primary, "secondary": []} for row in payload["comments"]]}
            archive.writestr(name.replace("comments/", "results/"), json.dumps(result, ensure_ascii=False))
`;
  execFileSync(path.resolve(__dirname, "../../.venv/bin/python"), ["-c", script, input, output, stage]);
}

async function exportedPath(page: import("@playwright/test").Page, title: string) {
  const pattern = /ZIP guardado en (.+?\.zip)/;
  const message = await page.locator("article").filter({has:page.getByRole("heading", {name:title,exact:true})}).last().getByText(pattern).textContent();
  const match = message?.match(pattern);
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

  await expect(page.getByTestId("upload-input")).toBeVisible();
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
  const [reprocessed] = await Promise.all([
    page.waitForResponse(response => response.url().includes("/api/reprocess") && response.request().method() === "POST"),
    page.getByTestId("reprocess-button").click(),
  ]);
  expect(reprocessed.ok()).toBeTruthy();
  await expect(page.getByTestId("reprocess-button")).toHaveText("Reprocesar agregados", {timeout: 15000});
  await page.getByRole("button", { name: /Cerrar configuración/i }).click();

  await page.getByRole("button", { name: /Datos/i }).click();
  await expect(page.getByTestId("data-table")).toContainText("Browser");
  await expect(page.getByTestId("error-banner")).toHaveCount(0);

  await page.getByRole("button", { name: /Taxonomy Studio/i }).click();
  await expect(page.getByLabel("Marco de clasificación")).toHaveValue("SOURCE");
  await expect(page.getByText("Normalización · tabla de equivalencias")).toHaveCount(0);
  await page.getByRole("tab",{name:"Análisis con LLM"}).click();
  await expect(page.getByText("1. Clasificar incidencias", {exact:true})).toBeVisible();
  await expect(page.getByText("2. Vincular", {exact:true})).toBeVisible();
  await expect(page.getByRole("switch", {name:"Usar vinculación semántica LLM"})).toBeDisabled();
  await expect(page.getByLabel("Importar ZIP de vínculos evaluados")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Usar como lente" })).toHaveCount(0);
  await page.getByRole("button", { name: "Exportar comentarios para crear taxonomía" }).click();
  const designerInput = await exportedPath(page, "Crear Taxonomía");
  const designerOutput = path.join(path.dirname(designerInput), "designer-response.zip");
  responseZip(designerInput, designerOutput, "designer");
  await page.getByLabel("Importar ZIP de taxonomía", {exact:true}).setInputFiles(designerOutput);
  await expect(page.getByText(/Propuesta de taxonomía importada/)).toBeVisible();
  await page.getByRole("button", { name: "Activar propuesta DISCOVERED" }).click();
  await expect(page.getByLabel("Marco de clasificación")).toHaveValue("DISCOVERED");
  let exportCount = 0;
  page.on("request", request => {
    if (request.url().includes("/discovery/classifier/export") && request.method() === "POST") exportCount++;
  });
  const [classificationExport] = await Promise.all([
    page.waitForResponse(response => response.url().includes("/discovery/classifier/export") && response.request().method() === "POST"),
    page.getByRole("button", { name: "Descargar todos los ZIP de comentarios pendientes" }).click(),
  ]);
  expect(classificationExport.ok(), await classificationExport.text()).toBeTruthy();
  const { saved_paths: classifierInputs } = await classificationExport.json();
  expect(classifierInputs.length).toBeGreaterThan(0);
  await expect(page.getByRole("region", { name: "ZIP preparados" })).toBeVisible();
  const classifierUpload = page.getByLabel("Importar ZIP de comentarios clasificados");
  let previousPending = Infinity;
  for (const classifierInput of classifierInputs) {
    const classifierOutput = classifierInput.replace(/\.zip$/, "-response.zip");
    responseZip(classifierInput, classifierOutput, "classifier");
    await expect(classifierUpload).toBeEnabled({ timeout: 15000 });
    const [classificationImport] = await Promise.all([
      page.waitForResponse(response => response.url().includes("/discovery/classifier/import") && response.request().method() === "POST"),
      classifierUpload.setInputFiles(classifierOutput),
    ]);
    expect(classificationImport.ok(), await classificationImport.text()).toBeTruthy();
    const { progress } = await classificationImport.json();
    expect(progress.pending).toBeGreaterThanOrEqual(0);
    expect(progress.pending).toBeLessThan(previousPending);
    expect(progress.received + progress.pending).toBe(progress.total);
    previousPending = progress.pending;
    await expect(page.getByText(/Importación validada/)).toBeVisible();
  }
  expect(previousPending).toBe(0);
  expect(exportCount).toBe(1);
  await expect(classifierUpload).toBeEnabled({ timeout: 15000 });
  await expect(page.getByRole("button", { name: "Descargar todos los ZIP de comentarios pendientes" })).toBeDisabled();
  await expect(page.getByText("Explorar Descubierta por LLM", {exact:true})).toBeVisible();
  await page.getByLabel("Marco de clasificación").selectOption("DISCOVERED");
  await expect(page.getByLabel("Marco de clasificación")).toHaveValue("DISCOVERED");
  await expect(page.getByTestId("error-banner")).toHaveCount(0);
  await page.getByRole("button", { name: /Insights/i }).click();
  await page.getByRole("tab", { name: "Comentarios", exact: true }).click();
  const llmSwitch = page.getByRole("switch", {name:"Usar clasificación LLM"});
  await expect(llmSwitch).toBeEnabled();
  const [enabled] = await Promise.all([
    page.waitForResponse(response => response.url().includes("/comments/engine") && response.request().method() === "PUT"),
    llmSwitch.click(),
  ]);
  expect(enabled.ok()).toBeTruthy();
  expect((await enabled.json()).selected_engine).toBe("llm");
  await expect(llmSwitch).toBeChecked({ timeout: 15000 });
  await expect(llmSwitch).toBeEnabled({ timeout: 15000 });
  const [disabled] = await Promise.all([
    page.waitForResponse(response => response.url().includes("/comments/engine") && response.request().method() === "PUT"),
    llmSwitch.click(),
  ]);
  expect(disabled.ok()).toBeTruthy();
  expect((await disabled.json()).selected_engine).toBe("rules");
  await expect(llmSwitch).not.toBeChecked({ timeout: 15000 });
  await expect(page.getByTestId("error-banner")).toHaveCount(0);
  await page.reload();
  await expect(page.getByRole("tab", { name: "Evolución NPS", exact:true })).toBeVisible();
});
