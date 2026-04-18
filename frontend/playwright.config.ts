import { defineConfig } from "@playwright/test";

delete process.env.NO_COLOR;
process.env.FORCE_COLOR = "0";

export default defineConfig({
  testDir: "./tests",
  use: {
    baseURL: "http://127.0.0.1:4100",
    headless: true
  },
  webServer: {
    command:
      "sh -lc 'rm -rf .playwright-data && mkdir -p .playwright-data && NPS_LENS_DATA_DIR=.playwright-data NPS_LENS_DATABASE_PATH=.playwright-data/e2e.sqlite3 NPS_LENS_FRONTEND_DIST_DIR=dist NPS_LENS_SERVICE_ORIGIN_BUUG=\"BBVA México\" NPS_LENS_SERVICE_ORIGIN_N1=\"BBVA México:ENTERPRISE WEB\" NPS_LENS_DEFAULT_SERVICE_ORIGIN=\"BBVA México\" NPS_LENS_DEFAULT_SERVICE_ORIGIN_N1=\"ENTERPRISE WEB\" ../.venv/bin/python -m nps_lens.cli serve --host 127.0.0.1 --port 4100'",
    port: 4100,
    reuseExistingServer: false,
    timeout: 120000
  }
});
