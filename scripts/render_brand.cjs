// Rasterize the official SVG once for PowerPoint and email clients.
const { chromium } = require('../frontend/node_modules/playwright');
const { readFileSync } = require('node:fs');
(async () => {
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 213, height: 130 }, deviceScaleFactor: 4 });
    await page.setContent('<style>body{margin:0}</style>' + readFileSync(process.argv[2], 'utf8'));
    await page.locator('svg').screenshot({ path: process.argv[3] });
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
