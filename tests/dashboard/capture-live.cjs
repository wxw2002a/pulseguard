const { chromium } = require('playwright');
const fs = require('node:fs');
const path = require('node:path');

(async () => {
  const base = process.env.BASE_URL || 'http://127.0.0.1:8080';
  const output = path.resolve(process.env.SCREENSHOT_PATH || 'artifacts/dashboard-live.png');
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 1512, height: 1400 } });
    await page.goto(base);
    await page.waitForFunction(() => document.querySelector('#connection').textContent.includes('API connected'), null, { timeout: 60000 });
    await page.waitForFunction(() => document.querySelector('#metric-pending').textContent === '0' && Number(document.querySelector('#metric-transactions').textContent.replaceAll(',', '')) > 0, null, { timeout: 60000 });
    if (await page.locator('#sample-toggle').isChecked()) throw new Error('Runtime evidence must not use sample fixtures');
    await page.evaluate(() => document.fonts.ready);
    fs.mkdirSync(path.dirname(output), { recursive: true });
    await page.screenshot({ path: output, fullPage: true });
    console.log(`Captured live API-backed workspace: ${output}`);
  } finally { await browser.close(); }
})().catch((error) => { console.error(error); process.exitCode = 1; });
