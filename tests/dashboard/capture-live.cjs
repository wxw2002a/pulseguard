const { chromium } = require('playwright');
const fs = require('node:fs');
const path = require('node:path');

async function exportVerifiedSnapshot(page, base) {
  if (process.env.PUBLISH_DEMO_SNAPSHOT !== 'true') return;
  const { GITHUB_SHA, GITHUB_REPOSITORY, GITHUB_RUN_ID, GITHUB_ACTIONS } = process.env;
  if (GITHUB_ACTIONS !== 'true' || !/^[a-f0-9]{40}$/i.test(GITHUB_SHA || '')
      || !/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(GITHUB_REPOSITORY || '')
      || !/^\d+$/.test(GITHUB_RUN_ID || '')) {
    throw new Error('Publishing a demo snapshot requires explicit opt-in and complete GitHub Actions provenance');
  }
  const read = async (endpoint) => {
    const response = await page.request.get(`${base.replace(/\/$/, '')}${endpoint}`, { timeout: 15000 });
    if (!response.ok()) throw new Error(`Snapshot API request failed: ${endpoint} returned HTTP ${response.status()}`);
    return response.json();
  };
  const [overview, alertPage, transactionPage, windowPage] = await Promise.all([
    read('/api/v1/overview'), read('/api/v1/alerts?limit=200'),
    read('/api/v1/transactions?limit=200'), read('/api/v1/windows?limit=200'),
  ]);
  if (overview.pendingDelivery !== 0 || overview.transactions < 1) {
    throw new Error('Publishable snapshots require a nonempty verified dataset and a drained durable outbox');
  }
  const details = Object.create(null);
  const evidence = Object.create(null);
  for (const alert of alertPage.items) {
    const endpoint = `/api/v1/alerts/${encodeURIComponent(alert.id)}`;
    const [detail, evidencePage] = await Promise.all([
      read(endpoint), read(`${endpoint}/evidence?limit=200`),
    ]);
    details[alert.id] = detail;
    evidence[alert.id] = evidencePage.items;
  }
  const snapshot = {
    schemaVersion: 1,
    capturedAt: new Date().toISOString(),
    sourceCommit: GITHUB_SHA,
    runUrl: `https://github.com/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}`,
    source: 'verified-ci',
    dataset: 'synthetic e2e and load events',
    overview,
    alerts: alertPage.items,
    transactions: transactionPage.items,
    windows: windowPage.items,
    details,
    evidence,
  };
  const snapshotPath = path.resolve('artifacts/dashboard-snapshot.json');
  fs.mkdirSync(path.dirname(snapshotPath), { recursive: true });
  fs.writeFileSync(snapshotPath, `${JSON.stringify(snapshot, null, 2)}\n`, 'utf8');
  console.log(`Exported verified synthetic CI snapshot: ${snapshotPath}`);
}

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
    await exportVerifiedSnapshot(page, base);
  } finally { await browser.close(); }
})().catch((error) => { console.error(error); process.exitCode = 1; });
