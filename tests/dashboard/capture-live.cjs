const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

async function verifyLiveAnalystReview(page, base, browserErrors) {
  const read = async (endpoint) => {
    const response = await page.request.get(
      `${base.replace(/\/$/, "")}/api/v1${endpoint}`,
      { timeout: 15000 },
    );
    assert(
      response.ok(),
      `Live review API read failed: ${endpoint} returned HTTP ${response.status()}`,
    );
    return response.json();
  };
  // This script exercises the ephemeral synthetic CI stack. The key stays in the
  // browser session and is never included in the exported report or snapshot.
  await page.evaluate(
    (key) => sessionStorage.setItem("pulseguard-api-key", key),
    process.env.INGEST_API_KEY || "local-dev-key",
  );
  await page.locator("[data-view=alerts]").click();
  await Promise.all([
    page.waitForResponse((response) => {
      const url = new URL(response.url());
      return (
        url.pathname === "/api/v1/alerts" &&
        url.searchParams.get("status") === "OPEN" &&
        response.ok()
      );
    }),
    page.locator("#status-filter").selectOption("OPEN"),
  ]);
  // Filtering happens on the server, so newer resolved signals cannot hide the
  // open queue even when the synthetic load produced hundreds of records.
  let row;
  for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
    const candidates = page.locator("#all-alerts tbody tr").filter({
      has: page.locator("td:nth-child(6) .cell-sub", {
        hasText: /^Unassigned$/,
      }),
    });
    await page.waitForFunction(
      () =>
        document.querySelector("#all-alerts tbody tr") ||
        document.querySelector("#all-alerts .empty-state"),
    );
    if (await candidates.count()) {
      row = candidates.first();
      break;
    }
    if (await page.locator("#queue-next").isDisabled()) break;
    await Promise.all([
      page.waitForResponse((response) => {
        const url = new URL(response.url());
        return (
          url.pathname === "/api/v1/alerts" &&
          url.searchParams.has("cursor") &&
          response.ok()
        );
      }),
      page.locator("#queue-next").click(),
    ]);
  }
  assert(
    row,
    "The synthetic CI dataset must contain an OPEN, unowned alert for the browser investigation",
  );
  const alertId = await row.getAttribute("data-alert-id");
  await row.click();
  const endpoint = `/alerts/${encodeURIComponent(alertId)}`;
  const [before, evidence, outcomeBefore] = await Promise.all([
    read(endpoint),
    read(`${endpoint}/evidence`),
    read("/outcomes"),
  ]);
  assert.equal(before.status, "OPEN");
  assert(
    !before.owner,
    "Live browser verification must claim an unowned signal",
  );
  const initialVersion = before.version || 0;
  await page.waitForFunction(
    (version) =>
      document
        .querySelector("#review-ownership")
        .textContent.includes(`Version ${version}`),
    initialVersion,
  );
  const evidenceLabel =
    evidence.provenance === "PINNED_DETECTION"
      ? "Pinned detection evidence"
      : "Legacy contextual evidence";
  await page.waitForFunction(
    (label) =>
      document.querySelector("#detail-evidence").textContent.includes(label),
    evidenceLabel,
  );
  assert(
    [
      "PINNED_DETECTION",
      "LEGACY_WINDOW_CONTEXT",
      "LEGACY_TRANSACTION_CONTEXT",
    ].includes(evidence.provenance),
    "Evidence API must identify its provenance",
  );

  const analyst = "ci-browser-analyst";
  const claimNote =
    "CI browser verification: claimed this synthetic signal to inspect its detection evidence.";
  const resolveNote =
    "CI browser verification: synthetic merchant activity reviewed and classified as benign.";
  const submit = async (action, note, disposition) => {
    await page.locator("#review-action").selectOption(action);
    if (disposition)
      await page.locator("#review-disposition").selectOption(disposition);
    await page.locator("#review-note").fill(note);
    const [response] = await Promise.all([
      page.waitForResponse(
        (response) =>
          response.request().method() === "PATCH" &&
          new URL(response.url()).pathname === `/api/v1${endpoint}/review` &&
          response.request().postDataJSON()?.action === action,
      ),
      page.locator("#save-review").click(),
    ]);
    assert.equal(
      response.status(),
      200,
      `Browser ${action} must be accepted by the running API`,
    );
    await page.waitForFunction(
      () => document.querySelector("#review-note").value === "",
    );
  };
  await page.locator("#review-analyst").fill(analyst);
  await submit("CLAIM", claimNote);
  const claimed = await read(endpoint);
  assert.equal(claimed.version, initialVersion + 1);
  assert.equal(claimed.status, "INVESTIGATING");
  assert.equal(claimed.owner, analyst);
  await page.waitForFunction(
    (version) =>
      document
        .querySelector("#review-ownership")
        .textContent.includes(`Version ${version}`),
    claimed.version,
  );

  await submit("RESOLVE", resolveNote, "BENIGN");
  const [resolved, outcomeAfter] = await Promise.all([
    read(endpoint),
    read("/outcomes"),
  ]);
  assert.equal(resolved.version, initialVersion + 2);
  assert.equal(resolved.status, "RESOLVED");
  assert.equal(resolved.owner, analyst);
  assert.equal(resolved.disposition, "BENIGN");
  const entries = resolved.reviewHistory.filter(
    (entry) =>
      entry.analyst === analyst &&
      [claimNote, resolveNote].includes(entry.note),
  );
  assert.deepEqual(
    entries.map((entry) => entry.action),
    ["CLAIM", "RESOLVE"],
  );
  assert.deepEqual(
    entries.map((entry) => entry.version),
    [claimed.version, resolved.version],
  );
  assert.equal(entries[1].disposition, "BENIGN");
  const previousRule = outcomeBefore.byRule.find(
    (row) => row.rule === before.rule,
  );
  const resolvedRule = outcomeAfter.byRule.find(
    (row) => row.rule === before.rule,
  );
  assert(resolvedRule, "The resolution must create a per-rule outcome");
  assert.equal(resolvedRule.benign, (previousRule?.benign || 0) + 1);
  assert.equal(outcomeAfter.resolvedAlerts, outcomeBefore.resolvedAlerts + 1);
  await page.waitForFunction(
    (version) =>
      document.querySelector("#review-status").value === "RESOLVED" &&
      document
        .querySelector("#review-ownership")
        .textContent.includes(`Version ${version}`),
    resolved.version,
  );
  assert(
    (await page.locator("#detail-history").textContent()).includes(claimNote),
  );
  assert(
    (await page.locator("#detail-history").textContent()).includes(resolveNote),
  );
  const ruleTitle = before.rule
    .replaceAll("_", " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
  await page.waitForFunction(
    ({ rule, benign }) =>
      Array.from(document.querySelectorAll("#rule-outcomes tbody tr")).some(
        (row) =>
          row.cells[0].textContent === rule &&
          Number(row.cells[4].textContent.replaceAll(",", "")) === benign,
      ),
    { rule: ruleTitle, benign: resolvedRule.benign },
  );
  fs.mkdirSync(path.resolve("artifacts"), { recursive: true });
  await page.locator("#detail-dialog").evaluate((dialog) => {
    dialog.scrollTop = 0;
  });
  await page.screenshot({
    path: path.resolve("artifacts/analyst-live.png"),
    fullPage: true,
  });
  assert.deepEqual(
    browserErrors,
    [],
    "Live review must have no browser errors",
  );
  const report = {
    status: "passed",
    alertId,
    claimVersion: claimed.version,
    resolvedVersion: resolved.version,
    auditActions: ["CLAIM", "RESOLVE"],
    apiPersistenceChecked: true,
    evidenceRendered: true,
    outcomeRendered: true,
  };
  fs.writeFileSync(
    path.resolve("artifacts/live-review.json"),
    `${JSON.stringify(report, null, 2)}\n`,
    "utf8",
  );
  await page.locator("#detail-dialog .dialog-close").click();
  await page.locator("[data-view=overview]").click();
  console.log(
    "PASS: real browser claim/resolution, persisted audit versions, rendered evidence and rule outcome",
  );
}

async function exportVerifiedSnapshot(page, base) {
  if (process.env.PUBLISH_DEMO_SNAPSHOT !== "true") return;
  const { GITHUB_SHA, GITHUB_REPOSITORY, GITHUB_RUN_ID, GITHUB_ACTIONS } =
    process.env;
  if (
    GITHUB_ACTIONS !== "true" ||
    !/^[a-f0-9]{40}$/i.test(GITHUB_SHA || "") ||
    !/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(GITHUB_REPOSITORY || "") ||
    !/^\d+$/.test(GITHUB_RUN_ID || "")
  ) {
    throw new Error(
      "Publishing a demo snapshot requires explicit opt-in and complete GitHub Actions provenance",
    );
  }
  const read = async (endpoint) => {
    const response = await page.request.get(
      `${base.replace(/\/$/, "")}${endpoint}`,
      { timeout: 15000 },
    );
    if (!response.ok())
      throw new Error(
        `Snapshot API request failed: ${endpoint} returned HTTP ${response.status()}`,
      );
    return response.json();
  };
  const [overview, alertPage, transactionPage, windowPage, outcomes] =
    await Promise.all([
      read("/api/v1/overview"),
      read("/api/v1/alerts?limit=200"),
      read("/api/v1/transactions?limit=200"),
      read("/api/v1/windows?limit=200"),
      read("/api/v1/outcomes"),
    ]);
  if (overview.pendingDelivery !== 0 || overview.transactions < 1) {
    throw new Error(
      "Publishable snapshots require a nonempty verified dataset and a drained durable outbox",
    );
  }
  const details = Object.create(null);
  const evidence = Object.create(null);
  const evidenceMetadata = Object.create(null);
  for (const alert of alertPage.items) {
    const endpoint = `/api/v1/alerts/${encodeURIComponent(alert.id)}`;
    const [detail, evidencePage] = await Promise.all([
      read(endpoint),
      read(`${endpoint}/evidence?limit=200`),
    ]);
    details[alert.id] = detail;
    evidence[alert.id] = evidencePage.items;
    const { items, ...metadata } = evidencePage;
    evidenceMetadata[alert.id] = metadata;
  }
  const snapshot = {
    schemaVersion: 1,
    capturedAt: new Date().toISOString(),
    sourceCommit: GITHUB_SHA,
    runUrl: `https://github.com/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}`,
    source: "verified-ci",
    dataset: "synthetic e2e and load events",
    overview,
    alerts: alertPage.items,
    transactions: transactionPage.items,
    windows: windowPage.items,
    details,
    evidence,
    evidenceMetadata,
    outcomes,
  };
  const snapshotPath = path.resolve("artifacts/dashboard-snapshot.json");
  fs.mkdirSync(path.dirname(snapshotPath), { recursive: true });
  fs.writeFileSync(
    snapshotPath,
    `${JSON.stringify(snapshot, null, 2)}\n`,
    "utf8",
  );
  console.log(`Exported verified synthetic CI snapshot: ${snapshotPath}`);
}

(async () => {
  const base = process.env.BASE_URL || "http://127.0.0.1:8080";
  const output = path.resolve(
    process.env.SCREENSHOT_PATH || "artifacts/dashboard-live.png",
  );
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: 1512, height: 1400 },
    });
    page.setDefaultTimeout(30000);
    const browserErrors = [];
    page.on("pageerror", (error) => browserErrors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") browserErrors.push(message.text());
    });
    await page.goto(base);
    await page.waitForFunction(
      () =>
        document
          .querySelector("#connection")
          .textContent.includes("API connected"),
      null,
      { timeout: 60000 },
    );
    await page.waitForFunction(
      () =>
        document.querySelector("#metric-pending").textContent === "0" &&
        Number(
          document
            .querySelector("#metric-transactions")
            .textContent.replaceAll(",", ""),
        ) > 0,
      null,
      { timeout: 60000 },
    );
    if (await page.locator("#sample-toggle").isChecked())
      throw new Error("Runtime evidence must not use sample fixtures");
    await page.evaluate(() => document.fonts.ready);
    await verifyLiveAnalystReview(page, base, browserErrors);
    assert.deepEqual(
      browserErrors,
      [],
      "Live dashboard must have no browser errors",
    );
    fs.mkdirSync(path.dirname(output), { recursive: true });
    await page.screenshot({ path: output, fullPage: true });
    console.log(`Captured live API-backed workspace: ${output}`);
    await exportVerifiedSnapshot(page, base);
    assert.deepEqual(
      browserErrors,
      [],
      "Live dashboard must have no browser errors during capture",
    );
  } finally {
    await browser.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
