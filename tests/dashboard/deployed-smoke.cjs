"use strict";

const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const base = process.env.BASE_URL || "https://wxw2002a.github.io/pulseguard/";
const expectedCommit = process.env.EXPECTED_SOURCE_COMMIT;
const output = path.resolve(
  process.env.REPORT_DIRECTORY || "artifacts/deployed-browser",
);

async function main() {
  assert.match(
    expectedCommit || "",
    /^[0-9a-f]{40}$/,
    "EXPECTED_SOURCE_COMMIT must identify the verified deployment",
  );
  fs.mkdirSync(output, { recursive: true });
  const report = {
    checkedAt: new Date().toISOString(),
    url: base,
    expectedCommit,
    checks: [],
  };
  const browser = await chromium.launch({ headless: true });
  let page;
  try {
    const errors = [];
    const apiRequests = [];
    let snapshot;
    // Pages propagation can lag the deploy action. Reopen the actual page until
    // its own snapshot response identifies the expected source commit.
    for (let attempt = 0; attempt < 24; attempt++) {
      if (page) await page.close();
      page = await browser.newPage({ viewport: { width: 1512, height: 1200 } });
      page.setDefaultTimeout(15000);
      const snapshotResponse = page
        .waitForResponse(
          (response) =>
            new URL(response.url()).pathname.endsWith("/snapshot.json"),
          { timeout: 15000 },
        )
        .catch(() => null);
      const response = await page.goto(base, { waitUntil: "networkidle" });
      const captured = await snapshotResponse;
      snapshot = captured?.ok() ? await captured.json() : null;
      if (response.ok() && snapshot?.sourceCommit === expectedCommit) break;
      if (attempt === 23)
        throw new Error(
          "The published page did not serve the expected verified commit before the propagation deadline.",
        );
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
    // Reload once instrumented so asset failures and API requests are checked
    // for the complete user navigation, not just subsequent interactions.
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("response", (response) => {
      if (response.status() >= 400)
        errors.push(`HTTP ${response.status()}: ${response.url()}`);
    });
    page.on("request", (request) => {
      if (new URL(request.url()).pathname.includes("/api/v1"))
        apiRequests.push(request.url());
      if (!["GET", "HEAD", "OPTIONS"].includes(request.method()))
        errors.push(`Unexpected ${request.method()}: ${request.url()}`);
    });
    await page.reload({ waitUntil: "networkidle" });
    await page.waitForFunction(() =>
      document
        .querySelector("#connection")
        ?.textContent.includes("Verified run"),
    );
    assert.equal(await page.locator("#sample-toggle").isChecked(), false);
    assert.equal(
      await page.locator("#metric-transactions").textContent(),
      new Intl.NumberFormat("en-US").format(snapshot.overview.transactions),
    );
    assert.equal(await page.locator("#metric-pending").textContent(), "0");
    assert.equal(await page.locator("#simulate-button").isDisabled(), true);
    assert.equal(await page.locator("#settings-button").isVisible(), false);
    assert(
      (await page.locator("#notice").textContent()).includes(
        snapshot.capturedAt,
      ),
    );
    assert.equal(
      await page.locator(`#notice a[href="${snapshot.runUrl}"]`).count(),
      1,
    );
    report.checks.push(
      "Expected source commit and capture metadata",
      "Recorded overview totals and read-only controls",
    );
    assert(
      snapshot.alerts.length > 0,
      "Verified run must have produced risk signals",
    );
    await page.locator("[data-view=alerts]").click();
    assert.equal(
      await page.locator("#all-alerts tbody tr").count(),
      snapshot.alerts.length,
    );
    const reviewed = snapshot.alerts.find(
      (alert) => snapshot.details[alert.id]?.reviewHistory?.length,
    );
    assert(
      reviewed,
      "The published run must retain the end-to-end review decision",
    );
    await page
      .locator(`[data-alert-id="${reviewed.id}"]`)
      .filter({ visible: true })
      .first()
      .click();
    await page.waitForFunction(() =>
      document.querySelector("#detail-history .review-history li"),
    );
    const original = snapshot.evidence[reviewed.id];
    assert(
      original.length > 0,
      "Reviewed alert must have original transaction evidence",
    );
    assert(
      (await page.locator("#detail-evidence").textContent()).includes(
        original[0].transactionId,
      ),
    );
    assert(
      (await page.locator("#detail-history").textContent()).includes(
        snapshot.details[reviewed.id].reviewHistory[0].note,
      ),
    );
    assert.equal(await page.locator("#save-review").isDisabled(), true);
    await page.locator("#detail-dialog .dialog-close").click();
    report.checks.push(
      "Published alerts, original transaction evidence and preserved review history",
    );
    await page.locator("[data-view=transactions]").click();
    assert.equal(
      await page.locator("#transactions-table tbody tr").count(),
      snapshot.transactions.length,
    );
    await page.locator("[data-view=windows]").click();
    assert.equal(
      await page.locator("#windows-table tbody tr").count(),
      snapshot.windows.length,
    );
    await page.locator("[data-view=overview]").click();
    await page.screenshot({
      path: path.join(output, "desktop.png"),
      fullPage: true,
    });
    await page.setViewportSize({ width: 390, height: 844 });
    assert(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
      "Published page must fit a mobile viewport",
    );
    await page.screenshot({
      path: path.join(output, "mobile.png"),
      fullPage: true,
    });
    report.checks.push(
      "Transaction and window records",
      "Desktop and mobile layout",
    );
    assert.deepEqual(
      apiRequests,
      [],
      "Published website must not call an absent backend",
    );
    assert.deepEqual(
      errors,
      [],
      "Published website must have no browser or asset failures",
    );
    report.checks.push(
      "No browser errors, broken assets, API calls or remote mutations",
    );
    Object.assign(report, {
      status: "passed",
      sourceCommit: snapshot.sourceCommit,
      capturedAt: snapshot.capturedAt,
      sourceRun: snapshot.runUrl,
      transactions: snapshot.overview.transactions,
      alerts: snapshot.alerts.length,
    });
    console.log(
      `PASS: deployed GitHub Pages workspace at ${base}, verified commit ${expectedCommit}`,
    );
  } catch (error) {
    report.status = "failed";
    report.error = error.message;
    if (page)
      await page
        .screenshot({ path: path.join(output, "failure.png"), fullPage: true })
        .catch(() => {});
    throw error;
  } finally {
    fs.writeFileSync(
      path.join(output, "report.json"),
      JSON.stringify(report, null, 2) + "\n",
    );
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
