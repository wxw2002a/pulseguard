"use strict";

const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");

const repoRoot = path.resolve(__dirname, "../..");
const mount = "/pulseguard/";
const capturedAt = "2026-09-13T06:18:00Z";
const verifiedWindowId = `acct_verified_02:USD:${Date.parse("2026-09-13T06:15:00.000Z")}`;
const originalNote = "Verified the original transaction and retained evidence after replay.";
const originalTransaction = {
  schemaVersion: 1,
  transactionId: "tx_verified_001",
  accountId: "acct_verified_01",
  merchantId: "merchant_verified_01",
  amountMinor: 750000,
  currency: "USD",
  country: "US",
  channel: "WEB",
  eventTime: "2026-09-13T06:15:12.123456789Z",
  ingestedAt: "2026-09-13T06:15:13.000Z",
  deliveryStatus: "SENT",
};
const highAlert = {
  id: "HIGH_VALUE:tx_verified_001",
  transactionId: originalTransaction.transactionId,
  accountId: originalTransaction.accountId,
  currency: "USD",
  rule: "HIGH_VALUE",
  severity: "HIGH",
  score: 90,
  amountMinor: 750000,
  reasons: ["Amount is at least 500000 minor units (5000 currency units)"],
  status: "INVESTIGATING",
  eventTime: originalTransaction.eventTime,
  createdAt: "2026-09-13T06:15:15.000Z",
  reviewedAt: "2026-09-13T06:16:02.000Z",
};
const mediumAlert = {
  id: `VELOCITY:${verifiedWindowId}`,
  accountId: "acct_verified_02",
  currency: "USD",
  rule: "VELOCITY",
  severity: "MEDIUM",
  score: 75,
  transactionCount: 6,
  totalAmountMinor: 3000,
  reasons: ["At least 5 distinct transactions within a UTC minute"],
  status: "OPEN",
  eventTime: "2026-09-13T06:16:00.000Z",
  windowStart: "2026-09-13T06:15:00.000Z",
  windowEnd: "2026-09-13T06:16:00.000Z",
  createdAt: "2026-09-13T06:16:03.000Z",
};
const fixture = {
  schemaVersion: 1,
  capturedAt,
  sourceCommit: "0123456789abcdef0123456789abcdef01234567",
  runUrl: "https://github.com/wxw2002a/pulseguard/actions/runs/34742236920",
  source: "verified-ci",
  dataset: "synthetic e2e and load events",
  overview: {
    transactions: 42,
    alerts: 2,
    highRisk: 1,
    volumeByCurrency: { USD: 753000, CAD: 0, EUR: 0, GBP: 0 },
    pendingDelivery: 0,
    reviewedAlerts: 1,
  },
  alerts: [highAlert, mediumAlert],
  transactions: [originalTransaction],
  windows: [
    {
      id: verifiedWindowId,
      accountId: "acct_verified_02",
      currency: "USD",
      windowStart: "2026-09-13T06:15:00.000Z",
      windowEnd: "2026-09-13T06:16:00.000Z",
      transactionCount: 6,
      totalAmountMinor: 3000,
      highValueCount: 0,
      updatedAt: "2026-09-13T06:16:03.000Z",
    },
  ],
  details: {
    [highAlert.id]: {
      ...highAlert,
      reviewHistoryCount: 2,
      reviewHistory: [
        {
          status: "INVESTIGATING",
          note: originalNote,
          analyst: "verified-ci-analyst",
          reviewedAt: "2026-09-13T06:16:01.000Z",
        },
        {
          status: "INVESTIGATING",
          note: "Operator text remains literal: <img src=x onerror=alert(1)>",
          analyst: "verified-ci-analyst",
          reviewedAt: "2026-09-13T06:16:02.000Z",
        },
      ],
    },
    [mediumAlert.id]: { ...mediumAlert, reviewHistoryCount: 0, reviewHistory: [] },
  },
  evidence: { [highAlert.id]: [originalTransaction], [mediumAlert.id]: [] },
};

function serveBuiltSite(directory, requests) {
  return http.createServer((request, response) => {
    const pathname = new URL(request.url, "http://127.0.0.1").pathname;
    requests.push({ method: request.method, pathname });
    if (!pathname.startsWith(mount)) {
      response.writeHead(404).end("Site is available under /pulseguard/");
      return;
    }
    let relative;
    try {
      relative = decodeURIComponent(pathname.slice(mount.length)) || "index.html";
    } catch {
      response.writeHead(400).end("Invalid path");
      return;
    }
    const target = path.resolve(directory, relative);
    if (!target.startsWith(directory + path.sep) || !fs.existsSync(target) || !fs.statSync(target).isFile()) {
      response.writeHead(404).end("Not found");
      return;
    }
    response.setHeader("Content-Type", {
      ".html": "text/html; charset=utf-8",
      ".css": "text/css; charset=utf-8",
      ".js": "application/javascript; charset=utf-8",
      ".json": "application/json; charset=utf-8",
      ".svg": "image/svg+xml",
    }[path.extname(target)] || "application/octet-stream");
    fs.createReadStream(target).pipe(response);
  });
}

async function main() {
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "pulseguard-pages-"));
  let server;
  let browser;
  try {
    const snapshotFile = path.join(temporary, "verified-fixture.json");
    const output = path.join(temporary, "site");
    fs.writeFileSync(snapshotFile, JSON.stringify(fixture, null, 2));
    execFileSync(process.env.PYTHON || (process.platform === "win32" ? "python" : "python3"), [
      path.join(repoRoot, "scripts/build_pages.py"), "--snapshot", snapshotFile, "--output", output,
    ], { cwd: repoRoot, stdio: "pipe" });
    assert(fs.existsSync(path.join(output, "index.html")), "Builder must emit the Pages entry point");
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(output, "snapshot.json"), "utf8")), fixture,
      "Builder must preserve the supplied verified API results");

    const servedRequests = [];
    server = serveBuiltSite(output, servedRequests);
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const base = `http://127.0.0.1:${server.address().port}`;
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage({ viewport: { width: 1512, height: 1100 }, locale: "en-US", timezoneId: "UTC" });
    page.setDefaultTimeout(10000);
    const errors = [];
    const apiRequests = [];
    const mutationRequests = [];
    const missingAssets = [];
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("request", (request) => {
      if (new URL(request.url()).pathname.includes("/api/v1")) apiRequests.push(request.url());
      if (!["GET", "HEAD", "OPTIONS"].includes(request.method())) mutationRequests.push(`${request.method()} ${request.url()}`);
    });
    page.on("response", (response) => {
      if (response.status() >= 400 && /\.(?:js|css|json|svg)$/.test(new URL(response.url()).pathname)) {
        missingAssets.push(`${response.status()} ${response.url()}`);
      }
    });
    await page.route("**/api/v1/**", (route) => route.fulfill({
      status: 503, contentType: "application/json", body: '{"detail":"Pages must not call a live API"}',
    }));
    await page.clock.install();
    const navigation = await page.goto(`${base}${mount}`, { waitUntil: "networkidle" });
    assert.equal(navigation.status(), 200, `Pages entry point failed: ${await page.locator("body").innerText()}`);
    assert.equal(new URL(await page.locator("a.brand").getAttribute("href"), page.url()).pathname, mount,
      "Brand navigation must remain inside the GitHub project path");
    await page.waitForFunction(() => document.querySelector("#metric-transactions").textContent === "42");
    assert.equal(await page.locator("#sample-toggle").isChecked(), false, "Pages must open the verified snapshot by default");
    assert.match(await page.locator("#connection").textContent(), /verified run|snapshot/i);
    const notice = await page.locator("#notice").textContent();
    assert.match(notice, /snapshot/i);
    const timestampForms = await page.evaluate((value) => [value, new Date(value).toISOString(), new Date(value).toLocaleString()], capturedAt);
    assert(timestampForms.some((stamp) => notice.includes(stamp)), "Snapshot notice must expose its capture timestamp");
    assert.equal(await page.locator(`#notice a[href="${fixture.runUrl}"]`).count(), 1, "Snapshot notice must link its verified source run");
    assert.equal(await page.locator("#simulate-button").isDisabled(), true, "Captured data must not offer pipeline writes");

    await page.locator("[data-view=alerts]").click();
    assert.equal(await page.locator("#all-alerts tbody tr").count(), 2);
    await page.locator("#severity-filter").selectOption("MEDIUM");
    assert.equal(await page.locator("#all-alerts tbody tr").count(), 1);
    assert.match(await page.locator("#all-alerts").textContent(), /acct_verified_02/);
    await page.locator("#severity-filter").selectOption("HIGH");
    assert.equal(await page.locator("#all-alerts tbody tr").count(), 1);
    await page.locator("#all-alerts tbody tr").first().press("Enter");
    await page.waitForFunction(() => document.querySelector("#detail-evidence").textContent.includes("tx_verified_001"));
    assert.match(await page.locator("#detail-evidence").textContent(), /7,500\.00/);
    assert.match(await page.locator("#detail-history").textContent(), /verified-ci-analyst/);
    assert((await page.locator("#detail-history").textContent()).includes(originalNote));
    assert.equal(await page.locator("#detail-history img").count(), 0, "Snapshot operator notes must remain escaped text");
    assert.equal(await page.locator("#save-review").isDisabled(), true, "Snapshot reviews must be read-only");
    assert.equal(await page.locator("#review-status").inputValue(), "INVESTIGATING");
    await page.locator("#detail-dialog .dialog-close").click();

    await page.locator("[data-view=transactions]").click();
    assert.match(await page.locator("#transactions-table").textContent(), /tx_verified_001/);
    await page.locator("#refresh-button").click();
    await page.locator("[data-view=windows]").click();
    assert.match(await page.locator("#windows-table").textContent(), /acct_verified_02/);
    await page.clock.fastForward(6000);
    assert.equal(await page.locator("#metric-transactions").textContent(), "42");

    await page.locator("#sample-toggle").check();
    await page.waitForFunction(() => document.querySelector("#metric-transactions").textContent === "24,862");
    assert.match(await page.locator("#notice").textContent(), /sample/i);
    assert.equal(await page.locator("#simulate-button").isDisabled(), false);
    await page.locator("[data-view=alerts]").click();
    const sampleRow = page.locator("#all-alerts tbody tr").first();
    const sampleId = await sampleRow.getAttribute("data-alert-id");
    await sampleRow.click();
    assert.equal(await page.locator("#save-review").isDisabled(), false);
    await page.locator("#review-status").selectOption("RESOLVED");
    await page.locator("#review-analyst").fill("Local sample operator");
    await page.locator("#review-note").fill("This local sample decision must never alter the verified snapshot.");
    await page.locator("#save-review").click();
    await page.waitForFunction(() => !document.querySelector("#detail-dialog").open);
    assert.match(await page.locator(`#all-alerts [data-alert-id="${sampleId}"]`).textContent(), /Resolved/);
    await page.locator(`#all-alerts [data-alert-id="${sampleId}"]`).click();
    assert.match(await page.locator("#detail-history").textContent(), /local sample decision/);
    await page.locator("#detail-dialog .dialog-close").click();
    await page.locator("#simulate-button").click();
    await page.locator("#scenario-select").selectOption("card-testing");
    await page.locator("#send-scenario").click();
    await page.waitForFunction(() => document.querySelector("#scenario-result").textContent.length > 0);
    assert.match(await page.locator("#scenario-result").textContent(), /sample|local|illustrative|preview|connect/i);
    await page.locator("#scenario-dialog .dialog-close").click();

    await page.locator("#sample-toggle").uncheck();
    await page.waitForFunction(() => document.querySelector("#metric-transactions").textContent === "42");
    assert.match(await page.locator("#notice").textContent(), /snapshot/i);
    assert.equal(await page.locator("#simulate-button").isDisabled(), true);
    await page.locator("#all-alerts tbody tr").first().click();
    await page.waitForFunction(() => document.querySelector("#detail-history").textContent.includes("verified-ci-analyst"));
    assert.equal(await page.locator("#review-status").inputValue(), "INVESTIGATING", "Returning from sample mode must restore the original decision");
    assert.equal(await page.locator("#detail-history .review-history li").count(), 2);
    assert((await page.locator("#detail-history").textContent()).includes(originalNote));
    assert.doesNotMatch(await page.locator("#detail-history").textContent(), /local sample decision/);
    assert.equal(await page.locator("#save-review").isDisabled(), true);
    await page.locator("#detail-dialog .dialog-close").click();
    await page.locator("[data-view=overview]").click();
    await page.setViewportSize({ width: 390, height: 844 });
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), "Pages snapshot must fit a mobile viewport");

    assert.deepEqual(apiRequests, [], "Pages browsing, auto-refresh, and local samples must never request a live API");
    assert.deepEqual(mutationRequests, [], "Pages must never send remote mutations");
    assert.deepEqual(missingAssets, [], "Assets must resolve below the GitHub project path");
    assert(servedRequests.some((request) => request.pathname === `${mount}snapshot.json`), "Runtime must load the built snapshot below /pulseguard/");
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(output, "snapshot.json"), "utf8")), fixture);
    assert.deepEqual(errors, []);
    console.log("PASS: Pages build/subpath, verified source/capture, snapshot evidence/history, filters, read-only controls, isolated sample edits, mobile, zero live API requests");
  } finally {
    if (browser) await browser.close();
    if (server) await new Promise((resolve) => server.close(resolve));
    const resolved = path.resolve(temporary);
    const allowedPrefix = path.resolve(os.tmpdir()) + path.sep + "pulseguard-pages-";
    if (!resolved.startsWith(allowedPrefix)) throw new Error("Refusing to clean an unexpected test directory");
    fs.rmSync(resolved, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
