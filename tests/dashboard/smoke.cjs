const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

const staticRoot = path.resolve(
  __dirname,
  "../../services/api/src/main/resources/static",
);
const server = http.createServer((req, res) => {
  const target = path.join(
    staticRoot,
    req.url.split("?")[0] === "/" ? "index.html" : req.url.split("?")[0],
  );
  if (!target.startsWith(staticRoot + path.sep) || !fs.existsSync(target)) {
    res.writeHead(404);
    res.end();
    return;
  }
  res.setHeader(
    "Content-Type",
    {
      ".html": "text/html",
      ".css": "text/css",
      ".js": "application/javascript",
      ".svg": "image/svg+xml",
    }[path.extname(target)] || "text/plain",
  );
  fs.createReadStream(target).pipe(res);
});

async function waitText(page, selector, value) {
  await page.waitForFunction(
    ([selector, value]) =>
      document.querySelector(selector).textContent.includes(value),
    [selector, value],
  );
}
async function saveAction(page, action, note, disposition) {
  await page.locator("#review-action").selectOption(action);
  if (disposition)
    await page.locator("#review-disposition").selectOption(disposition);
  await page.locator("#review-note").fill(note);
  await page.locator("#save-review").click();
  await page.waitForFunction(
    () => document.querySelector("#review-note").value === "",
  );
}

(async () => {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: 1512, height: 1400 },
    });
    page.setDefaultTimeout(10000);
    const errors = [];
    page.on("pageerror", (error) => errors.push(error.message));
    await page.clock.install();
    await page.goto(`${base}/?demo=1`);
    await waitText(page, "#metric-transactions", "21");
    assert.match(
      await page.locator("#notice").textContent(),
      /not benchmark results/,
    );
    assert.equal(
      await page.locator("#transactions-table tbody tr").count(),
      21,
      "Sample totals must match available records",
    );
    await page.locator("[data-view=alerts]").click();
    await page.locator("#severity-filter").selectOption("HIGH");
    assert.equal(await page.locator("#all-alerts tbody tr").count(), 4);
    await page.locator("#all-alerts tbody tr").first().press("Enter");
    await waitText(page, "#detail-evidence", "Pinned detection evidence");
    assert.equal(await page.locator("#review-action").inputValue(), "CLAIM");
    assert.equal(
      await page.locator('#review-action option[value="RESOLVE"]').isDisabled(),
      true,
    );
    await page.locator("#review-analyst").fill("Risk operations");
    await saveAction(page, "CLAIM", "Investigating the high value payment.");
    await waitText(page, "#review-ownership", "Version 1");
    await page.locator("#review-action").selectOption("RESOLVE");
    await page
      .locator("#review-note")
      .fill("Checked this legitimate merchant order.");
    await page.locator("#save-review").click();
    assert.match(
      await page.locator("#toast").textContent(),
      /Choose a resolution outcome/,
    );
    await saveAction(
      page,
      "RESOLVE",
      "Checked this legitimate merchant order.",
      "BENIGN",
    );
    await waitText(page, "#review-ownership", "Outcome: Benign");
    assert.equal(await page.locator("#review-action").inputValue(), "REOPEN");
    await saveAction(page, "REOPEN", "Customer reported a new issue.");
    await saveAction(
      page,
      "RELEASE",
      "Escalate to the next available operator.",
    );
    assert.equal(await page.locator("#review-status").inputValue(), "OPEN");
    assert.match(
      await page.locator("#review-ownership").textContent(),
      /Unassigned/,
    );
    assert.equal(await page.locator("#detail-history li").count(), 4);
    await page.locator("#detail-dialog .dialog-close").click();
    await page.locator("[data-view=overview]").click();
    await page.evaluate(() => document.fonts.ready);
    if (process.env.SCREENSHOT_PATH) {
      await page.evaluate(() => {
        document.querySelector("#toast").hidden = true;
      });
      await page.screenshot({
        path: path.resolve(process.env.SCREENSHOT_PATH),
        fullPage: true,
      });
    }

    const posted = [],
      patched = [],
      queueQueries = [];
    let conflictNext = false,
      failNext = false;
    let record = {
      id: "HIGH_VALUE:txn_test",
      accountId: "<img src=x onerror=alert(1)>",
      currency: "USD",
      rule: "HIGH_VALUE",
      severity: "HIGH",
      score: 80,
      eventTime: new Date().toISOString(),
      reasons: ["Test signal"],
      status: "OPEN",
      owner: null,
      version: 0,
      reviewHistory: [],
    };
    const nextRecord = {
      ...record,
      id: "HIGH_VALUE:txn_next",
      accountId: "next-account",
    };
    await page.route("**/api/v1/**", async (route) => {
      const req = route.request(),
        url = new URL(req.url()),
        pathname = url.pathname;
      let body = {},
        status = 200;
      if (req.method() === "POST") {
        posted.push({
          body: req.postDataJSON(),
          key: req.headers()["x-api-key"],
        });
        body = { status: "ACCEPTED", duplicate: false };
        status = 202;
      } else if (req.method() === "PATCH") {
        const command = req.postDataJSON();
        patched.push({
          path: pathname,
          body: command,
          key: req.headers()["x-api-key"],
        });
        if (conflictNext) {
          conflictNext = false;
          record = {
            ...record,
            owner: "Other operator",
            version: record.version + 1,
          };
          body = { detail: "A concurrent review changed this record" };
          status = 409;
        } else if (failNext) {
          failNext = false;
          body = { detail: "Temporary upstream failure" };
          status = 503;
        } else {
          assert.equal(command.expectedVersion, record.version);
          assert.match(command.operationId, /^[A-Za-z0-9_-]{8,80}$/);
          const nextStatus = {
            CLAIM: "INVESTIGATING",
            COMMENT: "INVESTIGATING",
            RELEASE: "OPEN",
            RESOLVE: "RESOLVED",
            REOPEN: "INVESTIGATING",
          }[command.action];
          record = {
            ...record,
            status: nextStatus,
            owner: command.action === "RELEASE" ? null : command.analyst,
            version: record.version + 1,
            disposition: command.disposition || null,
            reviewHistory: [
              ...record.reviewHistory,
              {
                ...command,
                status: nextStatus,
                reviewedAt: new Date().toISOString(),
              },
            ],
          };
          body = record;
        }
      } else if (pathname.endsWith("/overview"))
        body = {
          transactions: 1,
          alerts: 2,
          highRisk: 1,
          volumeByCurrency: { USD: 750000 },
          pendingDelivery: 0,
        };
      else if (pathname.endsWith("/outcomes"))
        body = {
          resolvedAlerts: record.status === "RESOLVED" ? 1 : 0,
          byRule: [
            {
              rule: "HIGH_VALUE",
              resolved: record.status === "RESOLVED" ? 1 : 0,
              confirmedRisk: 0,
              falsePositive: record.status === "RESOLVED" ? 1 : 0,
              benign: 0,
            },
          ],
        };
      else if (pathname.endsWith("/alerts")) {
        queueQueries.push(Object.fromEntries(url.searchParams));
        const row = url.searchParams.get("cursor") ? nextRecord : record;
        const matches = ["severity", "status", "owner"].every(
          (key) =>
            !url.searchParams.get(key) ||
            row[key] === url.searchParams.get(key),
        );
        body = {
          items: matches ? [row] : [],
          nextCursor:
            !url.searchParams.get("cursor") &&
            !url.searchParams.get("status") &&
            !url.searchParams.get("owner")
              ? "next-token"
              : null,
        };
      } else if (pathname.endsWith("/evidence"))
        body = {
          items: [],
          provenance: "PINNED_DETECTION",
          evidenceCount: 3,
          matchedCount: 0,
          missingCount: 2,
          truncated: true,
          complete: false,
        };
      else if (pathname.includes("/alerts/")) body = record;
      else body = { items: [] };
      await route.fulfill({
        status,
        contentType: "application/json",
        body: JSON.stringify(body),
      });
    });
    await page.locator("#sample-toggle").uncheck();
    await waitText(page, "#connection", "API connected");
    assert.equal(
      await page.locator("#overview-alerts img").count(),
      0,
      "Untrusted data must be escaped",
    );
    await page.locator("[data-view=alerts]").click();
    await page.locator("#queue-next").click();
    await waitText(page, "#all-alerts", "next-account");
    assert(
      queueQueries.some((q) => q.cursor === "next-token"),
      "Next page must use server cursor",
    );
    await page.locator("#queue-previous").click();
    await waitText(page, "#all-alerts", "img src=x");
    await page.locator("#status-filter").selectOption("OPEN");
    await page.locator("#owner-filter").fill("Nobody");
    await page.locator("#apply-filters").click();
    await waitText(page, "#all-alerts", "No signals");
    assert(
      queueQueries.some(
        (q) =>
          q.status === "OPEN" && q.owner === "Nobody" && q.severity === "HIGH",
      ),
    );
    await page.locator("#owner-filter").fill("");
    await page.locator("#apply-filters").click();
    await page.locator("#status-filter").selectOption("");
    await page.locator("#settings-button").click();
    await page.locator("#api-key").fill("test-key");
    await page.locator("#save-settings").click();
    await page.locator("#simulate-button").click();
    await page.locator("#scenario-select").selectOption("card-testing");
    await page.locator("#send-scenario").click();
    await waitText(page, "#scenario-result", "6 transactions accepted");
    assert.equal(posted.length, 6);
    assert(
      posted.every(
        (entry) => entry.key === "test-key" && entry.body.amountMinor === 500,
      ),
    );
    assert.equal(
      new Set(posted.map((entry) => entry.body.transactionId)).size,
      6,
    );
    assert.equal(new Set(posted.map((entry) => entry.body.eventTime)).size, 1);
    await page.locator("#scenario-dialog .dialog-close").click();
    await page.locator("#all-alerts tbody tr").first().click();
    await waitText(page, "#detail-evidence", "Evidence is truncated");
    assert.match(
      await page.locator("#detail-evidence").textContent(),
      /2 pinned transaction\(s\) are missing/,
    );
    await page.locator("#review-analyst").fill("Reviewer one");
    await saveAction(page, "CLAIM", "Claimed to examine merchant activity.");
    assert.equal(patched[0].body.expectedVersion, 0);
    assert.equal(patched[0].key, "test-key");
    assert.equal(
      patched[0].path,
      "/api/v1/alerts/HIGH_VALUE%3Atxn_test/review",
    );
    conflictNext = true;
    await page
      .locator("#review-note")
      .fill("A draft that must survive the concurrent update.");
    await page.locator("#save-review").click();
    await waitText(page, "#review-ownership", "Other operator");
    assert.equal(
      await page.locator("#review-note").inputValue(),
      "A draft that must survive the concurrent update.",
    );
    assert.equal(await page.locator("#review-conflict").isVisible(), true);
    assert.equal(await page.locator("#save-review").isDisabled(), true);
    await page.locator("#review-analyst").fill("Other operator");
    failNext = true;
    await page.locator("#save-review").click();
    await waitText(page, "#toast", "Temporary upstream failure");
    const retryId = patched.at(-1).body.operationId;
    await page.locator("#save-review").click();
    await page.waitForFunction(
      () => document.querySelector("#review-note").value === "",
    );
    assert.equal(
      patched.at(-1).body.operationId,
      retryId,
      "Retry unchanged command with the same operation ID",
    );
    assert.equal(
      patched.at(-1).body.expectedVersion,
      2,
      "Retry must use reloaded version after 409",
    );
    await saveAction(
      page,
      "RESOLVE",
      "Merchant confirmed this was expected activity.",
      "FALSE_POSITIVE",
    );
    assert.equal(patched.at(-1).body.disposition, "FALSE_POSITIVE");
    assert.match(
      await page.locator("#detail-history").textContent(),
      /False Positive/,
    );
    await saveAction(
      page,
      "REOPEN",
      "New information requires another investigation.",
    );
    await saveAction(page, "RELEASE", "Handing the case back to the queue.");
    assert.equal(await page.locator("#review-status").inputValue(), "OPEN");
    await page.locator("#detail-dialog .dialog-close").click();

    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(`${base}/?demo=1`);
    assert(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
      "Mobile viewport must not overflow",
    );
    await page.locator("#mobile-view").selectOption("alerts");
    assert.equal(
      await page.locator("#alerts-view").isVisible(),
      true,
      "Mobile navigation must reach the queue",
    );
    assert(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
      "Mobile queue must not overflow",
    );
    await page.locator("#mobile-view").selectOption("settings");
    assert.equal(
      await page.locator("#settings-dialog").isVisible(),
      true,
      "Mobile operators must be able to configure API access",
    );
    await page.locator("#settings-dialog .dialog-close").click();
    await page.unroute("**/api/v1/**");
    await page.goto(base);
    await waitText(page, "#connection", "API unavailable");
    assert.match(
      await page.locator("#notice").textContent(),
      /Unable to refresh live data/,
    );
    assert.deepEqual(errors, []);
    console.log(
      "PASS: coherent sample evidence/totals, claim/resolve/reopen/release, required disposition, server queue filters/cursors, 409 draft retention and ownership refresh, idempotent retries, evidence warnings, escaping, scenario auth/IDs, mobile and offline state",
    );
  } finally {
    await browser.close();
    server.close();
  }
})().catch((error) => {
  console.error(error);
  server.close();
  process.exitCode = 1;
});
