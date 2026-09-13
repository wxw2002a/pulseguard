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

(async () => {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: 1512, height: 1400 },
    });
    const errors = [];
    page.on("pageerror", (error) => errors.push(error.message));
    await page.goto(`${base}/?demo=1`);
    await page.waitForFunction(
      () =>
        document.querySelector("#metric-transactions").textContent === "24,862",
    );
    assert.match(
      await page.locator("#notice").textContent(),
      /not benchmark results/,
    );
    await page.locator("[data-view=alerts]").click();
    await page.locator("#severity-filter").selectOption("HIGH");
    assert.equal(await page.locator("#all-alerts tbody tr").count(), 4);
    await page.locator("#all-alerts tbody tr").first().press("Enter");
    await page.locator("#review-status").selectOption("RESOLVED");
    await page.locator("#review-analyst").fill("Risk operations");
    await page
      .locator("#review-note")
      .fill("Checked the related transactions and recorded the decision.");
    await page.locator("#save-review").click();
    assert.match(
      await page.locator("#all-alerts tbody tr").first().textContent(),
      /Resolved/,
    );
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

    const posted = [];
    let patched;
    await page.route("**/api/v1/**", async (route) => {
      const req = route.request();
      const pathname = new URL(req.url()).pathname;
      let body = {};
      if (req.method() === "POST") {
        posted.push({
          body: req.postDataJSON(),
          key: req.headers()["x-api-key"],
        });
        body = { status: "ACCEPTED", duplicate: false };
      } else if (req.method() === "PATCH") {
        patched = { path: pathname, body: req.postDataJSON() };
      } else if (pathname.endsWith("/overview"))
        body = {
          transactions: 1,
          alerts: 1,
          highRisk: 1,
          volumeByCurrency: { USD: 750000 },
          pendingDelivery: 0,
        };
      else if (pathname.endsWith("/alerts"))
        body = {
          items: [
            {
              id: "HIGH_VALUE:txn_test",
              accountId: "<img src=x onerror=alert(1)>",
              currency: "USD",
              rule: "HIGH_VALUE",
              severity: "HIGH",
              score: 80,
              eventTime: new Date().toISOString(),
              reasons: ["Test signal"],
              status: "OPEN",
            },
          ],
        };
      else body = { items: [] };
      await route.fulfill({
        status: req.method() === "POST" ? 202 : 200,
        contentType: "application/json",
        body: JSON.stringify(body),
      });
    });
    await page.locator("#sample-toggle").uncheck();
    await page.waitForFunction(() =>
      document
        .querySelector("#connection")
        .textContent.includes("API connected"),
    );
    assert.equal(
      await page.locator("#overview-alerts img").count(),
      0,
      "Untrusted data must be escaped",
    );
    await page.locator("#settings-button").click();
    await page.locator("#api-key").fill("test-key");
    await page.locator("#save-settings").click();
    await page.locator("#simulate-button").click();
    await page.locator("#scenario-select").selectOption("card-testing");
    await page.locator("#send-scenario").click();
    await page.waitForFunction(() =>
      document
        .querySelector("#scenario-result")
        .textContent.includes("6 transactions accepted"),
    );
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
    assert.equal(
      new Set(posted.map((entry) => entry.body.eventTime)).size,
      1,
      "One burst must share a window timestamp",
    );
    await page.locator("#scenario-dialog .dialog-close").click();
    await page.locator("#overview-alerts tbody tr").click();
    await page.locator("#review-status").selectOption("INVESTIGATING");
    await page.locator("#review-analyst").fill("Reviewer one");
    await page
      .locator("#review-note")
      .fill("Check merchant activity before resolving this signal.");
    await page.locator("#save-review").click();
    await page.waitForFunction(
      () => !document.querySelector("#detail-dialog").open,
    );
    assert.equal(patched.body.status, "INVESTIGATING");
    assert.equal(patched.body.analyst, "Reviewer one");
    assert.match(patched.body.note, /merchant activity/);
    assert.equal(patched.path, "/api/v1/alerts/HIGH_VALUE%3Atxn_test/review");

    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(`${base}/?demo=1`);
    assert(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
      "Mobile viewport must not overflow",
    );
    await page.unroute("**/api/v1/**");
    await page.goto(base);
    await page.waitForFunction(() =>
      document
        .querySelector("#connection")
        .textContent.includes("API unavailable"),
    );
    assert.match(
      await page.locator("#notice").textContent(),
      /Unable to refresh live data/,
    );
    assert.deepEqual(errors, []);
    console.log(
      "PASS: preview labeling, filter, keyboard details, review, live API, HTML escaping, scenario auth/IDs, mobile, offline state",
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
