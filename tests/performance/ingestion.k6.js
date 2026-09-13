import http from "k6/http";
import { check, fail } from "k6";
import execution from "k6/execution";
import { Counter } from "k6/metrics";

const base = (__ENV.BASE_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const rate = Number(__ENV.PG_RATE || 20);
const durationSeconds = Number(__ENV.PG_DURATION_SECONDS || 30);
const p95 = Number(__ENV.PG_P95_MS || 500);
const p99 = Number(__ENV.PG_P99_MS || 1000);
if (
  ![rate, durationSeconds, p95, p99].every(
    (value) => Number.isInteger(value) && value > 0,
  )
) {
  throw new Error(
    "Rate, duration and latency budgets must be positive integers.",
  );
}
const accepted = new Counter("accepted_events");

export const options = {
  scenarios: {
    ingestion: {
      executor: "constant-arrival-rate",
      rate,
      timeUnit: "1s",
      duration: `${durationSeconds}s`,
      preAllocatedVUs: 10,
      maxVUs: 30,
      gracefulStop: "15s",
    },
  },
  thresholds: {
    http_req_failed: ["rate==0"],
    checks: ["rate==1"],
    "http_req_duration{endpoint:ingest}": [`p(95)<${p95}`, `p(99)<${p99}`],
    dropped_iterations: ["count==0"],
    accepted_events: [`count>=${rate * durationSeconds}`],
  },
  summaryTrendStats: ["avg", "min", "med", "max", "p(95)", "p(99)"],
};

export function setup() {
  const response = http.get(`${base}/actuator/health/readiness`, {
    tags: { endpoint: "readiness" },
    timeout: "10s",
  });
  if (response.status !== 200)
    fail(
      "The API and its required services must be ready before the performance regression test.",
    );
  return {
    runId: `k6_${Date.now()}_${Math.random().toString(16).slice(2, 10)}`,
  };
}

export default function (data) {
  const sequence = execution.scenario.iterationInTest;
  const transactionId = `txn_${data.runId}_${sequence}`;
  const payload = {
    schemaVersion: 1,
    transactionId,
    accountId: `acct_${data.runId}_${sequence % 20}`,
    merchantId: `merchant_${sequence % 4}`,
    amountMinor: sequence % 40 === 0 ? 750000 : 8000 + sequence,
    currency: "USD",
    country: "US",
    channel: "WEB",
    eventTime: new Date().toISOString(),
  };
  const response = http.post(
    `${base}/api/v1/transactions`,
    JSON.stringify(payload),
    {
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": __ENV.INGEST_API_KEY || "local-dev-key",
      },
      tags: { endpoint: "ingest", name: "POST /api/v1/transactions" },
      timeout: "10s",
    },
  );
  let body;
  try {
    body = response.json();
  } catch (_) {
    body = {};
  }
  const valid = check(response, {
    "accepted durably with HTTP 202": (result) => result.status === 202,
    "response identifies the submitted immutable event": () =>
      body.transactionId === transactionId,
    "unique event is not reported as a duplicate": () =>
      body.duplicate === false,
  });
  if (valid) accepted.add(1);
}

const xml = (value) =>
  String(value).replace(
    /[&<>"']/g,
    (character) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&apos;",
      })[character],
  );

export function handleSummary(data) {
  const criteria = [];
  for (const [metric, details] of Object.entries(data.metrics || {})) {
    for (const [threshold, result] of Object.entries(
      details.thresholds || {},
    )) {
      criteria.push({ name: `${metric}: ${threshold}`, passed: result.ok });
    }
  }
  const failures = criteria.filter((criterion) => !criterion.passed).length;
  const report = {
    tool: "k6",
    scope:
      "Rate-limited HTTP ingestion regression; not maximum capacity or end-to-end Spark latency",
    sourceCommit: __ENV.GITHUB_SHA || null,
    configuredRate: rate,
    durationSeconds,
    expectedAcceptedEvents: rate * durationSeconds,
    criteria,
    results: data,
  };
  const junit = `<?xml version="1.0" encoding="UTF-8"?><testsuite name="k6 ingestion thresholds" tests="${criteria.length}" failures="${failures}">${criteria.map((criterion) => `<testcase name="${xml(criterion.name)}">${criterion.passed ? "" : '<failure message="Performance threshold failed"/>'}</testcase>`).join("")}</testsuite>\n`;
  return {
    "artifacts/k6-summary.json": JSON.stringify(report, null, 2) + "\n",
    "artifacts/k6-junit.xml": junit,
    stdout: `${criteria.map((criterion) => `${criterion.passed ? "PASS" : "FAIL"}: ${criterion.name}`).join("\n")}\nConfigured ${rate} events/s for ${durationSeconds}s. HTTP regression only.\n`,
  };
}
