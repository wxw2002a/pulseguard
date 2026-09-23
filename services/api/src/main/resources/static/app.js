"use strict";
const $ = (selector) => document.querySelector(selector);
const hostedSnapshot = window.PULSEGUARD_RUNTIME?.mode === "snapshot";
let snapshotPromise;
const state = {
  sample: new URLSearchParams(location.search).get("demo") === "1",
  overview: {},
  alerts: [],
  transactions: [],
  windows: [],
  selected: null,
  loading: false,
  recentAlerts: [],
  sampleAlerts: null,
  detail: null,
  outcomes: null,
  cursor: null,
  nextCursor: null,
  previousCursors: [],
  pendingOperation: null,
  saving: false,
};
const keyName = "pulseguard-api-key";
const escapeHtml = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const pretty = (value) =>
  String(value || "")
    .replaceAll("_", " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
const number = (value) => new Intl.NumberFormat("en-US").format(value ?? 0);
const money = (minor, currency = "USD") =>
  new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format((minor || 0) / 100);
const time = (value) =>
  value && !Number.isNaN(new Date(value).getTime())
    ? new Date(value).toLocaleTimeString("en-GB", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      })
    : "—";
const idOf = (item) => item.id || item._id;
const titles = {
  overview: [
    "A clearer view of risk<span>.</span>",
    "Follow the flow. Find the signals. Act with context.",
    "Overview",
  ],
  alerts: [
    "Turn signals into decisions<span>.</span>",
    "An explainable reason behind every alert.",
    "Alert inbox",
  ],
  transactions: [
    "Every event, accounted for<span>.</span>",
    "A reliable record from acceptance to delivery.",
    "Transactions",
  ],
  windows: [
    "Patterns emerge over time<span>.</span>",
    "Explore activity by account, currency and event-time window.",
    "Account windows",
  ],
  pipeline: [
    "Built to keep its promises<span>.</span>",
    "Trace the path from an accepted transaction to a durable risk signal.",
    "Pipeline",
  ],
};

function toast(message) {
  $("#toast").textContent = message;
  $("#toast").hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => {
    $("#toast").hidden = true;
  }, 5500);
}

function switchView(view) {
  if (!titles[view]) return;
  document
    .querySelectorAll(".view")
    .forEach((item) =>
      item.classList.toggle("active", item.id === `${view}-view`),
    );
  document
    .querySelectorAll("[data-view]")
    .forEach((item) =>
      item.classList.toggle("active", item.dataset.view === view),
    );
  $("#page-title").innerHTML = titles[view][0];
  $("#page-subtitle").textContent = titles[view][1];
  $("#breadcrumb-label").textContent = titles[view][2];
  $("#mobile-view").value = view;
}

async function loadSnapshot() {
  if (!snapshotPromise) {
    snapshotPromise = (async () => {
      const response = await fetch(window.PULSEGUARD_RUNTIME.snapshotUrl, {
        signal: AbortSignal.timeout(12000),
      });
      if (!response.ok)
        throw new Error(`Snapshot returned HTTP ${response.status}`);
      const snapshot = await response.json();
      if (
        snapshot.schemaVersion !== 1 ||
        snapshot.source !== "verified-ci" ||
        !/^https:\/\/github\.com\/wxw2002a\/pulseguard\/actions\/runs\/\d+$/.test(
          snapshot.runUrl,
        ) ||
        Number.isNaN(new Date(snapshot.capturedAt).getTime()) ||
        ![snapshot.alerts, snapshot.transactions, snapshot.windows].every(
          Array.isArray,
        ) ||
        !snapshot.overview ||
        !snapshot.details ||
        !snapshot.evidence
      ) {
        throw new Error("The verified snapshot is incomplete or invalid.");
      }
      return snapshot;
    })().catch((error) => {
      snapshotPromise = undefined;
      throw error;
    });
  }
  return snapshotPromise;
}

async function snapshotRequest(path, options) {
  if (options.method && options.method !== "GET")
    throw new Error("The published snapshot is read-only.");
  const snapshot = await loadSnapshot();
  const url = new URL(path, location.origin);
  if (url.pathname === "/overview") return structuredClone(snapshot.overview);
  if (url.pathname === "/outcomes")
    return structuredClone(snapshot.outcomes || { unavailable: true });
  const collection = {
    "/alerts": "alerts",
    "/transactions": "transactions",
    "/windows": "windows",
  }[url.pathname];
  if (collection) {
    const requestedLimit = Number(url.searchParams.get("limit") || 200);
    const limit = Math.min(200, Math.max(1, requestedLimit || 200));
    const records = snapshot[collection].filter(
      (item) =>
        collection !== "alerts" ||
        ["status", "owner", "severity"].every(
          (field) =>
            !url.searchParams.get(field) ||
            (item[field] || (field === "status" ? "OPEN" : "")) ===
              url.searchParams.get(field),
        ),
    );
    const offset = Math.max(0, Number(url.searchParams.get("cursor")) || 0);
    return {
      items: structuredClone(records.slice(offset, offset + limit)),
      nextCursor:
        offset + limit < records.length ? String(offset + limit) : null,
    };
  }
  const match = url.pathname.match(/^\/alerts\/([^/]+)(\/evidence)?$/);
  if (match) {
    const id = decodeURIComponent(match[1]);
    if (!Object.hasOwn(snapshot.details, id))
      throw new Error("Alert is absent from this snapshot.");
    const evidence = snapshot.evidence[id];
    return match[2]
      ? structuredClone(
          Array.isArray(evidence)
            ? { ...snapshot.evidenceMetadata?.[id], items: evidence }
            : evidence || { items: [] },
        )
      : structuredClone(snapshot.details[id]);
  }
  throw new Error("This view is not available in the published snapshot.");
}

async function request(path, options = {}) {
  if (hostedSnapshot) return snapshotRequest(path, options);
  const headers = { Accept: "application/json", ...options.headers };
  if (options.body) headers["Content-Type"] = "application/json";
  if (options.method && options.method !== "GET")
    headers["X-API-Key"] = sessionStorage.getItem(keyName) || "";
  const response = await fetch(`/api/v1${path}`, {
    ...options,
    headers,
    signal: AbortSignal.timeout(12000),
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(
      response.status === 401
        ? "Set your API key in Connection settings first."
        : body.detail || body.message || `API returned HTTP ${response.status}`,
    );
    error.status = response.status;
    throw error;
  }
  return body;
}

function outcomesFor(alerts) {
  const byRule = new Map();
  for (const alert of alerts) {
    const row = byRule.get(alert.rule) || {
      rule: alert.rule,
      resolved: 0,
      confirmedRisk: 0,
      falsePositive: 0,
      benign: 0,
      unclassified: 0,
    };
    if (alert.status === "RESOLVED") {
      row.resolved++;
      const key = {
        CONFIRMED_RISK: "confirmedRisk",
        FALSE_POSITIVE: "falsePositive",
        BENIGN: "benign",
      }[alert.disposition];
      if (key) row[key]++;
      else row.unclassified++;
    }
    byRule.set(alert.rule, row);
  }
  return {
    resolvedAlerts: [...byRule.values()].reduce(
      (sum, row) => sum + row.resolved,
      0,
    ),
    byRule: [...byRule.values()],
  };
}

function sampleData() {
  const minute = Math.floor(Date.now() / 60000) * 60000 - 120000;
  const rules = [
    "HIGH_VALUE",
    "VELOCITY",
    "CARD_TESTING",
    "HIGH_VALUE",
    "VELOCITY",
    "HIGH_VALUE",
  ];
  const transactions = [];
  const windows = [];
  const alerts = rules.map((rule, index) => {
    const accountId = `acc_sample_${index + 1}`;
    const count = rule === "HIGH_VALUE" ? 1 : 6;
    const records = Array.from({ length: count }, (_, n) => ({
      transactionId: `txn_sample_${index}_${n}`,
      accountId,
      merchantId: `merchant_${(n % 3) + 1}`,
      currency: "USD",
      country: "US",
      channel: "WEB",
      amountMinor:
        rule === "CARD_TESTING" ? 500 : rule === "HIGH_VALUE" ? 750000 : 12500,
      eventTime: new Date(minute + n * 3000).toISOString(),
      deliveryStatus: "SENT",
    }));
    transactions.push(...records);
    const window = {
      id: `window_sample_${index}`,
      accountId,
      currency: "USD",
      windowStart: new Date(minute).toISOString(),
      windowEnd: new Date(minute + 60000).toISOString(),
      transactionCount: count,
      totalAmountMinor: records.reduce((sum, t) => sum + t.amountMinor, 0),
      highValueCount: rule === "HIGH_VALUE" ? 1 : 0,
    };
    windows.push(window);
    return {
      id: `sample_${index}`,
      accountId,
      rule,
      currency: "USD",
      severity: rule === "VELOCITY" ? "MEDIUM" : "HIGH",
      score: rule === "VELOCITY" ? 65 : 80,
      transactionId: rule === "HIGH_VALUE" ? records[0].transactionId : null,
      amountMinor: rule === "HIGH_VALUE" ? records[0].amountMinor : null,
      windowStart: window.windowStart,
      windowEnd: window.windowEnd,
      evidenceTransactionIds: records.map((t) => t.transactionId),
      eventTime: records[0].eventTime,
      createdAt: records[0].eventTime,
      status: index === 3 ? "RESOLVED" : index === 1 ? "INVESTIGATING" : "OPEN",
      owner: [1, 3].includes(index) ? "Sample analyst" : null,
      version: index === 3 ? 2 : index === 1 ? 1 : 0,
      disposition: index === 3 ? "BENIGN" : null,
      reasons: [
        rule === "HIGH_VALUE"
          ? "Transaction is at least 500,000 minor units."
          : rule === "CARD_TESTING"
            ? "Six payments of 500 minor units occurred in one account minute."
            : "Six distinct payments occurred in one account minute.",
      ],
      reviewHistory: [
        ...([1, 3].includes(index)
          ? [
              {
                action: "CLAIM",
                status: "INVESTIGATING",
                analyst: "Sample analyst",
                note: "Synthetic example: claimed for merchant investigation.",
                reviewedAt: new Date(minute + 70000).toISOString(),
              },
            ]
          : []),
        ...(index === 3
          ? [
              {
                action: "RESOLVE",
                status: "RESOLVED",
                analyst: "Sample analyst",
                disposition: "BENIGN",
                note: "Synthetic example: this was a scheduled merchant payment.",
                reviewedAt: new Date(minute + 90000).toISOString(),
              },
            ]
          : []),
      ],
    };
  });
  return {
    overview: {
      transactions: transactions.length,
      alerts: alerts.length,
      highRisk: alerts.filter(
        (a) => a.severity === "HIGH" && a.status !== "RESOLVED",
      ).length,
      volumeByCurrency: {
        USD: transactions.reduce((sum, t) => sum + t.amountMinor, 0),
      },
      pendingDelivery: 0,
      reviewedAlerts: 2,
    },
    alerts,
    recentAlerts: alerts,
    sampleAlerts: alerts,
    outcomes: outcomesFor(alerts),
    windows,
    transactions,
  };
}

function empty(
  message,
  detail = "Run a scenario to send synthetic events through the pipeline.",
) {
  return `<div class="empty-state"><strong>${escapeHtml(message)}</strong>${escapeHtml(detail)}</div>`;
}

function alertTable(alerts) {
  if (!alerts.length) return empty("No signals in this view");
  return `<div class="table-wrap"><table><thead><tr><th>Account / signal</th><th>Risk level</th><th>Rule</th><th>Score</th><th>Event time</th><th>Status / owner</th><th></th></tr></thead><tbody>${alerts.map((alert) => `<tr class="clickable" tabindex="0" data-alert-id="${escapeHtml(idOf(alert))}"><td><div class="account"><span class="account-icon">↗</span><span>${escapeHtml(alert.accountId)}<span class="cell-sub">${alert.amountMinor ? escapeHtml(money(alert.amountMinor, alert.currency)) : "Account activity"} · ${escapeHtml(alert.currency)}</span></span></div></td><td><span class="badge ${alert.severity === "HIGH" ? "high" : "medium"}">${escapeHtml(pretty(alert.severity))}</span></td><td>${escapeHtml(pretty(alert.rule))}</td><td>${escapeHtml(alert.score)}<span class="score-track"><i style="width:${Math.max(0, Math.min(100, Number(alert.score) || 0))}%"></i></span></td><td>${escapeHtml(time(alert.eventTime || alert.createdAt))}</td><td><span class="badge ${["OPEN", "INVESTIGATING", "RESOLVED"].includes(alert.status) ? alert.status.toLowerCase() : "open"}">${escapeHtml(pretty(alert.status || "OPEN"))}</span><span class="cell-sub">${escapeHtml(alert.owner || "Unassigned")}</span></td><td class="row-arrow">↗</td></tr>`).join("")}</tbody></table></div>`;
}

function renderChart() {
  const grouped = new Map();
  for (const window of state.windows) {
    const stamp = new Date(window.windowStart).getTime();
    if (!Number.isFinite(stamp)) continue;
    const old = grouped.get(stamp) || { count: 0, high: 0 };
    grouped.set(stamp, {
      count: old.count + Number(window.transactionCount || 0),
      high: old.high + Number(window.highValueCount || 0),
    });
  }
  const buckets = [...grouped.entries()].sort((a, b) => a[0] - b[0]).slice(-12);
  if (!buckets.length) {
    $("#activity-chart").innerHTML = empty(
      "Waiting for event-time windows",
      "Window activity appears after Spark processes your transactions.",
    );
    return;
  }
  const max = Math.max(...buckets.map(([, value]) => value.count), 4);
  const ceiling = Math.ceil(max / 4) * 4;
  const width = 620,
    height = 156,
    left = 32,
    top = 7,
    plotHeight = 117;
  const step = (width - left - 8) / buckets.length;
  let svg = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Recent transaction activity by minute"><title>Recent transaction activity by minute</title>`;
  for (let i = 0; i <= 4; i++) {
    const y = top + plotHeight - (i * plotHeight) / 4;
    svg += `<line x1="${left}" y1="${y}" x2="${width}" y2="${y}" stroke="#e9ede4" stroke-dasharray="3 5"/><text x="0" y="${y + 3}">${number((ceiling * i) / 4)}</text>`;
  }
  buckets.forEach(([stamp, value], i) => {
    const x = left + step * i + step * 0.2;
    const barWidth = Math.min(19, step * 0.32);
    const barHeight = (value.count / ceiling) * plotHeight;
    const highHeight = (value.high / ceiling) * plotHeight;
    svg += `<rect x="${x}" y="${top + plotHeight - barHeight}" width="${barWidth}" height="${barHeight}" rx="3" fill="#58927f"><title>${value.count} transactions</title></rect><rect x="${x + barWidth + 3}" y="${top + plotHeight - highHeight}" width="${Math.max(4, barWidth * 0.38)}" height="${highHeight}" rx="2" fill="#cd957c"><title>${value.high} high-value transactions</title></rect>`;
    if (buckets.length < 9 || i % 2 === 0 || i === buckets.length - 1)
      svg += `<text text-anchor="middle" x="${left + step * (i + 0.5)}" y="148">${escapeHtml(new Date(stamp).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" }))}</text>`;
  });
  $("#activity-chart").innerHTML = svg + "</svg>";
}

function render() {
  const o = state.overview;
  $("#metric-transactions").textContent = number(o.transactions);
  $("#metric-highrisk").textContent = number(o.highRisk);
  $("#metric-pending").textContent = number(o.pendingDelivery);
  const currencies = Object.entries(o.volumeByCurrency || {});
  const mainCurrency =
    currencies.find(([currency]) => currency === "USD") || currencies[0];
  $("#metric-volume").textContent = mainCurrency
    ? money(mainCurrency[1], mainCurrency[0])
    : "$0.00";
  $("#volume-detail").textContent = mainCurrency
    ? `${mainCurrency[0]}${
        currencies.length > 1
          ? ` · ${currencies
              .filter(([c]) => c !== mainCurrency[0])
              .map(([c, amount]) => `${money(amount, c)} ${c}`)
              .join(" · ")}`
          : " · Amounts stored in minor units"
      }`
    : "No ingested transaction volume yet";
  $("#nav-alert-count").textContent = number(o.alerts);
  $("#signal-count").textContent = number(o.alerts);
  $(".metric svg").style.display = state.sample ? "" : "none";
  $("#overview-alerts").innerHTML = alertTable(state.recentAlerts.slice(0, 5));
  renderQueue();
  renderOutcomes();
  $("#transactions-table").innerHTML = state.transactions.length
    ? `<div class="table-wrap"><table><thead><tr><th>Transaction</th><th>Account</th><th>Merchant</th><th>Amount</th><th>Channel</th><th>Event time</th><th>Delivery</th></tr></thead><tbody>${state.transactions.map((t) => `<tr><td class="mono">${escapeHtml(t.transactionId)}</td><td>${escapeHtml(t.accountId)}</td><td>${escapeHtml(t.merchantId)}</td><td>${escapeHtml(money(t.amountMinor, t.currency))} ${escapeHtml(t.currency)}</td><td>${escapeHtml(t.channel)}</td><td>${escapeHtml(time(t.eventTime))}</td><td><span class="badge ${t.deliveryStatus === "SENT" ? "resolved" : "open"}">${escapeHtml(t.deliveryStatus || "PENDING")}</span></td></tr>`).join("")}</tbody></table></div>`
    : empty("Your ledger is ready");
  $("#windows-table").innerHTML = state.windows.length
    ? `<div class="table-wrap"><table><thead><tr><th>Account</th><th>Currency</th><th>Window start</th><th>Window end</th><th>Unique events</th><th>Volume</th><th>High value</th></tr></thead><tbody>${state.windows.map((w) => `<tr><td>${escapeHtml(w.accountId)}</td><td>${escapeHtml(w.currency)}</td><td>${escapeHtml(time(w.windowStart))}</td><td>${escapeHtml(time(w.windowEnd))}</td><td>${number(w.transactionCount)}</td><td>${escapeHtml(money(w.totalAmountMinor, w.currency))}</td><td>${number(w.highValueCount)}</td></tr>`).join("")}</tbody></table></div>`
    : empty("No account windows yet");
  renderChart();
}

function renderOutcomes() {
  const outcomes = state.outcomes;
  $("#rule-outcomes").innerHTML =
    !outcomes || outcomes.unavailable
      ? empty(
          "Outcome counts unavailable",
          "This captured run predates outcome tracking, or the outcome endpoint could not be loaded.",
        )
      : outcomes.byRule?.length
        ? `<div class="table-wrap"><table><thead><tr><th>Rule</th><th>Resolved</th><th>Confirmed risk</th><th>False positive</th><th>Benign</th><th>Unclassified legacy</th></tr></thead><tbody>${outcomes.byRule.map((row) => `<tr><td>${escapeHtml(pretty(row.rule))}</td><td>${number(row.resolved)}</td><td>${number(row.confirmedRisk)}</td><td>${number(row.falsePositive)}</td><td>${number(row.benign)}</td><td>${number(row.unclassified)}</td></tr>`).join("")}</tbody></table></div>`
        : empty(
            "No outcomes recorded",
            "Resolve an owned investigation with a disposition to record its operational outcome.",
          );
}

function queueQuery() {
  const params = new URLSearchParams({ limit: hostedSnapshot ? "200" : "25" });
  for (const field of ["status", "owner", "severity"]) {
    const value = $(`#${field}-filter`).value.trim();
    if (value) params.set(field, value);
  }
  if (state.cursor) params.set("cursor", state.cursor);
  return params;
}
function renderQueue() {
  $("#all-alerts").innerHTML = alertTable(state.alerts);
  $("#queue-summary").textContent =
    `Page ${state.previousCursors.length + 1} · ${number(state.alerts.length)} signals${hostedSnapshot && !state.sample ? " in captured records" : ""}`;
  $("#queue-previous").disabled = !state.previousCursors.length;
  $("#queue-next").disabled = !state.nextCursor;
}
let queueEpoch = 0;
async function refreshQueue(reset = false) {
  if (reset) {
    state.cursor = null;
    state.previousCursors = [];
  }
  const epoch = ++queueEpoch;
  try {
    let page;
    if (state.sample) {
      const params = queueQuery();
      const filtered = (state.sampleAlerts || []).filter((a) =>
        ["status", "owner", "severity"].every(
          (field) => !params.get(field) || a[field] === params.get(field),
        ),
      );
      const offset = Number(state.cursor) || 0;
      const limit = Number(params.get("limit"));
      page = {
        items: filtered.slice(offset, offset + limit),
        nextCursor:
          offset + limit < filtered.length ? String(offset + limit) : null,
      };
    } else page = await request(`/alerts?${queueQuery()}`);
    if (epoch !== queueEpoch) return;
    state.alerts = page.items || [];
    state.nextCursor = page.nextCursor || null;
    renderQueue();
  } catch (error) {
    if (epoch !== queueEpoch) return;
    toast(`Queue could not be refreshed: ${error.message}`);
  }
}

let refreshEpoch = 0;
async function refresh() {
  const epoch = ++refreshEpoch;
  if (hostedSnapshot) {
    const readOnly = !state.sample;
    [
      "#save-review",
      "#review-action",
      "#review-disposition",
      "#review-note",
      "#review-analyst",
      "#simulate-button",
    ].forEach((selector) => {
      $(selector).disabled = readOnly;
    });
    $("#settings-button").hidden = true;
    $('#mobile-view option[value="settings"]').hidden = true;
    $("#simulate-button").title = readOnly
      ? "Run the full stack to submit transactions."
      : "Explore the sample workspace";
    $(".workspace small").textContent = state.sample
      ? "Sample workspace"
      : "Verified CI snapshot";
    $("#alerts-view .panel-heading p").textContent = readOnly
      ? "Inspect recorded signals, original evidence and review history."
      : "Try review decisions on illustrative records in this tab.";
    $("#transactions-view .panel-heading p").textContent = readOnly
      ? "Up to 200 immutable transaction records captured from the verified run."
      : "Illustrative transaction records for exploring the workspace.";
    $("#review-access-hint").textContent = readOnly
      ? "Captured decisions are read-only. Turn on Sample data to try a review locally."
      : "A note and operator label are required. Sample reviews stay in this tab and reset on page reload.";
    $("#scenario-dialog h2").textContent = "Explore pipeline scenarios.";
    $("#scenario-dialog > p").textContent =
      "These scenarios are available in the full application. This hosted preview displays captured results and does not submit transactions.";
    $("#send-scenario").textContent = "How to run this scenario →";
  }
  if (state.sample) {
    if (!state.sampleAlerts) Object.assign(state, sampleData());
    state.outcomes = outcomesFor(state.sampleAlerts);
    await refreshQueue();
    $("#connection").className = "connection sample";
    $("#connection").innerHTML = "<i></i> Sample workspace";
    $("#notice").textContent =
      "SAMPLE DATA · This is an interactive preview using synthetic fixtures. " +
      (hostedSnapshot
        ? "Reviews stay in this tab. Switch off Sample data to return to the verified snapshot."
        : "Switch off Sample data to connect to your running API.") +
      " These values are not benchmark results.";
    $("#notice").hidden = false;
    $("#updated-at").textContent = "Illustrative dataset · not live traffic";
    render();
    return;
  }
  try {
    const [overview, recent, transactions, windows, outcomes] =
      await Promise.all([
        request("/overview"),
        request("/alerts?limit=5"),
        request(`/transactions?limit=${hostedSnapshot ? 200 : 100}`),
        request(`/windows?limit=${hostedSnapshot ? 200 : 100}`),
        request("/outcomes").catch(() => ({ unavailable: true })),
      ]);
    if (epoch !== refreshEpoch) return;
    Object.assign(state, {
      overview,
      recentAlerts: recent.items || [],
      outcomes,
      transactions: transactions.items || [],
      windows: windows.items || [],
    });
    await refreshQueue();
    if (epoch !== refreshEpoch) return;
    if (hostedSnapshot) {
      const snapshot = await loadSnapshot();
      if (epoch !== refreshEpoch) return;
      $("#connection").className = "connection sample";
      $("#connection").innerHTML = "<i></i> Verified run";
      $("#notice").innerHTML =
        `READ-ONLY SNAPSHOT · Captured ${escapeHtml(snapshot.capturedAt)} from a real Java → Kafka → Spark → MongoDB verification run using synthetic test events. This page does not run the backend. <a href="${escapeHtml(snapshot.runUrl)}" target="_blank" rel="noreferrer">View source run ↗</a> · Turn on Sample data to try local review actions.`;
      $("#notice").hidden = false;
      $("#updated-at").textContent =
        `Captured ${snapshot.capturedAt} · up to 200 records per list`;
    } else {
      $("#connection").className = "connection live";
      $("#connection").innerHTML = "<i></i> API connected";
      $("#notice").hidden = true;
      $("#updated-at").textContent =
        `Updated ${time(Date.now())} · refreshes every 5s`;
    }
    render();
  } catch (error) {
    if (epoch !== refreshEpoch) return;
    $("#connection").className = "connection";
    $("#connection").innerHTML = hostedSnapshot
      ? "<i></i> Snapshot unavailable"
      : "<i></i> API unavailable";
    $("#notice").textContent = hostedSnapshot
      ? `Unable to load the verified snapshot. ${error.message} Try Refresh, or turn on Sample data to explore illustrative records.`
      : `Unable to refresh live data. ${error.message} Start the Compose stack, or turn on Sample data to explore the workspace. Previous values, if any, are stale.`;
    $("#notice").hidden = false;
  }
}

function updateReviewControls() {
  const detail = state.detail;
  const readOnly = hostedSnapshot && !state.sample;
  const analyst = $("#review-analyst").value.trim();
  const owned = detail?.owner && detail.owner === analyst;
  const status = detail?.status || "OPEN";
  const allowed =
    ["OPEN", "INVESTIGATING"].includes(status) && !detail?.owner
      ? ["CLAIM"]
      : status === "RESOLVED"
        ? ["REOPEN"]
        : owned
          ? ["COMMENT", "RELEASE", "RESOLVE"]
          : [];
  const action = $("#review-action");
  for (const option of action.options)
    option.disabled = !allowed.includes(option.value);
  if (!allowed.includes(action.value)) action.value = allowed[0] || "";
  action.disabled = readOnly || !detail || state.saving || !allowed.length;
  $("#disposition-field").hidden = action.value !== "RESOLVE";
  for (const selector of [
    "#review-note",
    "#review-analyst",
    "#review-disposition",
  ])
    $(selector).disabled = readOnly || state.saving;
  $("#save-review").disabled =
    readOnly || !detail || state.saving || !allowed.includes(action.value);
  $("#save-review").textContent = state.saving
    ? "Saving…"
    : "Save action & note";
  $("#review-status").value = status;
  $("#review-ownership").textContent = detail
    ? `Owner: ${detail.owner || "Unassigned"} · Version ${detail.version || 0}${detail.disposition ? ` · Outcome: ${pretty(detail.disposition)}` : ""}. ${readOnly ? "Captured record; actions are read-only." : status === "INVESTIGATING" && !owned ? "Only the current owner can add notes, release, or resolve this investigation." : "Every action requires a note and is checked against the current version."}`
    : "Loading the current ownership and version…";
}

function renderDetail(detail) {
  state.detail = detail;
  $("#detail-title").textContent = pretty(detail.rule);
  const fields = [
    ["Account", detail.accountId],
    ["Severity / score", `${detail.severity} / ${detail.score}`],
    ["Currency", detail.currency],
    [
      "Event time",
      new Date(detail.eventTime || detail.createdAt).toLocaleString(),
    ],
    ["Transaction", detail.transactionId || "Window-level signal"],
    ["Alert ID", idOf(detail)],
  ];
  $("#detail-content").innerHTML =
    `<div class="detail-grid">${fields.map(([label, value]) => `<div><small>${label}</small><span>${escapeHtml(value)}</span></div>`).join("")}</div><h3>Why it was flagged</h3><ul class="detail-reasons">${(detail.reasons || []).map((reason) => `<li>${escapeHtml(reason)}</li>`).join("")}</ul>`;
  $("#detail-history").innerHTML = detail.reviewHistory?.length
    ? `<ol class="review-history">${[...detail.reviewHistory]
        .reverse()
        .map(
          (entry) =>
            `<li><div><strong>${escapeHtml(pretty(entry.action || entry.status))}${entry.disposition ? ` · ${escapeHtml(pretty(entry.disposition))}` : ""}</strong><span>${escapeHtml(entry.analyst)} · ${escapeHtml(time(entry.reviewedAt))}</span></div><p>${escapeHtml(entry.note)}</p></li>`,
        )
        .join("")}</ol>`
    : '<p class="dialog-hint">No review recorded yet.</p>';
  updateReviewControls();
}

function renderEvidence(evidence) {
  const pinned = evidence.provenance === "PINNED_DETECTION";
  const warnings = [];
  if (!pinned)
    warnings.push(
      "Legacy contextual evidence: these records were looked up after detection. They are not a pinned list of events used by the rule.",
    );
  if (evidence.truncated)
    warnings.push(
      "Evidence is truncated: this is only part of the detection evidence. Do not infer completeness from this list.",
    );
  if (evidence.missingCount)
    warnings.push(
      `${number(evidence.missingCount)} pinned transaction(s) are missing from the API ledger.`,
    );
  if (
    pinned &&
    !evidence.complete &&
    !evidence.truncated &&
    !evidence.missingCount
  )
    warnings.push("Evidence completeness could not be established.");
  const summary = pinned
    ? `Pinned detection evidence · ${number(evidence.items?.length)} shown / ${number(evidence.evidenceCount)} detected · ${number(evidence.matchedCount)} available in ledger${evidence.complete ? " · Complete" : " · Incomplete"}`
    : "Contextual transaction records · Exact detection membership unavailable";
  $("#detail-evidence").innerHTML =
    `<p class="dialog-hint">${escapeHtml(summary)}</p>${warnings.map((warning) => `<p class="evidence-warning">${escapeHtml(warning)}</p>`).join("")}` +
    (evidence.items?.length
      ? `<div class="evidence-list">${evidence.items.map((t) => `<div><span class="mono">${escapeHtml(t.transactionId)}</span><strong>${escapeHtml(money(t.amountMinor, t.currency))} ${escapeHtml(t.currency)}</strong><small>${escapeHtml(time(t.eventTime))} · ${escapeHtml(t.merchantId)}</small></div>`).join("")}</div>`
      : '<p class="dialog-hint">No API ledger transactions are available for this signal. Events sent directly to Kafka may have no API ledger entry.</p>');
}

let detailEpoch = 0;
async function openAlert(id) {
  const alert = [...state.alerts, ...state.recentAlerts].find(
    (item) => idOf(item) === id,
  );
  if (!alert) return;
  const epoch = ++detailEpoch;
  state.selected = id;
  state.detail = null;
  state.pendingOperation = null;
  $("#review-analyst").value =
    sessionStorage.getItem("pulseguard-operator") || "";
  $("#review-note").value = "";
  $("#review-disposition").value = "";
  $("#review-conflict").hidden = true;
  $("#detail-title").textContent = pretty(alert.rule);
  $("#detail-content").textContent = "Loading current signal…";
  $("#detail-evidence").textContent = "Loading detection evidence…";
  $("#detail-history").textContent = "Loading review history…";
  updateReviewControls();
  $("#detail-dialog").showModal();
  try {
    const [detail, evidence] = state.sample
      ? [
          alert,
          {
            items: state.transactions.filter((t) =>
              alert.evidenceTransactionIds.includes(t.transactionId),
            ),
            provenance: "PINNED_DETECTION",
            evidenceCount: alert.evidenceTransactionIds.length,
            matchedCount: alert.evidenceTransactionIds.length,
            missingCount: 0,
            truncated: false,
            complete: true,
          },
        ]
      : await Promise.all([
          request(`/alerts/${encodeURIComponent(id)}`),
          request(`/alerts/${encodeURIComponent(id)}/evidence`),
        ]);
    if (epoch !== detailEpoch) return;
    renderDetail(detail);
    renderEvidence(evidence);
  } catch (error) {
    if (epoch !== detailEpoch) return;
    $("#detail-evidence").textContent =
      `Details could not be loaded: ${error.message}`;
    $("#detail-history").textContent =
      "Actions remain disabled until the current record loads.";
  }
}

function sampleReview(review) {
  const alert = state.sampleAlerts.find((a) => idOf(a) === state.selected);
  const status = {
    CLAIM: "INVESTIGATING",
    COMMENT: "INVESTIGATING",
    RELEASE: "OPEN",
    RESOLVE: "RESOLVED",
    REOPEN: "INVESTIGATING",
  }[review.action];
  const at = new Date().toISOString();
  Object.assign(alert, {
    status,
    version: (alert.version || 0) + 1,
    owner: review.action === "RELEASE" ? null : review.analyst,
    disposition: review.action === "RESOLVE" ? review.disposition : null,
    resolvedAt: review.action === "RESOLVE" ? at : null,
  });
  (alert.reviewHistory ||= []).push({ ...review, status, reviewedAt: at });
  state.overview.highRisk = state.sampleAlerts.filter(
    (a) => a.severity === "HIGH" && a.status !== "RESOLVED",
  ).length;
  state.outcomes = outcomesFor(state.sampleAlerts);
  return alert;
}

async function saveReview() {
  if ((hostedSnapshot && !state.sample) || state.saving || !state.detail)
    return;
  const action = $("#review-action").value;
  if (!action || $("#review-action").selectedOptions[0]?.disabled) return;
  const note = $("#review-note").value.trim();
  const analyst = $("#review-analyst").value.trim();
  const disposition = $("#review-disposition").value;
  if (note.length < 3 || analyst.length < 2) {
    toast("Add an operator label and a decision note before saving.");
    return;
  }
  if (action === "RESOLVE" && !disposition) {
    toast("Choose a resolution outcome before resolving.");
    return;
  }
  const command = {
    action,
    note,
    analyst,
    expectedVersion: state.detail.version || 0,
    ...(action === "RESOLVE" ? { disposition } : {}),
  };
  const fingerprint = JSON.stringify(command);
  if (state.pendingOperation?.fingerprint !== fingerprint)
    state.pendingOperation = { fingerprint, operationId: crypto.randomUUID() };
  const review = {
    ...command,
    operationId: state.pendingOperation.operationId,
  };
  const id = state.selected;
  const epoch = detailEpoch;
  state.saving = true;
  updateReviewControls();
  try {
    const updated = state.sample
      ? sampleReview(review)
      : await request(`/alerts/${encodeURIComponent(id)}/review`, {
          method: "PATCH",
          body: JSON.stringify(review),
        });
    if (epoch !== detailEpoch) return;
    sessionStorage.setItem("pulseguard-operator", analyst);
    state.pendingOperation = null;
    $("#review-note").value = "";
    $("#review-disposition").value = "";
    $("#review-conflict").hidden = true;
    renderDetail(updated);
    toast(
      state.sample
        ? "Sample action saved in this tab; resets on page reload."
        : "Action saved with a versioned audit record.",
    );
    await refresh();
  } catch (error) {
    if (epoch !== detailEpoch) return;
    if (error.status === 409) {
      state.pendingOperation = null;
      $("#review-conflict").textContent =
        "This signal changed or this action is no longer allowed. Your note is preserved. Check the latest owner and version before submitting again.";
      $("#review-conflict").hidden = false;
      // Preserve the analyst's draft while refreshing the record used for the next command.
      state.detail = null;
      try {
        const latest = await request(`/alerts/${encodeURIComponent(id)}`);
        if (epoch === detailEpoch) renderDetail(latest);
      } catch (refreshError) {
        $("#review-conflict").textContent +=
          ` Latest record could not load: ${refreshError.message}. Reopen the signal before retrying.`;
      }
    } else
      toast(
        `${error.message} Your note is preserved; retrying the same action reuses its operation ID.`,
      );
  } finally {
    state.saving = false;
    if (epoch === detailEpoch) updateReviewControls();
  }
}

async function runScenario() {
  if (hostedSnapshot) {
    $("#scenario-result").textContent =
      "This sample preview cannot submit transactions. Run the full stack locally to connect to the API and execute a scenario.";
    return;
  }
  if (state.sample) {
    $("#scenario-result").textContent =
      "Switch off Sample data and connect to your running API to send transactions.";
    return;
  }
  const mode = $("#scenario-select").value;
  const count = { mixed: 24, "high-value": 1, velocity: 8, "card-testing": 6 }[
    mode
  ];
  const batch = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
  const timestamp = new Date().toISOString();
  $("#send-scenario").disabled = true;
  let accepted = 0;
  try {
    for (let i = 0; i < count; i++) {
      const small = mode === "card-testing" || (mode === "mixed" && i < 6);
      const high = mode === "high-value" || (mode === "mixed" && i >= 20);
      const group =
        mode === "mixed"
          ? i < 6
            ? "testing"
            : i < 14
              ? "velocity"
              : `normal_${i}`
          : mode.replaceAll("-", "_");
      await request("/transactions", {
        method: "POST",
        body: JSON.stringify({
          schemaVersion: 1,
          transactionId: `txn_${batch}_${i}`,
          accountId: `acc_${batch}_${group}`,
          merchantId: `merchant_${(i % 4) + 1}`,
          amountMinor: small ? 500 : high ? 750000 : 12500 + i * 170,
          currency: "USD",
          country: "US",
          channel: "WEB",
          eventTime: timestamp,
        }),
      });
      accepted++;
      $("#scenario-result").textContent = `${accepted} / ${count} accepted…`;
    }
    $("#scenario-result").textContent =
      `${accepted} transactions accepted. Spark signals appear after the next micro-batches; allow up to a minute on a cold start.`;
    toast("Scenario accepted. Watch your alert inbox.");
    await refresh();
  } catch (error) {
    $("#scenario-result").textContent =
      `${accepted} accepted before the request stopped. ${error.message}`;
  } finally {
    $("#send-scenario").disabled = false;
  }
}

document.addEventListener("click", (event) => {
  const navigation = event.target.closest("[data-view]");
  if (navigation) switchView(navigation.dataset.view);
  const goto = event.target.closest("[data-goto]");
  if (goto) switchView(goto.dataset.goto);
  const row = event.target.closest("[data-alert-id]");
  if (row) openAlert(row.dataset.alertId);
});
document.addEventListener("keydown", (event) => {
  const row = event.target.closest("[data-alert-id]");
  if (row && (event.key === "Enter" || event.key === " ")) {
    event.preventDefault();
    openAlert(row.dataset.alertId);
  }
});
$("#sample-toggle").checked = state.sample;
$("#sample-toggle").addEventListener("change", () => {
  ++detailEpoch;
  ++queueEpoch;
  state.selected = null;
  state.detail = null;
  state.pendingOperation = null;
  $("#detail-dialog").close();
  state.sample = $("#sample-toggle").checked;
  Object.assign(state, {
    overview: {},
    alerts: [],
    recentAlerts: [],
    sampleAlerts: null,
    outcomes: null,
    cursor: null,
    nextCursor: null,
    previousCursors: [],
    windows: [],
    transactions: [],
  });
  render();
  refresh();
});
$("#severity-filter").addEventListener("change", () => refreshQueue(true));
$("#status-filter").addEventListener("change", () => refreshQueue(true));
$("#apply-filters").addEventListener("click", () => refreshQueue(true));
$("#owner-filter").addEventListener("keydown", (event) => {
  if (event.key === "Enter") refreshQueue(true);
});
$("#queue-refresh").addEventListener("click", () => refreshQueue(true));
$("#queue-next").addEventListener("click", () => {
  if (!state.nextCursor) return;
  state.previousCursors.push(state.cursor);
  state.cursor = state.nextCursor;
  refreshQueue();
});
$("#queue-previous").addEventListener("click", () => {
  if (!state.previousCursors.length) return;
  state.cursor = state.previousCursors.pop();
  refreshQueue();
});
$("#review-analyst").addEventListener("input", updateReviewControls);
$("#review-action").addEventListener("change", updateReviewControls);
$("#detail-dialog").addEventListener("close", () => {
  ++detailEpoch;
  state.selected = null;
  state.detail = null;
});
$("#settings-button").addEventListener("click", () => {
  $("#api-key").value = sessionStorage.getItem(keyName) || "";
  $("#settings-dialog").showModal();
});
$("#mobile-view").addEventListener("change", () => {
  const view = $("#mobile-view").value;
  if (view === "settings") {
    if (!hostedSnapshot) $("#settings-button").click();
    $("#mobile-view").value = $(".view.active").id.replace("-view", "");
  } else switchView(view);
});
$("#save-settings").addEventListener("click", () => {
  sessionStorage.setItem(keyName, $("#api-key").value.trim());
  $("#settings-dialog").close();
  toast("Connection settings saved for this tab.");
});
$("#simulate-button").addEventListener("click", () => {
  $("#scenario-result").textContent = "";
  $("#scenario-dialog").showModal();
});
$("#send-scenario").addEventListener("click", runScenario);
$("#save-review").addEventListener("click", saveReview);
$("#refresh-button").addEventListener("click", refresh);
render();
refresh();
setInterval(() => {
  if (!hostedSnapshot && !state.sample && !document.hidden) refresh();
}, 5000);
