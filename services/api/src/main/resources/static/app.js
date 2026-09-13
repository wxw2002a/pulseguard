"use strict";
const $ = (selector) => document.querySelector(selector);
const state = {
  sample: new URLSearchParams(location.search).get("demo") === "1",
  overview: {},
  alerts: [],
  transactions: [],
  windows: [],
  selected: null,
  loading: false,
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
}

async function request(path, options = {}) {
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
  if (!response.ok)
    throw new Error(
      response.status === 401
        ? "Set your API key in Connection settings first."
        : body.detail || body.message || `API returned HTTP ${response.status}`,
    );
  return body;
}

function sampleData() {
  const now = Date.now();
  const minute = Math.floor(now / 60000) * 60000;
  const rules = [
    "HIGH_VALUE",
    "VELOCITY",
    "CARD_TESTING",
    "HIGH_VALUE",
    "VELOCITY",
    "HIGH_VALUE",
  ];
  const accounts = [
    "acc_8f21",
    "acc_4d09",
    "acc_7a32",
    "acc_2c85",
    "acc_9b16",
    "acc_5e43",
  ];
  const alerts = rules.map((rule, i) => ({
    id: `sample_${i}`,
    transactionId: `txn_sample_${1000 + i}`,
    accountId: accounts[i],
    rule,
    severity: rule === "VELOCITY" ? "MEDIUM" : "HIGH",
    score: rule === "VELOCITY" ? 65 : rule === "CARD_TESTING" ? 90 : 80,
    currency: "USD",
    amountMinor: [894200, null, null, 1267000, null, 648000][i],
    eventTime: new Date(now - i * 42000).toISOString(),
    createdAt: new Date(now - i * 42000).toISOString(),
    status: ["OPEN", "INVESTIGATING", "OPEN", "RESOLVED", "OPEN", "OPEN"][i],
    reasons: [
      rule === "HIGH_VALUE"
        ? "Transaction amount exceeds the illustrative 500,000 minor-unit threshold."
        : rule === "VELOCITY"
          ? "At least five distinct transactions occurred in a one-minute account window."
          : "At least five transactions of 1,000 minor units or less occurred in one minute.",
    ],
  }));
  const windows = [
    138, 152, 117, 176, 158, 196, 182, 163, 217, 188, 232, 204,
  ].map((count, i) => ({
    id: `window_${i}`,
    accountId: accounts[i % 6],
    currency: "USD",
    windowStart: new Date(minute - (11 - i) * 60000).toISOString(),
    windowEnd: new Date(minute - (10 - i) * 60000).toISOString(),
    transactionCount: count,
    totalAmountMinor: count * 23700,
    highValueCount: [7, 12, 5, 15, 13, 23, 18, 14, 25, 17, 28, 19][i],
  }));
  const transactions = Array.from({ length: 20 }, (_, i) => ({
    transactionId: `txn_sample_${1020 - i}`,
    accountId: accounts[i % 6],
    merchantId: `merchant_${(i % 4) + 1}`,
    amountMinor: 12740 + ((i * 7919) % 900000),
    currency: "USD",
    country: "US",
    channel: ["WEB", "MOBILE", "POS"][i % 3],
    eventTime: new Date(now - i * 19000).toISOString(),
    deliveryStatus: "SENT",
  }));
  return {
    overview: {
      transactions: 24862,
      alerts: 142,
      highRisk: 38,
      volumeByCurrency: { USD: 183294720, CAD: 2987300 },
      pendingDelivery: 0,
      reviewedAlerts: 104,
    },
    alerts,
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
  return `<div class="table-wrap"><table><thead><tr><th>Account / signal</th><th>Risk level</th><th>Rule</th><th>Score</th><th>Event time</th><th>Status</th><th></th></tr></thead><tbody>${alerts.map((alert) => `<tr class="clickable" tabindex="0" data-alert-id="${escapeHtml(idOf(alert))}"><td><div class="account"><span class="account-icon">↗</span><span>${escapeHtml(alert.accountId)}<span class="cell-sub">${alert.amountMinor ? escapeHtml(money(alert.amountMinor, alert.currency)) : "Account activity"} · ${escapeHtml(alert.currency)}</span></span></div></td><td><span class="badge ${alert.severity === "HIGH" ? "high" : "medium"}">${escapeHtml(pretty(alert.severity))}</span></td><td>${escapeHtml(pretty(alert.rule))}</td><td>${escapeHtml(alert.score)}<span class="score-track"><i style="width:${Math.max(0, Math.min(100, Number(alert.score) || 0))}%"></i></span></td><td>${escapeHtml(time(alert.eventTime || alert.createdAt))}</td><td><span class="badge ${["OPEN", "INVESTIGATING", "RESOLVED"].includes(alert.status) ? alert.status.toLowerCase() : "open"}">${escapeHtml(pretty(alert.status || "OPEN"))}</span></td><td class="row-arrow">↗</td></tr>`).join("")}</tbody></table></div>`;
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
  $("#overview-alerts").innerHTML = alertTable(state.alerts.slice(0, 5));
  $("#all-alerts").innerHTML = alertTable(
    state.alerts.filter(
      (a) =>
        !$("#severity-filter").value ||
        a.severity === $("#severity-filter").value,
    ),
  );
  $("#transactions-table").innerHTML = state.transactions.length
    ? `<div class="table-wrap"><table><thead><tr><th>Transaction</th><th>Account</th><th>Merchant</th><th>Amount</th><th>Channel</th><th>Event time</th><th>Delivery</th></tr></thead><tbody>${state.transactions.map((t) => `<tr><td class="mono">${escapeHtml(t.transactionId)}</td><td>${escapeHtml(t.accountId)}</td><td>${escapeHtml(t.merchantId)}</td><td>${escapeHtml(money(t.amountMinor, t.currency))} ${escapeHtml(t.currency)}</td><td>${escapeHtml(t.channel)}</td><td>${escapeHtml(time(t.eventTime))}</td><td><span class="badge ${t.deliveryStatus === "SENT" ? "resolved" : "open"}">${escapeHtml(t.deliveryStatus || "PENDING")}</span></td></tr>`).join("")}</tbody></table></div>`
    : empty("Your ledger is ready");
  $("#windows-table").innerHTML = state.windows.length
    ? `<div class="table-wrap"><table><thead><tr><th>Account</th><th>Currency</th><th>Window start</th><th>Window end</th><th>Unique events</th><th>Volume</th><th>High value</th></tr></thead><tbody>${state.windows.map((w) => `<tr><td>${escapeHtml(w.accountId)}</td><td>${escapeHtml(w.currency)}</td><td>${escapeHtml(time(w.windowStart))}</td><td>${escapeHtml(time(w.windowEnd))}</td><td>${number(w.transactionCount)}</td><td>${escapeHtml(money(w.totalAmountMinor, w.currency))}</td><td>${number(w.highValueCount)}</td></tr>`).join("")}</tbody></table></div>`
    : empty("No account windows yet");
  renderChart();
}

let refreshEpoch = 0;
async function refresh() {
  const epoch = ++refreshEpoch;
  if (state.sample) {
    Object.assign(state, sampleData());
    $("#connection").className = "connection sample";
    $("#connection").innerHTML = "<i></i> Sample workspace";
    $("#notice").textContent =
      "SAMPLE DATA · This is an interactive preview using synthetic fixtures. Switch off Sample data to connect to your running API. These values are not benchmark results.";
    $("#notice").hidden = false;
    $("#updated-at").textContent = "Illustrative dataset · not live traffic";
    render();
    return;
  }
  try {
    const [overview, alerts, transactions, windows] = await Promise.all([
      request("/overview"),
      request("/alerts?limit=100"),
      request("/transactions?limit=100"),
      request("/windows?limit=100"),
    ]);
    if (epoch !== refreshEpoch) return;
    Object.assign(state, {
      overview,
      alerts: alerts.items || [],
      transactions: transactions.items || [],
      windows: windows.items || [],
    });
    $("#connection").className = "connection live";
    $("#connection").innerHTML = "<i></i> API connected";
    $("#notice").hidden = true;
    $("#updated-at").textContent =
      `Updated ${time(Date.now())} · refreshes every 5s`;
    render();
  } catch (error) {
    if (epoch !== refreshEpoch) return;
    $("#connection").className = "connection";
    $("#connection").innerHTML = "<i></i> API unavailable";
    $("#notice").textContent =
      `Unable to refresh live data. ${error.message} Start the Compose stack, or turn on Sample data to explore the workspace. Previous values, if any, are stale.`;
    $("#notice").hidden = false;
  }
}

async function openAlert(id) {
  const alert = state.alerts.find((item) => idOf(item) === id);
  if (!alert) return;
  state.selected = id;
  $("#detail-title").textContent = pretty(alert.rule);
  const fields = [
    ["Account", alert.accountId],
    ["Severity / score", `${alert.severity} / ${alert.score}`],
    ["Currency", alert.currency],
    [
      "Event time",
      new Date(alert.eventTime || alert.createdAt).toLocaleString(),
    ],
    ["Transaction", alert.transactionId || "Window-level signal"],
    ["Alert ID", idOf(alert)],
  ];
  $("#detail-content").innerHTML =
    `<div class="detail-grid">${fields.map(([label, value]) => `<div><small>${label}</small><span>${escapeHtml(value)}</span></div>`).join("")}</div><h3>Why it was flagged</h3><ul class="detail-reasons">${(alert.reasons || []).map((reason) => `<li>${escapeHtml(reason)}</li>`).join("")}</ul>`;
  $("#review-status").value = alert.status || "OPEN";
  $("#review-analyst").value =
    sessionStorage.getItem("pulseguard-operator") || "";
  $("#review-note").value = "";
  $("#detail-evidence").textContent = "Loading related transactions…";
  $("#detail-history").textContent = "Loading review history…";
  $("#detail-dialog").showModal();
  try {
    const [detail, evidence] = state.sample
      ? [
          { ...alert, reviewHistory: alert.reviewHistory || [] },
          {
            items: state.transactions
              .filter((t) => t.accountId === alert.accountId)
              .slice(0, 4),
          },
        ]
      : await Promise.all([
          request(`/alerts/${encodeURIComponent(id)}`),
          request(`/alerts/${encodeURIComponent(id)}/evidence`),
        ]);
    if (state.selected !== id) return;
    $("#detail-evidence").innerHTML = evidence.items?.length
      ? `<div class="evidence-list">${evidence.items.map((t) => `<div><span class="mono">${escapeHtml(t.transactionId)}</span><strong>${escapeHtml(money(t.amountMinor, t.currency))} ${escapeHtml(t.currency)}</strong><small>${escapeHtml(time(t.eventTime))} · ${escapeHtml(t.merchantId)}</small></div>`).join("")}</div><p class="dialog-hint">${state.sample ? "Illustrative account records." : "Up to 200 original transactions matching this signal; earliest event time first."}</p>`
      : '<p class="dialog-hint">No ingested transaction evidence found. Records published directly to Kafka may not have an API ledger entry.</p>';
    $("#detail-history").innerHTML = detail.reviewHistory?.length
      ? `<ol class="review-history">${[...detail.reviewHistory]
          .reverse()
          .map(
            (entry) =>
              `<li><div><strong>${escapeHtml(pretty(entry.status))}</strong><span>${escapeHtml(entry.analyst)} · ${escapeHtml(time(entry.reviewedAt))}</span></div><p>${escapeHtml(entry.note)}</p></li>`,
          )
          .join("")}</ol>`
      : '<p class="dialog-hint">No review recorded yet.</p>';
  } catch (error) {
    if (state.selected !== id) return;
    $("#detail-evidence").textContent =
      `Evidence could not be loaded: ${error.message}`;
    $("#detail-history").textContent =
      "History unavailable until the API responds.";
  }
}

async function saveReview() {
  const status = $("#review-status").value;
  const note = $("#review-note").value.trim();
  const analyst = $("#review-analyst").value.trim();
  if (note.length < 3 || analyst.length < 2) {
    toast("Add an operator label and a decision note before saving.");
    return;
  }
  const review = { status, note, analyst };
  try {
    if (!state.sample)
      await request(`/alerts/${encodeURIComponent(state.selected)}/review`, {
        method: "PATCH",
        body: JSON.stringify(review),
      });
    sessionStorage.setItem("pulseguard-operator", analyst);
    const alert = state.alerts.find((item) => idOf(item) === state.selected);
    if (alert) {
      alert.status = status;
      if (state.sample)
        (alert.reviewHistory ||= []).push({
          ...review,
          reviewedAt: new Date().toISOString(),
        });
    }
    $("#detail-dialog").close();
    render();
    toast(
      state.sample
        ? "Sample review updated locally; it resets on refresh."
        : "Review saved. Replay will preserve this decision.",
    );
    if (!state.sample) await refresh();
  } catch (error) {
    toast(error.message);
  }
}

async function runScenario() {
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
  state.sample = $("#sample-toggle").checked;
  Object.assign(state, {
    overview: {},
    alerts: [],
    windows: [],
    transactions: [],
  });
  render();
  refresh();
});
$("#severity-filter").addEventListener("change", render);
$("#settings-button").addEventListener("click", () => {
  $("#api-key").value = sessionStorage.getItem(keyName) || "";
  $("#settings-dialog").showModal();
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
  if (!state.sample && !document.hidden) refresh();
}, 5000);
