const state = {
  runs: [],
  metrics: null,
  selectedRunId: null,
  selectedRun: null,
  selectedEvents: [],
};

const nodes = {
  validateOrder: { x: 45, y: 155, label: "Validate order" },
  fraudScreen: { x: 220, y: 55, label: "Fraud screen" },
  reserveInventory: { x: 220, y: 255, label: "Reserve inventory" },
  manualReview: { x: 390, y: 55, label: "Manual review" },
  chargePayment: { x: 540, y: 155, label: "Charge payment" },
  createShipment: { x: 720, y: 105, label: "Create shipment" },
  sendConfirmation: { x: 720, y: 265, label: "Send confirmation" },
};

const edges = [
  ["validateOrder", "fraudScreen"],
  ["validateOrder", "reserveInventory"],
  ["fraudScreen", "manualReview"],
  ["manualReview", "chargePayment"],
  ["reserveInventory", "chargePayment"],
  ["chargePayment", "createShipment"],
  ["createShipment", "sendConfirmation"],
];

const runList = document.querySelector("#run-list");
const eventList = document.querySelector("#event-list");
const effectLog = document.querySelector("#effect-log");
const inventory = document.querySelector("#inventory");
const dag = document.querySelector("#dag");
const selectedTitle = document.querySelector("#selected-title");
const selectedSubtitle = document.querySelector("#selected-subtitle");
const cancelButton = document.querySelector("#cancel-run");
const approveButton = document.querySelector("#approve-review");
const rejectButton = document.querySelector("#reject-review");
const health = document.querySelector("#health");

document.querySelector("#order-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  await api("/api/orders", {
    method: "POST",
    body: JSON.stringify({
      customerId: form.get("customerId"),
      sku: form.get("sku"),
      quantity: Number(form.get("quantity")),
      amount: Number(form.get("amount")),
      chaos: form.get("chaos"),
    }),
  });
  await refresh(true);
});

document.querySelectorAll("[data-scenario]").forEach((button) => {
  button.addEventListener("click", async () => {
    document.querySelector("[name=chaos]").value = button.dataset.scenario;
    document.querySelector("#order-form").requestSubmit();
  });
});

document.querySelector("[data-review-order]").addEventListener("click", () => {
  document.querySelector("[name=chaos]").value = "none";
  document.querySelector("[name=amount]").value = "1499";
  document.querySelector("#order-form").requestSubmit();
});

document.querySelector("#refresh").addEventListener("click", () => refresh(true));
cancelButton.addEventListener("click", async () => {
  if (!state.selectedRunId) return;
  await api(`/api/runs/${encodeURIComponent(state.selectedRunId)}/cancel`, {
    method: "POST",
    body: JSON.stringify({ reason: "Cancelled from the operations console" }),
  });
  await refresh(true);
});

approveButton.addEventListener("click", () => sendReviewDecision(true));
rejectButton.addEventListener("click", () => sendReviewDecision(false));

async function sendReviewDecision(approved) {
  const waiting = waitingSignalStep(state.selectedRun);
  if (!state.selectedRunId || !waiting) return;
  await api(`/api/runs/${encodeURIComponent(state.selectedRunId)}/signal`, {
    method: "POST",
    body: JSON.stringify({
      signal: waiting.waitForSignal,
      payload: { approved, reviewer: "operations-console" },
    }),
  });
  await refresh(true);
}

async function refresh(forceSelectLatest = false) {
  try {
    const [runsResponse, metricsResponse] = await Promise.all([
      api("/api/runs"),
      api("/api/metrics"),
    ]);
    state.runs = runsResponse.runs;
    state.metrics = metricsResponse;
    setHealth(true);

    if (forceSelectLatest || !state.selectedRunId || !state.runs.some((run) => run.runId === state.selectedRunId)) {
      state.selectedRunId = state.runs[0]?.runId ?? null;
    }

    if (state.selectedRunId) {
      const detail = await api(`/api/runs/${encodeURIComponent(state.selectedRunId)}`);
      state.selectedRun = detail.run;
      state.selectedEvents = detail.events;
    } else {
      state.selectedRun = null;
      state.selectedEvents = [];
    }

    render();
  } catch (error) {
    console.error(error);
    setHealth(false);
  }
}

function render() {
  renderMetrics();
  renderRuns();
  renderSelected();
  renderDag();
  renderEvents();
  renderInventory();
  renderEffects();
}

function renderMetrics() {
  const metrics = state.metrics;
  document.querySelector("#metric-total").textContent = metrics?.totalRuns ?? 0;
  document.querySelector("#metric-succeeded").textContent = metrics?.byStatus?.succeeded ?? 0;
  document.querySelector("#metric-compensated").textContent = metrics?.byStatus?.compensated ?? 0;
  document.querySelector("#metric-events").textContent = metrics?.totalEvents ?? 0;
}

function renderRuns() {
  runList.innerHTML = "";
  runList.classList.toggle("empty", state.runs.length === 0);
  if (state.runs.length === 0) {
    runList.textContent = "No workflow runs yet";
    return;
  }

  for (const run of state.runs) {
    const card = document.createElement("button");
    card.className = `run-card ${run.runId === state.selectedRunId ? "active" : ""}`;
    card.type = "button";

    const row = document.createElement("div");
    row.className = "row";
    const workflow = document.createElement("strong");
    workflow.textContent = `${run.workflowName}@${run.version}`;
    const badge = document.createElement("span");
    badge.className = `badge status-${run.status}`;
    badge.textContent = run.status;
    row.append(workflow, badge);

    const id = document.createElement("div");
    id.className = "id";
    id.textContent = run.runId;

    const meta = document.createElement("div");
    meta.className = "row";
    meta.innerHTML = `<span>${run.eventCount} events</span><span>${relativeTime(run.createdAt)}</span>`;

    card.append(row, id, meta);
    card.addEventListener("click", async () => {
      state.selectedRunId = run.runId;
      await refresh();
    });
    runList.append(card);
  }
}

function renderSelected() {
  const run = state.selectedRun;
  if (!run) {
    selectedTitle.textContent = "Select a run";
    selectedSubtitle.textContent = "Workflow execution graph appears here.";
    cancelButton.classList.add("hidden");
    approveButton.classList.add("hidden");
    rejectButton.classList.add("hidden");
    return;
  }

  selectedTitle.textContent = `${run.workflowName}@${run.version} · ${run.status}`;
  selectedSubtitle.textContent = `${run.runId} · ${run.eventCount} durable events`;
  const cancellable = run.status === "running" || run.status === "compensating";
  const waiting = waitingSignalStep(run);
  cancelButton.classList.toggle("hidden", !cancellable);
  approveButton.classList.toggle("hidden", !waiting || run.status !== "running");
  rejectButton.classList.toggle("hidden", !waiting || run.status !== "running");
}

function waitingSignalStep(run) {
  return Object.values(run?.steps ?? {}).find((step) => step.status === "pending" && typeof step.waitForSignal === "string");
}

function renderDag() {
  dag.innerHTML = "";
  const defs = svgElement("defs");
  const marker = svgElement("marker", {
    id: "arrow",
    markerWidth: "10",
    markerHeight: "10",
    refX: "9",
    refY: "3",
    orient: "auto",
    markerUnits: "strokeWidth",
  });
  marker.append(svgElement("path", { d: "M0,0 L0,6 L9,3 z", fill: "rgba(148,163,184,0.55)" }));
  defs.append(marker);
  dag.append(defs);

  const stepStates = state.selectedRun?.steps ?? {};

  for (const [from, to] of edges) {
    const a = nodes[from];
    const b = nodes[to];
    const sourceState = stepStates[from]?.status;
    const targetState = stepStates[to]?.status;
    const path = svgElement("path", {
      class: `edge ${sourceState === "succeeded" && ["running", "pending", "compensation_pending"].includes(targetState) ? "active" : ""}`,
      d: edgePath(a, b),
      "marker-end": "url(#arrow)",
    });
    dag.append(path);
  }

  for (const [name, position] of Object.entries(nodes)) {
    const step = stepStates[name] ?? { status: "pending", attempts: 0, compensationAttempts: 0 };
    const group = svgElement("g", { class: `node status-${step.status}` });
    group.append(svgElement("rect", { x: position.x, y: position.y, width: 150, height: 78 }));
    group.append(svgElement("circle", { class: "status-dot", cx: position.x + 132, cy: position.y + 16, r: 7 }));

    const title = svgElement("text", { x: position.x + 15, y: position.y + 27 });
    title.textContent = position.label;
    const meta = svgElement("text", { class: "meta", x: position.x + 15, y: position.y + 45 });
    meta.textContent = stepMeta(step);
    const detail = svgElement("text", { class: "meta", x: position.x + 15, y: position.y + 61 });
    detail.textContent = stepDetail(step);
    group.append(title, meta, detail);
    dag.append(group);
  }
}

function edgePath(a, b) {
  const width = 150;
  const height = 78;
  const x1 = a.x + width;
  const y1 = a.y + height / 2;
  const x2 = b.x;
  const y2 = b.y + height / 2;
  const mid = (x1 + x2) / 2;
  return `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2 - 5} ${y2}`;
}

function stepMeta(step) {
  const parts = [step.status.replaceAll("_", " ")];
  if (step.attempts) parts.push(`${step.attempts} attempt${step.attempts === 1 ? "" : "s"}`);
  if (step.compensationAttempts) parts.push(`${step.compensationAttempts} rollback`);
  return parts.join(" · ");
}

function stepDetail(step) {
  const error = step.error?.message ?? step.compensationError?.message;
  if (error) return truncate(error, 31);
  if (step.output === undefined) return step.idempotencyKey ? truncate(`key ${step.idempotencyKey}`, 31) : "waiting";
  if (typeof step.output === "object") {
    const output = step.output;
    return truncate(output.paymentId ?? output.shipmentId ?? output.reservationId ?? output.messageId ?? JSON.stringify(output), 31);
  }
  return truncate(String(step.output), 31);
}

function renderEvents() {
  eventList.innerHTML = "";
  eventList.classList.toggle("empty", state.selectedEvents.length === 0);
  if (state.selectedEvents.length === 0) {
    eventList.textContent = "Durable events appear here";
    return;
  }

  for (const event of [...state.selectedEvents].reverse().slice(0, 120)) {
    const item = document.createElement("div");
    item.className = "event";
    const title = document.createElement("strong");
    title.textContent = event.type;
    const meta = document.createElement("span");
    meta.textContent = `#${event.seq} · ${new Date(event.at).toLocaleTimeString()}`;
    item.append(title, meta);
    const meaningful = Object.fromEntries(Object.entries(event.data).filter(([, value]) => value !== undefined));
    if (Object.keys(meaningful).length > 0) {
      const code = document.createElement("code");
      code.textContent = truncate(JSON.stringify(meaningful), 180);
      item.append(code);
    }
    eventList.append(item);
  }
}

function renderInventory() {
  inventory.innerHTML = "";
  const stock = state.metrics?.inventory ?? {};
  for (const [sku, count] of Object.entries(stock)) {
    const row = document.createElement("div");
    row.className = "stock-row";
    const name = document.createElement("span");
    name.textContent = sku.replace("sku-", "");
    const bar = document.createElement("div");
    bar.className = "stock-bar";
    const fill = document.createElement("span");
    fill.style.width = `${Math.min(100, count * 8)}%`;
    bar.append(fill);
    const value = document.createElement("strong");
    value.textContent = count;
    row.append(name, bar, value);
    inventory.append(row);
  }
}

function renderEffects() {
  effectLog.innerHTML = "";
  const effects = state.metrics?.effects ?? [];
  effectLog.classList.toggle("empty", effects.length === 0);
  if (effects.length === 0) {
    effectLog.textContent = "No side effects yet";
    return;
  }
  for (const effect of effects.slice(0, 30)) {
    const row = document.createElement("div");
    row.className = "effect";
    const left = document.createElement("strong");
    left.textContent = effect.type;
    const detail = document.createElement("span");
    detail.textContent = effect.detail;
    row.append(left, detail);
    effectLog.append(row);
  }
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { "content-type": "application/json", ...(options.headers ?? {}) },
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${body}`);
  }
  return response.json();
}

function svgElement(name, attributes = {}) {
  const element = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value);
  return element;
}

function truncate(value, length) {
  return value.length > length ? `${value.slice(0, length - 1)}…` : value;
}

function relativeTime(timestamp) {
  const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
  if (seconds < 5) return "now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ago`;
}

function setHealth(ok) {
  health.classList.toggle("ok", ok);
  health.classList.toggle("bad", !ok);
  health.innerHTML = `<span class="dot"></span>${ok ? "journal connected" : "connection lost"}`;
}

refresh(true);
setInterval(() => refresh(), 700);
