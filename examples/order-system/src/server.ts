import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { mkdir, readFile } from "node:fs/promises";
import { extname, join, normalize, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import { WorkflowRuntime } from "@continuum/core";
import { SqliteJournal } from "@continuum/sqlite";
import { DemoServices, type OrderInput } from "./services.js";
import { createCheckoutWorkflow } from "./workflows.js";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const publicDir = resolve(__dirname, "../public");
const dataDir = resolve(process.env.CONTINUUM_DATA_DIR ?? join(process.cwd(), ".continuum"));
const port = Number(process.env.PORT ?? 4173);
const host = process.env.HOST ?? "127.0.0.1";

await mkdir(dataDir, { recursive: true });
const journal = new SqliteJournal(join(dataDir, "continuum.db"));
const services = new DemoServices();
const checkoutWorkflow = createCheckoutWorkflow(services);
const runtime = new WorkflowRuntime({ journal, context: services, concurrency: 4 });
runtime.registerWorkflow(checkoutWorkflow);
services.restoreFromEvents(await journal.allEvents());
const recovered = await runtime.recover();
if (recovered.length > 0) {
  console.log(`Recovered ${recovered.length} interrupted workflow run(s): ${recovered.join(", ")}`);
}

const server = createServer(async (request, response) => {
  try {
    await route(request, response);
  } catch (error) {
    sendJson(response, 500, {
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

server.listen(port, host, () => {
  console.log(`Continuum order-system demo listening on http://${host}:${port}`);
});

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function shutdown(): Promise<void> {
  server.close();
  await journal.close();
  process.exit(0);
}

async function route(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
  const method = request.method ?? "GET";

  if (method === "GET" && url.pathname === "/api/health") {
    sendJson(response, 200, { ok: true, framework: "continuum", recoveredAtStartup: recovered });
    return;
  }

  if (method === "GET" && url.pathname === "/api/runs") {
    sendJson(response, 200, { runs: await runtime.listRuns() });
    return;
  }

  const runMatch = url.pathname.match(/^\/api\/runs\/([^/]+)$/);
  if (method === "GET" && runMatch) {
    const runId = decodeURIComponent(runMatch[1] ?? "");
    const run = await runtime.getRun(runId);
    if (!run) {
      sendJson(response, 404, { error: "run not found" });
      return;
    }
    sendJson(response, 200, { run, events: await runtime.getEvents(runId) });
    return;
  }

  const cancelMatch = url.pathname.match(/^\/api\/runs\/([^/]+)\/cancel$/);
  if (method === "POST" && cancelMatch) {
    const runId = decodeURIComponent(cancelMatch[1] ?? "");
    const body = await readJson(request);
    await runtime.cancel(runId, typeof body.reason === "string" ? body.reason : "Cancelled from operations dashboard");
    sendJson(response, 202, { ok: true });
    return;
  }

  if (method === "POST" && url.pathname === "/api/orders") {
    const body = await readJson(request);
    const order = normalizeOrder(body);
    const runId = await runtime.startWorkflow(checkoutWorkflow, order, { runId: `checkout:${order.orderId}` });
    sendJson(response, 201, { runId, order });
    return;
  }

  if (method === "GET" && url.pathname === "/api/metrics") {
    const runs = await runtime.listRuns();
    const byStatus = Object.fromEntries(runs.map((run) => [run.status, 0]));
    for (const run of runs) byStatus[run.status] = (byStatus[run.status] ?? 0) + 1;
    sendJson(response, 200, {
      totalRuns: runs.length,
      byStatus,
      totalEvents: runs.reduce((total, run) => total + run.eventCount, 0),
      inventory: services.inventorySnapshot(),
      effects: services.effectLog().slice(-80).reverse(),
    });
    return;
  }

  if (method === "POST" && url.pathname === "/api/recover") {
    sendJson(response, 200, { recovered: await runtime.recover() });
    return;
  }

  if (method === "GET") {
    await serveStatic(url.pathname, response);
    return;
  }

  sendJson(response, 404, { error: "not found" });
}

function normalizeOrder(body: Record<string, unknown>): OrderInput {
  const amount = Number(body.amount ?? 249);
  const quantity = Number(body.quantity ?? 1);
  const sku = typeof body.sku === "string" ? body.sku : "sku-keyboard";
  return {
    orderId: typeof body.orderId === "string" && body.orderId ? body.orderId : `ord_${randomUUID().slice(0, 8)}`,
    customerId: typeof body.customerId === "string" && body.customerId ? body.customerId : "customer@example.com",
    amount,
    currency: typeof body.currency === "string" ? body.currency : "USD",
    items: [{ sku, quantity: Math.max(1, Math.floor(quantity)) }],
    chaos: isChaosMode(body.chaos) ? body.chaos : "none",
  };
}

function isChaosMode(value: unknown): value is OrderInput["chaos"] {
  return value === "none" || value === "flaky-payment" || value === "payment-outage" || value === "inventory-outage" || value === "slow-payment";
}

async function readJson(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 64 * 1024) {
      request.destroy();
      throw new Error("request body exceeds 64 KiB");
    }
    chunks.push(Buffer.from(chunk));
  }
  if (chunks.length === 0) return {};
  const text = Buffer.concat(chunks).toString("utf8");
  if (!text.trim()) return {};
  const parsed = JSON.parse(text) as unknown;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error("request body must be a JSON object");
  return parsed as Record<string, unknown>;
}

function sendJson(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  response.end(JSON.stringify(value));
}

async function serveStatic(pathname: string, response: ServerResponse): Promise<void> {
  const requested = pathname === "/" ? "/index.html" : pathname;
  const filePath = normalize(join(publicDir, requested));
  if (!filePath.startsWith(publicDir)) {
    sendJson(response, 403, { error: "forbidden" });
    return;
  }

  try {
    const content = await readFile(filePath);
    response.writeHead(200, {
      "content-type": contentType(extname(filePath)),
      "cache-control": "no-store",
    });
    response.end(content);
  } catch {
    const content = await readFile(join(publicDir, "index.html"));
    response.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
    response.end(content);
  }
}

function contentType(extension: string): string {
  switch (extension) {
    case ".html": return "text/html; charset=utf-8";
    case ".css": return "text/css; charset=utf-8";
    case ".js": return "text/javascript; charset=utf-8";
    case ".svg": return "image/svg+xml";
    case ".png": return "image/png";
    default: return "application/octet-stream";
  }
}
