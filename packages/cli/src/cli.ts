#!/usr/bin/env node
import { writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { parseArgs } from "node:util";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { WorkflowRuntime, type AnyWorkflowDefinition, type WorkflowJournal } from "@continuum/core";
import { PostgresJournal } from "@continuum/postgres";
import { SqliteJournal } from "@continuum/sqlite";
import { formatRunSummary, formatStepTable, json, parseWorkflowSelector, table } from "./format.js";

const HELP = `continuum - operations CLI for durable workflows

Usage:
  continuum list [--db file]
  continuum inspect <run-id> [--db file] [--events]
  continuum start <workflow[@version]> --workflows module.js --input '{"json":true}' [--db file] [--wait]
  continuum cancel <run-id> [--db file] [--workflows module.js] [--reason text]
  continuum signal <run-id> <signal-name> [--payload '{"json":true}'] [--db file]
  continuum query <run-id> <query-name> --workflows module.js [--db file]
  continuum recover --workflows module.js [--db file]
  continuum worker --workflows module.js [--db file] [--postgres url] [--lease-ttl ms] [--metrics-port port]
  continuum leases [--db file] [--postgres url]
  continuum export [--db file] [--out file]

Examples:
  continuum start checkout@1 --workflows ./dist/workflows.js --input '{"orderId":"ord_1"}' --wait
  continuum inspect 2ba6... --events
  continuum recover --workflows ./dist/workflows.js
`;

async function main(argv: string[]): Promise<number> {
  const [command, ...rest] = argv;
  if (!command || command === "help" || command === "--help" || command === "-h") {
    console.log(HELP);
    return 0;
  }

  const { values, positionals } = parseArgs({
    args: rest,
    allowPositionals: true,
    options: {
      db: { type: "string", default: "./continuum.db" },
      workflows: { type: "string" },
      input: { type: "string" },
      out: { type: "string" },
      reason: { type: "string" },
      postgres: { type: "string" },
      payload: { type: "string" },
      leaseTtl: { type: "string" },
      "lease-ttl": { type: "string" },
      metricsPort: { type: "string" },
      "metrics-port": { type: "string" },
      events: { type: "boolean", default: false },
      wait: { type: "boolean", default: false },
    },
  });

  const leaseTtl = values["lease-ttl"] ?? values.leaseTtl;
  const metricsPort = values["metrics-port"] ?? values.metricsPort;
  const journal = createJournal(values.postgres ? String(values.postgres) : undefined, resolve(String(values.db)));
  const runtime = new WorkflowRuntime({
    journal,
    context: {},
    ...(leaseTtl ? { leaseTtlMs: Number(leaseTtl) } : {}),
  });

  try {
    switch (command) {
      case "list": {
        const runs = await runtime.listRuns();
        if (runs.length === 0) {
          console.log("No workflow runs found.");
          return 0;
        }
        console.log(table(
          ["run", "workflow", "status", "events", "created", "error"],
          runs.map((run) => [
            run.runId,
            `${run.workflowName}@${run.version}`,
            run.status,
            String(run.eventCount),
            new Date(run.createdAt).toISOString(),
            run.error?.message ?? "",
          ]),
        ));
        return 0;
      }

      case "inspect": {
        const runId = positionals[0];
        if (!runId) throw new Error("inspect requires a run id");
        const state = await runtime.getRun(runId);
        if (!state) throw new Error(`Run "${runId}" does not exist`);
        console.log(formatRunSummary({ ...state }));
        console.log("\nSteps");
        console.log(formatStepTable(state));
        if (values.events) {
          console.log("\nEvents");
          console.log(json(await runtime.getEvents(runId)));
        }
        return 0;
      }

      case "start": {
        const selector = positionals[0];
        if (!selector) throw new Error("start requires a workflow selector like checkout@1");
        const workflows = await loadWorkflows(required(values.workflows, "--workflows is required for start"));
        const selected = selectWorkflow(workflows, selector);
        const input = values.input ? JSON.parse(String(values.input)) : {};
        const runId = await runtime.startWorkflow(selected, input, { defer: !values.wait });
        console.log(runId);
        if (values.wait) {
          const state = await runtime.waitForRun(runId);
          console.log(formatRunSummary({ ...state }));
        }
        return 0;
      }

      case "signal": {
        const [runId, signalName] = positionals;
        if (!runId || !signalName) throw new Error("signal requires a run id and signal name");
        const payload = values.payload ? JSON.parse(String(values.payload)) : null;
        await runtime.signalRun(runId, signalName, payload);
        console.log(`Signal ${signalName} recorded for ${runId}`);
        return 0;
      }

      case "query": {
        const [runId, queryName] = positionals;
        if (!runId || !queryName) throw new Error("query requires a run id and query name");
        const workflows = await loadWorkflows(required(values.workflows, "--workflows is required for query"));
        for (const workflow of workflows) runtime.registerWorkflow(workflow);
        console.log(json(await runtime.queryWorkflow(runId, queryName)));
        return 0;
      }

      case "cancel": {
        const runId = positionals[0];
        if (!runId) throw new Error("cancel requires a run id");
        if (values.workflows) {
          for (const workflow of await loadWorkflows(String(values.workflows))) runtime.registerWorkflow(workflow);
        }
        await runtime.cancel(runId, values.reason ? String(values.reason) : "Cancelled from continuum CLI");
        if (values.workflows) {
          const state = await runtime.waitForRun(runId);
          console.log(`Cancellation finished for ${runId} with status ${state.status}`);
        } else {
          console.log(`Cancellation requested for ${runId}`);
        }
        return 0;
      }

      case "recover": {
        const workflows = await loadWorkflows(required(values.workflows, "--workflows is required for recover"));
        for (const workflow of workflows) runtime.registerWorkflow(workflow);
        const recovered = await runtime.recover();
        const states = await Promise.all(recovered.map((runId) => runtime.waitForRun(runId)));
        console.log(json({ recovered, count: recovered.length, statuses: states.map((state) => ({ runId: state.runId, status: state.status })) }));
        return 0;
      }

      case "worker": {
        const workflows = await loadWorkflows(required(values.workflows, "--workflows is required for worker"));
        for (const workflow of workflows) runtime.registerWorkflow(workflow);
        const initiallyRecovered = await runtime.recover();
        console.log(`continuum worker ${runtime.getWorkerId()} online; recovered ${initiallyRecovered.length} run(s)`);
        const metricsServer = metricsPort
          ? await startMetricsServer(runtime, journal, Number(metricsPort))
          : undefined;
        let tickRunning = false;
        const tick = async () => {
          if (tickRunning) return;
          tickRunning = true;
          try {
            const scheduled = await runtime.startDueScheduledWorkflows();
            if (scheduled.length > 0) console.log(`started ${scheduled.length} scheduled run(s): ${scheduled.join(", ")}`);
            const runIds = await runtime.recover();
            if (runIds.length > 0) console.log(`adopted ${runIds.length} deferred run(s): ${runIds.join(", ")}`);
          } catch (error) {
            console.error(error instanceof Error ? error.message : String(error));
          } finally {
            tickRunning = false;
          }
        };
        await tick();
        const interval = setInterval(() => {
          void tick();
        }, 1_000);
        await new Promise<void>((resolve) => {
          process.once("SIGINT", resolve);
          process.once("SIGTERM", resolve);
        });
        clearInterval(interval);
        if (metricsServer) await new Promise<void>((resolveClose) => metricsServer.close(() => resolveClose()));
        console.log("continuum worker stopped");
        return 0;
      }

      case "leases": {
        if (!journal.listRunLeases) throw new Error("The selected journal does not expose worker leases");
        const leases = (await journal.listRunLeases()).filter((lease) => lease.expiresAt > Date.now());
        if (leases.length === 0) {
          console.log("No active worker leases found.");
          return 0;
        }
        console.log(table(
          ["run", "worker", "fence", "expires"],
          leases.map((lease) => [
            lease.runId,
            lease.workerId,
            String(lease.fencingToken),
            new Date(lease.expiresAt).toISOString(),
          ]),
        ));
        return 0;
      }

      case "export": {
        const events = await journal.allEvents();
        const payload = json({ exportedAt: new Date().toISOString(), events });
        if (values.out) {
          const outPath = resolve(String(values.out));
          if (outPath === resolve(String(values.db))) throw new Error("export output must not overwrite the journal database");
          await writeFile(outPath, `${payload}\n`, "utf8");
        } else {
          console.log(payload);
        }
        return 0;
      }

      default:
        throw new Error(`Unknown command "${command}"\n\n${HELP}`);
    }
  } finally {
    await journal.close?.();
  }
}

async function startMetricsServer(runtime: WorkflowRuntime<unknown>, journal: WorkflowJournal, port: number): Promise<Server> {
  const server = createServer((request, response) => {
    void (async () => {
      try {
        if (request.url === "/health") {
          response.writeHead(200, { "content-type": "application/json" });
          response.end(json({ ok: true, workerId: runtime.getWorkerId() }));
          return;
        }
        if (request.url === "/runs") {
          response.writeHead(200, { "content-type": "application/json" });
          response.end(json(await runtime.listRuns()));
          return;
        }
        if (request.url === "/metrics") {
          const runs = await runtime.listRuns();
          const leases = journal.listRunLeases ? await journal.listRunLeases() : [];
          const activeLeases = leases.filter((lease) => lease.expiresAt > Date.now());
          const statuses = new Map<string, number>();
          for (const run of runs) statuses.set(run.status, (statuses.get(run.status) ?? 0) + 1);
          const lines = [
            "# HELP continuum_workflow_runs Workflow runs by terminal or active status.",
            "# TYPE continuum_workflow_runs gauge",
            ...[...statuses.entries()].map(([status, count]) => `continuum_workflow_runs{status="${status}"} ${count}`),
            "# HELP continuum_worker_active_leases Active distributed worker leases visible in the journal.",
            "# TYPE continuum_worker_active_leases gauge",
            `continuum_worker_active_leases ${activeLeases.length}`,
            "# HELP continuum_worker_info Static worker identity information.",
            "# TYPE continuum_worker_info gauge",
            `continuum_worker_info{worker_id="${runtime.getWorkerId().replaceAll("\\", "\\\\").replaceAll("\"", "\\\"")}"} 1`,
          ];
          response.writeHead(200, { "content-type": "text/plain; version=0.0.4" });
          response.end(`${lines.join("\n")}\n`);
          return;
        }
        response.writeHead(404, { "content-type": "text/plain" });
        response.end("not found");
      } catch (error) {
        response.writeHead(500, { "content-type": "text/plain" });
        response.end(error instanceof Error ? error.message : String(error));
      }
    })();
  });
  await new Promise<void>((resolveListen) => server.listen(port, "127.0.0.1", resolveListen));
  console.log(`continuum worker metrics listening on http://127.0.0.1:${port}`);
  return server;
}

function createJournal(postgresUrl: string | undefined, sqlitePath: string): WorkflowJournal {
  if (postgresUrl) return new PostgresJournal(postgresUrl);
  return new SqliteJournal(sqlitePath);
}

function required(value: unknown, message: string): string {
  if (typeof value !== "string" || value.length === 0) throw new Error(message);
  return value;
}

async function loadWorkflows(modulePath: string): Promise<AnyWorkflowDefinition[]> {
  const imported = await import(pathToFileURL(resolve(modulePath)).href) as Record<string, unknown>;
  const candidate = imported.workflows ?? imported.default;
  const workflows = Array.isArray(candidate) ? candidate : candidate ? [candidate] : [];
  if (workflows.length === 0) {
    throw new Error(`Module "${modulePath}" must export a workflow, an array of workflows, or a default workflow`);
  }
  return workflows as AnyWorkflowDefinition[];
}

function selectWorkflow(workflows: AnyWorkflowDefinition[], selectorText: string): AnyWorkflowDefinition {
  const selector = parseWorkflowSelector(selectorText);
  const matches = workflows.filter((workflow) => workflow.name === selector.name)
    .filter((workflow) => selector.version === undefined || workflow.version === selector.version)
    .sort((a, b) => b.version - a.version);
  const selected = matches[0];
  if (!selected) throw new Error(`No registered workflow matches "${selectorText}"`);
  return selected;
}

main(process.argv.slice(2)).then((code) => {
  process.exitCode = code;
}).catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
