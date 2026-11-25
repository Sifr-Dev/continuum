#!/usr/bin/env node
import { writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { WorkflowRuntime, type AnyWorkflowDefinition } from "@continuum/core";
import { SqliteJournal } from "@continuum/sqlite";
import { formatRunSummary, formatStepTable, json, parseWorkflowSelector, table } from "./format.js";

const HELP = `continuum - operations CLI for durable workflows

Usage:
  continuum list [--db file]
  continuum inspect <run-id> [--db file] [--events]
  continuum start <workflow[@version]> --workflows module.js --input '{"json":true}' [--db file] [--wait]
  continuum cancel <run-id> [--db file] [--workflows module.js] [--reason text]
  continuum recover --workflows module.js [--db file]
  continuum worker --workflows module.js [--db file]
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
      events: { type: "boolean", default: false },
      wait: { type: "boolean", default: false },
    },
  });

  const journal = new SqliteJournal(resolve(String(values.db)));
  const runtime = new WorkflowRuntime({ journal, context: {} });

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

      case "cancel": {
        const runId = positionals[0];
        if (!runId) throw new Error("cancel requires a run id");
        if (values.workflows) {
          for (const workflow of await loadWorkflows(String(values.workflows))) runtime.registerWorkflow(workflow);
        }
        await runtime.cancel(runId, values.reason ? String(values.reason) : "Cancelled from continuum CLI");
        console.log(`Cancellation requested for ${runId}`);
        return 0;
      }

      case "recover": {
        const workflows = await loadWorkflows(required(values.workflows, "--workflows is required for recover"));
        for (const workflow of workflows) runtime.registerWorkflow(workflow);
        const recovered = await runtime.recover();
        console.log(json({ recovered, count: recovered.length }));
        return 0;
      }

      case "worker": {
        const workflows = await loadWorkflows(required(values.workflows, "--workflows is required for worker"));
        for (const workflow of workflows) runtime.registerWorkflow(workflow);
        const initiallyRecovered = await runtime.recover();
        console.log(`continuum worker online; recovered ${initiallyRecovered.length} run(s)`);
        const interval = setInterval(() => {
          void runtime.recover().then((runIds) => {
            if (runIds.length > 0) console.log(`adopted ${runIds.length} deferred run(s): ${runIds.join(", ")}`);
          });
        }, 1_000);
        await new Promise<void>((resolve) => {
          process.once("SIGINT", resolve);
          process.once("SIGTERM", resolve);
        });
        clearInterval(interval);
        console.log("continuum worker stopped");
        return 0;
      }

      case "export": {
        const events = await journal.allEvents();
        const payload = json({ exportedAt: new Date().toISOString(), events });
        if (values.out) await writeFile(String(values.out), `${payload}\n`, "utf8");
        else console.log(payload);
        return 0;
      }

      default:
        throw new Error(`Unknown command "${command}"\n\n${HELP}`);
    }
  } finally {
    await journal.close();
  }
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
