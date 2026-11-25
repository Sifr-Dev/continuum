import pg from "pg";
import { WorkflowRuntime, defineStep, defineWorkflow } from "@continuum/core";
import { PostgresJournal } from "../src/index.js";

// Helper process for process-crash.test.ts.
// Modes:
//   start — starts the workflow and simulates a crash-vulnerable worker:
//           the first execution of the side-effect step hangs after applying
//           the external effect but before the outcome can be journaled.
//   adopt — recovers orphaned runs and finishes them, printing the final
//           run status as JSON on stdout.

const connectionString = process.env.CONTINUUM_POSTGRES_TEST_URL;
const runId = process.env.CRASH_RUN_ID;
const table = process.env.CRASH_TABLE;
const mode = process.env.CRASH_MODE;

if (!connectionString || !runId || !table || !mode) {
  throw new Error("CONTINUUM_POSTGRES_TEST_URL, CRASH_RUN_ID, CRASH_TABLE and CRASH_MODE are required");
}
const conn = connectionString;
const crashRunId = runId;

async function applyEffect(client: pg.Client, key: string): Promise<boolean> {
  await client.query(`INSERT INTO ${table}_attempts (key) VALUES ($1)`, [key]);
  const result = await client.query(`INSERT INTO ${table}_effects (key) VALUES ($1) ON CONFLICT DO NOTHING RETURNING key`, [key]);
  return (result.rowCount ?? 0) > 0;
}

async function main() {
  const client = new pg.Client({ connectionString: conn });
  await client.connect();
  if (mode === "start") {
    await client.query(`CREATE TABLE IF NOT EXISTS ${table}_effects (key text PRIMARY KEY)`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${table}_attempts (key text, at timestamptz DEFAULT now())`);
  }

  const charge = defineStep<{}, string>({
    name: "charge",
    retry: { attempts: 3, delayMs: 1, jitter: false },
    execute: async ({ idempotencyKey }) => {
      const appliedNow = await applyEffect(client, idempotencyKey ?? "missing-key");
      if (appliedNow && mode === "start") {
        // The effect is applied in the external system but the outcome is not
        // yet journaled: hang here so the test can SIGKILL this process inside
        // the crash window.
        await new Promise(() => {});
      }
      return "charged";
    },
  });
  const confirm = defineStep<{}, string>({ name: "confirm", needs: ["charge"], execute: async () => "confirmed" });
  const workflow = defineWorkflow<{}, [typeof charge, typeof confirm]>({
    name: "process-crash-order",
    version: 1,
    steps: [charge, confirm],
  });

  const journal = new PostgresJournal(conn);
  const runtime = new WorkflowRuntime({
    journal,
    context: {},
    workerId: `${mode}-${process.pid}`,
    leaseTtlMs: 500,
    random: () => 0.5,
  });
  runtime.registerWorkflow(workflow);

  if (mode === "start") {
    await runtime.startWorkflow(workflow, {}, { runId: crashRunId });
    console.log("started");
  } else {
    await runtime.recover();
    const state = await runtime.waitForRun(crashRunId, 30_000);
    console.log(JSON.stringify({ status: state.status, output: state.output }));
    await runtime.close();
    await journal.close();
    await client.end();
    process.exit(0);
  }
}

void main();
