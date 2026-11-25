import assert from "node:assert/strict";
import test from "node:test";
import { MemoryJournal, WorkflowRuntime, WorkflowValidationError, defineStep, defineWorkflow } from "../src/index.js";

function buildOrderWorkflow() {
  const reserve = defineStep<{ item: string }, number>({ name: "reserve", execute: () => 3 });
  const charge = defineStep<{ item: string }, number>({ name: "charge", needs: ["reserve"], execute: () => 100 });
  return defineWorkflow<{ item: string }, [typeof reserve, typeof charge]>({
    name: "query-order",
    version: 1,
    steps: [reserve, charge],
    queries: {
      progress: ({ steps }) => ({
        total: Object.keys(steps).length,
        completed: Object.values(steps).filter((step) => step.status === "succeeded").length,
      }),
      chargedAmount: ({ outputs }) => (typeof outputs.charge === "number" ? outputs.charge : 0),
      audit: ({ input, status, runId }) => ({ item: (input as { item: string }).item, status, runId }),
    },
  });
}

test("queries read live projected state and final outputs", async () => {
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  const workflow = runtime.registerWorkflow(buildOrderWorkflow());
  const runId = await runtime.startWorkflow(workflow, { item: "book" });

  const state = await runtime.waitForRun(runId, 5_000);
  assert.equal(state.status, "succeeded");

  assert.deepEqual(await runtime.queryWorkflow(runId, "progress"), { total: 2, completed: 2 });
  assert.equal(await runtime.queryWorkflow(runId, "chargedAmount"), 100);
  assert.deepEqual(await runtime.queryWorkflow(runId, "audit"), { item: "book", status: "succeeded", runId });
  await runtime.close();
});

test("queries reflect signals and mid-run progress", async () => {
  const approve = defineStep<{}, string>({
    name: "approve",
    waitForSignal: "approval",
    execute: ({ signalPayload }) => `approved-by-${(signalPayload as { user: string }).user}`,
  });
  const workflow = defineWorkflow<{}, [typeof approve]>({
    name: "query-approval",
    version: 1,
    steps: [approve],
    queries: {
      waitingFor: ({ signals }) => (signals.approval === undefined ? "approval" : null),
    },
  });
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  runtime.registerWorkflow(workflow);
  const runId = await runtime.startWorkflow(workflow, {});

  assert.equal(await runtime.queryWorkflow(runId, "waitingFor"), "approval");
  await runtime.signalRun(runId, "approval", { user: "sam" });
  const state = await runtime.waitForRun(runId, 5_000);
  assert.equal(state.status, "succeeded");
  assert.equal(await runtime.queryWorkflow(runId, "waitingFor"), null);
  await runtime.close();
});

test("unknown queries and unregistered workflows fail with clear errors", async () => {
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  const workflow = runtime.registerWorkflow(buildOrderWorkflow());
  const runId = await runtime.startWorkflow(workflow, { item: "pen" });
  await runtime.waitForRun(runId, 5_000);

  await assert.rejects(() => runtime.queryWorkflow(runId, "nonexistent"), /no query "nonexistent".*chargedAmount/s);
  await assert.rejects(() => runtime.queryWorkflow("missing-run", "progress"), /does not exist/);

  const otherRuntime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  await assert.rejects(() => otherRuntime.queryWorkflow(runId, "progress"), /not registered/);
  await runtime.close();
});

test("query results must be JSON-serializable", async () => {
  const step = defineStep<{}, number>({ name: "x", execute: () => 1 });
  const workflow = defineWorkflow<{}, [typeof step]>({
    name: "query-json",
    version: 1,
    steps: [step],
    queries: { bad: () => undefined },
  });
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  runtime.registerWorkflow(workflow);
  const runId = await runtime.startWorkflow(workflow, {});
  await runtime.waitForRun(runId, 5_000);
  await assert.rejects(() => runtime.queryWorkflow(runId, "bad"), /JSON/);
  await runtime.close();
});

test("non-function query definitions are rejected at validation", async () => {
  const step = defineStep<{}, number>({ name: "x", execute: () => 1 });
  const workflow = defineWorkflow<{}, [typeof step]>({
    name: "query-invalid",
    version: 1,
    steps: [step],
    queries: { broken: 42 as never },
  });
  const runtime = new WorkflowRuntime({ journal: new MemoryJournal(), context: {}, random: () => 0.5 });
  assert.throws(() => runtime.registerWorkflow(workflow), WorkflowValidationError);
  await runtime.close();
});
