import assert from "node:assert/strict";
import test from "node:test";
import { MemoryJournal, WorkflowRuntime, defineStep, defineWorkflow } from "../src/index.js";

test("maxConcurrentSteps bounds work across all runs on a worker", async () => {
  let live = 0;
  let peak = 0;
  const slow = defineStep<{}, string>({
    name: "slow",
    execute: async () => {
      live += 1;
      peak = Math.max(peak, live);
      await new Promise((resolve) => setTimeout(resolve, 120));
      live -= 1;
      return "done";
    },
  });
  const workflow = defineWorkflow<{}, [typeof slow]>({ name: "backpressure", version: 1, steps: [slow] });

  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({
    journal,
    context: {},
    random: () => 0.5,
    concurrency: 4,
    maxConcurrentSteps: 2,
  });
  runtime.registerWorkflow(workflow);

  const runIds = await Promise.all(
    Array.from({ length: 6 }, () => runtime.startWorkflow(workflow, {})),
  );
  const states = await Promise.all(runIds.map((runId) => runtime.waitForRun(runId, 15_000)));

  for (const state of states) assert.equal(state.status, "succeeded");
  assert.ok(peak <= 2, `peak concurrent step executions ${peak} exceeded the global cap of 2`);
  assert.ok(peak >= 2, `peak ${peak} should reach the cap when work is available`);
  await runtime.close();
});

test("per-run concurrency cap still applies within the global budget", async () => {
  let live = 0;
  let peak = 0;
  const make = (name: string) =>
    defineStep<{}, string>({
      name,
      execute: async () => {
        live += 1;
        peak = Math.max(peak, live);
        await new Promise((resolve) => setTimeout(resolve, 80));
        live -= 1;
        return name;
      },
    });
  const a = make("a");
  const b = make("b");
  const c = make("c");
  const workflow = defineWorkflow<{}, [typeof a, typeof b, typeof c]>({
    name: "per-run-cap",
    version: 1,
    steps: [a, b, c],
  });

  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5, concurrency: 2, maxConcurrentSteps: 100 });
  runtime.registerWorkflow(workflow);
  const runId = await runtime.startWorkflow(workflow, {});
  const state = await runtime.waitForRun(runId, 10_000);
  assert.equal(state.status, "succeeded");
  assert.ok(peak <= 2, `peak ${peak} exceeded the per-run cap of 2`);
  await runtime.close();
});
