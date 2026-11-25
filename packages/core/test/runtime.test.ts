import assert from "node:assert/strict";
import test from "node:test";
import {
  MemoryJournal,
  StepInterruptedError,
  WorkflowRuntime,
  defineStep,
  defineWorkflow,
} from "../src/index.js";

interface TestInput {
  value: number;
  failAt?: string;
}

function testRuntime(journal = new MemoryJournal()) {
  return new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
}

test("executes DAG dependencies in order and exposes typed outputs", async () => {
  const order: string[] = [];
  const first = defineStep<TestInput, number>({
    name: "first",
    execute: async ({ input }) => {
      order.push("first");
      return input.value + 1;
    },
  });
  const left = defineStep<TestInput, number>({
    name: "left",
    needs: ["first"],
    execute: async ({ outputs }) => {
      order.push("left");
      return Number(outputs.first ?? 0) * 2;
    },
  });
  const right = defineStep<TestInput, number>({
    name: "right",
    needs: ["first"],
    execute: async ({ outputs }) => {
      order.push("right");
      return Number(outputs.first ?? 0) * 3;
    },
  });
  const join = defineStep<TestInput, number>({
    name: "join",
    needs: ["left", "right"],
    execute: async ({ outputs }) => {
      order.push("join");
      return Number(outputs.left) + Number(outputs.right);
    },
  });

  const workflow = defineWorkflow<TestInput, [typeof first, typeof left, typeof right, typeof join]>({
    name: "dag-success",
    version: 1,
    steps: [first, left, right, join],
    output: ({ outputs }) => ({ total: outputs.join }),
  });

  const runtime = testRuntime();
  const handle = await runtime.runWorkflow<TestInput, { total: number }>(workflow, { value: 2 });
  assert.deepEqual(await handle.result, { total: 15 });
  assert.equal(order[0], "first");
  assert.equal(order.at(-1), "join");
});

test("retries failed steps with persisted idempotency keys", async () => {
  let attempts = 0;
  const keys: string[] = [];
  const flaky = defineStep<TestInput, string>({
    name: "flaky",
    retry: { attempts: 3, delayMs: 1, jitter: false },
    execute: ({ idempotencyKey }) => {
      attempts += 1;
      keys.push(idempotencyKey ?? "");
      if (attempts < 3) throw new Error("temporary outage");
      return "ok";
    },
  });
  const workflow = defineWorkflow<TestInput, [typeof flaky]>({
    name: "retry-success",
    version: 1,
    steps: [flaky],
  });

  const runtime = testRuntime();
  const runId = await runtime.startWorkflow(workflow, { value: 1 });
  const state = await runtime.waitForRun(runId);

  assert.equal(state.status, "succeeded");
  assert.equal(attempts, 3);
  assert.equal(new Set(keys).size, 1);
  assert.match(keys[0] ?? "", new RegExp(`^${runId}:flaky$`));
});

test("runs compensations in reverse topological order after terminal failure", async () => {
  const compensated: string[] = [];
  const reserve = defineStep<TestInput, string>({
    name: "reserve",
    execute: () => "reservation",
    compensate: () => {
      compensated.push("reserve");
    },
  });
  const charge = defineStep<TestInput, string>({
    name: "charge",
    needs: ["reserve"],
    execute: () => "receipt",
    compensate: () => {
      compensated.push("charge");
    },
  });
  const ship = defineStep<TestInput, never>({
    name: "ship",
    needs: ["charge"],
    retry: { attempts: 1 },
    execute: () => {
      throw new Error("carrier unavailable");
    },
  });

  const workflow = defineWorkflow<TestInput, [typeof reserve, typeof charge, typeof ship]>({
    name: "saga",
    version: 1,
    steps: [reserve, charge, ship],
  });

  const runtime = testRuntime();
  const runId = await runtime.startWorkflow(workflow, { value: 1 });
  const state = await runtime.waitForRun(runId);

  assert.equal(state.status, "compensated");
  assert.deepEqual(compensated, ["charge", "reserve"]);
  assert.equal(state.steps.ship?.status, "failed");
});

test("recovers an interrupted step without re-executing completed work", async () => {
  const journal = new MemoryJournal();
  const firstExecutions: number[] = [];
  const secondExecutions: number[] = [];

  const first = defineStep<TestInput, string>({
    name: "first",
    execute: () => {
      firstExecutions.push(1);
      return "done";
    },
  });
  const second = defineStep<TestInput, string>({
    name: "second",
    needs: ["first"],
    retry: { attempts: 2, delayMs: 1, jitter: false },
    execute: () => {
      secondExecutions.push(1);
      return "recovered";
    },
  });
  const workflow = defineWorkflow<TestInput, [typeof first, typeof second]>({
    name: "recovery",
    version: 1,
    steps: [first, second],
  });

  const runtime = testRuntime(journal);
  runtime.registerWorkflow(workflow);
  const runId = "recovery-run";
  await journal.append([
    {
      runId,
      workflowName: "recovery",
      type: "run.created",
      data: {
        workflowName: "recovery",
        version: 1,
        input: { value: 1 },
        steps: [
          { name: "first", needs: [], idempotencyKey: "first-key", hasCompensation: false, sideEffect: true },
          { name: "second", needs: ["first"], idempotencyKey: "second-key", hasCompensation: false, sideEffect: true },
        ],
      },
    },
    { runId, workflowName: "recovery", type: "run.started", data: {} },
    { runId, workflowName: "recovery", type: "step.started", data: { step: "first", attempt: 1 } },
    { runId, workflowName: "recovery", type: "step.succeeded", data: { step: "first", attempt: 1, output: "done" } },
    { runId, workflowName: "recovery", type: "step.started", data: { step: "second", attempt: 1 } },
  ]);

  assert.deepEqual(await runtime.recover(), [runId]);
  const state = await runtime.waitForRun(runId);
  assert.equal(state.status, "succeeded");
  assert.equal(firstExecutions.length, 0);
  assert.equal(secondExecutions.length, 1);
  assert.equal(state.steps.second?.attempts, 2);
});

test("marks run failed when a step exhausts retries and has no compensable work", async () => {
  const failing = defineStep<TestInput, never>({
    name: "failing",
    sideEffect: false,
    retry: { attempts: 2, delayMs: 1, jitter: false },
    execute: () => {
      throw new Error("permanent");
    },
  });
  const workflow = defineWorkflow<TestInput, [typeof failing]>({
    name: "permanent-failure",
    version: 1,
    steps: [failing],
  });

  const runtime = testRuntime();
  const runId = await runtime.startWorkflow(workflow, { value: 1 });
  const state = await runtime.waitForRun(runId);
  assert.equal(state.status, "failed");
  assert.equal(state.steps.failing?.attempts, 2);
});

test("cancels a running workflow and rolls back completed effects", async () => {
  const compensated: string[] = [];

  const first = defineStep<TestInput, string>({
    name: "first",
    execute: () => "effect",
    compensate: () => {
      compensated.push("first");
    },
  });
  const slow = defineStep<TestInput, string>({
    name: "slow",
    needs: ["first"],
    execute: async ({ signal }) => {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(resolve, 10_000);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          reject(signal.reason);
        }, { once: true });
      });
      return "too-late";
    },
  });
  const workflow = defineWorkflow<TestInput, [typeof first, typeof slow]>({
    name: "cancel-workflow",
    version: 1,
    steps: [first, slow],
  });

  const runtime = testRuntime();
  const runId = await runtime.startWorkflow(workflow, { value: 1 });
  await new Promise((resolve) => setTimeout(resolve, 30));
  await runtime.cancel(runId, "operator test");
  const state = await runtime.waitForRun(runId);

  assert.equal(state.status, "cancelled");
  assert.deepEqual(compensated, ["first"]);
  assert.equal(state.steps.slow?.status, "failed");
});

test("rejects invalid dependency graphs", async () => {
  const a = defineStep<TestInput, string>({ name: "a", needs: ["b"], execute: () => "a" });
  const b = defineStep<TestInput, string>({ name: "b", needs: ["a"], execute: () => "b" });
  const workflow = defineWorkflow<TestInput, [typeof a, typeof b]>({
    name: "cyclic",
    version: 1,
    steps: [a, b],
  });
  const runtime = testRuntime();
  await assert.rejects(() => runtime.startWorkflow(workflow, { value: 1 }), /cycle/);
});
