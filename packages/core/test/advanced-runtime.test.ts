import assert from "node:assert/strict";
import test from "node:test";
import {
  MemoryJournal,
  WorkflowRuntime,
  defineStep,
  defineWorkflow,
  type ContinuumEvent,
} from "../src/index.js";

function runtime(journal = new MemoryJournal(), observers: Array<{ onEvent?: (event: ContinuumEvent) => void }> = []) {
  return new WorkflowRuntime({ journal, context: {}, observers, random: () => 0.5 });
}

test("times out slow steps and retries them independently using one runtime", async () => {
  let attempts = 0;
  const slow = defineStep<{}, string>({
    name: "slow",
    timeoutMs: 30,
    retry: { attempts: 2, delayMs: 1, jitter: false },
    execute: async () => {
      attempts += 1;
      if (attempts === 1) await new Promise((resolve) => setTimeout(resolve, 120));
      return "finished";
    },
  });
  const workflow = defineWorkflow<{}, [typeof slow]>({ name: "timeout", version: 1, steps: [slow] });

  const instance = runtime();
  const runId = await instance.startWorkflow(workflow, {});
  const state = await instance.waitForRun(runId);

  assert.equal(state.status, "succeeded");
  assert.equal(attempts, 2);
  assert.equal(state.steps.slow?.attempts, 2);
});

test("retries compensation and records compensation attempts", async () => {
  let compensationAttempts = 0;
  const effect = defineStep<{}, string>({
    name: "effect",
    execute: () => "done",
    compensationRetry: { attempts: 2, delayMs: 1, jitter: false },
    compensate: () => {
      compensationAttempts += 1;
      if (compensationAttempts === 1) throw new Error("refund gateway unavailable");
    },
  });
  const failure = defineStep<{}, never>({
    name: "failure",
    needs: ["effect"],
    execute: () => {
      throw new Error("forward failure");
    },
  });
  const workflow = defineWorkflow<{}, [typeof effect, typeof failure]>({
    name: "compensation-retry",
    version: 1,
    steps: [effect, failure],
  });

  const instance = runtime();
  const runId = await instance.startWorkflow(workflow, {});
  const state = await instance.waitForRun(runId);

  assert.equal(state.status, "compensated");
  assert.equal(compensationAttempts, 2);
  assert.equal(state.steps.effect?.compensationAttempts, 2);
});

test("fails compensation explicitly when a side-effecting step has no compensation", async () => {
  const effect = defineStep<{}, string>({
    name: "effect",
    execute: () => "irreversible-email-sent",
  });
  const failure = defineStep<{}, never>({
    name: "failure",
    needs: ["effect"],
    execute: () => {
      throw new Error("downstream failed");
    },
  });
  const workflow = defineWorkflow<{}, [typeof effect, typeof failure]>({
    name: "missing-compensation",
    version: 1,
    steps: [effect, failure],
  });

  const instance = runtime();
  const runId = await instance.startWorkflow(workflow, {});
  const state = await instance.waitForRun(runId);

  assert.equal(state.status, "compensation_failed");
  assert.match(state.error?.message ?? "", /does not define compensation/);
  assert.equal(state.steps.effect?.status, "compensation_skipped");
});

test("emits custom domain events and notifies observers in journal order", async () => {
  const seen: string[] = [];
  const observer = {
    onEvent(event: ContinuumEvent) {
      seen.push(event.type);
    },
  };
  const instrumented = defineStep<{}, string>({
    name: "instrumented",
    execute: async ({ emit }) => {
      await emit("payment.authorize_started", { provider: "demo" });
      return "ok";
    },
  });
  const workflow = defineWorkflow<{}, [typeof instrumented]>({
    name: "observability",
    version: 1,
    steps: [instrumented],
  });

  const instance = runtime(new MemoryJournal(), [observer]);
  const runId = await instance.startWorkflow(workflow, {});
  const state = await instance.waitForRun(runId);
  const events = await instance.getEvents(runId);

  assert.equal(state.status, "succeeded");
  assert.ok(events.some((event) => event.type === "custom" && event.data.name === "payment.authorize_started"));
  assert.deepEqual(seen[0], "run.created");
  assert.equal(seen.at(-1), "run.succeeded");
});

test("parallel independent steps can overlap under the concurrency limit", async () => {
  let active = 0;
  let maxActive = 0;
  const makeStep = (name: string) => defineStep<{}, string>({
    name,
    execute: async () => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 35));
      active -= 1;
      return name;
    },
  });
  const a = makeStep("a");
  const b = makeStep("b");
  const c = makeStep("c");
  const workflow = defineWorkflow<{}, [typeof a, typeof b, typeof c]>({
    name: "parallel",
    version: 1,
    steps: [a, b, c],
  });

  const instance = new WorkflowRuntime({ journal: new MemoryJournal(), context: {}, concurrency: 2, random: () => 0.5 });
  const runId = await instance.startWorkflow(workflow, {});
  const state = await instance.waitForRun(runId);

  assert.equal(state.status, "succeeded");
  assert.equal(maxActive, 2);
});

test("workflow validation rejects unknown dependencies before writing events", async () => {
  const broken = defineStep<{}, string>({ name: "broken", needs: ["missing"], execute: () => "never" });
  const workflow = defineWorkflow<{}, [typeof broken]>({ name: "broken-graph", version: 1, steps: [broken] });
  const journal = new MemoryJournal();
  const instance = runtime(journal);

  await assert.rejects(() => instance.startWorkflow(workflow, {}), /unknown step/);
  assert.deepEqual(await journal.allEvents(), []);
});
