import assert from "node:assert/strict";
import test from "node:test";
import { MemoryJournal, WorkflowRuntime, defineStep, defineWorkflow } from "../src/index.js";

test("graceful close lets in-flight steps journal their outcomes", async () => {
  const slow = defineStep<{}, string>({
    name: "slow",
    execute: async () => {
      await new Promise((resolve) => setTimeout(resolve, 200));
      return "finished-naturally";
    },
  });
  const workflow = defineWorkflow<{}, [typeof slow]>({ name: "shutdown-drain", version: 1, steps: [slow] });

  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  runtime.registerWorkflow(workflow);
  const runId = await runtime.startWorkflow(workflow, {});

  // Wait until the step is definitely in flight, then close with grace.
  await new Promise((resolve) => setTimeout(resolve, 50));
  await runtime.close({ graceMs: 2_000 });

  const events = await journal.events(runId);
  const succeeded = events.find((event) => event.type === "step.succeeded");
  assert.ok(succeeded, "in-flight step should have journaled its outcome during graceful close");
  assert.equal((succeeded.data as { output: unknown }).output, "finished-naturally");
  assert.equal(events.some((event) => event.type === "step.interrupted"), false);
});

test("close without enough grace falls back to crash-safe interruption", async () => {
  const hanging = defineStep<{}, string>({
    name: "hanging",
    execute: () => new Promise(() => {}),
  });
  const workflow = defineWorkflow<{}, [typeof hanging]>({ name: "shutdown-abort", version: 1, steps: [hanging] });

  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  runtime.registerWorkflow(workflow);
  const runId = await runtime.startWorkflow(workflow, {});

  await new Promise((resolve) => setTimeout(resolve, 50));
  const startedAt = Date.now();
  await runtime.close({ graceMs: 100 });
  assert.ok(Date.now() - startedAt < 2_000, "close must remain bounded when work ignores the grace period");

  const events = await journal.events(runId);
  assert.equal(events.some((event) => event.type === "step.succeeded"), false);
});

test("a closing runtime rejects new runs", async () => {
  const step = defineStep<{}, number>({ name: "x", execute: () => 1 });
  const workflow = defineWorkflow<{}, [typeof step]>({ name: "shutdown-reject", version: 1, steps: [step] });
  const runtime = new WorkflowRuntime({ journal: new MemoryJournal(), context: {}, random: () => 0.5 });
  runtime.registerWorkflow(workflow);
  const closing = runtime.close({ graceMs: 0 });
  await assert.rejects(() => runtime.startWorkflow(workflow, {}), /shutting down/);
  await closing;
});

interface RollbackFixture {
  journal: MemoryJournal;
  runtime: WorkflowRuntime;
  runId: string;
  refunds: string[];
}

/**
 * Starts a run that charges, fails forward, and then begins a slow refund.
 * Returns once the compensation attempt is journaled as started, so the caller
 * knows the rollback is genuinely in flight.
 */
async function beginInFlightRollback(refundMs: number): Promise<RollbackFixture> {
  const journal = new MemoryJournal();
  const refunds: string[] = [];
  const charge = defineStep<{}, string>({
    name: "charge",
    execute: () => "payment-captured",
    compensate: async () => {
      refunds.push("refund");
      await new Promise((resolve) => setTimeout(resolve, refundMs));
    },
  });
  const fail = defineStep<{}, never>({
    name: "fail",
    needs: ["charge"],
    execute: () => {
      throw new Error("forward failure");
    },
  });
  const workflow = defineWorkflow<{}, [typeof charge, typeof fail]>({
    name: "rollback-on-deploy",
    version: 1,
    steps: [charge, fail],
  });

  const runtime = new WorkflowRuntime({
    journal,
    context: {},
    workerId: "worker-a",
    leaseTtlMs: 5_000,
    random: () => 0.5,
  });
  const runId = await runtime.startWorkflow(workflow, {});
  for (let index = 0; index < 400; index += 1) {
    const events = await journal.events(runId);
    if (events.some((event) => event.type === "compensation.step_started")) {
      return { journal, runtime, runId, refunds };
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("compensation never started");
}

test("graceful close waits for an in-flight compensation to finish", async () => {
  // The refund takes longer than the old 2s hard pump deadline, so this only
  // passes when the grace window actually covers compensation.
  const { journal, runtime, runId, refunds } = await beginInFlightRollback(2_500);

  const startedAt = Date.now();
  await runtime.close({ graceMs: 5_000 });
  const elapsed = Date.now() - startedAt;

  assert.ok(elapsed >= 2_400, `close() returned after ${elapsed}ms without draining the rollback`);
  assert.equal(refunds.length, 1, "compensation should have been attempted exactly once");

  const events = await journal.events(runId);
  assert.ok(events.some((event) => event.type === "compensation.step_completed"));
  assert.ok(events.some((event) => event.type === "run.compensated"));
  assert.equal(events.some((event) => event.type === "compensation.step_interrupted"), false);
});

test("a drained compensation is never re-executed by the next worker", async () => {
  const { journal, runtime, runId, refunds } = await beginInFlightRollback(2_500);
  await runtime.close({ graceMs: 5_000 });

  // A replacement worker adopts anything unfinished in the same journal.
  const replacement = new WorkflowRuntime({
    journal,
    context: {},
    workerId: "worker-b",
    leaseTtlMs: 5_000,
    random: () => 0.5,
  });
  const charge = defineStep<{}, string>({
    name: "charge",
    execute: () => "payment-captured",
    compensate: () => {
      refunds.push("refund");
    },
  });
  const fail = defineStep<{}, never>({
    name: "fail",
    needs: ["charge"],
    execute: () => {
      throw new Error("forward failure");
    },
  });
  replacement.registerWorkflow(
    defineWorkflow<{}, [typeof charge, typeof fail]>({
      name: "rollback-on-deploy",
      version: 1,
      steps: [charge, fail],
    }),
  );

  const recovered = await replacement.recover();
  assert.deepEqual(recovered, [], "a fully compensated run must not be recoverable");

  const final = await replacement.getRun(runId);
  assert.equal(final?.status, "compensated");
  assert.equal(final?.steps.charge?.status, "compensated");
  assert.equal(refunds.length, 1, `compensation ran ${refunds.length} times for one charge`);
  await replacement.close({ graceMs: 0 });
});

test("close past the grace window journals an interrupted, still-retryable compensation", async () => {
  const { journal, runtime, runId, refunds } = await beginInFlightRollback(5_000);

  const startedAt = Date.now();
  await runtime.close({ graceMs: 150 });
  const elapsed = Date.now() - startedAt;
  assert.ok(elapsed < 4_000, `close() must stay bounded when compensation ignores the abort, took ${elapsed}ms`);

  const events = await journal.events(runId);
  assert.equal(refunds.length, 1);
  assert.ok(
    events.some((event) => event.type === "compensation.step_interrupted"),
    "an abandoned rollback must be journaled as interrupted, not completed",
  );
  assert.equal(events.some((event) => event.type === "compensation.step_completed"), false);
  assert.equal(events.some((event) => event.type === "run.compensated"), false);

  // The run stays unresolved so recovery can retry the rollback; nothing may be
  // appended after the journal is closed.
  const state = (await import("../src/index.js")).projectRun(events);
  assert.equal(state?.status, "compensating");
  assert.equal(state?.steps.charge?.status, "compensation_pending");
});
