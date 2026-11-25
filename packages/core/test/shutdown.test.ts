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
