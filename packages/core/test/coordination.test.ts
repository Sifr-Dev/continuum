import assert from "node:assert/strict";
import test from "node:test";
import {
  MemoryJournal,
  WorkflowRuntime,
  defineSignalStep,
  defineStep,
  defineTimerStep,
  defineWorkflow,
} from "../src/index.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test("conditional steps skip without blocking downstream work", async () => {
  const first = defineStep<{ enabled: boolean }, string>({ name: "first", execute: () => "first" });
  const conditional = defineStep<{ enabled: boolean }, string>({
    name: "conditional",
    needs: ["first"],
    sideEffect: false,
    waitForSignal: "unused.review",
    when: ({ input }) => input.enabled,
    execute: () => "should-not-run",
  });
  const downstream = defineStep<{ enabled: boolean }, string>({
    name: "downstream",
    needs: ["conditional"],
    execute: () => "downstream",
  });
  const workflow = defineWorkflow<{ enabled: boolean }, [typeof first, typeof conditional, typeof downstream]>({
    name: "conditional-workflow",
    version: 1,
    steps: [first, conditional, downstream],
  });
  const runtime = new WorkflowRuntime({ journal: new MemoryJournal(), context: {}, random: () => 0.5 });

  const runId = await runtime.startWorkflow(workflow, { enabled: false });
  const state = await runtime.waitForRun(runId);

  assert.equal(state.status, "succeeded");
  assert.equal(state.steps.conditional?.status, "skipped");
  assert.equal(state.steps.downstream?.status, "succeeded");
});

test("durable signals hold a step until the payload arrives", async () => {
  const approval = defineSignalStep<{ approved: boolean }>("approval", "order.approved");
  const finish = defineStep<{}, string>({
    name: "finish",
    needs: ["approval"],
    execute: ({ outputs }) => outputs.approval && typeof outputs.approval === "object" && (outputs.approval as { approved: boolean }).approved
      ? "approved"
      : "rejected",
  });
  const workflow = defineWorkflow<{}, [typeof approval, typeof finish]>({
    name: "signal-coordination",
    version: 1,
    steps: [approval, finish],
    output: ({ outputs }) => ({ result: outputs.finish }),
  });
  const runtime = new WorkflowRuntime({ journal: new MemoryJournal(), context: {}, random: () => 0.5 });

  const runId = await runtime.startWorkflow(workflow, {});
  await sleep(30);
  const blocked = await runtime.getRun(runId);
  assert.equal(blocked?.status, "running");
  assert.equal(blocked?.steps.approval?.status, "pending");

  await runtime.signalRun(runId, "order.approved", { approved: true });
  const state = await runtime.waitForRun(runId);
  assert.equal(state.status, "succeeded");
  assert.deepEqual(state.signals["order.approved"], { approved: true });
  assert.deepEqual(state.output, { result: "approved" });
});

test("durable timers delay execution without occupying a worker task", async () => {
  const timer = defineTimerStep("cooldown", 60);
  const after = defineStep<{}, string>({ name: "after", needs: ["cooldown"], execute: () => "done" });
  const workflow = defineWorkflow<{}, [typeof timer, typeof after]>({
    name: "timer-coordination",
    version: 1,
    steps: [timer, after],
  });
  const runtime = new WorkflowRuntime({ journal: new MemoryJournal(), context: {}, random: () => 0.5 });

  const started = Date.now();
  const runId = await runtime.startWorkflow(workflow, {});
  const state = await runtime.waitForRun(runId);

  assert.equal(state.status, "succeeded");
  assert.ok(Date.now() - started >= 50);
  assert.deepEqual(state.steps.cooldown?.output, { waitedMs: 60 });
});

test("child workflow retries reattach to the deterministic child run", async () => {
  let childExecutions = 0;
  let parentAttempts = 0;
  const childStep = defineStep<{ value: number }, number>({
    name: "child-work",
    execute: ({ input }) => {
      childExecutions += 1;
      return input.value * 2;
    },
  });
  const childWorkflow = defineWorkflow<{ value: number }, [typeof childStep]>({
    name: "child-workflow",
    version: 1,
    steps: [childStep],
    output: ({ outputs }) => outputs["child-work"],
  });
  const parentStep = defineStep<{}, number>({
    name: "parent-child-step",
    retry: { attempts: 2, delayMs: 1, jitter: false },
    execute: async ({ startChildWorkflow }) => {
      const output = await startChildWorkflow<{ value: number }, number>(childWorkflow, { value: 21 });
      parentAttempts += 1;
      if (parentAttempts === 1) throw new Error("parent crashed after child completion");
      return output;
    },
  });
  const parentWorkflow = defineWorkflow<{}, [typeof parentStep]>({
    name: "parent-workflow",
    version: 1,
    steps: [parentStep],
    output: ({ outputs }) => outputs["parent-child-step"],
  });
  const runtime = new WorkflowRuntime({ journal: new MemoryJournal(), context: {}, random: () => 0.5 });

  const runId = await runtime.startWorkflow(parentWorkflow, {}, { runId: "parent-run" });
  const state = await runtime.waitForRun(runId);

  assert.equal(state.status, "succeeded");
  assert.equal(state.output, 42);
  assert.equal(childExecutions, 1);
  assert.equal(parentAttempts, 2);
  assert.equal((await runtime.getRun("parent-run:parent-child-step"))?.status, "succeeded");
});


test("recovery adopts a deferred run when a dead worker lease expires", async () => {
  const journal = new MemoryJournal();
  const step = defineStep<{}, string>({ name: "work", execute: () => "done" });
  const workflow = defineWorkflow<{}, [typeof step]>({ name: "lease-expiry-recovery", version: 1, steps: [step] });
  const runtime = new WorkflowRuntime({ journal, context: {}, workerId: "replacement", random: () => 0.5 });

  const runId = await runtime.startWorkflow(workflow, {}, { defer: true });
  const deadWorkerLease = await journal.acquireRunLease(runId, "dead-worker", 35);
  assert.ok(deadWorkerLease);
  assert.deepEqual(await runtime.recover(), []);

  const state = await runtime.waitForRun(runId);
  assert.equal(state.status, "succeeded");
  assert.deepEqual(state.output, { work: "done" });
});

test("reattaching to a deferred child workflow starts its pump", async () => {
  const journal = new MemoryJournal();
  const childStep = defineStep<{ value: number }, number>({ name: "child-work", execute: ({ input }) => input.value });
  const childWorkflow = defineWorkflow<{ value: number }, [typeof childStep]>({
    name: "deferred-child",
    version: 1,
    steps: [childStep],
    output: ({ outputs }) => outputs["child-work"],
  });
  const parentStep = defineStep<{}, number>({
    name: "parent",
    execute: ({ startChildWorkflow }) => startChildWorkflow<{ value: number }, number>(childWorkflow, { value: 9 }),
  });
  const parentWorkflow = defineWorkflow<{}, [typeof parentStep]>({
    name: "deferred-child-parent",
    version: 1,
    steps: [parentStep],
    output: ({ outputs }) => outputs.parent,
  });
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  await runtime.startWorkflow(childWorkflow, { value: 9 }, {
    runId: "parent-run:parent",
    defer: true,
  });

  const parentRunId = await runtime.startWorkflow(parentWorkflow, {}, {
    runId: "parent-run",
  });
  const state = await runtime.waitForRun(parentRunId);

  assert.equal(state.status, "succeeded");
  assert.equal(state.output, 9);
  assert.equal((await runtime.getRun("parent-run:parent"))?.status, "succeeded");
});

test("deterministic interval schedules are duplicate-safe across workers", async () => {
  const step = defineStep<{ value: number }, number>({ name: "work", execute: ({ input }) => input.value });
  const workflow = defineWorkflow<{ value: number }, [typeof step]>({
    name: "scheduled-workflow",
    version: 1,
    steps: [step],
    schedule: { intervalMs: 1_000, input: { value: 7 }, catchupWindows: 1 },
  });
  const journal = new MemoryJournal();
  const workerA = new WorkflowRuntime({ journal, context: {}, workerId: "scheduler-a", random: () => 0.5 });
  const workerB = new WorkflowRuntime({ journal, context: {}, workerId: "scheduler-b", random: () => 0.5 });
  workerA.registerWorkflow(workflow);
  workerB.registerWorkflow(workflow);

  const [startedA, startedB] = await Promise.all([
    workerA.startDueScheduledWorkflows(2_500),
    workerB.startDueScheduledWorkflows(2_500),
  ]);
  const repeat = await workerA.startDueScheduledWorkflows(2_500);

  assert.equal(startedA.length + startedB.length, 2);
  assert.deepEqual(repeat, []);
  assert.equal((await journal.allEvents()).filter((event) => event.type === "run.created").length, 2);
  assert.ok((await journal.runIds()).includes("scheduled:scheduled-workflow:1:2000"));
  await workerA.close();
  await workerB.close();
});

test("run leases mutually exclude workers sharing a journal", async () => {
  const journal = new MemoryJournal();
  const first = await journal.acquireRunLease("run-1", "worker-a", 500);
  assert.ok(first);
  assert.equal(await journal.acquireRunLease("run-1", "worker-b", 500), undefined);
  assert.equal(await journal.renewRunLease({ ...first, fencingToken: first.fencingToken + 1 }, 500), false);
  assert.equal(await journal.renewRunLease(first, 500), true);
  await journal.releaseRunLease(first);
  const second = await journal.acquireRunLease("run-1", "worker-b", 500);
  assert.equal(second?.workerId, "worker-b");
  assert.ok((second?.fencingToken ?? 0) > first.fencingToken);
});

test("a second runtime does not duplicate an actively leased workflow", async () => {
  const journal = new MemoryJournal();
  let executions = 0;
  const slow = defineStep<{}, string>({
    name: "slow",
    execute: async () => {
      executions += 1;
      await sleep(80);
      return "done";
    },
  });
  const workflow = defineWorkflow<{}, [typeof slow]>({ name: "leased-workflow", version: 1, steps: [slow] });
  const workerA = new WorkflowRuntime({ journal, context: {}, workerId: "worker-a", leaseTtlMs: 1_000, random: () => 0.5 });
  const workerB = new WorkflowRuntime({ journal, context: {}, workerId: "worker-b", leaseTtlMs: 1_000, random: () => 0.5 });
  workerB.registerWorkflow(workflow);

  const runId = await workerA.startWorkflow(workflow, {});
  await sleep(15);
  await workerB.recover();
  const state = await workerA.waitForRun(runId);

  assert.equal(state.status, "succeeded");
  assert.equal(executions, 1);
});
