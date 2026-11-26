import assert from "node:assert/strict";
import test from "node:test";
import { MemoryJournal, WorkflowRuntime, defineStep, defineWorkflow, type RunState } from "@continuum/core";
import {
  classifyRun,
  formatPhase,
  formatRecoveryReport,
  recoveryExitCode,
  settleRuns,
  stepReadyAt,
  type RecoveryResult,
} from "../src/status.js";

const T0 = 1_700_000_000_000;

/** Minimal projected run state; each test overrides only what it exercises. */
function runState(overrides: Partial<RunState> = {}): RunState {
  return {
    runId: "run-1",
    workflowName: "checkout",
    version: 1,
    input: {},
    status: "running",
    createdAt: T0,
    updatedAt: T0,
    cancellationRequested: false,
    compensationStarted: false,
    steps: {},
    signals: {},
    eventCount: 2,
    ...overrides,
  };
}

function step(name: string, overrides: Record<string, unknown> = {}) {
  return {
    name,
    needs: [],
    status: "pending" as const,
    attempts: 0,
    compensationAttempts: 0,
    hasCompensation: false,
    sideEffect: true,
    ...overrides,
  };
}

test("terminal runs are classified as terminal", () => {
  for (const status of ["succeeded", "failed", "cancelled", "compensated", "compensation_failed"] as const) {
    const info = classifyRun(runState({ status }));
    assert.equal(info.phase, "terminal", `${status} should be terminal`);
  }
});

test("a run parked on a durable signal is awaiting_signal, not executing", () => {
  const state = runState({
    steps: { approve: step("approve", { waitForSignal: "order.review" }) as never },
  });
  const info = classifyRun(state);
  assert.equal(info.phase, "awaiting_signal");
  assert.equal(info.signal, "order.review");
  assert.match(info.detail, /order\.review/);
});

test("a delivered signal un-parks the run", () => {
  const state = runState({
    steps: { approve: step("approve", { waitForSignal: "order.review" }) as never },
    signals: { "order.review": { approved: true } },
  });
  assert.equal(classifyRun(state).phase, "runnable");
});

test("retry backoff surfaces as awaiting_retry with a resume time", () => {
  const state = runState({
    steps: { charge: step("charge", { nextAttemptAt: T0 + 5_000 }) as never },
  });
  const info = classifyRun(state, T0);
  assert.equal(info.phase, "awaiting_retry");
  assert.equal(info.resumeAt, T0 + 5_000);
});

test("an elapsed backoff is runnable rather than parked", () => {
  const state = runState({
    steps: { charge: step("charge", { nextAttemptAt: T0 - 1_000 }) as never },
  });
  assert.equal(classifyRun(state, T0).phase, "runnable");
});

test("a durable timer gate surfaces as awaiting_timer with a resume time", () => {
  const state = runState({
    steps: { wait: step("wait", { delayMs: 60_000 }) as never },
  });
  const info = classifyRun(state, T0);
  assert.equal(info.phase, "awaiting_timer");
  assert.equal(info.resumeAt, T0 + 60_000);
});

test("stepReadyAt anchors timers to dependency completion when present", () => {
  const state = runState({
    createdAt: T0,
    steps: {
      first: step("first", { status: "succeeded", completedAt: T0 + 1_000 }) as never,
      wait: step("wait", { delayMs: 2_000, needs: ["first"] }) as never,
    },
  });
  assert.equal(stepReadyAt(state.steps.wait as never, state), T0 + 3_000);
});

test("pending dependencies classify as blocked, not runnable", () => {
  const state = runState({
    steps: {
      first: step("first") as never,
      second: step("second", { needs: ["first"] }) as never,
    },
  });
  assert.equal(classifyRun(state).phase, "runnable");
  const blocked = runState({
    steps: { second: step("second", { needs: ["missing"] }) as never },
  });
  assert.equal(classifyRun(blocked).phase, "blocked");
});

test("running step work is executing and outranks other signals", () => {
  const state = runState({
    steps: {
      charge: step("charge", { status: "running" }) as never,
      review: step("review", { waitForSignal: "go" }) as never,
    },
  });
  const info = classifyRun(state);
  assert.equal(info.phase, "executing");
  assert.match(info.detail, /charge/);
});

test("cancellation that has not been rolled back is cancel_requested, not cancelled", () => {
  const state = runState({ cancellationRequested: true, cancellationReason: "customer asked" });
  const info = classifyRun(state);
  assert.equal(info.phase, "cancel_requested");
  assert.match(info.detail, /customer asked/);
  assert.match(info.detail, /rollback/);
});

test("compensating runs report the rollback reason", () => {
  const info = classifyRun(runState({ status: "compensating", compensationReason: "payment failed" }));
  assert.equal(info.phase, "compensating");
  assert.equal(info.detail, "payment failed");
});

test("settleRuns returns parked runs immediately instead of waiting out the timeout", async () => {
  let clock = 0;
  const state = runState({ steps: { approve: step("approve", { waitForSignal: "go" }) as never } });
  const started = Date.now();
  const results = await settleRuns(async () => state, ["run-1"], {
    timeoutMs: 30_000,
    now: () => clock,
    sleep: async (ms) => { clock += ms; },
  });
  // A parked run must settle on the first observation, without polling.
  assert.equal(results.length, 1);
  assert.equal(results[0]?.outcome, "awaiting_signal");
  assert.equal(results[0]?.waitedMs, 0);
  assert.ok(Date.now() - started < 5_000, "must not block for the timeout");
});

test("settleRuns waits for a run that is genuinely progressing", async () => {
  let clock = 0;
  let polls = 0;
  const results = await settleRuns(async () => {
    polls += 1;
    const running = polls < 3;
    return runState({
      status: running ? "running" : "succeeded",
      steps: { a: step("a", { status: running ? "running" : "succeeded" }) as never },
    });
  }, ["run-1"], {
    timeoutMs: 60_000,
    pollMs: 100,
    now: () => clock,
    sleep: async (ms) => { clock += ms; },
  });
  assert.equal(results[0]?.outcome, "succeeded");
  assert.equal(results[0]?.status, "succeeded");
});

test("settleRuns reports timed_out for runs that never settle", async () => {
  let clock = 0;
  const results = await settleRuns(
    async () => runState({ steps: { a: step("a", { status: "running" }) as never } }),
    ["run-1"],
    { timeoutMs: 500, pollMs: 100, now: () => clock, sleep: async (ms) => { clock += ms; } },
  );
  assert.equal(results[0]?.outcome, "timed_out");
  assert.equal(results[0]?.status, "running");
});

test("settleRuns preserves input order and reports missing runs as unobserved", async () => {
  const results = await settleRuns(async (runId) => {
    if (runId === "gone") return undefined;
    return runState({ runId, status: "succeeded" });
  }, ["second", "gone", "first"], { timeoutMs: 1_000, now: () => 0, sleep: async () => {} });
  assert.deepEqual(results.map((result) => result.runId), ["second", "gone", "first"]);
  assert.equal(results[1]?.outcome, "timed_out");
  assert.match(results[1]?.detail ?? "", /not observed/);
});

test("recovery exit code fails only on real operator-actionable outcomes", () => {
  const parked: RecoveryResult[] = [
    { runId: "a", outcome: "awaiting_signal", status: "running", detail: "", waitedMs: 0 },
    { runId: "b", outcome: "succeeded", status: "succeeded", detail: "", waitedMs: 0 },
    { runId: "c", outcome: "cancelled", status: "cancelled", detail: "", waitedMs: 0 },
  ];
  assert.equal(recoveryExitCode(parked), 0);
  assert.equal(recoveryExitCode([...parked, { runId: "d", outcome: "failed", status: "failed", detail: "", waitedMs: 0 }]), 1);
  assert.equal(recoveryExitCode([{ runId: "e", outcome: "compensation_failed", status: "compensation_failed", detail: "", waitedMs: 0 }]), 1);
  assert.equal(recoveryExitCode([{ runId: "f", outcome: "timed_out", status: "running", detail: "", waitedMs: 0 }]), 1);
});

test("recovery report summarizes finished, parked, and moving runs", () => {
  const report = formatRecoveryReport([
    { runId: "a", outcome: "awaiting_signal", status: "running", detail: "waiting for signal \"go\" on step \"approve\"", waitedMs: 0 },
    { runId: "b", outcome: "succeeded", status: "succeeded", detail: "completed successfully", waitedMs: 5 },
  ]);
  assert.match(report, /1 finished, 1 parked on external input, 0 still moving/);
  assert.match(report, /a\s+awaiting_signal\s+waiting for signal "go"/);
});

test("recovery report handles an empty result set", () => {
  assert.match(formatRecoveryReport([]), /no runs needed attention/);
});

test("formatPhase includes the resume time when one is known", () => {
  const text = formatPhase({
    phase: "awaiting_retry",
    status: "running",
    detail: "backing off before retrying step \"charge\"",
    resumeAt: T0,
  });
  assert.match(text, /awaiting_retry/);
  assert.match(text, new RegExp(new Date(T0).toISOString()));
});

// Integration coverage: drive a real runtime + journal so the classifier is
// validated against projected state the runtime actually produces.
test("classifyRun reflects real runtime projections for a signal-parked run", async () => {
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, workerId: "test-worker" });
  const approve = defineStep({
    name: "approve",
    waitForSignal: "go",
    sideEffect: false,
    execute: ({ signalPayload }) => ({ approved: signalPayload === true }),
  });
  const workflow = defineWorkflow({ name: "probe", version: 1, steps: [approve] });
  runtime.registerWorkflow(workflow);
  const runId = await runtime.startWorkflow(workflow, {}, { defer: true });

  await runtime.recover();
  const parked = await runtime.getRun(runId);
  assert.ok(parked);
  assert.equal(classifyRun(parked).phase, "awaiting_signal");

  await runtime.signalRun(runId, "go", true);
  const finished = await runtime.waitForRun(runId, 5_000);
  assert.equal(finished.status, "succeeded");
  assert.equal(classifyRun(finished).phase, "terminal");
  await runtime.close({ graceMs: 100 });
});

test("classifyRun reflects real runtime projections for a cancellation awaiting rollback", async () => {
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, workerId: "test-worker" });
  const approve = defineStep({
    name: "approve",
    waitForSignal: "go",
    sideEffect: false,
    execute: () => ({ ok: true }),
  });
  const workflow = defineWorkflow({ name: "probe2", version: 1, steps: [approve] });
  runtime.registerWorkflow(workflow);
  const runId = await runtime.startWorkflow(workflow, {}, { defer: true });
  await runtime.recover();

  // Cancelling through a runtime that cannot execute the definition leaves the
  // run requested-but-not-rolled-back, which is exactly what the CLI reports.
  await runtime.cancel(runId, "customer asked");
  const state = await runtime.getRun(runId);
  assert.ok(state);
  const info = classifyRun(state);
  assert.equal(info.phase, "cancel_requested");
  assert.match(info.detail, /customer asked/);
  await runtime.close({ graceMs: 100 });
});
