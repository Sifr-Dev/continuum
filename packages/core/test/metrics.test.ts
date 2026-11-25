import assert from "node:assert/strict";
import test from "node:test";
import { ContinuumMetrics, MemoryJournal, WorkflowRuntime, defineStep, defineWorkflow } from "../src/index.js";

test("metrics observer accumulates step counts, durations, and run completions", async () => {
  const metrics = new ContinuumMetrics();
  const fast = defineStep<{}, string>({ name: "fast", execute: () => "ok" });
  let calls = 0;
  const flaky = defineStep<{}, string>({
    name: "flaky",
    retry: { attempts: 2, delayMs: 1, jitter: false },
    execute: () => {
      calls += 1;
      if (calls === 1) throw new Error("boom");
      return "recovered";
    },
  });
  const workflow = defineWorkflow<{}, [typeof fast, typeof flaky]>({ name: "metrics-workflow", version: 1, steps: [fast, flaky] });

  const runtime = new WorkflowRuntime({ journal: new MemoryJournal(), context: {}, random: () => 0.5, observers: [metrics] });
  runtime.registerWorkflow(workflow);
  const runId = await runtime.startWorkflow(workflow, {});
  const state = await runtime.waitForRun(runId, 5_000);
  assert.equal(state.status, "succeeded");
  await runtime.close();

  const text = metrics.textFormat();
  assert.match(text, /continuum_step_executions_total\{workflow="metrics-workflow",step="fast",outcome="succeeded"\} 1/);
  assert.match(text, /continuum_step_executions_total\{workflow="metrics-workflow",step="flaky",outcome="failed"\} 1/);
  assert.match(text, /continuum_step_executions_total\{workflow="metrics-workflow",step="flaky",outcome="succeeded"\} 1/);
  assert.match(text, /continuum_step_duration_ms_count\{workflow="metrics-workflow",step="fast"\} 1/);
  assert.match(text, /continuum_runs_completed_total\{workflow="metrics-workflow",status="succeeded"\} 1/);
});
