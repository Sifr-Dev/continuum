import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { WorkflowRuntime, defineStep, defineWorkflow } from "@continuum/core";
import { SqliteJournal } from "../src/index.js";

test("persists event history across journal instances", async () => {
  const directory = mkdtempSync(join(tmpdir(), "continuum-sqlite-"));
  const filename = join(directory, "events.db");

  try {
    const firstJournal = new SqliteJournal(filename);
    const firstRuntime = new WorkflowRuntime({ journal: firstJournal, context: {} });

    const step = defineStep<{ value: number }, number>({
      name: "double",
      execute: ({ input }) => input.value * 2,
    });
    const workflow = defineWorkflow<{ value: number }, [typeof step]>({
      name: "sqlite-success",
      version: 1,
      steps: [step],
    });

    const runId = await firstRuntime.startWorkflow(workflow, { value: 4 });
    const firstState = await firstRuntime.waitForRun(runId);
    assert.equal(firstState.status, "succeeded");
    await firstJournal.close();

    const secondJournal = new SqliteJournal(filename);
    const events = await secondJournal.events(runId);
    assert.ok(events.length >= 4);
    assert.equal(events[0]?.type, "run.created");
    assert.equal(events.at(-1)?.type, "run.succeeded");
    await secondJournal.close();
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("rejects duplicate run creation atomically", async () => {
  const journal = SqliteJournal.memory();
  const created = {
    runId: "duplicate",
    workflowName: "workflow",
    type: "run.created" as const,
    data: { workflowName: "workflow", version: 1, input: {}, steps: [] },
  };

  await journal.append([created]);
  await assert.rejects(() => journal.append([created]), /already exists/);
  assert.equal((await journal.events("duplicate")).filter((event) => event.type === "run.created").length, 1);
  await journal.close();
});

test("serializes circular output error details without blocking rollback", async () => {
  const journal = SqliteJournal.memory();
  let compensated = 0;
  const effect = defineStep<{}, string>({
    name: "effect",
    execute: () => "done",
    compensate: () => {
      compensated += 1;
    },
  });
  const workflow = defineWorkflow<{}, [typeof effect]>({
    name: "sqlite-output-error",
    version: 1,
    steps: [effect],
    output: () => {
      const details: Record<string, unknown> = {};
      details.self = details;
      const error = new Error("output shape failed") as Error & { details: Record<string, unknown> };
      error.details = details;
      throw error;
    },
  });
  const runtime = new WorkflowRuntime({ journal, context: {} });

  const runId = await runtime.startWorkflow(workflow, {});
  const state = await runtime.waitForRun(runId);

  assert.equal(state.status, "compensated");
  assert.equal(compensated, 1);
  assert.equal(state.error?.details?.self, "[Circular]");
  await journal.close();
});

test("recovers an interrupted run from a reopened SQLite journal", async () => {
  const directory = mkdtempSync(join(tmpdir(), "continuum-sqlite-"));
  const filename = join(directory, "events.db");

  try {
    const setupJournal = new SqliteJournal(filename);
    const completed = defineStep<{ value: number }, string>({ name: "completed", execute: () => "done" });
    const interrupted = defineStep<{ value: number }, string>({
      name: "interrupted",
      needs: ["completed"],
      retry: { attempts: 2, delayMs: 1, jitter: false },
      execute: () => "recovered",
    });
    const workflow = defineWorkflow<{ value: number }, [typeof completed, typeof interrupted]>({
      name: "sqlite-recovery",
      version: 1,
      steps: [completed, interrupted],
    });

    const runId = "sqlite-recovery-run";
    await setupJournal.append([
      {
        runId,
        workflowName: "sqlite-recovery",
        type: "run.created",
        data: {
          workflowName: "sqlite-recovery",
          version: 1,
          input: { value: 1 },
          steps: [
            { name: "completed", needs: [], idempotencyKey: "completed-key", hasCompensation: false, sideEffect: true },
            { name: "interrupted", needs: ["completed"], idempotencyKey: "interrupted-key", hasCompensation: false, sideEffect: true },
          ],
        },
      },
      { runId, workflowName: "sqlite-recovery", type: "run.started", data: {} },
      { runId, workflowName: "sqlite-recovery", type: "step.started", data: { step: "completed", attempt: 1 } },
      { runId, workflowName: "sqlite-recovery", type: "step.succeeded", data: { step: "completed", attempt: 1, output: "done" } },
      { runId, workflowName: "sqlite-recovery", type: "step.started", data: { step: "interrupted", attempt: 1 } },
    ]);
    await setupJournal.close();

    const recoveryJournal = new SqliteJournal(filename);
    let interruptedExecutions = 0;
    const recoveryInterrupted = defineStep<{ value: number }, string>({
      name: "interrupted",
      needs: ["completed"],
      retry: { attempts: 2, delayMs: 1, jitter: false },
      execute: () => {
        interruptedExecutions += 1;
        return "recovered";
      },
    });
    const recoveryWorkflow = defineWorkflow<{ value: number }, [typeof completed, typeof recoveryInterrupted]>({
      name: "sqlite-recovery",
      version: 1,
      steps: [completed, recoveryInterrupted],
    });
    const runtime = new WorkflowRuntime({ journal: recoveryJournal, context: {}, random: () => 0.5 });
    runtime.registerWorkflow(recoveryWorkflow);

    assert.deepEqual(await runtime.recover(), [runId]);
    const state = await runtime.waitForRun(runId);
    assert.equal(state.status, "succeeded");
    assert.equal(interruptedExecutions, 1);
    assert.equal(state.steps.interrupted?.attempts, 2);
    await recoveryJournal.close();
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
