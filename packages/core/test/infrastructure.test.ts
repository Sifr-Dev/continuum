import assert from "node:assert/strict";
import test from "node:test";
import {
  MemoryJournal,
  WorkflowRuntime,
  defineStep,
  defineWorkflow,
} from "../src/index.js";
import type { AppendEventOptions, ContinuumEvent, ContinuumEventType, NewContinuumEvent } from "../src/index.js";

class OutageJournal extends MemoryJournal {
  private unavailable = true;
  constructor(private readonly failures: ContinuumEventType[]) {
    super();
  }

  recoverStorage(): void {
    this.unavailable = false;
  }

  override async append(events: readonly NewContinuumEvent[], options: AppendEventOptions = {}): Promise<ContinuumEvent[]> {
    if (this.unavailable && events.some((event) => this.failures.includes(event.type))) {
      const error = new Error("database is locked") as Error & { code: string };
      error.code = "SQLITE_BUSY";
      throw error;
    }
    return super.append(events, options);
  }
}

test("a journal error after a successful effect never consumes an application retry", async () => {
  const journal = new OutageJournal(["step.succeeded"]);
  let effects = 0;
  const charge = defineStep<{}, string>({
    name: "charge",
    execute: () => {
      effects += 1;
      return `charge-${effects}`;
    },
  });
  const workflow = defineWorkflow<{}, [typeof charge]>({ name: "outcome-outage", version: 1, steps: [charge] });
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5, leaseTtlMs: 5_000 });
  const runId = await runtime.startWorkflow(workflow, {});
  await new Promise((resolve) => setTimeout(resolve, 200));
  journal.recoverStorage();
  const state = await runtime.waitForRun(runId, 5_000);
  assert.equal(state.status, "succeeded");
  assert.equal(effects, 1, "the effect must not be repeated because its completion append failed");
  const events = await journal.events(runId);
  assert.equal(events.some((event) => event.type === "step.failed"), false);
  assert.equal(events.filter((event) => event.type === "step.interrupted").length <= 1, true);
  assert.equal(events.filter((event) => event.type === "step.succeeded").length, 1);
  await runtime.close();
});

test("a journal error cannot turn a completed refund into a failed compensation", async () => {
  const journal = new OutageJournal(["compensation.step_completed"]);
  let refunds = 0;
  const charge = defineStep<{}, string>({
    name: "charge",
    execute: () => "charged",
    compensate: () => { refunds += 1; },
  });
  const fail = defineStep<{}, never>({
    name: "fail",
    needs: ["charge"],
    execute: () => { throw new Error("forward failure"); },
  });
  const workflow = defineWorkflow<{}, [typeof charge, typeof fail]>({ name: "refund-outage", version: 1, steps: [charge, fail] });
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5, leaseTtlMs: 5_000 });
  const runId = await runtime.startWorkflow(workflow, {});
  for (let index = 0; index < 100; index += 1) {
    const state = await runtime.getRun(runId);
    if (state?.steps.charge?.status === "compensating") break;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  journal.recoverStorage();
  const state = await runtime.waitForRun(runId, 5_000);
  assert.equal(state.status, "compensated");
  assert.equal(refunds, 1);
  const events = await journal.events(runId);
  assert.equal(events.some((event) => event.type === "compensation.step_failed"), false);
  assert.equal(events.some((event) => event.type === "run.compensation_failed"), false);
  await runtime.close();
});

test("an embedded worker re-drives a run after a transient journal outage", async () => {
  const journal = new OutageJournal(["step.started"]);
  const step = defineStep<{}, number>({ name: "step", execute: () => 1 });
  const workflow = defineWorkflow<{}, [typeof step]>({ name: "pump-outage", version: 1, steps: [step] });
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5, leaseTtlMs: 5_000 });
  const runId = await runtime.startWorkflow(workflow, {});
  await new Promise((resolve) => setTimeout(resolve, 150));
  assert.equal((await runtime.getRun(runId))?.steps.step?.status, "pending");
  journal.recoverStorage();
  assert.equal((await runtime.waitForRun(runId, 5_000)).status, "succeeded");
  await runtime.close();
});
