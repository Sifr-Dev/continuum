import assert from "node:assert/strict";
import test from "node:test";
import { MemoryJournal, WorkflowRuntime, WorkflowValidationError, defineStep, defineWorkflow } from "../src/index.js";

test("a newer definition migrates and completes an in-flight older run", async () => {
  const journal = new MemoryJournal();

  // v1: charge hangs forever, simulating a run stranded mid-flight by a deploy.
  const chargeV1 = defineStep<{}, string>({ name: "charge", execute: () => new Promise(() => {}) });
  const workflowV1 = defineWorkflow<{}, [typeof chargeV1]>({ name: "migrating-checkout", version: 1, steps: [chargeV1] });

  const writer = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  writer.registerWorkflow(workflowV1);
  const runId = await writer.startWorkflow(workflowV1, {});
  await new Promise((resolve) => setTimeout(resolve, 50));
  // Simulate the deploy: the old worker shuts down and releases its lease.
  await writer.close({ graceMs: 0 });

  // v2: same step name, working implementation, opts into migrating v1.
  const chargeV2 = defineStep<{}, string>({ name: "charge", execute: () => "charged-by-v2" });
  const workflowV2 = defineWorkflow<{}, [typeof chargeV2]>({
    name: "migrating-checkout",
    version: 2,
    steps: [chargeV2],
    migratesFrom: [1],
  });

  const worker = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  worker.registerWorkflow(workflowV2);
  const recovered = await worker.recover();
  assert.deepEqual(recovered, [runId]);

  const state = await worker.waitForRun(runId, 5_000);
  assert.equal(state.status, "succeeded");
  assert.equal((state.output as { charge: string }).charge, "charged-by-v2");
  assert.equal(state.version, 2);

  const events = await journal.events(runId);
  const migration = events.find((event) => event.type === "run.migrated");
  assert.ok(migration, "migration must be journaled");
  assert.deepEqual(migration.data, { fromVersion: 1, toVersion: 2 });
  await worker.close();
});

test("migration is refused when a live step is missing from the new definition", async () => {
  const journal = new MemoryJournal();
  const chargeV1 = defineStep<{}, string>({ name: "charge", execute: () => new Promise(() => {}) });
  const shipV1 = defineStep<{}, string>({ name: "ship", needs: ["charge"], execute: () => "shipped" });
  const workflowV1 = defineWorkflow<{}, [typeof chargeV1, typeof shipV1]>({
    name: "incompatible-checkout",
    version: 1,
    steps: [chargeV1, shipV1],
  });

  const writer = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  writer.registerWorkflow(workflowV1);
  const runId = await writer.startWorkflow(workflowV1, {});
  await writer.close({ graceMs: 0 });

  // v2 drops the still-pending "ship" step: migration must be refused.
  const chargeV2 = defineStep<{}, string>({ name: "charge", execute: () => "charged" });
  const workflowV2 = defineWorkflow<{}, [typeof chargeV2]>({
    name: "incompatible-checkout",
    version: 2,
    steps: [chargeV2],
    migratesFrom: [1],
  });

  const worker = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  worker.registerWorkflow(workflowV2);
  const recovered = await worker.recover();
  assert.deepEqual(recovered, []);
  await worker.close();
});

test("migration is refused without explicit migratesFrom opt-in", async () => {
  const journal = new MemoryJournal();
  const chargeV1 = defineStep<{}, string>({ name: "charge", execute: () => new Promise(() => {}) });
  const workflowV1 = defineWorkflow<{}, [typeof chargeV1]>({ name: "strict-checkout", version: 1, steps: [chargeV1] });

  const writer = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  writer.registerWorkflow(workflowV1);
  const runId = await writer.startWorkflow(workflowV1, {});
  await writer.close({ graceMs: 0 });

  const chargeV2 = defineStep<{}, string>({ name: "charge", execute: () => "charged" });
  const workflowV2 = defineWorkflow<{}, [typeof chargeV2]>({ name: "strict-checkout", version: 2, steps: [chargeV2] });

  const worker = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  worker.registerWorkflow(workflowV2);
  assert.deepEqual(await worker.recover(), []);
  const state = await worker.getRun(runId);
  assert.equal(state?.version, 1);
  await worker.close();
});

test("a completed step may be removed in the migrating version", async () => {
  const journal = new MemoryJournal();
  // v1: audit completes, then charge hangs. v2 drops "audit" — legal, because
  // audit is already terminal in the journaled plan.
  const auditV1 = defineStep<{}, string>({ name: "audit", sideEffect: false, execute: () => "audited" });
  const chargeV1 = defineStep<{}, string>({ name: "charge", needs: ["audit"], execute: () => new Promise(() => {}) });
  const workflowV1 = defineWorkflow<{}, [typeof auditV1, typeof chargeV1]>({
    name: "shrinking-checkout",
    version: 1,
    steps: [auditV1, chargeV1],
  });

  const writer = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  writer.registerWorkflow(workflowV1);
  const runId = await writer.startWorkflow(workflowV1, {});
  await new Promise((resolve) => setTimeout(resolve, 100));
  await writer.close({ graceMs: 0 });

  const chargeV2 = defineStep<{}, string>({ name: "charge", execute: () => "charged-by-v2" });
  const workflowV2 = defineWorkflow<{}, [typeof chargeV2]>({
    name: "shrinking-checkout",
    version: 2,
    steps: [chargeV2],
    migratesFrom: [1],
  });

  const worker = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  worker.registerWorkflow(workflowV2);
  assert.deepEqual(await worker.recover(), [runId]);
  const state = await worker.waitForRun(runId, 5_000);
  assert.equal(state.status, "succeeded");
  await worker.close();
});

test("migratesFrom is validated", async () => {
  const step = defineStep<{}, number>({ name: "x", execute: () => 1 });
  for (const bad of [[2], [0], [1.5], [1, 1]]) {
    const workflow = defineWorkflow<{}, [typeof step]>({
      name: "bad-migration",
      version: 2,
      steps: [step],
      migratesFrom: bad as number[],
    });
    const runtime = new WorkflowRuntime({ journal: new MemoryJournal(), context: {}, random: () => 0.5 });
    assert.throws(() => runtime.registerWorkflow(workflow), WorkflowValidationError, `migratesFrom ${bad} must be rejected`);
    await runtime.close();
  }
});
