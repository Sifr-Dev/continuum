import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { WorkflowRuntime, defineStep, defineWorkflow } from "@continuum/core";
import { PostgresJournal } from "../src/index.js";

const connectionString = process.env.CONTINUUM_POSTGRES_TEST_URL;
const postgresTest = connectionString ? test : test.skip;

postgresTest("concurrent lease acquisition has exactly one winner", async () => {
  const journal = new PostgresJournal(connectionString as string);
  try {
    const runId = `lease-${randomUUID()}`;
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, index) => journal.acquireRunLease(runId, `worker-${index}`, 5_000)),
    );
    const winners = results.filter((lease) => lease !== undefined);
    assert.equal(winners.length, 1);
    assert.equal(winners[0]?.fencingToken, 1);

    const loser = await journal.acquireRunLease(runId, "loser", 5_000);
    assert.equal(loser, undefined);
    await journal.releaseRunLease(winners[0]!);
    const successor = await journal.acquireRunLease(runId, "successor", 5_000);
    assert.equal(successor?.workerId, "successor");
    assert.ok((successor?.fencingToken ?? 0) > winners[0]!.fencingToken);
  } finally {
    await journal.close();
  }
});

postgresTest("fenced appends reject events from superseded workers", async () => {
  const journal = new PostgresJournal(connectionString as string);
  try {
    const runId = `fenced-${randomUUID()}`;
    const first = (await journal.acquireRunLease(runId, "first", 5_000))!;
    await journal.append([
      { runId, workflowName: "workflow", type: "custom", data: { name: "owned" } },
    ], { lease: first });
    await journal.releaseRunLease(first);
    const second = (await journal.acquireRunLease(runId, "second", 5_000))!;

    await assert.rejects(() => journal.append([
      { runId, workflowName: "workflow", type: "custom", data: { name: "zombie" } },
    ], { lease: first }), /lease.*lost/i);
    assert.equal((await journal.events(runId)).filter((event) => event.data.name === "zombie").length, 0);

    await journal.append([
      { runId, workflowName: "workflow", type: "custom", data: { name: "successor" } },
    ], { lease: second });
    assert.equal((await journal.events(runId)).filter((event) => event.data.name === "successor").length, 1);
  } finally {
    await journal.close();
  }
});

postgresTest("rejects duplicate run creation transactionally", async () => {
  const journal = new PostgresJournal(connectionString as string);
  try {
    const runId = `duplicate-${randomUUID()}`;
    const created = {
      runId,
      workflowName: "workflow",
      type: "run.created" as const,
      data: { workflowName: "workflow", version: 1, input: {}, steps: [] },
    };
    await journal.append([created]);
    await assert.rejects(() => journal.append([created]), /already exists/);
    assert.equal((await journal.events(runId)).filter((event) => event.type === "run.created").length, 1);
  } finally {
    await journal.close();
  }
});

postgresTest("separate runtimes coordinate through postgres leases without duplicate execution", async () => {
  const journalA = new PostgresJournal(connectionString as string);
  const journalB = new PostgresJournal(connectionString as string);
  try {
    let executions = 0;
    const slow = defineStep<{}, string>({
      name: "slow",
      execute: async () => {
        executions += 1;
        await new Promise((resolve) => setTimeout(resolve, 80));
        return "done";
      },
    });
    const workflow = defineWorkflow<{}, [typeof slow]>({ name: "postgres-leased", version: 1, steps: [slow] });
    const workerA = new WorkflowRuntime({ journal: journalA, context: {}, workerId: `worker-a-${randomUUID()}`, random: () => 0.5 });
    const workerB = new WorkflowRuntime({ journal: journalB, context: {}, workerId: `worker-b-${randomUUID()}`, random: () => 0.5 });
    workerB.registerWorkflow(workflow);

    const runId = await workerA.startWorkflow(workflow, {});
    await new Promise((resolve) => setTimeout(resolve, 15));
    await workerB.recover();
    const state = await workerA.waitForRun(runId);

    assert.equal(state.status, "succeeded");
    assert.equal(executions, 1);
    await workerA.close();
    await workerB.close();
  } finally {
    await journalA.close();
    await journalB.close();
  }
});

postgresTest("a worker can adopt a deferred run from postgres", async () => {
  const writerJournal = new PostgresJournal(connectionString as string);
  const workerJournal = new PostgresJournal(connectionString as string);
  try {
    const step = defineStep<{ value: number }, number>({ name: "double", execute: ({ input }) => input.value * 2 });
    const workflow = defineWorkflow<{ value: number }, [typeof step]>({ name: "postgres-adoption", version: 1, steps: [step] });
    const writer = new WorkflowRuntime({ journal: writerJournal, context: {}, random: () => 0.5 });
    const worker = new WorkflowRuntime({ journal: workerJournal, context: {}, random: () => 0.5 });
    worker.registerWorkflow(workflow);

    const runId = await writer.startWorkflow(workflow, { value: 21 }, { defer: true });
    assert.deepEqual(await worker.recover(), [runId]);
    const state = await worker.waitForRun(runId);

    assert.equal(state.status, "succeeded");
    assert.equal((state.output as { double: number }).double, 42);
    await writer.close();
    await worker.close();
  } finally {
    await writerJournal.close();
    await workerJournal.close();
  }
});
