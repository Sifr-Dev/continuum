import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { WorkflowRuntime, projectRun, type NewContinuumEvent } from "@continuum/core";
import { PostgresJournal } from "../src/index.js";

const connectionString = process.env.CONTINUUM_POSTGRES_TEST_URL;
const pgTest = connectionString ? test : test.skip;

function created(runId: string): NewContinuumEvent {
  return {
    runId,
    workflowName: "wf",
    type: "run.created",
    data: {
      workflowName: "wf",
      version: 1,
      input: { value: 1 },
      steps: [{ name: "work", needs: [], hasCompensation: false, sideEffect: false }],
    },
  };
}

pgTest("Postgres snapshots persist across journal instances", async () => {
  const runId = `snapshot-${randomUUID()}`;
  const first = new PostgresJournal(connectionString as string);
  try {
    await first.append([created(runId)]);
    await first.append(
      Array.from({ length: 25 }, (_, i) => ({
        runId,
        workflowName: "wf",
        type: "custom" as const,
        data: { note: i },
      })),
    );
    const runtime = new WorkflowRuntime({ journal: first, context: {} });
    const snapshot = await runtime.snapshotRun(runId);
    assert.ok(snapshot);

    const second = new PostgresJournal(connectionString as string);
    const resumedRuntime = new WorkflowRuntime({ journal: second, context: {} });
    assert.deepEqual(await resumedRuntime.getRunWithSnapshot(runId), projectRun(await first.events(runId)));
    assert.equal((await second.latestRunSnapshot(runId))!.lastSeq, snapshot.lastSeq);
    assert.equal((await second.eventsAfter!(runId, snapshot.lastSeq)).length, 0);
    await second.close();
  } finally {
    await first.close();
  }
});