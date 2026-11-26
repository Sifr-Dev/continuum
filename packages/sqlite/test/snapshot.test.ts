import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { unlink } from "node:fs/promises";
import test from "node:test";
import { WorkflowRuntime, projectRun, type NewContinuumEvent } from "@continuum/core";
import { SqliteJournal } from "../src/index.js";

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

test("SQLite snapshots persist across journal instances", async () => {
  const filename = join(tmpdir(), `continuum-snapshot-${randomUUID()}.db`);
  const runId = `snapshot-${randomUUID()}`;
  const first = new SqliteJournal(filename);
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

    // Reopen with a fresh connection; the snapshot must survive the restart.
    const second = new SqliteJournal(filename);
    const resumedRuntime = new WorkflowRuntime({ journal: second, context: {} });
    const expected = projectRun(await first.events(runId));
    assert.deepEqual(await resumedRuntime.getRunWithSnapshot(runId), expected);
    assert.equal((await second.latestRunSnapshot(runId))!.lastSeq, snapshot.lastSeq);
    // eventsAfter returns only the tail (none here).
    assert.equal((await second.eventsAfter!(runId, snapshot.lastSeq)).length, 0);
    await second.close();
  } finally {
    await first.close();
    await unlink(filename).catch(() => undefined);
  }
});