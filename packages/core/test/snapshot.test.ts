import assert from "node:assert/strict";
import test from "node:test";
import {
  IntegrityJournal,
  MemoryJournal,
  WorkflowRuntime,
  defineStep,
  defineWorkflow,
  projectRun,
  type ContinuumEvent,
  type ContinuumEventType,
  type NewContinuumEvent,
  type RunSnapshot,
  type WorkflowJournal,
} from "../src/index.js";

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

function custom(runId: string, note: number): NewContinuumEvent {
  return { runId, workflowName: "wf", type: "custom", data: { note } } as NewContinuumEvent;
}

function succeeded(runId: string): NewContinuumEvent {
  return { runId, workflowName: "wf", type: "run.succeeded", data: { output: { ok: true } } } as NewContinuumEvent;
}

async function seededHistory(runId: string, count: number): Promise<{ journal: MemoryJournal; events: ContinuumEvent[] }> {
  const journal = new MemoryJournal();
  await journal.append([created(runId)]);
  await journal.append(Array.from({ length: count }, (_, i) => custom(runId, i)));
  await journal.append([succeeded(runId)]);
  return { journal, events: await journal.events(runId) };
}

test("projectRun with a snapshot base + tail equals a full replay", async () => {
  const { events } = await seededHistory("run-1", 40);
  const cut = 20;
  const snapshot: RunSnapshot = {
    runId: "run-1",
    lastSeq: events[cut]!.seq,
    state: projectRun(events.slice(0, cut + 1))!,
  };
  const full = projectRun(events);
  const resumed = projectRun(events.slice(cut + 1), { base: snapshot });
  assert.deepEqual(resumed, full);
});

test("projectRun does not mutate the caller's snapshot", async () => {
  const { events } = await seededHistory("run-2", 10);
  const snapshot: RunSnapshot = {
    runId: "run-2",
    lastSeq: events[4]!.seq,
    state: projectRun(events.slice(0, 5))!,
  };
  const before = structuredClone(snapshot.state);
  projectRun(events.slice(5), { base: snapshot });
  assert.deepEqual(snapshot.state, before);
});

test("runtime.snapshotRun persists a snapshot and getRun resumes from it", async () => {
  const { journal, events } = await seededHistory("run-3", 30);
  const runtime = new WorkflowRuntime({ journal, context: {} });
  const full = projectRun(events);
  const snapshot = await runtime.snapshotRun("run-3");
  assert.ok(snapshot);
  assert.equal(snapshot.lastSeq, events.at(-1)!.seq);
  assert.deepEqual(await runtime.getRunWithSnapshot("run-3"), full);
});

test("getRun folds only events appended after the snapshot", async () => {
  const { journal, events } = await seededHistory("run-4", 20);
  const runtime = new WorkflowRuntime({ journal, context: {} });
  await runtime.snapshotRun("run-4");
  await journal.append([custom("run-4", 999), succeeded("run-4")]);
  const all = await journal.events("run-4");
  assert.deepEqual(await runtime.getRunWithSnapshot("run-4"), projectRun(all));
  assert.equal(events.length, 22); // sanity: full history is 22, tail is 2
});

test("getRun uses eventsAfter and never reads full history when a snapshot exists", async () => {
  const inner = new MemoryJournal();
  await inner.append([created("run-5"), custom("run-5", 1), custom("run-5", 2)]);
  let fullReads = 0;
  let tailReads = 0;
  const spy: WorkflowJournal = {
    append: (events, options) => inner.append(events, options),
    events: async (runId) => {
      fullReads += 1;
      return inner.events(runId);
    },
    eventsAfter: async (runId, afterSeq) => {
      tailReads += 1;
      return inner.eventsAfter!(runId, afterSeq);
    },
    allEvents: () => inner.allEvents(),
    runIds: () => inner.runIds(),
    saveRunSnapshot: (snapshot) => inner.saveRunSnapshot!(snapshot),
    latestRunSnapshot: (runId) => inner.latestRunSnapshot!(runId),
  };
  const runtime = new WorkflowRuntime({ journal: spy, context: {} });
  await runtime.snapshotRun("run-5");
  const beforeFull = fullReads;
  await runtime.getRunWithSnapshot("run-5");
  assert.equal(fullReads, beforeFull, "getRun must not read the full event history");
  assert.ok(tailReads > 0, "getRun must read the tail via eventsAfter");
});

test("journal keeps the newest snapshot when older ones are written later", async () => {
  const journal = new MemoryJournal();
  await journal.append([created("run-6"), custom("run-6", 1), custom("run-6", 2)]);
  const events = await journal.events("run-6");
  const newest: RunSnapshot = { runId: "run-6", lastSeq: events[2]!.seq, state: projectRun(events.slice(0, 3))! };
  const older: RunSnapshot = { runId: "run-6", lastSeq: events[1]!.seq, state: projectRun(events.slice(0, 2))! };
  await journal.saveRunSnapshot(newest);
  await journal.saveRunSnapshot(older);
  assert.equal((await journal.latestRunSnapshot("run-6"))!.lastSeq, newest.lastSeq);
});

test("snapshotOnTerminal materializes a snapshot automatically", async () => {
  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, snapshotOnTerminal: true });
  const step = defineStep<{ value: number }, number>({ name: "work", execute: async ({ input }) => input.value + 1 });
  const workflow = defineWorkflow<{ value: number }>({ name: "wf", version: 1, steps: [step] });
  const handle = await runtime.runWorkflow(workflow, { value: 1 });
  await handle.result;
  const snapshot = await journal.latestRunSnapshot("run-6");
  // The random run id is discovered from runIds; assert some snapshot exists.
  const runId = (await journal.runIds())[0]!;
  let persisted: RunSnapshot | undefined;
  for (let attempt = 0; attempt < 50; attempt += 1) {
    persisted = await journal.latestRunSnapshot(runId);
    if (persisted) break;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.ok(persisted, "terminal run should have an auto snapshot");
  assert.equal(snapshot, undefined);
  assert.deepEqual(await runtime.getRunWithSnapshot(runId), projectRun(await journal.events(runId)));
});

test("IntegrityJournal forwards snapshot and tail-read methods", async () => {
  const inner = new MemoryJournal();
  await inner.append([created("run-7"), custom("run-7", 1), custom("run-7", 2)]);
  const wrapped = new IntegrityJournal(inner);
  const runtime = new WorkflowRuntime({ journal: wrapped, context: {} });
  const snapshot = await runtime.snapshotRun("run-7");
  assert.ok(snapshot);
  assert.equal((await wrapped.latestRunSnapshot!("run-7"))!.lastSeq, snapshot.lastSeq);
  assert.equal((await wrapped.eventsAfter!("run-7", snapshot.lastSeq)).length, 0);
  assert.deepEqual(await runtime.getRunWithSnapshot("run-7"), projectRun(await inner.events("run-7")));
});