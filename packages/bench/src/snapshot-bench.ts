import { MemoryJournal, WorkflowRuntime, projectRun, type NewContinuumEvent } from "@continuum/core";

const ITERATIONS = 300;
const EVENTS = 2000;
const runId = "snapshot-benchmark";

function created(): NewContinuumEvent {
  return {
    runId,
    workflowName: "wf",
    type: "run.created",
    data: {
      workflowName: "wf",
      version: 1,
      input: {},
      steps: [{ name: "work", needs: [], hasCompensation: false, sideEffect: false }],
    },
  };
}

const journal = new MemoryJournal();
await journal.append([created()]);
for (let offset = 0; offset < EVENTS; offset += 250) {
  await journal.append(
    Array.from({ length: 250 }, (_, i) => ({
      runId,
      workflowName: "wf",
      type: "custom" as const,
      data: { note: offset + i },
    })),
  );
}

const events = await journal.events(runId);
const runtime = new WorkflowRuntime({ journal, context: {} });
const snapshot = (await runtime.snapshotRun(runId))!;
const tail = await journal.eventsAfter!(runId, snapshot.lastSeq);

function measure(label: string, fn: () => unknown): number {
  // warm-up
  for (let i = 0; i < 20; i += 1) fn();
  const start = performance.now();
  for (let i = 0; i < ITERATIONS; i += 1) fn();
  const elapsed = performance.now() - start;
  const perOp = elapsed / ITERATIONS;
  console.log(`${label}: ${perOp.toFixed(3)} ms/op (${(1000 / perOp).toFixed(0)} reads/sec)`);
  return perOp;
}

console.log(`Replaying a ${events.length}-event run, ${ITERATIONS} iterations:\n`);
const full = measure("full replay     ", () => projectRun(events));
const resumed = measure("snapshot + tail ", () => projectRun(tail, { base: snapshot }));
console.log(`\nspeedup: ${(full / resumed).toFixed(1)}x faster with a snapshot`);
console.log(`events read: ${events.length} -> ${tail.length}`);