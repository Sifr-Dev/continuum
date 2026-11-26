import assert from "node:assert/strict";
import test from "node:test";
import {
  IntegrityJournal,
  MemoryJournal,
  stableStringify,
  stripIntegrity,
  verifyEventChain,
  type ContinuumEvent,
  type NewContinuumEvent,
} from "../src/index.js";

function event(seq: number, overrides: Partial<NewContinuumEvent> = {}): NewContinuumEvent {
  return {
    runId: "run-1",
    workflowName: "wf",
    type: "step.succeeded",
    data: { output: seq },
    ...overrides,
  } as NewContinuumEvent;
}

test("stableStringify is key-order independent", () => {
  assert.equal(stableStringify({ b: 1, a: 2 }), stableStringify({ a: 2, b: 1 }));
  assert.equal(stableStringify({ a: 2, b: 1 }), '{"a":2,"b":1}');
});

test("stripIntegrity removes only the reserved key", () => {
  const data = { output: 1, __integrity: { i: 0, prev: "x", hash: "y" } };
  assert.deepEqual(stripIntegrity(data), { output: 1 });
});

test("a fresh chain verifies", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  const appended = await journal.append([event(0), event(1), event(2)]);
  const report = await journal.verify();
  assert.deepEqual(report, { ok: true, verified: 3 });
  assert.equal(appended.length, 3);
});

test("tampering with event content is detected", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  const appended = await journal.append([event(0), event(1), event(2)]);
  const tampered = structuredClone(appended) as ContinuumEvent[];
  (tampered[1]!.data as Record<string, unknown>).output = 999;
  const report = verifyEventChain(tampered);
  assert.equal(report.ok, false);
  assert.equal(report.brokenAt, 1);
  assert.match(report.reason ?? "", /content hash mismatch/);
});

test("reordering events is detected", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  const appended = await journal.append([event(0), event(1), event(2)]);
  const reordered = [...appended].reverse();
  const report = verifyEventChain(reordered);
  assert.equal(report.ok, false);
});

test("deleting a middle event is detected", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  const appended = await journal.append([event(0), event(1), event(2), event(3)]);
  const deleted = appended.filter((_, index) => index !== 2);
  const report = verifyEventChain(deleted);
  assert.equal(report.ok, false);
});

test("duplicating an event is detected", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  const appended = await journal.append([event(0), event(1)]);
  const duplicated = [...appended, appended[1]!];
  const report = verifyEventChain(duplicated);
  assert.equal(report.ok, false);
});

test("an event without integrity metadata fails verification", async () => {
  const inner = new MemoryJournal();
  await inner.append([event(0)]);
  const events = await inner.allEvents();
  const report = verifyEventChain(events);
  assert.equal(report.ok, false);
  assert.match(report.reason ?? "", /missing integrity metadata/);
});

test("concurrent appends keep a single valid chain", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  const batches = Array.from({ length: 8 }, (_, batch) =>
    Array.from({ length: 5 }, (_, n) => event(batch * 5 + n, { runId: `run-${batch}` })),
  );
  await Promise.all(batches.map((batch) => journal.append(batch)));
  const report = await journal.verify();
  assert.equal(report.ok, true, report.reason);
  assert.equal(report.verified, 40);

  const all = await journal.allEvents();
  const indices = all.map((e) => (e.data as Record<string, { i: number }>).__integrity.i);
  assert.deepEqual([...indices].sort((a, b) => a - b), Array.from({ length: 40 }, (_, i) => i));
});

test("chain survives a simulated restart (new decorator over same store)", async () => {
  const store = new MemoryJournal();
  const first = new IntegrityJournal(store);
  await first.append([event(0), event(1)]);
  // Simulate process restart: fresh decorator, same underlying store.
  const second = new IntegrityJournal(store);
  const report = await second.verify();
  assert.deepEqual(report, { ok: true, verified: 2 });
  // Appends continue the existing chain rather than restarting it.
  await second.append([event(2)]);
  const continued = await second.verify();
  assert.equal(continued.ok, true, continued.reason);
  assert.equal(continued.verified, 3);
});