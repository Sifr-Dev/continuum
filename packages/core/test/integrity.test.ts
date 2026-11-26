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
// ---------------------------------------------------------------------------
// Durable chain head: truncation, emptiness, and transactional appends.
// ---------------------------------------------------------------------------

test("deleting the newest event is detected against the durable head", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  await journal.append([event(0), event(1), event(2), event(3)]);
  const all = await journal.allEvents();
  // A tail deletion leaves a perfectly self-consistent chain; only the durable
  // head can prove events are missing.
  const report = verifyEventChain(all.slice(0, 3), { head: await journal.integrityHead!() });
  assert.equal(report.ok, false);
  assert.equal(report.expected, 4);
  assert.match(report.reason ?? "", /truncated/);

  // The untrimmed journal still verifies.
  const full = await journal.verify();
  assert.equal(full.ok, true, full.reason);
  assert.equal(full.verified, 4);
});

test("verify() fails when the newest events are deleted from the store", async () => {
  const inner = new MemoryJournal();
  const journal = new IntegrityJournal(inner);
  await journal.append([event(0), event(1), event(2)]);
  // Simulate an attacker deleting the tail rows directly in the store, leaving
  // the durable head untouched.
  const rows = await inner.allEvents();
  const truncated = new MemoryJournal();
  await truncated.append(rows.slice(0, 2));
  const report = await verifyEventChain(await truncated.allEvents(), { head: await inner.integrityHead!() });
  assert.equal(report.ok, false);
  assert.equal(report.verified, 0);
  assert.equal(report.expected, 3);
  assert.match(report.reason ?? "", /truncated/);
});

test("an emptied journal is a truncation failure, not a valid chain", async () => {
  const inner = new MemoryJournal();
  const journal = new IntegrityJournal(inner);
  await journal.append([event(0), event(1)]);
  const head = await inner.integrityHead!();
  const report = await verifyEventChain([], { head });
  assert.equal(report.ok, false);
  assert.equal(report.empty, true);
  assert.equal(report.verified, 0);
  assert.match(report.reason ?? "", /truncated/);
});

test("an empty journal is reported as empty, never as a verified chain", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  const report = await journal.verify();
  assert.equal(report.ok, true);
  assert.equal(report.empty, true);
  assert.equal(report.verified, 0);
  // MemoryJournal never sealed a head, so there is no expected count to check:
  // the report says it verified nothing instead of implying a checked history.
  assert.equal(report.expected, undefined);
  assert.match(report.reason ?? "", /nothing was verified/);

  // A journal with no durable head at all reports that it verified nothing
  // rather than implying the history was checked.
  const headless = verifyEventChain([]);
  assert.equal(headless.ok, true);
  assert.equal(headless.empty, true);
  assert.equal(headless.verified, 0);
  assert.match(headless.reason ?? "", /nothing was verified/);
});

test("appending to a truncated store is refused instead of forking the chain", async () => {
  const inner = new MemoryJournal();
  const journal = new IntegrityJournal(inner);
  await journal.append([event(0), event(1), event(2)]);

  // Rewrite the store so its last event no longer matches the durable head.
  const rows = await inner.allEvents();
  const forged = new MemoryJournal();
  await forged.append(rows.slice(0, 1));
  await forged.transaction(async (tx) => {
    await tx.writeIntegrityHead(await inner.integrityHead!());
  });
  const reopened = new IntegrityJournal(forged);
  await assert.rejects(() => reopened.append([event(9)]), /truncated|replaced/);
  assert.equal((await forged.allEvents()).length, 1);
});

test("code cannot append pre-stamped events or nest decorators", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  const stamped = { ...event(0), data: { output: 0, __integrity: { i: 0, prev: "x", hash: "y" } } };
  await assert.rejects(() => journal.append([stamped as NewContinuumEvent]), /already contains an integrity link/);

  const nested = new IntegrityJournal(journal);
  await assert.rejects(() => nested.append([event(0)]), /already contains an integrity link/);
  assert.equal((await journal.allEvents()).length, 0);
});

test("a failed transactional append leaves no chain state behind", async () => {
  const inner = new MemoryJournal();
  const journal = new IntegrityJournal(inner);
  await journal.append([event(0)]);
  const before = await inner.integrityHead!();

  // A duplicate run.created inside the batch aborts the whole transaction.
  const created = (n: number): NewContinuumEvent => ({
    runId: "run-dup",
    workflowName: "wf",
    type: "run.created",
    data: { n },
  });
  await assert.rejects(() => journal.append([created(1), created(2)]), /already exists/);
  assert.deepEqual(await inner.integrityHead!(), before);
  assert.equal((await journal.allEvents()).length, 1);
  const report = await journal.verify();
  assert.equal(report.ok, true, report.reason);
  assert.equal(report.verified, 1);
});

test("verification rejects a head that does not match the final link", async () => {
  const journal = new IntegrityJournal(new MemoryJournal());
  await journal.append([event(0), event(1)]);
  const head = await journal.integrityHead!();
  const report = verifyEventChain(await journal.allEvents(), {
    head: { counter: head!.counter, lastHash: "0".repeat(64) },
  });
  assert.equal(report.ok, false);
  assert.match(report.reason ?? "", /does not match the durable chain head/);
});

test("transactions serialize concurrent appends into one contiguous chain", async () => {
  const inner = new MemoryJournal();
  const journal = new IntegrityJournal(inner);
  await Promise.all(
    Array.from({ length: 10 }, (_, batch) => journal.append([event(batch, { runId: `run-${batch}` })])),
  );
  assert.equal((await inner.integrityHead!())!.counter, 10);
  const report = await journal.verify();
  assert.equal(report.ok, true, report.reason);
  assert.equal(report.verified, 10);
});
