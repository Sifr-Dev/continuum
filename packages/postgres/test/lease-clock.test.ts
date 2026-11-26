import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { PostgresJournal } from "../src/index.js";

/**
 * These tests require a PostgreSQL server and are skipped without it:
 *
 *   CONTINUUM_POSTGRES_TEST_URL=postgres://user:pass@host:5432/db \
 *     npm test --workspace @continuum/postgres
 *
 * CI provides the server (see .github/workflows/ci.yml); there is no
 * in-process substitute for PostgreSQL's statement clock.
 */
const connectionString = process.env.CONTINUUM_POSTGRES_TEST_URL;
const postgresTest = connectionString ? test : test.skip;

/** Shifts only this process's `Date.now`, the clock the old lease code trusted. */
async function withSkewedClock<T>(offsetMs: number, body: () => Promise<T>): Promise<T> {
  const realNow = Date.now;
  Date.now = () => realNow() + offsetMs;
  try {
    return await body();
  } finally {
    Date.now = realNow;
  }
}

postgresTest("a worker with a fast clock cannot steal a live lease", async () => {
  const owner = new PostgresJournal(connectionString as string);
  const skewed = new PostgresJournal(connectionString as string);
  const runId = `skew-fast-${randomUUID()}`;
  try {
    const lease = await owner.acquireRunLease(runId, "owner", 60_000);
    assert.equal(lease?.fencingToken, 1);

    // Five minutes ahead of the database clock: far beyond the 60s TTL.
    const stolen = await withSkewedClock(5 * 60_000, () =>
      skewed.acquireRunLease(runId, "skewed", 60_000),
    );
    assert.equal(stolen, undefined, "a live lease must not be taken over because the caller's clock is fast");

    const appended = await owner.append(
      [{ runId, workflowName: "workflow", type: "custom", data: { by: "owner" } }],
      { lease },
    );
    assert.equal(appended.length, 1);
    assert.equal((await skewed.listRunLeases()).find((row) => row.runId === runId)?.workerId, "owner");
  } finally {
    await owner.close();
    await skewed.close();
  }
});

postgresTest("a worker with a slow clock does not hold an expired lease forever", async () => {
  const owner = new PostgresJournal(connectionString as string);
  const successor = new PostgresJournal(connectionString as string);
  const runId = `skew-slow-${randomUUID()}`;
  try {
    const lease = await owner.acquireRunLease(runId, "owner", 1);
    assert.ok(lease);
    await new Promise((resolve) => setTimeout(resolve, 25));

    // The old owner believes only 25ms of a 1ms lease has elapsed, but expiry
    // is computed by the database, so the successor must still win.
    const adopted = await withSkewedClock(-60 * 60_000, () =>
      successor.acquireRunLease(runId, "successor", 60_000),
    );
    assert.equal(adopted?.workerId, "successor");
    assert.ok((adopted?.fencingToken ?? 0) > lease.fencingToken);

    await assert.rejects(
      () => owner.append([{ runId, workflowName: "workflow", type: "custom", data: {} }], { lease }),
      /lease.*lost/i,
    );
  } finally {
    await owner.close();
    await successor.close();
  }
});

postgresTest("lease expiry uses the database clock, not the client clock", async () => {
  const journal = new PostgresJournal(connectionString as string);
  const runId = `clock-authority-${randomUUID()}`;
  try {
    // Acquire under a clock an hour behind the database. If the adapter mixed
    // the client clock into expires_at, the stored horizon would be an hour in
    // the past and the lease would not be live.
    const lease = await withSkewedClock(-60 * 60_000, () => journal.acquireRunLease(runId, "worker", 60_000));
    assert.ok(lease, "lease should be acquired");
    assert.ok(
      lease.expiresAt > Date.now() + 30_000,
      `expiresAt should be a database-clock horizon, got ${lease.expiresAt}`,
    );

    // A second worker must be denied while that lease is live, even though the
    // client clock says an hour has passed.
    const denied = await withSkewedClock(60 * 60_000, () => journal.acquireRunLease(runId, "other", 60_000));
    assert.equal(denied, undefined);

    // Renewal writes a database-clock horizon too.
    const renewed = await withSkewedClock(60 * 60_000, () => journal.renewRunLease(lease, 120_000));
    assert.equal(renewed, true);
    const stored = (await journal.listRunLeases()).find((row) => row.runId === runId);
    assert.ok(stored);
    assert.ok(
      stored.expiresAt < Date.now() + 180_000,
      `renewal stored a skewed horizon: ${stored.expiresAt - Date.now()}ms ahead`,
    );
    assert.ok(stored.expiresAt > Date.now() + 60_000, "renewal still extends the lease");
  } finally {
    await journal.close();
  }
});
