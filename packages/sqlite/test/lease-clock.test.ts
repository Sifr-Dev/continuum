import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import Database from "better-sqlite3";
import { SqliteJournal } from "../src/index.js";

const __dirname = dirname(new URL(import.meta.url).pathname);

function withTempJournal(run: (filename: string) => Promise<void>): Promise<void> {
  const directory = mkdtempSync(join(tmpdir(), "continuum-lease-clock-"));
  return (async () => {
    try {
      await run(join(directory, "events.db"));
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  })();
}

/**
 * Runs `body` with this process's wall clock shifted by `offsetMs`. Only
 * `Date.now` is patched, which is exactly the input the old lease code trusted
 * for expiry decisions.
 */
async function withSkewedClock<T>(offsetMs: number, body: () => Promise<T>): Promise<T> {
  const realNow = Date.now;
  Date.now = () => realNow() + offsetMs;
  try {
    return await body();
  } finally {
    Date.now = realNow;
  }
}

test("a worker with a fast clock cannot steal a live lease", async () => {
  await withTempJournal(async (filename) => {
    const owner = new SqliteJournal(filename);
    const skewed = new SqliteJournal(filename);
    try {
      const lease = await owner.acquireRunLease("run", "owner", 60_000);
      assert.equal(lease?.fencingToken, 1);

      // Five minutes ahead of the database: far beyond the 60s TTL.
      const stolen = await withSkewedClock(5 * 60_000, () => skewed.acquireRunLease("run", "skewed", 60_000));
      assert.equal(stolen, undefined, "a live lease must not be taken over because the caller's clock is fast");

      // The original owner keeps writing, which proves ownership was retained.
      const appended = await owner.append(
        [{ runId: "run", workflowName: "workflow", type: "custom", data: { by: "owner" } }],
        { lease },
      );
      assert.equal(appended.length, 1);
      assert.equal((await skewed.listRunLeases()).find((row) => row.runId === "run")?.workerId, "owner");
    } finally {
      await owner.close();
      await skewed.close();
    }
  });
});

test("a worker with a slow clock does not hold an expired lease forever", async () => {
  await withTempJournal(async (filename) => {
    const owner = new SqliteJournal(filename);
    const successor = new SqliteJournal(filename);
    try {
      // A short TTL that has already elapsed by the time the successor tries.
      const lease = await owner.acquireRunLease("run", "owner", 1);
      assert.ok(lease);
      await new Promise((resolve) => setTimeout(resolve, 25));

      // The old owner believes only 25ms of a 1ms lease has passed, but expiry
      // is evaluated by the database, so the successor must still win.
      const adopted = await withSkewedClock(-60 * 60_000, () => successor.acquireRunLease("run", "successor", 60_000));
      assert.equal(adopted?.workerId, "successor");
      assert.ok((adopted?.fencingToken ?? 0) > lease.fencingToken);

      // Fencing still rejects the superseded owner.
      await assert.rejects(
        () => owner.append([{ runId: "run", workflowName: "workflow", type: "custom", data: {} }], { lease }),
        /lease.*lost/i,
      );
    } finally {
      await owner.close();
      await successor.close();
    }
  });
});

test("acquisition reports expiresAt on the database clock", async () => {
  await withTempJournal(async (filename) => {
    const journal = new SqliteJournal(filename);
    try {
      const before = Date.now();
      const lease = await journal.acquireRunLease("run", "worker", 30_000);
      const after = Date.now();
      assert.ok(lease);
      // TTL is applied to the database clock; the value must still land in the
      // expected window when both clocks agree.
      assert.ok(lease.expiresAt >= before + 30_000 - 1_000, `expiresAt ${lease.expiresAt} too early`);
      assert.ok(lease.expiresAt <= after + 30_000 + 1_000, `expiresAt ${lease.expiresAt} too late`);
    } finally {
      await journal.close();
    }
  });
});

test("renewal extends expiry on the database clock", async () => {
  await withTempJournal(async (filename) => {
    const journal = new SqliteJournal(filename);
    try {
      const lease = await journal.acquireRunLease("run", "worker", 5_000);
      assert.ok(lease);
      const renewed = await withSkewedClock(60 * 60_000, () => journal.renewRunLease(lease, 120_000));
      assert.equal(renewed, true);
      const stored = (await journal.listRunLeases()).find((row) => row.runId === "run");
      assert.ok(stored);
      // A skewed renewal must not write a horizon based on the skewed clock.
      assert.ok(
        stored.expiresAt < Date.now() + 180_000,
        `renewal stored a skewed horizon: ${stored.expiresAt - Date.now()}ms ahead`,
      );
      assert.ok(stored.expiresAt > Date.now() + 60_000, "renewal still extends the lease");
    } finally {
      await journal.close();
    }
  });
});

test("a failed write leaves the connection usable instead of a dangling transaction", async () => {
  await withTempJournal(async (filename) => {
    const journal = new SqliteJournal({ filename, busyTimeoutMs: 0 });
    try {
      // A BigInt payload fails JSON serialization inside the immediate write
      // transaction, after BEGIN IMMEDIATE has succeeded.
      await assert.rejects(() =>
        journal.append([
          { runId: "run", workflowName: "workflow", type: "custom", data: { big: 1n as unknown as number } },
        ]),
      );

      // The rollback path must leave the connection usable, with nothing
      // partially written.
      assert.deepEqual(await journal.events("run"), []);
      const appended = await journal.append([
        { runId: "run", workflowName: "workflow", type: "custom", data: { ok: true } },
      ]);
      assert.equal(appended.length, 1);
    } finally {
      await journal.close();
    }
  });
});

test("a disk-full failure surfaces SQLITE_FULL rather than a rollback error", async () => {
  await withTempJournal(async (filename) => {
    // A shared connection lets the test tighten the page cap. This matters
    // because SQLITE_FULL rolls the transaction back automatically: a second
    // unconditional ROLLBACK then raises SQLITE_ERROR "cannot rollback - no
    // transaction is active" and hides the real storage failure.
    const database = new Database(filename);
    database.pragma("page_size = 512");
    const journal = new SqliteJournal({ filename, database });
    try {
      await journal.append([{ runId: "seed", workflowName: "workflow", type: "custom", data: { seed: true } }]);
      const pages = database.pragma("page_count", { simple: true });
      database.pragma(`max_page_count = ${Number(pages) + 1}`);

      await assert.rejects(
        () =>
          journal.append([
            { runId: "run", workflowName: "workflow", type: "custom", data: { big: "y".repeat(500_000) } },
          ]),
        (error: NodeJS.ErrnoException) => {
          assert.equal(
            error.code,
            "SQLITE_FULL",
            `expected SQLITE_FULL to survive rollback handling, got ${error.code}: ${error.message}`,
          );
          return true;
        },
      );

      // The connection must still work after the failed transaction.
      database.pragma("max_page_count = 1000");
      const appended = await journal.append([
        { runId: "after", workflowName: "workflow", type: "custom", data: { ok: true } },
      ]);
      assert.equal(appended.length, 1);
    } finally {
      await journal.close();
      database.close();
    }
  });
});

test("waits for another process's write lock instead of failing immediately", async () => {
  await withTempJournal(async (filename) => {
    // The default busy timeout is 5s. A deferred read-then-write transaction
    // takes a read snapshot and then fails with SQLITE_BUSY the moment that
    // snapshot goes stale, ignoring the timeout entirely. The lock must be held
    // by another process: holding it in this process would block the event loop.
    const journal = new SqliteJournal(filename);
    try {
      await journal.acquireRunLease("run", "seed", 1);
      await new Promise((resolve) => setTimeout(resolve, 25));

      const holder = spawn(process.execPath, [join(__dirname, "lock-holder.cjs"), filename, "250", "run"], {
        stdio: ["ignore", "pipe", "inherit"],
      });
      await new Promise<void>((resolve, reject) => {
        holder.stdout.on("data", (chunk: Buffer) => {
          if (chunk.toString().includes("locked")) resolve();
        });
        holder.on("error", reject);
        holder.on("exit", (code) => reject(new Error(`lock holder exited early with ${code}`)));
      });

      const started = Date.now();
      const lease = await journal.acquireRunLease("run", "challenger", 30_000);
      const waited = Date.now() - started;

      assert.equal(lease?.workerId, "challenger", "the waiting writer must acquire once the lock is released");
      assert.equal(lease?.fencingToken, 2);
      assert.ok(waited >= 100, `expected to wait for the lock, returned after ${waited}ms`);

      await new Promise<void>((resolve) => holder.on("exit", () => resolve()));
    } finally {
      await journal.close();
    }
  });
});

test("busyTimeoutMs bounds how long a blocked acquisition waits", async () => {
  await withTempJournal(async (filename) => {
    const journal = new SqliteJournal({ filename, busyTimeoutMs: 0 });
    const holder = new Database(filename);
    try {
      holder.exec("BEGIN IMMEDIATE");
      holder.prepare("INSERT INTO continuum_runs (run_id, created_at) VALUES (?, ?)").run("blocker", Date.now());

      const started = Date.now();
      await assert.rejects(() => journal.acquireRunLease("run", "worker", 5_000), /locked/i);
      const waited = Date.now() - started;
      assert.ok(waited < 500, `busyTimeoutMs: 0 should fail fast, waited ${waited}ms`);
    } finally {
      if (holder.inTransaction) holder.exec("ROLLBACK");
      holder.close();
      await journal.close();
    }
  });
});

test("a supplied connection keeps its own settings and is not closed", async () => {
  await withTempJournal(async (filename) => {
    const database = new Database(filename);
    database.pragma("busy_timeout = 17");
    const journal = new SqliteJournal({ filename, database });
    try {
      // The caller's connection tuning must survive journal construction.
      assert.equal(database.pragma("busy_timeout", { simple: true }), 17);
      await journal.acquireRunLease("run", "worker", 1_000);
    } finally {
      await journal.close();
    }
    // close() must not close a connection the journal does not own.
    assert.doesNotThrow(() => database.prepare("SELECT 1").get());
    database.close();
  });
});

test("concurrent acquisitions across connections have exactly one winner", async () => {
  await withTempJournal(async (filename) => {
    const journals = Array.from({ length: 6 }, () => new SqliteJournal(filename));
    try {
      for (const journal of journals) {
        await journal.acquireRunLease("warmup", "warmup", 1);
      }
      // Expire the warmup lease so the race below starts from a contended row.
      await new Promise((resolve) => setTimeout(resolve, 20));

      const results = await Promise.all(
        journals.map((journal, index) => journal.acquireRunLease("contended", `worker-${index}`, 30_000)),
      );
      const winners = results.filter((lease) => lease !== undefined);
      assert.equal(winners.length, 1, `expected one winner, got ${winners.length}`);
      assert.equal(winners[0]?.fencingToken, 1);

      const second = await Promise.all(
        journals.map((journal, index) => journal.acquireRunLease("contended", `later-${index}`, 30_000)),
      );
      assert.equal(second.filter((lease) => lease !== undefined).length, 0);
    } finally {
      await Promise.all(journals.map((journal) => journal.close()));
    }
  });
});
