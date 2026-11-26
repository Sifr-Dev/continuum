import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { IntegrityJournal, RunAlreadyExistsError, verifyEventChain } from "@continuum/core";
import { SqliteJournal } from "../src/index.js";

const testDir = dirname(new URL(import.meta.url).pathname);
const writerScript = join(testDir, "integrity-writer.ts");
/** Repo-local tsx, so the fixture compiles exactly like the test that spawns it. */
const tsxBin = join(testDir, "../../../node_modules/.bin/tsx");

function withTempDb(run: (filename: string) => Promise<void>): Promise<void> {
  const directory = mkdtempSync(join(tmpdir(), "continuum-integrity-"));
  return (async () => {
    try {
      await run(join(directory, "events.db"));
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  })();
}

function custom(runId: string, n: number, writer = "test") {
  return {
    runId,
    workflowName: "integrity",
    type: "custom" as const,
    data: { writer, n },
  };
}

/**
 * Runs the writer fixture in a separate OS process against the same database
 * file. Two Node processes cannot share a better-sqlite3 handle, so this is the
 * only honest way to prove cross-process chain extension.
 */
function runWriterProcess(
  filename: string,
  writerId: string,
  batches: number,
  perBatch: number,
): Promise<{ ok: boolean; appended: number; writer: string; error?: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(tsxBin, [writerScript, filename, writerId, String(batches), String(perBatch)], {
      cwd: testDir,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("error", reject);
    child.on("exit", (code) => {
      const line = stdout.trim().split("\n").at(-1) ?? "";
      try {
        resolve(JSON.parse(line));
      } catch {
        reject(new Error(`writer process exited ${code} without JSON output\nstdout: ${stdout}\nstderr: ${stderr}`));
      }
    });
  });
}

test("two decorator instances over one SQLite file extend a single chain", async () => {
  await withTempDb(async (filename) => {
    const first = new IntegrityJournal(new SqliteJournal(filename));
    const second = new IntegrityJournal(new SqliteJournal(filename));

    // Interleave appends: with in-memory counters this is exactly the shape
    // that forks the chain, because each decorator believes it owns link 0.
    const interleaved: Promise<unknown>[] = [];
    for (let round = 0; round < 10; round += 1) {
      interleaved.push(first.append([custom("run-a", round, "a")]));
      interleaved.push(second.append([custom("run-b", round, "b")]));
    }
    await Promise.all(interleaved);

    const report = await first.verify();
    assert.equal(report.ok, true, report.reason);
    assert.equal(report.verified, 20);

    // Both decorators agree on the same durable head.
    assert.deepEqual(await first.integrityHead!(), await second.integrityHead!());

    const events = await first.allEvents();
    const indices = events.map((e) => (e.data as Record<string, { i: number }>).__integrity.i);
    assert.deepEqual(indices, Array.from({ length: 20 }, (_, i) => i));

    await first.close?.();
    await second.close?.();
  });
});

test("a second OS process continues the chain instead of forking it", async () => {
  await withTempDb(async (filename) => {
    const parent = new IntegrityJournal(new SqliteJournal(filename));
    await parent.append([custom("run-parent", 0, "parent"), custom("run-parent", 1, "parent")]);
    assert.equal((await parent.integrityHead!())!.counter, 2);
    // Release the handle before another process writes: two writers are fine,
    // but this test is about a hand-off, not contention.
    await parent.close?.();

    const child = await runWriterProcess(filename, "child", 5, 4);
    assert.equal(child.ok, true, child.error);
    assert.equal(child.appended, 20);

    // A fresh decorator in this process adopts the child's work and keeps going.
    const reopened = new IntegrityJournal(new SqliteJournal(filename));
    const report = await reopened.verify();
    assert.equal(report.ok, true, report.reason);
    assert.equal(report.verified, 22);

    await reopened.append([custom("run-parent", 2, "parent")]);
    assert.equal((await reopened.integrityHead!())!.counter, 23);
    assert.equal((await reopened.verify()).ok, true);
    await reopened.close?.();

    const direct = new SqliteJournal(filename);
    const head = await direct.integrityHead!();
    assert.deepEqual(head, { counter: 23, lastHash: (await direct.allEvents()).at(-1)!.data.__integrity.hash });
    await direct.close();
  });
});

test("two OS processes writing the same file concurrently keep one valid chain", async () => {
  await withTempDb(async (filename) => {
    const [left, right] = await Promise.all([
      runWriterProcess(filename, "left", 10, 3),
      runWriterProcess(filename, "right", 10, 3),
    ]);
    assert.equal(left.ok, true, left.error);
    assert.equal(right.ok, true, right.error);

    const journal = new SqliteJournal(filename);
    const events = await journal.allEvents();
    assert.equal(events.length, 60);

    const head = await journal.integrityHead!();
    const report = verifyEventChain(events, { head });
    assert.equal(report.ok, true, report.reason);
    assert.equal(report.verified, 60);
    assert.equal(head!.counter, 60);

    const indices = events.map((e) => (e.data as Record<string, { i: number }>).__integrity.i);
    assert.deepEqual(indices, Array.from({ length: 60 }, (_, i) => i));
    await journal.close();
  });
});

test("deleting the newest rows is detected through the durable SQLite head", async () => {
  await withTempDb(async (filename) => {
    const journal = new IntegrityJournal(new SqliteJournal(filename));
    await journal.append([custom("run-t", 0), custom("run-t", 1), custom("run-t", 2)]);
    assert.equal((await journal.verify()).ok, true);
    const head = await journal.integrityHead!();
    await journal.close?.();

    // Delete the tail directly, as an attacker with database access would.
    const raw = new SqliteJournal(filename);
    await raw.close();
    const { default: Database } = await import("better-sqlite3");
    const db = new Database(filename);
    db.prepare("DELETE FROM continuum_events WHERE seq = (SELECT MAX(seq) FROM continuum_events)").run();
    db.close();

    const reopened = new IntegrityJournal(new SqliteJournal(filename));
    const report = await reopened.verify();
    assert.equal(report.ok, false);
    assert.equal(report.expected, head!.counter);
    assert.match(report.reason ?? "", /truncated/);
    // Relocating to a store with the matching head would fork the chain, so
    // extending it must fail loudly instead.
    await assert.rejects(() => reopened.append([custom("run-t", 3)]), /truncated|replaced/);
    await reopened.close?.();
  });
});

test("two processes racing one run id produce exactly one run.created", async () => {
  await withTempDb(async (filename) => {
    const left = new IntegrityJournal(new SqliteJournal(filename, { busyTimeoutMs: 10_000 }));
    const right = new IntegrityJournal(new SqliteJournal(filename, { busyTimeoutMs: 10_000 }));
    const created = (writer: string) => ({
      runId: "shared-run",
      workflowName: "integrity",
      type: "run.created" as const,
      data: { workflowName: "integrity", version: 1, input: {}, steps: [], writer },
    });

    const results = await Promise.allSettled([left.append([created("left")]), right.append([created("right")])]);
    const fulfilled = results.filter((r) => r.status === "fulfilled");
    assert.equal(fulfilled.length, 1);

    const rejected = results.find((r) => r.status === "rejected");
    assert.ok(rejected && rejected.status === "rejected");
    assert.ok(rejected.reason instanceof RunAlreadyExistsError || /already exists/.test(String(rejected.reason)));

    const events = await left.allEvents();
    assert.equal(events.filter((e) => e.type === "run.created").length, 1);
    const report = await left.verify();
    assert.equal(report.ok, true, report.reason);
    assert.equal(report.verified, 1);
    await left.close?.();
    await right.close?.();
  });
});

test("a rejected duplicate append rolls back its chain head advance", async () => {
  await withTempDb(async (filename) => {
    const journal = new IntegrityJournal(new SqliteJournal(filename));
    await journal.append([
      {
        runId: "dup-run",
        workflowName: "integrity",
        type: "run.created",
        data: { workflowName: "integrity", version: 1, input: {}, steps: [] },
      },
    ]);
    const before = await journal.integrityHead!();

    await assert.rejects(
      () =>
        journal.append([
          {
            runId: "dup-run",
            workflowName: "integrity",
            type: "run.created",
            data: { workflowName: "integrity", version: 1, input: {}, steps: [] },
          },
        ]),
      /already exists/,
    );

    assert.deepEqual(await journal.integrityHead!(), before);
    assert.equal((await journal.allEvents()).length, 1);
    assert.equal((await journal.verify()).ok, true);
    await journal.close?.();
  });
});

test("an interrupted transaction leaves neither events nor a head advance", async () => {
  await withTempDb(async (filename) => {
    const journal = new SqliteJournal(filename);
    const integrity = new IntegrityJournal(journal);
    await integrity.append([custom("run-i", 0)]);
    const before = await journal.integrityHead!();

    await assert.rejects(
      () =>
        journal.transaction(async (tx) => {
          await tx.appendEvents([custom("run-i", 1)]);
          throw new Error("boom after append");
        }),
      /boom/,
    );

    assert.deepEqual(await journal.integrityHead!(), before);
    assert.equal((await journal.allEvents()).length, 1);
    assert.equal((await integrity.verify()).ok, true);
    await journal.close();
  });
});

test("fenced appends still reject superseded workers inside a chain transaction", async () => {
  await withTempDb(async (filename) => {
    const journal = new IntegrityJournal(new SqliteJournal(filename));
    const first = (await journal.acquireRunLease!("run-fenced", "first", 5_000))!;
    await journal.append([custom("run-fenced", 0, "first")], { lease: first });
    await journal.releaseRunLease!(first);
    const second = (await journal.acquireRunLease!("run-fenced", "second", 5_000))!;

    await assert.rejects(
      () => journal.append([custom("run-fenced", 1, "zombie")], { lease: first }),
      /lease.*lost/i,
    );
    assert.equal((await journal.allEvents()).filter((e) => e.data.writer === "zombie").length, 0);
    assert.equal((await journal.verify()).ok, true);

    await journal.append([custom("run-fenced", 2, "second")], { lease: second });
    const report = await journal.verify();
    assert.equal(report.ok, true, report.reason);
    assert.equal(report.verified, 2);
    await journal.close?.();
  });
});

test("plain appends and chain transactions in one process do not deadlock", async () => {
  await withTempDb(async (filename) => {
    // better-sqlite3 is synchronous and one connection cannot nest transactions.
    // A plain append issued while a chain transaction on the same file is open
    // must therefore queue behind the in-process file lock, not busy-wait (which
    // would stall the event loop the transaction needs) and not attempt a
    // nested BEGIN. The gate below holds the transaction open across an await,
    // which is the exact window the lock has to cover.
    const journal = new SqliteJournal(filename);
    let releaseTransaction!: () => void;
    const gate = new Promise<void>((resolve) => {
      releaseTransaction = resolve;
    });
    let inside!: () => void;
    const transactionOpened = new Promise<void>((resolve) => {
      inside = resolve;
    });

    const transaction = journal.transaction(async (tx) => {
      await tx.appendEvents([custom("run-mixed", 0)]);
      inside();
      await gate;
      await tx.writeIntegrityHead({ counter: 1, lastHash: "a".repeat(64) });
    });

    await transactionOpened;
    const plain = journal.append([custom("run-mixed", 1)]);
    releaseTransaction();
    await transaction;
    const appended = await plain;

    assert.equal(appended.length, 1);
    assert.equal((await journal.allEvents()).length, 2);
    await journal.close();
  });
});
