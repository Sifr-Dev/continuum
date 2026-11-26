import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import test from "node:test";
import pg from "pg";
import { IntegrityJournal, RunAlreadyExistsError, verifyEventChain } from "@continuum/core";
import { PostgresJournal } from "../src/index.js";

const connectionString = process.env.CONTINUUM_POSTGRES_TEST_URL;
const postgresTest = connectionString ? test : test.skip;
const testDir = dirname(new URL(import.meta.url).pathname);
/** Repo-local tsx, so the fixture compiles exactly like the test that spawns it. */
const tsxBin = join(testDir, "../../../node_modules/.bin/tsx");

/**
 * The chain is global to a database, and other test files append raw
 * (unstamped) events to the shared test database — which would look like
 * "history rewritten outside the chain". These tests therefore run in their own
 * schema, on a private search_path, so the chain they exercise is their own.
 */
export const INTEGRITY_SCHEMA = "continuum_integrity_it";

/** Pool config that puts every connection in the isolated schema. */
function isolated(): { connectionString: string; options: string } {
  return { connectionString: connectionString as string, options: `-c search_path=${INTEGRITY_SCHEMA}` };
}

async function prepareSchema(): Promise<void> {
  const admin = new pg.Client({ connectionString });
  await admin.connect();
  try {
    await admin.query(`DROP SCHEMA IF EXISTS ${INTEGRITY_SCHEMA} CASCADE`);
    await admin.query(`CREATE SCHEMA ${INTEGRITY_SCHEMA}`);
  } finally {
    await admin.end();
  }
}

function newJournal(): PostgresJournal {
  return new PostgresJournal(isolated());
}

function custom(runId: string, n: number, writer = "test") {
  return { runId, workflowName: "integrity", type: "custom" as const, data: { writer, n } };
}

/**
 * Older schemas (and databases written before the durable head existed) must
 * keep working: the head is seeded from the last journaled link, never
 * overwritten, and verification then covers the pre-existing tail.
 */
postgresTest("migration seeds the durable head from an existing stamped tail", async () => {
  await prepareSchema();
  const journal = newJournal();
  try {
    const runId = `seed-${randomUUID()}`;
    const integrity = new IntegrityJournal(journal);
    await integrity.append([custom(runId, 0), custom(runId, 1)]);
    const head = await journal.integrityHead!();
    assert.equal(head?.counter, 2);

    // Re-running migration (a second journal instance) must not reset it.
    const second = newJournal();
    try {
      assert.deepEqual(await second.integrityHead!(), head);
      const events = await second.allEvents();
      assert.equal(verifyEventChain(events, { head: await second.integrityHead!() }).ok, true);
    } finally {
      await second.close();
    }
  } finally {
    await journal.close();
  }
});

postgresTest("two journal instances extend one chain instead of forking it", async () => {
  await prepareSchema();
  const left = newJournal();
  const right = newJournal();
  try {
    const a = new IntegrityJournal(left);
    const b = new IntegrityJournal(right);
    await Promise.all(
      Array.from({ length: 12 }, (_, round) => {
        const runA = `fork-a-${round}`;
        const runB = `fork-b-${round}`;
        return Promise.all([a.append([custom(runA, round, "a")]), b.append([custom(runB, round, "b")])]);
      }),
    );

    const head = await left.integrityHead!();
    const events = await left.allEvents();
    const report = verifyEventChain(events, { head });
    assert.equal(report.ok, true, report.reason);
    assert.equal(head!.counter, 24);

    const indices = events.map((e) => (e.data as Record<string, { i: number }>).__integrity.i);
    assert.deepEqual(indices, Array.from({ length: 24 }, (_, i) => i));
  } finally {
    await left.close();
    await right.close();
  }
});

postgresTest("deleting the newest rows is detected through the durable Postgres head", async () => {
  await prepareSchema();
  const journal = newJournal();
  // The admin client must delete in the isolated schema, not the shared one.
  const admin = new pg.Client(isolated());
  await admin.connect();
  let runId = "";
  try {
    const integrity = new IntegrityJournal(journal);
    runId = `truncate-${randomUUID()}`;
    await integrity.append([custom(runId, 0), custom(runId, 1), custom(runId, 2)]);
    assert.equal((await integrity.verify()).ok, true);
    const head = await journal.integrityHead!();

    // Delete the tail of THIS run only; the chain is global, so deletion of the
    // last global event is what matters. Delete the global maximum instead.
    const last = await journal.allEvents();
    const lastSeq = last.at(-1)!.seq;
    await admin.query("DELETE FROM continuum_events WHERE seq = $1", [lastSeq]);

    const report = await integrity.verify();
    assert.equal(report.ok, false);
    assert.equal(report.expected, head!.counter);
    assert.match(report.reason ?? "", /truncated/);

    // Extending the truncated store must fail rather than fork the chain.
    await assert.rejects(() => integrity.append([custom(runId, 3)]), /truncated|replaced/);
  } finally {
    await journal.close();
    await admin.end();
  }
});

postgresTest("racing writers on one run id produce exactly one run.created", async () => {
  await prepareSchema();
  const left = newJournal();
  const right = newJournal();
  try {
    const a = new IntegrityJournal(left);
    const b = new IntegrityJournal(right);
    const runId = `dup-${randomUUID()}`;
    const created = (writer: string) => ({
      runId,
      workflowName: "integrity",
      type: "run.created" as const,
      data: { workflowName: "integrity", version: 1, input: {}, steps: [], writer },
    });

    const results = await Promise.allSettled([a.append([created("a")]), b.append([created("b")])]);
    assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
    const rejected = results.find((r) => r.status === "rejected");
    assert.ok(rejected && rejected.status === "rejected");
    assert.ok(rejected.reason instanceof RunAlreadyExistsError || /already exists/.test(String(rejected.reason)));

    const events = await left.events(runId);
    assert.equal(events.filter((event) => event.type === "run.created").length, 1);
    assert.equal((await a.verify()).ok, true);
  } finally {
    await left.close();
    await right.close();
  }
});

postgresTest("a rejected append rolls back its chain head advance", async () => {
  await prepareSchema();
  const journal = newJournal();
  try {
    const integrity = new IntegrityJournal(journal);
    const runId = `rollback-${randomUUID()}`;
    await integrity.append([custom(runId, 0)]);
    const before = await journal.integrityHead!();

    await assert.rejects(
      () =>
        journal.transaction(async (tx) => {
          await tx.appendEvents([custom(runId, 1)]);
          throw new Error("boom after append");
        }),
      /boom/,
    );
    assert.deepEqual(await journal.integrityHead!(), before);
    assert.equal((await journal.events(runId)).length, 1);
    assert.equal((await integrity.verify()).ok, true);
  } finally {
    await journal.close();
  }
});

postgresTest("fenced appends still reject superseded workers inside a chain transaction", async () => {
  await prepareSchema();
  const journal = newJournal();
  try {
    const integrity = new IntegrityJournal(journal);
    const runId = `fenced-chain-${randomUUID()}`;
    const first = (await journal.acquireRunLease!(runId, "first", 5_000))!;
    await integrity.append([custom(runId, 0, "first")], { lease: first });
    await journal.releaseRunLease!(first);
    const second = (await journal.acquireRunLease!(runId, "second", 5_000))!;

    await assert.rejects(
      () => integrity.append([custom(runId, 1, "zombie")], { lease: first }),
      /lease.*lost/i,
    );
    assert.equal((await journal.events(runId)).filter((e) => e.data.writer === "zombie").length, 0);

    await integrity.append([custom(runId, 1, "second")], { lease: second });
    assert.equal((await integrity.verify()).ok, true);
  } finally {
    await journal.close();
  }
});

function runWriterProcess(writerId: string, batches: number): Promise<{ ok: boolean; appended: number; error?: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(tsxBin, [join(testDir, "integrity-writer.ts"), writerId, String(batches)], {
      cwd: testDir,
      env: {
        ...process.env,
        CONTINUUM_POSTGRES_TEST_URL: connectionString as string,
        CONTINUUM_INTEGRITY_SCHEMA: INTEGRITY_SCHEMA,
      },
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
        reject(new Error(`writer exited ${code} without JSON\nstdout: ${stdout}\nstderr: ${stderr}`));
      }
    });
  });
}

postgresTest("two OS processes writing one database keep a single valid chain", async () => {
  await prepareSchema();
  const [left, right] = await Promise.all([
    runWriterProcess("left", 8),
    runWriterProcess("right", 8),
  ]);
  assert.equal(left.ok, true, left.error);
  assert.equal(right.ok, true, right.error);

  const journal = newJournal();
  try {
    const events = await journal.allEvents();
    const head = await journal.integrityHead!();
    const report = verifyEventChain(events, { head });
    assert.equal(report.ok, true, report.reason);
    assert.equal(head!.counter, events.length);
  } finally {
    await journal.close();
  }
});
