import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import pg from "pg";

const connectionString = process.env.CONTINUUM_POSTGRES_TEST_URL;
const postgresTest = connectionString ? test : test.skip;
const packageDir = path.dirname(fileURLToPath(new URL("../package.json", import.meta.url)));

function killProcessTree(child: ReturnType<typeof spawn>) {
  // The worker runs as a grandchild of the npx wrapper; kill the whole
  // process group so nothing keeps renewing the lease — like a real crash.
  try {
    if (child.pid) process.kill(-child.pid, "SIGKILL");
  } catch {
    child.kill("SIGKILL");
  }
}

function spawnCrashWorker(env: Record<string, string>) {
  const child = spawn("npx", ["tsx", "test/crash-worker.ts"], {
    cwd: packageDir,
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
    detached: true,
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  return { child, output: () => ({ stdout, stderr }) };
}

async function waitForExit(child: ReturnType<typeof spawn>, timeoutMs: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      killProcessTree(child);
      reject(new Error(`process did not exit within ${timeoutMs}ms`));
    }, timeoutMs);
    child.on("exit", (code) => {
      clearTimeout(timer);
      resolve(code ?? -1);
    });
  });
}

postgresTest("a SIGKILLed worker's run is adopted without duplicating the external effect", async (t) => {
  const suffix = randomUUID().replaceAll("-", "").slice(0, 12);
  const table = `crash_${suffix}`;
  const runId = `crash-run-${suffix}`;
  const admin = new pg.Client({ connectionString });
  await admin.connect();

  // Phase 1: a worker starts the workflow and applies the external effect,
  // then is SIGKILLed before the outcome reaches the journal.
  const first = spawnCrashWorker({
    CONTINUUM_POSTGRES_TEST_URL: connectionString as string,
    CRASH_RUN_ID: runId,
    CRASH_TABLE: table,
    CRASH_MODE: "start",
  });
  t.after(() => { killProcessTree(first.child); });

  let effectRows = 0;
  for (let attempt = 0; attempt < 200 && effectRows === 0; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 100));
    const rows = await admin.query(`SELECT count(*)::int AS count FROM ${table}_effects`).catch(() => undefined);
    effectRows = rows?.rows[0]?.count ?? 0;
  }
  assert.equal(effectRows, 1, `external effect never applied: ${first.output().stderr}`);

  killProcessTree(first.child);
  await waitForExit(first.child, 5_000);

  // Phase 2: an independent process adopts the orphaned run and finishes it.
  const adopter = spawnCrashWorker({
    CONTINUUM_POSTGRES_TEST_URL: connectionString as string,
    CRASH_RUN_ID: runId,
    CRASH_TABLE: table,
    CRASH_MODE: "adopt",
  });
  const exitCode = await waitForExit(adopter.child, 60_000);
  const { stdout, stderr } = adopter.output();
  assert.equal(exitCode, 0, `adopter failed: ${stderr}`);

  const finalState = JSON.parse(stdout.trim().split("\n").pop() ?? "{}");
  assert.equal(finalState.status, "succeeded");

  // The step executed at least twice (once pre-kill, once post-adoption) but
  // the external effect was applied exactly once because both executions used
  // the same stable idempotency key.
  try {
    const effects = await admin.query(`SELECT key FROM ${table}_effects`);
    assert.equal(effects.rows.length, 1);
    assert.equal(effects.rows[0].key, `${runId}:charge`);

    const attempts = await admin.query(`SELECT key FROM ${table}_attempts ORDER BY at`);
    assert.ok(attempts.rows.length >= 2, `expected at least two attempts, got ${attempts.rows.length}`);
    for (const attempt of attempts.rows) {
      assert.equal(attempt.key, `${runId}:charge`);
    }
  } finally {
    await admin.query(`DROP TABLE IF EXISTS ${table}_effects, ${table}_attempts`).catch(() => {});
    await admin.end();
  }
});
