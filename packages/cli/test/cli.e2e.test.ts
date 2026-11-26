import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

/**
 * End-to-end coverage of the operator-facing CLI contract.
 *
 * These run the real CLI entry point against a real SQLite journal, because the
 * regressions they guard (a recovery pass that blocks on runs parked on a
 * durable signal and reports them as failures, and an honest cancellation
 * status) live in command wiring, not in the pure classifier.
 */

const packageRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const repoRoot = resolve(packageRoot, "../..");
const cliEntry = join(repoRoot, "packages/cli/src/cli.ts");
const workflowsModule = join(packageRoot, "test/fixtures/workflows.ts");
const tsxBin = join(repoRoot, "node_modules/.bin/tsx");

interface CliResult {
  code: number;
  stdout: string;
  stderr: string;
}

function runCli(args: readonly string[]): Promise<CliResult> {
  return new Promise((resolveRun) => {
    execFile(
      tsxBin,
      [cliEntry, ...args],
      { cwd: repoRoot, timeout: 120_000 },
      (error, stdout, stderr) => {
        const code = error && typeof error.code === "number" ? error.code : error ? 1 : 0;
        resolveRun({ code, stdout, stderr });
      },
    );
  });
}

async function withJournal(run: (dbPath: string) => Promise<void>): Promise<void> {
  const dir = await mkdtemp(join(tmpdir(), "continuum-cli-"));
  try {
    await run(join(dir, "continuum.db"));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

/** Starts a deferred run and returns its id (the `start` command prints it). */
async function startRun(name: string, dbPath: string): Promise<string> {
  const result = await runCli([
    "start", `${name}@1`, "--workflows", workflowsModule, "--input", "{}", "--db", dbPath,
  ]);
  assert.equal(result.code, 0, `start failed: ${result.stderr}`);
  const runId = result.stdout.trim().split("\n").at(-1)?.trim();
  assert.ok(runId && runId.length > 0, "start must print a run id");
  return runId;
}

test("recover does not hang or fail on a run parked on a durable signal", async () => {
  await withJournal(async (dbPath) => {
    const runId = await startRun("signal-probe", dbPath);

    const started = Date.now();
    const result = await runCli(["recover", "--workflows", workflowsModule, "--db", dbPath]);
    const elapsed = Date.now() - started;

    // The run cannot finish without a human signal, which is a healthy parking
    // state, not a recovery failure.
    assert.equal(result.code, 0, `expected exit 0, got ${result.code}: ${result.stderr || result.stdout}`);
    assert.match(result.stdout, /parked on external input/);
    assert.match(result.stdout, /awaiting_signal/);
    assert.ok(result.stdout.includes(runId), "report must name the run");
    assert.ok(elapsed < 20_000, `recovery must settle promptly, took ${elapsed}ms`);

    // The run is still live and recoverable, not destroyed.
    const list = await runCli(["list", "--db", dbPath]);
    assert.equal(list.code, 0);
    assert.match(list.stdout, /awaiting_signal/);
  });
});

test("recover reports a genuinely failing run and exits non-zero", async () => {
  await withJournal(async (dbPath) => {
    await startRun("failing-probe", dbPath);
    const result = await runCli(["recover", "--workflows", workflowsModule, "--db", dbPath]);
    assert.equal(result.code, 1, "a failed run must produce a non-zero exit code");
    assert.match(result.stdout, /failed/);
    assert.match(result.stdout, /permanent boom/);
  });
});

test("recover completes a runnable run and exits zero", async () => {
  await withJournal(async (dbPath) => {
    await startRun("quick-probe", dbPath);
    const result = await runCli(["recover", "--workflows", workflowsModule, "--db", dbPath]);
    assert.equal(result.code, 0, result.stderr);
    assert.match(result.stdout, /1 finished/);
    assert.match(result.stdout, /succeeded/);
  });
});

test("recover with nothing to adopt succeeds and says so", async () => {
  await withJournal(async (dbPath) => {
    const result = await runCli(["recover", "--workflows", workflowsModule, "--db", dbPath]);
    assert.equal(result.code, 0, result.stderr);
    assert.match(result.stdout, /No recoverable runs found/);
  });
});

test("cancel without workflows reports the rollback as pending, not done", async () => {
  await withJournal(async (dbPath) => {
    const runId = await startRun("signal-probe", dbPath);

    const cancelled = await runCli([
      "cancel", runId, "--db", dbPath, "--reason", "customer asked",
    ]);
    assert.equal(cancelled.code, 0, cancelled.stderr);
    assert.match(cancelled.stdout, /still running/);
    assert.match(cancelled.stdout, /worker/);

    // The stored status must not be reported as cancellled while rollback is pending.
    const inspect = await runCli(["inspect", runId, "--db", dbPath]);
    assert.equal(inspect.code, 0);
    assert.match(inspect.stdout, /status:\s+running/);
    assert.match(inspect.stdout, /cancel_requested/);
    assert.match(inspect.stdout, /customer asked/);
  });
});

test("inspect surfaces the operator phase and the awaited signal", async () => {
  await withJournal(async (dbPath) => {
    const runId = await startRun("signal-probe", dbPath);
    const inspect = await runCli(["inspect", runId, "--db", dbPath]);
    assert.equal(inspect.code, 0);
    assert.match(inspect.stdout, /phase:\s+awaiting_signal/);
    assert.match(inspect.stdout, /order\.review/);
  });
});

test("inspect rejects an unknown run with a non-zero exit", async () => {
  await withJournal(async (dbPath) => {
    const result = await runCli(["inspect", "does-not-exist", "--db", dbPath]);
    assert.equal(result.code, 1);
    assert.match(result.stderr, /does not exist/);
  });
});

test("recover-timeout rejects a malformed value", async () => {
  await withJournal(async (dbPath) => {
    const result = await runCli([
      "recover", "--workflows", workflowsModule, "--db", dbPath, "--recover-timeout", "soon",
    ]);
    assert.equal(result.code, 1);
    assert.match(result.stderr, /recover-timeout/);
  });
});
