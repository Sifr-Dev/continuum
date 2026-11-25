import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import { MemoryJournal, WorkflowRuntime, defineStep, defineWorkflow, type WorkflowJournal } from "@continuum/core";
import { PostgresJournal } from "@continuum/postgres";
import { SqliteJournal } from "@continuum/sqlite";

// Reproducible Continuum performance harness.
//
//   npm run bench --workspace @continuum/bench
//
// Environment:
//   CONTINUUM_BENCH_RUNS          runs per backend for the throughput benchmark (default 100)
//   CONTINUUM_BENCH_ADOPT_RUNS    orphaned runs for the adoption benchmark (default 50)
//   CONTINUUM_POSTGRES_TEST_URL   enables the PostgreSQL backend rows
//   CONTINUUM_BENCH_OUTPUT        JSON output path (default bench-results.json in this package)

interface BackendFactory {
  name: string;
  create: () => WorkflowJournal;
  /** Two journal handles backed by the same store, as separate workers would use. */
  createPair: () => [WorkflowJournal, WorkflowJournal];
  dispose: (journal: WorkflowJournal) => Promise<void>;
}

interface ThroughputResult {
  backend: string;
  runs: number;
  stepsPerRun: number;
  wallMs: number;
  runsPerSec: number;
  stepsPerSec: number;
  runLatencyP50Ms: number;
  runLatencyP95Ms: number;
}

interface AdoptionResult {
  backend: string;
  orphanedRuns: number;
  wallMs: number;
  adoptedPerSec: number;
}

interface AppendResult {
  backend: string;
  appends: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
}

function percentile(sorted: number[], fraction: number): number {
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.ceil(fraction * sorted.length) - 1);
  return sorted[index] ?? 0;
}

function round(value: number, digits = 1): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function buildBenchWorkflow() {
  const make = (name: string, needs?: string[]) =>
    defineStep<{ seed: number }, number>({
      name,
      ...(needs ? { needs } : {}),
      sideEffect: false,
      execute: ({ input, outputs }) => {
        const upstream = Object.values(outputs).reduce<number>((sum, value) => sum + (typeof value === "number" ? value : 0), 0);
        return input.seed + upstream + name.length;
      },
    });
  const a = make("a");
  const b = make("b");
  const c = make("c", ["a"]);
  const d = make("d", ["a", "b"]);
  const e = make("e", ["c", "d"]);
  const f = make("f", ["e"]);
  return defineWorkflow<{ seed: number }, [typeof a, typeof b, typeof c, typeof d, typeof e, typeof f]>({
    name: `bench-${randomUUID().slice(0, 8)}`,
    version: 1,
    steps: [a, b, c, d, e, f],
  });
}

async function throughputBench(factory: BackendFactory, runCount: number): Promise<ThroughputResult> {
  const journal = factory.create();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  const workflow = runtime.registerWorkflow(buildBenchWorkflow());
  const latencies: number[] = [];

  const startedAt = performance.now();
  const completions = await Promise.all(
    Array.from({ length: runCount }, async (_, index) => {
      const perRunStart = performance.now();
      const runId = await runtime.startWorkflow(workflow, { seed: index });
      await runtime.waitForRun(runId, 120_000);
      latencies.push(performance.now() - perRunStart);
    }),
  );
  const wallMs = performance.now() - startedAt;
  void completions;

  await runtime.close();
  await factory.dispose(journal);

  latencies.sort((x, y) => x - y);
  const stepsPerRun = 6;
  return {
    backend: factory.name,
    runs: runCount,
    stepsPerRun,
    wallMs: round(wallMs),
    runsPerSec: round((runCount / wallMs) * 1000),
    stepsPerSec: round(((runCount * stepsPerRun) / wallMs) * 1000),
    runLatencyP50Ms: round(percentile(latencies, 0.5), 2),
    runLatencyP95Ms: round(percentile(latencies, 0.95), 2),
  };
}

async function adoptionBench(factory: BackendFactory, orphanedCount: number): Promise<AdoptionResult> {
  // Simulate orphaned runs: one runtime creates them deferred (no lease held),
  // a fresh runtime must discover and drain all of them.
  const [writerJournal, workerJournal] = factory.createPair();
  const writer = new WorkflowRuntime({ journal: writerJournal, context: {}, random: () => 0.5 });
  const workflow = writer.registerWorkflow(buildBenchWorkflow());
  const runIds: string[] = [];
  for (let index = 0; index < orphanedCount; index += 1) {
    runIds.push(await writer.startWorkflow(workflow, { seed: index }, { defer: true }));
  }
  await writer.close();

  const worker = new WorkflowRuntime({ journal: workerJournal, context: {}, random: () => 0.5 });
  worker.registerWorkflow(workflow);

  const startedAt = performance.now();
  await worker.recover();
  await Promise.all(runIds.map((runId) => worker.waitForRun(runId, 180_000)));
  const wallMs = performance.now() - startedAt;

  await worker.close();
  await factory.dispose(writerJournal);
  await factory.dispose(workerJournal);

  return {
    backend: factory.name,
    orphanedRuns: orphanedCount,
    wallMs: round(wallMs),
    adoptedPerSec: round((orphanedCount / wallMs) * 1000),
  };
}

async function appendBench(factory: BackendFactory, appendCount: number): Promise<AppendResult> {
  const journal = factory.create();
  const runId = `append-bench-${randomUUID()}`;
  await journal.append([{ runId, workflowName: "bench", type: "run.created", data: { workflowName: "bench", version: 1, input: {}, steps: [] } }]);
  const latencies: number[] = [];
  for (let index = 0; index < appendCount; index += 1) {
    const start = performance.now();
    await journal.append([{ runId, workflowName: "bench", type: "custom", data: { index } }]);
    latencies.push(performance.now() - start);
  }
  await factory.dispose(journal);
  latencies.sort((x, y) => x - y);
  return {
    backend: factory.name,
    appends: appendCount,
    p50Ms: round(percentile(latencies, 0.5), 2),
    p95Ms: round(percentile(latencies, 0.95), 2),
    p99Ms: round(percentile(latencies, 0.99), 2),
  };
}

async function main() {
  const runCount = Number(process.env.CONTINUUM_BENCH_RUNS ?? 100);
  const adoptCount = Number(process.env.CONTINUUM_BENCH_ADOPT_RUNS ?? 50);
  const outputPath = process.env.CONTINUUM_BENCH_OUTPUT ?? new URL("../bench-results.json", import.meta.url).pathname;

  const factories: BackendFactory[] = [
    {
      name: "memory",
      create: () => new MemoryJournal(),
      createPair: () => {
        const shared = new MemoryJournal();
        return [shared, shared];
      },
      dispose: async () => {},
    },
    {
      name: "sqlite",
      create: () => new SqliteJournal(`/tmp/continuum-bench-${randomUUID()}.db`),
      createPair: () => {
        const file = `/tmp/continuum-bench-${randomUUID()}.db`;
        return [new SqliteJournal(file), new SqliteJournal(file)];
      },
      dispose: async (journal) => { await journal.close?.(); },
    },
  ];
  if (process.env.CONTINUUM_POSTGRES_TEST_URL) {
    const url = process.env.CONTINUUM_POSTGRES_TEST_URL;
    factories.push({
      name: "postgres",
      create: () => new PostgresJournal(url),
      createPair: () => [new PostgresJournal(url), new PostgresJournal(url)],
      dispose: async (journal) => { await journal.close?.(); },
    });
  }

  const throughput: ThroughputResult[] = [];
  const adoption: AdoptionResult[] = [];
  const appends: AppendResult[] = [];

  for (const factory of factories) {
    console.error(`benchmarking ${factory.name}...`);
    throughput.push(await throughputBench(factory, runCount));
    adoption.push(await adoptionBench(factory, adoptCount));
    appends.push(await appendBench(factory, 200));
  }

  const results = {
    measuredAt: new Date().toISOString(),
    node: process.version,
    platform: `${process.platform}/${process.arch}`,
    config: { runs: runCount, orphanedRuns: adoptCount },
    throughput,
    adoption,
    appends,
  };
  writeFileSync(outputPath, `${JSON.stringify(results, null, 2)}\n`);

  console.log("\n### Throughput (6-step DAG, parallel branches)\n");
  console.log("| backend | runs/sec | steps/sec | run latency p50 | run latency p95 |");
  console.log("| --- | --- | --- | --- | --- |");
  for (const row of throughput) {
    console.log(`| ${row.backend} | ${row.runsPerSec} | ${row.stepsPerSec} | ${row.runLatencyP50Ms}ms | ${row.runLatencyP95Ms}ms |`);
  }
  console.log("\n### Orphaned-run adoption (deferred runs drained by a fresh worker)\n");
  console.log("| backend | runs | wall time | runs/sec |");
  console.log("| --- | --- | --- | --- |");
  for (const row of adoption) {
    console.log(`| ${row.backend} | ${row.orphanedRuns} | ${row.wallMs}ms | ${row.adoptedPerSec} |`);
  }
  console.log("\n### Journal append latency\n");
  console.log("| backend | p50 | p95 | p99 |");
  console.log("| --- | --- | --- | --- |");
  for (const row of appends) {
    console.log(`| ${row.backend} | ${row.p50Ms}ms | ${row.p95Ms}ms | ${row.p99Ms}ms |`);
  }
  console.log(`\nresults written to ${outputPath}`);
}

void main();
