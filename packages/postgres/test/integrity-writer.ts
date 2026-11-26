// Test fixture: appends integrity-chained events from a *separate OS process*
// against a real PostgreSQL database, so a regression test can prove that two
// processes extend one chain instead of forking it.
//
// Usage: tsx integrity-writer.ts <writer-id> <batches> [events-per-batch]
// Requires CONTINUUM_POSTGRES_TEST_URL and, for isolation from other test
// files, CONTINUUM_INTEGRITY_SCHEMA.

import { IntegrityJournal } from "@continuum/core";
import { PostgresJournal } from "../src/index.js";

const [writerId, batchesRaw, perBatchRaw] = process.argv.slice(2);
const connectionString = process.env.CONTINUUM_POSTGRES_TEST_URL;
if (!connectionString || !writerId) {
  console.error("CONTINUUM_POSTGRES_TEST_URL and a writer id are required");
  process.exit(2);
}

const schema = process.env.CONTINUUM_INTEGRITY_SCHEMA;
const batches = Number(batchesRaw ?? 5);
const perBatch = Number(perBatchRaw ?? 4);

const journal = new IntegrityJournal(
  new PostgresJournal({ connectionString, ...(schema ? { options: `-c search_path=${schema}` } : {}) }),
);
const runId = `proc-${writerId}-${process.pid}`;
try {
  let appended = 0;
  for (let batch = 0; batch < batches; batch += 1) {
    const events = Array.from({ length: perBatch }, (_, n) => ({
      runId,
      workflowName: "integrity-writer",
      type: "custom" as const,
      data: { writer: writerId, batch, n },
    }));
    await journal.append(events);
    appended += events.length;
  }
  process.stdout.write(`${JSON.stringify({ ok: true, appended, writer: writerId })}\n`);
} catch (error) {
  process.stdout.write(`${JSON.stringify({ ok: false, writer: writerId, error: String(error) })}\n`);
  process.exitCode = 1;
} finally {
  await journal.close?.();
}
