// Test fixture: appends integrity-chained events from a *separate process* so a
// regression test can prove two decorator instances sharing one SQLite file
// extend a single chain instead of forking it.
//
// Usage: tsx integrity-writer.ts <db-file> <writer-id> <batches> <per-batch>
// Prints one JSON line: { "ok": true, "appended": n, "writer": "..." }.

import { IntegrityJournal } from "@continuum/core";
import { SqliteJournal } from "../../sqlite/src/index.js";

const [filename, writerId, batchesRaw, perBatchRaw] = process.argv.slice(2);
if (!filename || !writerId) {
  console.error("usage: integrity-writer.ts <db-file> <writer-id> [batches] [events-per-batch]");
  process.exit(2);
}

const batches = Number(batchesRaw ?? 5);
const perBatch = Number(perBatchRaw ?? 4);

const journal = new IntegrityJournal(new SqliteJournal(filename));
try {
  let appended = 0;
  for (let batch = 0; batch < batches; batch += 1) {
    const events = Array.from({ length: perBatch }, (_, n) => ({
      runId: `${writerId}-run`,
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
