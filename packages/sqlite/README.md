# @continuum/sqlite

SQLite journal adapter for Continuum.

```ts
import { SqliteJournal } from "@continuum/sqlite";
import { WorkflowRuntime } from "@continuum/core";

const journal = new SqliteJournal("./continuum.db");
const runtime = new WorkflowRuntime({ journal, context: services });
```

Events are stored in an append-only `continuum_events` table. Batched appends use immediate transactions and payloads are validated as JSON by SQLite. `continuum_runs` enforces unique run IDs and `continuum_leases` implements the same lease/fencing contract as the PostgreSQL adapter.

## Lease ownership and clock authority

Lease expiry is evaluated against SQLite's own clock (`unixepoch('subsec')`), never the worker process's `Date.now()`. A host whose clock runs fast therefore cannot steal a live lease, and a host whose clock runs slow cannot keep an expired one past its TTL. Lease acquisition runs in a single `BEGIN IMMEDIATE` transaction, so a read-check-write cannot interleave with another connection's acquisition.

## Options

```ts
const journal = new SqliteJournal({
  filename: "./continuum.db",
  // Milliseconds to wait for the write lock before failing with SQLITE_BUSY.
  // Defaults to better-sqlite3's 5000ms; use 0 to fail fast.
  busyTimeoutMs: 5000,
  // Reuse an existing connection. A supplied connection is not closed by
  // journal.close(), mirroring PostgresJournal's `pool` option.
  database: existingDatabase,
});
```

Write failures roll back only when a transaction is still open, so the original error (for example `SQLITE_FULL` on a full disk, which SQLite rolls back automatically) reaches the caller instead of being replaced by a secondary rollback error.

## Tests

`npm test` runs entirely against in-process SQLite databases and needs no external services. Some contention tests spawn a short-lived child process to hold the write lock (`test/lock-holder.cjs`).

The adapter uses `better-sqlite3`, a mature embedded SQLite client. It is best suited to embedded deployments, demos, edge nodes, tests, and single-node services. Horizontally scaled multi-worker deployments should use `@continuum/postgres`.
