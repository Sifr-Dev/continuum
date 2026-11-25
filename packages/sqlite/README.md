# @continuum/sqlite

SQLite journal adapter for Continuum.

```ts
import { SqliteJournal } from "@continuum/sqlite";
import { WorkflowRuntime } from "@continuum/core";

const journal = new SqliteJournal("./continuum.db");
const runtime = new WorkflowRuntime({ journal, context: services });
```

Events are stored in an append-only `continuum_events` table. Batched appends use immediate transactions and payloads are validated as JSON by SQLite. `continuum_runs` enforces unique run IDs and `continuum_leases` implements the same lease/fencing contract as the PostgreSQL adapter.

The adapter uses `better-sqlite3`, a mature embedded SQLite client. It is best suited to embedded deployments, demos, edge nodes, tests, and single-node services. Horizontally scaled multi-worker deployments should use `@continuum/postgres`.
