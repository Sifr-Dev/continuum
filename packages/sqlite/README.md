# @continuum/sqlite

SQLite journal adapter for Continuum.

```ts
import { SqliteJournal } from "@continuum/sqlite";
import { WorkflowRuntime } from "@continuum/core";

const journal = new SqliteJournal("./continuum.db");
const runtime = new WorkflowRuntime({ journal, context: services });
```

Events are stored in a single append-only `continuum_events` table. Batched appends use transactions and payloads are validated as JSON by SQLite.

The adapter uses `better-sqlite3`, a mature embedded SQLite client. It is best suited to embedded deployments, demos, tests, and single-node services; high-concurrency multi-worker deployments should use a network database adapter with leases.
