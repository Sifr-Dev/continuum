# @continuum/postgres

PostgreSQL journal adapter for distributed Continuum workers.

```ts
import { PostgresJournal } from "@continuum/postgres";
import { WorkflowRuntime } from "@continuum/core";

const journal = new PostgresJournal(process.env.DATABASE_URL!);
const runtime = new WorkflowRuntime({
  journal,
  context: services,
  leaseTtlMs: 10_000,
});

runtime.registerWorkflow(checkoutWorkflow);
await runtime.recover();
```

## Tables

The adapter manages three tables:

- `continuum_events` — immutable JSONB event journal
- `continuum_runs` — transactional run-ID uniqueness
- `continuum_leases` — worker ownership and fencing tokens

Migrations are idempotent and protected by a PostgreSQL advisory lock.

## Distributed correctness

Run leases are acquired atomically. Worker execution writes carry the lease fencing token and verify it inside the same transaction as event insertion. A superseded worker cannot write outcomes after a successor takes ownership.

Leases reduce duplicate execution and protect journal ownership, but external side effects still require downstream idempotency keys.

## CLI

```bash
continuum worker --workflows ./dist/workflows.js --postgres "$DATABASE_URL" --metrics-port 9090
continuum leases --postgres "$DATABASE_URL"
```
