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

Run leases are acquired atomically in one `INSERT ... ON CONFLICT DO UPDATE` statement. Worker execution writes carry the lease fencing token and verify it inside the same transaction as event insertion. A superseded worker cannot write outcomes after a successor takes ownership.

Lease expiry is computed from the database's `statement_timestamp()`, never the client's `Date.now()`. All workers therefore agree on whether a lease is live: a worker whose host clock is fast cannot steal a live lease, and a worker whose clock is slow cannot hold an expired one past its TTL. `expiresAt` values returned by the adapter are database-clock epoch milliseconds.

Leases reduce duplicate execution and protect journal ownership, but external side effects still require downstream idempotency keys.

## Tests

The test suite requires a PostgreSQL server and is skipped when `CONTINUUM_POSTGRES_TEST_URL` is unset:

```bash
CONTINUUM_POSTGRES_TEST_URL=postgres://postgres:continuum@localhost:5432/continuum \
  npm test --workspace @continuum/postgres
```

Run it against a database dedicated to the suite. Tests share one schema and assume it holds only their own runs. `test/process-crash.test.ts` additionally spawns and `SIGKILL`s a real worker process, so it needs permission to fork child processes.

## CLI

```bash
continuum worker --workflows ./dist/workflows.js --postgres "$DATABASE_URL" --metrics-port 9090
continuum leases --postgres "$DATABASE_URL"
```
