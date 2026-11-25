# Continuum architecture

Continuum is built around one principle: **the execution journal is the source of truth, and workers are replaceable projection consumers**. A worker can die at any point. Another worker can rebuild the same state from the journal and continue without application-specific recovery glue.

## Core concepts

### Workflow definition

A workflow is a versioned directed acyclic graph of steps. Each step declares:

- a unique name and dependency names (`needs`),
- an `execute` function,
- an optional `compensate` function,
- retry and timeout policies,
- an optional custom idempotency-key function,
- optional durable delay (`delayMs`), external signal (`waitForSignal`), and conditional gate (`when`),
- whether it represents an externally visible side effect.

Definitions are validated before execution: duplicate names, missing dependencies, self-dependencies, cycles, invalid delays, invalid schedules, and invalid versions are rejected before any journal write.

Topological order determines scheduling eligibility. Reverse topological order determines compensation order.

### Workflow run

A run is one execution of a workflow definition for a JSON-serializable input. Caller-provided IDs such as `checkout:ord_123` are natural domain deduplication handles. Journal adapters enforce uniqueness transactionally.

### Step attempt

Every execution attempt is explicit. The journal records `step.started` before invoking user code and records an outcome when the worker observes success, failure, timeout, cancellation, or interruption.

### Compensation

When forward execution cannot continue, Continuum stops scheduling new work, waits for or aborts in-flight work, skips pending steps, and invokes compensation for completed compensable steps in reverse topological order. Compensation receives the original input/output, stable idempotency key, service context, and abort signal, and retries independently.

## Event journal

The journal is append-only. Runtime state is never mutated in place; it is derived by replaying immutable events.

### Event envelope

| Field | Meaning |
| --- | --- |
| `seq` | Journal-local monotonically increasing sequence |
| `eventId` | Unique event identifier |
| `runId` | Workflow run identifier |
| `workflowName` | Workflow definition name |
| `type` | Event type |
| `at` | Millisecond timestamp |
| `data` | JSON-serializable event payload |

### Event types

Run lifecycle:

- `run.created` — input, version, and step plan are durable
- `run.started` — run execution was initialized
- `run.cancel_requested` — operator requested cancellation
- `run.signaled` — durable external signal and payload were recorded
- `run.succeeded` — all required steps completed successfully
- `run.failed` — execution failed before compensable output existed
- `run.cancelled` — cancellation completed, including required rollback
- `run.compensated` — failure rollback completed
- `run.compensation_failed` — rollback itself exhausted retries

Step execution:

- `step.started`
- `step.succeeded`
- `step.failed`
- `step.interrupted`
- `step.skipped` — rollback skip or conditional branch skip; conditional events include `conditional: true`

Compensation:

- `compensation.started`
- `compensation.step_started`
- `compensation.step_completed`
- `compensation.step_failed`
- `compensation.step_interrupted`
- `compensation.step_skipped`
- `compensation.completed`
- `compensation.failed`

Domain observability:

- `custom` — application-defined event emitted through step context

## Projected state

Run statuses:

| Status | Meaning | Terminal |
| --- | --- | --- |
| `running` | forward execution can still progress | no |
| `compensating` | rollback is in progress | no |
| `succeeded` | every non-skipped step succeeded | yes |
| `failed` | execution failed before compensable output existed | yes |
| `cancelled` | operator cancellation completed | yes |
| `compensated` | failure rollback completed | yes |
| `compensation_failed` | rollback exhausted retries | yes |

Step statuses include `pending`, `running`, `succeeded`, `failed`, `skipped`, `compensating`, `compensated`, `compensation_pending`, `compensation_failed`, and `compensation_skipped`.

## Scheduling algorithm

Each run-owned worker repeatedly performs this loop:

1. Acquire or confirm the database-backed run lease.
2. Load and project the run journal.
3. Stop if the projected state is terminal.
4. Find steps marked `running` that have no live in-memory task; these are interruptions and receive retry decisions.
5. If cancellation or terminal failure is present, stop new work and run compensation/finalization.
6. Otherwise find `pending` steps whose dependencies completed (`succeeded` or conditionally `skipped`) and whose retry/timer deadline is due.
7. Evaluate pure `when` gates; false gates append `step.skipped` instead of executing.
8. Require durable signal payloads for `waitForSignal` steps.
9. Start eligible steps up to the configured concurrency limit.
10. Wait for task completion, retry/timer deadline, signal/cancellation wake, or periodic external wake.
11. When every step is successful or conditionally skipped, compose workflow output and append `run.succeeded`.

Because eligibility is derived from the journal rather than memory, recovery uses the same scheduling loop as normal execution.

## Coordination primitives

### Durable timers

`delayMs` makes a step eligible only after its dependencies complete plus the configured delay. No worker promise or memory timer is required while waiting. The deadline is recomputed deterministically from durable timestamps after restart.

### External signals

`waitForSignal` blocks eligibility until a matching `run.signaled` event exists. The latest payload is projected into run state and passed to the step as `signalPayload`. Signals are durable, JSON-validated, and safe to send from a separate process.

### Conditional branches

`when` is evaluated after ordering dependencies complete. A false result records `step.skipped` with `conditional: true`. Downstream ordering dependencies treat conditional skips as satisfied. Application steps that need an output should declare their data dependency explicitly and handle the absent output.

### Queries

Workflows may declare named `queries`: pure functions of projected run state (`input`, step `outputs`, `signals`, `status`, `steps`) evaluated on demand. Queries never mutate the journal, never replay step code, and are safe to evaluate from any process — including while a different worker owns the run. Results must be JSON-serializable.

```ts
const state = await runtime.queryWorkflow(runId, "orderStatus");
```

Because eligibility and outputs are journal projections rather than in-memory worker state, query results are consistent for any observer that can read the journal; no connection to the owning worker is required. The CLI exposes them as `continuum query <run-id> <name> --workflows module.js`.

### Child workflows

`startChildWorkflow` uses a deterministic child run ID (`parentRunId:parentStepName` by default). If the parent worker crashes or retries, the helper catches the duplicate-run conflict and reattaches to the existing child instead of starting another one. Optional parent-interrupt cancellation propagation is enabled by default.

### Distributed schedules

A workflow may declare a fixed `schedule.intervalMs` and static input. Workers compute deterministic window IDs such as:

```text
scheduled:<workflow>:<version>:<window-start-ms>
```

All workers may attempt the same window; transactional run uniqueness ensures only one run exists. `catchupWindows` optionally backfills a bounded number of missed windows.

## Distributed worker ownership

Continuum uses lease ownership plus fencing tokens.

### Lease acquisition

Each lease row contains:

```text
run_id, worker_id, fencing_token, expires_at
```

Acquisition is atomic in the database:

- if no lease exists, the worker creates token 1;
- if the lease is expired, the worker increments the token and takes ownership;
- if another live worker owns it, acquisition fails;
- release expires the lease but preserves the row, keeping tokens monotonic.

### Lease renewal and loss

The owner renews in the background. If renewal fails, the runtime stops reconciliation and aborts local tasks. Lease expiry is the liveness backstop when a process crashes or is partitioned.

### Fenced event appends

Execution writes carry the lease fencing token. PostgreSQL verifies the token with `SELECT ... FOR UPDATE` inside the same transaction as event insertion. SQLite applies the same ownership check inside its immediate write transaction.

This ordering matters:

- if the owner commits first, a successor can only acquire afterward;
- if the successor acquires first, the old token no longer matches and the zombie write rolls back.

Fencing protects journal ownership. External side effects still require downstream idempotency because a worker can crash after calling a third-party API but before recording the outcome.

## Recovery semantics

A worker interruption is detected structurally:

```text
step.started exists
AND no outcome event exists
AND no live task in this runtime owns the step
```

Recovery appends `step.interrupted` and reschedules the step with backoff derived from its retry policy. Interruptions are infrastructure, not application failure: they never consume the step's failure retry budget, so a deploy or crash cannot kill an otherwise healthy workflow. Operator cancellation is the exception — interrupted work under a cancellation request does not retry.

This is intentionally conservative. The external effect may or may not have happened before the crash. Stable idempotency keys are therefore part of the framework contract.

## Retry, timeout, and cancellation

Retry deadlines are calculated when failure is observed and persisted in the journal. `attempts` includes the first attempt.

Every step receives an `AbortSignal`:

- timeout aborts the signal and records `step.failed` with `STEP_TIMEOUT`,
- operator cancellation aborts live tasks and moves the run toward rollback,
- lease loss aborts local tasks so the new owner becomes the authoritative writer.

JavaScript cannot forcefully kill arbitrary synchronous user code. Cooperative cancellation is part of the framework contract.

## Persistence

### `WorkflowJournal`

The storage contract is deliberately small:

```ts
interface WorkflowJournal {
  append(events: NewContinuumEvent[], options?: { lease?: RunLease }): Promise<ContinuumEvent[]>;
  events(runId: string): Promise<ContinuumEvent[]>;
  allEvents(): Promise<ContinuumEvent[]>;
  runIds(): Promise<string[]>;
  acquireRunLease?(runId: string, workerId: string, ttlMs: number): Promise<RunLease | undefined>;
  renewRunLease?(lease: RunLease, ttlMs: number): Promise<boolean>;
  releaseRunLease?(lease: RunLease): Promise<void>;
  listRunLeases?(): Promise<RunLease[]>;
  close?(): Promise<void> | void;
}
```

### PostgreSQL implementation

`PostgresJournal` uses:

- `continuum_events` (`BIGSERIAL` ordering, JSONB payload, run/type indexes),
- `continuum_runs` for transactional run-ID uniqueness,
- `continuum_leases` for atomic ownership and fencing,
- an advisory lock during idempotent migrations.

This is the horizontally scalable deployment backend.

### SQLite implementation

`SqliteJournal` uses equivalent tables in a local database, WAL mode, JSON validation, immediate write transactions, and the same lease/fencing semantics. It is appropriate for embedded applications, demos, edge nodes, tests, and single-node services.

### Memory implementation

`MemoryJournal` implements the same contract in process for fast tests. It clones stored and returned event payloads so later workflow mutation cannot rewrite history.

## Observability model

The journal itself is the audit log. Observers can consume every appended event and terminal projected states without affecting correctness. The CLI worker can additionally expose:

- `GET /health` — worker identity and liveness,
- `GET /metrics` — Prometheus text metrics,
- `GET /runs` — JSON run summaries.

## Versioning model

Workflow identity is `name@version`. Because the step plan is journaled at `run.created`, replay never depends on the shape of the code that happens to be deployed — unlike rehydration-based systems, deploying a graph change cannot corrupt an in-flight run's history.

In-flight migration is explicit and conservative. A definition may declare `migratesFrom: [<earlier versions>]`. A worker may then adopt an in-flight run of one of those versions when every step that can still execute or compensate exists under the same name in the new definition — steps already terminal in the journal may legitimately be absent, which is what makes removing a completed step a legal migration. The adoption is recorded in the journal as `run.migrated` with the from/to versions, and the run finishes its original journaled graph using the new code. Migration never retrofits new steps into an in-flight run: the plan is fixed at creation, which keeps the meaning of every journaled event stable. Without the opt-in, exact-version matching applies and migration is refused.

## Worker lifecycle

Graceful shutdown is a bounded drain, distinct from a crash: in-flight steps finish and journal their outcomes within the grace window, remaining tasks are aborted, leases are released for immediate adoption, and non-cooperative tasks are abandoned to fencing rather than allowed to hang the shutdown. Crashes differ only in skipping the drain — the same interruption recovery path converges both.

## Design invariants

1. **Journal before execution.** A step is never invoked before its start event is durable.
2. **Outcome before dependent work.** A dependency becomes eligible only after its outcome is durable.
3. **No silent loss of ambiguity.** Interrupted work is explicitly marked and retried with the same idempotency key.
4. **Compensation order is graph-aware.** Rollback runs in reverse topological order.
5. **Recovery is replay of the same state machine.** There is not a separate untested recovery implementation.
6. **Observers cannot corrupt execution.** Observer failures are isolated.
7. **Leases are enforced at write time.** Ownership is verified transactionally with fenced appends, not assumed from memory.
8. **No exactly-once false claims.** The framework provides idempotent at-least-once execution and explicit rollback semantics.
