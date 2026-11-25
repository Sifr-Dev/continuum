# Continuum architecture

Continuum is built around a simple idea: **the execution journal is the source of truth, and the runtime is a replaceable projection consumer**. A worker can die at any point. Another worker can rebuild the same state from the journal and continue without asking the application programmer to hand-write recovery logic.

## Core concepts

### Workflow definition

A workflow is a versioned directed acyclic graph of steps. Each step declares:

- a unique name,
- dependency names (`needs`),
- an `execute` function,
- an optional `compensate` function,
- retry and timeout policies,
- an optional custom idempotency-key function,
- whether it represents an externally visible side effect.

Definitions are validated before execution:

- duplicate step names are rejected,
- dependencies must exist,
- self-dependencies are rejected,
- cycles are rejected,
- version must be a positive integer.

The topological order determines scheduling eligibility. Reverse topological order determines compensation order.

### Workflow run

A run is one execution of a workflow definition for a JSON-serializable input. Runs are identified by caller-provided or generated IDs. Providing a domain run ID such as `checkout:ord_123` gives operators a natural deduplication handle.

### Step attempt

Every execution attempt is explicit. The journal records `step.started` before the user function is invoked and records exactly one outcome event when the runtime observes success, failure, timeout, cancellation, or interruption.

### Compensation

When forward execution cannot continue, Continuum stops scheduling new work, waits for in-flight work to finish or abort, skips pending steps, and invokes compensation for completed compensable steps in reverse topological order.

Compensation is retried independently with its own policy. A compensation function receives the original step output, original workflow input, stable idempotency key, services context, and abort signal.

## Event journal

The journal is append-only. Runtime state is never mutated in place; it is derived by replaying events.

### Event envelope

Every event has:

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

#### Run lifecycle

- `run.created` — workflow input and step plan are durable
- `run.started` — worker adopted the run
- `run.cancel_requested` — operator requested cancellation
- `run.succeeded` — all steps completed successfully
- `run.failed` — forward execution failed and there was no compensable completed effect
- `run.cancelled` — cancellation completed, including any required rollback
- `run.compensated` — forward execution failed and rollback completed
- `run.compensation_failed` — rollback itself exhausted retries

#### Step execution

- `step.started`
- `step.succeeded`
- `step.failed`
- `step.interrupted`
- `step.skipped`

#### Compensation

- `compensation.started`
- `compensation.step_started`
- `compensation.step_completed`
- `compensation.step_failed`
- `compensation.step_interrupted`
- `compensation.step_skipped`
- `compensation.completed`
- `compensation.failed`

#### Domain observability

- `custom` — application-defined event emitted through step context

## Projected run state

The reducer derives this state machine:

### Run statuses

| Status | Meaning | Terminal |
| --- | --- | --- |
| `running` | forward execution can still progress | no |
| `compensating` | rollback is in progress | no |
| `succeeded` | every step succeeded | yes |
| `failed` | execution failed before compensable output existed | yes |
| `cancelled` | operator cancellation completed | yes |
| `compensated` | failure rollback completed | yes |
| `compensation_failed` | rollback exhausted retries | yes |

### Step statuses

| Status | Meaning |
| --- | --- |
| `pending` | not running; either never started or waiting for retry |
| `running` | started but no outcome event exists |
| `succeeded` | output is durable |
| `failed` | retries exhausted or execution was interrupted non-retryably |
| `skipped` | never ran because the run moved to rollback |
| `compensating` | compensation started but no outcome exists |
| `compensated` | compensation completed |
| `compensation_pending` | compensation is waiting for retry |
| `compensation_failed` | compensation retries exhausted |
| `compensation_skipped` | step completed but declared no compensation function |

## Scheduling algorithm

The runtime repeatedly performs this loop for each active run:

1. Load and project the run journal.
2. Stop if the projected state is terminal.
3. Find steps marked `running` that have no live in-memory task. These are worker interruptions; record `step.interrupted` with a retry decision.
4. If cancellation or terminal step failure is present:
   - stop scheduling new steps,
   - abort or wait for live tasks,
   - run compensation/finalization.
5. Otherwise find `pending` steps whose dependencies all succeeded and whose retry time is due.
6. Start due steps up to the configured concurrency limit.
7. Wait for a task completion, retry deadline, or wake signal.
8. When every step is successful, compose the workflow output and append `run.succeeded`.

Because eligibility is derived from the journal rather than memory, recovery uses the same scheduling loop as normal execution.

## Recovery semantics

A worker interruption is detected structurally:

```text
step.started exists
AND no step.succeeded/step.failed/step.interrupted exists
AND no live task in this runtime owns the step
```

On recovery the runtime appends `step.interrupted` and makes a retry decision. If attempts remain, the step returns to `pending` with a persisted `nextAttemptAt`; otherwise it becomes terminally failed.

This is intentionally conservative. The external effect may or may not have happened before the crash. That is why stable idempotency keys are part of the framework contract rather than an optional convenience.

## Retry semantics

A retry policy contains:

```ts
{
  attempts: 4,
  delayMs: 100,
  strategy: "exponential",
  maxDelayMs: 30_000,
  jitter: true,
}
```

`attempts` includes the first attempt. For example, `attempts: 4` permits the initial execution plus three retries.

The retry deadline is calculated when the failure is observed and persisted in the failure event. Recovery does not depend on in-memory timers existing before a crash.

## Timeout and cancellation semantics

Every step receives an `AbortSignal`.

- Timeout aborts the signal and records `step.failed` with `STEP_TIMEOUT`.
- Operator cancellation aborts live tasks and records interruption/failure outcomes.
- A step that ignores cancellation and later completes can still have its completion recorded, preventing the journal from losing knowledge of an external effect.

JavaScript cannot forcefully kill arbitrary synchronous user code. Cooperative cancellation is therefore part of the framework contract.

## Compensation semantics

Compensation is saga-style rollback, not time travel.

1. Pending steps are skipped.
2. Successful compensable steps are visited in reverse topological order.
3. Each compensation attempt is journaled before invocation.
4. Compensation failures retry independently.
5. If rollback finishes, the run becomes `cancelled` for operator cancellation or `compensated` for failure rollback.
6. If rollback cannot finish, the run becomes `compensation_failed` and the journal preserves exactly which step could not be rolled back.

Side-effecting steps that do not declare compensation are marked `compensation_skipped`, and the run then finishes as `compensation_failed` with a `MissingCompensationError`. Pure steps marked `sideEffect: false` do not participate in rollback. This makes partial rollback explicit rather than pretending it did not happen.

## Persistence

### `WorkflowJournal`

The storage contract is deliberately small:

```ts
interface WorkflowJournal {
  append(events: NewContinuumEvent[]): Promise<ContinuumEvent[]>;
  events(runId: string): Promise<ContinuumEvent[]>;
  allEvents(): Promise<ContinuumEvent[]>;
  runIds(): Promise<string[]>;
  close?(): Promise<void> | void;
}
```

### SQLite implementation

`SqliteJournal` stores events in one immutable table with indexes on `(run_id, seq)` and `type`. Batches are inserted in a transaction. `json_valid` enforces serialized payloads at the database level.

SQLite is appropriate for embedded applications, demos, tests, and single-node services. High-concurrency multi-worker deployments need a network database adapter with leases.

## Concurrency model

The current scheduler is single-writer per runtime process. A run-local pump prevents concurrent reconciliation of the same run. Independent steps execute concurrently up to the runtime's configured limit.

A future multi-worker implementation needs:

- run-level leases,
- fencing tokens in event appends,
- worker identity in start/outcome events,
- database-backed compare-and-append semantics.

The event model already leaves room for those fields, but they are not yet implemented.

## Versioning model

Workflow identity is `name@version`. Recovery requires the exact version to be registered. This prevents a worker from accidentally resuming a run with a different graph shape.

Safe workflow evolution is future work. It will require explicit migration rules because adding, renaming, or reordering steps changes the meaning of replayed events.

## Observability model

The journal itself is the audit log. Observers can additionally consume:

- every appended event through `onEvent`,
- terminal projected state through `onRunFinished`.

This supports metrics, tracing, alerting, or shipping events to a warehouse without changing workflow correctness.

## Design invariants

The implementation treats these as non-negotiable invariants:

1. **Journal before execution.** A step is never invoked before its start event is durable.
2. **Outcome before scheduling dependent work.** A dependency becomes eligible only after `step.succeeded` is durable.
3. **No silent loss of ambiguity.** Interrupted work is explicitly marked and retried with the same idempotency key.
4. **Compensation order is graph-aware.** Rollback runs in reverse topological order, not arbitrary completion order.
5. **Recovery is replay of the same state machine.** There is not a separate untested recovery implementation.
6. **Observers cannot corrupt execution.** Observer failures are isolated from the runtime.
7. **No exactly-once false claims.** The framework provides idempotent at-least-once execution and explicit rollback semantics.
