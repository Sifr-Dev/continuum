# Continuum

**Continuum is a TypeScript durable execution framework for multi-step application workflows that must survive crashes, flaky dependencies, retries, and operator cancellation without corrupting business state.**

It targets a problem engineers usually solve with scattered glue code: a checkout, provisioning flow, deployment pipeline, migration, or onboarding sequence performs several external effects, then a process crashes or a dependency fails halfway through. Retrying the whole operation duplicates charges, emails, inventory reservations, or infrastructure mutations. Not retrying leaves the system half-finished. Continuum makes the execution itself durable and gives every step a recovery, retry, idempotency, timeout, cancellation, and compensation model.

> Status: substantial framework prototype. The core, SQLite journal, CLI, tests, and operations demo work end-to-end. It is not yet a distributed replacement for Temporal; see [limitations](#current-limitations).

## Why this exists

Typical application code has hidden distributed-systems problems:

```ts
await reserveInventory(order);
await chargePayment(order);      // succeeds
await createShipment(order);     // process crashes here
// Did payment happen? Should we retry? What do we roll back?
```

Continuum turns that into an explicit durable workflow:

```ts
const checkout = defineWorkflow({
  name: "checkout",
  version: 1,
  steps: [
    validateOrder,
    fraudScreen,
    reserveInventory,
    chargePayment,
    createShipment,
    sendConfirmation,
  ],
});

const runId = await runtime.startWorkflow(checkout, order);
```

The framework records an append-only journal before, during, and after execution. If a worker dies, a new worker rehydrates the run from the journal, skips completed steps, retries interrupted steps with the same idempotency key, and runs compensating actions in reverse dependency order when forward execution cannot complete.

## Feature set

### Execution model

- **Typed workflow DAGs** with dependency edges and parallel branch execution
- **Durable event journal**; run state is derived by replaying immutable events
- **Crash recovery** for steps interrupted between `step.started` and their outcome event
- **Retry policies** with fixed/exponential backoff, jitter, maximum delay, and persisted retry times
- **Per-step timeouts** backed by `AbortSignal`
- **Stable idempotency keys** propagated to every attempt and compensation
- **Saga compensation** in reverse topological order
- **Operator cancellation** with abort propagation and rollback
- **Workflow output composition** from typed step outputs
- **Input validation hooks** and dependency graph validation, including cycle detection
- **Custom domain events** embedded in the same durable journal

### Operations

- **Pluggable journal interface** with SQLite and in-memory implementations
- **CLI** for starting, inspecting, cancelling, exporting, recovering, and serving workflows
- **Interactive operations console** showing run status, DAG progress, event history, retries, rollback, inventory, and external effect logs
- **Observer hooks** for metrics, tracing, log shipping, or custom audit pipelines
- **Node test-runner coverage** for retries, crash recovery, cancellation, compensation, validation, and persistence

## Repository layout

```text
packages/core          workflow model, runtime, reducer, retry engine, memory journal
packages/sqlite        embedded durable SQLite journal adapter
packages/cli           operations CLI (`continuum`)
examples/order-system  realistic checkout demo and operations console
docs                   architecture and operational semantics
```

## Quick start

Requires Node.js 22.5+.

```bash
npm install
npm test
npm run build
npm run demo
```

Open the demo at <http://localhost:4173>.

The demo includes failure-injection scenarios:

- **Happy path** — all DAG branches complete
- **Flaky payment** — payment fails twice, retries with the same idempotency key, then succeeds
- **Rollback saga** — inventory is reserved, payment permanently fails, inventory compensation runs in reverse order
- **Timeout retry** — a slow payment attempt times out and is retried idempotently

## Defining a workflow

```ts
import { defineStep, defineWorkflow } from "@continuum/core";

const reserveInventory = defineStep<OrderInput, Reservation, CheckoutOutputs, Services>({
  name: "reserveInventory",
  needs: ["validateOrder"],
  retry: { attempts: 4, delayMs: 100, strategy: "exponential" },
  timeoutMs: 1_000,
  execute: ({ input, context, idempotencyKey }) =>
    context.inventory.reserve(idempotencyKey!, input),
  compensate: ({ context, idempotencyKey, output }) =>
    context.inventory.release(idempotencyKey!, output),
});

const chargePayment = defineStep<OrderInput, Payment, CheckoutOutputs, Services>({
  name: "chargePayment",
  needs: ["fraudScreen", "reserveInventory"],
  retry: { attempts: 4, delayMs: 150 },
  execute: ({ input, context, idempotencyKey }) =>
    context.payments.charge(idempotencyKey!, input),
  compensate: ({ context, idempotencyKey, output }) =>
    context.payments.refund(idempotencyKey!, output),
});
```

A step receives:

| Field | Purpose |
| --- | --- |
| `input` | Durable workflow input |
| `outputs` | Outputs of already completed dependency steps |
| `context` | Application services supplied to the runtime |
| `attempt` | 1-based attempt number |
| `idempotencyKey` | Stable key for every retry of this step in this run |
| `signal` | Abort signal for timeout/cancellation |
| `emit` | Append a custom domain event to the run journal |

## Running and recovering

```ts
import { WorkflowRuntime } from "@continuum/core";
import { SqliteJournal } from "@continuum/sqlite";

const journal = new SqliteJournal("./continuum.db");
const runtime = new WorkflowRuntime({
  journal,
  context: services,
  concurrency: 8,
});

runtime.registerWorkflow(checkoutWorkflow);
await runtime.recover(); // adopt interrupted non-terminal runs after restart

const handle = await runtime.runWorkflow(checkoutWorkflow, order);
const result = await handle.result;
```

For a distributed deployment model, `startWorkflow(..., { defer: true })` durably enqueues the run and a separate CLI worker adopts it:

```bash
continuum worker --workflows ./dist/workflows.js --db ./continuum.db
continuum start checkout@1 --workflows ./dist/workflows.js --db ./continuum.db --input '{"orderId":"ord_1"}'
```

## CLI

```bash
continuum list --db ./continuum.db
continuum inspect <run-id> --db ./continuum.db --events
continuum start checkout@1 --workflows ./dist/workflows.js --input '{"orderId":"ord_1"}' --wait
continuum cancel <run-id> --workflows ./dist/workflows.js --reason "operator requested stop"
continuum recover --workflows ./dist/workflows.js
continuum worker --workflows ./dist/workflows.js
continuum export --db ./continuum.db --out events.json
```

The inspect output includes workflow status, every step state, attempts, compensation attempts, dependencies, and the raw event journal.

## Correctness model

Continuum deliberately uses an **at-least-once execution** model. The framework cannot make a third-party API inherently exactly-once. What it can do is:

1. persist the workflow intent before execution,
2. persist every observed outcome,
3. give each step a stable idempotency key,
4. avoid re-running steps with recorded successful outputs,
5. retry interrupted or failed work from the last durable state,
6. execute compensation when forward progress is impossible.

Application steps that perform external effects should use the provided idempotency key in the downstream system. Compensation functions must also be idempotent because a worker can crash after performing a compensation but before recording its completion.

## Architecture

At a high level:

```text
workflow definition
       │
       ▼
runtime scheduler ── executes ready DAG nodes with concurrency control
       │
       ▼
append-only journal ── run.created, step.started, step.succeeded, ...
       │
       ▼
projection/reducer ── derives current run state for scheduling and operations
       │
       ▼
recovery scan ── finds non-terminal runs and resumes from durable state
```

See [docs/architecture.md](docs/architecture.md) for event semantics, state transitions, and design invariants.

## Current limitations

These are explicit design boundaries, not hidden caveats:

- The scheduler is currently single-process per runtime instance. The journal can share state, but multi-worker lease/claim fencing is not implemented yet.
- The bundled SQLite adapter is embedded/single-node oriented; high-concurrency deployments need a network database adapter.
- Workflow versioning exists, but automatic migration of in-flight runs across definition versions is not implemented.
- Durable timers, external signals, child workflows, and cron scheduling are roadmap features.
- Exactly-once external effects are impossible in general; Continuum provides the idempotency and recovery machinery required to approximate them safely.
- The operations console is a demo application, not a packaged production UI.

## Roadmap

Near-term architectural extensions:

1. Postgres journal adapter with advisory-lock-backed worker leases
2. Durable timers and sleep steps
3. External signals and human-in-the-loop approval steps
4. Child workflows and workflow composition
5. Replay-safe workflow versioning and migration tooling
6. OpenTelemetry spans/metrics exporter
7. Packaged embeddable operations dashboard
8. Chaos-testing harness and deterministic simulation time

## Development

```bash
npm install
npm run typecheck
npm test
npm run build
npm run demo
```

CI runs typechecking, tests, and builds on Node 22.
