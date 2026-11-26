# Continuum

**Continuum is a TypeScript durable execution framework for multi-step application workflows that must survive crashes, flaky dependencies, retries, deployment restarts, cancellation, and partial failure without corrupting business state.**

It targets a problem engineers usually solve with scattered glue code: a checkout, provisioning flow, deployment pipeline, migration, or onboarding sequence performs several external effects, then a process crashes or a dependency fails halfway through. Retrying the whole operation duplicates charges, emails, inventory reservations, or infrastructure mutations. Not retrying leaves the system half-finished. Continuum makes execution itself durable and gives every step a recovery, retry, idempotency, timeout, cancellation, and compensation model.

> Status: production-shaped framework prototype. The core runtime, distributed worker leases, fenced event writes, PostgreSQL and SQLite journals, CLI worker, coordination primitives, tests, and operations demo work end-to-end. It still needs hardening at scale before claiming production parity with mature systems such as Temporal.

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
    manualReview,
    reserveInventory,
    chargePayment,
    createShipment,
    sendConfirmation,
  ],
});

const runId = await runtime.startWorkflow(checkout, order);
```

The framework records an append-only journal before, during, and after execution. If a worker dies, another worker rehydrates the run from the journal, skips completed steps, retries interrupted steps with the same idempotency key, and runs compensating actions in reverse dependency order when forward execution cannot complete.

## Feature set

### Durable execution

- **Typed workflow DAGs** with dependency edges and parallel branch execution
- **Append-only event journal**; run state is derived by replaying immutable events
- **Crash recovery** for steps interrupted between `step.started` and their outcome event
- **Retry policies** with fixed/exponential backoff, jitter, maximum delay, and persisted retry times
- **Per-step timeouts** backed by `AbortSignal`
- **Stable idempotency keys** propagated to every attempt and compensation
- **Saga compensation** in reverse topological order
- **Operator cancellation** with abort propagation and rollback
- **Conditional branches** through pure step-level `when` gates
- **Workflow output composition** from typed step outputs
- **Input validation hooks** and dependency graph validation, including cycle detection
- **Custom domain events** embedded in the same durable journal

### Coordination primitives

- **Durable timers** that delay step eligibility without occupying worker capacity
- **External signals** with durable JSON payloads for human approval and asynchronous callbacks
- **Conditional human-in-the-loop steps** built from signals and `when` gates
- **Child workflows** with deterministic child run IDs and automatic reattachment after parent retries
- **Distributed interval schedules** with deterministic window run IDs and optional catch-up windows
- **Deferred runs** so API processes can enqueue work and separate workers can execute it

### Distributed runtime and persistence

- **PostgreSQL journal** for horizontally scalable multi-worker deployments
- **SQLite journal** for embedded, local, edge, and single-node deployments
- **Atomic run creation** that rejects duplicate caller-assigned run IDs transactionally
- **Distributed worker leases** with monotonically increasing fencing tokens
- **Fenced event writes** so superseded/zombie workers cannot write execution outcomes after losing ownership
- **Lease recovery** after worker death using expiration and reacquisition
- **In-memory journal** for fast deterministic tests

### Operations

- **CLI** for starting, inspecting, signalling, cancelling, exporting, recovering, scheduling, and serving workflows
- **Prometheus-compatible worker metrics endpoint** plus health and run JSON endpoints
- **Interactive operations console** showing run status, DAG progress, manual approvals, event history, retries, rollback, inventory, and external effect logs
- **Observer hooks** for metrics, tracing, log shipping, or custom audit pipelines
- **Node test-runner coverage** for distributed leases, fenced writes, retries, crash recovery, cancellation, signals, timers, schedules, child workflows, compensation, validation, and persistence

## Repository layout

```text
packages/core          workflow model, runtime, reducer, retry engine, memory journal
packages/sqlite        embedded durable SQLite journal adapter
packages/postgres      distributed PostgreSQL journal adapter with leases/fencing
packages/cli           operations CLI (`continuum`)
examples/order-system  realistic checkout demo and operations console
docs                   architecture and operational semantics
```

## Quick start

Requires Node.js 22.5+ (tested in CI on Node 22 and Node 26).

```bash
npm install
npm test
npm run build
npm run demo
```

Open the demo at <http://localhost:4173>.

The demo includes scenarios for:

- **Happy path** — all DAG branches complete
- **Flaky payment** — payment fails twice, retries with the same idempotency key, then succeeds
- **Rollback saga** — inventory is reserved, payment permanently fails, inventory compensation runs in reverse order
- **Timeout retry** — a slow payment attempt times out and is retried idempotently
- **Manual review** — a high-value order waits for a durable operator approval; rejection rolls back inventory

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
  needs: ["manualReview", "reserveInventory"],
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
| `signalPayload` | Latest durable payload for `waitForSignal` steps |
| `emit` | Append a custom domain event to the run journal |
| `startChildWorkflow` | Start or reattach to a deterministic child workflow |

## Signals, timers, conditions, and child workflows

Durable signals pause a workflow without consuming a worker:

```ts
const approval = defineStep<Input, Approval, Outputs, Services>({
  name: "approval",
  waitForSignal: "order.review",
  execute: ({ signalPayload }) => signalPayload as Approval,
});

await runtime.signalRun(runId, "order.review", { approved: true, reviewer: "ops" });
```

Durable timers delay eligibility from the journal rather than holding a sleeping promise:

```ts
const cooldown = defineTimerStep("cooldown", 60_000);
```

Conditional steps skip cleanly while allowing downstream ordering dependencies to continue:

```ts
const manualReview = defineStep<Input, Approval, Outputs, Services>({
  name: "manualReview",
  needs: ["fraudScreen"],
  waitForSignal: "order.review",
  when: ({ outputs }) => outputs.fraudScreen?.risk === "review",
  execute: ({ signalPayload }) => signalPayload as Approval,
});
```

Child workflows use deterministic run IDs, so a parent retry reattaches instead of duplicating the child:

```ts
const childOutput = await startChildWorkflow<ChildInput, ChildOutput>(
  childWorkflow,
  childInput,
);
```

## Distributed deployment with PostgreSQL

Run any number of workers against the same PostgreSQL database:

```bash
continuum worker \
  --workflows ./dist/workflows.js \
  --postgres postgres://postgres:continuum@localhost:5432/continuum \
  --lease-ttl 10000 \
  --metrics-port 9090
```

Enqueue from an API process or another operator command:

```bash
continuum start checkout@1 \
  --workflows ./dist/workflows.js \
  --postgres postgres://postgres:continuum@localhost:5432/continuum \
  --input '{"orderId":"ord_1"}'
```

Operational commands:

```bash
continuum list --postgres "$DATABASE_URL"
continuum inspect <run-id> --postgres "$DATABASE_URL" --events
continuum signal <run-id> order.review --postgres "$DATABASE_URL" --payload '{"approved":true}'
continuum cancel <run-id> --postgres "$DATABASE_URL"
continuum leases --postgres "$DATABASE_URL"
continuum export --postgres "$DATABASE_URL" --out events.json
```

Worker coordination uses database-backed leases. Every execution write carries the worker fencing token and is verified in the same transaction as the event append. A zombie worker that loses its lease cannot complete a write after another worker takes ownership.

## Embedded deployment with SQLite

```ts
import { WorkflowRuntime } from "@continuum/core";
import { SqliteJournal } from "@continuum/sqlite";

const journal = new SqliteJournal("./continuum.db");
const runtime = new WorkflowRuntime({
  journal,
  context: services,
  concurrency: 8,
  workerId: "api-1",
});

runtime.registerWorkflow(checkoutWorkflow);
await runtime.recover();

const handle = await runtime.runWorkflow(checkoutWorkflow, order);
const result = await handle.result;
```

SQLite is appropriate for embedded and single-node deployments. Use PostgreSQL for horizontally scaled workers.

## Observability

The worker can expose Prometheus-compatible metrics on localhost:

```bash
continuum worker --workflows ./dist/workflows.js --postgres "$DATABASE_URL" --metrics-port 9090
```

Endpoints:

- `GET /health` — worker identity and liveness
- `GET /metrics` — Prometheus text metrics for run statuses and active leases
- `GET /runs` — JSON run summaries

For application-specific telemetry, implement `RuntimeObserver`; every durable journal event can be forwarded to logs, metrics, tracing, or audit infrastructure.

## Correctness model

Continuum deliberately uses an **at-least-once execution** model. The framework cannot make a third-party API inherently exactly-once. What it can do is:

1. persist the workflow intent before execution,
2. transactionally reject duplicate run IDs,
3. persist every observed outcome,
4. give each step a stable idempotency key,
5. avoid re-running steps with recorded successful outputs,
6. use leases and fencing to prevent superseded workers from writing outcomes,
7. retry interrupted or failed work from the last durable state,
8. execute compensation when forward progress is impossible.

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
lease/fencing layer ── one owner per run; zombie writes rejected transactionally
       │
       ▼
append-only journal ── run.created, step.started, step.succeeded, ...
       │
       ▼
projection/reducer ── derives current run state for scheduling and operations
       │
       ▼
recovery scan ── adopts non-terminal runs after lease expiry or restart
```

See [docs/architecture.md](docs/architecture.md) for event semantics, state transitions, and design invariants.

## Current limitations

These are explicit design boundaries, not hidden caveats:

- Workflow versioning exists, but automatic migration of in-flight runs across definition versions is not implemented.
- Worker discovery uses periodic recovery polling rather than database change notifications, adding up to about one second of adoption latency.
- The journal replays full run history for projection; very long-running workflows need snapshotting.
- Leases reduce duplicate execution but do not make third-party side effects exactly-once; downstream idempotency is still required.
- Scheduled workflows use fixed intervals, not full cron expressions.
- The operations console is a demo application, not a packaged multi-tenant production UI.
- The prototype has integration coverage but not yet large-scale soak, fault-injection, or performance benchmarks.

## Roadmap toward category leadership

1. `LISTEN/NOTIFY` or logical-decoding-based wakeups for low-latency distributed scheduling
2. Snapshots and journal compaction for long-lived workflows
3. Full cron expressions and schedule pause/resume metadata
4. OpenTelemetry spans and exemplars in addition to Prometheus metrics
5. Deterministic simulation time and chaos-test harness
6. Packaged embeddable operations dashboard with authentication
7. Deployment templates for Kubernetes, Terraform, and managed PostgreSQL
8. Throughput benchmarks, backpressure controls, and batch journal writes
9. Migration tooling for workflow definition evolution

## Development

```bash
npm install
npm run typecheck
npm test
npm run build
npm run demo
```

CI runs typechecking, all unit/integration tests, and builds on Node 22 and 26 with a PostgreSQL 18 service.
