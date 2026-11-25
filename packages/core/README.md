# @continuum/core

Core durable execution primitives for Continuum.

## Exports

- `defineStep` — declare one typed workflow step
- `defineWorkflow` — declare a versioned DAG of steps
- `defineTimerStep` — declare a durable delay step
- `defineSignalStep` — declare a durable signal wait step
- `WorkflowRuntime` — schedule, execute, signal, cancel, recover, inspect, and schedule runs
- `MemoryJournal` — ephemeral journal for tests
- projection, validation, retry, lease, and error helpers

## Minimal example

```ts
import { defineStep, defineWorkflow, MemoryJournal, WorkflowRuntime } from "@continuum/core";

const charge = defineStep<{ amount: number }, string>({
  name: "charge",
  retry: { attempts: 3, delayMs: 100 },
  execute: ({ input, idempotencyKey }) => payments.charge(idempotencyKey!, input.amount),
  compensate: ({ output, idempotencyKey }) => payments.refund(idempotencyKey!, output),
});

const workflow = defineWorkflow({
  name: "billing",
  version: 1,
  steps: [charge],
});

const runtime = new WorkflowRuntime({ journal: new MemoryJournal(), context: {} });
const handle = await runtime.runWorkflow(workflow, { amount: 4200 });
console.log(await handle.result);
```

## Coordination primitives

- `delayMs` waits durably without consuming worker capacity
- `waitForSignal` waits for `runtime.signalRun`
- `when` conditionally skips branches
- `startChildWorkflow` starts or reattaches to deterministic child runs
- `schedule` creates duplicate-safe fixed-interval runs across distributed workers

Use `@continuum/sqlite` for embedded durable storage or `@continuum/postgres` for distributed workers.
