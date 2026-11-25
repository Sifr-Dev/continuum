# @continuum/core

Core durable execution primitives for Continuum.

## Exports

- `defineStep` — declare one typed workflow step
- `defineWorkflow` — declare a versioned DAG of steps
- `WorkflowRuntime` — schedule, execute, cancel, recover, and inspect runs
- `MemoryJournal` — ephemeral journal for tests
- projection, validation, retry, and error helpers

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

Use `@continuum/sqlite` for durable storage outside tests.
