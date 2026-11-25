import assert from "node:assert/strict";
import test from "node:test";
import { MemoryJournal, WorkflowRuntime, defineStep, defineWorkflow } from "@continuum/core";

// The harness itself is benchmarked manually; this guards the workflow shape
// the benchmark depends on so a broken DAG never silently skews results.
test("bench workflow shape completes on the memory backend", async () => {
  const make = (name: string, needs?: string[]) =>
    defineStep<{ seed: number }, number>({
      name,
      ...(needs ? { needs } : {}),
      sideEffect: false,
      execute: ({ input }) => input.seed + name.length,
    });
  const a = make("a");
  const b = make("b");
  const c = make("c", ["a"]);
  const d = make("d", ["a", "b"]);
  const e = make("e", ["c", "d"]);
  const f = make("f", ["e"]);
  const workflow = defineWorkflow<{ seed: number }, [typeof a, typeof b, typeof c, typeof d, typeof e, typeof f]>({
    name: "bench-smoke",
    version: 1,
    steps: [a, b, c, d, e, f],
  });

  const journal = new MemoryJournal();
  const runtime = new WorkflowRuntime({ journal, context: {}, random: () => 0.5 });
  const runId = await runtime.startWorkflow(workflow, { seed: 1 });
  const state = await runtime.waitForRun(runId, 10_000);
  assert.equal(state.status, "succeeded");
  await runtime.close();
});
