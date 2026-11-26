import { defineStep, defineWorkflow } from "@continuum/core";

/**
 * Fixtures for the CLI end-to-end test. Each workflow is a separate export so a
 * test can point `--workflows` at this module and pick the workload with
 * `start <name>@1`.
 */

const approve = defineStep({
  name: "approve",
  waitForSignal: "order.review",
  sideEffect: false,
  execute: ({ signalPayload }) => ({ approved: signalPayload === true }),
});

export const signalWorkflow = defineWorkflow({
  name: "signal-probe",
  version: 1,
  steps: [approve],
});

const ok = defineStep({
  name: "ok",
  sideEffect: false,
  execute: () => ({ ok: true }),
});

export const quickWorkflow = defineWorkflow({
  name: "quick-probe",
  version: 1,
  steps: [ok],
});

const boom = defineStep({
  name: "boom",
  sideEffect: false,
  execute: () => {
    throw new Error("permanent boom");
  },
});

export const failingWorkflow = defineWorkflow({
  name: "failing-probe",
  version: 1,
  steps: [boom],
});

/**
 * The CLI's `--workflows` loader reads `workflows`, a bare workflow, or a
 * default export. Exporting all three lets a test select one with
 * `start <name>@1`.
 */
export const workflows = [signalWorkflow, quickWorkflow, failingWorkflow];
