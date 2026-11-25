import { WorkflowValidationError } from "./errors.js";
import type { AnyStepDefinition, AnyWorkflowDefinition } from "./types.js";

export function validateWorkflow(definition: AnyWorkflowDefinition): string[] {
  if (!definition.name || typeof definition.name !== "string") {
    throw new WorkflowValidationError("Workflow must have a non-empty string name");
  }
  if (!Number.isInteger(definition.version) || definition.version < 1) {
    throw new WorkflowValidationError("Workflow version must be a positive integer", { workflow: definition.name });
  }
  if (!Array.isArray(definition.steps) || definition.steps.length === 0) {
    throw new WorkflowValidationError("Workflow must contain at least one step", { workflow: definition.name });
  }

  const seen = new Set<string>();
  const graph = new Map<string, readonly string[]>();

  for (const step of definition.steps) {
    if (!step.name || typeof step.name !== "string") {
      throw new WorkflowValidationError("Every workflow step must have a non-empty string name", {
        workflow: definition.name,
      });
    }
    if (seen.has(step.name)) {
      throw new WorkflowValidationError(`Duplicate workflow step "${step.name}"`, {
        workflow: definition.name,
        step: step.name,
      });
    }
    if (typeof step.execute !== "function") {
      throw new WorkflowValidationError(`Step "${step.name}" does not define an execute function`, {
        workflow: definition.name,
        step: step.name,
      });
    }
    seen.add(step.name);
    graph.set(step.name, step.needs ?? []);
  }

  for (const step of definition.steps) {
    for (const dependency of step.needs ?? []) {
      if (!graph.has(dependency)) {
        throw new WorkflowValidationError(`Step "${step.name}" depends on unknown step "${dependency}"`, {
          workflow: definition.name,
          step: step.name,
          dependency,
        });
      }
      if (dependency === step.name) {
        throw new WorkflowValidationError(`Step "${step.name}" cannot depend on itself`, {
          workflow: definition.name,
          step: step.name,
        });
      }
    }
  }

  return topologicalOrder(definition.steps);
}

export function topologicalOrder(steps: readonly AnyStepDefinition[]): string[] {
  const indegree = new Map<string, number>();
  const dependents = new Map<string, string[]>();

  for (const step of steps) {
    indegree.set(step.name, 0);
    dependents.set(step.name, []);
  }

  for (const step of steps) {
    for (const dependency of step.needs ?? []) {
      indegree.set(step.name, (indegree.get(step.name) ?? 0) + 1);
      dependents.get(dependency)?.push(step.name);
    }
  }

  const queue = steps.filter((step) => (indegree.get(step.name) ?? 0) === 0).map((step) => step.name);
  const ordered: string[] = [];

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current) break;
    ordered.push(current);
    for (const dependent of dependents.get(current) ?? []) {
      const nextDegree = (indegree.get(dependent) ?? 0) - 1;
      indegree.set(dependent, nextDegree);
      if (nextDegree === 0) queue.push(dependent);
    }
  }

  if (ordered.length !== steps.length) {
    const cyclic = steps.map((step) => step.name).filter((name) => !ordered.includes(name));
    throw new WorkflowValidationError("Workflow dependency graph contains a cycle", { cyclicSteps: cyclic });
  }

  return ordered;
}

export function reverseTopologicalOrder(steps: readonly AnyStepDefinition[]): string[] {
  return topologicalOrder(steps).reverse();
}
