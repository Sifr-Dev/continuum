import assert from "node:assert/strict";
import test from "node:test";
import type { ContinuumEvent } from "@continuum/core";
import { DemoServices, type OrderInput } from "../src/services.js";

const order: OrderInput = {
  orderId: "ord_test",
  customerId: "customer@example.com",
  amount: 100,
  currency: "USD",
  items: [{ sku: "sku-mouse", quantity: 2 }],
  chaos: "slow-payment",
};

test("concurrent payment attempts share one in-flight capture", async () => {
  const services = new DemoServices();
  const [first, second] = await Promise.all([
    services.chargePayment("payment-key", order),
    services.chargePayment("payment-key", order),
  ]);

  assert.equal(first, second);
  assert.equal(services.effectLog().filter((entry) => entry.type === "payment.captured").length, 1);
});

test("event restore preserves uncompensated inventory reservations across restarts", () => {
  const services = new DemoServices();
  const before = services.inventorySnapshot()["sku-mouse"] ?? 0;
  const reservation = { reservationId: "res_test", items: [{ sku: "sku-mouse", quantity: 2 }] };
  const events = [
    {
      seq: 1,
      eventId: "event-1",
      runId: "run-1",
      workflowName: "checkout",
      type: "step.started",
      at: 1,
      data: { step: "reserveInventory", attempt: 1, idempotencyKey: "reservation-key" },
    },
    {
      seq: 2,
      eventId: "event-2",
      runId: "run-1",
      workflowName: "checkout",
      type: "step.succeeded",
      at: 2,
      data: { step: "reserveInventory", attempt: 1, output: reservation },
    },
  ] satisfies ContinuumEvent[];

  services.restoreFromEvents(events);

  assert.equal(services.inventorySnapshot()["sku-mouse"], before - 2);
});
