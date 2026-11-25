import { defineStep, defineWorkflow } from "@continuum/core";
import { DemoServices, type OrderInput, type Payment, type Reservation, type Shipment } from "./services.js";

export interface CheckoutOutputs {
  validateOrder: { valid: true; itemCount: number };
  fraudScreen: { risk: "low" | "review"; score: number };
  manualReview: { approved: true; reviewer: string };
  reserveInventory: Reservation;
  chargePayment: Payment;
  createShipment: Shipment;
  sendConfirmation: { messageId: string };
}

export function createCheckoutWorkflow(services: DemoServices) {
  const validateOrder = defineStep<OrderInput, CheckoutOutputs["validateOrder"], CheckoutOutputs, DemoServices>({
    name: "validateOrder",
    description: "Validate order shape, positive amount, and known SKUs",
    sideEffect: false,
    execute: ({ input }) => services.validateOrder(input),
  });

  const fraudScreen = defineStep<OrderInput, CheckoutOutputs["fraudScreen"], CheckoutOutputs, DemoServices>({
    name: "fraudScreen",
    description: "Score the order for fraud in parallel with inventory reservation",
    needs: ["validateOrder"],
    sideEffect: false,
    execute: ({ input }) => services.fraudScreen(input),
  });

  const reserveInventory = defineStep<OrderInput, Reservation, CheckoutOutputs, DemoServices>({
    name: "reserveInventory",
    description: "Atomically reserve stock using the step idempotency key",
    needs: ["validateOrder"],
    retry: { attempts: 4, delayMs: 120, strategy: "exponential", maxDelayMs: 700 },
    timeoutMs: 900,
    execute: ({ input, idempotencyKey }) => services.reserveInventory(idempotencyKey ?? "", input),
    compensate: ({ idempotencyKey, output }) => services.releaseInventory(idempotencyKey ?? "", output),
  });

  const manualReview = defineStep<OrderInput, CheckoutOutputs["manualReview"], CheckoutOutputs, DemoServices>({
    name: "manualReview",
    description: "Wait for a durable operator approval only when fraud risk requires review",
    needs: ["fraudScreen"],
    waitForSignal: "order.review",
    sideEffect: false,
    when: ({ outputs }) => outputs.fraudScreen?.risk === "review",
    execute: ({ signalPayload }) => {
      const review = signalPayload as { approved?: unknown; reviewer?: unknown } | undefined;
      if (review?.approved !== true) throw new Error("manual review rejected the order");
      return { approved: true, reviewer: typeof review.reviewer === "string" ? review.reviewer : "operations" };
    },
  });

  const chargePayment = defineStep<OrderInput, Payment, CheckoutOutputs, DemoServices>({
    name: "chargePayment",
    description: "Capture payment once, even when the gateway is flaky",
    needs: ["manualReview", "reserveInventory"],
    retry: { attempts: 4, delayMs: 150, strategy: "exponential", maxDelayMs: 900 },
    timeoutMs: 1_000,
    execute: ({ input, idempotencyKey, signal }) => services.chargePayment(idempotencyKey ?? "", input, signal),
    compensate: ({ idempotencyKey, output }) => services.refundPayment(idempotencyKey ?? "", output),
  });

  const createShipment = defineStep<OrderInput, Shipment, CheckoutOutputs, DemoServices>({
    name: "createShipment",
    description: "Create the outbound shipment after payment succeeds",
    needs: ["chargePayment"],
    retry: { attempts: 3, delayMs: 100 },
    execute: ({ input, idempotencyKey }) => services.createShipment(idempotencyKey ?? "", input),
    compensate: ({ idempotencyKey, output }) => services.cancelShipment(idempotencyKey ?? "", output),
  });

  const sendConfirmation = defineStep<OrderInput, CheckoutOutputs["sendConfirmation"], CheckoutOutputs, DemoServices>({
    name: "sendConfirmation",
    description: "Send the customer-facing confirmation notification",
    needs: ["createShipment"],
    retry: { attempts: 3, delayMs: 80 },
    execute: ({ input, outputs }) => {
      if (!outputs.createShipment) throw new Error("shipment output missing");
      return services.sendConfirmation(input, outputs.createShipment);
    },
  });

  return defineWorkflow<
    OrderInput,
    [
      typeof validateOrder,
      typeof fraudScreen,
      typeof manualReview,
      typeof reserveInventory,
      typeof chargePayment,
      typeof createShipment,
      typeof sendConfirmation,
    ],
    DemoServices
  >({
    name: "checkout",
    version: 1,
    description: "Resilient order checkout with idempotent effects and saga rollback",
    steps: [validateOrder, fraudScreen, manualReview, reserveInventory, chargePayment, createShipment, sendConfirmation],
    validateInput: (input) => input as OrderInput,
    output: ({ input, outputs }) => ({
      orderId: input.orderId,
      reservation: outputs.reserveInventory,
      payment: outputs.chargePayment,
      review: outputs.manualReview ?? null,
      shipment: outputs.createShipment,
      confirmation: outputs.sendConfirmation,
    }),
    queries: {
      orderStatus: ({ input, outputs, status, steps }) => ({
        orderId: input.orderId,
        runStatus: status,
        paid: outputs.chargePayment !== undefined,
        shipped: outputs.createShipment !== undefined,
        awaitingReview: steps.manualReview?.status === "pending",
        completedSteps: Object.values(steps).filter((step) => step.status === "succeeded").length,
        totalSteps: Object.keys(steps).length,
      }),
      paymentSummary: ({ outputs }) => outputs.chargePayment ?? null,
    },
  });
}

// This export lets the generic CLI load and operate the demo workflow in a
// standalone worker process. The HTTP server instead creates a workflow bound
// to the exact service instance backing its inventory/effect endpoints.
export const checkoutWorkflow = createCheckoutWorkflow(new DemoServices());
export const workflows = [checkoutWorkflow];
