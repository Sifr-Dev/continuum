export type ChaosMode = "none" | "flaky-payment" | "payment-outage" | "inventory-outage" | "slow-payment";

export interface OrderItem {
  sku: string;
  quantity: number;
}

export interface OrderInput {
  orderId: string;
  customerId: string;
  amount: number;
  currency: string;
  items: OrderItem[];
  chaos?: ChaosMode;
}

export interface Reservation {
  reservationId: string;
  items: OrderItem[];
}

export interface Payment {
  paymentId: string;
  amount: number;
  currency: string;
  attempts: number;
}

export interface Shipment {
  shipmentId: string;
  carrier: string;
  trackingCode: string;
}

interface EffectLogEntry {
  at: number;
  type: string;
  key: string;
  detail: string;
}

export class DemoServices {
  readonly stock = new Map<string, number>([
    ["sku-keyboard", 12],
    ["sku-mouse", 20],
    ["sku-monitor", 7],
  ]);

  private readonly reservationsByKey = new Map<string, Reservation>();
  private readonly paymentsByKey = new Map<string, Payment>();
  private readonly shipmentsByKey = new Map<string, Shipment>();
  private readonly paymentAttempts = new Map<string, number>();
  private readonly effects: EffectLogEntry[] = [];

  validateOrder(order: OrderInput): { valid: true; itemCount: number } {
    if (!order.orderId) throw new Error("orderId is required");
    if (!order.customerId) throw new Error("customerId is required");
    if (!Number.isFinite(order.amount) || order.amount <= 0) throw new Error("amount must be positive");
    if (!Array.isArray(order.items) || order.items.length === 0) throw new Error("at least one item is required");
    for (const item of order.items) {
      if (!this.stock.has(item.sku)) throw new Error(`unknown SKU ${item.sku}`);
      if (!Number.isInteger(item.quantity) || item.quantity < 1) throw new Error(`invalid quantity for ${item.sku}`);
    }
    return { valid: true, itemCount: order.items.reduce((total, item) => total + item.quantity, 0) };
  }

  async fraudScreen(order: OrderInput): Promise<{ risk: "low" | "review"; score: number }> {
    await delay(80);
    const score = order.amount > 1_000 ? 71 : 18;
    return { risk: score > 80 ? "review" : "low", score };
  }

  async reserveInventory(key: string, order: OrderInput): Promise<Reservation> {
    await delay(120);
    if (order.chaos === "inventory-outage") throw new Error("inventory service is unavailable");
    const existing = this.reservationsByKey.get(key);
    if (existing) return existing;

    for (const item of order.items) {
      const available = this.stock.get(item.sku) ?? 0;
      if (available < item.quantity) throw new Error(`insufficient inventory for ${item.sku}`);
    }
    for (const item of order.items) {
      this.stock.set(item.sku, (this.stock.get(item.sku) ?? 0) - item.quantity);
    }

    const reservation = {
      reservationId: `res_${stableId(key)}`,
      items: order.items,
    };
    this.reservationsByKey.set(key, reservation);
    this.log("inventory.reserved", key, `${reservation.reservationId} for ${order.orderId}`);
    return reservation;
  }

  async releaseInventory(key: string, reservation: Reservation): Promise<void> {
    await delay(60);
    if (!this.reservationsByKey.delete(key)) return;
    for (const item of reservation.items) {
      this.stock.set(item.sku, (this.stock.get(item.sku) ?? 0) + item.quantity);
    }
    this.log("inventory.released", key, reservation.reservationId);
  }

  async chargePayment(key: string, order: OrderInput): Promise<Payment> {
    const existing = this.paymentsByKey.get(key);
    if (existing) return existing;

    const attempts = (this.paymentAttempts.get(key) ?? 0) + 1;
    this.paymentAttempts.set(key, attempts);

    if (order.chaos === "slow-payment" && attempts === 1) {
      await delay(1_500);
    } else {
      await delay(180);
    }

    if (order.chaos === "flaky-payment" && attempts < 3) {
      throw new Error(`payment gateway transiently unavailable (attempt ${attempts})`);
    }
    if (order.chaos === "payment-outage") {
      throw new Error("payment gateway declined the transaction");
    }

    const payment = {
      paymentId: `pay_${stableId(key)}`,
      amount: order.amount,
      currency: order.currency,
      attempts,
    };
    this.paymentsByKey.set(key, payment);
    this.log("payment.captured", key, `${payment.paymentId} ${payment.amount} ${payment.currency}`);
    return payment;
  }

  async refundPayment(key: string, payment: Payment): Promise<void> {
    await delay(90);
    if (!this.paymentsByKey.delete(key)) return;
    this.log("payment.refunded", key, payment.paymentId);
  }

  async createShipment(key: string, order: OrderInput): Promise<Shipment> {
    await delay(140);
    const existing = this.shipmentsByKey.get(key);
    if (existing) return existing;
    const shipment = {
      shipmentId: `shp_${stableId(key)}`,
      carrier: "Continuum Express",
      trackingCode: `TRK-${stableId(key).slice(0, 8).toUpperCase()}`,
    };
    this.shipmentsByKey.set(key, shipment);
    this.log("shipment.created", key, `${shipment.shipmentId} for ${order.orderId}`);
    return shipment;
  }

  async cancelShipment(key: string, shipment: Shipment): Promise<void> {
    await delay(70);
    if (!this.shipmentsByKey.delete(key)) return;
    this.log("shipment.cancelled", key, shipment.shipmentId);
  }

  async sendConfirmation(order: OrderInput, shipment: Shipment): Promise<{ messageId: string }> {
    await delay(50);
    const messageId = `msg_${stableId(`${order.orderId}:${shipment.shipmentId}`)}`;
    this.log("notification.sent", order.orderId, `${messageId} to ${order.customerId}`);
    return { messageId };
  }

  effectLog(): EffectLogEntry[] {
    return [...this.effects];
  }

  inventorySnapshot(): Record<string, number> {
    return Object.fromEntries(this.stock.entries());
  }

  private log(type: string, key: string, detail: string): void {
    this.effects.push({ at: Date.now(), type, key, detail });
  }
}

function stableId(input: string): string {
  let hash = 2166136261;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
