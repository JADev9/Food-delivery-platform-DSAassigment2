# Event catalogue

Every service talks to Kafka. This is the contract between them.

## Topics

| Topic | Partitions | Producer | Consumers |
|---|---|---|---|
| orders.created | 3 | order-service | payment-consumer, notification-consumer |
| orders.confirmed | 3 | order-service | restaurant-consumer, delivery-consumer |
| orders.status.changed | 3 | order-service | customer-consumer, notification-consumer, admin-consumer |
| orders.cancelled | 1 | order-service | payment-consumer, restaurant-consumer, delivery-consumer, notification-consumer |
| payments.completed | 3 | payment-consumer | order-consumer, notification-consumer |
| payments.failed | 1 | payment-consumer | order-consumer, notification-consumer |
| restaurant.order.accepted | 3 | restaurant-consumer | order-consumer, notification-consumer, admin-consumer-kitchen |
| restaurant.order.ready | 3 | restaurant-consumer | order-consumer, delivery-consumer, notification-consumer |
| delivery.assigned | 3 | delivery-consumer | notification-consumer, admin-consumer-delivery |
| delivery.picked-up | 3 | delivery-consumer | order-consumer, notification-consumer |
| delivery.completed | 3 | delivery-consumer | order-consumer, notification-consumer, admin-consumer-delivery |
| delivery.unassigned | 1 | delivery-consumer | notification-consumer |
| notifications.dispatched | 1 | notification-consumer | — (audit only) |

Busy topics get 3 partitions. Quiet ones get 1.

Every event is keyed by `orderId`. All events for one order land on the
same partition and are consumed in order.

## Envelope

Every event carries these four fields:

| Field | Type | Notes |
|---|---|---|
| eventId | string (UUID) | Unique per event. Consumers detect replays with it. |
| eventType | string | The topic name. Lets one handler serve several topics. |
| occurredAt | string | ISO-8601 UTC. |
| orderId | string | The order this concerns. Also the partition key. |

## Event types

### OrderEvent

Topics: `orders.created`, `orders.confirmed`, `orders.status.changed`,
`orders.cancelled`

```json
{
  "eventId": "8f2c1a4b-...",
  "eventType": "orders.created",
  "occurredAt": "2026-10-01T09:15:32.451Z",
  "orderId": "a41b2c3d-...",
  "customerId": "c19d4e5f-...",
  "restaurantId": "r77e8f9a-...",
  "items": [
    {"menuItemId": "m01", "name": "Game Platter", "quantity": 1, "unitPrice": 245.00}
  ],
  "totalAmount": 415.00,
  "status": "CREATED",
  "deliveryAddress": "12 Independence Avenue, Windhoek",
  "reason": null
}
```

`reason` is populated only on cancellation.

### PaymentEvent

Topics: `payments.completed`, `payments.failed`

```json
{
  "eventId": "1d4a7b2c-...",
  "eventType": "payments.completed",
  "occurredAt": "2026-10-01T09:15:33.102Z",
  "paymentId": "p88f3a1b-...",
  "orderId": "a41b2c3d-...",
  "customerId": "c19d4e5f-...",
  "amount": 415.00,
  "status": "COMPLETED",
  "failureReason": null
}
```

`status` is `COMPLETED` or `FAILED`. Refunds are a stored state, not an event.

### KitchenEvent

Topics: `restaurant.order.accepted`, `restaurant.order.ready`

```json
{
  "eventId": "55c1e2f3-...",
  "eventType": "restaurant.order.accepted",
  "occurredAt": "2026-10-01T09:15:34.660Z",
  "orderId": "a41b2c3d-...",
  "restaurantId": "r77e8f9a-...",
  "status": "ACCEPTED",
  "prepTimeMinutes": 15,
  "reason": null
}
```

`status` is `ACCEPTED`, `REJECTED`, or `READY`. A REJECTED event is how
the kitchen cancels an order it cannot fulfil.

### DeliveryEvent

Topics: `delivery.assigned`, `delivery.picked-up`, `delivery.completed`,
`delivery.unassigned`

```json
{
  "eventId": "9ab2c3d4-...",
  "eventType": "delivery.assigned",
  "occurredAt": "2026-10-01T09:30:12.004Z",
  "deliveryId": "d12c3e4f-...",
  "orderId": "a41b2c3d-...",
  "restaurantId": "r77e8f9a-...",
  "customerId": "c19d4e5f-...",
  "driverId": "dr04a1b2-...",
  "driverName": "Petrus Amutenya",
  "status": "ASSIGNED",
  "reason": null
}
```

On `delivery.unassigned`, `driverId` and `driverName` are empty strings
and `reason` explains why no driver was found.

### NotificationEvent

Topic: `notifications.dispatched`

```json
{
  "eventId": "7e31f2a4-...",
  "eventType": "notifications.dispatched",
  "occurredAt": "2026-10-01T09:15:32.900Z",
  "notificationId": "n55a6b7c-...",
  "orderId": "a41b2c3d-...",
  "recipientType": "CUSTOMER",
  "recipientId": "c19d4e5f-...",
  "channel": "PUSH",
  "message": "We have your order, total N$ 415.00."
}
```

`recipientType` is `CUSTOMER`, `RESTAURANT`, `DRIVER`, or `OPS`.
`channel` is `PUSH`, `SMS`, or `EMAIL`.

## Consumer groups

One group per service per concern. A slow group cannot block an
unrelated one.

| Group | Topics |
|---|---|
| order-service-payments | payments.* |
| order-service-kitchen | restaurant.order.* |
| order-service-delivery | delivery.picked-up, delivery.completed |
| payment-service-orders | orders.created, orders.cancelled |
| restaurant-service-orders | orders.confirmed, orders.cancelled |
| delivery-service-kitchen | restaurant.order.ready |
| delivery-service-orders | orders.confirmed, orders.cancelled |
| customer-service-orders | orders.status.changed |
| notification-service-all | every topic |
| admin-service-orders | orders.created, orders.status.changed |
| admin-service-kitchen | restaurant.order.accepted |
| admin-service-delivery | delivery.assigned, delivery.completed |

## Idempotency

Kafka gives at-least-once delivery. Every consumer is idempotent.

| Service | How |
|---|---|
| order-service | transition table rejects a repeat transition |
| payment-service | unique index on orderId |
| restaurant-service | reservations marker per orderId |
| delivery-service | unique index on orderId in deliveries |
| notification-service | handled_events marker per source eventId |
| admin-service | rollups recomputed on read |
