# Architecture

## Shape of the system

16 packages. Each service is a REST API plus one or more Kafka consumers.

```
REST APIs                    Kafka consumers

customer-service :8081
restaurant-service :8082
order-service :8083          order-consumer
payment-service :8084        payment-consumer
delivery-service :8085       delivery-consumer
notification-service :8086   notification-consumer
admin-service :8087          admin-consumer
                             admin-consumer-kitchen
                             admin-consumer-delivery
```

Everything talks to Kafka on `kafka:9092` and MongoDB on
`mongodb:27017`. One database per service.

## Why the split

Ballerina runs HTTP handlers and function invocations on the same
strand pool. A consumer poll loop holds a strand while it polls. With
multiple consumer groups in the same process as an HTTP listener, the
HTTP handlers starve.

The first symptom was weird. A request reached the TCP listener but
never ran the handler. Client got HTTP 408 after 60 seconds. Nothing
in the logs — the handler never started.

Fix: run the REST API and the consumers as separate processes. Same
code, same database, same events. Just two runtimes.

## The order saga

Customer places an order. This is what happens:

```
POST /orders              order-service
   |
   v
orders.created            from order-service
   |
   +--> payment-consumer charges the customer
   |       |
   |       v
   |    payments.completed  from payment-consumer
   |       |
   |       v
   |    order-consumer sets status to CONFIRMED
   |       |
   |       v
   |    orders.confirmed    from order-service
   |       |
   |       v
   |    restaurant-consumer reserves stock, raises a ticket
   |       |
   |       v
   |    restaurant.order.accepted  from restaurant-consumer
   |       |
   |       v
   |    order-consumer sets status to PREPARING
   |
   +--> customer cancels
        orders.cancelled    from order-service
           |
           v
        payment-consumer refunds
        restaurant-consumer restores stock
        delivery-consumer frees the driver
```

Four HTTP calls happen in the whole flow:

- `POST /orders` — customer places the order
- `POST /tickets/{orderId}/ready` — cook marks the food ready
- `POST /deliveries/{deliveryId}/pickup` — driver collects
- `POST /deliveries/{deliveryId}/complete` — driver hands over

Everything else is Kafka.

## The state machine

```
CREATED --payments.completed--> CONFIRMED
   |                                |
   +--payments.failed--> CANCELLED  |
                                    v
                              PREPARING
                                    |
                                    v
                                 READY
                                    |
                          delivery.picked-up
                                    v
                          OUT_FOR_DELIVERY
                                    |
                          delivery.completed
                                    v
                                DELIVERED
```

The transition table is in `order-service/types.bal`. Both the REST
API and the consumers call the same `transition()` function from
`order-service/lifecycle.bal`. Nothing else changes status.

Two things that fall out of this:

- A duplicate event does nothing. Order is already in the target
  state, `transition()` returns it unchanged.
- An out-of-order event errors. Target isn't in the allowed list for
  the current status. Caller logs it and doesn't apply.

## Data

One MongoDB database per service. Nothing reads another service's.

```
customers        customer-service
restaurants      restaurant-service
orders           order-service
payments         payment-service
deliveries       delivery-service
notifications    notification-service
adminreports     admin-service
```

The same `orderId` shows up in several of those. That's expected. The
databases are never joined. If a service needs another's data it
either gets it in an event or calls over HTTP.

## Idempotency

Kafka is at-least-once. A consumer that crashes mid-batch replays the
batch on restart. Every handler deals with that on its own.

| Service | How |
|---|---|
| order-service | transition table rejects a repeat |
| payment-service | unique index on orderId, so a replayed charge finds the existing payment |
| restaurant-service | reservations marker per orderId blocks a second reservation |
| delivery-service | unique index on orderId in deliveries |
| notification-service | handled_events marker per source eventId |
| admin-service | rollups recomputed on read, not incremented |

Producers use acks=all and enableIdempotence. Consumers commit after
the batch, not before. Replay is possible, corruption isn't.

## HTTP between services

Two calls in the whole platform:

- delivery-consumer asks restaurant-service for coordinates. Cached
  events go stale. Timeout plus circuit breaker, degrades to a
  fallback pickup point.
- customer-service asks order-service for order history. Live read.
  Copying orders into the customer database would drift.

Everything else is Kafka.
