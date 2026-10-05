# API reference

Every service exposes `GET /api/v1/health`. Base URLs on localhost.

## customer-service — :8081

| Method | Path | Purpose |
|---|---|---|
| POST | /api/v1/customers | Register a customer |
| GET | /api/v1/customers | List customers |
| GET | /api/v1/customers/{customerId} | Fetch one |
| POST | /api/v1/customers/{customerId}/addresses | Save an address |
| GET | /api/v1/customers/{customerId}/addresses | List addresses |
| GET | /api/v1/customers/{customerId}/orders | Order history, proxied to order-service |

POST /customers returns 409 if the email is already registered.

## restaurant-service — :8082

| Method | Path | Purpose |
|---|---|---|
| POST | /api/v1/restaurants | Register a restaurant with its menu |
| GET | /api/v1/restaurants?openOnly=true | List restaurants |
| GET | /api/v1/restaurants/{id} | Fetch one |
| GET | /api/v1/restaurants/{id}/menu | Fetch the menu |
| POST | /api/v1/restaurants/{id}/menu | Add a dish |
| PUT | /api/v1/restaurants/{id}/menu/{menuItemId}/stock | Set stock |
| PUT | /api/v1/restaurants/{id}/availability | Open or close the kitchen |
| GET | /api/v1/restaurants/{id}/tickets?status= | Kitchen queue |
| POST | /api/v1/tickets/{orderId}/ready | Mark food ready — publishes restaurant.order.ready |

## order-service — :8083

| Method | Path | Purpose |
|---|---|---|
| POST | /api/v1/orders | Place an order — publishes orders.created |
| GET | /api/v1/orders?customerId=&status= | List orders |
| GET | /api/v1/orders/{orderId} | Fetch one with its audit trail |
| POST | /api/v1/orders/{orderId}/cancel | Cancel — publishes orders.cancelled |

`totalAmount` is computed server-side from the line items.

Cancel returns 409 once the order has left the restaurant.

## payment-service — :8084

Read-only. Charging happens only in the consumer.

| Method | Path | Purpose |
|---|---|---|
| GET | /api/v1/payments?customerId= | List payments |
| GET | /api/v1/payments/{paymentId} | Fetch one |
| GET | /api/v1/orders/{orderId}/payment | The payment for an order |

## delivery-service — :8085

| Method | Path | Purpose |
|---|---|---|
| POST | /api/v1/drivers | Register a driver |
| GET | /api/v1/drivers?availableOnly=true | List drivers |
| GET | /api/v1/drivers/{driverId} | Fetch one |
| PUT | /api/v1/drivers/{driverId}/location | Report position |
| PUT | /api/v1/drivers/{driverId}/status | On/off shift |
| GET | /api/v1/deliveries?driverId=&status= | List deliveries |
| GET | /api/v1/deliveries/by-order/{orderId} | Track an order |
| GET | /api/v1/deliveries/{deliveryId} | Fetch one |
| POST | /api/v1/deliveries/{deliveryId}/pickup | Collected — publishes delivery.picked-up |
| POST | /api/v1/deliveries/{deliveryId}/complete | Handed over — publishes delivery.completed |

Assignment is automatic on restaurant.order.ready. There is no assign endpoint.

## notification-service — :8086

Read-only.

| Method | Path | Purpose |
|---|---|---|
| GET | /api/v1/notifications?orderId=&recipientId= | List alerts |
| GET | /api/v1/notifications/find/{orderId} | Every alert for one order |

## admin-service — :8087

Read-only. Computed from the local read model.

| Method | Path | Purpose |
|---|---|---|
| GET | /api/v1/reports/summary | Platform totals |
| GET | /api/v1/reports/restaurants | Per-restaurant statistics |
| GET | /api/v1/reports/drivers | Per-driver statistics |
| GET | /api/v1/reports/delivery/performance | Average minutes order to delivery |
| GET | /api/v1/reports/orders | The read model |
