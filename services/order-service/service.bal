// REST API for order-service.
//
// The HTTP endpoints and the Kafka consumers both funnel state changes
// through transition() in lifecycle.bal. An order cannot reach an illegal
// state, no matter which path the request came in on.
//
// The consumer loops live in the order-consumer package, run as a separate
// process. See docs/ARCHITECTURE.md for the reasoning.

import ballerina/http;
import ballerina/log;

service /api/v1 on new http:Listener(SERVICE_PORT) {

    resource function get health() returns json {
        return {status: "UP", 'service: SERVICE_NAME};
    }

    resource function post orders(@http:Payload PlaceOrderRequest req) returns http:Created|http:BadRequest|http:UnprocessableEntity|http:InternalServerError {
        if req.items.length() == 0 {
            return <http:UnprocessableEntity>{
                body: {code: "UNPROCESSABLE", message: "an order must contain at least one item"}
            };
        }

        decimal total = computeTotal(req.items);
        string orderId = newOrderId();
        Order fresh = newOrder(orderId, req, total);

        log:printInfo("about to save order", orderId = orderId);
        error? saved = saveOrder(fresh);
        log:printInfo("saveOrder returned", orderId = orderId);
        if saved is error {
            log:printError("failed to persist order", saved, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not save order"}
            };
        }

        OrderEvent event = orderToEvent(fresh, TOPIC_ORDERS_CREATED);
        error? published = publish(TOPIC_ORDERS_CREATED, event, orderId);
        if published is error {
            log:printError("failed to publish orders.created", published, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "order saved but event not published"}
            };
        }

        log:printInfo("order placed", orderId = orderId, total = total);
        return <http:Created>{body: fresh};
    }

    resource function get orders(string? customerId, string? status) returns OrderListResponse|http:InternalServerError {
        Order[]|error found = listOrders(customerId, status);
        if found is error {
            log:printError("failed to list orders", found);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list orders"}
            };
        }
        return {items: found, total: found.length()};
    }

    resource function get orders/[string orderId]() returns Order|http:NotFound|http:InternalServerError {
        Order?|error found = findOrder(orderId);
        if found is error {
            log:printError("failed to fetch order", found, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not fetch order"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no order ${orderId}`}
            };
        }
        return found;
    }

    resource function post orders/[string orderId]/cancel(@http:Payload CancelOrderRequest req) returns Order|http:NotFound|http:Conflict|http:InternalServerError {
        Order?|error maybeOrder = findOrder(orderId);
        if maybeOrder is error {
            log:printError("failed to fetch order for cancellation", maybeOrder, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not fetch order"}
            };
        }
        if maybeOrder is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no order ${orderId}`}
            };
        }

        Order existing = maybeOrder;
        Order|error result = transition(existing, CANCELLED, "HTTP", ());
        if result is error {
            return <http:Conflict>{
                body: {code: "CONFLICT", message: result.message()}
            };
        }

        Order updated = result;
        updated.reason = req.reason;

        error? saved = updateOrder(updated);
        if saved is error {
            log:printError("failed to persist cancellation", saved, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not save cancellation"}
            };
        }

        OrderEvent event = orderToEvent(updated, TOPIC_ORDERS_CANCELLED);
        error? published = publish(TOPIC_ORDERS_CANCELLED, event, orderId);
        if published is error {
            log:printError("failed to publish orders.cancelled", published, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "order cancelled but event not published"}
            };
        }

        log:printInfo("order cancelled", orderId = orderId, reason = req.reason);
        return updated;
    }
}
